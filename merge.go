package fold

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb/v2"
)

// Merge performs the CRDT-style merge from inc/ into main/.
func (t *Table[T]) Merge() error {
	if err := mkdirAllDurable(t.mainDir(), t.db.dir); err != nil {
		return err
	}

	t.db.cleanStaleStaging()

	incFiles := listParquetFiles(t.incDir())
	if len(incFiles) == 0 {
		return nil
	}

	if len(t.schema.Partitions) > 0 {
		return t.mergePartitioned()
	}
	return t.mergeFull()
}

// mergePartitioned merges partitions concurrently.
func (t *Table[T]) mergePartitioned() error {
	schema := t.schema
	partDirs := discoverPartitions(t.incDir(), len(schema.Partitions))
	if len(partDirs) == 0 {
		return nil
	}

	t.db.logf("[Fold] %s: found %d partitions to merge", schema.Name, len(partDirs))

	errs := t.db.runPartitionJobs(schema.Name, partDirs, t.mergeOnePartition)

	cleanIncLeftovers(t.incDir())

	if len(errs) > 0 {
		return fmt.Errorf("some partitions failed: %s", strings.Join(errs, "; "))
	}

	t.db.logf("[Fold] %s: %d partitions merged", schema.Name, len(partDirs))
	return nil
}

// runPartitionJobs runs fn over parts with a bounded worker pool, logging each
// failure as it happens and returning one "part: err" entry per failure.
func (db *DB) runPartitionJobs(name string, parts []string, fn func(string) error) []string {
	workers := db.compact.Workers
	if workers > len(parts) {
		workers = len(parts)
	}

	jobs := make(chan string, len(parts))
	for _, p := range parts {
		jobs <- p
	}
	close(jobs)

	type result struct {
		part string
		err  error
	}
	results := make(chan result, len(parts))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				results <- result{part: p, err: fn(p)}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	var errs []string
	for r := range results {
		if r.err != nil {
			db.logf("[Fold] %s partition %s failed: %v", name, r.part, r.err)
			errs = append(errs, fmt.Sprintf("%s: %v", r.part, r.err))
		}
	}
	return errs
}

// mergeOnePartition merges a single partition.
func (t *Table[T]) mergeOnePartition(partDir string) error {
	mainPartDir := filepath.Join(t.mainDir(), partDir)
	if err := mkdirAllDurable(mainPartDir, t.db.dir); err != nil {
		return err
	}
	return t.compactPartition(mainPartDir, func() ([]string, error) {
		// Collect this partition's inc files from all sources; recovery has
		// already removed any that the last commit consumed.
		var files []string
		sources, err := os.ReadDir(t.incDir())
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		for _, src := range sources {
			if !src.IsDir() {
				continue
			}
			files = append(files, listParquetFiles(filepath.Join(t.incDir(), src.Name(), partDir))...)
		}
		return files, nil
	}, true)
}

// mergeFull merges an unpartitioned table.
func (t *Table[T]) mergeFull() error {
	err := t.compactPartition(t.mainDir(), func() ([]string, error) {
		return listParquetFiles(t.incDir()), nil
	}, true)
	if err != nil {
		return err
	}

	cleanIncLeftovers(t.incDir())

	t.db.logf("[Fold] %s: merge complete", t.schema.Name)
	return nil
}

// compactPartition folds collected input parquet into dir's active file set
// and publishes the result as the partition's single active file. It is the
// shared core of Merge and Upsert. collect runs after crash recovery, so it
// never sees inputs a previous commit already consumed. consumeInputs records
// the collected files in the commit so a retry never re-applies them (true
// for merge; false for upsert, whose inputs are transient staging files
// outside the partition directory).
func (t *Table[T]) compactPartition(dir string, collect func() ([]string, error), consumeInputs bool) error {
	// Serialize with any concurrent publish (merge or upsert) on this partition.
	unlock := t.db.lockPartition(dir)
	defer unlock()

	// Complete any cleanup an interrupted publish skipped (delete already-
	// consumed inc, remove superseded/orphaned files) before reading.
	if err := recoverPartition(t.db.storage, dir); err != nil {
		return err
	}

	incFiles, err := collect()
	if err != nil {
		return err
	}
	if len(incFiles) == 0 {
		return nil
	}

	// Active main files come from the manifest, so files left behind by an
	// interrupted publish are ignored rather than read twice.
	mainFiles, err := activeMainFiles(t.db.storage, dir)
	if err != nil {
		return err
	}
	localMain, cleanupMain, err := localizeMainFiles(t.db.storage, dir, mainFiles)
	if err != nil {
		return err
	}
	defer cleanupMain()

	db, cleanup, err := openDuckDB(dir, t.db.compact.DuckDB)
	if err != nil {
		return err
	}
	defer cleanup()

	incCols, err := describeColumns(db, incFiles)
	if err != nil {
		return fmt.Errorf("inspect inc columns: %w", err)
	}
	mainCols := map[string]bool{}
	if len(localMain) > 0 {
		if mainCols, err = describeColumns(db, localMain); err != nil {
			return fmt.Errorf("inspect main columns: %w", err)
		}
	}

	query := buildCompactQuery(t.schema, incFiles, localMain, incCols, mainCols)
	var consumed []string
	if consumeInputs {
		consumed = incFiles
	}
	return t.publishCompact(db, dir, query, consumed)
}

// buildCompactQuery returns the single streaming SELECT that pre-merges inc
// rows (GROUP BY primary key) and folds them into the active main rows (FULL
// OUTER JOIN). DuckDB executes it as one pipeline feeding the COPY: nothing
// is materialized into tables, and operators spill to the job's temp
// directory under memory pressure.
func buildCompactQuery(schema *Schema, incFiles, mainFiles []string, incCols, mainCols map[string]bool) string {
	pkCols := schema.PKColumns()
	activeFields := filterActiveFields(schema.Fields, incCols)
	quotedPKs := make([]string, len(pkCols))
	for i, col := range pkCols {
		quotedPKs[i] = sqlIdent(col)
	}
	groupBy := strings.Join(quotedPKs, ", ")

	sel := groupBy
	for _, f := range activeFields {
		if f.Strategy != StrategyPK {
			sel += ", " + f.GetAggExpr()
		}
	}
	incQuery := fmt.Sprintf(`SELECT %s FROM read_parquet([%s], union_by_name=true) GROUP BY %s`,
		sel, buildFileList(incFiles), groupBy)

	if len(mainFiles) == 0 {
		return incQuery
	}

	// Column sets on each side of the join: s (pre-merged inc) has exactly
	// the active fields; t (main) has whatever the active files contain.
	inInc := make(map[string]bool, len(activeFields))
	for _, f := range activeFields {
		inInc[f.Column] = true
	}
	var selectExprs []string
	for _, f := range schema.Fields {
		switch {
		case inInc[f.Column] && mainCols[f.Column]:
			selectExprs = append(selectExprs, f.GetSQLExpr()+" AS "+sqlIdent(f.Column))
		case inInc[f.Column]:
			selectExprs = append(selectExprs, "s."+sqlIdent(f.Column))
		case mainCols[f.Column]:
			selectExprs = append(selectExprs, "t."+sqlIdent(f.Column))
		}
	}
	joinConds := make([]string, len(quotedPKs))
	for i, col := range quotedPKs {
		joinConds[i] = fmt.Sprintf("s.%s = t.%s", col, col)
	}

	return fmt.Sprintf(`SELECT %s FROM (%s) s FULL OUTER JOIN read_parquet([%s], union_by_name=true) t ON %s`,
		strings.Join(selectExprs, ", "), incQuery, buildFileList(mainFiles), strings.Join(joinConds, " AND "))
}

// publishCompact streams query into a staged parquet file, finishes it
// completely (bloom rewrite) and validates it before it becomes active, then
// atomically publishes it as the partition's single active file via the
// manifest, recording consumedInc so a retry never re-applies it. Bloom and
// validation run on the staging file so a failure never publishes over live
// data.
func (t *Table[T]) publishCompact(db *sql.DB, dir, query string, consumedInc []string) error {
	name := segmentFileName(time.Now().UnixMilli())
	filesDir := filepath.Join(dir, filesSubdir)
	// Durable up to the partition dir (whose own chain the merge/upsert entry
	// points already made durable): the files/ dirent must not depend on the
	// later manifest-write sync for its durability.
	if err := mkdirAllDurable(filesDir, dir); err != nil {
		return fmt.Errorf("create files dir: %w", err)
	}
	tmpFile := filepath.Join(filesDir, "."+name+".tmp")
	finalRel := filepath.Join(filesSubdir, name)
	finalFile := filepath.Join(dir, finalRel)

	res, err := db.Exec(fmt.Sprintf(`COPY (%s) TO %s (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)`,
		query, sqlQuote(tmpFile)))
	if err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("write result: %w", err)
	}
	written, err := res.RowsAffected()
	if err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("rows written: %w", err)
	}

	// Bloom rewrite runs on the staging file, before it becomes active. It is
	// skipped (an optimization, not required for correctness) when disabled or
	// when the output is large enough that the whole-file rewrite risks memory
	// pressure.
	if err := t.maybeAddBloom(tmpFile); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("add bloom filters: %w", err)
	}

	// A truncated or corrupt staging file must never be published over live
	// data: it has to read back with exactly the row count COPY reported.
	var fileRows int64
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet(%s)`, sqlQuote(tmpFile))).Scan(&fileRows); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("count staged file: %w", err)
	}
	if fileRows != written {
		os.Remove(tmpFile)
		return fmt.Errorf("row count mismatch: wrote %d rows, staged file has %d", written, fileRows)
	}

	if err := t.db.storage.UploadFile(tmpFile, finalFile); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("publish result: %w", err)
	}

	return commitActive(t.db.storage, dir, []string{finalRel}, consumedInc)
}

// segmentFileName names one published segment. The wall clock alone is not a
// safe name: two publishes on the same partition within one millisecond — or
// after a clock step backwards — would reuse it, and uploading the new output
// over the still-active previous file mutates live data outside the manifest
// commit point. The full 122-bit random UUID makes names collision-free
// without relying on clock monotonicity (a truncated suffix would reopen a
// residual collision window).
func segmentFileName(unixMilli int64) string {
	return fmt.Sprintf("merged_%d_%s.parquet", unixMilli, uuid.New())
}

// maybeAddBloom rewrites the staged file with bloom filters unless the schema
// has no bloom columns, they are disabled, or the output is large enough that
// the whole-file rewrite would risk memory pressure. Bloom filters only
// accelerate primary-key lookups; they are never required for correctness, so
// skipping them is always safe.
func (t *Table[T]) maybeAddBloom(path string) error {
	opts := t.db.compact
	if opts.DisableBloom || len(t.schema.BloomColumns()) == 0 {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > opts.BloomMaxFileBytes {
		t.db.logf("[Fold] %s: skipping bloom rewrite for %d-byte output (limit %d)", t.schema.Name, info.Size(), opts.BloomMaxFileBytes)
		return nil
	}
	// Serialize the whole-file rewrite so concurrent partition workers don't load
	// several files into memory at once. Combined with the per-file size cap
	// above, peak bloom-rewrite memory stays near a single file.
	t.db.bloomMu.Lock()
	defer t.db.bloomMu.Unlock()
	return addBloomFilters(path, t.schema)
}

// --- DuckDB helpers ---

// openDuckDB opens an in-memory DuckDB for one compaction job. The streaming
// compact query materializes nothing into tables, so the database itself stays
// tiny; operators spill to a per-job temp directory under dir (or opts.TempDir
// when set) once memory_limit is reached. cleanup closes the DB and removes
// the derived spill directory.
func openDuckDB(dir string, opts DuckDBOptions) (db *sql.DB, cleanup func(), err error) {
	db, err = sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, err
	}
	tempDir := opts.TempDir
	removeTemp := ""
	if tempDir == "" {
		tempDir = filepath.Join(dir, fmt.Sprintf(".duckdb_tmp_%d", time.Now().UnixNano()))
		removeTemp = tempDir
	}
	cleanup = func() {
		db.Close()
		if removeTemp != "" {
			os.RemoveAll(removeTemp)
		}
	}

	// Apply execution settings, surfacing a rejected option (e.g. an invalid
	// memory_limit or temp_directory) instead of silently running with
	// unintended defaults.
	pragmas := []string{
		fmt.Sprintf("PRAGMA memory_limit=%s", sqlQuote(opts.MemoryLimit)),
		fmt.Sprintf("PRAGMA threads=%d", opts.Threads),
		fmt.Sprintf("PRAGMA temp_directory=%s", sqlQuote(tempDir)),
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("configure duckdb (%s): %w", p, err)
		}
	}
	return db, cleanup, nil
}

// describeColumns returns the union column set of parquet files without
// materializing them (DESCRIBE plans the scan but reads no rows). Failures
// are fatal to the caller: swallowing one would filter every non-PK field out
// of the merge and silently drop data.
func describeColumns(db *sql.DB, files []string) (map[string]bool, error) {
	rows, err := db.Query(fmt.Sprintf(`DESCRIBE SELECT * FROM read_parquet([%s], union_by_name=true)`, buildFileList(files)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	outCols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	cols := make(map[string]bool)
	for rows.Next() {
		var name string
		dest := make([]any, len(outCols))
		dest[0] = &name
		for i := 1; i < len(dest); i++ {
			dest[i] = new(sql.NullString)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

func filterActiveFields(fields []Field, cols map[string]bool) []Field {
	var result []Field
	for _, f := range fields {
		if f.Strategy == StrategyPK || cols[f.Column] {
			result = append(result, f)
		}
	}
	return result
}

func discoverPartitions(incTableDir string, partDepth int) []string {
	partSet := make(map[string]bool)

	filepath.Walk(incTableDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".parquet") {
			return nil
		}
		rel, _ := filepath.Rel(incTableDir, path)
		parts := strings.Split(filepath.Dir(rel), string(filepath.Separator))
		if len(parts) < partDepth+1 {
			return nil
		}
		partParts := parts[len(parts)-partDepth:]
		valid := true
		for _, p := range partParts {
			if !strings.Contains(p, "=") {
				valid = false
				break
			}
		}
		if valid {
			partSet[filepath.Join(partParts...)] = true
		}
		return nil
	})

	var result []string
	for p := range partSet {
		result = append(result, p)
	}
	sort.Strings(result)
	return result
}

func listParquetFiles(dir string) []string {
	var files []string
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".parquet") && !strings.HasPrefix(info.Name(), ".") {
			files = append(files, path)
		}
		return nil
	})
	return files
}

// sqlQuote single-quotes a string for interpolation into DuckDB SQL, escaping
// embedded quotes so paths containing ' cannot break the statement.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// sqlIdent double-quotes a column name for interpolation into DuckDB SQL.
// Column names come from Go field names or column: tag overrides, so a
// reserved word ("order", "from", "group") or an exotic character would
// otherwise break the generated statement.
func sqlIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func buildFileList(files []string) string {
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = sqlQuote(f)
	}
	return strings.Join(quoted, ", ")
}

// staleIncTmpAge is how old an inc-side .parquet.tmp must be before merge
// garbage-collects it as a crash leftover. A live ImportWriter finishes a
// staged file in well under this, so only files whose writer died are removed.
const staleIncTmpAge = time.Hour

// cleanIncLeftovers removes crash leftovers under the inc tree: staged
// .parquet.tmp files old enough that no live writer can still own them
// (main-side temp files are GC'd by finalizeDir; nothing else covers inc),
// then any directories left empty.
func cleanIncLeftovers(dir string) {
	cutoff := time.Now().Add(-staleIncTmpAge)
	filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() &&
			strings.HasSuffix(info.Name(), ".parquet.tmp") && info.ModTime().Before(cutoff) {
			os.Remove(p)
		}
		return nil
	})
	cleanEmptyDirs(dir)
}

// stagingDirName is the directory under the data root where upsert stages its
// transient parquet inputs, outside inc/ and main/.
const stagingDirName = ".staging"

// staleStagingAge is how old an upsert staging directory must be before it is
// garbage-collected as a crash leftover. Age is only the cross-process
// heuristic: staging that belongs to a live upsert of THIS handle is skipped
// via db.liveStaging regardless of age (a compaction blocked on a partition
// lock can legitimately outlive any threshold), and a crashed process's
// staging is by definition in no registry. Nothing else covers these
// directories: they sit outside inc/ and main/, so neither cleanIncLeftovers
// nor finalizeDir ever sees them.
const staleStagingAge = 24 * time.Hour

// cleanStaleStaging removes upsert staging directories abandoned by a crash.
// Directories registered by an in-flight upsert on this handle are never
// touched; for anything else the age threshold applies (other live processes
// are out of scope — Fold documents a single writer per table).
func (db *DB) cleanStaleStaging() {
	root := filepath.Join(db.dir, stagingDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-staleStagingAge)
	for _, e := range entries {
		p := filepath.Join(root, e.Name())
		if _, live := db.liveStaging.Load(p); live {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		os.RemoveAll(p)
	}
}

func cleanEmptyDirs(dir string) {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			sub := filepath.Join(dir, e.Name())
			cleanEmptyDirs(sub)
			os.Remove(sub) // Ignore failures for non-empty directories.
		}
	}
}
