package fold

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

const mergeWorkers = 10

// Merge performs the CRDT-style merge from inc/ into main/.
func (t *Table[T]) Merge() error {
	schema := t.schema
	incDir := t.incDir()
	mainDir := t.mainDir()

	if err := os.MkdirAll(mainDir, 0755); err != nil {
		return err
	}

	incFiles := listParquetFiles(incDir)
	if len(incFiles) == 0 {
		return nil
	}

	mainFiles := listParquetFiles(mainDir)

	if len(schema.Partitions) > 0 {
		return t.mergePartitioned(incFiles)
	}
	return t.mergeFull(incFiles, mainFiles)
}

// mergePartitioned merges partitions concurrently.
func (t *Table[T]) mergePartitioned(incFiles []string) error {
	schema := t.schema
	partDirs := discoverPartitions(t.incDir(), len(schema.Partitions))
	if len(partDirs) == 0 {
		return nil
	}

	log.Printf("[Fold] %s: found %d partitions to merge", schema.Name, len(partDirs))

	jobs := make(chan string, len(partDirs))
	for _, pd := range partDirs {
		jobs <- pd
	}
	close(jobs)

	results := make(chan mergeResult, len(partDirs))
	var wg sync.WaitGroup
	var mergedCount int64

	workers := mergeWorkers
	if workers > len(partDirs) {
		workers = len(partDirs)
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partDir := range jobs {
				err := t.mergeOnePartition(partDir)
				if err == nil {
					atomic.AddInt64(&mergedCount, 1)
				}
				results <- mergeResult{partDir: partDir, err: err}
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
			log.Printf("[Fold] %s partition %s failed: %v", schema.Name, r.partDir, r.err)
			errs = append(errs, fmt.Sprintf("%s: %v", r.partDir, r.err))
		}
	}

	cleanEmptyDirs(t.incDir())

	if len(errs) > 0 {
		return fmt.Errorf("some partitions failed: %s", strings.Join(errs, "; "))
	}

	log.Printf("[Fold] %s: %d partitions merged", schema.Name, mergedCount)
	return nil
}

type mergeResult struct {
	partDir string
	err     error
}

// mergeOnePartition merges a single partition.
func (t *Table[T]) mergeOnePartition(partDir string) error {
	schema := t.schema
	pkCols := schema.PKColumns()

	// Collect inc files for this partition from all sources.
	var incPartFiles []string
	sources, _ := os.ReadDir(t.incDir())
	for _, src := range sources {
		if !src.IsDir() {
			continue
		}
		srcPartDir := filepath.Join(t.incDir(), src.Name(), partDir)
		incPartFiles = append(incPartFiles, listParquetFiles(srcPartDir)...)
	}
	if len(incPartFiles) == 0 {
		return nil
	}

	mainPartDir := filepath.Join(t.mainDir(), partDir)
	mainPartFiles := listParquetFiles(mainPartDir)
	os.MkdirAll(mainPartDir, 0755)

	db, cleanup, err := openDuckDB(mainPartDir)
	if err != nil {
		return err
	}
	defer cleanup()

	// Read inc data.
	incGlob := buildFileList(incPartFiles)
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE inc_data AS SELECT * FROM read_parquet([%s], union_by_name=true)`, incGlob)); err != nil {
		return fmt.Errorf("read inc: %w", err)
	}

	// Detect actual columns.
	incCols := queryTableColumns(db, "inc_data")
	activeFields := filterActiveFields(schema.Fields, incCols)

	// Pre-merge inc rows.
	if err := buildIncMerged(db, pkCols, activeFields); err != nil {
		return err
	}

	if len(mainPartFiles) > 0 {
		incMergedCols := queryTableColumns(db, "inc_merged")
		mainGlob := buildFileList(mainPartFiles)
		if _, err := db.Exec(fmt.Sprintf(`CREATE VIEW main_view AS SELECT * FROM read_parquet([%s], union_by_name=true)`, mainGlob)); err != nil {
			return fmt.Errorf("read main: %w", err)
		}
		mainCols := queryTableColumns(db, "main_view")

		if err := buildMerged(db, schema, pkCols, incMergedCols, mainCols); err != nil {
			return err
		}
	} else {
		if _, err := db.Exec(`CREATE TABLE result AS SELECT * FROM inc_merged`); err != nil {
			return fmt.Errorf("create result table: %w", err)
		}
	}

	// Delete old main files.
	for _, f := range mainPartFiles {
		os.Remove(f)
	}

	// Write the result.
	ts := time.Now().UnixMilli()
	outFile := filepath.Join(mainPartDir, fmt.Sprintf("merged_%d.parquet", ts))
	if _, err := db.Exec(fmt.Sprintf(`COPY result TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)`, outFile)); err != nil {
		return fmt.Errorf("write result: %w", err)
	}

	// Add bloom filters for bloom-enabled columns.
	if err := addBloomFilters(outFile, schema); err != nil {
		return fmt.Errorf("add bloom filters: %w", err)
	}

	// Delete inc files.
	for _, f := range incPartFiles {
		os.Remove(f)
	}

	return nil
}

// mergeFull merges an unpartitioned table.
func (t *Table[T]) mergeFull(incFiles, mainFiles []string) error {
	schema := t.schema
	pkCols := schema.PKColumns()

	log.Printf("[Fold] %s: full merge inc=%d main=%d", schema.Name, len(incFiles), len(mainFiles))

	db, cleanup, err := openDuckDB(t.mainDir())
	if err != nil {
		return err
	}
	defer cleanup()

	// Read inc data.
	incGlob := buildFileList(incFiles)
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE inc_data AS SELECT * FROM read_parquet([%s], union_by_name=true)`, incGlob)); err != nil {
		return fmt.Errorf("read inc: %w", err)
	}

	incCols := queryTableColumns(db, "inc_data")
	activeFields := filterActiveFields(schema.Fields, incCols)

	if err := buildIncMerged(db, pkCols, activeFields); err != nil {
		return err
	}

	if len(mainFiles) > 0 {
		incMergedCols := queryTableColumns(db, "inc_merged")
		mainGlob := buildFileList(mainFiles)
		if _, err := db.Exec(fmt.Sprintf(`CREATE VIEW main_view AS SELECT * FROM read_parquet([%s], union_by_name=true)`, mainGlob)); err != nil {
			return fmt.Errorf("read main: %w", err)
		}
		mainCols := queryTableColumns(db, "main_view")

		if err := buildMerged(db, schema, pkCols, incMergedCols, mainCols); err != nil {
			return err
		}
	} else {
		if _, err := db.Exec(`CREATE TABLE result AS SELECT * FROM inc_merged`); err != nil {
			return fmt.Errorf("create result table: %w", err)
		}
	}

	// Atomic write.
	ts := time.Now().UnixMilli()
	tmpFile := filepath.Join(t.mainDir(), fmt.Sprintf(".merged_%d.parquet.tmp", ts))
	finalFile := filepath.Join(t.mainDir(), fmt.Sprintf("merged_%d.parquet", ts))

	if _, err := db.Exec(fmt.Sprintf(`COPY result TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)`, tmpFile)); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("write result: %w", err)
	}

	if err := os.Rename(tmpFile, finalFile); err != nil {
		os.Remove(tmpFile)
		return err
	}

	// Add bloom filters for bloom-enabled columns.
	if err := addBloomFilters(finalFile, schema); err != nil {
		return fmt.Errorf("add bloom filters: %w", err)
	}

	// Clean up.
	for _, f := range mainFiles {
		if f != finalFile {
			os.Remove(f)
		}
	}
	for _, f := range incFiles {
		os.Remove(f)
	}

	log.Printf("[Fold] %s: merge complete", schema.Name)
	return nil
}

// buildIncMerged creates the inc pre-merge table: GROUP BY pk(s).
func buildIncMerged(db *sql.DB, pkCols []string, fields []Field) error {
	var aggExprs []string
	for _, f := range fields {
		if f.Strategy == StrategyPK {
			continue
		}
		aggExprs = append(aggExprs, f.GetAggExpr())
	}

	aggStr := ""
	if len(aggExprs) > 0 {
		aggStr = ",\n  " + strings.Join(aggExprs, ",\n  ")
	}

	groupBy := strings.Join(pkCols, ", ")
	sql := fmt.Sprintf(`CREATE TABLE inc_merged AS
SELECT %s%s
FROM inc_data
GROUP BY %s`, groupBy, aggStr, groupBy)

	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("inc pre-merge failed: %w", err)
	}
	return nil
}

// buildMerged creates the FULL OUTER JOIN merge table and writes it to result.
// json_merge is handled directly through DuckDB json_merge_patch SQL.
func buildMerged(db *sql.DB, schema *Schema, pkCols []string, incCols, mainCols map[string]bool) error {
	var selectExprs []string

	for _, f := range schema.Fields {
		inInc := incCols[f.Column]
		inMain := mainCols[f.Column]
		if !inInc && !inMain {
			continue
		}

		if inInc && inMain {
			selectExprs = append(selectExprs, f.GetSQLExpr()+" AS "+f.Column)
		} else if inInc {
			selectExprs = append(selectExprs, "s."+f.Column)
		} else {
			selectExprs = append(selectExprs, "t."+f.Column)
		}
	}

	var joinConds []string
	for _, col := range pkCols {
		joinConds = append(joinConds, fmt.Sprintf("s.%s = t.%s", col, col))
	}

	mergeSQL := fmt.Sprintf(`CREATE TABLE result AS
SELECT %s
FROM inc_merged s
FULL OUTER JOIN main_view t ON %s`,
		strings.Join(selectExprs, ", "), strings.Join(joinConds, " AND "))

	if _, err := db.Exec(mergeSQL); err != nil {
		return fmt.Errorf("merge execution failed: %w", err)
	}
	return nil
}

// --- DuckDB helpers ---

func openDuckDB(dir string) (db *sql.DB, cleanup func(), err error) {
	dbPath := filepath.Join(dir, fmt.Sprintf(".duckdb_%d.db", time.Now().UnixNano()))
	db, err = sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, nil, err
	}
	db.Exec("PRAGMA memory_limit='2GB'")
	db.Exec("PRAGMA threads=4")

	cleanup = func() {
		db.Close()
		os.Remove(dbPath)
		os.Remove(dbPath + ".wal")
	}
	return db, cleanup, nil
}

func queryTableColumns(db *sql.DB, tableName string) map[string]bool {
	cols := make(map[string]bool)
	rows, err := db.Query(fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tableName))
	if err != nil {
		return cols
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil {
			cols[name] = true
		}
	}
	return cols
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

func buildFileList(files []string) string {
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = "'" + f + "'"
	}
	return strings.Join(quoted, ", ")
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
