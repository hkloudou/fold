package fold

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Upsert merges RawRecord rows directly into main/.
// Internally it writes temporary Parquet, merges with DuckDB, writes main, and cleans up.
func (t *Table[T]) Upsert(source string, records []RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	schema := t.schema
	rows := convertRawRecords(schema, records)
	if len(rows) == 0 {
		return nil
	}

	if err := os.MkdirAll(t.mainDir(), 0755); err != nil {
		return err
	}

	if len(schema.Partitions) > 0 {
		return t.upsertPartitioned(rows)
	}
	return t.upsertFull(rows)
}

// upsertFull merges an unpartitioned table directly.
func (t *Table[T]) upsertFull(rows []map[string]any) error {
	schema := t.schema
	pkCols := schema.PKColumns()
	mainDir := t.mainDir()

	// Recover any inc consumed by a prior crashed merge before advancing the
	// commit record, so its consumed-inc state is never stranded.
	if err := recoverPartition(mainDir); err != nil {
		return err
	}

	// Active main files come from the manifest, consistent with Merge.
	mainFiles, err := activeMainFiles(mainDir)
	if err != nil {
		return err
	}

	// Write temporary Parquet.
	tmpDir := filepath.Join(mainDir, fmt.Sprintf(".upsert_%d", time.Now().UnixNano()))
	if err := writeParquet(tmpDir, schema, rows); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("fold: upsert write temporary file: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	tmpFiles := listParquetFiles(tmpDir)
	if len(tmpFiles) == 0 {
		return nil
	}

	db, cleanup, err := openDuckDB(mainDir, t.db.compact.DuckDB)
	if err != nil {
		return err
	}
	defer cleanup()

	// Read temporary data.
	tmpGlob := buildFileList(tmpFiles)
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE inc_data AS SELECT * FROM read_parquet([%s], union_by_name=true)`, tmpGlob)); err != nil {
		return fmt.Errorf("fold: upsert read temporary data: %w", err)
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
			return fmt.Errorf("fold: upsert read main: %w", err)
		}
		mainCols := queryTableColumns(db, "main_view")

		if err := buildMerged(db, schema, pkCols, incMergedCols, mainCols); err != nil {
			return err
		}
	} else {
		if _, err := db.Exec(`CREATE TABLE result AS SELECT * FROM inc_merged`); err != nil {
			return fmt.Errorf("fold: upsert create result table: %w", err)
		}
	}

	if err := t.publishMerged(db, mainDir, nil); err != nil {
		return fmt.Errorf("fold: upsert publish: %w", err)
	}

	log.Printf("[Fold] %s: upsert complete (%d records)", schema.Name, len(rows))
	return nil
}

// upsertPartitioned merges a partitioned table directly.
func (t *Table[T]) upsertPartitioned(rows []map[string]any) error {
	schema := t.schema
	groups := groupByPartitions(rows, schema)

	if len(groups) == 0 {
		return nil
	}

	log.Printf("[Fold] %s: upsert %d partitions", schema.Name, len(groups))

	type partJob struct {
		partDir string
		rows    []map[string]any
	}

	jobs := make(chan partJob, len(groups))
	for partDir, partRows := range groups {
		jobs <- partJob{partDir: partDir, rows: partRows}
	}
	close(jobs)

	results := make(chan mergeResult, len(groups))
	var wg sync.WaitGroup
	var upsertedCount int64

	workers := t.db.compact.Workers
	if workers > len(groups) {
		workers = len(groups)
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := t.upsertOnePartition(job.partDir, job.rows)
				if err == nil {
					atomic.AddInt64(&upsertedCount, 1)
				}
				results <- mergeResult{partDir: job.partDir, err: err}
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
			log.Printf("[Fold] %s upsert partition %s failed: %v", schema.Name, r.partDir, r.err)
			errs = append(errs, fmt.Sprintf("%s: %v", r.partDir, r.err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("some partitions failed during upsert: %s", strings.Join(errs, "; "))
	}

	log.Printf("[Fold] %s: %d partitions upserted", schema.Name, upsertedCount)
	return nil
}

// upsertOnePartition merges a single partition.
func (t *Table[T]) upsertOnePartition(partDir string, rows []map[string]any) error {
	schema := t.schema
	pkCols := schema.PKColumns()

	mainPartDir := filepath.Join(t.mainDir(), partDir)
	os.MkdirAll(mainPartDir, 0755)

	// Recover any inc consumed by a prior crashed merge before advancing the
	// commit record, so its consumed-inc state is never stranded.
	if err := recoverPartition(mainPartDir); err != nil {
		return err
	}

	mainPartFiles, err := activeMainFiles(mainPartDir)
	if err != nil {
		return err
	}

	// Write temporary Parquet.
	tmpDir := filepath.Join(mainPartDir, fmt.Sprintf(".upsert_%d", time.Now().UnixNano()))
	if err := writeParquet(tmpDir, schema, rows); err != nil {
		os.RemoveAll(tmpDir)
		return fmt.Errorf("write temporary file: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	tmpFiles := listParquetFiles(tmpDir)
	if len(tmpFiles) == 0 {
		return nil
	}

	db, cleanup, err := openDuckDB(mainPartDir, t.db.compact.DuckDB)
	if err != nil {
		return err
	}
	defer cleanup()

	tmpGlob := buildFileList(tmpFiles)
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE inc_data AS SELECT * FROM read_parquet([%s], union_by_name=true)`, tmpGlob)); err != nil {
		return fmt.Errorf("read temporary data: %w", err)
	}

	incCols := queryTableColumns(db, "inc_data")
	activeFields := filterActiveFields(schema.Fields, incCols)

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

	return t.publishMerged(db, mainPartDir, nil)
}

// convertRawRecords converts RawRecord rows into the map form used by writeParquet.
func convertRawRecords(schema *Schema, records []RawRecord) []map[string]any {
	pkCols := schema.PKColumns()
	rows := make([]map[string]any, 0, len(records))

	for _, rec := range records {
		row := make(map[string]any, len(schema.Fields))
		for _, f := range schema.Fields {
			v, ok := rec[f.Column]
			if !ok || v == nil {
				continue
			}

			switch f.Type {
			case FieldString:
				if f.Strategy == StrategyJSONMerge {
					row[f.Column] = coerceJSON(v)
				} else {
					row[f.Column] = coerceString(v)
				}
			case FieldInt64:
				if n := coerceInt64(v); n != 0 {
					row[f.Column] = n
				}
			case FieldList:
				if list := coerceStringList(v); len(list) > 0 {
					row[f.Column] = list
				}
			}
		}

		// All primary-key columns must exist.
		allPKs := true
		for _, col := range pkCols {
			if _, ok := row[col]; !ok {
				allPKs = false
				break
			}
		}
		if !allPKs {
			continue
		}
		rows = append(rows, row)
	}

	// Sort by composite primary key.
	sort.Slice(rows, func(i, j int) bool {
		for _, col := range pkCols {
			a, _ := rows[i][col].(string)
			b, _ := rows[j][col].(string)
			if a != b {
				return a < b
			}
		}
		return false
	})

	return rows
}

// --- Type conversion helpers ---

func coerceString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	default:
		return fmt.Sprintf("%v", v)
	}
}

func coerceInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	case string:
		i, _ := strconv.ParseInt(n, 10, 64)
		return i
	default:
		return 0
	}
}

func coerceStringList(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case string:
		if s == "" {
			return nil
		}
		return []string{s}
	case []any:
		var result []string
		for _, item := range s {
			result = append(result, fmt.Sprintf("%v", item))
		}
		return result
	default:
		return nil
	}
}

func coerceJSON(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
