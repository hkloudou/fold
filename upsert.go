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

	"github.com/google/uuid"
)

// Upsert merges RawRecord rows directly into main/.
// Internally it writes temporary Parquet, merges with DuckDB, writes main, and cleans up.
//
// The source parameter is currently unused: unlike Import, an upsert goes
// straight to main/ and leaves no per-source trace. It is kept so call sites
// stay symmetric with Import and a future audit trail can use it without an
// API break.
func (t *Table[T]) Upsert(source string, records []RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	schema := t.schema
	rows := convertRawRecords(schema, records)
	if len(rows) == 0 {
		return nil
	}

	if err := mkdirAllDurable(t.mainDir(), t.db.dir); err != nil {
		return err
	}

	t.db.cleanStaleStaging()

	if len(schema.Partitions) > 0 {
		return t.upsertPartitioned(rows)
	}

	if err := t.upsertRows(t.mainDir(), rows); err != nil {
		return fmt.Errorf("fold: upsert: %w", err)
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

	parts := make([]string, 0, len(groups))
	for p := range groups {
		parts = append(parts, p)
	}
	sort.Strings(parts)

	errs := runPartitionJobs(schema.Name, t.db.compact.Workers, parts, func(partDir string) error {
		mainPartDir := filepath.Join(t.mainDir(), partDir)
		if err := mkdirAllDurable(mainPartDir, t.db.dir); err != nil {
			return err
		}
		return t.upsertRows(mainPartDir, groups[partDir])
	})

	if len(errs) > 0 {
		return fmt.Errorf("some partitions failed during upsert: %s", strings.Join(errs, "; "))
	}

	log.Printf("[Fold] %s: %d partitions upserted", schema.Name, len(groups))
	return nil
}

// upsertRows stages rows as transient parquet OUTSIDE the partition directory
// (so a recovery finalize triggered inside compactPartition can never mistake
// them for publish outputs) and compacts them into dir. The staging files are
// not recorded as consumed inputs — they are removed here, crash or not, and
// a crashed upsert is simply retried by the caller.
func (t *Table[T]) upsertRows(dir string, rows []map[string]any) error {
	staging := filepath.Join(t.db.dir, stagingDirName, "upsert_"+uuid.New().String())
	// Register before the directory exists so the stale-staging sweep can
	// never observe it unregistered: age alone must not decide liveness,
	// because a compaction blocked on a partition lock can legitimately
	// outlive any threshold. Deregistration is the last deferred step, after
	// the directory is already removed.
	t.db.liveStaging.Store(staging, struct{}{})
	defer t.db.liveStaging.Delete(staging)
	if err := writeParquet(staging, t.schema, rows); err != nil {
		os.RemoveAll(staging)
		return fmt.Errorf("write staging: %w", err)
	}
	defer os.RemoveAll(staging)

	stagingFiles := listParquetFiles(staging)
	if len(stagingFiles) == 0 {
		return nil
	}
	return t.compactPartition(dir, func() ([]string, error) { return stagingFiles, nil }, false)
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

			// Zero values are treated as absent, matching Import (extractRow
			// skips IsZero fields): an empty string must not overwrite or
			// coalesce over existing data.
			switch f.Type {
			case FieldString:
				if f.Strategy == StrategyJSONMerge {
					if s := JSON(v); s != "" {
						row[f.Column] = s
					}
				} else if s := coerceString(v); s != "" {
					row[f.Column] = s
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
		result := make([]string, 0, len(s))
		for _, item := range s {
			result = append(result, coerceString(item))
		}
		return result
	default:
		return nil
	}
}
