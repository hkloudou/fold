# Fold

Fold is a Go library for incrementally merging structured data into partitioned
Parquet datasets with DuckDB.

It is designed for data-lake style master-data pipelines: import many batches
from many sources, keep incremental files in `inc/`, and compact them into
stable `main/` Parquet files using merge strategies declared on Go struct tags.

## Status

This is an early foundation release. The current implementation targets local
filesystem workflows and batch-oriented merges. Object storage manifests,
bounded-memory compaction, and production scheduling are expected future layers.

## Install

```bash
go get github.com/hkloudou/fold
```

## Quick Start

```go
package main

import (
	"time"

	"github.com/hkloudou/fold"
)

type Company struct {
	ID          string   `bd:"pk;bloom;partition:b=hash(256)"`
	Name        string
	Phones      []string
	Source      string `bd:"json_merge"`
	FirstSeen   int64  `bd:"min"`
	LastSeen    int64  `bd:"max"`
	ImportCount int64  `bd:"sum"`
	UpdatedAt   int64  `bd:"overwrite"`
}

func main() {
	db, err := fold.Open("./data")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	table := fold.Register[Company](db)
	now := time.Now().Unix()

	err = table.Import("source-a", []Company{
		{
			ID:          "company-001",
			Name:        "Acme Inc",
			Phones:      []string{"100", "200"},
			Source:      fold.JSON(map[string]any{"source-a": map[string]any{"seen": now}}),
			FirstSeen:   now,
			LastSeen:    now,
			ImportCount: 1,
			UpdatedAt:   now,
		},
	})
	if err != nil {
		panic(err)
	}

	if err := table.Merge(); err != nil {
		panic(err)
	}
}
```

## Merge Strategies

Declare strategies with the `bd` struct tag.

| Tag | Meaning |
| --- | --- |
| `pk` | Primary key. Multiple fields create a composite key. |
| `bloom` | Enable Parquet bloom filters for the column. |
| `partition:p=[0:2]` | Partition by substring slice. |
| `partition:b=hash(256)` | Partition by hash bucket. |
| `column:name` | Override the default snake_case column name. |
| `-` | Skip the field. |
| `coalesce` | Use the incoming non-null value, otherwise keep existing value. |
| `overwrite` | Same merge behavior as coalesce; inc pre-merge uses `MAX`. |
| `list_union` | Merge `[]string` values as a distinct set. Untagged `[]string` defaults to this strategy. |
| `json_merge` | Merge JSON strings with RFC 7396 JSON Merge Patch semantics. |
| `max` | Keep the greatest value. |
| `min` | Keep the smallest value. |
| `sum` | Add values together. |
| `expr:SQL;agg:SQL` | Provide custom DuckDB SQL for merge and inc pre-aggregation. |

### json_merge conflict contract

`json_merge` follows RFC 7396 JSON Merge Patch. Non-conflicting keys are always
merged. When the same key is set to different values, the winner depends on
where the patches sit relative to a merge:

- **Across merge cycles**, a later `Import` + `Merge` wins (last-write-wins).
  This is the supported way to express precedence.
- **Within a single batch**, patches are folded in ascending JSON-text order, so
  the lexicographically-greatest patch wins the key. Fold keeps no per-row
  sequence inside a batch, so this is a stable tie-break, not a temporal one.
  Split patches across merge cycles when order matters.

## Layout

Fold writes a simple two-area layout:

```text
data/
  inc/
    table_name/
      source/
        partition_key=value/
          batch.parquet
  main/
    table_name/
      partition_key=value/
        _manifest.json           # active file set for this partition
        _commit_<tx>.json        # inputs consumed / outputs produced by the last commit
        files/
          merged_<ts>.parquet    # active data
```

`Import` appends typed records into `inc/`. `Merge` compacts pending `inc/`
files into `main/` by partition, publishing each result by atomically swapping
the partition's `_manifest.json`; reads use the manifest's active file list
rather than a directory glob, so merges are crash-safe and retry-idempotent.
`Upsert` is available for small direct updates, but large pipelines should
prefer append-then-merge flows.

## Tuning

Merge and upsert run DuckDB per partition with conservative defaults. Override
them at `Open`:

```go
db, _ := fold.Open("./data", fold.WithCompactOptions(fold.CompactOptions{
	Workers: 4,
	DuckDB:  fold.DuckDBOptions{MemoryLimit: "4GB", Threads: 8, TempDir: "/tmp/fold"},
}))
```

Unset fields fall back to defaults: 10 workers, `2GB` memory, 4 threads, and no
explicit temp directory.

## Streaming import

`Import` materializes its slice. For large flows use the streaming writer, which
appends inc files bounded by row count (or estimated bytes) without holding the
whole batch in memory:

```go
w := table.NewImportWriter("source", fold.ImportOptions{MaxRowsPerFile: 50_000})
for scanner.Scan() {
	if err := w.Add(parse(scanner.Text())); err != nil {
		return err
	}
}
if err := w.Close(); err != nil { // flushes remaining buffers
	return err
}
```

Or stream from an iterator with `table.ImportRows("source", seq, fold.ImportOptions{})`.
Streaming import only appends to `inc/`; call `Merge` afterwards to compact.

## Readers

Fold includes lightweight helpers for Excel and JSONL ingestion.

```go
records, err := fold.ReadExcel("input.xlsx", &fold.ExcelOpt{
	Header: 1,
	Fields: map[string]string{
		"id":     "ID",
		"name":   "Name",
		"phones": "Phone A & Phone B",
	},
	Split: map[string]string{"phones": ","},
})
```

`&` merges multiple columns or JSON paths into a string list. `|` selects the
first non-empty value.

## Requirements

- Go 1.24 or newer
- DuckDB Go bindings through `github.com/marcboeker/go-duckdb/v2`
- Apache Arrow Go for Parquet writing

## License

No license has been declared yet.
