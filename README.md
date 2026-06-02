# Fold

Fold is a Go library for incrementally merging structured data into partitioned
Parquet datasets with DuckDB.

It is designed for data-lake style master-data pipelines: import many batches
from many sources, keep incremental files in `inc/`, and compact them into
stable `main/` Parquet files using merge strategies declared on Go struct tags.

## Status

Fold runs on the local filesystem with crash-safe, retry-idempotent,
manifest-backed merges, streaming import, and configurable resource bounds. An
object-storage adapter (and direct DuckDB `s3://` / `httpfs` I/O), LSM-style
levels, and a scheduler are deliberately deferred — the `Storage` seam is in
place so a remote backend can be added later without changing the merge model.

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

## How it works

Fold is an append-and-compact engine:

1. **Append.** `Import` / `ImportRows` write immutable Parquet into `inc/`; they
   never merge inline.
2. **Compact.** `Merge` reads a partition's active files plus its pending `inc/`
   files, applies the declared strategies in DuckDB, and publishes one new
   segment under `files/`.
3. **Publish.** Each partition's `_manifest.json` lists its active files, and a
   `_commit_<tx>.json` records the inputs consumed and outputs produced. Writing
   the manifest is the single commit point.

The resulting guarantees:

- **Crash-safe.** Output is staged and validated before the manifest references
  it. A crash before the commit leaves the previous state authoritative; the
  orphaned output is garbage-collected on the next run.
- **Retry-idempotent.** Consumed `inc/` inputs are recorded and removed before
  the next publish, so aggregates such as `sum` are never double-applied.
- **Simple reads.** Active files are primary-key-disjoint, so a read is a plain
  `read_parquet([active_files])` — merge strategies run only at compaction, never
  at read time. (Derive partition keys from the immutable primary key, as in the
  examples, so a key never moves partitions; partitioning by a mutable field can
  leave the same key active in two partitions, which a cross-partition read would
  then need to dedup.)
- **Bounded.** DuckDB memory and threads, merge workers, import buffering, and
  the bloom rewrite are all capped (see Tuning), so large workloads stay within
  memory.

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

Bloom filters only accelerate primary-key lookups; they are never required for
correctness. The post-merge rewrite is skipped automatically for outputs larger
than `BloomMaxFileBytes` (default 256 MiB) to bound its memory, and can be
turned off entirely with `DisableBloom`.

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

## Storage

Manifest metadata and published files go through a narrow `Storage` interface
(JSON read/write, file upload/download, list, delete); bulk Parquet is read and
written by DuckDB in a local workspace. The default is the local filesystem, and
an adapter can target another backend by staging locally then publishing:

```go
db, _ := fold.Open("./data", fold.WithStorage(myStorage))
```

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
