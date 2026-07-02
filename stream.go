package fold

import (
	"fmt"
	"iter"
	"path/filepath"
	"sort"
)

const (
	defaultMaxRowsPerFile          = 100_000
	defaultMaxOpenPartitionWriters = 256
)

// ImportOptions bounds the streaming import writer. Zero values are replaced
// with conservative defaults.
type ImportOptions struct {
	MaxRowsPerFile          int   // flush a partition's buffer at this many rows (default 100000)
	MaxBytesPerFile         int64 // also flush when buffered bytes (estimated) reach this (0 = disabled)
	MaxOpenPartitionWriters int   // cap buffered partitions; the largest is flushed when exceeded (default 256)
}

func (o ImportOptions) normalized() ImportOptions {
	if o.MaxRowsPerFile <= 0 {
		o.MaxRowsPerFile = defaultMaxRowsPerFile
	}
	if o.MaxOpenPartitionWriters <= 0 {
		o.MaxOpenPartitionWriters = defaultMaxOpenPartitionWriters
	}
	return o
}

// ImportWriter appends typed records into inc/ as immutable Parquet files,
// flushing per partition by row count (or estimated bytes) so a large import
// never holds the whole batch in memory. It does not merge. It is not safe for
// concurrent use; call Close to flush the remaining buffers.
type ImportWriter[T any] struct {
	table  *Table[T]
	source string
	opts   ImportOptions
	parts  map[string]*partitionBuffer
	err    error
}

type partitionBuffer struct {
	rows  []map[string]any
	bytes int64
}

// NewImportWriter creates a streaming import writer for a source. The source
// becomes one directory level under inc/<table>/, so it is percent-encoded
// like a partition value: a separator in it ("2026/07/02", "../x") would
// otherwise write outside the source level — escaping the data root or
// stranding the batch where merge never collects it. An empty source is
// replaced with "default" for the same reason.
func (t *Table[T]) NewImportWriter(source string, opts ImportOptions) *ImportWriter[T] {
	source = encodePartitionValue(source)
	switch source {
	case "":
		source = "default"
	case ".":
		// filepath.Join elides "." (dropping the source level merge expects)
		// and collapses ".." (escaping inc/<table>), so encode bare dot
		// segments the same way separators are encoded.
		source = "%2E"
	case "..":
		source = "%2E%2E"
	}
	return &ImportWriter[T]{
		table:  t,
		source: source,
		opts:   opts.normalized(),
		parts:  make(map[string]*partitionBuffer),
	}
}

// Add buffers one record, flushing its partition once a size threshold is hit.
// A record missing a primary key is skipped, matching Import.
func (w *ImportWriter[T]) Add(rec T) error {
	if w.err != nil {
		return w.err
	}
	schema := w.table.schema
	row, ok, err := extractRow(schema, rec)
	if err != nil {
		w.err = err
		return err
	}
	if !ok {
		return nil
	}

	partDir := partitionDirFor(row, schema)
	buf := w.parts[partDir]
	if buf == nil {
		// Bound the number of buffered partitions: flush the largest before
		// opening a new one so memory stays bounded for wide key spaces.
		if len(w.parts) >= w.opts.MaxOpenPartitionWriters {
			if err := w.flushLargest(); err != nil {
				return err
			}
		}
		buf = &partitionBuffer{}
		w.parts[partDir] = buf
	}

	buf.rows = append(buf.rows, row)
	if w.opts.MaxBytesPerFile > 0 {
		buf.bytes += estimateRowBytes(row)
	}
	if len(buf.rows) >= w.opts.MaxRowsPerFile ||
		(w.opts.MaxBytesPerFile > 0 && buf.bytes >= w.opts.MaxBytesPerFile) {
		return w.flush(partDir)
	}
	return nil
}

// Close flushes all remaining buffered partitions.
func (w *ImportWriter[T]) Close() error {
	if w.err != nil {
		return w.err
	}
	for partDir := range w.parts {
		if err := w.flush(partDir); err != nil {
			return err
		}
	}
	return nil
}

// flush writes one partition's buffered rows to a new inc Parquet file.
func (w *ImportWriter[T]) flush(partDir string) error {
	buf := w.parts[partDir]
	if buf == nil || len(buf.rows) == 0 {
		delete(w.parts, partDir)
		return nil
	}

	pkCols := w.table.schema.PKColumns()
	sort.Slice(buf.rows, func(i, j int) bool {
		for _, col := range pkCols {
			a, _ := buf.rows[i][col].(string)
			b, _ := buf.rows[j][col].(string)
			if a != b {
				return a < b
			}
		}
		return false
	})

	outDir := filepath.Join(w.table.incDir(), w.source, partDir)
	if err := writeParquet(outDir, w.table.schema, buf.rows); err != nil {
		w.err = fmt.Errorf("fold: write partition %q: %w", partDir, err)
		return w.err
	}
	delete(w.parts, partDir)
	return nil
}

// flushLargest flushes the buffered partition holding the most rows.
func (w *ImportWriter[T]) flushLargest() error {
	largest, most := "", -1
	for partDir, buf := range w.parts {
		if len(buf.rows) > most {
			most = len(buf.rows)
			largest = partDir
		}
	}
	if most < 0 {
		return nil
	}
	return w.flush(largest)
}

// ImportRows streams records from an iterator into inc/, bounded by opts. It is
// the streaming counterpart to Import; the current Import remains a convenience
// wrapper for small in-memory batches.
func (t *Table[T]) ImportRows(source string, rows iter.Seq[T], opts ImportOptions) error {
	w := t.NewImportWriter(source, opts)
	for rec := range rows {
		if err := w.Add(rec); err != nil {
			return err
		}
	}
	return w.Close()
}

// estimateRowBytes is a rough uncompressed-size estimate for MaxBytesPerFile.
func estimateRowBytes(row map[string]any) int64 {
	var n int64
	for _, v := range row {
		switch x := v.(type) {
		case string:
			n += int64(len(x))
		case []string:
			for _, s := range x {
				n += int64(len(s))
			}
		default:
			n += 8
		}
	}
	return n
}
