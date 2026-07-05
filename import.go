package fold

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
)

// Import writes typed records into the inc/ area. It is a convenience wrapper
// over the streaming writer for small batches; large flows should use
// ImportRows or NewImportWriter so the whole batch need not be materialized.
func (t *Table[T]) Import(source string, records []T) error {
	w := t.NewImportWriter(source, ImportOptions{})
	for i := range records {
		if err := w.Add(records[i]); err != nil {
			return err
		}
	}
	return w.Close()
}

// extractRow converts one record into a map[column]any by reflection. It
// returns ok=false when the record is missing a primary-key column (and is
// therefore skipped, matching Import's behavior).
func extractRow(schema *Schema, rec any) (map[string]any, bool, error) {
	rv := reflect.ValueOf(rec)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	row := make(map[string]any, len(schema.Fields))
	for _, f := range schema.Fields {
		fv := rv.Field(f.Index)
		if fv.IsZero() {
			continue // Skip zero values instead of writing them to Parquet.
		}
		if f.Strategy == StrategyJSONMerge {
			// json_merge: pass string/[]byte through; serialize other values.
			switch v := fv.Interface().(type) {
			case string:
				row[f.Column] = v
			case []byte:
				row[f.Column] = string(v)
			default:
				b, err := json.Marshal(v)
				if err != nil {
					return nil, false, fmt.Errorf("serialize json_merge field %s: %w", f.Column, err)
				}
				row[f.Column] = string(b)
			}
		} else {
			row[f.Column] = fv.Interface()
		}
	}
	// Drop rows with any missing primary-key column.
	for _, pk := range schema.PKs {
		if _, ok := row[pk.Column]; !ok {
			return nil, false, nil
		}
	}
	return row, true, nil
}

// partitionDirFor returns the partition subdirectory for a row, or "" when the
// table is unpartitioned. Values are percent-encoded so a value containing a
// path separator can neither escape the table directory nor create nesting
// that discoverPartitions would never match (stranding the data unmerged).
func partitionDirFor(row map[string]any, schema *Schema) string {
	if len(schema.Partitions) == 0 {
		return ""
	}
	parts := make([]string, 0, len(schema.Partitions))
	for _, p := range schema.Partitions {
		parts = append(parts, p.Key+"="+encodePartitionValue(evalPartition(row, p, schema)))
	}
	return filepath.Join(parts...)
}

// encodePartitionValue percent-encodes the bytes that would break the
// key=value directory scheme: path separators, '%' itself, and control
// bytes. Typical values (letters, digits, CJK, '-', '_', '=') pass through
// unchanged, so existing well-formed layouts keep their directory names.
func encodePartitionValue(s string) string {
	needsEscape := func(c byte) bool {
		return c == '%' || c == '/' || c == '\\' || c < 0x20 || c == 0x7f
	}
	clean := true
	for i := 0; i < len(s); i++ {
		if needsEscape(s[i]) {
			clean = false
			break
		}
	}
	if clean {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if c := s[i]; needsEscape(c) {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// groupByPartitions groups rows by partition rules.
func groupByPartitions(rows []map[string]any, schema *Schema) map[string][]map[string]any {
	groups := make(map[string][]map[string]any)
	for _, row := range rows {
		key := partitionDirFor(row, schema)
		groups[key] = append(groups[key], row)
	}
	return groups
}

// evalPartition evaluates one partition rule for a row.
func evalPartition(row map[string]any, p PartitionRule, schema *Schema) string {
	col := ""
	for _, f := range schema.Fields {
		if f.GoName == p.Field {
			col = f.Column
			break
		}
	}
	if col == "" {
		return ""
	}

	val := ""
	if v, ok := row[col]; ok && v != nil {
		if s, isStr := v.(string); isStr {
			val = s
		} else {
			val = fmt.Sprintf("%v", v)
		}
	}

	if p.Hash > 0 {
		h := sha256.Sum256([]byte(val))
		bucket := int(h[0])<<8 | int(h[1])
		bucket = bucket % p.Hash
		return fmt.Sprintf("%02x", bucket)
	}

	if p.Slice != [2]int{0, 0} {
		runes := []rune(val)
		start, end := p.Slice[0], p.Slice[1]
		if start >= len(runes) {
			return ""
		}
		if end > len(runes) {
			end = len(runes)
		}
		return string(runes[start:end])
	}

	return val
}

// writeParquet writes rows into a single Parquet file.
func writeParquet(dir string, schema *Schema, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Stage to a .tmp name and rename on success: a crash mid-write must not
	// leave a truncated .parquet that every later merge would read and fail
	// on. Readers skip the .parquet.tmp suffix; a crash leftover is removed by
	// merge's cleanIncLeftovers once it is stale.
	outPath := filepath.Join(dir, uuid.New().String()+".parquet")
	tmpPath := outPath + ".tmp"

	// Build the Arrow schema.
	arrowFields := make([]arrow.Field, len(schema.Fields))
	for i, f := range schema.Fields {
		arrowFields[i] = buildArrowField(f)
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)

	alloc := memory.NewGoAllocator()

	// Build Arrow builders.
	builders := make([]array.Builder, len(schema.Fields))
	for i, f := range schema.Fields {
		builders[i] = buildArrowBuilder(alloc, f)
	}

	// Append row values.
	for _, row := range rows {
		for i, f := range schema.Fields {
			val := row[f.Column]
			appendValue(builders[i], f, val)
		}
	}

	// Create Arrow arrays.
	arrays := make([]arrow.Array, len(schema.Fields))
	for i, b := range builders {
		arrays[i] = b.NewArray()
		defer arrays[i].Release()
		defer b.Release()
	}

	record := array.NewRecord(arrowSchema, arrays, int64(len(rows)))
	defer record.Release()

	// Write Parquet. A concurrent merge's cleanIncLeftovers may remove the
	// partition directory in the window between MkdirAll above and this
	// create (it only removes empty dirs), so recreate and retry on ENOENT.
	outFile, err := os.Create(tmpPath)
	for retries := 0; os.IsNotExist(err) && retries < 3; retries++ {
		if mkErr := os.MkdirAll(dir, 0755); mkErr != nil {
			return mkErr
		}
		outFile, err = os.Create(tmpPath)
	}
	if err != nil {
		return err
	}

	// Writer properties: Zstd compression and bloom filters.
	writerOpts := []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
	}
	for _, f := range schema.Fields {
		if f.Bloom {
			writerOpts = append(writerOpts, parquet.WithBloomFilterEnabledFor(f.Column, true))
		}
	}
	writerProps := parquet.NewWriterProperties(writerOpts...)
	arrowProps := pqarrow.DefaultWriterProps()

	writer, err := pqarrow.NewFileWriter(arrowSchema, outFile, writerProps, arrowProps)
	if err != nil {
		outFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := writer.Write(record); err != nil {
		writer.Close()
		os.Remove(tmpPath)
		return err
	}

	// writer.Close writes the footer and closes outFile (the parquet writer
	// closes its sink), so the staged file is closed before the rename — a
	// rename of an open file fails on Windows.
	if err := writer.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}

// buildArrowField builds an Arrow field from Fold field metadata.
func buildArrowField(f Field) arrow.Field {
	switch f.Type {
	case FieldInt64:
		return arrow.Field{Name: f.Column, Nullable: true, Type: arrow.PrimitiveTypes.Int64}
	case FieldList:
		return arrow.Field{Name: f.Column, Nullable: true, Type: arrow.ListOf(arrow.BinaryTypes.String)}
	default: // FieldString and json_merge are stored as VARCHAR.
		return arrow.Field{Name: f.Column, Nullable: true, Type: arrow.BinaryTypes.String}
	}
}

// buildArrowBuilder creates an Arrow builder for a Fold field.
func buildArrowBuilder(alloc memory.Allocator, f Field) array.Builder {
	switch f.Type {
	case FieldInt64:
		return array.NewInt64Builder(alloc)
	case FieldList:
		return array.NewListBuilder(alloc, arrow.BinaryTypes.String)
	default:
		return array.NewStringBuilder(alloc)
	}
}

// appendValue appends one value to the matching Arrow builder.
func appendValue(builder array.Builder, f Field, val any) {
	switch b := builder.(type) {
	case *array.StringBuilder:
		switch v := val.(type) {
		case nil:
			b.AppendNull()
		case string:
			b.Append(v)
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.Int64Builder:
		if val == nil {
			b.AppendNull()
		} else {
			switch v := val.(type) {
			case int64:
				b.Append(v)
			case int:
				b.Append(int64(v))
			default:
				b.AppendNull()
			}
		}
	case *array.ListBuilder:
		if val == nil {
			b.AppendNull()
		} else {
			b.Append(true)
			vb := b.ValueBuilder().(*array.StringBuilder)
			switch v := val.(type) {
			case []string:
				for _, s := range v {
					vb.Append(s)
				}
			case string:
				vb.Append(v)
			default:
				vb.Append(fmt.Sprintf("%v", v))
			}
		}
	}
}

// addBloomFilters rewrites a DuckDB-produced Parquet file with Arrow bloom filters.
// It runs only when the schema contains bloom-enabled fields.
func addBloomFilters(path string, schema *Schema) error {
	bloomCols := schema.BloomColumns()
	if len(bloomCols) == 0 {
		return nil
	}

	srcFile, err := os.Open(path)
	if err != nil {
		return err
	}

	rdr, err := file.NewParquetReader(srcFile)
	if err != nil {
		srcFile.Close()
		return err
	}

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		rdr.Close()
		srcFile.Close()
		return err
	}

	arrowSchema, err := arrowRdr.Schema()
	if err != nil {
		rdr.Close()
		srcFile.Close()
		return err
	}

	ctx := context.Background()
	tbl, err := arrowRdr.ReadTable(ctx)
	if err != nil {
		rdr.Close()
		srcFile.Close()
		return err
	}
	defer tbl.Release()

	rdr.Close()
	srcFile.Close()

	writerOpts := []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
	}
	for _, col := range bloomCols {
		writerOpts = append(writerOpts, parquet.WithBloomFilterEnabledFor(col, true))
	}
	writerProps := parquet.NewWriterProperties(writerOpts...)
	arrowProps := pqarrow.DefaultWriterProps()

	tmpPath := path + ".bloom.tmp"
	outFile, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	writer, err := pqarrow.NewFileWriter(arrowSchema, outFile, writerProps, arrowProps)
	if err != nil {
		outFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := writer.WriteTable(tbl, tbl.NumRows()); err != nil {
		writer.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := writer.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, path)
}
