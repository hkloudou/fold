package fold

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

// Import writes typed records into the inc/ area.
// source identifies the data producer; records is a user-defined struct slice.
func (t *Table[T]) Import(source string, records []T) error {
	if len(records) == 0 {
		return nil
	}

	schema := t.schema

	// Extract all record values into []map[column]any by reflection.
	rows, err := extractRows(schema, records)
	if err != nil {
		return err
	}

	// Sort by composite primary key.
	pkCols := schema.PKColumns()
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

	// Group rows by partition rules.
	if len(schema.Partitions) > 0 {
		groups := groupByPartitions(rows, schema)
		for partDir, groupRows := range groups {
			outDir := filepath.Join(t.incDir(), source, partDir)
			if err := writeParquet(outDir, schema, groupRows); err != nil {
				return fmt.Errorf("fold: write partition %s: %w", partDir, err)
			}
		}
	} else {
		outDir := filepath.Join(t.incDir(), source)
		if err := writeParquet(outDir, schema, rows); err != nil {
			return fmt.Errorf("fold: write failed: %w", err)
		}
	}

	return nil
}

// extractRows converts []T into []map[column]any by reflection.
func extractRows[T any](schema *Schema, records []T) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(records))
	for _, rec := range records {
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
						return nil, fmt.Errorf("serialize json_merge field %s: %w", f.Column, err)
					}
					row[f.Column] = string(b)
				}
			} else {
				row[f.Column] = fv.Interface()
			}
		}
		// Drop rows with any missing primary-key column.
		allPKs := true
		for _, pk := range schema.PKs {
			if _, ok := row[pk.Column]; !ok {
				allPKs = false
				break
			}
		}
		if !allPKs {
			continue
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// groupByPartitions groups rows by partition rules.
func groupByPartitions(rows []map[string]any, schema *Schema) map[string][]map[string]any {
	groups := make(map[string][]map[string]any)

	for _, row := range rows {
		var parts []string
		for _, p := range schema.Partitions {
			val := evalPartition(row, p, schema)
			parts = append(parts, fmt.Sprintf("%s=%s", p.Key, val))
		}
		key := filepath.Join(parts...)
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
		val = fmt.Sprintf("%v", v)
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

	outPath := filepath.Join(dir, uuid.New().String()+".parquet")

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

	// Write Parquet.
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

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
		return err
	}

	if err := writer.Write(record); err != nil {
		writer.Close()
		return err
	}

	return writer.Close()
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
		if val == nil {
			b.AppendNull()
		} else {
			b.Append(fmt.Sprintf("%v", val))
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

// partitionExprToSQL converts a partition rule into a DuckDB SQL expression.
func partitionExprToSQL(p PartitionRule, schema *Schema) string {
	col := ""
	for _, f := range schema.Fields {
		if f.GoName == p.Field {
			col = f.Column
			break
		}
	}
	if col == "" {
		return "''"
	}

	if p.Hash > 0 {
		return fmt.Sprintf("printf('%%02x', hash(%s) %% %d)", col, p.Hash)
	}

	if p.Slice != [2]int{0, 0} {
		start := p.Slice[0]
		length := p.Slice[1] - p.Slice[0]
		return fmt.Sprintf("substr(%s, %d, %d)", col, start+1, length)
	}

	return col
}

// addBloomFilters rewrites a DuckDB-produced Parquet file with Arrow bloom filters.
// It runs only when the schema contains bloom-enabled fields.
func addBloomFilters(path string, schema *Schema) error {
	var bloomCols []string
	for _, f := range schema.Fields {
		if f.Bloom {
			bloomCols = append(bloomCols, f.Column)
		}
	}
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

// partitionDirSQL builds the full partition directory SQL expression.
func partitionDirSQL(schema *Schema) string {
	if len(schema.Partitions) == 0 {
		return ""
	}
	var parts []string
	for _, p := range schema.Partitions {
		parts = append(parts, fmt.Sprintf("'%s=' || %s", p.Key, partitionExprToSQL(p, schema)))
	}
	return strings.Join(parts, " || '/' || ")
}
