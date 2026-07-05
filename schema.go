package fold

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// Strategy defines the CRDT-style merge strategy for a field.
type Strategy string

const (
	StrategyPK        Strategy = "pk"
	StrategyCoalesce  Strategy = "coalesce"
	StrategyListUnion Strategy = "list_union"
	StrategyJSONMerge Strategy = "json_merge" // RFC 7396 JSON Merge Patch
	StrategyOverwrite Strategy = "overwrite"
	StrategyMax       Strategy = "max" // GREATEST / MAX
	StrategyMin       Strategy = "min" // LEAST / MIN
	StrategySum       Strategy = "sum" // additive counter
	StrategySkip      Strategy = "-"
)

// FieldType is the physical Parquet storage type for a field.
type FieldType string

const (
	FieldString FieldType = "string"
	FieldInt64  FieldType = "int64"
	FieldList   FieldType = "list" // list<string>
)

// PartitionRule describes one partitioning rule.
type PartitionRule struct {
	Key   string // partition key name, for example "p", "a", or "b"
	Field string // source Go field name
	Slice [2]int // substring range [start, end]; zero means direct value
	Hash  int    // hash bucket count; zero disables hash bucketing
}

// Field stores metadata for one persisted Parquet column.
type Field struct {
	GoName    string          // Go struct field name
	Column    string          // Parquet column name, usually snake_case
	Strategy  Strategy        // merge strategy
	Type      FieldType       // storage type
	Bloom     bool            // whether to enable Parquet bloom filters
	Partition []PartitionRule // partition rules derived from this field
	Index     int             // field index in the Go struct
	MergeExpr string          // custom SQL expression for FULL OUTER JOIN; {} = column name
	AggExpr   string          // custom SQL expression for inc GROUP BY; {} = column name
}

// GetSQLExpr returns the SQL expression used during FULL OUTER JOIN merge.
// Custom MergeExpr takes precedence over the built-in strategy expression.
// {} expands to the quoted column name, so exprs like "COALESCE(s.{}, t.{})"
// stay valid for reserved-word columns.
func (f *Field) GetSQLExpr() string {
	if f.MergeExpr != "" {
		return strings.ReplaceAll(f.MergeExpr, "{}", sqlIdent(f.Column))
	}
	return f.Strategy.SQLExpr(f.Column)
}

// GetAggExpr returns the aggregation expression used to pre-merge inc rows.
// Custom AggExpr takes precedence over the built-in strategy expression.
func (f *Field) GetAggExpr() string {
	if f.AggExpr != "" {
		return strings.ReplaceAll(f.AggExpr, "{}", sqlIdent(f.Column)) + " AS " + sqlIdent(f.Column)
	}
	return f.Strategy.IncAggExpr(f.Column)
}

// Schema contains all metadata for one table.
type Schema struct {
	Name       string          // table name, usually snake_case
	Fields     []Field         // persisted fields
	PKs        []*Field        // primary-key fields; composite keys are supported
	Partitions []PartitionRule // flattened partition rules
	structType reflect.Type
}

// PKColumns returns all primary-key column names.
func (s *Schema) PKColumns() []string {
	cols := make([]string, len(s.PKs))
	for i, pk := range s.PKs {
		cols[i] = pk.Column
	}
	return cols
}

var schemaCache sync.Map

// TableNamer lets a struct override the default table name.
type TableNamer interface {
	TableName() string
}

// parseSchema parses a Go struct and its bd tags into a Schema.
func parseSchema[T any]() (*Schema, error) {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("fold: Register requires a struct type, got %s", t.Kind())
	}

	// Cache parsed schemas by reflected type.
	if cached, ok := schemaCache.Load(t); ok {
		return cached.(*Schema), nil
	}

	schema := &Schema{
		Name:       tableName[T](t),
		structType: t,
	}

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}

		field, err := parseField(sf, i)
		if err != nil {
			return nil, fmt.Errorf("fold: parse field %s: %w", sf.Name, err)
		}
		if field == nil {
			continue // bd:"-"
		}

		schema.Fields = append(schema.Fields, *field)
		schema.Partitions = append(schema.Partitions, field.Partition...)
	}

	// Collect PK pointers only after Fields stops growing: appending can
	// reallocate the backing array, which would leave earlier-taken pointers
	// aimed at the stale copy.
	for i := range schema.Fields {
		if schema.Fields[i].Strategy == StrategyPK {
			schema.PKs = append(schema.PKs, &schema.Fields[i])
		}
	}

	if len(schema.PKs) == 0 {
		return nil, fmt.Errorf("fold: struct %s is missing a pk field", t.Name())
	}

	schemaCache.Store(t, schema)
	return schema, nil
}

// parseField parses one struct field and its bd tag.
func parseField(sf reflect.StructField, index int) (*Field, error) {
	tag := sf.Tag.Get("bd")

	field := &Field{
		GoName:   sf.Name,
		Column:   toSnakeCase(sf.Name),
		Strategy: StrategyCoalesce, // default
		Index:    index,
	}

	// Infer storage type from the Go field type.
	ft, err := detectFieldType(sf.Type, tag)
	if err != nil {
		return nil, err
	}
	field.Type = ft

	if tag == "" {
		// Untagged []string fields default to set union.
		if field.Type == FieldList {
			field.Strategy = StrategyListUnion
		}
		return field, nil
	}

	if tag == "-" {
		return nil, nil
	}

	// Parse semicolon-separated tag segments.
	hasExplicitStrategy := false
	parts := strings.Split(tag, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		switch {
		case part == "pk":
			field.Strategy = StrategyPK
			hasExplicitStrategy = true
		case part == "coalesce":
			field.Strategy = StrategyCoalesce
			hasExplicitStrategy = true
		case part == "list_union":
			field.Strategy = StrategyListUnion
			hasExplicitStrategy = true
		case part == "json_merge":
			field.Strategy = StrategyJSONMerge
			field.Type = FieldString // JSON is stored as VARCHAR
			hasExplicitStrategy = true
		case part == "overwrite":
			field.Strategy = StrategyOverwrite
			hasExplicitStrategy = true
		case part == "max":
			field.Strategy = StrategyMax
			hasExplicitStrategy = true
		case part == "min":
			field.Strategy = StrategyMin
			hasExplicitStrategy = true
		case part == "sum":
			field.Strategy = StrategySum
			hasExplicitStrategy = true
		case strings.HasPrefix(part, "expr:"):
			field.MergeExpr = part[len("expr:"):]
			hasExplicitStrategy = true
		case strings.HasPrefix(part, "agg:"):
			field.AggExpr = part[len("agg:"):]
			hasExplicitStrategy = true
		case part == "bloom":
			field.Bloom = true
		case strings.HasPrefix(part, "partition:"):
			rules, err := parsePartitionTag(part[len("partition:"):], sf.Name)
			if err != nil {
				return nil, err
			}
			field.Partition = rules
		case strings.HasPrefix(part, "column:"):
			name := part[len("column:"):]
			if name == "" {
				return nil, fmt.Errorf("empty column name in bd tag %q", part)
			}
			field.Column = name
		default:
			return nil, fmt.Errorf("unknown bd tag: %q", part)
		}
	}

	// []string fields without an explicit strategy default to set union.
	if field.Type == FieldList && !hasExplicitStrategy {
		field.Strategy = StrategyListUnion
	}

	return field, nil
}

// detectFieldType infers the Parquet storage type from a Go type.
func detectFieldType(t reflect.Type, tag string) (FieldType, error) {
	// json_merge fields are stored as VARCHAR.
	if strings.Contains(tag, "json_merge") {
		return FieldString, nil
	}

	switch t.Kind() {
	case reflect.String:
		return FieldString, nil
	case reflect.Int64:
		return FieldInt64, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.String {
			return FieldList, nil
		}
		return "", fmt.Errorf("unsupported slice type: []%s; only []string is supported", t.Elem().Kind())
	case reflect.Map, reflect.Struct:
		// map/struct fields must opt into JSON storage.
		return "", fmt.Errorf("map/struct fields must use bd:\"json_merge\" to be stored as JSON")
	default:
		return "", fmt.Errorf("unsupported field type: %s", t.Kind())
	}
}

// Partition tag parsers.
var (
	partSliceRe  = regexp.MustCompile(`^(\w+)=\[(\d+):(\d+)\]$`) // p=[2:4]
	partHashRe   = regexp.MustCompile(`^(\w+)=hash\((\d+)\)$`)   // b=hash(256)
	partDirectRe = regexp.MustCompile(`^(\w+)$`)                 // area
)

// parsePartitionTag parses expressions such as partition:p=[2:4],a=[4:8].
func parsePartitionTag(expr string, fieldName string) ([]PartitionRule, error) {
	var rules []PartitionRule
	for _, seg := range strings.Split(expr, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}

		if m := partSliceRe.FindStringSubmatch(seg); m != nil {
			start, _ := strconv.Atoi(m[2])
			end, _ := strconv.Atoi(m[3])
			rules = append(rules, PartitionRule{
				Key:   m[1],
				Field: fieldName,
				Slice: [2]int{start, end},
			})
		} else if m := partHashRe.FindStringSubmatch(seg); m != nil {
			n, _ := strconv.Atoi(m[2])
			rules = append(rules, PartitionRule{
				Key:   m[1],
				Field: fieldName,
				Hash:  n,
			})
		} else if m := partDirectRe.FindStringSubmatch(seg); m != nil {
			rules = append(rules, PartitionRule{
				Key:   m[1],
				Field: fieldName,
			})
		} else {
			return nil, fmt.Errorf("cannot parse partition expression: %q", seg)
		}
	}
	return rules, nil
}

// tableName returns TableName() when implemented, otherwise snake_case(type).
func tableName[T any](t reflect.Type) string {
	var zero T
	if namer, ok := any(&zero).(TableNamer); ok {
		return namer.TableName()
	}
	return toSnakeCase(t.Name())
}

// toSnakeCase converts CamelCase into snake_case.
func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := rune(s[i-1])
				if unicode.IsLower(prev) || (i+1 < len(s) && unicode.IsLower(rune(s[i+1]))) {
					result = append(result, '_')
				}
			}
			result = append(result, unicode.ToLower(r))
		} else {
			result = append(result, r)
		}
	}
	return string(result)
}

// SQLExpr returns the DuckDB SQL expression for inc/main FULL OUTER JOIN merge.
// s = inc_merged (new data), t = main (existing data). The column name is
// interpolated quoted, so reserved words cannot break the statement.
func (s Strategy) SQLExpr(field string) string {
	q := sqlIdent(field)
	switch s {
	case StrategyPK:
		return fmt.Sprintf("COALESCE(s.%s, t.%s)", q, q)
	case StrategyCoalesce:
		return fmt.Sprintf("COALESCE(s.%s, t.%s)", q, q)
	case StrategyListUnion:
		return fmt.Sprintf("list_distinct(list_cat(COALESCE(CAST(t.%s AS VARCHAR[]), []), COALESCE(CAST(s.%s AS VARCHAR[]), [])))", q, q)
	case StrategyOverwrite:
		return fmt.Sprintf("COALESCE(s.%s, t.%s)", q, q)
	case StrategyJSONMerge:
		// RFC7396: json_merge_patch(old, new)
		return fmt.Sprintf("CASE WHEN s.%[1]s IS NOT NULL AND t.%[1]s IS NOT NULL THEN json_merge_patch(t.%[1]s::JSON, s.%[1]s::JSON)::VARCHAR WHEN s.%[1]s IS NOT NULL THEN s.%[1]s ELSE t.%[1]s END", q)
	case StrategyMax:
		return fmt.Sprintf("GREATEST(s.%s, t.%s)", q, q)
	case StrategyMin:
		return fmt.Sprintf("LEAST(s.%s, t.%s)", q, q)
	case StrategySum:
		return fmt.Sprintf("COALESCE(s.%s, 0) + COALESCE(t.%s, 0)", q, q)
	default:
		return fmt.Sprintf("COALESCE(s.%s, t.%s)", q, q)
	}
}

// IncAggExpr returns the GROUP BY aggregation expression for inc pre-merge.
// The column name is interpolated quoted, so reserved words cannot break the
// statement.
func (s Strategy) IncAggExpr(field string) string {
	q := sqlIdent(field)
	switch s {
	case StrategyListUnion:
		return fmt.Sprintf("list_distinct(flatten(list(%s))) AS %s", q, q)
	case StrategyOverwrite:
		return fmt.Sprintf("MAX(%s) AS %s", q, q)
	case StrategyCoalesce:
		return fmt.Sprintf("COALESCE(MAX(%s), FIRST(%s)) AS %s", q, q, q)
	case StrategyJSONMerge:
		// Fold every patch for the primary key (RFC 7396) instead of keeping an
		// arbitrary one as ANY_VALUE did, which silently dropped patches.
		// Non-conflicting keys union. For a key set to different values within a
		// single batch, folding in ascending patch-text order makes the greatest
		// patch win — a stable tie-break, not a temporal one, because Fold keeps
		// no per-row sequence within a batch. Temporal precedence (a later merge
		// wins) is applied across merge cycles by GetSQLExpr. NULLs are skipped.
		return fmt.Sprintf("CAST(list_reduce(list(CAST(%[1]s AS JSON) ORDER BY %[1]s) FILTER (WHERE %[1]s IS NOT NULL), (a, b) -> json_merge_patch(a, b)) AS VARCHAR) AS %[1]s", q)
	case StrategyMax:
		return fmt.Sprintf("MAX(%s) AS %s", q, q)
	case StrategyMin:
		return fmt.Sprintf("MIN(%s) AS %s", q, q)
	case StrategySum:
		return fmt.Sprintf("SUM(%s) AS %s", q, q)
	default:
		return fmt.Sprintf("MAX(%s) AS %s", q, q)
	}
}
