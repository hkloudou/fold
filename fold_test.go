package fold

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/xuri/excelize/v2"
)

type SourceInfo struct {
	FirstSeen int64
	LastSeen  int64
}

type PartitionedRow struct {
	ID        string `bd:"pk;bloom;partition:p=[0:2],b=hash(16)"`
	Name      string
	Region    string
	UpdatedAt int64 `bd:"overwrite"`
}

type MergeRow struct {
	ID       string `bd:"pk;bloom"`
	Name     string
	Tags     []string
	Meta     string `bd:"json_merge"`
	Score    int64  `bd:"overwrite"`
	MaxScore int64  `bd:"max"`
	MinTime  int64  `bd:"min"`
	Total    int64  `bd:"sum"`
}

type JSONMapRow struct {
	ID     string                `bd:"pk"`
	Source map[string]SourceInfo `bd:"json_merge"`
}

type ContactObservation struct {
	Phone     string `bd:"pk;column:phone"`
	Source    string `bd:"pk"`
	FirstSeen int64  `bd:"min"`
	LastSeen  int64  `bd:"max"`
	Count     int64  `bd:"sum"`
}

type AutoListRow struct {
	ID   string `bd:"pk"`
	Tags []string
}

func TestParseSchemaAndPartition(t *testing.T) {
	schema, err := parseSchema[PartitionedRow]()
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if schema.Name != "partitioned_row" {
		t.Fatalf("table name = %q", schema.Name)
	}
	if len(schema.PKs) != 1 || schema.PKs[0].Column != "id" {
		t.Fatalf("unexpected primary keys: %v", schema.PKColumns())
	}
	if len(schema.Partitions) != 2 {
		t.Fatalf("partition count = %d", len(schema.Partitions))
	}
	if !schema.PKs[0].Bloom {
		t.Fatal("primary key should have bloom filter enabled")
	}

	row := map[string]any{"id": "ab123"}
	if got := evalPartition(row, schema.Partitions[0], schema); got != "ab" {
		t.Fatalf("slice partition = %q", got)
	}
	if got := evalPartition(row, schema.Partitions[1], schema); len(got) != 2 {
		t.Fatalf("hash partition should be two hex digits, got %q", got)
	}
}

func TestAutoListUnionSchema(t *testing.T) {
	schema, err := parseSchema[AutoListRow]()
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	for _, field := range schema.Fields {
		if field.Column == "tags" && field.Strategy != StrategyListUnion {
			t.Fatalf("untagged []string strategy = %s", field.Strategy)
		}
	}
}

func TestReadExcel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.xlsx")
	book := excelize.NewFile()
	sheet := book.GetSheetName(0)
	rows := [][]string{
		{"id", "name", "phone_a", "phone_b", "address_a", "address_b"},
		{"r1", "alpha", "111,222", "333", "", "west"},
		{"r2", "beta", "N/A", "444", "east", "west"},
	}
	for r, row := range rows {
		for c, value := range row {
			cell, err := excelize.CoordinatesToCellName(c+1, r+1)
			if err != nil {
				t.Fatalf("cell name: %v", err)
			}
			if err := book.SetCellValue(sheet, cell, value); err != nil {
				t.Fatalf("set cell: %v", err)
			}
		}
	}
	if err := book.SaveAs(path); err != nil {
		t.Fatalf("save workbook: %v", err)
	}

	records, err := ReadExcel(path, &ExcelOpt{
		Header: 1,
		Fields: map[string]string{
			"id":      "id",
			"name":    "name",
			"phones":  "phone_a & phone_b",
			"address": "address_a | address_b",
		},
		Null:  []string{"N/A"},
		Split: map[string]string{"phones": ","},
		Transform: map[string]func(any) any{
			"name": func(v any) any {
				return strings.ToUpper(fmt.Sprintf("%v", v))
			},
		},
	})
	if err != nil {
		t.Fatalf("read Excel: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d", len(records))
	}
	if records[0].Str("name") != "ALPHA" {
		t.Fatalf("transformed name = %q", records[0].Str("name"))
	}
	if strings.Join(records[0].StrList("phones"), ",") != "111,222,333" {
		t.Fatalf("merged phones = %v", records[0].StrList("phones"))
	}
	if records[0].Str("address") != "west" || records[1].Str("address") != "east" {
		t.Fatalf("address fallback failed: %q %q", records[0].Str("address"), records[1].Str("address"))
	}
}

func TestReadJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.jsonl")
	body := strings.Join([]string{
		`{"items":[{"profile":{"name":"alpha"},"alias":"","contact":{"email":"a@example.com","alt":"b@example.com"}}]}`,
		`{"items":[{"profile":{"name":""},"alias":"beta","contact":{"email":"N/A","alt":"c@example.com"}}]}`,
	}, "\n")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write JSONL: %v", err)
	}

	records, err := ReadJSONL(path, &JSONLOpt{
		Root: "items",
		Fields: map[string]string{
			"name":  "profile.name | alias",
			"email": "contact.email & contact.alt",
		},
		Null: []string{"N/A"},
	})
	if err != nil {
		t.Fatalf("read JSONL: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d", len(records))
	}
	if records[0].Str("name") != "alpha" || records[1].Str("name") != "beta" {
		t.Fatalf("name fallback failed: %q %q", records[0].Str("name"), records[1].Str("name"))
	}
	if strings.Join(records[1].StrList("email"), ",") != "c@example.com" {
		t.Fatalf("email merge failed: %v", records[1].StrList("email"))
	}
}

func TestImportMergeStrategies(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("batch-a", []MergeRow{
		{ID: "a", Name: "Alice", Tags: []string{"go", "rust"}, Meta: `{"x":"1"}`, Score: 10, MaxScore: 10, MinTime: 100, Total: 2},
		{ID: "b", Name: "Bob", Tags: []string{"python"}, Meta: `{"a":"old"}`, Score: 20, MaxScore: 20, MinTime: 200, Total: 5},
	}); err != nil {
		t.Fatalf("import batch a: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge batch a: %v", err)
	}

	if err := table.Import("batch-b", []MergeRow{
		{ID: "a", Tags: []string{"rust", "java"}, Meta: `{"y":"2"}`, Score: 15, MaxScore: 8, MinTime: 50, Total: 3},
		{ID: "b", Name: "Bobby", Tags: []string{"js"}, Meta: `{"a":"new","b":"added"}`, Score: 25, MaxScore: 30, MinTime: 250, Total: 7},
	}); err != nil {
		t.Fatalf("import batch b: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge batch b: %v", err)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(table.mainDir()))

	var name, tags, meta string
	var score, maxScore, minTime, total int64
	err = queryDB.QueryRow(fmt.Sprintf(
		`SELECT name, CAST(tags AS VARCHAR), meta, score, max_score, min_time, total
		 FROM read_parquet([%s], union_by_name=true) WHERE id='a'`, files,
	)).Scan(&name, &tags, &meta, &score, &maxScore, &minTime, &total)
	if err != nil {
		t.Fatalf("query row a: %v", err)
	}
	if name != "Alice" || score != 15 || maxScore != 10 || minTime != 50 || total != 5 {
		t.Fatalf("unexpected row a values: %q %d %d %d %d", name, score, maxScore, minTime, total)
	}
	for _, expected := range []string{"go", "rust", "java"} {
		if !strings.Contains(tags, expected) {
			t.Fatalf("row a tags missing %s: %s", expected, tags)
		}
	}
	if !strings.Contains(meta, `"x":"1"`) || !strings.Contains(meta, `"y":"2"`) {
		t.Fatalf("row a meta = %s", meta)
	}

	err = queryDB.QueryRow(fmt.Sprintf(
		`SELECT name, meta, score, max_score, min_time, total
		 FROM read_parquet([%s], union_by_name=true) WHERE id='b'`, files,
	)).Scan(&name, &meta, &score, &maxScore, &minTime, &total)
	if err != nil {
		t.Fatalf("query row b: %v", err)
	}
	if name != "Bobby" || score != 25 || maxScore != 30 || minTime != 200 || total != 12 {
		t.Fatalf("unexpected row b values: %q %d %d %d %d", name, score, maxScore, minTime, total)
	}
	if !strings.Contains(meta, `"a":"new"`) || !strings.Contains(meta, `"b":"added"`) {
		t.Fatalf("row b meta = %s", meta)
	}
	if len(listParquetFiles(table.incDir())) != 0 {
		t.Fatal("inc files should be removed after merge")
	}
}

func TestPartitionedMerge(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[PartitionedRow](db)

	if err := table.Import("source", []PartitionedRow{
		{ID: "aa001", Name: "Alpha", Region: "west", UpdatedAt: 100},
		{ID: "bb001", Name: "Beta", Region: "east", UpdatedAt: 100},
	}); err != nil {
		t.Fatalf("import partitioned rows: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge partitioned rows: %v", err)
	}

	files := listParquetFiles(table.mainDir())
	if len(files) != 2 {
		t.Fatalf("expected two partition output files, got %d", len(files))
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	var count int
	glob := filepath.Join(table.mainDir(), "**", "*.parquet")
	if err := queryDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s', union_by_name=true)`, glob)).Scan(&count); err != nil {
		t.Fatalf("query partitioned rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("partitioned row count = %d", count)
	}
}

func TestUpsertCompositePrimaryKey(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[ContactObservation](db)

	if err := table.Upsert("source-a", []RawRecord{
		{"phone": "100", "source": "alpha", "first_seen": int64(100), "last_seen": int64(100), "count": int64(1)},
		{"phone": "100", "source": "beta", "first_seen": int64(200), "last_seen": int64(200), "count": int64(1)},
	}); err != nil {
		t.Fatalf("upsert batch a: %v", err)
	}
	if err := table.Upsert("source-b", []RawRecord{
		{"phone": "100", "source": "alpha", "first_seen": int64(50), "last_seen": int64(300), "count": int64(2)},
	}); err != nil {
		t.Fatalf("upsert batch b: %v", err)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(table.mainDir()))
	rows, err := queryDB.Query(fmt.Sprintf(
		`SELECT phone, source, first_seen, last_seen, count
		 FROM read_parquet([%s], union_by_name=true) ORDER BY phone, source`, files,
	))
	if err != nil {
		t.Fatalf("query observations: %v", err)
	}
	defer rows.Close()

	type observed struct {
		phone     string
		source    string
		firstSeen int64
		lastSeen  int64
		count     int64
	}
	var got []observed
	for rows.Next() {
		var item observed
		if err := rows.Scan(&item.phone, &item.source, &item.firstSeen, &item.lastSeen, &item.count); err != nil {
			t.Fatalf("scan observation: %v", err)
		}
		got = append(got, item)
	}
	if len(got) != 2 {
		t.Fatalf("observation count = %d", len(got))
	}
	if got[0] != (observed{phone: "100", source: "alpha", firstSeen: 50, lastSeen: 300, count: 3}) {
		t.Fatalf("alpha observation = %+v", got[0])
	}
	if got[1] != (observed{phone: "100", source: "beta", firstSeen: 200, lastSeen: 200, count: 1}) {
		t.Fatalf("beta observation = %+v", got[1])
	}
}

func TestJSONMergeMapAndHelper(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[JSONMapRow](db)

	if JSON(`{"already":"json"}`) != `{"already":"json"}` {
		t.Fatal("JSON should pass strings through")
	}
	if JSON([]byte(`{"bytes":"json"}`)) != `{"bytes":"json"}` {
		t.Fatal("JSON should convert bytes to string")
	}

	if err := table.Import("source", []JSONMapRow{
		{
			ID: "a",
			Source: map[string]SourceInfo{
				"source_a": {FirstSeen: 100, LastSeen: 200},
			},
		},
	}); err != nil {
		t.Fatalf("import JSON map row: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge JSON map row: %v", err)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(table.mainDir()))
	var source string
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT source FROM read_parquet([%s], union_by_name=true) WHERE id='a'`, files,
	)).Scan(&source); err != nil {
		t.Fatalf("query JSON map row: %v", err)
	}
	if !strings.Contains(source, "source_a") || !strings.Contains(source, "FirstSeen") {
		t.Fatalf("source JSON = %s", source)
	}
}

func TestCRDTAssociativityForNumericStrategies(t *testing.T) {
	records := []MergeRow{
		{ID: "x", MaxScore: 10, MinTime: 300, Total: 1},
		{ID: "x", MaxScore: 30, MinTime: 200, Total: 2},
		{ID: "x", MaxScore: 20, MinTime: 100, Total: 3},
	}

	batch := runMergePath(t, [][]MergeRow{records})
	chunked := runMergePath(t, [][]MergeRow{{records[0]}, {records[1], records[2]}})
	upserted := runUpsertPath(t, records)

	expected := numericResult{maxScore: 30, minTime: 100, total: 6}
	for name, result := range map[string]numericResult{
		"batch":   batch,
		"chunked": chunked,
		"upsert":  upserted,
	} {
		if result != expected {
			t.Fatalf("%s result = %+v", name, result)
		}
	}
}

type numericResult struct {
	maxScore int64
	minTime  int64
	total    int64
}

func runMergePath(t *testing.T, batches [][]MergeRow) numericResult {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	for i, batch := range batches {
		if err := table.Import(fmt.Sprintf("batch-%d", i), batch); err != nil {
			t.Fatalf("import batch %d: %v", i, err)
		}
		if err := table.Merge(); err != nil {
			t.Fatalf("merge batch %d: %v", i, err)
		}
	}
	return queryNumericResult(t, table.mainDir())
}

func runUpsertPath(t *testing.T, records []MergeRow) numericResult {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	for _, rec := range records {
		if err := table.Upsert("upsert", []RawRecord{{
			"id":        rec.ID,
			"max_score": rec.MaxScore,
			"min_time":  rec.MinTime,
			"total":     rec.Total,
		}}); err != nil {
			t.Fatalf("upsert record: %v", err)
		}
	}
	return queryNumericResult(t, table.mainDir())
}

func queryNumericResult(t *testing.T, dir string) numericResult {
	t.Helper()
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(dir))
	var result numericResult
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT max_score, min_time, total FROM read_parquet([%s], union_by_name=true) WHERE id='x'`, files,
	)).Scan(&result.maxScore, &result.minTime, &result.total); err != nil {
		t.Fatalf("query numeric result: %v", err)
	}
	return result
}

func openQueryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open DuckDB: %v", err)
	}
	return db
}

func TestCompositePKSchema(t *testing.T) {
	schema, err := parseSchema[ContactObservation]()
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if got := schema.PKColumns(); strings.Join(got, ",") != "phone,source" {
		t.Fatalf("primary keys = %v", got)
	}
}

func TestSnakeCase(t *testing.T) {
	tests := map[string]string{
		"ID":          "id",
		"HTTPServer":  "http_server",
		"CompanyName": "company_name",
		"UpdatedAt":   "updated_at",
	}
	keys := make([]string, 0, len(tests))
	for key := range tests {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, input := range keys {
		if got := toSnakeCase(input); got != tests[input] {
			t.Fatalf("toSnakeCase(%q) = %q", input, got)
		}
	}
}
