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

// TestJSONMergeConflictContract locks the json_merge conflict contract so the
// behavior cannot drift undocumented:
//   - Non-conflicting keys always union, regardless of how rows are batched
//     (this is the original silent-data-loss regression).
//   - Across merge cycles, a later batch wins for a conflicting key (temporal
//     last-write-wins, handled by the FULL OUTER JOIN merge in GetSQLExpr).
//   - Within a single batch, conflicting keys are folded in ascending JSON-text
//     order (the greatest patch wins), independent of row order: Fold keeps no
//     per-row sequence inside a batch. Callers express precedence with separate
//     merge cycles.
func TestJSONMergeConflictContract(t *testing.T) {
	// (1) Non-conflicting patches in one batch must all survive.
	meta := mergeJSONOneBatch(t, "a", []string{`{"x":"1"}`, `{"y":"2"}`, `{"z":"3"}`})
	for _, key := range []string{`"x":"1"`, `"y":"2"`, `"z":"3"`} {
		if !strings.Contains(meta, key) {
			t.Fatalf("non-conflicting patch dropped: meta=%s missing %s", meta, key)
		}
	}

	// (2) A same-batch conflict is resolved by ascending JSON-text order, so the
	// lexicographically-greatest patch wins the key, independent of input row
	// order: {"k":"b"} > {"k":"a"}, so "b" wins either way.
	forward := mergeJSONOneBatch(t, "a", []string{`{"k":"a"}`, `{"k":"b"}`})
	reverse := mergeJSONOneBatch(t, "a", []string{`{"k":"b"}`, `{"k":"a"}`})
	if forward != reverse {
		t.Fatalf("same-batch conflict not order-independent: %q vs %q", forward, reverse)
	}
	if !strings.Contains(forward, `"k":"b"`) {
		t.Fatalf("same-batch conflict should pick the greatest patch by JSON text: meta=%s", forward)
	}

	// (3) Across merge cycles, the later batch wins for a conflicting key. This
	// is the documented way to express precedence.
	later := mergeJSONAcrossBatches(t, "a", []string{`{"k":"old"}`, `{"k":"new"}`})
	if !strings.Contains(later, `"k":"new"`) {
		t.Fatalf("cross-batch conflict must be last-write-wins: meta=%s", later)
	}
}

// mergeJSONOneBatch imports every patch for one primary key in a single batch,
// merges once, and returns the stored json_merge column.
func mergeJSONOneBatch(t *testing.T, id string, patches []string) string {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	rows := make([]MergeRow, len(patches))
	for i, p := range patches {
		rows[i] = MergeRow{ID: id, Meta: p}
	}
	if err := table.Import("batch", rows); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	return queryMeta(t, table.mainDir(), id)
}

// mergeJSONAcrossBatches imports each patch as its own batch and merge cycle.
func mergeJSONAcrossBatches(t *testing.T, id string, patches []string) string {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	for i, p := range patches {
		if err := table.Import(fmt.Sprintf("batch-%d", i), []MergeRow{{ID: id, Meta: p}}); err != nil {
			t.Fatalf("import batch %d: %v", i, err)
		}
		if err := table.Merge(); err != nil {
			t.Fatalf("merge batch %d: %v", i, err)
		}
	}
	return queryMeta(t, table.mainDir(), id)
}

func queryMeta(t *testing.T, dir, id string) string {
	t.Helper()
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(dir))
	var meta string
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT meta FROM read_parquet([%s], union_by_name=true) WHERE id='%s'`, files, id,
	)).Scan(&meta); err != nil {
		t.Fatalf("query meta for %s: %v", id, err)
	}
	return meta
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

// TestPartitionedMergeStagedPublish exercises the crash-safe publish path: a
// second merge into a partition that already has main data must stage and
// validate the replacement before removing the old file, leaving no duplicate
// primary keys and no staging temp files behind.
func TestPartitionedMergeStagedPublish(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[PartitionedRow](db)

	// First merge establishes main files for two partitions.
	if err := table.Import("s1", []PartitionedRow{
		{ID: "aa1", Name: "Alpha", Region: "west", UpdatedAt: 100},
		{ID: "bb1", Name: "Beta", Region: "east", UpdatedAt: 100},
	}); err != nil {
		t.Fatalf("import s1: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge s1: %v", err)
	}

	// Second merge updates an existing row (same partition, existing main) and
	// adds a new one, exercising replacement over active data.
	if err := table.Import("s2", []PartitionedRow{
		{ID: "aa1", Name: "AlphaV2", Region: "west", UpdatedAt: 200},
		{ID: "cc1", Name: "Gamma", Region: "south", UpdatedAt: 150},
	}); err != nil {
		t.Fatalf("import s2: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge s2: %v", err)
	}

	// No staging temp file may survive a successful publish.
	filepath.Walk(table.mainDir(), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".tmp") {
			t.Fatalf("staging temp file left behind: %s", p)
		}
		return nil
	})

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	glob := filepath.Join(table.mainDir(), "**", "*.parquet")

	// No duplicate primary keys: the superseded main file must be removed, not
	// left alongside the replacement.
	var dups int
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT count(*) FROM (SELECT id FROM read_parquet('%s', union_by_name=true) GROUP BY id HAVING count(*) > 1)`, glob,
	)).Scan(&dups); err != nil {
		t.Fatalf("duplicate check: %v", err)
	}
	if dups != 0 {
		t.Fatalf("found %d duplicated primary keys after re-merge", dups)
	}

	// The overwrite landed and no rows were lost.
	rows, err := queryDB.Query(fmt.Sprintf(
		`SELECT id, name, updated_at FROM read_parquet('%s', union_by_name=true) ORDER BY id`, glob,
	))
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var id, name string
		var updated int64
		if err := rows.Scan(&id, &name, &updated); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = fmt.Sprintf("%s/%d", name, updated)
	}
	want := map[string]string{"aa1": "AlphaV2/200", "bb1": "Beta/100", "cc1": "Gamma/150"}
	if len(got) != len(want) {
		t.Fatalf("row set = %v, want %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Fatalf("row %s = %q, want %q", id, got[id], w)
		}
	}

	// The published files must already carry the bloom filter on the bloom
	// column: the bloom rewrite runs on the staging file before it becomes
	// active, so an active main file is never bloom-incomplete.
	var bloomChunks int
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT count(*) FROM parquet_metadata('%s') WHERE path_in_schema = 'id' AND bloom_filter_offset IS NOT NULL`, glob,
	)).Scan(&bloomChunks); err != nil {
		t.Fatalf("bloom metadata query: %v", err)
	}
	if bloomChunks == 0 {
		t.Fatal("published main files carry no bloom filter on id; bloom did not complete before publish")
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

// TestMergeWritesManifest confirms a merge publishes through the partition
// manifest rather than relying on directory globs.
func TestMergeWritesManifest(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("b", []MergeRow{{ID: "x", Total: 1}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	m, err := readManifest(table.mainDir())
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if m == nil {
		t.Fatal("merge did not write a manifest")
	}
	if len(m.ActiveFiles) != 1 || m.LastCommit == "" {
		t.Fatalf("unexpected manifest: %+v", m)
	}
	if _, err := os.Stat(filepath.Join(table.mainDir(), m.ActiveFiles[0])); err != nil {
		t.Fatalf("active file from manifest is missing: %v", err)
	}
}

// TestMergeRetryIdempotent simulates a crash after a successful commit but
// before the consumed inc files were cleaned up: on the next run those files
// must be recognized as already applied and dropped, not re-merged (which would
// double-count the sum aggregate).
func TestMergeRetryIdempotent(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("b", []MergeRow{{ID: "x", Total: 5}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	// Snapshot the inc files so we can replay them as crash survivors.
	incBackup := map[string][]byte{}
	for _, f := range listParquetFiles(table.incDir()) {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read inc: %v", err)
		}
		incBackup[f] = data
	}
	if len(incBackup) == 0 {
		t.Fatal("no inc files captured")
	}

	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 5 {
		t.Fatalf("after first merge total = %d, want 5", got)
	}

	// Restore the consumed inc files, as if cleanup had been interrupted.
	for f, data := range incBackup {
		if err := os.MkdirAll(filepath.Dir(f), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(f, data, 0644); err != nil {
			t.Fatalf("restore inc: %v", err)
		}
	}

	if err := table.Merge(); err != nil {
		t.Fatalf("retry merge: %v", err)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 5 {
		t.Fatalf("retry double-applied the batch: total = %d, want 5", got)
	}
}

// TestMergeIgnoresStaleMainFile simulates a publish that crashed before cleaning
// up an old file: a stale duplicate parquet is left in the partition dir. The
// next merge must read main via the manifest (ignoring the stale file, so it is
// not double-counted) and garbage-collect it.
func TestMergeIgnoresStaleMainFile(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("b1", []MergeRow{{ID: "x", Total: 2}}); err != nil {
		t.Fatalf("import b1: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge b1: %v", err)
	}

	// Inject a stale duplicate of the active main file.
	m, err := readManifest(table.mainDir())
	if err != nil || m == nil {
		t.Fatalf("read manifest: %v (m=%v)", err, m)
	}
	data, err := os.ReadFile(filepath.Join(table.mainDir(), m.ActiveFiles[0]))
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	stale := filepath.Join(table.mainDir(), "merged_000000.parquet")
	if err := os.WriteFile(stale, data, 0644); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	if err := table.Import("b2", []MergeRow{{ID: "x", Total: 3}}); err != nil {
		t.Fatalf("import b2: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge b2: %v", err)
	}

	if got := queryTotal(t, table.mainDir(), "x"); got != 5 {
		t.Fatalf("stale main file was double-counted: total = %d, want 5", got)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale main file was not garbage-collected")
	}
	if files := listParquetFiles(table.mainDir()); len(files) != 1 {
		t.Fatalf("expected exactly one active file, got %d: %v", len(files), files)
	}
}

func queryTotal(t *testing.T, dir, id string) int64 {
	t.Helper()
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(dir))
	var total int64
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT total FROM read_parquet([%s], union_by_name=true) WHERE id='%s'`, files, id,
	)).Scan(&total); err != nil {
		t.Fatalf("query total for %s: %v", id, err)
	}
	return total
}

// TestUpsertPreservesConsumedIncState reproduces the reviewer's scenario: a
// merge commits but crashes before deleting its inc files, an Upsert runs before
// the retry, and the retry must not re-apply the crash-surviving inc.
func TestUpsertPreservesConsumedIncState(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("b", []MergeRow{{ID: "x", Total: 5}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	incBackup := snapshotFiles(t, listParquetFiles(table.incDir()))
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 5 {
		t.Fatalf("after merge total = %d, want 5", got)
	}

	// Simulate the merge crashing before inc cleanup.
	restoreFiles(t, incBackup)

	if err := table.Upsert("u", []RawRecord{{"id": "x", "total": int64(1)}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 6 {
		t.Fatalf("after upsert total = %d, want 6", got)
	}

	if err := table.Merge(); err != nil {
		t.Fatalf("retry merge: %v", err)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 6 {
		t.Fatalf("upsert stranded consumed-inc state; retry double-applied: total = %d, want 6", got)
	}
}

// TestMergeRecoversInterruptedPublish simulates a merge that committed the
// manifest but crashed before finalizing the directory: a superseded main file
// and the consumed inc both survive. The next merge (which sees only the
// consumed inc) must still finalize — dropping the superseded file and the
// leftover inc — instead of returning early.
func TestMergeRecoversInterruptedPublish(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("b", []MergeRow{{ID: "x", Total: 4}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	incBackup := snapshotFiles(t, listParquetFiles(table.incDir()))
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	// Simulate an interrupted finalize: restore the consumed inc and inject a
	// superseded main file that finalizeDir should have removed.
	restoreFiles(t, incBackup)
	m, err := readManifest(table.mainDir())
	if err != nil || m == nil {
		t.Fatalf("read manifest: %v (m=%v)", err, m)
	}
	data, err := os.ReadFile(filepath.Join(table.mainDir(), m.ActiveFiles[0]))
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	stale := filepath.Join(table.mainDir(), "merged_000000.parquet")
	if err := os.WriteFile(stale, data, 0644); err != nil {
		t.Fatalf("inject stale: %v", err)
	}

	if err := table.Merge(); err != nil {
		t.Fatalf("retry merge: %v", err)
	}

	if got := queryTotal(t, table.mainDir(), "x"); got != 4 {
		t.Fatalf("retry double-applied inc: total = %d, want 4", got)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("superseded main file was not finalized away on retry")
	}
	if files := listParquetFiles(table.mainDir()); len(files) != 1 {
		t.Fatalf("expected one active main file, got %d: %v", len(files), files)
	}
	if files := listParquetFiles(table.incDir()); len(files) != 0 {
		t.Fatalf("leftover consumed inc not cleaned: %v", files)
	}
}

func snapshotFiles(t *testing.T, paths []string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("snapshot %s: %v", p, err)
		}
		out[p] = data
	}
	if len(out) == 0 {
		t.Fatal("no files captured for snapshot")
	}
	return out
}

func restoreFiles(t *testing.T, files map[string][]byte) {
	t.Helper()
	for p, data := range files {
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatalf("restore mkdir %s: %v", p, err)
		}
		if err := os.WriteFile(p, data, 0644); err != nil {
			t.Fatalf("restore %s: %v", p, err)
		}
	}
}

// TestRecoveryErrorsWhenConsumedIncCannotBeRemoved covers the cleanup-failure
// path: if recovery cannot remove a recorded consumed inc file, a publish must
// not advance last_commit (which would GC the commit record and later re-apply
// the surviving inc). The failure is simulated deterministically by recreating
// the consumed inc path as a non-empty directory, so os.Remove fails with a
// non-IsNotExist error regardless of process privileges.
func TestRecoveryErrorsWhenConsumedIncCannotBeRemoved(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("b", []MergeRow{{ID: "x", Total: 5}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	consumed := listParquetFiles(table.incDir())
	if len(consumed) == 0 {
		t.Fatal("no inc files captured")
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	before, err := readManifest(table.mainDir())
	if err != nil || before == nil {
		t.Fatalf("read manifest: %v (m=%v)", err, before)
	}

	// Make each recorded consumed inc path un-removable: recreate it as a
	// non-empty directory so os.Remove returns a non-IsNotExist error.
	for _, p := range consumed {
		if err := os.MkdirAll(filepath.Join(p, "blocker"), 0755); err != nil {
			t.Fatalf("create blocker: %v", err)
		}
	}

	if err := table.Upsert("u", []RawRecord{{"id": "x", "total": int64(1)}}); err == nil {
		t.Fatal("upsert advanced despite un-removable consumed inc; want an error")
	}

	after, err := readManifest(table.mainDir())
	if err != nil || after == nil {
		t.Fatalf("read manifest after: %v (m=%v)", err, after)
	}
	if before.LastCommit != after.LastCommit || before.Version != after.Version {
		t.Fatalf("manifest advanced despite recovery failure: before=%+v after=%+v", before, after)
	}
}

// TestMergeIgnoresUncommittedFirstPublishOutput covers a first publish that
// crashed after writing its output but before committing: the output sits in
// the publish location (files/) with no manifest yet. The retry must treat the
// still-present inc as the only authoritative state — not adopt the orphan and
// re-apply inc — and must garbage-collect the orphan.
func TestMergeIgnoresUncommittedFirstPublishOutput(t *testing.T) {
	// Build a real parquet (total=99) to stand in for the uncommitted output.
	srcDB, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer srcDB.Close()
	src := Register[MergeRow](srcDB)
	if err := src.Import("s", []MergeRow{{ID: "x", Total: 99}}); err != nil {
		t.Fatalf("src import: %v", err)
	}
	if err := src.Merge(); err != nil {
		t.Fatalf("src merge: %v", err)
	}
	srcFiles := listParquetFiles(src.mainDir())
	if len(srcFiles) != 1 {
		t.Fatalf("src produced %d files", len(srcFiles))
	}
	orphanBytes, err := os.ReadFile(srcFiles[0])
	if err != nil {
		t.Fatalf("read src output: %v", err)
	}

	// Subject table: inc live (total=5), an uncommitted output in files/, no
	// manifest/commit metadata yet.
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	if err := table.Import("b", []MergeRow{{ID: "x", Total: 5}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	filesDir := filepath.Join(table.mainDir(), filesSubdir)
	if err := os.MkdirAll(filesDir, 0755); err != nil {
		t.Fatalf("mkdir files: %v", err)
	}
	orphan := filepath.Join(filesDir, "merged_orphan.parquet")
	if err := os.WriteFile(orphan, orphanBytes, 0644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	if got := queryTotal(t, table.mainDir(), "x"); got != 5 {
		t.Fatalf("uncommitted output adopted (inc re-applied): total=%d, want 5", got)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("uncommitted output was not garbage-collected")
	}
}

func TestCompactOptionsDefaults(t *testing.T) {
	got := CompactOptions{}.normalized()
	if got.Workers != defaultCompactWorkers {
		t.Fatalf("default workers = %d, want %d", got.Workers, defaultCompactWorkers)
	}
	if got.DuckDB.MemoryLimit != defaultMemoryLimit {
		t.Fatalf("default memory limit = %q, want %q", got.DuckDB.MemoryLimit, defaultMemoryLimit)
	}
	if got.DuckDB.Threads != defaultThreads {
		t.Fatalf("default threads = %d, want %d", got.DuckDB.Threads, defaultThreads)
	}
	if got.BloomMaxFileBytes != defaultBloomMaxFileBytes {
		t.Fatalf("default BloomMaxFileBytes = %d, want %d", got.BloomMaxFileBytes, defaultBloomMaxFileBytes)
	}

	// Explicit values are preserved; only unset fields are defaulted.
	custom := CompactOptions{Workers: 2, DuckDB: DuckDBOptions{Threads: 1, TempDir: "/tmp/x"}}.normalized()
	if custom.Workers != 2 || custom.DuckDB.Threads != 1 || custom.DuckDB.TempDir != "/tmp/x" {
		t.Fatalf("explicit options not preserved: %+v", custom)
	}
	if custom.DuckDB.MemoryLimit != defaultMemoryLimit {
		t.Fatalf("unset memory limit not defaulted: %q", custom.DuckDB.MemoryLimit)
	}
}

func TestMergeWithCustomDuckDBOptions(t *testing.T) {
	db, err := Open(t.TempDir(), WithCompactOptions(CompactOptions{
		Workers: 2,
		DuckDB:  DuckDBOptions{MemoryLimit: "512MB", Threads: 2, TempDir: t.TempDir()},
	}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if db.compact.Workers != 2 || db.compact.DuckDB.MemoryLimit != "512MB" || db.compact.DuckDB.Threads != 2 {
		t.Fatalf("options not applied: %+v", db.compact)
	}

	table := Register[PartitionedRow](db)
	if err := table.Import("s", []PartitionedRow{
		{ID: "aa1", Name: "Alpha", Region: "west", UpdatedAt: 1},
		{ID: "bb1", Name: "Beta", Region: "east", UpdatedAt: 1},
	}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge with custom options: %v", err)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	var count int
	glob := filepath.Join(table.mainDir(), "**", "*.parquet")
	if err := queryDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s', union_by_name=true)`, glob)).Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 2 {
		t.Fatalf("row count = %d, want 2", count)
	}
}

// TestMergeRejectsInvalidDuckDBOption verifies a rejected DuckDB option surfaces
// as an error instead of silently falling back to defaults.
func TestMergeRejectsInvalidDuckDBOption(t *testing.T) {
	db, err := Open(t.TempDir(), WithCompactOptions(CompactOptions{
		DuckDB: DuckDBOptions{MemoryLimit: "not-a-valid-size"},
	}))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	if err := table.Import("b", []MergeRow{{ID: "x", Total: 1}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err == nil {
		t.Fatal("merge should fail when a DuckDB option is invalid, but it succeeded")
	}
}

func TestImportOptionsDefaults(t *testing.T) {
	got := ImportOptions{}.normalized()
	if got.MaxRowsPerFile != defaultMaxRowsPerFile {
		t.Fatalf("default MaxRowsPerFile = %d, want %d", got.MaxRowsPerFile, defaultMaxRowsPerFile)
	}
	if got.MaxOpenPartitionWriters != defaultMaxOpenPartitionWriters {
		t.Fatalf("default MaxOpenPartitionWriters = %d, want %d", got.MaxOpenPartitionWriters, defaultMaxOpenPartitionWriters)
	}
}

// TestImportWriterFlushesByRowCount proves the streaming writer flushes inc
// files mid-stream rather than buffering the whole batch: 25 rows with a 10-row
// threshold must produce 3 files, and all 25 rows survive a merge.
func TestImportWriterFlushesByRowCount(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	w := table.NewImportWriter("stream", ImportOptions{MaxRowsPerFile: 10})
	for i := 0; i < 25; i++ {
		if err := w.Add(MergeRow{ID: fmt.Sprintf("id-%02d", i), Total: int64(i)}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if incFiles := listParquetFiles(table.incDir()); len(incFiles) != 3 {
		t.Fatalf("expected 3 flushed inc files (10+10+5), got %d", len(incFiles))
	}

	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(table.mainDir()))
	var count int
	if err := queryDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet([%s], union_by_name=true)`, files)).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 25 {
		t.Fatalf("merged row count = %d, want 25", count)
	}
}

// TestImportRowsStreamingPartitioned exercises the iterator API across
// partitions with a small flush threshold.
func TestImportRowsStreamingPartitioned(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[PartitionedRow](db)

	seq := func(yield func(PartitionedRow) bool) {
		for i := 0; i < 6; i++ {
			if !yield(PartitionedRow{ID: fmt.Sprintf("k%d", i), Name: "n", Region: "r", UpdatedAt: int64(i)}) {
				return
			}
		}
	}
	if err := table.ImportRows("stream", seq, ImportOptions{MaxRowsPerFile: 2}); err != nil {
		t.Fatalf("import rows: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	glob := filepath.Join(table.mainDir(), "**", "*.parquet")
	var count int
	if err := queryDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s', union_by_name=true)`, glob)).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 6 {
		t.Fatalf("row count = %d, want 6", count)
	}
}

// TestImportWriterBoundsOpenPartitions verifies that capping open partition
// writers (forcing eviction flushes) loses no data.
func TestImportWriterBoundsOpenPartitions(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[PartitionedRow](db)

	w := table.NewImportWriter("stream", ImportOptions{MaxRowsPerFile: 1000, MaxOpenPartitionWriters: 1})
	ids := []string{"aa1", "bb1", "cc1", "aa2", "dd1"}
	for _, id := range ids {
		if err := w.Add(PartitionedRow{ID: id, Name: "n", Region: "r", UpdatedAt: 1}); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	glob := filepath.Join(table.mainDir(), "**", "*.parquet")
	var count int
	if err := queryDB.QueryRow(fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s', union_by_name=true)`, glob)).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != len(ids) {
		t.Fatalf("row count = %d, want %d", count, len(ids))
	}
}

// bloomFilterColumns counts the parquet column chunks under dir that carry a
// bloom filter on the given column.
func bloomFilterColumns(t *testing.T, dir, column string) int {
	t.Helper()
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	glob := filepath.Join(dir, "**", "*.parquet")
	var n int
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT count(*) FROM parquet_metadata('%s') WHERE path_in_schema = '%s' AND bloom_filter_offset IS NOT NULL`, glob, column,
	)).Scan(&n); err != nil {
		t.Fatalf("bloom metadata query: %v", err)
	}
	return n
}

// TestBloomRewriteDefaultPresent confirms the bloom rewrite still runs by
// default for small outputs.
func TestBloomRewriteDefaultPresent(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	if err := table.Import("b", []MergeRow{{ID: "x", Total: 1}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if n := bloomFilterColumns(t, table.mainDir(), "id"); n == 0 {
		t.Fatal("expected a bloom filter on id by default")
	}
}

// TestBloomRewriteCanBeDisabled verifies DisableBloom omits the filter while
// data stays correct.
func TestBloomRewriteCanBeDisabled(t *testing.T) {
	db, err := Open(t.TempDir(), WithCompactOptions(CompactOptions{DisableBloom: true}))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	if err := table.Import("b", []MergeRow{{ID: "x", Total: 7}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if n := bloomFilterColumns(t, table.mainDir(), "id"); n != 0 {
		t.Fatalf("bloom filter present despite DisableBloom: %d chunks", n)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 7 {
		t.Fatalf("data wrong after disabling bloom: total=%d, want 7", got)
	}
}

// TestBloomRewriteSkippedForLargeOutput verifies the conservative size cap skips
// the rewrite (here forced with a 1-byte cap) without affecting data.
func TestBloomRewriteSkippedForLargeOutput(t *testing.T) {
	db, err := Open(t.TempDir(), WithCompactOptions(CompactOptions{BloomMaxFileBytes: 1}))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)
	if err := table.Import("b", []MergeRow{{ID: "x", Total: 3}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if n := bloomFilterColumns(t, table.mainDir(), "id"); n != 0 {
		t.Fatalf("bloom filter present despite size cap: %d chunks", n)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 3 {
		t.Fatalf("data wrong after skipping bloom: total=%d, want 3", got)
	}
}
