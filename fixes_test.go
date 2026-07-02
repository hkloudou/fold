package fold

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xuri/excelize/v2"
)

// AreaRow partitions directly on a caller-supplied string value, the case
// where dirty input used to escape the table directory or strand data.
type AreaRow struct {
	ID    string `bd:"pk"`
	Area  string `bd:"partition:area"`
	Total int64  `bd:"sum"`
}

// TestPartitionValueSanitized proves a partition value containing path
// separators can neither write outside the table's inc/ area nor create
// nesting that discoverPartitions misses: everything stays under one
// key=value directory, is discovered, merged, and queryable.
func TestPartitionValueSanitized(t *testing.T) {
	root := t.TempDir()
	db, err := Open(root)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[AreaRow](db)

	dirty := []AreaRow{
		{ID: "a", Area: "../../../../escape", Total: 1},
		{ID: "b", Area: "hua/dong", Total: 2},
		{ID: "c", Area: `back\slash`, Total: 3},
		{ID: "d", Area: "pct%20", Total: 4},
		{ID: "e", Area: "华东", Total: 5}, // clean value keeps its literal directory name
	}
	if err := table.Import("s", dirty); err != nil {
		t.Fatalf("import: %v", err)
	}

	// Nothing may exist outside the data root's inc/main areas.
	var strays []string
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		if !strings.HasPrefix(rel, "inc"+string(filepath.Separator)) &&
			!strings.HasPrefix(rel, "main"+string(filepath.Separator)) {
			strays = append(strays, rel)
		}
		return nil
	})
	if len(strays) > 0 {
		t.Fatalf("files written outside inc/ and main/: %v", strays)
	}

	// Every inc file must sit exactly one area=... directory under the source,
	// i.e. no separator survived into the partition value.
	incFiles := listParquetFiles(table.incDir())
	if len(incFiles) != len(dirty) {
		t.Fatalf("inc files = %d, want %d", len(incFiles), len(dirty))
	}
	for _, f := range incFiles {
		rel, _ := filepath.Rel(table.incDir(), f)
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) != 3 || !strings.HasPrefix(parts[1], "area=") {
			t.Fatalf("unexpected inc layout for dirty partition value: %s", rel)
		}
	}

	// The clean value must keep its literal directory name.
	if _, err := os.Stat(filepath.Join(table.incDir(), "s", "area=华东")); err != nil {
		t.Fatalf("clean partition value did not keep its literal directory: %v", err)
	}

	// All partitions must be discovered and merged — dirty values used to
	// strand their data unmerged.
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if files := listParquetFiles(table.incDir()); len(files) != 0 {
		t.Fatalf("dirty partition values stranded inc data: %v", files)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	files := buildFileList(listParquetFiles(table.mainDir()))
	var n, sum int64
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT count(*), sum(total) FROM read_parquet([%s], union_by_name=true)`, files,
	)).Scan(&n, &sum); err != nil {
		t.Fatalf("query merged: %v", err)
	}
	if n != 5 || sum != 15 {
		t.Fatalf("merged rows = %d (sum %d), want 5 (sum 15)", n, sum)
	}
}

func TestEncodePartitionValue(t *testing.T) {
	cases := map[string]string{
		"clean-Value_01": "clean-Value_01",
		"华东":             "华东",
		"a/b":            "a%2Fb",
		`a\b`:            "a%5Cb",
		"100%":           "100%25",
		"../../etc":      "..%2F..%2Fetc",
		"a\nb":           "a%0Ab",
		"":               "",
	}
	for in, want := range cases {
		if got := encodePartitionValue(in); got != want {
			t.Fatalf("encodePartitionValue(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestIncWriteIsAtomic covers the staged inc write: a leftover .parquet.tmp
// (a simulated crash mid-write) must be invisible to merge, and a successful
// import must leave no temp files behind.
func TestIncWriteIsAtomic(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("s", []MergeRow{{ID: "x", Total: 5}}); err != nil {
		t.Fatalf("import: %v", err)
	}

	// No staging leftovers after a successful import.
	filepath.Walk(table.incDir(), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".tmp") {
			t.Fatalf("staging temp left after successful import: %s", p)
		}
		return nil
	})

	// Inject two crashed writes: a stale one (its writer is long dead) and a
	// fresh one (could belong to a live ImportWriter). Merge must ignore both
	// as data, garbage-collect only the stale one, and keep the fresh one.
	srcDir := filepath.Dir(listParquetFiles(table.incDir())[0])
	stale := filepath.Join(srcDir, "stale.parquet.tmp")
	fresh := filepath.Join(srcDir, "fresh.parquet.tmp")
	for _, p := range []string{stale, fresh} {
		if err := os.WriteFile(p, []byte("not parquet"), 0644); err != nil {
			t.Fatalf("inject crashed write: %v", err)
		}
	}
	old := time.Now().Add(-2 * staleIncTmpAge)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("age stale tmp: %v", err)
	}

	if err := table.Merge(); err != nil {
		t.Fatalf("merge must ignore truncated staged writes: %v", err)
	}
	if got := queryTotal(t, table.mainDir(), "x"); got != 5 {
		t.Fatalf("total = %d, want 5", got)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale crashed write was not garbage-collected by merge")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("fresh staged write of a possibly-live writer was removed: %v", err)
	}
}

// TestConcurrentUpsertsDoNotLoseUpdates drives parallel Upserts at one key:
// without per-partition locking, interleaved read-manifest/commit cycles
// dropped increments.
func TestConcurrentUpsertsDoNotLoseUpdates(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- table.Upsert("u", []RawRecord{{"id": "x", "total": int64(1)}})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent upsert: %v", err)
		}
	}

	if got := queryTotal(t, table.mainDir(), "x"); got != workers {
		t.Fatalf("concurrent upserts lost updates: total = %d, want %d", got, workers)
	}
}

// TestConcurrentMergeAndUpsert interleaves a Merge (consuming staged inc)
// with direct Upserts on the same unpartitioned table.
func TestConcurrentMergeAndUpsert(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("s", []MergeRow{{ID: "x", Total: 10}}); err != nil {
		t.Fatalf("import: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- table.Merge()
	}()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- table.Upsert("u", []RawRecord{{"id": "x", "total": int64(1)}})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent merge/upsert: %v", err)
		}
	}

	if got := queryTotal(t, table.mainDir(), "x"); got != 13 {
		t.Fatalf("concurrent merge+upsert lost updates: total = %d, want 13", got)
	}
}

// TestReadersNilOptions ensures nil options degrade to errors or empty
// results instead of panicking. The Excel file must actually exist so the
// nil opt is dereferenced past the open call.
func TestReadersNilOptions(t *testing.T) {
	jsonl := filepath.Join(t.TempDir(), "in.jsonl")
	if err := os.WriteFile(jsonl, []byte(`{"id":"1"}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	recs, err := ReadJSONL(jsonl, nil)
	if err != nil {
		t.Fatalf("ReadJSONL(nil opts): %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("no field mapping configured, want 0 records, got %d", len(recs))
	}

	xlsx := filepath.Join(t.TempDir(), "in.xlsx")
	xf := excelize.NewFile()
	if err := xf.SetSheetRow("Sheet1", "A1", &[]any{"ID", "Name"}); err != nil {
		t.Fatal(err)
	}
	if err := xf.SaveAs(xlsx); err != nil {
		t.Fatal(err)
	}
	xf.Close()
	// Without the nil guard this dereferenced opt.Sheet and panicked; with it,
	// the zero Header is reported as an ordinary error.
	if _, err := ReadExcel(xlsx, nil); err == nil {
		t.Fatal("ReadExcel(nil opts): want header-row error, got nil")
	}
}

// TestImportSourceSanitized proves a source containing path separators can
// neither escape the data root nor strand a partitioned batch outside the
// single source level that merge collects from.
func TestImportSourceSanitized(t *testing.T) {
	root := t.TempDir()
	db, err := Open(root)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[AreaRow](db)

	// Date-derived source with separators, plus a traversal attempt and an
	// empty source: all must land exactly one directory under inc/<table>/.
	for _, src := range []string{"2026/07/02", "../../evil", ""} {
		if err := table.Import(src, []AreaRow{{ID: "id-" + src, Area: "east", Total: 1}}); err != nil {
			t.Fatalf("import %q: %v", src, err)
		}
	}

	var strays []string
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		if !strings.HasPrefix(rel, "inc"+string(filepath.Separator)) {
			strays = append(strays, rel)
		}
		return nil
	})
	if len(strays) > 0 {
		t.Fatalf("files written outside inc/: %v", strays)
	}
	for _, f := range listParquetFiles(table.incDir()) {
		rel, _ := filepath.Rel(table.incDir(), f)
		if parts := strings.Split(rel, string(filepath.Separator)); len(parts) != 3 {
			t.Fatalf("source produced nesting outside <source>/<partition>/: %s", rel)
		}
	}

	// The batches must actually merge: pre-fix, a nested source was silently
	// stranded (merge reported success but consumed nothing).
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if files := listParquetFiles(table.incDir()); len(files) != 0 {
		t.Fatalf("nested source stranded inc data: %v", files)
	}

	queryDB := openQueryDB(t)
	defer queryDB.Close()
	var n int64
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT count(*) FROM read_parquet([%s], union_by_name=true)`,
		buildFileList(listParquetFiles(table.mainDir())),
	)).Scan(&n); err != nil {
		t.Fatalf("query merged: %v", err)
	}
	if n != 3 {
		t.Fatalf("merged rows = %d, want 3", n)
	}
}
