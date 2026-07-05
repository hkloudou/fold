package fold

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
)

// recordingLogger captures Printf calls for assertions.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

// TestWithLoggerRoutesProgressOutput proves merge progress goes through the
// configured logger and nothing reaches the global logger.
func TestWithLoggerRoutesProgressOutput(t *testing.T) {
	rec := &recordingLogger{}
	db, err := Open(t.TempDir(), WithLogger(rec))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	var global bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&global)
	defer log.SetOutput(prev)

	if err := table.Import("s", []MergeRow{{ID: "x", Total: 1}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	rec.mu.Lock()
	captured := strings.Join(rec.lines, "\n")
	rec.mu.Unlock()
	if !strings.Contains(captured, "merge complete") {
		t.Fatalf("custom logger did not receive merge progress: %q", captured)
	}
	if global.Len() != 0 {
		t.Fatalf("global logger received output despite WithLogger: %q", global.String())
	}
}

// TestWithLoggerNilSilences proves WithLogger(nil) disables logging entirely.
func TestWithLoggerNilSilences(t *testing.T) {
	db, err := Open(t.TempDir(), WithLogger(nil))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	var global bytes.Buffer
	prev := log.Writer()
	log.SetOutput(&global)
	defer log.SetOutput(prev)

	if err := table.Import("s", []MergeRow{{ID: "x", Total: 1}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if err := table.Upsert("u", []RawRecord{{"id": "x", "total": int64(1)}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if global.Len() != 0 {
		t.Fatalf("logging not silenced by WithLogger(nil): %q", global.String())
	}
}

// TestPartitionTagValidation pins parse-time rejection of malformed partition
// rules that previously surfaced only at import time (a reversed slice
// panicked in evalPartition; hash(0) silently degraded to direct-value
// partitioning; hash counts beyond two bytes of entropy were unreachable).
func TestPartitionTagValidation(t *testing.T) {
	if _, err := parsePartitionTag("p=[4:2]", "F"); err == nil {
		t.Fatal("reversed slice [4:2] should be rejected")
	}
	if _, err := parsePartitionTag("p=[2:2]", "F"); err == nil {
		t.Fatal("empty slice [2:2] should be rejected")
	}
	if _, err := parsePartitionTag("b=hash(0)", "F"); err == nil {
		t.Fatal("hash(0) should be rejected")
	}
	if _, err := parsePartitionTag("b=hash(65537)", "F"); err == nil {
		t.Fatal("hash(65537) should be rejected")
	}
	if _, err := parsePartitionTag("p=[0:2],b=hash(65536)", "F"); err != nil {
		t.Fatalf("valid rules rejected: %v", err)
	}
}

// JSONNameRow embeds "json_merge" inside a column name; the int64 storage
// type must survive (a substring match used to coerce it to string).
type JSONNameRow struct {
	ID    string `bd:"pk"`
	Count int64  `bd:"sum;column:json_merge_count"`
}

func TestJSONMergeSubstringColumnKeepsType(t *testing.T) {
	schema, err := parseSchema[JSONNameRow]()
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	for _, f := range schema.Fields {
		if f.Column == "json_merge_count" {
			if f.Type != FieldInt64 {
				t.Fatalf("column embedding json_merge mis-typed as %s", f.Type)
			}
			if f.Strategy != StrategySum {
				t.Fatalf("strategy = %s, want sum", f.Strategy)
			}
			return
		}
	}
	t.Fatal("column json_merge_count not found")
}

// TestRawRecordInt64Coercion aligns the reader helper with Upsert's coercion.
func TestRawRecordInt64Coercion(t *testing.T) {
	rec := RawRecord{"a": int64(5), "b": 6, "c": 7.0, "d": "8", "e": "not-a-number", "f": nil}
	for field, want := range map[string]int64{"a": 5, "b": 6, "c": 7, "d": 8, "e": 0, "f": 0, "missing": 0} {
		if got := rec.Int64(field); got != want {
			t.Fatalf("Int64(%q) = %d, want %d", field, got, want)
		}
	}
}
