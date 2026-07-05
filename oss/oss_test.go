package oss

import (
	"path/filepath"
	"testing"
)

func TestKeyMapping(t *testing.T) {
	st := NewFromClient(nil, "bucket", "/data/fold", "prod/fold")

	cases := []struct {
		path string
		key  string
	}{
		{"/data/fold/main/company/_manifest.json", "prod/fold/main/company/_manifest.json"},
		{"/data/fold/main/company/b=0a/files/merged_1.parquet", "prod/fold/main/company/b=0a/files/merged_1.parquet"},
		{"/data/fold", "prod/fold"},
	}
	for _, c := range cases {
		key, err := st.keyFor(c.path)
		if err != nil {
			t.Fatalf("keyFor(%q): %v", c.path, err)
		}
		if key != c.key {
			t.Fatalf("keyFor(%q) = %q, want %q", c.path, key, c.key)
		}
	}

	if got := st.pathFor("prod/fold/main/company/_manifest.json"); got != filepath.FromSlash("/data/fold/main/company/_manifest.json") {
		t.Fatalf("pathFor round trip = %q", got)
	}
}

func TestKeyMappingNoPrefix(t *testing.T) {
	st := NewFromClient(nil, "bucket", "/data/fold", "")
	key, err := st.keyFor("/data/fold/main/t/_manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if key != "main/t/_manifest.json" {
		t.Fatalf("key = %q", key)
	}
	if got := st.pathFor("main/t/_manifest.json"); got != filepath.FromSlash("/data/fold/main/t/_manifest.json") {
		t.Fatalf("pathFor = %q", got)
	}
}

// TestPathForPrefixNotGreedy pins the prefix strip to whole path segments: a
// key that merely starts with the prefix string ("folder/x" vs prefix "fold")
// must not lose characters.
func TestPathForPrefixNotGreedy(t *testing.T) {
	st := NewFromClient(nil, "bucket", "/data/fold", "fold")
	if got := st.pathFor("fold/main/t/x.parquet"); got != filepath.FromSlash("/data/fold/main/t/x.parquet") {
		t.Fatalf("prefixed key mapped to %q", got)
	}
	if got := st.pathFor("folder/main/t/x.parquet"); got != filepath.FromSlash("/data/fold/folder/main/t/x.parquet") {
		t.Fatalf("sibling key mangled: %q", got)
	}
	if got := st.pathFor("fold"); got != filepath.FromSlash("/data/fold") {
		t.Fatalf("prefix root mapped to %q", got)
	}
}

func TestKeyForRejectsOutsideRoot(t *testing.T) {
	st := NewFromClient(nil, "bucket", "/data/fold", "")
	if _, err := st.keyFor("/etc/passwd"); err == nil {
		t.Fatal("expected error for path outside root")
	}
	if _, err := st.keyFor("/data/fold/../other"); err == nil {
		t.Fatal("expected error for escaping path")
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(Config{LocalRoot: "./data"}); err == nil {
		t.Fatal("expected error for missing bucket")
	}
	if _, err := New(Config{Bucket: "b"}); err == nil {
		t.Fatal("expected error for missing local root")
	}
}
