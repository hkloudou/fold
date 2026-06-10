package fold

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// memRemoteStorage is an in-memory object store standing in for a remote
// backend such as Aliyun OSS: objects live in a map, never on disk, so any
// attempt to read a published file without going through DownloadFile fails.
type memRemoteStorage struct {
	mu        sync.Mutex
	objects   map[string][]byte
	downloads int
}

func newMemRemoteStorage() *memRemoteStorage {
	return &memRemoteStorage{objects: map[string][]byte{}}
}

func (m *memRemoteStorage) List(prefix string) ([]Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var objs []Object
	sep := string(filepath.Separator)
	for p, b := range m.objects {
		if strings.HasPrefix(p, strings.TrimSuffix(prefix, sep)+sep) {
			objs = append(objs, Object{Path: p, Size: int64(len(b))})
		}
	}
	return objs, nil
}

func (m *memRemoteStorage) ReadJSON(path string, dst any) error {
	m.mu.Lock()
	b, ok := m.objects[path]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("remote: %s: %w", path, fs.ErrNotExist)
	}
	return json.Unmarshal(b, dst)
}

func (m *memRemoteStorage) WriteJSON(path string, src any, _ WriteOptions) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[path] = b
	m.mu.Unlock()
	return nil
}

func (m *memRemoteStorage) UploadFile(localPath, dst string) error {
	b, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[dst] = b
	m.mu.Unlock()
	return os.Remove(localPath)
}

func (m *memRemoteStorage) DownloadFile(src, localPath string) error {
	m.mu.Lock()
	b, ok := m.objects[src]
	m.downloads++
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("remote: %s: %w", src, fs.ErrNotExist)
	}
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return err
	}
	return os.WriteFile(localPath, b, 0644)
}

func (m *memRemoteStorage) Delete(path string) error {
	m.mu.Lock()
	delete(m.objects, path)
	m.mu.Unlock()
	return nil
}

// activeRemoteFiles resolves the manifest's active set through the remote
// storage and downloads the files to a local dir for verification.
func fetchActiveRemote(t *testing.T, st Storage, mainDir string) []string {
	t.Helper()
	m, err := readManifest(st, mainDir)
	if err != nil || m == nil {
		t.Fatalf("read remote manifest: %v (m=%v)", err, m)
	}
	outDir := t.TempDir()
	var local []string
	for i, rel := range m.ActiveFiles {
		lp := filepath.Join(outDir, fmt.Sprintf("active_%d.parquet", i))
		if err := st.DownloadFile(filepath.Join(mainDir, rel), lp); err != nil {
			t.Fatalf("download active %s: %v", rel, err)
		}
		local = append(local, lp)
	}
	return local
}

// TestMergeWithRemoteStorage proves the full merge cycle works when main/
// lives behind a remote (OSS-style) backend: the first merge publishes
// through UploadFile, the second merge reads the previous segment through
// DownloadFile, strategies apply once (sum is not double-applied), and no
// published parquet is left on the local disk.
func TestMergeWithRemoteStorage(t *testing.T) {
	remote := newMemRemoteStorage()
	db, err := Open(t.TempDir(), WithStorage(remote))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("a", []MergeRow{{ID: "x", Name: "first", Total: 5}}); err != nil {
		t.Fatalf("import 1: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge 1: %v", err)
	}

	if err := table.Import("a", []MergeRow{{ID: "x", Total: 2}}); err != nil {
		t.Fatalf("import 2: %v", err)
	}
	if err := table.Merge(); err != nil {
		t.Fatalf("merge 2: %v", err)
	}
	if remote.downloads == 0 {
		t.Fatal("second merge did not localize main files through DownloadFile")
	}

	// Published data must exist only remotely.
	if files := listParquetFiles(table.mainDir()); len(files) != 0 {
		t.Fatalf("published parquet left on local disk: %v", files)
	}
	// Consumed inc must be removed locally, not via remote Delete.
	if files := listParquetFiles(table.incDir()); len(files) != 0 {
		t.Fatalf("consumed inc not cleaned locally: %v", files)
	}

	local := fetchActiveRemote(t, remote, table.mainDir())
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	var name string
	var total int64
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT name, total FROM read_parquet([%s], union_by_name=true) WHERE id='x'`, buildFileList(local),
	)).Scan(&name, &total); err != nil {
		t.Fatalf("query merged remote data: %v", err)
	}
	if name != "first" || total != 7 {
		t.Fatalf("merged remote row = (%q, %d), want (\"first\", 7)", name, total)
	}
}

// TestUpsertWithRemoteStorage covers the direct-upsert path against a remote
// backend, including a partitioned table.
func TestUpsertWithRemoteStorage(t *testing.T) {
	remote := newMemRemoteStorage()
	db, err := Open(t.TempDir(), WithStorage(remote))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Upsert("u", []RawRecord{{"id": "x", "total": int64(3)}}); err != nil {
		t.Fatalf("upsert 1: %v", err)
	}
	if err := table.Upsert("u", []RawRecord{{"id": "x", "total": int64(4)}}); err != nil {
		t.Fatalf("upsert 2: %v", err)
	}
	if remote.downloads == 0 {
		t.Fatal("second upsert did not localize main files through DownloadFile")
	}

	local := fetchActiveRemote(t, remote, table.mainDir())
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	var total int64
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT total FROM read_parquet([%s], union_by_name=true) WHERE id='x'`, buildFileList(local),
	)).Scan(&total); err != nil {
		t.Fatalf("query upserted remote data: %v", err)
	}
	if total != 7 {
		t.Fatalf("remote upsert total = %d, want 7", total)
	}
}

// TestMergeRetryIdempotentWithRemoteStorage replays a crash between commit and
// inc cleanup while main/ is remote: the surviving inc files must not be
// re-applied by the retry.
func TestMergeRetryIdempotentWithRemoteStorage(t *testing.T) {
	remote := newMemRemoteStorage()
	db, err := Open(t.TempDir(), WithStorage(remote))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	table := Register[MergeRow](db)

	if err := table.Import("a", []MergeRow{{ID: "x", Total: 5}}); err != nil {
		t.Fatalf("import: %v", err)
	}
	incBackup := snapshotFiles(t, listParquetFiles(table.incDir()))
	if err := table.Merge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	// Simulate the crash: consumed inc reappears, then the merge is retried.
	restoreFiles(t, incBackup)
	if err := table.Merge(); err != nil {
		t.Fatalf("retry merge: %v", err)
	}

	local := fetchActiveRemote(t, remote, table.mainDir())
	queryDB := openQueryDB(t)
	defer queryDB.Close()
	var total int64
	if err := queryDB.QueryRow(fmt.Sprintf(
		`SELECT total FROM read_parquet([%s], union_by_name=true) WHERE id='x'`, buildFileList(local),
	)).Scan(&total); err != nil {
		t.Fatalf("query: %v", err)
	}
	if total != 5 {
		t.Fatalf("retry double-applied inc against remote storage: total = %d, want 5", total)
	}
}
