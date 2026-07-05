package fold

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Object is a stored item returned by Storage.List.
type Object struct {
	Path string
	Size int64
}

// WriteOptions carries backend-specific hints for WriteJSON. It is reserved for
// object-storage adapters (e.g. conditional writes or content type); the local
// filesystem implementation ignores it.
type WriteOptions struct{}

// Storage is the narrow persistence contract Fold needs: small JSON metadata,
// immutable object movement, listing, and deletion. It deliberately does not
// model streaming bulk I/O — DuckDB reads and writes Parquet in a local
// workspace, and Storage publishes the staged files plus the manifest. The
// local filesystem implementation is the default; an object-storage adapter
// (for example the Aliyun OSS adapter in github.com/hkloudou/fold/oss)
// implements the same contract: stage locally, then UploadFile + WriteJSON,
// swapping the manifest as the commit point. When the storage is not the
// built-in local backend, Fold fetches a partition's active main files into
// the local workspace through DownloadFile before DuckDB reads them.
//
// The inc/ area is always part of the local workspace and is never persisted
// through Storage; only main/ metadata and published outputs go through it.
type Storage interface {
	// List returns every object whose path is under prefix (recursive).
	List(prefix string) ([]Object, error)
	// ReadJSON decodes the JSON object at path into dst. A missing object
	// returns an error satisfying errors.Is(err, fs.ErrNotExist).
	ReadJSON(path string, dst any) error
	// WriteJSON atomically encodes src as JSON at path.
	WriteJSON(path string, src any, opts WriteOptions) error
	// UploadFile publishes a finished local file to dst.
	UploadFile(localPath, dst string) error
	// DownloadFile fetches an object into the local workspace. Reserved for
	// object-storage reads; local reads happen in place.
	DownloadFile(src, localPath string) error
	// Delete removes an object. Deleting a missing object is not an error.
	Delete(path string) error
}

// localStorage implements Storage on the local filesystem, operating directly on
// the paths Fold computes. Writes are atomic (temp file + rename).
type localStorage struct{}

// isLocalStorage reports whether st is the built-in local filesystem backend,
// whose object paths DuckDB can read in place without a download step.
func isLocalStorage(st Storage) bool {
	_, ok := st.(localStorage)
	return ok
}

// localizeMainFiles makes a partition's active main files readable by DuckDB.
// For the local backend the manifest paths already are local files. For any
// other backend the objects are fetched through DownloadFile into a temporary
// fetch directory under dir; the returned cleanup removes it. cleanup is never
// nil and is safe to call on the error path too.
func localizeMainFiles(st Storage, dir string, files []string) ([]string, func(), error) {
	noop := func() {}
	if len(files) == 0 || isLocalStorage(st) {
		return files, noop, nil
	}
	fetchDir := filepath.Join(dir, fmt.Sprintf(".fetch_%d", time.Now().UnixNano()))
	cleanup := func() { os.RemoveAll(fetchDir) }
	local := make([]string, len(files))
	for i, f := range files {
		lp := filepath.Join(fetchDir, fmt.Sprintf("main_%d.parquet", i))
		if err := st.DownloadFile(f, lp); err != nil {
			cleanup()
			return nil, noop, fmt.Errorf("download main file %s: %w", f, err)
		}
		local[i] = lp
	}
	return local, cleanup, nil
}

func (localStorage) List(prefix string) ([]Object, error) {
	var objs []Object
	err := filepath.Walk(prefix, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // a missing prefix lists as empty
			}
			return err
		}
		if !info.IsDir() {
			objs = append(objs, Object{Path: p, Size: info.Size()})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return objs, nil
}

func (localStorage) ReadJSON(path string, dst any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

func (localStorage) WriteJSON(path string, src any, _ WriteOptions) error {
	return writeJSONAtomic(path, src)
}

func (localStorage) UploadFile(localPath, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	// The staged content must be durable before it is published: the manifest
	// commit that follows is fsynced, so after a power loss a durable manifest
	// could otherwise reference a torn segment whose consumed inc inputs are
	// already deleted.
	if err := syncFile(localPath); err != nil {
		return err
	}
	if err := os.Rename(localPath, dst); err == nil {
		syncDir(filepath.Dir(dst))
		return nil
	}
	// Fall back to copy for cross-device moves.
	if err := copyFile(localPath, dst); err != nil {
		return err
	}
	syncDir(filepath.Dir(dst))
	return os.Remove(localPath)
}

// syncFile fsyncs an existing file's content. It opens read-write because
// flushing a read-only handle is not permitted on all platforms.
func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (localStorage) DownloadFile(src, localPath string) error {
	return copyFile(src, localPath)
}

func (localStorage) Delete(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	return out.Close()
}
