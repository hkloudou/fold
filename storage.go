package fold

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
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
// local filesystem implementation is the default; an object-storage adapter can
// implement the same contract (stage locally, then UploadFile + WriteJSON,
// swapping the manifest as the commit point).
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
	if err := os.Rename(localPath, dst); err == nil {
		return nil
	}
	// Fall back to copy for cross-device moves.
	if err := copyFile(localPath, dst); err != nil {
		return err
	}
	return os.Remove(localPath)
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
	return out.Close()
}
