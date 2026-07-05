package fold

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// Fold tracks the active state of a partition directory with two small JSON
// files, so a crash or retry can never lose data or re-apply an inc batch:
//
//	_manifest.json    the current read set: which parquet files are active now
//	_commit_<tx>.json the inputs consumed and outputs produced by that commit
//
// All of this metadata is read and written through a Storage, and the manifest
// write is the single commit point. Reads use the manifest's active file list
// rather than a directory glob.
//
// Publish outputs are written under a files/ subdirectory; the top level of a
// partition directory holds only metadata (and any pre-manifest "legacy" data
// being migrated). That separation is what lets recovery tell a genuine
// pre-existing file (top level) apart from an output left behind by a publish
// that crashed before committing (under files/, never adopted until a manifest
// references it).
const (
	manifestName = "_manifest.json"
	commitPrefix = "_commit_"
	commitSuffix = ".json"
	filesSubdir  = "files"
)

// partitionManifest is the source of truth for a partition's active main files.
// It stays small by design: it answers "which files are active now?" and points
// at the commit that produced them. It does not keep historical batch state.
type partitionManifest struct {
	Version     int      `json:"version"`
	ActiveFiles []string `json:"active_files"` // paths relative to the partition dir
	LastCommit  string   `json:"last_commit"`  // tx id of the commit that produced ActiveFiles
}

// commitRecord records the inc inputs consumed and outputs produced by one
// commit, so a retry after a crash neither re-applies consumed inc files nor
// reads a half-published file set. One record is kept per partition (the last
// commit); older records are garbage-collected after a successful commit.
type commitRecord struct {
	TxID        string   `json:"tx_id"`
	InputFiles  []string `json:"input_files"`  // absolute inc paths consumed by this commit
	OutputFiles []string `json:"output_files"` // paths relative to the partition dir
}

func manifestPath(dir string) string     { return filepath.Join(dir, manifestName) }
func commitPath(dir, txID string) string { return filepath.Join(dir, commitPrefix+txID+commitSuffix) }

// readManifest loads a partition manifest, returning (nil, nil) when absent.
func readManifest(st Storage, dir string) (*partitionManifest, error) {
	var m partitionManifest
	err := st.ReadJSON(manifestPath(dir), &m)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read manifest %s: %w", dir, err)
	}
	return &m, nil
}

// readCommit loads a commit record by tx id, returning (nil, nil) when absent.
func readCommit(st Storage, dir, txID string) (*commitRecord, error) {
	if txID == "" {
		return nil, nil
	}
	var c commitRecord
	err := st.ReadJSON(commitPath(dir, txID), &c)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read commit %s: %w", txID, err)
	}
	return &c, nil
}

// writeJSONAtomic writes v as JSON to a temp file, fsyncs it, then renames it
// into place. It is the local filesystem's atomic write primitive (see
// localStorage.WriteJSON). The fsync makes the content durable before the
// rename can expose it: without it, a power loss can leave a visible but
// truncated manifest as the partition's commit point — a process crash alone
// never does, but "crash-safe" should not stop at process crashes.
func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	// Make the rename itself durable before the caller acts on it. This
	// matters most for the manifest commit: consumed inc files are deleted
	// right after, and if power loss rolled the rename back afterwards, the
	// old manifest would resurrect while the data's only other copy (the inc
	// files) is already gone. Best-effort because directory fsync is
	// unsupported on some platforms (e.g. Windows); the content fsync above
	// is the consistency-critical one.
	syncDir(filepath.Dir(path))
	return nil
}

// syncDir best-effort fsyncs a directory so completed renames in it survive
// power loss. See writeJSONAtomic for why errors are ignored.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// activeMainFiles returns the absolute paths of a partition's active main files,
// always from the manifest. recoverPartition installs a manifest before any
// publish, so a directory reaching a publish has one; an output left behind by
// an interrupted publish is therefore never adopted here — only a committed
// manifest makes a file active.
func activeMainFiles(st Storage, dir string) ([]string, error) {
	m, err := readManifest(st, dir)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, nil
	}
	files := make([]string, 0, len(m.ActiveFiles))
	for _, name := range m.ActiveFiles {
		files = append(files, filepath.Join(dir, name))
	}
	return files, nil
}

// recoverPartition completes any cleanup that an interrupted publish skipped and
// guarantees a manifest exists. It is a no-op on a clean directory, and is run
// before every publish — merge and upsert alike.
func recoverPartition(st Storage, dir string) error {
	m, err := readManifest(st, dir)
	if err != nil {
		return err
	}
	if m == nil {
		// First publish, or migration of a pre-manifest directory: adopt only the
		// legacy parquet files at the top level as the initial active set and
		// install a manifest. Publish outputs live under files/, so this runs
		// before any new output is adoptable: an output left by a later publish
		// that crashes before committing is under files/ and is never mistaken
		// for active data — the retry finds the manifest and cleans it instead.
		legacy, err := legacyTopLevelFiles(st, dir)
		if err != nil {
			return err
		}
		return st.WriteJSON(manifestPath(dir), partitionManifest{ActiveFiles: legacy}, WriteOptions{})
	}
	if c, err := readCommit(st, dir, m.LastCommit); err != nil {
		return err
	} else if c != nil {
		for _, f := range c.InputFiles {
			// Removal must succeed before a new publish may advance last_commit.
			// The commit record is the only record of these consumed inputs; if
			// it were GC'd while the inc files survived, a later merge would
			// re-apply them. Consumed inc always lives in the local workspace,
			// never behind a remote Storage, so it is removed locally. Delete
			// already tolerates an absent file.
			if err := (localStorage{}).Delete(f); err != nil {
				return fmt.Errorf("recover consumed inc %s: %w", f, err)
			}
		}
	}
	return finalizeDir(st, dir, m.ActiveFiles, m.LastCommit)
}

// commitActive atomically publishes newActive (paths relative to dir) as the
// partition's active file set and records the consumed inc inputs. Writing the
// commit record first and then writing the manifest makes the manifest write the
// single commit point: a crash before it leaves the old state authoritative (the
// new output is an ignored orphan under files/), a crash after it leaves the new
// state authoritative (the consumed inc are recorded so they are never
// re-applied). Superseded/orphaned files, consumed inc, and stale commit records
// are then garbage-collected.
func commitActive(st Storage, dir string, newActive, consumedInc []string) error {
	prev, err := readManifest(st, dir)
	if err != nil {
		return err
	}
	version := 1
	if prev != nil {
		version = prev.Version + 1
	}
	txID := uuid.New().String()

	rec := commitRecord{TxID: txID, InputFiles: consumedInc, OutputFiles: newActive}
	if err := st.WriteJSON(commitPath(dir, txID), rec, WriteOptions{}); err != nil {
		return fmt.Errorf("write commit record: %w", err)
	}

	m := partitionManifest{Version: version, ActiveFiles: newActive, LastCommit: txID}
	if err := st.WriteJSON(manifestPath(dir), m, WriteOptions{}); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Post-commit GC. Interruption here is safe: recovery tolerates leftovers.
	if err := finalizeDir(st, dir, newActive, txID); err != nil {
		return err
	}
	// Consumed inc lives in the local workspace, never behind a remote Storage.
	for _, f := range consumedInc {
		localStorage{}.Delete(f)
	}
	return nil
}

// finalizeDir removes everything in a published partition directory that is not
// part of the current commit: parquet files no longer active (superseded legacy
// files, or outputs orphaned by an interrupted publish), stray staging temp
// files, and commit records other than keepTx. active holds paths relative to
// dir. Listing failure is fatal; individual deletes are best-effort (recovery
// retries leftovers).
func finalizeDir(st Storage, dir string, active []string, keepTx string) error {
	keep := make(map[string]bool, len(active))
	for _, a := range active {
		keep[a] = true
	}
	keepCommit := commitPrefix + keepTx + commitSuffix
	objs, err := st.List(dir)
	if err != nil {
		return fmt.Errorf("list %s: %w", dir, err)
	}
	for _, o := range objs {
		rel, relErr := filepath.Rel(dir, o.Path)
		if relErr != nil {
			continue
		}
		name := filepath.Base(rel)
		switch {
		case strings.HasSuffix(name, ".tmp"):
			st.Delete(o.Path)
		case strings.HasSuffix(name, ".parquet") && !keep[rel]:
			st.Delete(o.Path)
		case filepath.Dir(rel) == "." && strings.HasPrefix(name, commitPrefix) && strings.HasSuffix(name, commitSuffix) && name != keepCommit:
			st.Delete(o.Path)
		}
	}
	return nil
}

// legacyTopLevelFiles lists parquet files directly under dir (not under files/),
// i.e. data written by a pre-manifest version. Publish outputs live under
// files/, so they are never mistaken for legacy data here.
func legacyTopLevelFiles(st Storage, dir string) ([]string, error) {
	objs, err := st.List(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, o := range objs {
		rel, relErr := filepath.Rel(dir, o.Path)
		if relErr != nil || filepath.Dir(rel) != "." {
			continue // skip nested (files/, metadata) entries
		}
		name := filepath.Base(rel)
		if !strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".parquet") {
			names = append(names, name)
		}
	}
	return names, nil
}
