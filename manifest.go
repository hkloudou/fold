package fold

import (
	"encoding/json"
	"fmt"
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
// The manifest swap (an atomic rename) is the single commit point. Reads use
// the manifest's active file list rather than a directory glob.
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
func readManifest(dir string) (*partitionManifest, error) {
	b, err := os.ReadFile(manifestPath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m partitionManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", dir, err)
	}
	return &m, nil
}

// readCommit loads a commit record by tx id, returning (nil, nil) when absent.
func readCommit(dir, txID string) (*commitRecord, error) {
	if txID == "" {
		return nil, nil
	}
	b, err := os.ReadFile(commitPath(dir, txID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var c commitRecord
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse commit %s: %w", txID, err)
	}
	return &c, nil
}

// writeJSONAtomic writes v as JSON to a temp file then renames it into place.
func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// activeMainFiles returns the absolute paths of a partition's active main files,
// always from the manifest. recoverPartition installs a manifest before any
// publish, so a directory reaching a publish has one; an output left behind by
// an interrupted publish is therefore never adopted here — only a committed
// manifest makes a file active.
func activeMainFiles(dir string) ([]string, error) {
	m, err := readManifest(dir)
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
func recoverPartition(dir string) error {
	m, err := readManifest(dir)
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
		return writeJSONAtomic(manifestPath(dir), partitionManifest{ActiveFiles: legacyTopLevelFiles(dir)})
	}
	if c, err := readCommit(dir, m.LastCommit); err != nil {
		return err
	} else if c != nil {
		for _, f := range c.InputFiles {
			// Removal must succeed before a new publish may advance last_commit.
			// The commit record is the only record of these consumed inputs; if
			// it were GC'd while the inc files survived, a later merge would
			// re-apply them. A file that is already gone is fine.
			if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("recover consumed inc %s: %w", f, err)
			}
		}
	}
	finalizeDir(dir, m.ActiveFiles, m.LastCommit)
	return nil
}

// commitActive atomically publishes newActive (paths relative to dir) as the
// partition's active file set and records the consumed inc inputs. Writing the
// commit record first and then renaming the manifest makes the manifest swap the
// single commit point: a crash before it leaves the old state authoritative (the
// new output is an ignored orphan under files/), a crash after it leaves the new
// state authoritative (the consumed inc are recorded so they are never
// re-applied). Superseded/orphaned files, consumed inc, and stale commit records
// are then garbage-collected.
func commitActive(dir string, newActive, consumedInc []string) error {
	prev, err := readManifest(dir)
	if err != nil {
		return err
	}
	version := 1
	if prev != nil {
		version = prev.Version + 1
	}
	txID := uuid.New().String()

	rec := commitRecord{TxID: txID, InputFiles: consumedInc, OutputFiles: newActive}
	if err := writeJSONAtomic(commitPath(dir, txID), rec); err != nil {
		return fmt.Errorf("write commit record: %w", err)
	}

	m := partitionManifest{Version: version, ActiveFiles: newActive, LastCommit: txID}
	if err := writeJSONAtomic(manifestPath(dir), m); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Post-commit GC. Interruption here is safe: recovery tolerates leftovers.
	finalizeDir(dir, newActive, txID)
	for _, f := range consumedInc {
		os.Remove(f)
	}
	return nil
}

// finalizeDir removes everything in a published partition directory that is not
// part of the current commit: parquet files no longer active (superseded
// legacy files, or outputs orphaned by an interrupted publish), stray staging
// temp files, and commit records other than keepTx. active holds paths relative
// to dir.
func finalizeDir(dir string, active []string, keepTx string) {
	keep := make(map[string]bool, len(active))
	for _, a := range active {
		keep[a] = true
	}
	keepCommit := commitPrefix + keepTx + commitSuffix
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		name := info.Name()
		rel, relErr := filepath.Rel(dir, path)
		if relErr != nil {
			return nil
		}
		switch {
		case strings.HasSuffix(name, ".tmp"):
			os.Remove(path)
		case strings.HasSuffix(name, ".parquet") && !keep[rel]:
			os.Remove(path)
		case filepath.Dir(rel) == "." && strings.HasPrefix(name, commitPrefix) && strings.HasSuffix(name, commitSuffix) && name != keepCommit:
			os.Remove(path)
		}
		return nil
	})
}

// legacyTopLevelFiles lists parquet files directly under dir (not under files/),
// i.e. data written by a pre-manifest version. Publish outputs live under
// files/, so they are never mistaken for legacy data here.
func legacyTopLevelFiles(dir string) []string {
	var names []string
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") && strings.HasSuffix(e.Name(), ".parquet") {
			names = append(names, e.Name())
		}
	}
	return names
}
