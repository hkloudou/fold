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
//	_manifest.json   the current read set: which parquet files are active now
//	_commit_<tx>.json the inputs consumed and outputs produced by that commit
//
// The manifest swap (an atomic rename) is the single commit point. Reads use
// the manifest's active file list rather than a directory glob, so files left
// behind by an interrupted publish are ignored and later garbage-collected.
const (
	manifestName = "_manifest.json"
	commitPrefix = "_commit_"
	commitSuffix = ".json"
)

// partitionManifest is the source of truth for a partition's active main files.
// It stays small by design: it answers "which files are active now?" and points
// at the commit that produced them. It does not keep historical batch state.
type partitionManifest struct {
	Version     int      `json:"version"`
	ActiveFiles []string `json:"active_files"` // basenames within the partition dir
	LastCommit  string   `json:"last_commit"`  // tx id of the commit that produced ActiveFiles
}

// commitRecord records the inc inputs consumed and outputs produced by one
// commit, so a retry after a crash neither re-applies consumed inc files nor
// reads a half-published file set. One record is kept per partition (the last
// commit); older records are garbage-collected after a successful commit.
type commitRecord struct {
	TxID        string   `json:"tx_id"`
	InputFiles  []string `json:"input_files"`  // absolute inc paths consumed by this commit
	OutputFiles []string `json:"output_files"` // basenames published into the partition dir
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

// activeMainFiles returns the absolute paths of a partition's active main files.
// It prefers the manifest; with no manifest yet (data written by an earlier
// version, or the first publish), it adopts the parquet files already present,
// migrating that directory into manifest control on the next commit.
func activeMainFiles(dir string) ([]string, error) {
	m, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return listParquetFiles(dir), nil
	}
	files := make([]string, 0, len(m.ActiveFiles))
	for _, name := range m.ActiveFiles {
		files = append(files, filepath.Join(dir, name))
	}
	return files, nil
}

// planInc splits the present inc files into those that still need merging and
// leftovers already consumed by the committed state. Leftovers occur when a
// crash interrupted inc cleanup after a successful commit; re-merging them would
// double-apply aggregates such as sum, so they are returned for deletion only.
func planInc(dir string, incFiles []string) (newInc, consumedLeftovers []string, err error) {
	m, err := readManifest(dir)
	if err != nil {
		return nil, nil, err
	}
	var consumed map[string]bool
	if m != nil {
		c, err := readCommit(dir, m.LastCommit)
		if err != nil {
			return nil, nil, err
		}
		if c != nil {
			consumed = make(map[string]bool, len(c.InputFiles))
			for _, f := range c.InputFiles {
				consumed[f] = true
			}
		}
	}
	for _, f := range incFiles {
		if consumed[f] {
			consumedLeftovers = append(consumedLeftovers, f)
		} else {
			newInc = append(newInc, f)
		}
	}
	return newInc, consumedLeftovers, nil
}

// commitActive atomically publishes newActive as the partition's active file
// set and records the consumed inc inputs. Writing the commit record first and
// then renaming the manifest makes the manifest swap the single commit point: a
// crash before it leaves the old state authoritative (the new output is an
// ignored orphan), a crash after it leaves the new state authoritative (the
// consumed inc are recorded so they are never re-applied). Superseded/orphaned
// files, consumed inc, and stale commit records are then garbage-collected.
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

	rec := commitRecord{TxID: txID, InputFiles: consumedInc, OutputFiles: baseNames(newActive)}
	if err := writeJSONAtomic(commitPath(dir, txID), rec); err != nil {
		return fmt.Errorf("write commit record: %w", err)
	}

	m := partitionManifest{Version: version, ActiveFiles: baseNames(newActive), LastCommit: txID}
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
// part of the current commit: parquet files no longer active (superseded or
// orphaned by an interrupted publish), stray staging temp files, and commit
// records other than keepTx.
func finalizeDir(dir string, active []string, keepTx string) {
	keep := make(map[string]bool, len(active))
	for _, a := range active {
		keep[filepath.Base(a)] = true
	}
	keepCommit := commitPrefix + keepTx + commitSuffix
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			os.Remove(filepath.Join(dir, name))
		case strings.HasSuffix(name, ".parquet") && !keep[name]:
			os.Remove(filepath.Join(dir, name))
		case strings.HasPrefix(name, commitPrefix) && strings.HasSuffix(name, commitSuffix) && name != keepCommit:
			os.Remove(filepath.Join(dir, name))
		}
	}
}

func baseNames(paths []string) []string {
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = filepath.Base(p)
	}
	return out
}
