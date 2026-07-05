package fold

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// JSON serializes a Go value into a JSON string for json_merge fields.
func JSON(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// DB is the root Fold handle, similar in spirit to gorm.DB.
type DB struct {
	dir         string             // data root directory
	tables      map[string]*Schema // registered table schemas
	compact     CompactOptions     // merge/upsert worker and DuckDB execution tuning
	storage     Storage            // metadata + object persistence (default: local filesystem)
	bloomMu     sync.Mutex         // serializes whole-file bloom rewrites across concurrent workers
	partLocks   sync.Map           // partition dir -> *sync.Mutex; serializes publishes per partition
	liveStaging sync.Map           // staging dir -> struct{}; in-flight upsert inputs the stale sweep must skip
	mu          sync.RWMutex
}

// lockPartition serializes merge/upsert publishes for one partition directory
// within this DB handle, so a concurrent Merge and Upsert on the same
// partition cannot interleave read-manifest/commit and lose one of the
// updates. Two Open() handles on the same data root are NOT coordinated —
// like cross-process and cross-machine writers, that remains out of scope
// (single writer per table). The returned func releases the lock.
func (db *DB) lockPartition(dir string) func() {
	m, _ := db.partLocks.LoadOrStore(dir, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// DuckDBOptions tunes the per-job DuckDB execution used by merge and upsert.
// Zero values are replaced with conservative defaults.
type DuckDBOptions struct {
	MemoryLimit string // DuckDB memory_limit, e.g. "2GB" (default "2GB")
	Threads     int    // DuckDB threads per job (default 4)
	TempDir     string // DuckDB temp_directory for spilling (default: unset)
}

// CompactOptions bounds merge/upsert resource use. Zero values are replaced with
// conservative defaults suitable for local development.
type CompactOptions struct {
	Workers int           // concurrent partition workers (default 10)
	DuckDB  DuckDBOptions // per-job DuckDB execution options

	// DisableBloom turns off the post-merge bloom-filter rewrite entirely.
	// Bloom filters only speed up primary-key lookups; they are never required
	// for correctness, so disabling them is always safe.
	DisableBloom bool
	// BloomMaxFileBytes skips the bloom rewrite when a staged output exceeds
	// this size, bounding the rewrite's (whole-file) memory use. Default 256 MiB.
	BloomMaxFileBytes int64
}

const (
	defaultCompactWorkers    = 10
	defaultMemoryLimit       = "2GB"
	defaultThreads           = 4
	defaultBloomMaxFileBytes = 256 << 20 // 256 MiB
)

// normalized fills unset fields with conservative defaults.
func (o CompactOptions) normalized() CompactOptions {
	if o.Workers <= 0 {
		o.Workers = defaultCompactWorkers
	}
	if o.DuckDB.MemoryLimit == "" {
		o.DuckDB.MemoryLimit = defaultMemoryLimit
	}
	if o.DuckDB.Threads <= 0 {
		o.DuckDB.Threads = defaultThreads
	}
	if o.BloomMaxFileBytes <= 0 {
		o.BloomMaxFileBytes = defaultBloomMaxFileBytes
	}
	return o
}

// Option configures a DB at Open time.
type Option func(*DB)

// WithCompactOptions sets merge/upsert worker and DuckDB execution tuning.
// Unset (zero) fields fall back to conservative defaults.
func WithCompactOptions(o CompactOptions) Option {
	return func(db *DB) { db.compact = o }
}

// WithStorage sets the metadata/object persistence backend. The default is the
// local filesystem; an object-storage adapter can implement Storage to publish
// manifests and staged outputs elsewhere.
func WithStorage(s Storage) Option {
	return func(db *DB) { db.storage = s }
}

// Open initializes a data root and creates inc/ and main/ subdirectories.
func Open(dir string, opts ...Option) (*DB, error) {
	for _, sub := range []string{"inc", "main"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0755); err != nil {
			return nil, fmt.Errorf("fold: create %s directory: %w", sub, err)
		}
	}
	db := &DB{
		dir:    dir,
		tables: make(map[string]*Schema),
	}
	for _, opt := range opts {
		opt(db)
	}
	db.compact = db.compact.normalized()
	if db.storage == nil {
		db.storage = localStorage{}
	}
	return db, nil
}

// Close releases resources held by the DB.
func (db *DB) Close() error {
	return nil
}

// Dir returns the data root directory.
func (db *DB) Dir() string {
	return db.dir
}

// Table is a typed table handle bound to a DB and Schema.
type Table[T any] struct {
	db     *DB
	schema *Schema
}

// Register parses a struct type into a Schema and returns a typed Table handle.
func Register[T any](db *DB) *Table[T] {
	schema, err := parseSchema[T]()
	if err != nil {
		panic(err)
	}

	db.mu.Lock()
	db.tables[schema.Name] = schema
	db.mu.Unlock()

	return &Table[T]{db: db, schema: schema}
}

// Schema returns the parsed table schema.
func (t *Table[T]) Schema() *Schema {
	return t.schema
}

// incDir returns the table incremental directory.
func (t *Table[T]) incDir() string {
	return filepath.Join(t.db.dir, "inc", t.schema.Name)
}

// mainDir returns the table main directory.
func (t *Table[T]) mainDir() string {
	return filepath.Join(t.db.dir, "main", t.schema.Name)
}
