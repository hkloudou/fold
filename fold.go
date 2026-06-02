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
	dir    string             // data root directory
	tables map[string]*Schema // registered table schemas
	mu     sync.RWMutex
}

// Open initializes a data root and creates inc/ and main/ subdirectories.
func Open(dir string) (*DB, error) {
	for _, sub := range []string{"inc", "main"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0755); err != nil {
			return nil, fmt.Errorf("fold: create %s directory: %w", sub, err)
		}
	}
	return &DB{
		dir:    dir,
		tables: make(map[string]*Schema),
	}, nil
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
