// Package store owns the SQLite schema of messages.db: one writer connection and a separate
// read-only pool for the API.
package store

import (
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

const SchemaVersion = 1

// TimeLayout is UTC with Z; every stored time uses it so text order is time order.
const TimeLayout = "2006-01-02T15:04:05Z"

var (
	ErrSchemaNewer = errors.New("schema_newer")
	ErrBusy        = errors.New("busy")
)

const defaultQueryTimeout = 5 * time.Second

type DB struct {
	w            *sql.DB
	r            *sql.DB
	dir          string
	QueryTimeout time.Duration
}

func MessagesPath(dir string) string { return filepath.Join(dir, "messages.db") }

// OpenSQLite opens a writer: one connection, because SQLite has one writer at a time.
func OpenSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+
		"?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// OpenSession opens session.db holding its lock for as long as it is open: a second process on
// the same folder fails right away instead of running a second copy of the device keys.
func OpenSession(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+
		"?_pragma=locking_mode(EXCLUSIVE)&_pragma=busy_timeout(0)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// The lock lives in the connection: closing it after idle time would let a second process in.
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	return db, nil
}

func Open(dir string) (*DB, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	w, err := OpenSQLite(MessagesPath(dir))
	if err != nil {
		return nil, err
	}
	if err := migrate(w); err != nil {
		w.Close()
		return nil, err
	}
	// The read pool never writes: a slow API query cannot hold the writer.
	r, err := sql.Open("sqlite", "file:"+MessagesPath(dir)+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(4)
	return &DB{w: w, r: r, dir: dir, QueryTimeout: defaultQueryTimeout}, nil
}

func migrate(w *sql.DB) error {
	var v int
	if err := w.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v > SchemaVersion {
		return fmt.Errorf("%w: messages.db is version %d, this binary knows up to %d", ErrSchemaNewer, v, SchemaVersion)
	}
	if _, err := w.Exec(schema); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}
	_, err := w.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion))
	return err
}

func (d *DB) Close() error {
	return errors.Join(d.r.Close(), d.w.Close())
}

// RemoveDB deletes a SQLite file with its -wal and -shm; a missing file is not an error.
func RemoveDB(path string) error {
	var errs []error
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
