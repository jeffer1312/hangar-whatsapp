package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestOpenRefusesNewerSchema(t *testing.T) {
	dir := t.TempDir()
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	db, err := Open(dir)
	if err == nil {
		db.Close()
	}
	if !errors.Is(err, ErrSchemaNewer) {
		t.Fatalf("Open on user_version 2 = %v, want ErrSchemaNewer", err)
	}
}

func TestOpenStampsSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v int
	if err := db.w.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("user_version = %d, want 1", v)
	}
}
