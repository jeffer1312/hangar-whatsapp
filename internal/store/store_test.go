package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenRefusesNewerSchema(t *testing.T) {
	dir := t.TempDir()
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	db, err := Open(dir)
	if err == nil {
		db.Close()
	}
	if !errors.Is(err, ErrSchemaNewer) {
		t.Fatalf("Open on a newer user_version = %v, want ErrSchemaNewer", err)
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
	if v != 2 {
		t.Fatalf("user_version = %d, want 2", v)
	}
}

func TestOpenMigratesVersion1(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO messages (chat, id, from_me, at, kind, source) VALUES ('5564@s.whatsapp.net', 'M1', 0, '2026-11-22T10:00:00Z', 'text', 'whatsapp')`,
		`DROP TABLE IF EXISTS outbox`, `DROP TABLE IF EXISTS settings`, `PRAGMA user_version = 1`,
	} {
		if _, err := db.w.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()

	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v, outbox, messages int
	if err := db.w.QueryRow(`SELECT (SELECT user_version FROM pragma_user_version),
			(SELECT count(*) FROM sqlite_master WHERE name = 'outbox'), (SELECT count(*) FROM messages)`).Scan(&v, &outbox, &messages); err != nil {
		t.Fatal(err)
	}
	if v != 2 || outbox != 1 || messages != 1 {
		t.Fatalf("after migrating = version %d, outbox tables %d, messages %d; want 2, 1, 1", v, outbox, messages)
	}
}

func TestOpenAddsTheAccountToVersion1Messages(t *testing.T) {
	dir := t.TempDir()
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE messages (chat TEXT NOT NULL, id TEXT NOT NULL, sender TEXT NOT NULL DEFAULT '', from_me INTEGER NOT NULL,
			at TEXT NOT NULL, kind TEXT NOT NULL, text TEXT NOT NULL DEFAULT '', media TEXT, media_mime TEXT, media_error TEXT,
			reply_to TEXT, source TEXT NOT NULL, raw TEXT, PRIMARY KEY (chat, id))`,
		`INSERT INTO messages (chat, id, from_me, at, kind, source) VALUES ('5564@s.whatsapp.net', 'M1', 0, '2026-11-22T10:00:00Z', 'text', 'whatsapp')`,
		`PRAGMA user_version = 1`,
	} {
		if _, err := raw.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	raw.Close()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	account := func() string {
		t.Helper()
		var a string
		if err := db.w.QueryRow(`SELECT account FROM messages WHERE id = 'M1'`).Scan(&a); err != nil {
			t.Fatal(err)
		}
		return a
	}
	if a := account(); a != "" {
		t.Fatalf("migrated row account = %q, want empty until a number is paired", a)
	}
	// The 0.1.x rows go to the number paired when it first runs; a later number does not take them.
	if err := db.AdoptMessages(context.Background(), "556411112222"); err != nil {
		t.Fatal(err)
	}
	if err := db.AdoptMessages(context.Background(), "5511988887777"); err != nil {
		t.Fatal(err)
	}
	if a := account(); a != "556411112222" {
		t.Fatalf("adopted account = %q, want the first paired number", a)
	}
}

// insert020 is how the 0.2.0 binary stores a draft: a version that runs on this file after an
// update must still write it.
const insert020 = `INSERT INTO outbox (id, dedup, origin, chat, address, chat_kind, chat_number, chat_name,
		chat_name_source, recent, text, reply_to, quoted_text, quoted_raw, file_name, file_size, file_sha256, file_source,
		file_outside_cwd, created_at, expires_at, state)
	VALUES (?, ?, 'srv::s1', '556499990000@s.whatsapp.net', '556499990000@s.whatsapp.net', 'contact', '556499990000', '', '',
		'[{"from_me":false,"sender":"Maria","at":"2026-11-24T11:59:00Z","text":"oi"}]', 'chego às 15h', NULL, NULL, NULL,
		NULL, NULL, NULL, NULL, NULL, '2026-11-24T12:00:00Z', '2026-11-24T12:10:00Z', 'pending')`

func TestOpenMigratesThe020Outbox(t *testing.T) {
	dir := t.TempDir()
	schema020, err := os.ReadFile(filepath.Join("testdata", "v020_schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{string(schema020), `PRAGMA user_version = 2`} {
		if _, err := raw.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.Exec(insert020, "D1", "k1"); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	d, err := db.Draft(ctx, "D1")
	if err != nil || d.Text != "chego às 15h" || d.Edited || d.SentText != "" || len(d.Recent) != 1 || d.Recent[0].Kind != "" {
		t.Fatalf("0.2.0 draft after migrating = %+v, %v", d, err)
	}
	if _, err := db.w.Exec(insert020, "D2", "k2"); err != nil {
		t.Fatalf("the 0.2.0 insert after migrating = %v", err)
	}
	sent, err := db.ClaimSend(ctx, "D2", "desktop", time.Date(2026, 11, 24, 12, 1, 0, 0, time.UTC))
	if err != nil || sent.SentText != "chego às 15h" || sent.Edited {
		t.Fatalf("send of a 0.2.0 draft = %+v, %v", sent, err)
	}
	var v int
	if err := db.w.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != 2 {
		t.Fatalf("user_version = %d, %v; want 2", v, err)
	}
}
