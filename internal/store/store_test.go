package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

// version1 leaves in dir a messages.db at user_version 1 with one message and no outbox.
func version1(t *testing.T, dir string) {
	t.Helper()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		`INSERT INTO messages (chat, id, from_me, at, kind, source) VALUES ('5564@s.whatsapp.net', 'M1', 0, '2026-11-22T10:00:00Z', 'text', 'whatsapp')`,
		`DROP TABLE IF EXISTS outbox`, `DROP TABLE IF EXISTS settings`, `PRAGMA user_version = 1`,
	} {
		if _, err := db.w.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
}

// inspect reads user_version, the outbox table count and the message count of a SQLite file.
func inspect(t *testing.T, path string) (version, outbox, messages int) {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.QueryRow(`SELECT (SELECT user_version FROM pragma_user_version),
			(SELECT count(*) FROM sqlite_master WHERE name = 'outbox'), (SELECT count(*) FROM messages)`).Scan(&version, &outbox, &messages); err != nil {
		t.Fatal(err)
	}
	return
}

func TestOpenMigratesVersion1AfterCopyingIt(t *testing.T) {
	dir := t.TempDir()
	bak := filepath.Join(dir, "messages.db.schema1.bak")
	if err := os.WriteFile(bak, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	version1(t, dir)
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if v, outbox, messages := inspect(t, MessagesPath(dir)); v != 2 || outbox != 1 || messages != 1 {
		t.Fatalf("after migrating = version %d, outbox tables %d, messages %d; want 2, 1, 1", v, outbox, messages)
	}
	if v, outbox, messages := inspect(t, bak); v != 1 || outbox != 0 || messages != 1 {
		t.Fatalf("copy = version %d, outbox tables %d, messages %d; want 1, 0, 1", v, outbox, messages)
	}
	if fi, err := os.Stat(bak); err != nil || (runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600) {
		t.Fatalf("copy mode = %v, %v; want 0600", fi.Mode().Perm(), err)
	}
}

func TestOpenInAFolderWithURICharacters(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "a#b?c%41d")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err := os.Stat(MessagesPath(dir)); err != nil {
		t.Fatalf("messages.db not in the folder: %v", err)
	}
	if entries, _ := os.ReadDir(parent); len(entries) != 1 {
		t.Fatalf("files outside the folder: %v", entries)
	}
	if v, err := DataSchemaVersion(dir); err != nil || v != SchemaVersion {
		t.Fatalf("DataSchemaVersion = %d, %v; want %d", v, err, SchemaVersion)
	}
}

func TestNoBackupWithoutRaisingVersion(t *testing.T) {
	dir := t.TempDir()
	for range 2 {
		db, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	if baks, _ := filepath.Glob(filepath.Join(dir, "*.bak*")); len(baks) != 0 {
		t.Fatalf("backups without a version change: %v", baks)
	}
}

func TestBackupFailureLeavesTheDatabaseAlone(t *testing.T) {
	dir := t.TempDir()
	version1(t, dir)
	// A non-empty folder where the copy goes cannot be replaced, even by root.
	if err := os.MkdirAll(filepath.Join(dir, "messages.db.schema1.bak", "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir)
	if err == nil {
		db.Close()
	}
	if !errors.Is(err, ErrBackupFailed) {
		t.Fatalf("Open with an unwritable backup = %v, want ErrBackupFailed", err)
	}
	if v, outbox, messages := inspect(t, MessagesPath(dir)); v != 1 || outbox != 0 || messages != 1 {
		t.Fatalf("after the failed backup = version %d, outbox tables %d, messages %d; want 1, 0, 1", v, outbox, messages)
	}
	if _, err := os.Stat(filepath.Join(dir, "messages.db.schema1.bak.tmp")); !os.IsNotExist(err) {
		t.Fatalf("the failed copy left its temporary file: %v", err)
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
	for _, id := range []string{"D1", "S1"} {
		if _, err := raw.Exec(insert020, id, "k"+id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.Exec(`UPDATE outbox SET state = 'sent', via = 'desktop', answered_at = '2026-11-24T12:01:00Z' WHERE id = 'S1'`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if s, err := db.Draft(ctx, "S1"); err != nil || s.SentText != "chego às 15h" || s.Edited {
		t.Fatalf("draft the 0.2.0 sent = sent_text %q edited %v, %v; want its text", s.SentText, s.Edited, err)
	}
	d, err := db.Draft(ctx, "D1")
	if err != nil || d.Text != "chego às 15h" || d.Edited || d.SentText != "" || len(d.Recent) != 1 || d.Recent[0].Kind != "" {
		t.Fatalf("0.2.0 draft after migrating = %+v, %v", d, err)
	}
	if _, err := db.w.Exec(insert020, "D2", "k2"); err != nil {
		t.Fatalf("the 0.2.0 insert after migrating = %v", err)
	}
	sent, err := db.ClaimSend(ctx, "D2", "desktop", nil, time.Date(2026, 11, 24, 12, 1, 0, 0, time.UTC))
	if err != nil || sent.SentText != "chego às 15h" || sent.Edited {
		t.Fatalf("send of a 0.2.0 draft = %+v, %v", sent, err)
	}
	var v int
	if err := db.w.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != 2 {
		t.Fatalf("user_version = %d, %v; want 2", v, err)
	}
}

func TestOpenFinishesAHalfDoneOutboxMigration(t *testing.T) {
	dir := t.TempDir()
	schema020, err := os.ReadFile(filepath.Join("testdata", "v020_schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "messages.db"))
	if err != nil {
		t.Fatal(err)
	}
	// The first run stopped between the two ALTERs.
	for _, q := range []string{string(schema020), `ALTER TABLE outbox ADD COLUMN edited INTEGER NOT NULL DEFAULT 0`, `PRAGMA user_version = 2`} {
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
	if d, err := db.Draft(context.Background(), "D1"); err != nil || d.SentText != "" {
		t.Fatalf("draft after finishing the migration = %+v, %v", d, err)
	}
}
