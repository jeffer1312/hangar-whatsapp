package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeffer1312/hangar-whatsapp/internal/control"
	"github.com/jeffer1312/hangar-whatsapp/internal/store"
)

func TestVersion(t *testing.T) {
	var out bytes.Buffer
	if code := run([]string{"--version"}, &out, &out); code != 0 || strings.TrimSpace(out.String()) != "hangar-whatsapp dev (api 3)" {
		t.Fatalf("--version = %d %q", code, out.String())
	}
}

func TestSchemaVersion(t *testing.T) {
	var out, errOut bytes.Buffer
	data := filepath.Join(t.TempDir(), "absent")
	if code := run([]string{"--data", data, "--schema-version"}, &out, &errOut); code != 0 || out.String() != "2\n" {
		t.Fatalf("--schema-version = %d %q, stderr %q; want 0 \"2\\n\"", code, out.String(), errOut.String())
	}
	if _, err := os.Stat(data); !os.IsNotExist(err) {
		t.Fatalf("--schema-version touched --data: %v", err)
	}
}

func TestExitCode(t *testing.T) {
	for _, c := range []struct {
		err  error
		want int
	}{
		{fmt.Errorf("open: %w", store.ErrSchemaNewer), 3},
		{fmt.Errorf("open: %w", store.ErrBackupFailed), 4},
		{errors.New("other"), 1},
	} {
		if got := exitCode(c.err); got != c.want {
			t.Errorf("exitCode(%v) = %d, want %d", c.err, got, c.want)
		}
	}
}

// dataSchema runs --data-schema-version on data and returns the exit code, stdout and stderr.
func dataSchema(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(append([]string{"--data-schema-version"}, args...), &out, &errOut)
	return code, out.String(), errOut.String()
}

// walDB leaves a WAL-mode messages.db at user_version v in data and returns the open writer.
func walDB(t *testing.T, data string, v int) *sql.DB {
	t.Helper()
	w, err := sql.Open("sqlite", "file:"+store.MessagesPath(data)+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	w.SetMaxOpenConns(1)
	for _, q := range []string{`CREATE TABLE IF NOT EXISTS t (x)`, fmt.Sprintf(`PRAGMA user_version = %d`, v)} {
		if _, err := w.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

func TestDataSchemaVersionWithoutTheFile(t *testing.T) {
	data := filepath.Join(t.TempDir(), "absent")
	if code, out, errOut := dataSchema(t, "--data", data); code != 0 || out != "0\n" {
		t.Fatalf("--data-schema-version on a missing folder = %d %q, stderr %q; want 0 \"0\\n\"", code, out, errOut)
	}
	if _, err := os.Stat(data); !os.IsNotExist(err) {
		t.Fatalf("--data-schema-version created --data: %v", err)
	}
}

func TestDataSchemaVersionLeavesTheFileAlone(t *testing.T) {
	data := t.TempDir()
	walDB(t, data, 1).Close()
	path := store.MessagesPath(data)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := dataSchema(t, "--data", data); code != 0 || out != "1\n" {
		t.Fatalf("--data-schema-version = %d %q, stderr %q; want 0 \"1\\n\"", code, out, errOut)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("messages.db changed: %d %v → %d %v", before.Size(), before.ModTime(), after.Size(), after.ModTime())
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Errorf("%s left behind: %v", suffix, err)
		}
	}
}

func TestDataSchemaVersionWhileAnotherBinaryWrites(t *testing.T) {
	data := t.TempDir()
	w := walDB(t, data, 1)
	defer w.Close()
	// The raise sits in the WAL, not yet in the main file.
	if _, err := w.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := dataSchema(t, "--data", data); code != 0 || out != "2\n" {
		t.Fatalf("--data-schema-version with a writer = %d %q, stderr %q; want 0 \"2\\n\"", code, out, errOut)
	}
}

func TestDataSchemaVersionOnAFileThatIsNotSQLite(t *testing.T) {
	data := t.TempDir()
	if err := os.WriteFile(store.MessagesPath(data), []byte("not a database, just text long enough to have a header"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := dataSchema(t, "--data", data); code != 1 || out != "" || errOut == "" {
		t.Fatalf("--data-schema-version on a text file = %d %q, stderr %q; want 1, nothing, a reason", code, out, errOut)
	}
}

func TestDataSchemaVersionNeedsData(t *testing.T) {
	if code, out, _ := dataSchema(t); code != 2 || out != "" {
		t.Fatalf("--data-schema-version without --data = %d %q; want 2", code, out)
	}
}

func TestImportRefusesWhileTheServiceRuns(t *testing.T) {
	data := t.TempDir()
	if err := control.WriteControl(data, control.Control{Version: 1, API: 1, Port: 1, PID: os.Getpid()}); err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	for _, name := range []string{"mensagens.db", "sessao.db"} {
		if err := os.WriteFile(filepath.Join(src, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if code := run([]string{"--data", data, "--import", src}, &out, &out); code == 0 {
		t.Fatalf("import with the service running succeeded: %s", out.String())
	}
	if _, err := os.Stat(filepath.Join(src, "sessao.db")); err != nil {
		t.Fatalf("refused import touched the source: %v", err)
	}
	if !strings.Contains(out.String(), "pare o serviço") {
		t.Fatalf("message does not say to stop the service: %q", out.String())
	}
}
