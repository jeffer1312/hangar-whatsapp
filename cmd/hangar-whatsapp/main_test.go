package main

import (
	"bytes"
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
