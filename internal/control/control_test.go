package control

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestKeyIsCreatedOnceAndPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	first, err := LoadOrCreateKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 {
		t.Fatalf("key has %d bytes, want 32", len(first))
	}
	second, err := LoadOrCreateKey(dir)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("second load = %x, %v; want the same key", second, err)
	}
	text, err := os.ReadFile(filepath.Join(dir, "control.key"))
	if err != nil || string(bytes.TrimSpace(text)) != hex.EncodeToString(first) {
		t.Fatalf("control.key = %q, want the key in hex", text)
	}
	if runtime.GOOS == "windows" {
		t.Skip("modes are POSIX only")
	}
	for path, want := range map[string]os.FileMode{dir: 0o700, filepath.Join(dir, "control.key"): 0o600} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != want {
			t.Fatalf("%s mode = %v, want %v", path, info.Mode().Perm(), want)
		}
	}
}

func TestWriteControlReplacesTheFile(t *testing.T) {
	dir := t.TempDir()
	for _, port := range []uint16{40001, 40002} {
		if err := WriteControl(dir, Control{Version: 1, API: 1, Port: port, PID: 4242}); err != nil {
			t.Fatal(err)
		}
	}
	var got map[string]any
	b, err := os.ReadFile(filepath.Join(dir, "control.json"))
	if err != nil || json.Unmarshal(b, &got) != nil {
		t.Fatalf("control.json = %q, %v", b, err)
	}
	want := map[string]any{"version": 1.0, "api": 1.0, "port": 40002.0, "pid": 4242.0}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("control.json = %v, want %v", got, want)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("leftover files next to control.json: %v", entries)
	}
}

// specMAC follows the C2 text, not the code under test.
func specMAC(key []byte, parts ...string) string {
	m := hmac.New(sha256.New, key)
	for i, p := range parts {
		if i > 0 {
			m.Write([]byte("\n"))
		}
		m.Write([]byte(p))
	}
	return hex.EncodeToString(m.Sum(nil))
}

func specBodyHash(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestVerifyRequest(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	now := time.Unix(1_800_000_000, 0)
	body := []byte(`{"chat":"5564"}`)
	uri := "/v1/media/request?x=1"
	good := fmt.Sprintf("%d.%s", now.Unix(), specMAC(key, "POST", uri, fmt.Sprint(now.Unix()), specBodyHash(body)))

	if ts, err := VerifyRequest(key, good, "POST", uri, body, now); err != nil || ts != fmt.Sprint(now.Unix()) {
		t.Fatalf("valid signature = %q, %v", ts, err)
	}
	if got := SignRequest(key, "POST", uri, now.Unix(), body); got != good {
		t.Fatalf("SignRequest = %s, want %s", got, good)
	}
	cases := map[string]struct {
		header, method, uri string
		body                []byte
		now                 time.Time
	}{
		"expired":      {good, "POST", uri, body, now.Add(31 * time.Second)},
		"from future":  {good, "POST", uri, body, now.Add(-31 * time.Second)},
		"other body":   {good, "POST", uri, []byte(`{"chat":"5565"}`), now},
		"other query":  {good, "POST", "/v1/media/request?x=2", body, now},
		"other method": {good, "GET", uri, body, now},
		"wrong key":    {fmt.Sprintf("%d.%s", now.Unix(), specMAC(bytes.Repeat([]byte{8}, 32), "POST", uri, fmt.Sprint(now.Unix()), specBodyHash(body))), "POST", uri, body, now},
		"no dot":       {"abc", "POST", uri, body, now},
		"empty":        {"", "POST", uri, body, now},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := VerifyRequest(key, c.header, c.method, c.uri, c.body, c.now); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
	if _, err := VerifyRequest(key, good, "POST", uri, body, now.Add(30*time.Second)); err != nil {
		t.Fatalf("30 s late should pass: %v", err)
	}
}

func TestSignResponse(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	body := []byte(`{"api":1}`)
	want := specMAC(key, "1800000000", "200", specBodyHash(body))
	if got := SignResponse(key, "1800000000", 200, body); got != want {
		t.Fatalf("SignResponse = %s, want %s", got, want)
	}
}
