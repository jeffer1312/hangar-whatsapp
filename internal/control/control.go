// Package control holds contract C1 (control.json and control.key) and the HMAC signatures of C2.
package control

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Control struct {
	Version int    `json:"version"`
	API     int    `json:"api"`
	Port    uint16 `json:"port"`
	PID     int    `json:"pid"`
}

// MaxSkew is how far the request timestamp may be from the clock.
const MaxSkew = 30 * time.Second

var ErrBadSignature = errors.New("unauthorized")

// PrepareDir creates the data folder readable only by its owner: it holds the session keys.
func PrepareDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

// LoadOrCreateKey returns the 32-byte HMAC key; control.key keeps it in hex.
func LoadOrCreateKey(dir string) ([]byte, error) {
	if err := PrepareDir(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "control.key")
	text, err := os.ReadFile(path)
	if err == nil {
		key, err := hex.DecodeString(string(bytes.TrimSpace(text)))
		if err != nil || len(key) != 32 {
			return nil, fmt.Errorf("control.key inválida: apague o arquivo para gerar outra")
		}
		return key, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if err := writeAtomic(path, []byte(hex.EncodeToString(key)+"\n"), 0o600); err != nil {
		return nil, err
	}
	return key, nil
}

// ReadControl reads control.json; a missing file is a zero Control and no error.
func ReadControl(dir string) (Control, error) {
	var c Control
	b, err := os.ReadFile(filepath.Join(dir, "control.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return c, nil
	} else if err != nil {
		return c, err
	}
	return c, json.Unmarshal(b, &c)
}

// WriteControl replaces control.json in one rename, so a reader never sees half a file.
func WriteControl(dir string, c Control) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, "control.json"), b, 0o600)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func mac(key []byte, parts ...string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(m.Sum(nil))
}

func bodyHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// SignRequest is what the Hangar sends in X-Hangar-Signature; uri is the path plus "?query" when
// there is one.
func SignRequest(key []byte, method, uri string, ts int64, body []byte) string {
	t := strconv.FormatInt(ts, 10)
	return t + "." + mac(key, method, uri, t, bodyHash(body))
}

// VerifyRequest returns the request timestamp, which the response signature repeats.
func VerifyRequest(key []byte, header, method, uri string, body []byte, now time.Time) (string, error) {
	ts, sig, ok := strings.Cut(header, ".")
	if !ok {
		return "", ErrBadSignature
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return "", ErrBadSignature
	}
	if skew := now.Sub(time.Unix(sec, 0)); skew > MaxSkew || skew < -MaxSkew {
		return ts, ErrBadSignature
	}
	if !hmac.Equal([]byte(sig), []byte(mac(key, method, uri, ts, bodyHash(body)))) {
		return ts, ErrBadSignature
	}
	return ts, nil
}

func SignResponse(key []byte, ts string, status int, body []byte) string {
	return mac(key, ts, strconv.Itoa(status), bodyHash(body))
}
