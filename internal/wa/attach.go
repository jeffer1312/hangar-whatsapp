package wa

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
)

const MaxFile = 100 << 20

var errSymlink = errors.New("link simbólico")

func refuse(msg string) error { return fmt.Errorf("%w: %s", ErrFileRefused, msg) }

func within(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

// resolved follows the links in a path that exists; otherwise it is the path itself.
func resolved(path string) string {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real
	}
	return path
}

// refusal says why path may not be attached, or "". The hidden-component rule covers ~/.hangar,
// ~/.ssh, ~/.gnupg, ~/.config, ~/.claude* and .env*.
func refusal(path, dataDir string) string {
	if within(path, dataDir) || within(path, resolved(dataDir)) {
		return "fica na pasta de dados do hangar-whatsapp"
	}
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if strings.HasPrefix(part, ".") {
			return "fica em pasta ou arquivo oculto (" + part + ")"
		}
	}
	if ext := strings.ToLower(filepath.Ext(path)); ext == ".key" || ext == ".pem" {
		return "parece chave ou certificado (" + ext + ")"
	}
	return ""
}

// attach copies path into dest for the draft. The checks run on the path as given and with its
// folders resolved; the file itself must not be a link and is copied from the descriptor that was
// checked, so what the card shows is what goes out.
func attach(path, cwd, dataDir, dest string) (f *store.DraftFile, err error) {
	if !filepath.IsAbs(path) {
		return nil, badDraft("file deve ser um caminho absoluto")
	}
	path = filepath.Clean(path)
	if why := refusal(path, dataDir); why != "" {
		return nil, refuse(why)
	}
	real := filepath.Join(resolved(filepath.Dir(path)), filepath.Base(path))
	if why := refusal(real, dataDir); why != "" {
		return nil, refuse(why)
	}
	src, err := openNoFollow(real)
	switch {
	case errors.Is(err, errSymlink):
		return nil, refuse("o arquivo é um link simbólico")
	case errors.Is(err, fs.ErrNotExist):
		return nil, refuse("o arquivo não existe")
	case err != nil:
		return nil, refuse(err.Error())
	}
	defer src.Close()
	st, err := src.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, refuse("não é um arquivo comum")
	}
	if st.Size() > MaxFile {
		return nil, ErrFileTooLarge
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dest, 0o700); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			os.RemoveAll(dest)
		}
	}()
	name := filepath.Base(real)
	out, err := os.OpenFile(filepath.Join(dest, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, sum), io.LimitReader(src, MaxFile+1))
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if n > MaxFile {
		return nil, ErrFileTooLarge
	}
	outside := cwd == "" || !(within(real, filepath.Clean(cwd)) || within(real, resolved(filepath.Clean(cwd))))
	return &store.DraftFile{Name: name, Size: n, SHA256: hex.EncodeToString(sum.Sum(nil)), Source: real, OutsideCwd: outside}, nil
}
