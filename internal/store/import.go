package store

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const prototypeExpired = "expirada no servidor do WhatsApp; só o celular tem (whatsapp-arquivo pedir-midia)"

func SessionPath(dir string) string { return filepath.Join(dir, "session.db") }

// Import copies a whatsapp-arquivo prototype folder (mensagens.db, sessao.db, midia/) into dst,
// which must not have messages.db nor session.db. At the end the source sessao.db is renamed to
// sessao.db.migrated, so the prototype cannot run with the same keys again.
func Import(src, dst string) (err error) {
	for _, path := range []string{MessagesPath(dst), SessionPath(dst)} {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s já existe: a importação só roda numa pasta sem dados", path)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	oldMessages, oldSession := filepath.Join(src, "mensagens.db"), filepath.Join(src, "sessao.db")
	for _, path := range []string{oldMessages, oldSession} {
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("origem sem %s: %w", filepath.Base(path), err)
		}
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return err
	}
	_, statErr := os.Stat(filepath.Join(dst, "media"))
	createdMedia := errors.Is(statErr, fs.ErrNotExist)
	defer func() {
		// A failed import leaves dst as it was, so it can run again.
		if err != nil {
			for _, path := range []string{MessagesPath(dst), SessionPath(dst)} {
				for _, suffix := range []string{"", "-wal", "-shm"} {
					os.Remove(path + suffix)
				}
			}
			if createdMedia {
				os.RemoveAll(filepath.Join(dst, "media"))
			}
		}
	}()

	if err := importMessages(oldMessages, dst); err != nil {
		return fmt.Errorf("importar mensagens: %w", err)
	}
	if err := importMedia(filepath.Join(src, "midia"), filepath.Join(dst, "media")); err != nil {
		return fmt.Errorf("importar mídia: %w", err)
	}
	if err := importSession(oldSession, SessionPath(dst)); err != nil {
		return fmt.Errorf("importar sessão: %w", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		from := oldSession + suffix
		if _, err := os.Stat(from); errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err := os.Rename(from, oldSession+".migrated"+suffix); err != nil {
			return fmt.Errorf("renomear %s: %w", filepath.Base(from), err)
		}
	}
	return nil
}

func importMessages(oldPath, dst string) error {
	d, err := Open(dst)
	if err != nil {
		return err
	}
	defer d.Close()
	if _, err := d.w.Exec(`ATTACH DATABASE ? AS old`, "file:"+oldPath+"?mode=ro"); err != nil {
		return err
	}
	defer d.w.Exec(`DETACH DATABASE old`)
	tx, err := d.w.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	const at = `'%Y-%m-%dT%H:%M:%SZ'`
	steps := []struct {
		query string
		args  []any
	}{
		{query: `INSERT INTO contacts (jid, name) SELECT jid, nome FROM old.contatos`},
		{query: `INSERT INTO messages (chat, id, sender, from_me, at, kind, text, media, media_mime, media_error, reply_to, source, raw)
			SELECT conversa, id, remetente, de_mim, strftime(` + at + `, horario),
				CASE tipo WHEN 'texto' THEN 'text' WHEN 'imagem' THEN 'image' WHEN 'audio' THEN 'audio'
					WHEN 'video' THEN 'video' WHEN 'documento' THEN 'document' WHEN 'figurinha' THEN 'sticker'
					WHEN 'reacao' THEN 'reaction' ELSE 'other' END,
				texto,
				CASE WHEN midia LIKE 'midia/%' THEN 'media/' || substr(midia, 7) END,
				midia_mime,
				CASE WHEN midia_erro IS NULL THEN NULL
					WHEN midia_erro = ? THEN 'expired'
					WHEN midia_erro LIKE 'indisponível%' THEN 'unavailable'
					WHEN midia_erro LIKE 'o celular não tem mais%' THEN 'phone_refused'
					WHEN midia_erro LIKE 'referência de mídia ilegível%' THEN 'unreadable'
					ELSE 'lost' END,
				responde_a, origem,
				CASE WHEN json_valid(bruto) AND json_type(bruto, '$.mensagem') = 'object' THEN json_object(
					'chat', json_extract(bruto, '$.chat'),
					'sender', json_extract(bruto, '$.remetente'),
					'from_me', json(iif(json_extract(bruto, '$.de_mim'), 'true', 'false')),
					'group', json(iif(json_extract(bruto, '$.grupo'), 'true', 'false')),
					'message', json(json_extract(bruto, '$.mensagem')))
				ELSE bruto END
			FROM old.mensagens`, args: []any{prototypeExpired}},
		{query: `INSERT INTO media_requests (chat, id, requested_at)
			SELECT conversa, id, strftime(` + at + `, pedida_em) FROM old.midia_pedida`},
	}
	for _, s := range steps {
		if _, err := tx.Exec(s.query, s.args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// importMedia hard-links when it can: the files are never rewritten in place, and 5 GB of media
// would otherwise be copied.
func importMedia(from, to string) error {
	if _, err := os.Stat(from); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return filepath.WalkDir(from, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		switch {
		case e.IsDir():
			return os.MkdirAll(target, 0o700)
		case !e.Type().IsRegular():
			return nil
		}
		if err := os.Link(path, target); err == nil {
			return nil
		}
		return copyFile(path, target)
	})
}

func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// importSession writes one consistent file even when the source still has a -wal.
func importSession(from, to string) error {
	db, err := sql.Open("sqlite", "file:"+from+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(`VACUUM INTO ?`, to); err != nil {
		return err
	}
	return os.Chmod(to, 0o600)
}
