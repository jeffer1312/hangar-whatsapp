package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrRateLimited = errors.New("rate_limited")

type Record struct {
	Chat, ID, Sender string
	// The paired number that archived or sent it.
	Account       string
	FromMe        bool
	At            time.Time
	Kind, Text    string
	Mime, ReplyTo string
	Raw           string
}

// Save never replaces what is there; only a row without a media file is redone, so media the
// ProChat could not fetch gets another chance.
func (d *DB) Save(ctx context.Context, r Record) error {
	return save(ctx, d.w, r)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func save(ctx context.Context, ex execer, r Record) error {
	_, err := ex.ExecContext(ctx, `INSERT INTO messages (chat, id, sender, from_me, at, kind, text,
			media_mime, reply_to, source, raw, account)
		VALUES (?, ?, ?, ?, ?, ?, ?, nullif(?, ''), nullif(?, ''), 'whatsapp', ?, ?)
		ON CONFLICT (chat, id) DO UPDATE SET kind = excluded.kind, text = excluded.text,
			media_mime = excluded.media_mime, media_error = NULL, source = excluded.source, raw = excluded.raw
		WHERE messages.media IS NULL AND excluded.media_mime IS NOT NULL`,
		r.Chat, r.ID, r.Sender, r.FromMe, r.At.UTC().Format(TimeLayout), r.Kind, r.Text, r.Mime, r.ReplyTo, r.Raw, r.Account)
	return err
}

func (d *DB) Edit(ctx context.Context, chat, id, text string) error {
	_, err := d.w.ExecContext(ctx, `UPDATE messages SET text = ? WHERE chat = ? AND id = ?`, text, chat, id)
	return err
}

// Contact with overwrite=false only fills a contact that has no name yet.
func (d *DB) Contact(ctx context.Context, jid, name string, overwrite bool) error {
	if name == "" {
		return nil
	}
	query := `INSERT INTO contacts (jid, name) VALUES (?, ?) ON CONFLICT (jid) DO NOTHING`
	if overwrite {
		query = `INSERT INTO contacts (jid, name) VALUES (?, ?)
			ON CONFLICT (jid) DO UPDATE SET name = excluded.name WHERE contacts.name <> excluded.name`
	}
	_, err := d.w.ExecContext(ctx, query, jid, name)
	return err
}

type Pending struct {
	Chat, ID, Raw string
	Requested     bool
}

const openRequest = `EXISTS (SELECT 1 FROM media_requests r WHERE r.chat = m.chat AND r.id = m.id AND r.done_at IS NULL)`

// Pending lists media without a file still worth trying, requested first, leaving out the keys
// in skip. Expired media only enters when it was requested.
func (d *DB) Pending(ctx context.Context, skip map[[2]string]bool, limit int) ([]Pending, error) {
	rows, err := d.w.QueryContext(ctx, `SELECT chat, id, raw, requested FROM (
			SELECT m.chat, m.id, coalesce(m.raw, '') AS raw, `+openRequest+` AS requested, m.media_error, m.at
			FROM messages m
			WHERE m.source = 'whatsapp' AND m.media IS NULL AND m.media_mime IS NOT NULL)
		WHERE media_error IS NULL OR requested
		ORDER BY requested DESC, at DESC LIMIT ?`, limit+len(skip))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := []Pending{}
	for rows.Next() && len(list) < limit {
		var p Pending
		if err := rows.Scan(&p.Chat, &p.ID, &p.Raw, &p.Requested); err != nil {
			return nil, err
		}
		if !skip[[2]string{p.Chat, p.ID}] {
			list = append(list, p)
		}
	}
	return list, rows.Err()
}

func (d *DB) MediaSaved(ctx context.Context, chat, id, rel string, now time.Time) error {
	if _, err := d.w.ExecContext(ctx, `UPDATE messages SET media = ?, media_error = NULL WHERE chat = ? AND id = ?`,
		rel, chat, id); err != nil {
		return err
	}
	return d.CloseRequest(ctx, chat, id, now)
}

// MediaLost with code expired keeps the request open path available: only the phone has the
// file and it can still be requested. Any other code is final and closes the request.
func (d *DB) MediaLost(ctx context.Context, chat, id, code string, now time.Time) error {
	if _, err := d.w.ExecContext(ctx, `UPDATE messages SET media_error = ? WHERE chat = ? AND id = ?`,
		code, chat, id); err != nil {
		return err
	}
	if code == "expired" {
		return nil
	}
	return d.CloseRequest(ctx, chat, id, now)
}

// CloseRequest ends an open request without touching the message: each request reaches the
// phone once, and asking again goes through RequestMedia and its caps.
func (d *DB) CloseRequest(ctx context.Context, chat, id string, now time.Time) error {
	_, err := d.w.ExecContext(ctx, `UPDATE media_requests SET done_at = ? WHERE chat = ? AND id = ? AND done_at IS NULL`,
		now.UTC().Format(TimeLayout), chat, id)
	return err
}

const (
	MediaPerCall = 20
	MediaPerDay  = 200
)

type MediaRequest struct {
	Requested      int  `json:"requested"`
	RemainingToday int  `json:"remaining_today"`
	Capped         bool `json:"capped"`
}

// RequestMedia marks media of a chat for the phone to send again. Each request shows up as a
// notification on the phone, hence the caps: MediaPerCall per call and MediaPerDay in 24 h.
// id and since (UTC, TimeLayout) empty do not filter.
func (d *DB) RequestMedia(ctx context.Context, chat, id, since string, now time.Time) (MediaRequest, error) {
	var r MediaRequest
	tx, err := d.w.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	var used int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM media_requests WHERE requested_at > ?`,
		now.Add(-24*time.Hour).UTC().Format(TimeLayout)).Scan(&used); err != nil {
		return r, err
	}
	allowed := min(MediaPerCall, max(MediaPerDay-used, 0))
	rows, err := tx.QueryContext(ctx, `SELECT m.chat, m.id FROM messages m
		WHERE m.chat = ? AND (? = '' OR m.id = ?) AND (? = '' OR m.at >= ?)
			AND m.source = 'whatsapp' AND m.media IS NULL AND m.media_mime IS NOT NULL
			AND (m.media_error IS NULL OR m.media_error = 'expired') AND NOT `+openRequest+`
		ORDER BY m.at DESC LIMIT ?`, chat, id, id, since, since, allowed+1)
	if err != nil {
		return r, err
	}
	var keys [][2]string
	for rows.Next() {
		var k [2]string
		if err := rows.Scan(&k[0], &k[1]); err != nil {
			rows.Close()
			return r, err
		}
		keys = append(keys, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return r, err
	}
	if allowed == 0 && len(keys) > 0 {
		return r, ErrRateLimited
	}
	r.Capped = len(keys) > allowed
	keys = keys[:min(len(keys), allowed)]
	at := now.UTC().Format(TimeLayout)
	for _, k := range keys {
		if _, err := tx.ExecContext(ctx, `INSERT INTO media_requests (chat, id, requested_at) VALUES (?, ?, ?)`, k[0], k[1], at); err != nil {
			return r, err
		}
	}
	if err := tx.Commit(); err != nil {
		return r, err
	}
	r.Requested = len(keys)
	r.RemainingToday = MediaPerDay - used - len(keys)
	return r, nil
}
