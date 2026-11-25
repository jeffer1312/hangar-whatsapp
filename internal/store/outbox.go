package store

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const (
	OutboxPending  = "pending"
	OutboxSending  = "sending"
	OutboxSent     = "sent"
	OutboxDeclined = "declined"
	OutboxExpired  = "expired"
	OutboxFailed   = "failed"
)

const (
	DraftTTL        = 10 * time.Minute
	DraftsPerOrigin = 3
	DraftsTotal     = 10
	SendsPerMinute  = 3
	SendsPerDay     = 20
)

var (
	ErrDraftNotFound = errors.New("rascunho não encontrado")
	ErrExpired       = errors.New("o rascunho expirou")
	ErrAnswered      = errors.New("o rascunho já foi respondido")
	ErrTooManyDrafts = errors.New("rascunhos abertos demais")
	ErrSendLimit     = errors.New("limite de envios atingido")
	ErrUnknownChat   = errors.New("conversa fora do arquivo")
	ErrNewContact    = errors.New("contato novo")
)

type DraftFile struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Source     string `json:"source"`
	OutsideCwd bool   `json:"outside_cwd"`
}

type Recent struct {
	FromMe bool   `json:"from_me"`
	Sender string `json:"sender"`
	At     string `json:"at"`
	Text   string `json:"text"`
}

type Draft struct {
	ID             string     `json:"id"`
	Origin         string     `json:"origin"`
	Chat           string     `json:"chat"`
	ChatKind       string     `json:"chat_kind"`
	ChatNumber     string     `json:"chat_number"`
	ChatName       string     `json:"chat_name"`
	ChatNameSource string     `json:"chat_name_source"`
	Recent         []Recent   `json:"recent"`
	Text           string     `json:"text"`
	ReplyTo        *string    `json:"reply_to"`
	QuotedText     *string    `json:"quoted_text"`
	File           *DraftFile `json:"file"`
	CreatedAt      string     `json:"created_at"`
	ExpiresAt      string     `json:"expires_at"`
	State          string     `json:"state"`
	Via            *string    `json:"via"`
	AnsweredAt     *string    `json:"answered_at"`
	MessageID      *string    `json:"message_id"`
	Error          *string    `json:"error"`
	WACode         *int       `json:"wa_code"`
	// Address is where the message goes; QuotedRaw is the raw column of the quoted message.
	Address   string `json:"-"`
	QuotedRaw string `json:"-"`
}

type ChatInfo struct {
	Address, Name string
	Recent        []Recent
}

const draftColumns = `id, origin, chat, address, chat_kind, chat_number, chat_name, chat_name_source, recent, text,
	reply_to, quoted_text, quoted_raw, file_name, file_size, file_sha256, file_source, file_outside_cwd,
	created_at, expires_at, state, via, answered_at, message_id, error, wa_code`

type rowScanner interface {
	Scan(dest ...any) error
}

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func scanDraft(row rowScanner) (Draft, error) {
	var dr Draft
	var recent string
	var quotedRaw, fileName, fileSHA, fileSource sql.NullString
	var fileSize, waCode sql.NullInt64
	var outside sql.NullBool
	err := row.Scan(&dr.ID, &dr.Origin, &dr.Chat, &dr.Address, &dr.ChatKind, &dr.ChatNumber, &dr.ChatName, &dr.ChatNameSource,
		&recent, &dr.Text, &dr.ReplyTo, &dr.QuotedText, &quotedRaw, &fileName, &fileSize, &fileSHA, &fileSource, &outside,
		&dr.CreatedAt, &dr.ExpiresAt, &dr.State, &dr.Via, &dr.AnsweredAt, &dr.MessageID, &dr.Error, &waCode)
	if errors.Is(err, sql.ErrNoRows) {
		return dr, ErrDraftNotFound
	} else if err != nil {
		return dr, err
	}
	if err := json.Unmarshal([]byte(recent), &dr.Recent); err != nil {
		return dr, err
	}
	if dr.Recent == nil {
		dr.Recent = []Recent{}
	}
	dr.QuotedRaw = quotedRaw.String
	if fileName.Valid {
		dr.File = &DraftFile{Name: fileName.String, Size: fileSize.Int64, SHA256: fileSHA.String, Source: fileSource.String,
			OutsideCwd: outside.Bool}
	}
	if waCode.Valid {
		code := int(waCode.Int64)
		dr.WACode = &code
	}
	return dr, nil
}

func getDraft(ctx context.Context, q querier, id string) (Draft, error) {
	return scanDraft(q.QueryRowContext(ctx, `SELECT `+draftColumns+` FROM outbox WHERE id = ?`, id))
}

func dedupKey(dr Draft) string {
	sha := ""
	if dr.File != nil {
		sha = dr.File.SHA256
	}
	sum := sha256.Sum256([]byte(strings.Join([]string{dr.Origin, dr.Chat, dr.Text, sha}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func expire(ctx context.Context, ex execer, now time.Time) (int64, error) {
	res, err := ex.ExecContext(ctx, `UPDATE outbox SET state = 'expired' WHERE state = 'pending' AND expires_at <= ?`,
		now.UTC().Format(TimeLayout))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ExpireDrafts closes the pending drafts past their deadline and says how many.
func (d *DB) ExpireDrafts(ctx context.Context, now time.Time) (int64, error) {
	return expire(ctx, d.w, now)
}

// CreateDraft stores a pending draft, or returns the pending one with the same origin, chat, text and
// attachment (false). Both happen in one IMMEDIATE transaction with the caps on open drafts.
func (d *DB) CreateDraft(ctx context.Context, dr Draft, now time.Time) (Draft, bool, error) {
	tx, err := d.w.BeginTx(ctx, nil)
	if err != nil {
		return Draft{}, false, err
	}
	defer tx.Rollback()
	if _, err := expire(ctx, tx, now); err != nil {
		return Draft{}, false, err
	}
	key := dedupKey(dr)
	twin, err := scanDraft(tx.QueryRowContext(ctx, `SELECT `+draftColumns+` FROM outbox WHERE dedup = ? AND state = 'pending'`, key))
	if err == nil {
		return twin, false, tx.Commit()
	} else if !errors.Is(err, ErrDraftNotFound) {
		return Draft{}, false, err
	}
	var mine, all int
	if err := tx.QueryRowContext(ctx, `SELECT count(CASE WHEN origin = ? THEN 1 END), count(*) FROM outbox WHERE state = 'pending'`,
		dr.Origin).Scan(&mine, &all); err != nil {
		return Draft{}, false, err
	}
	if mine >= DraftsPerOrigin || all >= DraftsTotal {
		return Draft{}, false, ErrTooManyDrafts
	}
	if dr.Recent == nil {
		dr.Recent = []Recent{}
	}
	recent, err := json.Marshal(dr.Recent)
	if err != nil {
		return Draft{}, false, err
	}
	// Without a file the five columns bind nil, which is NULL.
	var name, size, sha, source, outside any
	if f := dr.File; f != nil {
		name, size, sha, source, outside = f.Name, f.Size, f.SHA256, f.Source, f.OutsideCwd
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox (id, dedup, origin, chat, address, chat_kind, chat_number, chat_name,
			chat_name_source, recent, text, reply_to, quoted_text, quoted_raw, file_name, file_size, file_sha256, file_source,
			file_outside_cwd, created_at, expires_at, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, nullif(?, ''), ?, ?, ?, ?, ?, ?, ?, 'pending')`,
		dr.ID, key, dr.Origin, dr.Chat, dr.Address, dr.ChatKind, dr.ChatNumber, dr.ChatName, dr.ChatNameSource, string(recent),
		dr.Text, dr.ReplyTo, dr.QuotedText, dr.QuotedRaw, name, size, sha, source, outside,
		now.UTC().Format(TimeLayout), now.Add(DraftTTL).UTC().Format(TimeLayout)); err != nil {
		return Draft{}, false, err
	}
	created, err := getDraft(ctx, tx, dr.ID)
	if err != nil {
		return Draft{}, false, err
	}
	return created, true, tx.Commit()
}

// Draft and Drafts only read: callers run ExpireDrafts first with their clock.
func (d *DB) Draft(ctx context.Context, id string) (Draft, error) {
	var dr Draft
	err := d.read(ctx, func(ctx context.Context) (err error) {
		dr, err = getDraft(ctx, d.r, id)
		return err
	})
	return dr, err
}

// Drafts lists the newest 100 drafts in a state.
func (d *DB) Drafts(ctx context.Context, state string) ([]Draft, error) {
	list := []Draft{}
	err := d.read(ctx, func(ctx context.Context) error {
		rows, err := d.r.QueryContext(ctx, `SELECT `+draftColumns+` FROM outbox WHERE state = ? ORDER BY created_at DESC, id DESC LIMIT 100`, state)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			dr, err := scanDraft(rows)
			if err != nil {
				return err
			}
			list = append(list, dr)
		}
		return rows.Err()
	})
	return list, err
}

// StateErr is why a draft in this state cannot be answered, nil while it is pending.
func StateErr(state string) error {
	switch state {
	case OutboxPending:
		return nil
	case OutboxExpired:
		return ErrExpired
	}
	return ErrAnswered
}

// pendingInTx expires what is due and returns the draft only while it is still pending.
func pendingInTx(ctx context.Context, tx *sql.Tx, id string, now time.Time) (Draft, error) {
	if _, err := expire(ctx, tx, now); err != nil {
		return Draft{}, err
	}
	dr, err := getDraft(ctx, tx, id)
	if err != nil {
		return dr, err
	}
	return dr, StateErr(dr.State)
}

const sendWindow = `SELECT count(CASE WHEN answered_at > ? THEN 1 END), count(*) FROM outbox
	WHERE state IN ('sending', 'sent', 'failed') AND answered_at > ?`

func sendCounts(ctx context.Context, q querier, now time.Time) (minute, day int, err error) {
	err = q.QueryRowContext(ctx, sendWindow, now.Add(-time.Minute).UTC().Format(TimeLayout),
		now.Add(-24*time.Hour).UTC().Format(TimeLayout)).Scan(&minute, &day)
	return minute, day, err
}

// CountSends is how many drafts were approved for sending in the last minute and the last 24 h.
func (d *DB) CountSends(ctx context.Context, now time.Time) (minute, day int, err error) {
	err = d.read(ctx, func(ctx context.Context) (err error) {
		minute, day, err = sendCounts(ctx, d.r, now)
		return err
	})
	return minute, day, err
}

// answer runs change on a draft that is still pending, in one IMMEDIATE transaction with the check.
// An expiry found on the way is kept even though the answer is refused.
func (d *DB) answer(ctx context.Context, id string, now time.Time, change func(tx *sql.Tx) error) (Draft, error) {
	tx, err := d.w.BeginTx(ctx, nil)
	if err != nil {
		return Draft{}, err
	}
	defer tx.Rollback()
	if _, err := pendingInTx(ctx, tx, id, now); err != nil {
		if errors.Is(err, ErrExpired) {
			return Draft{}, errors.Join(err, tx.Commit())
		}
		return Draft{}, err
	}
	if err := change(tx); err != nil {
		return Draft{}, err
	}
	dr, err := getDraft(ctx, tx, id)
	if err != nil {
		return Draft{}, err
	}
	return dr, tx.Commit()
}

// ClaimSend moves a pending draft to sending. The caps and the change share the transaction, so two
// approvals at once cannot both pass the count or both win the same draft.
func (d *DB) ClaimSend(ctx context.Context, id, via string, now time.Time) (Draft, error) {
	return d.answer(ctx, id, now, func(tx *sql.Tx) error {
		minute, day, err := sendCounts(ctx, tx, now)
		if err != nil {
			return err
		}
		if minute >= SendsPerMinute || day >= SendsPerDay {
			return ErrSendLimit
		}
		_, err = tx.ExecContext(ctx, `UPDATE outbox SET state = 'sending', via = ?, answered_at = ? WHERE id = ?`,
			via, now.UTC().Format(TimeLayout), id)
		return err
	})
}

func (d *DB) Decline(ctx context.Context, id string, now time.Time) (Draft, error) {
	return d.answer(ctx, id, now, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE outbox SET state = 'declined', answered_at = ? WHERE id = ?`,
			now.UTC().Format(TimeLayout), id)
		return err
	})
}

// FinishSent records the message id and puts the sent message in the archive, with media as the
// stored path of the attachment when it was moved into media/.
func (d *DB) FinishSent(ctx context.Context, id string, r Record, media string) error {
	tx, err := d.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE outbox SET state = 'sent', message_id = ? WHERE id = ? AND state = 'sending'`,
		r.ID, id); err != nil {
		return err
	}
	if err := save(ctx, tx, r); err != nil {
		return err
	}
	if media != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET media = ? WHERE chat = ? AND id = ?`, media, r.Chat, r.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// MarkSent records a send that went out when FinishSent could not also archive it: the draft must
// not look failed, or the owner may approve it again.
func (d *DB) MarkSent(ctx context.Context, id, messageID string) error {
	_, err := d.w.ExecContext(ctx, `UPDATE outbox SET state = 'sent', message_id = ? WHERE id = ? AND state = 'sending'`,
		messageID, id)
	return err
}

// FinishFailed closes a send that did not go out; waCode 0 means WhatsApp gave no code.
func (d *DB) FinishFailed(ctx context.Context, id, reason string, waCode int) error {
	_, err := d.w.ExecContext(ctx, `UPDATE outbox SET state = 'failed', error = ?, wa_code = nullif(?, 0)
		WHERE id = ? AND state = 'sending'`, reason, waCode, id)
	return err
}

// InterruptSending fails the sends a previous run left halfway; they are never retried.
func (d *DB) InterruptSending(ctx context.Context) (int64, error) {
	res, err := d.w.ExecContext(ctx, `UPDATE outbox SET state = 'failed', error = 'interrupted' WHERE state = 'sending'`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// OutboxChat is what a draft needs from the archive: where to send, the name and the last messages.
// A chat with no message from the other side is a new contact.
func (d *DB) OutboxChat(ctx context.Context, chat string) (ChatInfo, error) {
	var info ChatInfo
	err := d.read(ctx, func(ctx context.Context) error {
		var address sql.NullString
		var incoming bool
		if err := d.r.QueryRowContext(ctx, `SELECT
				(SELECT coalesce(nullif(iif(json_valid(raw), json_extract(raw, '$.chat'), NULL), ''), chat)
					FROM messages WHERE chat = ?1 ORDER BY at DESC, id DESC LIMIT 1),
				EXISTS (SELECT 1 FROM messages WHERE chat = ?1 AND from_me = 0),
				coalesce((SELECT name FROM contacts WHERE jid = ?1), '')`, chat).Scan(&address, &incoming, &info.Name); err != nil {
			return err
		}
		if !address.Valid {
			return ErrUnknownChat
		}
		if !incoming {
			return ErrNewContact
		}
		info.Address = address.String
		rows, err := d.r.QueryContext(ctx, `SELECT m.from_me, m.sender, coalesce(s.name, ''), m.at, m.text
			FROM messages m LEFT JOIN contacts s ON s.jid = m.sender
			WHERE m.chat = ? ORDER BY m.at DESC, m.id DESC LIMIT 3`, chat)
		if err != nil {
			return err
		}
		defer rows.Close()
		info.Recent = []Recent{}
		for rows.Next() {
			var r Recent
			var sender, name string
			if err := rows.Scan(&r.FromMe, &sender, &name, &r.At, &r.Text); err != nil {
				return err
			}
			if !r.FromMe {
				user, _, _ := strings.Cut(sender, "@")
				r.Sender = cmp.Or(name, user)
			}
			info.Recent = append([]Recent{r}, info.Recent...)
		}
		return rows.Err()
	})
	return info, err
}

// Quoted returns the text and the raw column of a message to reply to.
func (d *DB) Quoted(ctx context.Context, chat, id string) (string, string, error) {
	var text, raw string
	err := d.read(ctx, func(ctx context.Context) error {
		err := d.r.QueryRowContext(ctx, `SELECT text, coalesce(raw, '') FROM messages WHERE chat = ? AND id = ?`, chat, id).
			Scan(&text, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	return text, raw, err
}

const reachoutKey = "reachout_locked_until"

// ReachoutLock is until when WhatsApp forbids messages to new contacts, empty without a lock.
func (d *DB) ReachoutLock(ctx context.Context) (string, error) {
	var until string
	err := d.read(ctx, func(ctx context.Context) error {
		err := d.r.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, reachoutKey).Scan(&until)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	return until, err
}

// ExtendReachoutLock records a lock end unless a later one is already recorded.
func (d *DB) ExtendReachoutLock(ctx context.Context, until string) error {
	_, err := d.w.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE excluded.value > settings.value`, reachoutKey, until)
	return err
}

func (d *DB) SetReachoutLock(ctx context.Context, until string) error {
	if until == "" {
		_, err := d.w.ExecContext(ctx, `DELETE FROM settings WHERE key = ?`, reachoutKey)
		return err
	}
	_, err := d.w.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, reachoutKey, until)
	return err
}
