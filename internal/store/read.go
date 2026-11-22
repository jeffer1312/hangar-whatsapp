package store

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
)

var ErrNotFound = errors.New("not found")

type Chat struct {
	Chat     string `json:"chat"`
	Name     string `json:"name"`
	IsGroup  bool   `json:"is_group"`
	Messages int    `json:"messages"`
	FirstAt  string `json:"first_at"`
	LastAt   string `json:"last_at"`
}

type Message struct {
	ID string `json:"id"`
	// Only in search results.
	Chat       string  `json:"chat,omitempty"`
	At         string  `json:"at"`
	FromMe     bool    `json:"from_me"`
	Sender     string  `json:"sender"`
	SenderName string  `json:"sender_name"`
	Kind       string  `json:"kind"`
	Text       string  `json:"text"`
	MediaPath  *string `json:"media_path"`
	MediaError *string `json:"media_error"`
	ReplyTo    *string `json:"reply_to"`
}

type MessagesQuery struct {
	Chat, Since, Until, BeforeID string
	Limit                        int
}

type MessagesPage struct {
	Chat     string    `json:"chat"`
	Name     string    `json:"name"`
	Messages []Message `json:"messages"`
	More     bool      `json:"more"`
}

type Totals struct {
	Messages       int `json:"messages"`
	Chats          int `json:"chats"`
	Groups         int `json:"groups"`
	MediaSaved     int `json:"media_saved"`
	MediaPhoneOnly int `json:"media_phone_only"`
	MediaLost      int `json:"media_lost"`
}

type Summary struct {
	Totals        Totals
	Since         string
	LastMessageAt string
}

// read runs fn on the read-only pool with the query deadline; a query past it is ErrBusy.
func (d *DB) read(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, d.QueryTimeout)
	defer cancel()
	err := fn(ctx)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ErrBusy
	}
	return err
}

func likePattern(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + r.Replace(s) + "%"
}

func (d *DB) Chats(ctx context.Context, q string, limit int) ([]Chat, error) {
	out := []Chat{}
	err := d.read(ctx, func(ctx context.Context) error {
		rows, err := d.r.QueryContext(ctx, `SELECT chat, name, messages, first_at, last_at FROM chats
			WHERE ? = '' OR name LIKE ? ESCAPE '\' OR chat LIKE ? ESCAPE '\'
			ORDER BY last_at DESC LIMIT ?`, q, likePattern(q), likePattern(q), limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c Chat
			if err := rows.Scan(&c.Chat, &c.Name, &c.Messages, &c.FirstAt, &c.LastAt); err != nil {
				return err
			}
			c.IsGroup = strings.HasSuffix(c.Chat, "@g.us")
			out = append(out, c)
		}
		return rows.Err()
	})
	return out, err
}

const messageColumns = `m.chat, m.id, m.at, m.from_me, m.sender, coalesce(s.name, ''), m.kind, m.text,
	m.media, m.media_error, nullif(m.reply_to, '')`

const messageFrom = `FROM messages m LEFT JOIN contacts s ON s.jid = m.sender`

func (d *DB) scanMessages(rows *sql.Rows) ([]Message, error) {
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		var m Message
		var media sql.NullString
		if err := rows.Scan(&m.Chat, &m.ID, &m.At, &m.FromMe, &m.Sender, &m.SenderName, &m.Kind, &m.Text,
			&media, &m.MediaError, &m.ReplyTo); err != nil {
			return nil, err
		}
		if media.Valid {
			if abs, ok := MediaAbs(d.dir, media.String); ok {
				m.MediaPath = &abs
			}
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Messages returns the newest Limit messages up to Until or before BeforeID, oldest first.
func (d *DB) Messages(ctx context.Context, q MessagesQuery) (MessagesPage, error) {
	page := MessagesPage{Chat: q.Chat, Messages: []Message{}}
	err := d.read(ctx, func(ctx context.Context) error {
		if err := d.r.QueryRowContext(ctx, `SELECT coalesce((SELECT name FROM contacts WHERE jid = ?), '')`, q.Chat).
			Scan(&page.Name); err != nil {
			return err
		}
		beforeAt := ""
		if q.BeforeID != "" {
			err := d.r.QueryRowContext(ctx, `SELECT at FROM messages WHERE chat = ? AND id = ?`, q.Chat, q.BeforeID).Scan(&beforeAt)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			} else if err != nil {
				return err
			}
		}
		rows, err := d.r.QueryContext(ctx, `SELECT `+messageColumns+` `+messageFrom+`
			WHERE m.chat = ? AND (? = '' OR m.at >= ?) AND (? = '' OR m.at <= ?)
				AND (? = '' OR m.at < ? OR (m.at = ? AND m.id < ?))
			ORDER BY m.at DESC, m.id DESC LIMIT ?`,
			q.Chat, q.Since, q.Since, q.Until, q.Until, beforeAt, beforeAt, beforeAt, q.BeforeID, q.Limit+1)
		if err != nil {
			return err
		}
		list, err := d.scanMessages(rows)
		if err != nil {
			return err
		}
		if len(list) > q.Limit {
			page.More = true
			list = list[:q.Limit]
		}
		slices.Reverse(list)
		for i := range list {
			list[i].Chat = ""
		}
		page.Messages = list
		return nil
	})
	return page, err
}

func (d *DB) Search(ctx context.Context, q, chat string, limit int) ([]Message, error) {
	var out []Message
	err := d.read(ctx, func(ctx context.Context) error {
		rows, err := d.r.QueryContext(ctx, `SELECT `+messageColumns+` `+messageFrom+`
			WHERE m.text LIKE ? ESCAPE '\' AND (? = '' OR m.chat = ?)
			ORDER BY m.at DESC, m.id DESC LIMIT ?`, likePattern(q), chat, chat, limit)
		if err != nil {
			return err
		}
		out, err = d.scanMessages(rows)
		return err
	})
	return out, err
}

func (d *DB) Totals(ctx context.Context) (Summary, error) {
	var s Summary
	err := d.read(ctx, func(ctx context.Context) error {
		var since, last sql.NullString
		err := d.r.QueryRowContext(ctx, `SELECT count(*), count(DISTINCT chat), count(DISTINCT CASE WHEN chat LIKE '%@g.us' THEN chat END),
				count(media), count(CASE WHEN media IS NULL AND media_error = 'expired' THEN 1 END),
				count(CASE WHEN media IS NULL AND media_error <> 'expired' THEN 1 END),
				min(at), max(at)
			FROM messages`).Scan(&s.Totals.Messages, &s.Totals.Chats, &s.Totals.Groups, &s.Totals.MediaSaved,
			&s.Totals.MediaPhoneOnly, &s.Totals.MediaLost, &since, &last)
		s.Since, s.LastMessageAt = since.String, last.String
		return err
	})
	return s, err
}
