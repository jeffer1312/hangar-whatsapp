package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const (
	AvatarOK     = "ok"
	AvatarNone   = "none"
	AvatarHidden = "hidden"
)

const (
	AvatarTTL           = 24 * time.Hour
	AvatarFetchesPerDay = 30
)

var ErrAvatarFresh = errors.New("foto em cache")

type Avatar struct {
	PictureID, State string
	Image            []byte
	FetchedAt        string
}

// ClaimAvatarFetch says whether the picture of jid, as account sees it, should be asked for now, and
// with which known picture id. Each claim counts towards the daily cap, failed fetches included.
func (d *DB) ClaimAvatarFetch(ctx context.Context, account, jid string, now time.Time) (string, error) {
	tx, err := d.w.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	day := now.Add(-AvatarTTL).UTC().Format(TimeLayout)
	var id, state, fetched string
	err = tx.QueryRowContext(ctx, `SELECT picture_id, state, fetched_at FROM avatars WHERE account = ? AND jid = ?`, account, jid).
		Scan(&id, &state, &fetched)
	switch {
	case err == nil && fetched > day:
		return "", ErrAvatarFresh
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM avatar_fetches WHERE at <= ?`, day); err != nil {
		return "", err
	}
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM avatar_fetches`).Scan(&n); err != nil {
		return "", err
	}
	if n >= AvatarFetchesPerDay {
		return "", ErrRateLimited
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO avatar_fetches (at) VALUES (?)`, now.UTC().Format(TimeLayout)); err != nil {
		return "", err
	}
	if state != AvatarOK {
		id = ""
	}
	return id, tx.Commit()
}

func (d *DB) SaveAvatar(ctx context.Context, account, jid string, a Avatar, now time.Time) error {
	_, err := d.w.ExecContext(ctx, `INSERT INTO avatars (account, jid, picture_id, state, image, fetched_at) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (account, jid) DO UPDATE SET picture_id = excluded.picture_id, state = excluded.state, image = excluded.image,
			fetched_at = excluded.fetched_at`, account, jid, a.PictureID, a.State, a.Image, now.UTC().Format(TimeLayout))
	return err
}

// RenewAvatar keeps the stored picture for another day: WhatsApp said it did not change.
func (d *DB) RenewAvatar(ctx context.Context, account, jid string, now time.Time) error {
	_, err := d.w.ExecContext(ctx, `UPDATE avatars SET fetched_at = ? WHERE account = ? AND jid = ?`,
		now.UTC().Format(TimeLayout), account, jid)
	return err
}

func (d *DB) Avatar(ctx context.Context, account, jid string) (Avatar, error) {
	var a Avatar
	err := d.read(ctx, func(ctx context.Context) error {
		err := d.r.QueryRowContext(ctx, `SELECT picture_id, state, image, fetched_at FROM avatars WHERE account = ? AND jid = ?`,
			account, jid).Scan(&a.PictureID, &a.State, &a.Image, &a.FetchedAt)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	return a, err
}

// ChatAvatar is the cached picture of jid as account sees it, nil without one.
func (d *DB) ChatAvatar(ctx context.Context, account, jid string) ([]byte, error) {
	a, err := d.Avatar(ctx, account, jid)
	switch {
	case errors.Is(err, ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	case a.State != AvatarOK:
		return nil, nil
	}
	return a.Image, nil
}
