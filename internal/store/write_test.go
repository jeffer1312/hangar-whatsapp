package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMediaTheProchatLostGetsAnotherChance(t *testing.T) {
	d := openTest(t)
	ctx := context.Background()
	exec(t, d, `INSERT INTO messages (chat, id, from_me, at, kind, text, media_error, source) VALUES
		(?, 'A1', 0, '2025-01-01T10:00:00Z', 'text', 'Mídia não disponível', 'unavailable', 'prochat'),
		(?, 'T1', 0, '2025-01-01T10:01:00Z', 'text', 'texto do prochat', NULL, 'prochat')`, chatA, chatA)
	at := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	for _, r := range []Record{
		{Chat: chatA, ID: "A1", At: at, Kind: "image", Text: "legenda", Mime: "image/jpeg", Raw: "{}"},
		{Chat: chatA, ID: "T1", At: at, Kind: "text", Text: "outro", Raw: "{}"},
	} {
		if err := d.Save(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	list, err := d.Pending(ctx, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != "A1" {
		t.Fatalf("Pending = %+v, want only A1", list)
	}
	var text string
	if err := d.w.QueryRow(`SELECT text FROM messages WHERE id = 'T1'`).Scan(&text); err != nil || text != "texto do prochat" {
		t.Fatalf("text row was overwritten: %q %v", text, err)
	}
	if err := d.MediaSaved(ctx, chatA, "A1", "media/556499990000/A1.jpg", at); err != nil {
		t.Fatal(err)
	}
	if list, _ = d.Pending(ctx, nil, 10); len(list) != 0 {
		t.Fatalf("saved media still pending: %+v", list)
	}
}

func TestOnlyRequestedExpiredMediaGoesToThePhone(t *testing.T) {
	d := openTest(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	exec(t, d, `INSERT INTO messages (chat, id, from_me, at, kind, media_mime, media_error, source, raw) VALUES
		(?, 'A', 0, '2025-01-03T10:00:00Z', 'image', 'image/jpeg', 'expired', 'whatsapp', '{}'),
		(?, 'B', 0, '2025-01-02T10:00:00Z', 'image', 'image/jpeg', 'expired', 'whatsapp', '{}'),
		(?, 'C', 0, '2025-01-01T10:00:00Z', 'image', 'image/jpeg', NULL, 'whatsapp', '{}')`, chatA, chatA, chatA)
	pending := func() []string {
		t.Helper()
		list, err := d.Pending(ctx, nil, 10)
		if err != nil {
			t.Fatal(err)
		}
		out := []string{}
		for _, p := range list {
			out = append(out, fmt.Sprintf("%s:%v", p.ID, p.Requested))
		}
		return out
	}
	if got := pending(); !equal(got, []string{"C:false"}) {
		t.Fatalf("without a request, queue = %v; want only C", got)
	}
	r, err := d.RequestMedia(ctx, chatA, "A", "", now)
	if err != nil || r.Requested != 1 {
		t.Fatalf("RequestMedia A = %+v, %v", r, err)
	}
	if got := pending(); !equal(got, []string{"A:true", "C:false"}) {
		t.Fatalf("with A requested, queue = %v; want [A:true C:false]", got)
	}
	if err := d.MediaSaved(ctx, chatA, "A", "media/x/A.jpg", now); err != nil {
		t.Fatal(err)
	}
	if got := pending(); !equal(got, []string{"C:false"}) {
		t.Fatalf("after saving A, queue = %v; want only C", got)
	}
	var open int
	if err := d.w.QueryRow(`SELECT count(*) FROM media_requests WHERE done_at IS NULL`).Scan(&open); err != nil || open != 0 {
		t.Fatalf("request for A still open: %d %v", open, err)
	}
}

func TestRequestMediaCaps(t *testing.T) {
	d := openTest(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	for i := range 25 {
		exec(t, d, `INSERT INTO messages (chat, id, from_me, at, kind, media_mime, media_error, source, raw)
			VALUES (?, ?, 0, ?, 'image', 'image/jpeg', 'expired', 'whatsapp', '{}')`,
			chatA, fmt.Sprintf("E%02d", i), time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC).Format(TimeLayout))
	}
	exec(t, d, `INSERT INTO messages (chat, id, from_me, at, kind, media_mime, media_error, source)
		VALUES (?, 'LOST', 0, '2026-02-01T00:00:00Z', 'image', 'image/jpeg', 'phone_refused', 'whatsapp'),
		       (?, 'OLD', 0, '2026-02-01T00:00:00Z', 'image', 'image/jpeg', 'unavailable', 'prochat')`, chatA, chatA)

	first, err := d.RequestMedia(ctx, chatA, "", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if first != (MediaRequest{Requested: 20, RemainingToday: 180, Capped: true}) {
		t.Fatalf("first call = %+v, want 20 requested, 180 left, capped", first)
	}
	second, err := d.RequestMedia(ctx, chatA, "", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if second != (MediaRequest{Requested: 5, RemainingToday: 175, Capped: false}) {
		t.Fatalf("second call = %+v, want the 5 left of the 25", second)
	}

	// 175 more in the last 24 h use up the day; one older than 24 h does not count.
	exec(t, d, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 175)
		INSERT INTO media_requests (chat, id, requested_at, done_at) SELECT 'x', 'y' || i, ?, ? FROM n`,
		now.Add(-time.Hour).Format(TimeLayout), now.Format(TimeLayout))
	exec(t, d, `INSERT INTO media_requests (chat, id, requested_at, done_at) VALUES ('x', 'old', ?, ?)`,
		now.Add(-25*time.Hour).Format(TimeLayout), now.Format(TimeLayout))
	exec(t, d, `INSERT INTO messages (chat, id, from_me, at, kind, media_mime, source, raw)
		VALUES (?, 'NEW', 0, '2026-03-01T00:00:00Z', 'image', 'image/jpeg', 'whatsapp', '{}')`, chatA)
	if _, err := d.RequestMedia(ctx, chatA, "NEW", "", now); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("request past 200 a day = %v, want ErrRateLimited", err)
	}
	later, err := d.RequestMedia(ctx, chatA, "NEW", "", now.Add(25*time.Hour))
	if err != nil || later.Requested != 1 {
		t.Fatalf("request after the window moved = %+v, %v", later, err)
	}
}

func TestRequestMediaFiltersByIDAndSince(t *testing.T) {
	d := openTest(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	exec(t, d, `INSERT INTO messages (chat, id, from_me, at, kind, media_mime, media_error, source, raw) VALUES
		(?, 'OLD', 0, '2026-10-01T00:00:00Z', 'image', 'image/jpeg', 'expired', 'whatsapp', '{}'),
		(?, 'NEW', 0, '2026-11-01T00:00:00Z', 'image', 'image/jpeg', 'expired', 'whatsapp', '{}')`, chatA, chatA)
	if r, err := d.RequestMedia(ctx, chatA, "", "2026-10-15T00:00:00Z", now); err != nil || r.Requested != 1 {
		t.Fatalf("since = %+v, %v; want only NEW", r, err)
	}
	if r, err := d.RequestMedia(ctx, chatA, "OLD", "", now); err != nil || r.Requested != 1 {
		t.Fatalf("id OLD = %+v, %v", r, err)
	}
	if r, err := d.RequestMedia(ctx, chatA, "OLD", "", now); err != nil || r.Requested != 0 {
		t.Fatalf("OLD requested twice while still open = %+v, %v", r, err)
	}
}
