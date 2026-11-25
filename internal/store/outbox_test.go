package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

func openDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func draft(id, origin, text string) Draft {
	return Draft{ID: id, Origin: origin, Chat: chatA, Address: chatA, ChatKind: "contact", ChatNumber: "556499990000",
		Recent: []Recent{}, Text: text}
}

func create(t *testing.T, db *DB, d Draft, now time.Time) (Draft, bool) {
	t.Helper()
	got, created, err := db.CreateDraft(context.Background(), d, now)
	if err != nil {
		t.Fatalf("CreateDraft %s: %v", d.ID, err)
	}
	return got, created
}

func TestCreateDraftReturnsThePendingTwin(t *testing.T) {
	db := openDB(t)
	first, created := create(t, db, draft("A1", "srv::s1", "oi"), t0)
	if !created || first.State != "pending" || first.CreatedAt != "2026-11-24T12:00:00Z" || first.ExpiresAt != "2026-11-24T12:10:00Z" {
		t.Fatalf("first = %+v created %v", first, created)
	}
	twin, created := create(t, db, draft("A2", "srv::s1", "oi"), t0.Add(time.Minute))
	if created || twin.ID != "A1" {
		t.Fatalf("same origin, chat and text = %s created %v; want A1 returned", twin.ID, created)
	}
	withFile := draft("A3", "srv::s1", "oi")
	withFile.File = &DraftFile{Name: "a.zip", Size: 3, SHA256: "abc", Source: "/tmp/a.zip"}
	if got, created := create(t, db, withFile, t0); !created || got.ID != "A3" || got.File == nil || got.File.SHA256 != "abc" {
		t.Fatalf("same text with an attachment = %+v created %v; want a new draft", got, created)
	}
	if _, err := db.Decline(context.Background(), "A1", t0); err != nil {
		t.Fatal(err)
	}
	if got, created := create(t, db, draft("A4", "srv::s1", "oi"), t0); !created || got.ID != "A4" {
		t.Fatalf("after declining the twin = %s created %v; want a new draft", got.ID, created)
	}
}

func TestCreateDraftCaps(t *testing.T) {
	db := openDB(t)
	for i := range 3 {
		create(t, db, draft(fmt.Sprintf("O%d", i), "srv::s1", fmt.Sprint(i)), t0)
	}
	if _, _, err := db.CreateDraft(context.Background(), draft("O3", "srv::s1", "3"), t0); !errors.Is(err, ErrTooManyDrafts) {
		t.Fatalf("fourth open draft of one origin = %v, want ErrTooManyDrafts", err)
	}
	for i := range 7 {
		create(t, db, draft(fmt.Sprintf("T%d", i), fmt.Sprintf("srv::other%d", i), "x"), t0)
	}
	if _, _, err := db.CreateDraft(context.Background(), draft("T7", "srv::eleventh", "x"), t0); !errors.Is(err, ErrTooManyDrafts) {
		t.Fatalf("eleventh open draft = %v, want ErrTooManyDrafts", err)
	}
	// Expired drafts are no longer open.
	later := t0.Add(10 * time.Minute)
	if n, err := db.ExpireDrafts(context.Background(), later); err != nil || n != 10 {
		t.Fatalf("ExpireDrafts = %d, %v; want 10", n, err)
	}
	if got, created := create(t, db, draft("O4", "srv::s1", "0"), later); !created || got.ID != "O4" {
		t.Fatalf("after expiry = %s created %v", got.ID, created)
	}
}

func TestClaimSend(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	create(t, db, draft("C1", "srv::s1", "oi"), t0)
	got, err := db.ClaimSend(ctx, "C1", "desktop", t0.Add(time.Minute))
	if err != nil || got.State != "sending" || got.Via == nil || *got.Via != "desktop" || got.AnsweredAt == nil || *got.AnsweredAt != "2026-11-24T12:01:00Z" {
		t.Fatalf("ClaimSend = %+v, %v", got, err)
	}
	if _, err := db.ClaimSend(ctx, "C1", "mobile", t0.Add(time.Minute)); !errors.Is(err, ErrAnswered) {
		t.Fatalf("second send = %v, want ErrAnswered", err)
	}
	if _, err := db.Decline(ctx, "C1", t0.Add(time.Minute)); !errors.Is(err, ErrAnswered) {
		t.Fatalf("decline after send = %v, want ErrAnswered", err)
	}
	if _, err := db.ClaimSend(ctx, "NOPE", "desktop", t0); !errors.Is(err, ErrDraftNotFound) {
		t.Fatalf("unknown id = %v, want ErrDraftNotFound", err)
	}
	create(t, db, draft("C2", "srv::s1", "tarde"), t0)
	if _, err := db.ClaimSend(ctx, "C2", "desktop", t0.Add(10*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("send at expiry = %v, want ErrExpired", err)
	}
	if d, err := db.Draft(ctx, "C2"); err != nil || d.State != "expired" {
		t.Fatalf("draft after a late send = %+v, %v; want expired", d, err)
	}
}

func TestClaimSendCaps(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	for i := range 5 {
		create(t, db, draft(fmt.Sprintf("M%d", i), fmt.Sprintf("srv::s%d", i), "x"), t0)
	}
	for i := range 3 {
		if _, err := db.ClaimSend(ctx, fmt.Sprintf("M%d", i), "desktop", t0.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ClaimSend(ctx, "M3", "desktop", t0.Add(59*time.Second)); !errors.Is(err, ErrSendLimit) {
		t.Fatalf("fourth send in a minute = %v, want ErrSendLimit", err)
	}
	if d, _ := db.Draft(ctx, "M3"); d.State != "pending" {
		t.Fatalf("refused send changed the draft to %s", d.State)
	}
	if _, err := db.ClaimSend(ctx, "M3", "desktop", t0.Add(61*time.Second)); err != nil {
		t.Fatalf("send after the minute = %v", err)
	}
	if minute, day, err := db.CountSends(ctx, t0.Add(61*time.Second)); err != nil || minute != 2 || day != 4 {
		t.Fatalf("CountSends = %d, %d, %v; want 2, 4", minute, day, err)
	}
	// Twenty approved in the last 24 h, failures included, close the day; declines do not count.
	for i := range 16 {
		state := "sent"
		if i%2 == 0 {
			state = "failed"
		}
		if _, err := db.w.Exec(`INSERT INTO outbox (id, dedup, origin, chat, address, chat_kind, chat_number, recent, text,
				created_at, expires_at, state, answered_at) VALUES (?, ?, 'srv::old', ?, ?, 'contact', '1', '[]', 'x', ?, ?, ?, ?)`,
			fmt.Sprintf("D%d", i), fmt.Sprintf("D%d", i), chatA, chatA, "2026-11-24T01:00:00Z", "2026-11-24T01:10:00Z", state, "2026-11-24T01:00:00Z"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.w.Exec(`INSERT INTO outbox (id, dedup, origin, chat, address, chat_kind, chat_number, recent, text,
			created_at, expires_at, state, answered_at) VALUES ('X', 'X', 'srv::old', ?, ?, 'contact', '1', '[]', 'x', ?, ?, 'declined', ?)`,
		chatA, chatA, "2026-11-24T01:00:00Z", "2026-11-24T01:10:00Z", "2026-11-24T01:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimSend(ctx, "M4", "desktop", t0.Add(5*time.Minute)); !errors.Is(err, ErrSendLimit) {
		t.Fatalf("21st send in 24 h = %v, want ErrSendLimit", err)
	}
}

func TestClaimSendRace(t *testing.T) {
	db := openDB(t)
	create(t, db, draft("R1", "srv::s1", "oi"), t0)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			_, err := db.ClaimSend(context.Background(), "R1", "desktop", t0)
			results <- err
		})
	}
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, ErrAnswered):
			t.Fatalf("losing send = %v, want ErrAnswered", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d sends won the same draft, want 1", won)
	}
}

func TestDecline(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	create(t, db, draft("N1", "srv::s1", "oi"), t0)
	d, err := db.Decline(ctx, "N1", t0.Add(time.Minute))
	if err != nil || d.State != "declined" || d.AnsweredAt == nil || d.Via != nil {
		t.Fatalf("Decline = %+v, %v", d, err)
	}
	if _, err := db.Decline(ctx, "N1", t0); !errors.Is(err, ErrAnswered) {
		t.Fatalf("second decline = %v, want ErrAnswered", err)
	}
	create(t, db, draft("N2", "srv::s1", "x"), t0)
	if _, err := db.Decline(ctx, "N2", t0.Add(11*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("late decline = %v, want ErrExpired", err)
	}
	if _, err := db.Decline(ctx, "NOPE", t0); !errors.Is(err, ErrDraftNotFound) {
		t.Fatalf("unknown decline = %v, want ErrDraftNotFound", err)
	}
}

func TestFinishAndInterrupt(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	for _, id := range []string{"F1", "F2", "F3"} {
		create(t, db, draft(id, "srv::"+id, "oi"), t0)
		if _, err := db.ClaimSend(ctx, id, "mobile", t0); err != nil {
			t.Fatal(err)
		}
	}
	rec := Record{Chat: chatA, ID: "WAID1", FromMe: true, At: t0, Kind: "text", Text: "oi", Raw: `{"chat":"556499990000@s.whatsapp.net"}`}
	if err := db.FinishSent(ctx, "F1", rec, ""); err != nil {
		t.Fatal(err)
	}
	d, _ := db.Draft(ctx, "F1")
	page, err := db.Messages(ctx, MessagesQuery{Chat: chatA, Limit: 10})
	if d.State != "sent" || d.MessageID == nil || *d.MessageID != "WAID1" || err != nil || len(page.Messages) != 1 || !page.Messages[0].FromMe {
		t.Fatalf("after FinishSent = draft %+v, messages %+v, %v", d, page.Messages, err)
	}
	if err := db.FinishFailed(ctx, "F2", "reachout_locked", 463); err != nil {
		t.Fatal(err)
	}
	if d, _ := db.Draft(ctx, "F2"); d.State != "failed" || d.Error == nil || *d.Error != "reachout_locked" || d.WACode == nil || *d.WACode != 463 {
		t.Fatalf("after FinishFailed = %+v", d)
	}
	if n, err := db.InterruptSending(ctx); err != nil || n != 1 {
		t.Fatalf("InterruptSending = %d, %v; want 1", n, err)
	}
	if d, _ := db.Draft(ctx, "F3"); d.State != "failed" || d.Error == nil || *d.Error != "interrupted" || d.WACode != nil {
		t.Fatalf("interrupted = %+v", d)
	}
	list, err := db.Drafts(ctx, "failed")
	if err != nil || len(list) != 2 {
		t.Fatalf("Drafts(failed) = %d, %v", len(list), err)
	}
}

func TestOutboxChat(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	if _, err := db.OutboxChat(ctx, chatA); !errors.Is(err, ErrUnknownChat) {
		t.Fatalf("empty archive = %v, want ErrUnknownChat", err)
	}
	save := func(id string, fromMe bool, at time.Time, text, rawChat string) {
		t.Helper()
		r := Record{Chat: chatA, ID: id, FromMe: fromMe, At: at, Kind: "text", Text: text,
			Raw: fmt.Sprintf(`{"chat":%q,"sender":"","from_me":%v,"group":false,"message":{"conversation":%q}}`, rawChat, fromMe, text)}
		if !fromMe {
			r.Sender = chatA
		}
		if err := db.Save(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	save("S1", true, t0.Add(-5*time.Minute), "fui eu", chatA)
	if _, err := db.OutboxChat(ctx, chatA); !errors.Is(err, ErrNewContact) {
		t.Fatalf("only outgoing messages = %v, want ErrNewContact", err)
	}
	save("S2", false, t0.Add(-4*time.Minute), "um", chatA)
	save("S3", false, t0.Add(-3*time.Minute), "dois", chatA)
	save("S4", true, t0.Add(-2*time.Minute), "três", "88887777@lid")
	if err := db.Contact(ctx, chatA, "Maria", true); err != nil {
		t.Fatal(err)
	}
	info, err := db.OutboxChat(ctx, chatA)
	if err != nil {
		t.Fatal(err)
	}
	if info.Address != "88887777@lid" || info.Name != "Maria" || len(info.Recent) != 3 {
		t.Fatalf("OutboxChat = %+v", info)
	}
	if r := info.Recent; r[0].Text != "um" || r[0].Sender != "Maria" || r[0].FromMe || r[2].Text != "três" || !r[2].FromMe || r[2].Sender != "" || r[2].At != "2026-11-24T11:58:00Z" {
		t.Fatalf("recent = %+v; want the last three, oldest first", r)
	}
	text, raw, err := db.Quoted(ctx, chatA, "S2")
	if err != nil || text != "um" || raw == "" {
		t.Fatalf("Quoted = %q %q %v", text, raw, err)
	}
	if _, _, err := db.Quoted(ctx, chatA, "NOPE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Quoted unknown = %v, want ErrNotFound", err)
	}
}

func TestReachoutLock(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	if until, err := db.ReachoutLock(ctx); err != nil || until != "" {
		t.Fatalf("fresh lock = %q, %v", until, err)
	}
	if err := db.SetReachoutLock(ctx, "2026-11-25T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if until, _ := db.ReachoutLock(ctx); until != "2026-11-25T12:00:00Z" {
		t.Fatalf("lock = %q", until)
	}
	if err := db.SetReachoutLock(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if until, _ := db.ReachoutLock(ctx); until != "" {
		t.Fatalf("cleared lock = %q", until)
	}
}

func TestReachoutLockOnlyGrows(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	if err := db.ExtendReachoutLock(ctx, "2026-11-25T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if until, _ := db.ReachoutLock(ctx); until != "2026-11-25T12:00:00Z" {
		t.Fatalf("first lock = %q", until)
	}
	// A 48 h lock from WhatsApp is not cut short by the 24 h hold after a 463.
	if err := db.ExtendReachoutLock(ctx, "2026-11-26T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := db.ExtendReachoutLock(ctx, "2026-11-25T18:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if until, _ := db.ReachoutLock(ctx); until != "2026-11-26T12:00:00Z" {
		t.Fatalf("lock after a shorter hold = %q, want the later end", until)
	}
}

func TestMarkSentWithoutTheArchive(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	create(t, db, draft("K1", "srv::s1", "oi"), t0)
	if _, err := db.ClaimSend(ctx, "K1", "desktop", t0); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkSent(ctx, "K1", "WAID9"); err != nil {
		t.Fatal(err)
	}
	if d, _ := db.Draft(ctx, "K1"); d.State != "sent" || d.MessageID == nil || *d.MessageID != "WAID9" {
		t.Fatalf("after MarkSent = %+v", d)
	}
	if n, _ := db.InterruptSending(ctx); n != 0 {
		t.Fatalf("a sent draft was interrupted on restart")
	}
}
