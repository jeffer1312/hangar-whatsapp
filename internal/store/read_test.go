package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

const chatA = "556499990000@s.whatsapp.net"
const chatB = "556488880000@s.whatsapp.net"
const group = "120363000000000000@g.us"

func openTest(t *testing.T) *DB {
	t.Helper()
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func exec(t *testing.T, d *DB, query string, args ...any) {
	t.Helper()
	if _, err := d.w.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func insert(t *testing.T, d *DB, chat, id, at, kind, text string) {
	t.Helper()
	exec(t, d, `INSERT INTO messages (chat, id, sender, from_me, at, kind, text, source) VALUES (?, ?, ?, 0, ?, ?, ?, 'whatsapp')`,
		chat, id, chat, at, kind, text)
}

func ids(ms []Message) []string {
	out := []string{}
	for _, m := range ms {
		out = append(out, m.ID)
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMessagesPagesBackwardsInAscendingOrder(t *testing.T) {
	d := openTest(t)
	ctx := context.Background()
	for i, id := range []string{"M1", "M2", "M3", "M4", "M5"} {
		insert(t, d, chatA, id, time.Date(2026, 11, 1, 10, i, 0, 0, time.UTC).Format(TimeLayout), "text", "msg "+id)
	}
	insert(t, d, chatB, "OTHER", "2026-11-01T10:09:00Z", "text", "outra conversa")

	cases := []struct {
		name string
		q    MessagesQuery
		want []string
		more bool
	}{
		{"newest page", MessagesQuery{Chat: chatA, Limit: 2}, []string{"M4", "M5"}, true},
		{"before_id", MessagesQuery{Chat: chatA, BeforeID: "M4", Limit: 2}, []string{"M2", "M3"}, true},
		{"last page", MessagesQuery{Chat: chatA, BeforeID: "M2", Limit: 2}, []string{"M1"}, false},
		{"since", MessagesQuery{Chat: chatA, Since: "2026-11-01T10:02:00Z", Limit: 10}, []string{"M3", "M4", "M5"}, false},
		{"until", MessagesQuery{Chat: chatA, Until: "2026-11-01T10:01:00Z", Limit: 10}, []string{"M1", "M2"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			page, err := d.Messages(ctx, c.q)
			if err != nil {
				t.Fatal(err)
			}
			if !equal(ids(page.Messages), c.want) || page.More != c.more {
				t.Fatalf("Messages = %v more=%v, want %v more=%v", ids(page.Messages), page.More, c.want, c.more)
			}
		})
	}
	if _, err := d.Messages(ctx, MessagesQuery{Chat: chatA, BeforeID: "NOPE", Limit: 2}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown before_id = %v, want ErrNotFound", err)
	}
}

func TestMessageFieldsAndMediaPathStayInsideMedia(t *testing.T) {
	d := openTest(t)
	exec(t, d, `INSERT INTO contacts (jid, name) VALUES (?, 'Maria')`, chatA)
	exec(t, d, `INSERT INTO messages (chat, id, sender, from_me, at, kind, text, media, media_mime, media_error, reply_to, source) VALUES
		(?, 'OK', ?, 0, '2026-11-01T10:00:00Z', 'image', 'foto', 'media/556499990000/OK.jpg', 'image/jpeg', NULL, 'X', 'whatsapp'),
		(?, 'EVIL', ?, 0, '2026-11-01T10:01:00Z', 'image', '', 'media/../../../etc/passwd', 'image/jpeg', NULL, NULL, 'whatsapp'),
		(?, 'MINE', '', 1, '2026-11-01T10:02:00Z', 'audio', '', NULL, 'audio/ogg', 'expired', NULL, 'whatsapp')`,
		chatA, chatA, chatA, chatA, chatA)
	page, err := d.Messages(context.Background(), MessagesQuery{Chat: chatA, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.Name != "Maria" || len(page.Messages) != 3 {
		t.Fatalf("page = %+v", page)
	}
	ok, evil, mine := page.Messages[0], page.Messages[1], page.Messages[2]
	want := filepath.Join(d.dir, "media", "556499990000", "OK.jpg")
	if ok.MediaPath == nil || *ok.MediaPath != want || ok.SenderName != "Maria" || ok.ReplyTo == nil || *ok.ReplyTo != "X" {
		t.Fatalf("OK = %+v, media_path want %s", ok, want)
	}
	if evil.MediaPath != nil {
		t.Fatalf("media_path outside media/ was returned: %s", *evil.MediaPath)
	}
	if !mine.FromMe || mine.Sender != "" || mine.MediaError == nil || *mine.MediaError != "expired" || mine.ReplyTo != nil {
		t.Fatalf("MINE = %+v", mine)
	}
}

func TestSearchNewestFirstAcrossChats(t *testing.T) {
	d := openTest(t)
	ctx := context.Background()
	insert(t, d, chatA, "A1", "2026-11-01T10:00:00Z", "text", "segue o boleto")
	insert(t, d, chatB, "B1", "2026-11-02T10:00:00Z", "text", "Boleto pago")
	insert(t, d, chatB, "B2", "2026-11-03T10:00:00Z", "text", "nada a ver")
	insert(t, d, chatB, "B3", "2026-11-04T10:00:00Z", "text", "100% certo")

	got, err := d.Search(ctx, "boleto", "", 50)
	if err != nil {
		t.Fatal(err)
	}
	if !equal(ids(got), []string{"B1", "A1"}) || got[0].Chat != chatB || got[1].Chat != chatA {
		t.Fatalf("Search boleto = %+v", got)
	}
	if got, _ := d.Search(ctx, "boleto", chatA, 50); !equal(ids(got), []string{"A1"}) {
		t.Fatalf("Search boleto in chat A = %v", ids(got))
	}
	if got, _ := d.Search(ctx, "%", "", 50); !equal(ids(got), []string{"B3"}) {
		t.Fatalf("Search %% matched as a wildcard: %v", ids(got))
	}
}

func TestChatsNewestFirst(t *testing.T) {
	d := openTest(t)
	exec(t, d, `INSERT INTO contacts (jid, name) VALUES (?, 'Maria'), (?, 'Família')`, chatA, group)
	insert(t, d, chatA, "A1", "2026-11-01T10:00:00Z", "text", "a")
	insert(t, d, chatA, "A2", "2026-11-05T10:00:00Z", "text", "b")
	insert(t, d, group, "G1", "2026-11-03T10:00:00Z", "text", "c")
	insert(t, d, chatB, "B1", "2026-11-02T10:00:00Z", "text", "d")

	got, err := d.Chats(context.Background(), "", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Chat != chatA || got[1].Chat != group || got[2].Chat != chatB {
		t.Fatalf("Chats order = %+v", got)
	}
	if a := got[0]; a.Name != "Maria" || a.Messages != 2 || a.FirstAt != "2026-11-01T10:00:00Z" || a.LastAt != "2026-11-05T10:00:00Z" || a.IsGroup {
		t.Fatalf("chat A = %+v", a)
	}
	if !got[1].IsGroup {
		t.Fatalf("group not marked: %+v", got[1])
	}
	if got, _ := d.Chats(context.Background(), "famí", 50); len(got) != 1 || got[0].Chat != group {
		t.Fatalf("Chats q=famí = %+v", got)
	}
	if got, _ := d.Chats(context.Background(), "", 1); len(got) != 1 {
		t.Fatalf("Chats limit 1 = %d items", len(got))
	}
}

func TestTotals(t *testing.T) {
	d := openTest(t)
	insert(t, d, chatA, "A1", "2026-11-01T10:00:00Z", "text", "a")
	insert(t, d, group, "G1", "2026-11-03T10:00:00Z", "text", "c")
	exec(t, d, `INSERT INTO messages (chat, id, from_me, at, kind, media, media_mime, media_error, source) VALUES
		(?, 'S', 0, '2026-11-02T00:00:00Z', 'image', 'media/x/S.jpg', 'image/jpeg', NULL, 'whatsapp'),
		(?, 'P', 0, '2026-11-02T00:00:00Z', 'image', NULL, 'image/jpeg', 'expired', 'whatsapp'),
		(?, 'L', 0, '2026-11-02T00:00:00Z', 'image', NULL, 'image/jpeg', 'phone_refused', 'whatsapp')`, chatA, chatA, chatA)
	got, err := d.Totals(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := Totals{Messages: 5, Chats: 2, Groups: 1, MediaSaved: 1, MediaPhoneOnly: 1, MediaLost: 1}
	if got.Totals != want || got.Since != "2026-11-01T10:00:00Z" || got.LastMessageAt != "2026-11-03T10:00:00Z" {
		t.Fatalf("Totals = %+v, want %+v", got, want)
	}
}

func TestQueryPastDeadlineIsBusy(t *testing.T) {
	d := openTest(t)
	d.QueryTimeout = time.Nanosecond
	if _, err := d.Chats(context.Background(), "", 50); !errors.Is(err, ErrBusy) {
		t.Fatalf("Chats past the deadline = %v, want ErrBusy", err)
	}
}
