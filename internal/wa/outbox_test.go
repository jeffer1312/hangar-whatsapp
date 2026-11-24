package wa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waE2E"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
	"github.com/jeffer1312/hangar-whatsapp/internal/wa/watest"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type outboxHarness struct {
	*harness
	clock *clock
}

func openOutbox(t *testing.T, dir string, clk *clock, clients *watest.Factory) *Service {
	t.Helper()
	svc, err := Open(context.Background(), Options{Dir: dir, Version: "0.2.0", Now: clk.Now,
		NewClient: func(d *wastore.Device, l waLog.Logger) Client { return clients.New(d, l) },
		Wait:      func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func newOutboxHarness(t *testing.T, paired bool) *outboxHarness {
	t.Helper()
	clk := &clock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	h := &harness{t: t, dir: t.TempDir(), clients: watest.NewFactory(paired)}
	h.svc = openOutbox(t, h.dir, clk, h.clients)
	t.Cleanup(func() { h.svc.Close() })
	return &outboxHarness{harness: h, clock: clk}
}

// receive archives an incoming message from 556499990000 and waits for it.
func (h *outboxHarness) receive(id, text string) {
	h.t.Helper()
	before := h.status().Totals.Messages
	h.client().Emit(watest.Text(id, text, h.clock.Now().Add(-time.Minute)))
	watest.WaitFor(h.t, "message "+id, func() bool { return h.status().Totals.Messages == before+1 })
}

func (h *outboxHarness) emit(evt *events.Message) {
	h.t.Helper()
	before := h.status().Totals.Messages
	h.client().Emit(evt)
	watest.WaitFor(h.t, "message "+evt.Info.ID, func() bool { return h.status().Totals.Messages == before+1 })
}

func (h *outboxHarness) draft(req DraftRequest) (store.Draft, bool, error) {
	if req.Origin == "" {
		req.Origin = "notebook::s1"
	}
	return h.svc.CreateDraft(context.Background(), req)
}

func (h *outboxHarness) mustDraft(req DraftRequest) store.Draft {
	h.t.Helper()
	d, created, err := h.draft(req)
	if err != nil || !created {
		h.t.Fatalf("CreateDraft(%+v) = %+v, %v, %v", req, d, created, err)
	}
	return d
}

func writeFile(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDraftNeedsAPairedDevice(t *testing.T) {
	h := newOutboxHarness(t, false)
	if _, _, err := h.draft(DraftRequest{Chat: chatA, Text: "oi"}); !errors.Is(err, ErrNotPaired) {
		t.Fatalf("draft unpaired = %v, want ErrNotPaired", err)
	}
}

func TestDraftValidation(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	other := types.NewJID("556488887777", types.DefaultUserServer)
	h.emit(&events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: other, Sender: watest.OwnJID, IsFromMe: true}, ID: "OUT1", Timestamp: h.clock.Now(),
	}, Message: &waE2E.Message{Conversation: proto.String("fui eu")}})

	cases := []struct {
		name string
		req  DraftRequest
		want error
	}{
		{"no origin", DraftRequest{Origin: " ", Chat: chatA, Text: "oi"}, ErrBadDraft},
		{"no text and no file", DraftRequest{Chat: chatA, Text: "  "}, ErrBadDraft},
		{"text above 4096", DraftRequest{Chat: chatA, Text: strings.Repeat("é", 4097)}, ErrBadDraft},
		{"broadcast", DraftRequest{Chat: "status@broadcast", Text: "oi"}, ErrUnsupportedChat},
		{"newsletter", DraftRequest{Chat: "1203630001@newsletter", Text: "oi"}, ErrUnsupportedChat},
		{"not archived", DraftRequest{Chat: "556411110000@s.whatsapp.net", Text: "oi"}, store.ErrUnknownChat},
		{"never wrote to us", DraftRequest{Chat: other.String(), Text: "oi"}, store.ErrNewContact},
		{"unknown reply_to", DraftRequest{Chat: chatA, Text: "oi", ReplyTo: "NOPE"}, ErrBadDraft},
	}
	for _, c := range cases {
		if _, _, err := h.draft(c.req); !errors.Is(err, c.want) {
			t.Errorf("%s: CreateDraft = %v, want %v", c.name, err, c.want)
		}
	}
	if _, _, err := h.draft(DraftRequest{Chat: chatA, Text: strings.Repeat("é", 4096)}); err != nil {
		t.Fatalf("4096 characters = %v", err)
	}
}

func TestDraftDescribesTheChat(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "um")
	h.receive("M2", "dois")
	h.receive("M3", "três")
	h.receive("M4", "quatro")
	dev := h.client().Device()
	// Saving the paired device gives it the address book of session.db.
	dev.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{}, AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	if err := dev.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	maria := types.NewJID("556499990000", types.DefaultUserServer)
	if _, _, err := dev.Contacts.PutPushName(context.Background(), maria, "Mari ✨"); err != nil {
		t.Fatal(err)
	}

	d := h.mustDraft(DraftRequest{Chat: "556499990000@s.whatsapp.net", Text: "respondendo", ReplyTo: "M2"})
	if d.ChatKind != "contact" || d.ChatNumber != "556499990000" || d.ChatName != "Mari ✨" || d.ChatNameSource != "push" {
		t.Fatalf("push-named chat = kind %s number %s name %q source %q", d.ChatKind, d.ChatNumber, d.ChatName, d.ChatNameSource)
	}
	if len(d.Recent) != 3 || d.Recent[0].Text != "dois" || d.Recent[2].Text != "quatro" {
		t.Fatalf("recent = %+v; want the last three, oldest first", d.Recent)
	}
	if d.ReplyTo == nil || *d.ReplyTo != "M2" || d.QuotedText == nil || *d.QuotedText != "dois" {
		t.Fatalf("reply = %v quoted %v", d.ReplyTo, d.QuotedText)
	}
	if d.State != "pending" || d.CreatedAt != "2026-11-24T12:00:00Z" || d.ExpiresAt != "2026-11-24T12:10:00Z" || d.Origin != "notebook::s1" {
		t.Fatalf("draft = %+v", d)
	}
	twin, created, err := h.draft(DraftRequest{Chat: "556499990000:3@s.whatsapp.net", Text: "respondendo", ReplyTo: "M2"})
	if err != nil || created || twin.ID != d.ID {
		t.Fatalf("same draft again = %s created %v, %v; want %s returned", twin.ID, created, err, d.ID)
	}

	if err := dev.Contacts.PutContactName(context.Background(), maria, "Maria", "Maria Souza"); err != nil {
		t.Fatal(err)
	}
	d = h.mustDraft(DraftRequest{Chat: chatA, Text: "outro"})
	if d.ChatName != "Maria Souza" || d.ChatNameSource != "contact" {
		t.Fatalf("address-book chat = name %q source %q", d.ChatName, d.ChatNameSource)
	}

	group := types.NewJID("120363000000000001", types.GroupServer)
	h.emit(&events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: group, Sender: maria, IsGroup: true}, ID: "G1", Timestamp: h.clock.Now(),
	}, Message: &waE2E.Message{Conversation: proto.String("oi grupo")}})
	if err := h.svc.in.db.Contact(context.Background(), group.String(), "Família", true); err != nil {
		t.Fatal(err)
	}
	d = h.mustDraft(DraftRequest{Chat: group.String(), Text: "oi"})
	if d.ChatKind != "group" || d.ChatNumber != "120363000000000001@g.us" || d.ChatName != "Família" || d.ChatNameSource != "group" {
		t.Fatalf("group = kind %s number %s name %q source %q", d.ChatKind, d.ChatNumber, d.ChatName, d.ChatNameSource)
	}
	// The archive's name for the sender: the push name the 1:1 messages brought.
	if d.Recent[0].Sender != "Maria" {
		t.Fatalf("group recent sender = %q", d.Recent[0].Sender)
	}
}

func TestDraftAttachment(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	work := t.TempDir()
	zip := writeFile(t, filepath.Join(work, "proj", "entrega.zip"), "PK-conteúdo")

	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "segue", File: zip, OriginCwd: filepath.Join(work, "proj")})
	sum := sha256.Sum256([]byte("PK-conteúdo"))
	want := store.DraftFile{Name: "entrega.zip", Size: int64(len("PK-conteúdo")), SHA256: hex.EncodeToString(sum[:]), Source: zip}
	if d.File == nil || *d.File != want {
		t.Fatalf("file = %+v, want %+v", d.File, want)
	}
	copyDir := filepath.Join(h.dir, "outbox", d.ID)
	if b, err := os.ReadFile(filepath.Join(copyDir, "entrega.zip")); err != nil || string(b) != "PK-conteúdo" {
		t.Fatalf("copy = %q, %v", b, err)
	}
	if st, err := os.Stat(copyDir); err != nil || st.Mode().Perm() != 0o700 {
		t.Fatalf("copy folder = %v, %v; want 0700", st, err)
	}
	// Changing the original after the draft does not change what goes out.
	writeFile(t, zip, "trocado")
	if b, _ := os.ReadFile(filepath.Join(copyDir, "entrega.zip")); string(b) != "PK-conteúdo" {
		t.Fatalf("copy followed the original: %q", b)
	}

	if d := h.mustDraft(DraftRequest{Chat: chatA, Text: "fora", File: zip, OriginCwd: filepath.Join(work, "outro")}); !d.File.OutsideCwd {
		t.Fatalf("file outside origin_cwd not flagged: %+v", d.File)
	}
	h.clock.Add(11 * time.Minute)

	if d := h.mustDraft(DraftRequest{Chat: chatA, File: zip}); !d.File.OutsideCwd || d.Text != "" {
		t.Fatalf("no origin_cwd = %+v text %q; want outside_cwd", d.File, d.Text)
	}
}

func TestDraftAttachmentRefusals(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	work := t.TempDir()
	plain := writeFile(t, filepath.Join(work, "a.txt"), "a")
	hiddenDir := filepath.Join(work, ".segredo")
	writeFile(t, filepath.Join(hiddenDir, "senha.txt"), "x")
	link := filepath.Join(work, "link.txt")
	if err := os.Symlink(plain, link); err != nil {
		t.Fatal(err)
	}
	// A visible folder that is a link into a hidden one.
	if err := os.Symlink(hiddenDir, filepath.Join(work, "atalho")); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(work, "grande.bin")
	f, err := os.Create(big)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(100<<20 + 1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	writeFile(t, filepath.Join(h.dir, "media", "x", "foto.jpg"), "x")

	cases := []struct {
		name, path string
		want       error
	}{
		{"relative path", "a.txt", ErrBadDraft},
		{"missing file", filepath.Join(work, "nada.txt"), ErrFileRefused},
		{"symlink", link, ErrFileRefused},
		{"folder", work, ErrFileRefused},
		{"hidden folder", filepath.Join(hiddenDir, "senha.txt"), ErrFileRefused},
		{"link into a hidden folder", filepath.Join(work, "atalho", "senha.txt"), ErrFileRefused},
		{".env", writeFile(t, filepath.Join(work, ".env.local"), "TOKEN=1"), ErrFileRefused},
		{"key", writeFile(t, filepath.Join(work, "servidor.KEY"), "k"), ErrFileRefused},
		{"pem", writeFile(t, filepath.Join(work, "cert.pem"), "k"), ErrFileRefused},
		{"data folder", filepath.Join(h.dir, "media", "x", "foto.jpg"), ErrFileRefused},
		{"above 100 MB", big, ErrFileTooLarge},
	}
	for _, c := range cases {
		if _, _, err := h.draft(DraftRequest{Chat: chatA, Text: c.name, File: c.path}); !errors.Is(err, c.want) {
			t.Errorf("%s: CreateDraft = %v, want %v", c.name, err, c.want)
		}
	}
	if entries, _ := os.ReadDir(filepath.Join(h.dir, "outbox")); len(entries) != 0 {
		t.Fatalf("refused attachments left copies: %v", entries)
	}
}

func TestDraftLifecycle(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	ctx := context.Background()
	zip := writeFile(t, filepath.Join(t.TempDir(), "a.zip"), "PK")

	declined := h.mustDraft(DraftRequest{Chat: chatA, Text: "recusa", File: zip})
	if d, err := h.svc.DeclineDraft(ctx, declined.ID); err != nil || d.State != "declined" {
		t.Fatalf("DeclineDraft = %+v, %v", d, err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "outbox", declined.ID)); !os.IsNotExist(err) {
		t.Fatalf("declined draft kept its copy: %v", err)
	}
	if _, err := h.svc.ApproveDraft(ctx, declined.ID, "desktop"); !errors.Is(err, store.ErrAnswered) {
		t.Fatalf("send after decline = %v, want ErrAnswered", err)
	}
	if _, err := h.svc.ApproveDraft(ctx, "nope", "desktop"); !errors.Is(err, store.ErrDraftNotFound) {
		t.Fatalf("send unknown = %v, want ErrDraftNotFound", err)
	}
	if _, err := h.svc.ApproveDraft(ctx, declined.ID, "tablet"); !errors.Is(err, ErrBadDraft) {
		t.Fatalf("send via tablet = %v, want ErrBadDraft", err)
	}

	late := h.mustDraft(DraftRequest{Chat: chatA, Text: "tarde", File: zip})
	h.clock.Add(10 * time.Minute)
	if _, err := h.svc.ApproveDraft(ctx, late.ID, "mobile"); !errors.Is(err, store.ErrExpired) {
		t.Fatalf("send after 10 min = %v, want ErrExpired", err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "outbox", late.ID)); !os.IsNotExist(err) {
		t.Fatalf("expired draft kept its copy: %v", err)
	}
	if list, err := h.svc.Drafts(ctx, "pending"); err != nil || len(list) != 0 {
		t.Fatalf("pending after expiry = %d, %v", len(list), err)
	}

	locked := h.mustDraft(DraftRequest{Chat: chatA, Text: "trava"})
	if err := h.svc.in.db.SetReachoutLock(ctx, "2026-11-25T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.ApproveDraft(ctx, locked.ID, "desktop"); !errors.Is(err, ErrReachoutLocked) {
		t.Fatalf("send while locked = %v, want ErrReachoutLocked", err)
	}
	if s := h.status(); s.ReachoutLockedUntil == nil || *s.ReachoutLockedUntil != "2026-11-25T00:00:00Z" {
		t.Fatalf("status reachout_locked_until = %v", s.ReachoutLockedUntil)
	}
	h.clock.Add(13 * time.Hour)
	if s := h.status(); s.ReachoutLockedUntil != nil {
		t.Fatalf("a lock in the past still shows: %v", *s.ReachoutLockedUntil)
	}

	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "vai"})
	if got, err := h.svc.Draft(ctx, d.ID); err != nil || got.ID != d.ID {
		t.Fatalf("Draft = %+v, %v", got, err)
	}
	sent, err := h.svc.ApproveDraft(ctx, d.ID, "desktop")
	if err != nil || sent.State != "sending" || sent.Via == nil || *sent.Via != "desktop" {
		t.Fatalf("ApproveDraft = %+v, %v", sent, err)
	}
	if s := h.status(); s.Outgoing.LastMinute != 1 || s.Outgoing.Today != 1 || s.Outgoing.PerMinute != 3 || s.Outgoing.PerDay != 20 {
		t.Fatalf("status send = %+v", s.Outgoing)
	}
	if _, err := h.svc.Drafts(ctx, "bogus"); !errors.Is(err, ErrBadDraft) {
		t.Fatalf("Drafts(bogus) = %v, want ErrBadDraft", err)
	}
}

func TestSendNeedsAPairedDevice(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"})
	h.client().Emit(&events.LoggedOut{})
	watest.WaitFor(t, "logged_out", func() bool { return h.status().State == "logged_out" })
	if _, err := h.svc.ApproveDraft(context.Background(), d.ID, "desktop"); !errors.Is(err, ErrNotPaired) {
		t.Fatalf("send logged out = %v, want ErrNotPaired", err)
	}
}

func TestRestartFailsHalfSentDraftsAndCleansCopies(t *testing.T) {
	clk := &clock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	dir := t.TempDir()
	clients := watest.NewFactory(true)
	svc := openOutbox(t, dir, clk, clients)
	clients.Last().Emit(watest.Text("M1", "olá", clk.Now().Add(-time.Minute)))
	watest.WaitFor(t, "the message", func() bool { s, _ := svc.Status(context.Background()); return s.Totals.Messages == 1 })
	zip := writeFile(t, filepath.Join(t.TempDir(), "a.zip"), "PK")
	ctx := context.Background()
	sending, _, err := svc.CreateDraft(ctx, DraftRequest{Origin: "n::a", Chat: chatA, Text: "a", File: zip})
	if err != nil {
		t.Fatal(err)
	}
	pending, _, err := svc.CreateDraft(ctx, DraftRequest{Origin: "n::b", Chat: chatA, Text: "b", File: zip})
	if err != nil {
		t.Fatal(err)
	}
	// Stands for a send cut by the process dying: the state moved, the message never went out.
	if _, err := svc.in.db.ClaimSend(ctx, sending.ID, "desktop", clk.Now()); err != nil {
		t.Fatal(err)
	}
	svc.Close()
	writeFile(t, filepath.Join(dir, "outbox", "orphan", "x.zip"), "x")

	svc = openOutbox(t, dir, clk, watest.NewFactory(true))
	defer svc.Close()
	d, err := svc.Draft(ctx, sending.ID)
	if err != nil || d.State != "failed" || d.Error == nil || *d.Error != "interrupted" {
		t.Fatalf("half-sent draft after restart = %+v, %v; want failed/interrupted", d, err)
	}
	for name, want := range map[string]bool{sending.ID: false, "orphan": false, pending.ID: true} {
		if _, err := os.Stat(filepath.Join(dir, "outbox", name)); (err == nil) != want {
			t.Errorf("outbox/%s present = %v, want %v", name, err == nil, want)
		}
	}
}
