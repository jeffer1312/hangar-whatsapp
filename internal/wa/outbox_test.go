package wa

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow"
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
	mu    sync.Mutex
	now   time.Time
	waits []time.Duration
	// onWait runs during the typing wait, before the message goes.
	onWait func()
}

func (c *clock) wait(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.waits = append(c.waits, d)
	fn := c.onWait
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
	return nil
}

func (c *clock) Waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.waits...)
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
		Wait:      clk.wait})
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
	if st, err := os.Stat(copyDir); err != nil || !st.IsDir() {
		t.Fatalf("copy folder = %v, %v", st, err)
	} else if runtime.GOOS != "windows" && st.Mode().Perm() != 0o700 {
		t.Fatalf("copy folder mode = %v, want 0700", st.Mode().Perm())
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
	if runtime.GOOS != "windows" {
		for _, p := range []string{"/proc/self/environ", "/sys/kernel/mm/transparent_hugepage/enabled"} {
			if _, err := os.Stat(p); err != nil {
				continue
			}
			if _, _, err := h.draft(DraftRequest{Chat: chatA, Text: p, File: p}); !errors.Is(err, ErrFileRefused) {
				t.Errorf("%s: CreateDraft = %v, want ErrFileRefused", p, err)
			}
		}
	}
	if entries, _ := os.ReadDir(filepath.Join(h.dir, "outbox")); len(entries) != 0 {
		t.Fatalf("refused attachments left copies: %v", entries)
	}
}

func TestHangarWorktreesCanAttach(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	tree := filepath.Join(home, ".hangar", "worktrees", "hangar", "feat-x")
	zip := writeFile(t, filepath.Join(tree, "dist", "entrega.zip"), "PK")
	if d := h.mustDraft(DraftRequest{Chat: chatA, Text: "zip", File: zip, OriginCwd: tree}); d.File == nil || d.File.OutsideCwd {
		t.Fatalf("worktree file = %+v", d.File)
	}
	for _, p := range []string{
		writeFile(t, filepath.Join(tree, ".git", "config"), "x"),
		writeFile(t, filepath.Join(tree, ".env"), "TOKEN=1"),
		writeFile(t, filepath.Join(tree, "certs", "a.pem"), "x"),
		writeFile(t, filepath.Join(home, ".hangar", "config", "notas.txt"), "x"),
		writeFile(t, filepath.Join(home, ".hangar", "worktrees.txt"), "x"),
	} {
		if _, _, err := h.draft(DraftRequest{Chat: chatA, Text: p, File: p}); !errors.Is(err, ErrFileRefused) {
			t.Errorf("%s: CreateDraft = %v, want ErrFileRefused", p, err)
		}
	}
}

func TestCopyOfADraftExpiredByAnotherAnswerIsDeleted(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	zip := writeFile(t, filepath.Join(t.TempDir(), "a.zip"), "PK")
	a := h.mustDraft(DraftRequest{Origin: "n::a", Chat: chatA, Text: "a", File: zip})
	h.clock.Add(9 * time.Minute)
	b := h.mustDraft(DraftRequest{Origin: "n::b", Chat: chatA, Text: "b"})
	h.clock.Add(2 * time.Minute)
	// The decline's own transaction expires a; nothing else expires afterwards.
	if _, err := h.svc.DeclineDraft(context.Background(), b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "outbox", a.ID)); !os.IsNotExist(err) {
		t.Fatalf("expired draft kept its copy: %v", err)
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

func TestOutdatedClientSaysSo(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"})
	h.client().Emit(&events.ClientOutdated{})
	watest.WaitFor(t, "outdated", func() bool { return h.status().State == "outdated" })
	if _, _, err := h.draft(DraftRequest{Chat: chatA, Text: "outro"}); !errors.Is(err, ErrOutdated) {
		t.Fatalf("draft while outdated = %v, want ErrOutdated", err)
	}
	if _, err := h.svc.ApproveDraft(context.Background(), d.ID, "desktop"); !errors.Is(err, ErrOutdated) {
		t.Fatalf("send while outdated = %v, want ErrOutdated", err)
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

func (h *outboxHarness) waitState(id, state string) store.Draft {
	h.t.Helper()
	var d store.Draft
	watest.WaitFor(h.t, "draft "+id+" "+state, func() bool {
		var err error
		d, err = h.svc.Draft(context.Background(), id)
		return err == nil && d.State == state
	})
	return d
}

func (h *outboxHarness) approve(id string) {
	h.t.Helper()
	if _, err := h.svc.ApproveDraft(context.Background(), id, "desktop"); err != nil {
		h.t.Fatalf("ApproveDraft(%s) = %v", id, err)
	}
}

func TestApprovedTextGoesOutOnce(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "chego às 15h"})
	h.approve(d.ID)
	d = h.waitState(d.ID, "sent")
	c := h.client()
	sent := c.Sent()
	if len(sent) != 1 || sent[0].To.String() != chatA || sent[0].Message.GetConversation() != "chego às 15h" {
		t.Fatalf("sent = %+v", sent)
	}
	if p := c.Presences(); len(p) != 2 || p[0] != "composing "+chatA || p[1] != "paused "+chatA {
		t.Fatalf("presences = %v; want composing then paused", p)
	}
	// 1 s plus 50 ms for each of the 12 characters.
	if w := h.clock.Waits(); len(w) != 1 || w[0] != 1600*time.Millisecond {
		t.Fatalf("typing wait = %v, want [1.6s]", w)
	}
	if d.MessageID == nil || *d.MessageID != "3EB0SENT1" || d.Error != nil {
		t.Fatalf("sent draft = %+v", d)
	}
	page, err := h.svc.Messages(context.Background(), store.MessagesQuery{Chat: chatA, Limit: 10})
	if err != nil || len(page.Messages) != 2 {
		t.Fatalf("archive after send = %+v, %v", page.Messages, err)
	}
	if m := page.Messages[1]; m.ID != "3EB0SENT1" || !m.FromMe || m.Text != "chego às 15h" || m.Kind != "text" || m.At != "2026-11-24T12:00:05Z" {
		t.Fatalf("archived send = %+v", m)
	}
}

func TestTypingWaitIsCapped(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: strings.Repeat("a", 500)})
	h.approve(d.ID)
	h.waitState(d.ID, "sent")
	if w := h.clock.Waits(); len(w) != 1 || w[0] != 6*time.Second {
		t.Fatalf("typing wait = %v, want [6s]", w)
	}
}

func TestReplyQuotesTheMessage(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "pode ser amanhã?")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "pode", ReplyTo: "M1"})
	h.approve(d.ID)
	h.waitState(d.ID, "sent")
	msg := h.client().Sent()[0].Message
	ext := msg.GetExtendedTextMessage()
	ci := ext.GetContextInfo()
	if ext.GetText() != "pode" || ci.GetStanzaID() != "M1" || ci.GetParticipant() != chatA || ci.GetQuotedMessage().GetConversation() != "pode ser amanhã?" {
		t.Fatalf("reply = %v", msg)
	}
	page, _ := h.svc.Messages(context.Background(), store.MessagesQuery{Chat: chatA, Limit: 10})
	if m := page.Messages[1]; m.ReplyTo == nil || *m.ReplyTo != "M1" {
		t.Fatalf("archived reply = %+v", m)
	}
}

func TestGroupReplyQuotesTheAuthor(t *testing.T) {
	h := newOutboxHarness(t, true)
	group := types.NewJID("120363000000000001", types.GroupServer)
	author := types.NewJID("556477776666", types.DefaultUserServer)
	h.emit(&events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: group, Sender: author, IsGroup: true}, ID: "G1", Timestamp: h.clock.Now(),
	}, Message: &waE2E.Message{Conversation: proto.String("quem leva o bolo?")}})
	d := h.mustDraft(DraftRequest{Chat: group.String(), Text: "eu", ReplyTo: "G1"})
	h.approve(d.ID)
	h.waitState(d.ID, "sent")
	sent := h.client().Sent()[0]
	if sent.To != group || sent.Message.GetExtendedTextMessage().GetContextInfo().GetParticipant() != author.String() {
		t.Fatalf("group reply = to %s %v", sent.To, sent.Message)
	}
}

func TestApprovedFileGoesAsOneDocument(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "manda o arquivo")
	zip := writeFile(t, filepath.Join(t.TempDir(), "entrega.zip"), "PK-zip")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "segue o zip", File: zip})
	h.approve(d.ID)
	h.waitState(d.ID, "sent")
	c := h.client()
	if up := c.Uploads(); len(up) != 1 || string(up[0]) != "PK-zip" {
		t.Fatalf("uploads = %q", up)
	}
	sent := c.Sent()
	doc := sent[0].Message.GetDocumentMessage()
	if len(sent) != 1 || doc.GetFileName() != "entrega.zip" || doc.GetCaption() != "segue o zip" || doc.GetMimetype() != "application/zip" ||
		doc.GetDirectPath() != "/v/t62/d1" || doc.GetFileLength() != 6 || len(doc.GetMediaKey()) != 32 {
		t.Fatalf("document = %v", sent[0].Message)
	}
	// The copy goes after the sent state is written: wait on the folder itself, not on a service read that sweeps it.
	watest.WaitFor(t, "the copy to leave outbox/", func() bool {
		_, err := os.Stat(filepath.Join(h.dir, "outbox", d.ID))
		return os.IsNotExist(err)
	})
	page, _ := h.svc.Messages(context.Background(), store.MessagesQuery{Chat: chatA, Limit: 10})
	m := page.Messages[1]
	if m.Kind != "document" || m.Text != "segue o zip" || m.MediaPath == nil {
		t.Fatalf("archived document = %+v", m)
	}
	if b, err := os.ReadFile(*m.MediaPath); err != nil || string(b) != "PK-zip" {
		t.Fatalf("archived file = %q, %v", b, err)
	}
}

func TestReachoutRefusalLocksFurtherSends(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	c := h.client()
	c.FailSend(fmt.Errorf("%w %d", whatsmeow.ErrServerReturnedError, 463))
	first := h.mustDraft(DraftRequest{Chat: chatA, Text: "primeira"})
	h.approve(first.ID)
	d := h.waitState(first.ID, "failed")
	if d.Error == nil || *d.Error != "reachout_locked" || d.WACode == nil || *d.WACode != 463 || d.MessageID != nil {
		t.Fatalf("463 = %+v", d)
	}
	if u := h.status().ReachoutLockedUntil; u == nil || *u != "2026-11-25T12:00:00Z" {
		t.Fatalf("lock after 463 = %v, want a day", u)
	}
	// Insisting after a 463 is what gets an account banned: the next approval is refused.
	again := h.mustDraft(DraftRequest{Chat: chatA, Text: "de novo"})
	if _, err := h.svc.ApproveDraft(context.Background(), again.ID, "desktop"); !errors.Is(err, ErrReachoutLocked) {
		t.Fatalf("send after 463 = %v, want ErrReachoutLocked", err)
	}
	if n := len(c.Sent()); n != 1 {
		t.Fatalf("SendMessage called %d times, want 1", n)
	}
	if page, _ := h.svc.Messages(context.Background(), store.MessagesQuery{Chat: chatA, Limit: 10}); len(page.Messages) != 1 {
		t.Fatalf("a failed send reached the archive: %+v", page.Messages)
	}
}

func TestFailedSendIsNotRetried(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	c := h.client()
	c.FailSend(errors.New("websocket closed"))
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "segunda"})
	h.approve(d.ID)
	d = h.waitState(d.ID, "failed")
	if d.Error == nil || !strings.Contains(*d.Error, "websocket closed") || d.WACode != nil {
		t.Fatalf("network failure = %+v", d)
	}
	// What may have reached WhatsApp says so: the person may have the message.
	c.FailSend(whatsmeow.ErrMessageTimedOut)
	u := h.mustDraft(DraftRequest{Chat: chatA, Text: "terceira"})
	h.approve(u.ID)
	if u = h.waitState(u.ID, "failed"); u.Error == nil || *u.Error != "unconfirmed" {
		t.Fatalf("timed-out send = %+v, want error unconfirmed", u)
	}
	if n := len(c.Sent()); n != 2 {
		t.Fatalf("SendMessage called %d times for 2 drafts; a failure must not be sent again", n)
	}
}

func TestLockArrivingWhileTypingStopsTheSend(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	h.clock.onWait = func() {
		h.client().Emit(&events.NotifyAccountReachoutTimelock{IsActive: true,
			TimeEnforcementEnds: jsontime.UnixString{Time: time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)}})
	}
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"})
	h.approve(d.ID)
	if d = h.waitState(d.ID, "failed"); d.Error == nil || *d.Error != "reachout_locked" {
		t.Fatalf("send with a lock during typing = %+v", d)
	}
	if n := len(h.client().Sent()); n != 0 {
		t.Fatalf("SendMessage called %d times under a lock", n)
	}
}

func TestReachoutTimelockStopsSends(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"})
	ends := time.Date(2026, 11, 25, 8, 0, 0, 0, time.UTC)
	h.client().Emit(&events.NotifyAccountReachoutTimelock{IsActive: true, TimeEnforcementEnds: jsontime.UnixString{Time: ends}})
	watest.WaitFor(t, "the lock", func() bool {
		u := h.status().ReachoutLockedUntil
		return u != nil && *u == "2026-11-25T08:00:00Z"
	})
	if _, err := h.svc.ApproveDraft(context.Background(), d.ID, "desktop"); !errors.Is(err, ErrReachoutLocked) {
		t.Fatalf("send while locked = %v, want ErrReachoutLocked", err)
	}
	h.client().Emit(&events.NotifyAccountReachoutTimelock{IsActive: false})
	watest.WaitFor(t, "the lock to lift", func() bool { return h.status().ReachoutLockedUntil == nil })
	h.approve(d.ID)
	h.waitState(d.ID, "sent")
}

func TestAddressBookNameOfTheLinkedIdentityWins(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	ctx := context.Background()
	dev := h.client().Device()
	dev.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{}, AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	if err := dev.Save(ctx); err != nil {
		t.Fatal(err)
	}
	pn := types.NewJID("556499990000", types.DefaultUserServer)
	lid := types.NewJID("123456789012345", types.HiddenUserServer)
	if err := dev.LIDs.PutLIDMapping(ctx, lid, pn); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dev.Contacts.PutPushName(ctx, pn, "Mari ✨"); err != nil {
		t.Fatal(err)
	}
	if err := dev.Contacts.PutContactName(ctx, lid, "Maria", "Maria Souza"); err != nil {
		t.Fatal(err)
	}
	if d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"}); d.ChatName != "Maria Souza" || d.ChatNameSource != "contact" {
		t.Fatalf("name = %q source %q; want the address book of the LID over the push name of the number", d.ChatName, d.ChatNameSource)
	}
}

func TestDisconnectWaitingForTheAckIsUnconfirmed(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	for _, err := range []error{&whatsmeow.DisconnectedError{Action: "message send"},
		&whatsmeow.DisconnectedError{Action: "message send (retry)"}, whatsmeow.ErrNotConnected} {
		h.clock.Add(time.Minute) // past the per-minute cap
		h.client().FailSend(err)
		d := h.mustDraft(DraftRequest{Chat: chatA, Text: err.Error()})
		h.approve(d.ID)
		if d = h.waitState(d.ID, "failed"); d.Error == nil || *d.Error != "unconfirmed" {
			t.Fatalf("%v: disconnect after the frame went = %+v, want error unconfirmed", err, d)
		}
	}
	// The resend after a reconnect can fail with the socket's own error, unwrapped like errors before the frame.
	h.clock.Add(time.Minute)
	h.client().FailSendAfterFrame(errors.New("frame socket is closed"))
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "resend failed"})
	h.approve(d.ID)
	if d = h.waitState(d.ID, "failed"); d.Error == nil || *d.Error != "unconfirmed" {
		t.Fatalf("resend failure after the frame = %+v, want error unconfirmed", d)
	}
	// Before the frame, the same errors come wrapped or from another request: the message did not go.
	for _, err := range []error{fmt.Errorf("failed to send message node: %w", whatsmeow.ErrNotConnected),
		fmt.Errorf("failed to get device list: %w", whatsmeow.ErrIQDisconnected)} {
		h.clock.Add(time.Minute)
		h.client().FailSend(err)
		d := h.mustDraft(DraftRequest{Chat: chatA, Text: err.Error()})
		h.approve(d.ID)
		if d = h.waitState(d.ID, "failed"); d.Error == nil || *d.Error == "unconfirmed" {
			t.Fatalf("%v: failure before the frame = %+v, want an ordinary failure", err, d)
		}
	}
}

func TestSentFileThatCannotBeArchivedLeavesNoCopy(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "manda o arquivo")
	db, err := sql.Open("sqlite", "file:"+store.MessagesPath(h.dir)+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TRIGGER no_sent BEFORE INSERT ON messages WHEN NEW.from_me = 1
		BEGIN SELECT RAISE(ABORT, 'archive refused'); END`); err != nil {
		t.Fatal(err)
	}
	zip := writeFile(t, filepath.Join(t.TempDir(), "entrega.zip"), "PK-zip")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "segue", File: zip})
	h.approve(d.ID)
	if d = h.waitState(d.ID, "sent"); d.MessageID == nil {
		t.Fatalf("sent without archive = %+v", d)
	}
	watest.WaitFor(t, "the copy to leave outbox/", func() bool {
		_, err := os.Stat(filepath.Join(h.dir, "outbox", d.ID))
		return os.IsNotExist(err)
	})
	var left []string
	filepath.WalkDir(filepath.Join(h.dir, "media"), func(p string, e fs.DirEntry, err error) error {
		if err == nil && !e.IsDir() {
			left = append(left, p)
		}
		return nil
	})
	if len(left) != 0 {
		t.Fatalf("plain copies left in media/ with no message pointing at them: %v", left)
	}
}

func TestReplacedClientCannotTouchTheReachoutLock(t *testing.T) {
	h := newOutboxHarness(t, false)
	old := h.client()
	if err := h.svc.Pair(""); err != nil {
		t.Fatal(err)
	}
	old.Emit(&events.NotifyAccountReachoutTimelock{IsActive: true,
		TimeEnforcementEnds: jsontime.UnixString{Time: h.clock.Now().Add(48 * time.Hour)}})
	if u := h.status().ReachoutLockedUntil; u != nil {
		t.Fatalf("a replaced client's notice set the lock to %s", *u)
	}
}

func TestNewContactIsPerNumber(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	var account string
	db, err := sql.Open("sqlite", "file:"+store.MessagesPath(h.dir)+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.QueryRow(`SELECT account FROM messages WHERE id = 'M1'`).Scan(&account); err != nil || account != watest.OwnJID.User {
		t.Fatalf("archived account = %q, %v; want the paired number", account, err)
	}
	// Someone who wrote only while another number was paired (A → B → A without a wipe).
	other := "556477776666@s.whatsapp.net"
	if _, err := db.Exec(`INSERT INTO messages (chat, id, sender, from_me, at, kind, text, source, account)
		VALUES (?, 'B1', ?, 0, '2026-11-24T11:00:00Z', 'text', 'oi B', 'whatsapp', '5511988887777')`, other, other); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.draft(DraftRequest{Chat: other, Text: "oi"}); !errors.Is(err, store.ErrNewContact) {
		t.Fatalf("draft to someone who wrote only to another number = %v, want ErrNewContact", err)
	}
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"})
	h.approve(d.ID)
	h.waitState(d.ID, "sent")
	var sentAccount string
	if err := db.QueryRow(`SELECT account FROM messages WHERE from_me = 1`).Scan(&sentAccount); err != nil || sentAccount != watest.OwnJID.User {
		t.Fatalf("sent message account = %q, %v", sentAccount, err)
	}
}

func TestMigratedMessagesCountForTheNumberPairedAtUpgrade(t *testing.T) {
	dir := t.TempDir()
	db, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Save(context.Background(), store.Record{Chat: chatA, ID: "OLD1", Sender: chatA, At: time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC),
		Kind: "text", Text: "da 0.1.x"}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	clk := &clock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	h := &outboxHarness{harness: &harness{t: t, dir: dir, clients: watest.NewFactory(true)}, clock: clk}
	h.svc = openOutbox(t, dir, clk, h.clients)
	t.Cleanup(func() { h.svc.Close() })
	if d, _, err := h.draft(DraftRequest{Chat: chatA, Text: "oi"}); err != nil {
		t.Fatalf("draft to a chat of the 0.1.x archive = %+v, %v; want it allowed for the paired number", d, err)
	}
}

func TestApprovalRechecksNewContactForTheSendingNumber(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"})
	// Before it expires, another number is paired: the person never wrote to it.
	db, err := sql.Open("sqlite", "file:"+store.MessagesPath(h.dir)+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE messages SET account = '5511988887777'`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.ApproveDraft(context.Background(), d.ID, "desktop"); !errors.Is(err, store.ErrNewContact) {
		t.Fatalf("ApproveDraft for a chat the paired number never heard from = %v, want ErrNewContact", err)
	}
	if got, _ := h.svc.Draft(context.Background(), d.ID); got.State != "pending" {
		t.Fatalf("draft after the refusal = %s, want pending", got.State)
	}
	if n := len(h.client().Sent()); n != 0 {
		t.Fatalf("SendMessage called %d times", n)
	}
}

func TestEditedTextIsWhatGoesOut(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "texto da sessão"})
	got, err := h.svc.ApproveEditedDraft(context.Background(), d.ID, "mobile", "texto do dono\r\nsegunda\rterceira")
	if err != nil || got.State != "sending" || !got.Edited || got.SentText != "texto do dono\nsegunda\nterceira" {
		t.Fatalf("ApproveEditedDraft = %+v, %v", got, err)
	}
	d = h.waitState(d.ID, "sent")
	sent := h.client().Sent()
	if len(sent) != 1 || sent[0].Message.GetConversation() != "texto do dono\nsegunda\nterceira" {
		t.Fatalf("sent = %+v; want the edited text", sent)
	}
	if d.Text != "texto da sessão" || d.SentText != "texto do dono\nsegunda\nterceira" || !d.Edited {
		t.Fatalf("sent draft = text %q sent_text %q edited %v", d.Text, d.SentText, d.Edited)
	}
}

func TestEditedTextIsCheckedLikeANewDraft(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "oi"})
	for _, text := range []string{" \r\n ", strings.Repeat("é", 4097), strings.Repeat("é", 4096) + "\r"} {
		if _, err := h.svc.ApproveEditedDraft(context.Background(), d.ID, "desktop", text); !errors.Is(err, ErrBadDraft) {
			t.Fatalf("edited text of %d characters = %v, want ErrBadDraft", len([]rune(text)), err)
		}
	}
	if got, err := h.svc.Draft(context.Background(), d.ID); err != nil || got.State != "pending" {
		t.Fatalf("draft after refused edits = %+v, %v; want pending", got, err)
	}
	if len(h.client().Sent()) != 0 {
		t.Fatal("a refused edit sent something")
	}
	// 4096 characters once \r\n is one character.
	ok := strings.Repeat("é", 4095) + "\r\n"
	if _, err := h.svc.ApproveEditedDraft(context.Background(), d.ID, "desktop", ok); err != nil {
		t.Fatalf("4096 characters after normalizing = %v", err)
	}

	zip := writeFile(t, filepath.Join(t.TempDir(), "a.zip"), "PK")
	f := h.mustDraft(DraftRequest{Chat: chatA, Text: "segue", File: zip, Origin: "notebook::s2"})
	got, err := h.svc.ApproveEditedDraft(context.Background(), f.ID, "desktop", "")
	if err != nil || got.SentText != "" || !got.Edited {
		t.Fatalf("empty edited text with a file = %+v, %v", got, err)
	}
	h.waitState(f.ID, "sent")
	if doc := h.client().Sent()[1].Message.GetDocumentMessage(); doc == nil || doc.Caption != nil {
		t.Fatalf("document = %v; want no caption", h.client().Sent()[1].Message)
	}
}

func TestSameTextAfterNormalizingIsANormalSend(t *testing.T) {
	h := newOutboxHarness(t, true)
	h.receive("M1", "olá")
	d := h.mustDraft(DraftRequest{Chat: chatA, Text: "linha 1\nlinha 2"})
	got, err := h.svc.ApproveEditedDraft(context.Background(), d.ID, "desktop", "linha 1\r\nlinha 2")
	if err != nil || got.Edited || got.SentText != "linha 1\nlinha 2" {
		t.Fatalf("same text = %+v, %v; want a normal send", got, err)
	}
	h.waitState(d.ID, "sent")

	n := h.mustDraft(DraftRequest{Chat: chatA, Text: "pela rota de sempre", Origin: "notebook::s2"})
	got, err = h.svc.ApproveDraft(context.Background(), n.ID, "desktop")
	if err != nil || got.Edited || got.SentText != "pela rota de sempre" {
		t.Fatalf("ApproveDraft = %+v, %v; want sent_text recorded", got, err)
	}
}
