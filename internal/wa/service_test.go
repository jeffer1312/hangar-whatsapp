package wa

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
	"github.com/jeffer1312/hangar-whatsapp/internal/wa/watest"
)

const chatA = "556499990000@s.whatsapp.net"

var msgAt = time.Date(2026, 11, 22, 11, 0, 0, 0, time.UTC)

type harness struct {
	t       *testing.T
	dir     string
	svc     *Service
	clients *watest.Factory
}

func newHarness(t *testing.T, paired bool) *harness {
	t.Helper()
	h := &harness{t: t, dir: t.TempDir(), clients: watest.NewFactory(paired)}
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	svc, err := Open(context.Background(), Options{
		Dir: h.dir, Version: "0.1.0", Now: func() time.Time { return now },
		NewClient: func(d *wastore.Device) Client { return h.clients.New(d) },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	t.Cleanup(svc.Close)
	return h
}

func (h *harness) client() *watest.Client { return h.clients.Last() }

func (h *harness) status() Status {
	h.t.Helper()
	s, err := h.svc.Status(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	return s
}

func str(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func TestStartsUnpairedAndPairShowsTheQR(t *testing.T) {
	h := newHarness(t, false)
	if s := h.status(); s.State != "unpaired" || s.QR != nil || s.API != 1 || s.Version != "0.1.0" || s.TZ == "" {
		t.Fatalf("fresh status = %+v", s)
	}
	if err := h.svc.Pair(""); err != nil {
		t.Fatal(err)
	}
	h.client().QR <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "QR1", Timeout: 60 * time.Second}
	watest.WaitFor(t, "the QR", func() bool { return str(h.status().QR) == "QR1" })
	s := h.status()
	if s.State != "pairing" || str(s.QRExpiresAt) != "2026-11-22T12:01:00Z" || s.PairCode != nil {
		t.Fatalf("pairing status = state %s qr_expires_at %s pair_code %s", s.State, str(s.QRExpiresAt), str(s.PairCode))
	}
}

func TestPairByPhoneShowsTheCode(t *testing.T) {
	h := newHarness(t, false)
	if err := h.svc.Pair("5564999990000"); err != nil {
		t.Fatal(err)
	}
	h.client().QR <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "QR1", Timeout: 60 * time.Second}
	watest.WaitFor(t, "the pair code", func() bool { return str(h.status().PairCode) == "ABCD-EFGH" })
	if s := h.status(); str(s.PairCodeExpiresAt) != "2026-11-22T12:02:40Z" {
		t.Fatalf("pair_code_expires_at = %s, want 160 s after the request", str(s.PairCodeExpiresAt))
	}
	if phones := h.client().Phones(); len(phones) != 1 || phones[0] != "5564999990000" {
		t.Fatalf("PairPhone calls = %v", phones)
	}
	if err := h.svc.Pair("55 64 9999"); !errors.Is(err, ErrBadPhone) {
		t.Fatalf("pair with a malformed phone = %v, want ErrBadPhone", err)
	}
}

func TestQRTimeoutThenPairAgainUsesANewClient(t *testing.T) {
	h := newHarness(t, false)
	if err := h.svc.Pair(""); err != nil {
		t.Fatal(err)
	}
	h.client().QR <- whatsmeow.QRChannelTimeout
	watest.WaitFor(t, "pair_timeout", func() bool { s := h.status(); return s.Error != nil && s.Error.Code == "pair_timeout" })
	if s := h.status(); s.State != "unpaired" || s.QR != nil {
		t.Fatalf("after timeout = %+v", s)
	}
	before := h.clients.Count()
	if err := h.svc.Pair(""); err != nil {
		t.Fatalf("pair after timeout: %v", err)
	}
	if h.clients.Count() != before+1 {
		t.Fatalf("pair after timeout reused the client (%d clients)", h.clients.Count())
	}
	if s := h.status(); s.State != "pairing" || s.Error != nil {
		t.Fatalf("second pairing = %+v", s)
	}
}

func TestPairSuccessConnects(t *testing.T) {
	h := newHarness(t, false)
	if err := h.svc.Pair(""); err != nil {
		t.Fatal(err)
	}
	c := h.client()
	c.Device().ID = &watest.OwnJID
	c.QR <- whatsmeow.QRChannelSuccess
	watest.WaitFor(t, "connected", func() bool { return h.status().State == "connected" })
	if s := h.status(); str(s.Device) != "+5564••••2222" || s.QR != nil {
		t.Fatalf("connected status = device %s qr %s", str(s.Device), str(s.QR))
	}
	if err := h.svc.Pair(""); !errors.Is(err, ErrAlreadyPaired) {
		t.Fatalf("pair while connected = %v, want ErrAlreadyPaired", err)
	}
}

func TestLoggedOutThenPairUsesANewClient(t *testing.T) {
	h := newHarness(t, true)
	if s := h.status(); s.State != "connected" {
		t.Fatalf("paired start = %s", s.State)
	}
	h.client().Emit(&events.LoggedOut{Reason: events.ConnectFailureLoggedOut})
	watest.WaitFor(t, "logged_out", func() bool { return h.status().State == "logged_out" })
	before := h.clients.Count()
	if err := h.svc.Pair(""); err != nil {
		t.Fatal(err)
	}
	if h.clients.Count() != before+1 || h.client().Device().ID != nil {
		t.Fatalf("pair after logout did not start from a new device")
	}
}

func TestClientOutdated(t *testing.T) {
	h := newHarness(t, true)
	h.client().Emit(&events.ClientOutdated{})
	watest.WaitFor(t, "outdated", func() bool { return h.status().State == "outdated" })
	if err := h.svc.Pair(""); !errors.Is(err, ErrOutdated) {
		t.Fatalf("pair while outdated = %v, want ErrOutdated", err)
	}
}

func TestLogoutKeepsTheData(t *testing.T) {
	h := newHarness(t, true)
	h.client().Emit(watest.Text("M1", "olá", msgAt))
	watest.WaitFor(t, "the message", func() bool { return h.status().Totals.Messages == 1 })
	if err := h.svc.Logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := h.status(); s.State != "logged_out" || h.client().Logouts() != 1 || s.Totals.Messages != 1 {
		t.Fatalf("after logout = state %s logouts %d messages %d", s.State, h.client().Logouts(), s.Totals.Messages)
	}
	if err := h.svc.Logout(context.Background()); !errors.Is(err, ErrNotPaired) {
		t.Fatalf("second logout = %v, want ErrNotPaired", err)
	}
}

func TestIncomingMessageIsReadable(t *testing.T) {
	h := newHarness(t, true)
	h.client().Emit(watest.Text("M1", "chegou agora", msgAt))
	var page store.MessagesPage
	watest.WaitFor(t, "the message", func() bool {
		var err error
		page, err = h.svc.Messages(context.Background(), store.MessagesQuery{Chat: chatA, Limit: 10})
		return err == nil && len(page.Messages) == 1
	})
	m := page.Messages[0]
	if m.ID != "M1" || m.Text != "chegou agora" || m.Kind != "text" || m.At != "2026-11-22T11:00:00Z" || page.Name != "Maria" {
		t.Fatalf("archived = %+v name %q", m, page.Name)
	}
}

func TestMediaWithMaliciousIDStaysInsideMedia(t *testing.T) {
	h := newHarness(t, true)
	evt := watest.Text("../../../../tmp/evil", "", msgAt)
	evt.Message = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("/x")}}
	h.client().Emit(evt)
	watest.WaitFor(t, "the download", func() bool { return h.status().Totals.MediaSaved == 1 })
	page, err := h.svc.Messages(context.Background(), store.MessagesQuery{Chat: chatA, Limit: 10})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].MediaPath == nil {
		t.Fatalf("page = %+v, %v", page, err)
	}
	path := *page.Messages[0].MediaPath
	rel, err := filepath.Rel(filepath.Join(h.dir, "media"), path)
	if err != nil || strings.HasPrefix(rel, "..") || strings.Count(filepath.ToSlash(rel), "/") != 1 {
		t.Fatalf("media saved at %s, outside media/", path)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "IMG" {
		t.Fatalf("media file = %q, %v", b, err)
	}
}

func TestWipeClosesAndDeletesThenStartsFresh(t *testing.T) {
	h := newHarness(t, true)
	h.client().Emit(watest.Text("M1", "olá", msgAt))
	watest.WaitFor(t, "the message", func() bool { return h.status().Totals.Messages == 1 })
	if err := os.MkdirAll(filepath.Join(h.dir, "media", "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.dir, "media", "x", "a.jpg"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := h.client()
	if err := h.svc.Wipe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if old.Logouts() != 1 {
		t.Fatalf("wipe did not log out first (%d)", old.Logouts())
	}
	if _, err := os.Stat(filepath.Join(h.dir, "media")); !os.IsNotExist(err) {
		t.Fatalf("media/ still there: %v", err)
	}
	s := h.status()
	if s.State != "unpaired" || s.Totals.Messages != 0 {
		t.Fatalf("after wipe = state %s messages %d", s.State, s.Totals.Messages)
	}
	if err := h.svc.Pair(""); err != nil {
		t.Fatalf("pair after wipe: %v", err)
	}
}

func TestSecondInstanceOnTheSameFolderIsRefused(t *testing.T) {
	h := newHarness(t, true)
	svc, err := Open(context.Background(), Options{Dir: h.dir, Version: "0.1.0",
		NewClient: func(d *wastore.Device) Client { return watest.New(d) }})
	if err == nil {
		svc.Close()
		t.Fatal("a second service opened the same session.db: two copies of the keys would run")
	}
}

func TestRequestMediaNeedsAPairedDevice(t *testing.T) {
	h := newHarness(t, false)
	if _, err := h.svc.RequestMedia(context.Background(), chatA, "", ""); !errors.Is(err, ErrNotPaired) {
		t.Fatalf("RequestMedia unpaired = %v, want ErrNotPaired", err)
	}
}
