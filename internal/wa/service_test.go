package wa

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
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
		NewClient: func(d *wastore.Device, l waLog.Logger) Client { return h.clients.New(d, l) },
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
	if s := h.status(); s.State != "unpaired" || s.QR != nil || s.DeviceName != nil || s.API != 2 || s.Version != "0.1.0" || s.TZ == "" {
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
	if s := h.status(); str(s.Device) != "+5564••••2222" || str(s.DeviceName) != "Google Chrome (Linux)" || s.QR != nil {
		t.Fatalf("connected status = device %s device_name %s qr %s", str(s.Device), str(s.DeviceName), str(s.QR))
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
	logout, err := h.svc.Wipe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if old.Logouts() != 1 || logout != "done" {
		t.Fatalf("wipe logout = %q after %d calls, want done after 1", logout, old.Logouts())
	}
	if logout, err := h.svc.Wipe(context.Background()); err != nil || logout != "not_paired" {
		t.Fatalf("second wipe = %q, %v; want not_paired", logout, err)
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

func TestWipeSaysWhenThePhoneStillListsTheDevice(t *testing.T) {
	h := newHarness(t, true)
	h.client().FailLogout(errors.New("phone offline"))
	logout, err := h.svc.Wipe(context.Background())
	if err != nil || logout != "not_done" {
		t.Fatalf("wipe with a failed logout = %q, %v; want not_done", logout, err)
	}
	if s := h.status(); s.State != "unpaired" {
		t.Fatalf("data not wiped: state %s", s.State)
	}
}

func TestFailedWipeKeepsTheServiceAnswering(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a folder the test user cannot empty")
	}
	h := newHarness(t, true)
	locked := filepath.Join(h.dir, "media", "x")
	if err := os.MkdirAll(locked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "a.jpg"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(locked, 0o700) })
	if _, err := h.svc.Wipe(context.Background()); err == nil {
		t.Fatal("wipe with an undeletable file reported success")
	}
	s := h.status()
	if s.State != "unpaired" {
		t.Fatalf("after a failed wipe = state %s, want unpaired (the keys are gone)", s.State)
	}
	if _, err := h.svc.Chats(context.Background(), "", 10); err != nil {
		t.Fatalf("reads after a failed wipe: %v", err)
	}
	if err := h.svc.Logout(context.Background()); !errors.Is(err, ErrNotPaired) {
		t.Fatalf("logout after a failed wipe = %v", err)
	}
}

func TestUnansweredPhoneRequestIsClosed(t *testing.T) {
	h := newHarness(t, true)
	c := h.client()
	// The retry receipt goes out and the phone never answers.
	c.Fail(whatsmeow.ErrMediaDownloadFailedWith404, nil)
	evt := watest.Text("OLD1", "", msgAt)
	evt.Message = &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Mimetype: proto.String("image/jpeg"), DirectPath: proto.String("/x")}}
	c.Emit(evt)
	watest.WaitFor(t, "the expired mark", func() bool { return h.status().Totals.MediaPhoneOnly == 1 })
	r, err := h.svc.RequestMedia(context.Background(), chatA, "OLD1", "")
	if err != nil || r.Requested != 1 {
		t.Fatalf("RequestMedia = %+v, %v", r, err)
	}
	// Left open, the request would be sent to the phone again on every restart, outside the cap.
	watest.WaitFor(t, "the request to close", func() bool {
		list, err := h.svc.in.db.Pending(context.Background(), nil, 10)
		return c.Retries() == 1 && err == nil && len(list) == 0
	})
	if s := h.status(); s.Totals.MediaPhoneOnly != 1 {
		t.Fatalf("media no longer marked as phone-only: %+v", s.Totals)
	}
}

func TestSecondInstanceOnTheSameFolderIsRefused(t *testing.T) {
	h := newHarness(t, true)
	svc, err := Open(context.Background(), Options{Dir: h.dir, Version: "0.1.0",
		NewClient: func(d *wastore.Device, l waLog.Logger) Client { return watest.New(d, l) }})
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
