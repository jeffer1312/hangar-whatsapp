package wa

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/jeffer1312/hangar-whatsapp/internal/wa/watest"
)

func TestWhatsmeowLogReachesServiceLog(t *testing.T) {
	var out bytes.Buffer
	l := waLogger(slog.New(slog.NewTextHandler(&out, nil)), "whatsapp", nil).Sub("Socket")
	l.Debugf("frame %d", 1)
	l.Infof("frame %d", 2)
	l.Warnf("handler for %s failed", "M1")
	l.Errorf("failed to decrypt: %v", "no session")
	got := out.String()
	for _, want := range []string{`level=WARN msg="handler for M1 failed" módulo=whatsapp/Socket`,
		`level=ERROR msg="failed to decrypt: no session" módulo=whatsapp/Socket`} {
		if !strings.Contains(got, want) {
			t.Fatalf("log without %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "frame") {
		t.Fatalf("debug/info leaked:\n%s", got)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestConnectionEventsAreLogged(t *testing.T) {
	var out syncBuffer
	clients := watest.NewFactory(true)
	svc, err := Open(context.Background(), Options{Dir: t.TempDir(), Version: "0.1.0",
		Log:       slog.New(slog.NewTextHandler(&out, nil)),
		NewClient: func(d *wastore.Device, l waLog.Logger) Client { return clients.New(d, l) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	c := clients.Last()
	c.Emit(&events.Connected{})
	c.Emit(&events.Disconnected{})
	c.Emit(&events.StreamReplaced{})
	c.Emit(&events.TemporaryBan{Code: events.TempBanSentToTooManyPeople})
	c.Emit(&events.ConnectFailure{Reason: events.ConnectFailureServiceUnavailable})
	c.Emit(&events.UndecryptableMessage{Info: types.MessageInfo{ID: "M9"}})
	got := out.String()
	for _, want := range []string{"level=INFO msg=\"conectado ao WhatsApp\"", "level=WARN msg=\"conexão com o WhatsApp caiu\"",
		"level=ERROR msg=\"outro cliente abriu esta mesma sessão", "level=ERROR msg=\"o WhatsApp suspendeu esta conta",
		"level=ERROR msg=\"o WhatsApp recusou a conexão\"", "level=WARN msg=\"mensagem não decifrada\" id=M9"} {
		if !strings.Contains(got, want) {
			t.Fatalf("log without %q:\n%s", want, got)
		}
	}
}

// The text is whatsmeow's own (connectionevents.go, handleConnectSuccess): the trigger depends on it.
func TestFailedActiveModeReconnects(t *testing.T) {
	var out syncBuffer
	var clockMu sync.Mutex
	clock := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	advance := func(d time.Duration) { clockMu.Lock(); clock = clock.Add(d); clockMu.Unlock() }
	clients := watest.NewFactory(true)
	svc, err := Open(context.Background(), Options{Dir: t.TempDir(), Version: "0.1.0",
		Log:       slog.New(slog.NewTextHandler(&out, nil)),
		Now:       func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock },
		NewClient: func(d *wastore.Device, l waLog.Logger) Client { return clients.New(d, l) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	svc.reactivateBase = 200 * time.Millisecond
	c := clients.Last()
	watest.WaitFor(t, "the first connection", func() bool { return c.Connects() == 1 })
	c.Log().Sub("Socket").Warnf("Failed to send post-connect passive IQ: %v", "timed out")
	watest.WaitFor(t, "the reconnection", func() bool { return c.Connects() == 2 })
	if clients.Count() != 1 {
		t.Fatalf("reconnecting made %d clients, want the same one", clients.Count())
	}
	if !strings.Contains(out.String(), "reconectando") {
		t.Fatalf("reconnection not logged:\n%s", out.String())
	}
	c.Log().Warnf("Failed to get number of prekeys on server: %v", "x")
	if c.Connects() != 2 {
		t.Fatalf("an unrelated warning reconnected")
	}
	// A second failure waits before reconnecting, so a server that keeps refusing is not hammered.
	c.Log().Warnf("Failed to send post-connect passive IQ: %v", "timed out")
	if c.Connects() != 2 {
		t.Fatalf("the second reconnection did not wait")
	}
	watest.WaitFor(t, "the delayed reconnection", func() bool { return c.Connects() == 3 })
	watest.WaitFor(t, "the reconnection logged", func() bool { return strings.Contains(out.String(), "reconectado ao WhatsApp") })
	// Long after the last one, a new failure is a fresh case and reconnects right away again.
	advance(2 * time.Hour)
	c.Log().Warnf("Failed to send post-connect passive IQ: %v", "timed out")
	watest.WaitFor(t, "the fresh reconnection", func() bool { return c.Connects() == 4 })
	if n := strings.Count(out.String(), "em=0s"); n != 2 {
		t.Fatalf("%d immediate reconnections, want 2 (first ever and the fresh one):\n%s", n, out.String())
	}
}

func TestFailedActiveModeAfterLogoutDoesNotReconnect(t *testing.T) {
	clients := watest.NewFactory(true)
	svc, err := Open(context.Background(), Options{Dir: t.TempDir(), Version: "0.1.0",
		NewClient: func(d *wastore.Device, l waLog.Logger) Client { return clients.New(d, l) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	c := clients.Last()
	watest.WaitFor(t, "the first connection", func() bool { return c.Connects() == 1 })
	c.Emit(&events.LoggedOut{})
	c.Log().Warnf("Failed to send post-connect passive IQ: %v", "stream closed")
	svc.Close()
	if c.Connects() != 1 {
		t.Fatalf("reconnected a device the phone unlinked")
	}
}
