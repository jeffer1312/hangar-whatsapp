package wa

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
)

const API = 1

// Pairing codes have no published expiry; the login socket closes when the QR codes run out
// (60 s plus five of 20 s).
const pairWindow = 160 * time.Second

const logoutTimeout = 10 * time.Second

// deviceName is how the phone lists this device; it follows the DeviceProps set in Open.
const deviceName = "Google Chrome (Linux)"

const (
	StateUnpaired  = "unpaired"
	StatePairing   = "pairing"
	StateConnected = "connected"
	StateLoggedOut = "logged_out"
	StateOutdated  = "outdated"
)

var (
	ErrNotPaired     = errors.New("not_paired")
	ErrAlreadyPaired = errors.New("este aparelho já está pareado; desconecte antes de parear de novo")
	ErrOutdated      = errors.New("outdated")
	ErrBadPhone      = errors.New("número inválido: só dígitos, com DDI (ex.: 5564999999999)")
	ErrUnavailable   = errors.New("a pasta de dados não reabriu depois de apagar; reinicie o serviço")
)

var phoneDigits = regexp.MustCompile(`^[1-9][0-9]{7,14}$`)

type Options struct {
	Dir       string
	Version   string
	Now       func() time.Time
	NewClient func(*wastore.Device) Client
	Log       *slog.Logger
}

type StatusError struct {
	Code string `json:"code"`
}

type Status struct {
	API               int          `json:"api"`
	Version           string       `json:"version"`
	State             string       `json:"state"`
	Error             *StatusError `json:"error"`
	Device            *string      `json:"device"`
	DeviceName        *string      `json:"device_name"`
	QR                *string      `json:"qr"`
	QRExpiresAt       *string      `json:"qr_expires_at"`
	PairCode          *string      `json:"pair_code"`
	PairCodeExpiresAt *string      `json:"pair_code_expires_at"`
	TZ                string       `json:"tz"`
	Totals            store.Totals `json:"totals"`
	Since             *string      `json:"since"`
	LastMessageAt     *string      `json:"last_message_at"`
}

// instance is everything opened on the data folder; wipe closes it and opens a new one.
type instance struct {
	db        *store.DB
	container *sqlstore.Container
	archive   *archiver
	media     *media
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

type Service struct {
	opt  Options
	root context.Context

	// rw guards in: readers hold it shared, wipe and close hold it alone.
	rw sync.RWMutex
	in *instance

	mu         sync.Mutex
	cli        Client
	gen        int
	state      string
	errCode    string
	qr, code   string
	qrExp      time.Time
	codeExp    time.Time
	pairCancel context.CancelFunc
}

func Open(ctx context.Context, opt Options) (*Service, error) {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.NewClient == nil {
		opt.NewClient = NewRealClient
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	dir, err := filepath.Abs(opt.Dir)
	if err != nil {
		return nil, err
	}
	opt.Dir = dir
	// The phone only sends the whole history if the pairing asks for it.
	wastore.DeviceProps.RequireFullSync = proto.Bool(true)
	wastore.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()
	wastore.DeviceProps.Os = proto.String("Linux")
	s := &Service{opt: opt, root: ctx}
	if err := s.start(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) now() time.Time { return s.opt.Now().UTC() }

func (s *Service) start() error {
	db, err := store.Open(s.opt.Dir)
	if err != nil {
		return err
	}
	sessionDB, err := store.OpenSession(store.SessionPath(s.opt.Dir))
	if err != nil {
		db.Close()
		return err
	}
	container := sqlstore.NewWithDB(sessionDB, "sqlite3", waLog.Noop)
	if err := container.Upgrade(s.root); err != nil {
		container.Close()
		db.Close()
		return errors.New("session.db em uso por outro processo ou ilegível: " + err.Error())
	}
	device, err := container.GetFirstDevice(s.root)
	if err != nil {
		container.Close()
		db.Close()
		return err
	}
	in := &instance{db: db, container: container}
	in.ctx, in.cancel = context.WithCancel(s.root)
	in.archive = &archiver{ctx: in.ctx, db: db, log: s.opt.Log}
	in.media = newMedia(db, s.opt.Dir, s.opt.Log, s.now, s.connectedClient)
	s.in = in

	s.mu.Lock()
	cli := s.newClientLocked(in, device)
	g := s.gen
	s.resetLocked(StateUnpaired, "")
	if device.ID != nil {
		s.state = StateConnected
		in.wg.Go(func() { s.connect(in.ctx, g, cli) })
	}
	s.mu.Unlock()
	in.wg.Go(func() { in.media.run(in.ctx) })
	return nil
}

// stop runs with rw held alone.
func (s *Service) stop() {
	in := s.in
	if in == nil {
		return
	}
	s.mu.Lock()
	if s.pairCancel != nil {
		s.pairCancel()
	}
	cli := s.cli
	s.cli = nil
	s.gen++
	s.resetLocked(StateUnpaired, "")
	s.mu.Unlock()
	if cli != nil {
		cli.Disconnect()
	}
	in.cancel()
	in.wg.Wait()
	in.container.Close()
	in.db.Close()
	s.in = nil
}

func (s *Service) Close() {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.stop()
}

func (s *Service) newClientLocked(in *instance, device *wastore.Device) Client {
	s.gen++
	g := s.gen
	cli := s.opt.NewClient(device)
	cli.AddEventHandler(func(evt any) { s.event(in, g, cli, evt) })
	s.cli = cli
	return cli
}

func (s *Service) resetLocked(state, errCode string) {
	s.state, s.errCode = state, errCode
	s.qr, s.code = "", ""
	s.qrExp, s.codeExp = time.Time{}, time.Time{}
}

// update applies fn only while generation g is still the current client.
func (s *Service) update(g int, fn func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g != s.gen {
		return false
	}
	fn()
	return true
}

func (s *Service) connectedClient() Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateConnected {
		return nil
	}
	return s.cli
}

// connect retries the first connection; after it, whatsmeow reconnects by itself.
func (s *Service) connect(ctx context.Context, g int, cli Client) {
	delay := 5 * time.Second
	for {
		err := cli.ConnectContext(ctx)
		if err == nil || errors.Is(err, whatsmeow.ErrAlreadyConnected) {
			return
		}
		s.opt.Log.Warn("conectar ao WhatsApp", "erro", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if s.update(g, func() {}) == false {
			return
		}
		delay = min(delay*2, 5*time.Minute)
	}
}

func (s *Service) event(in *instance, g int, cli Client, raw any) {
	switch e := raw.(type) {
	case *events.LoggedOut:
		s.opt.Log.Warn("o celular desconectou este aparelho", "motivo", e.Reason.String())
		s.update(g, func() { s.resetLocked(StateLoggedOut, "") })
	case *events.ClientOutdated:
		s.opt.Log.Error("o WhatsApp recusou esta versão do cliente; atualize o hangar-whatsapp")
		s.update(g, func() { s.resetLocked(StateOutdated, "") })
	case *events.MediaRetry:
		in.media.reply(e)
	case *events.Message:
		in.archive.message(cli, e, types.EmptyJID)
		in.media.wake()
	case *events.HistorySync:
		in.archive.history(cli, e)
		in.media.wake()
	}
}

// Pair starts pairing in the background; the QR and the code show up in Status.
func (s *Service) Pair(phone string) error {
	if phone != "" && !phoneDigits.MatchString(phone) {
		return ErrBadPhone
	}
	s.rw.RLock()
	defer s.rw.RUnlock()
	in, err := s.instance()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case StateConnected:
		return ErrAlreadyPaired
	case StateOutdated:
		return ErrOutdated
	}
	if s.pairCancel != nil {
		s.pairCancel()
	}
	// Always a new device and client: after a logout or a QR timeout the old one is spent.
	if old := s.cli; old != nil {
		go old.Disconnect()
	}
	cli := s.newClientLocked(in, in.container.NewDevice())
	g := s.gen
	s.resetLocked(StatePairing, "")
	ctx, cancel := context.WithCancel(in.ctx)
	s.pairCancel = cancel
	started := s.now()
	in.wg.Go(func() { s.pair(ctx, g, cli, phone, started) })
	return nil
}

func (s *Service) pair(ctx context.Context, g int, cli Client, phone string, started time.Time) {
	fail := func(code string) {
		cli.Disconnect()
		s.update(g, func() { s.resetLocked(StateUnpaired, code) })
	}
	qrs, err := cli.GetQRChannel(ctx)
	if err != nil {
		s.opt.Log.Error("abrir pareamento", "erro", err)
		fail("pair_failed")
		return
	}
	if err := cli.ConnectContext(ctx); err != nil {
		s.opt.Log.Error("conectar para parear", "erro", err)
		fail("connect_failed")
		return
	}
	askedCode := false
	for {
		var item whatsmeow.QRChannelItem
		var ok bool
		select {
		case <-ctx.Done():
			return
		case item, ok = <-qrs:
		}
		if !ok {
			return
		}
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			expires := s.now().Add(item.Timeout)
			s.update(g, func() { s.qr, s.qrExp = item.Code, expires })
			if phone != "" && !askedCode {
				askedCode = true
				code, err := cli.PairPhone(ctx, phone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
				if err != nil {
					s.opt.Log.Error("pedir código de pareamento", "erro", err)
					fail("pair_code_failed")
					return
				}
				s.update(g, func() { s.code, s.codeExp = code, started.Add(pairWindow) })
			}
		case whatsmeow.QRChannelSuccess.Event:
			s.opt.Log.Info("pareado; o histórico chega aos poucos")
			s.update(g, func() { s.resetLocked(StateConnected, "") })
			return
		case whatsmeow.QRChannelTimeout.Event:
			fail("pair_timeout")
			return
		case whatsmeow.QRChannelClientOutdated.Event:
			s.update(g, func() { s.resetLocked(StateOutdated, "") })
			return
		case whatsmeow.QRChannelEventPasskeyRequest, whatsmeow.QRChannelEventPasskeyResponse:
		default:
			s.opt.Log.Error("pareamento não concluído", "evento", item.Event, "erro", item.Error)
			fail("pair_failed")
			return
		}
	}
}

// Logout unlinks this device on the phone and keeps the data.
func (s *Service) Logout(ctx context.Context) error {
	s.rw.RLock()
	defer s.rw.RUnlock()
	s.mu.Lock()
	cli, g, state := s.cli, s.gen, s.state
	s.mu.Unlock()
	if state != StateConnected || cli == nil {
		return ErrNotPaired
	}
	// Bounded: a wipe waiting for rw would otherwise hold every other request behind this one.
	lctx, cancel := context.WithTimeout(ctx, logoutTimeout)
	defer cancel()
	if err := cli.Logout(lctx); err != nil {
		return err
	}
	s.update(g, func() { s.resetLocked(StateLoggedOut, "") })
	return nil
}

// Wipe logs out when it can, closes the databases and deletes messages.db, session.db and media/.
func (s *Service) Wipe(ctx context.Context) error {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.mu.Lock()
	cli, state := s.cli, s.state
	s.mu.Unlock()
	if state == StateConnected && cli != nil {
		lctx, cancel := context.WithTimeout(ctx, logoutTimeout)
		if err := cli.Logout(lctx); err != nil {
			s.opt.Log.Warn("desvincular antes de apagar", "erro", err)
		}
		cancel()
	}
	s.stop()
	// Reopens even when a delete failed: the service must keep answering, and what was left
	// behind is reported.
	var errs []error
	for _, path := range []string{store.MessagesPath(s.opt.Dir), store.SessionPath(s.opt.Dir)} {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, err)
			}
		}
	}
	errs = append(errs, os.RemoveAll(filepath.Join(s.opt.Dir, "media")))
	errs = append(errs, s.start())
	return errors.Join(errs...)
}

// instance returns the open data folder; nil only after a reopen failed, which the log explains.
func (s *Service) instance() (*instance, error) {
	if s.in == nil {
		return nil, ErrUnavailable
	}
	return s.in, nil
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func optionalTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	return optional(t.UTC().Format(store.TimeLayout))
}

func maskPhone(user string) string {
	if len(user) < 8 {
		return "••••"
	}
	return "+" + user[:4] + "••••" + user[len(user)-4:]
}

func (s *Service) Status(ctx context.Context) (Status, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	in, err := s.instance()
	if err != nil {
		return Status{}, err
	}
	sum, err := in.db.Totals(ctx)
	if err != nil {
		return Status{}, err
	}
	st := Status{API: API, Version: s.opt.Version, TZ: machineTZ(), Totals: sum.Totals,
		Since: optional(sum.Since), LastMessageAt: optional(sum.LastMessageAt)}
	s.mu.Lock()
	defer s.mu.Unlock()
	st.State = s.state
	if s.errCode != "" {
		st.Error = &StatusError{Code: s.errCode}
	}
	if s.state == StateConnected && s.cli != nil {
		if id := s.cli.Device().ID; id != nil {
			st.Device = optional(maskPhone(id.User))
			st.DeviceName = optional(deviceName)
		}
	}
	st.QR, st.QRExpiresAt = optional(s.qr), optionalTime(s.qrExp)
	st.PairCode, st.PairCodeExpiresAt = optional(s.code), optionalTime(s.codeExp)
	return st, nil
}

func (s *Service) Chats(ctx context.Context, q string, limit int) ([]store.Chat, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	in, err := s.instance()
	if err != nil {
		return nil, err
	}
	return in.db.Chats(ctx, q, limit)
}

func (s *Service) Messages(ctx context.Context, q store.MessagesQuery) (store.MessagesPage, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	in, err := s.instance()
	if err != nil {
		return store.MessagesPage{}, err
	}
	return in.db.Messages(ctx, q)
}

func (s *Service) Search(ctx context.Context, q, chat string, limit int) ([]store.Message, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	in, err := s.instance()
	if err != nil {
		return nil, err
	}
	return in.db.Search(ctx, q, chat, limit)
}

// RequestMedia marks old media for the phone to send; each one is a notification on the phone.
func (s *Service) RequestMedia(ctx context.Context, chat, id, since string) (store.MediaRequest, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	s.mu.Lock()
	state := s.state
	s.mu.Unlock()
	switch state {
	case StateConnected:
	case StateOutdated:
		return store.MediaRequest{}, ErrOutdated
	default:
		return store.MediaRequest{}, ErrNotPaired
	}
	in, err := s.instance()
	if err != nil {
		return store.MediaRequest{}, err
	}
	r, err := in.db.RequestMedia(ctx, chat, id, since, s.now())
	if err == nil && r.Requested > 0 {
		in.media.wake()
	}
	return r, err
}
