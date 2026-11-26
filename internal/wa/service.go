package wa

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

// API 2 adds the outbox (C5).
const API = 2

// Pairing codes have no published expiry; the login socket closes when the QR codes run out
// (60 s plus five of 20 s).
const pairWindow = 160 * time.Second

const logoutTimeout = 10 * time.Second

// A server that keeps refusing active mode gets reconnections spaced out from one minute doubling up
// to this: each one is a new login on the account.
const reactivateMaxWait = 30 * time.Minute

// deviceName is how the phone lists this device; it follows the DeviceProps set in Open.
const deviceName = "Google Chrome (Linux)"

const (
	StateUnpaired  = "unpaired"
	StatePairing   = "pairing"
	StateConnected = "connected"
	StateLoggedOut = "logged_out"
	StateOutdated  = "outdated"
)

// What Wipe says about the phone: whether it still lists this device.
const (
	LogoutDone      = "done"
	LogoutNotDone   = "not_done"
	LogoutNotPaired = "not_paired"
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
	Dir     string
	Version string
	Now     func() time.Time
	// NewClient gets the logger whatsmeow must write to: the service watches some of its warnings.
	NewClient func(*wastore.Device, waLog.Logger) Client
	Log       *slog.Logger
	// Wait holds the typing indicator before a send; tests skip it.
	Wait func(ctx context.Context, d time.Duration) error
	// AvatarTransport downloads contact pictures; nil is the network.
	AvatarTransport http.RoundTripper
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
	Outgoing          Outgoing     `json:"send"`
	// Until when WhatsApp refuses messages to new contacts; null without a lock.
	ReachoutLockedUntil *string `json:"reachout_locked_until"`
}

type Outgoing struct {
	LastMinute int `json:"last_minute"`
	Today      int `json:"today"`
	PerMinute  int `json:"per_minute"`
	PerDay     int `json:"per_day"`
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

	avatarMu   sync.Mutex
	avatarBusy map[string]bool
}

type Service struct {
	opt     Options
	root    context.Context
	tz      string
	avatars *http.Client

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
	// The first reconnection is right away; reactivateWait is the wait before the next one.
	// After a quiet stretch of 2×reactivateMaxWait it starts over.
	reactivateBase, reactivateWait time.Duration
	reactivatedAt                  time.Time

	// reachout orders a client's lock notice (generation check and write) with Pair replacing the
	// client, without holding mu during the database write.
	reachout sync.Mutex
}

func Open(ctx context.Context, opt Options) (*Service, error) {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opt.NewClient == nil {
		opt.NewClient = NewRealClient
	}
	if opt.Wait == nil {
		opt.Wait = wait
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
	s := &Service{opt: opt, root: ctx, tz: machineTZ(), reactivateBase: time.Minute, avatars: newAvatarClient(opt.AvatarTransport)}
	if err := s.start(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) now() time.Time { return s.opt.Now().UTC() }

func (s *Service) start() (err error) {
	db, err := store.Open(s.opt.Dir)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			db.Close()
		}
	}()
	sessionDB, err := store.OpenSession(store.SessionPath(s.opt.Dir))
	if err != nil {
		return err
	}
	container := sqlstore.NewWithDB(sessionDB, "sqlite3", waLogger(s.opt.Log, "sessão", nil))
	defer func() {
		if err != nil {
			container.Close()
		}
	}()
	if err := container.Upgrade(s.root); err != nil {
		return errors.New("session.db em uso por outro processo ou ilegível: " + err.Error())
	}
	device, err := container.GetFirstDevice(s.root)
	if err != nil {
		return err
	}
	in := &instance{db: db, container: container, avatarBusy: map[string]bool{}}
	in.ctx, in.cancel = context.WithCancel(s.root)
	// Before any worker starts: a draft left sending would otherwise stay so with nobody to finish it.
	if err := s.recoverOutbox(in); err != nil {
		in.cancel()
		return fmt.Errorf("recuperar rascunhos: %w", err)
	}
	in.archive = &archiver{ctx: in.ctx, db: db, log: s.opt.Log}
	in.media = newMedia(db, s.opt.Dir, s.opt.Log, s.now, s.connectedClient)
	s.in = in

	s.mu.Lock()
	cli, g := s.newClientLocked(in, device)
	s.resetLocked(StateUnpaired, "")
	if device.ID != nil {
		s.state = StateConnected
		in.wg.Go(func() { s.connect(in.ctx, g, cli) })
	}
	s.mu.Unlock()
	if device.ID != nil {
		s.adoptMessages(in, cli)
	}
	in.wg.Go(func() { in.media.run(in.ctx) })
	in.wg.Go(func() { s.expireLoop(in) })
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

// with runs fn on the open data folder, holding rw shared so a wipe waits for it. It is the only
// way in: after a reopen that failed there is no folder, and fn never sees a nil instance.
func with[T any](s *Service, fn func(in *instance) (T, error)) (T, error) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	if s.in == nil {
		var zero T
		return zero, ErrUnavailable
	}
	return fn(s.in)
}

func (s *Service) newClientLocked(in *instance, device *wastore.Device) (Client, int) {
	s.gen++
	g := s.gen
	var cli Client
	cli = s.opt.NewClient(device, waLogger(s.opt.Log, "whatsapp", func() { s.reactivate(in, g, cli) }))
	cli.AddEventHandler(func(evt any) { s.event(in, g, cli, evt) })
	s.cli = cli
	return cli, g
}

// reactivate reconnects when the server was not told the device is active: the new login makes
// whatsmeow ask again, and no new kind of outgoing call is needed.
func (s *Service) reactivate(in *instance, g int, cli Client) {
	// Under mu with the generation checked, so stop, which bumps it first, always waits for this.
	s.update(g, func() {
		if s.state == StateLoggedOut || s.state == StateOutdated {
			s.opt.Log.Warn("o WhatsApp não pôs o aparelho no modo ativo, mas ele já saiu; sem reconectar", "estado", s.state)
			return
		}
		now := s.now()
		if now.Sub(s.reactivatedAt) > 2*reactivateMaxWait {
			s.reactivateWait = 0
		}
		s.reactivatedAt = now
		wait := s.reactivateWait
		s.reactivateWait = min(max(2*wait, s.reactivateBase), reactivateMaxWait)
		s.opt.Log.Error("o WhatsApp não pôs o aparelho no modo ativo, sem ele não chegam mensagens; reconectando",
			"em", wait.String())
		in.wg.Go(func() {
			select {
			case <-in.ctx.Done():
				return
			case <-time.After(wait):
			}
			if !s.current(g) {
				return
			}
			cli.Disconnect()
			s.connect(in.ctx, g, cli)
			if in.ctx.Err() == nil && s.current(g) {
				s.opt.Log.Info("reconectado ao WhatsApp; o whatsmeow repete o pedido de modo ativo")
			}
		})
	})
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

func (s *Service) current(g int) bool { return s.update(g, func() {}) }

func (s *Service) connectedClient() Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateConnected {
		return nil
	}
	return s.cli
}

// pairedClient is the connected client, or why there is none.
func (s *Service) pairedClient() (Client, error) {
	switch cli, _, state := s.snapshot(); {
	case state == StateConnected && cli != nil:
		return cli, nil
	case state == StateOutdated:
		return nil, ErrOutdated
	default:
		return nil, ErrNotPaired
	}
}

// snapshot is the current client, its generation and state, read together.
func (s *Service) snapshot() (Client, int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cli, s.gen, s.state
}

// logout is bounded: a wipe waiting for rw would otherwise hold every request behind it.
func logout(ctx context.Context, cli Client) error {
	ctx, cancel := context.WithTimeout(ctx, logoutTimeout)
	defer cancel()
	return cli.Logout(ctx)
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
		if !s.current(g) {
			return
		}
		delay = min(delay*2, 5*time.Minute)
	}
}

func (s *Service) event(in *instance, g int, cli Client, raw any) {
	switch e := raw.(type) {
	case *events.Connected:
		s.opt.Log.Info("conectado ao WhatsApp")
		s.adoptMessages(in, cli)
	case *events.Disconnected:
		s.opt.Log.Warn("conexão com o WhatsApp caiu")
	case *events.StreamReplaced:
		s.opt.Log.Error("outro cliente abriu esta mesma sessão; este parou de receber")
	case *events.TemporaryBan:
		s.opt.Log.Error("o WhatsApp suspendeu esta conta por um tempo", "motivo", e.String())
	case *events.ConnectFailure:
		s.opt.Log.Error("o WhatsApp recusou a conexão", "motivo", e.Reason.String(), "mensagem", e.Message)
	case *events.UndecryptableMessage:
		s.opt.Log.Warn("mensagem não decifrada", "id", e.Info.ID, "conversa", e.Info.Chat.String())
	case *events.LoggedOut:
		s.opt.Log.Warn("o celular desconectou este aparelho", "motivo", e.Reason.String())
		s.update(g, func() { s.resetLocked(StateLoggedOut, "") })
	case *events.ClientOutdated:
		s.opt.Log.Error("o WhatsApp recusou esta versão do cliente; atualize o hangar-whatsapp")
		s.update(g, func() { s.resetLocked(StateOutdated, "") })
	case *events.MediaRetry:
		in.media.reply(e)
	case *events.NotifyAccountReachoutTimelock:
		// A client Pair replaced cannot set or lift the new one's lock. The write stays outside mu: the
		// archiver may hold the database for seconds.
		s.reachout.Lock()
		if s.current(g) {
			s.lockReachout(in, e)
		}
		s.reachout.Unlock()
	case *events.Message:
		if media, _ := in.archive.message(cli, e, types.EmptyJID); media {
			in.media.wake()
		}
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
	_, err := with(s, func(in *instance) (struct{}, error) {
		s.reachout.Lock()
		defer s.reachout.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		switch s.state {
		case StateConnected:
			return struct{}{}, ErrAlreadyPaired
		case StateOutdated:
			return struct{}{}, ErrOutdated
		}
		if s.pairCancel != nil {
			s.pairCancel()
		}
		// Always a new device and client: after a logout or a QR timeout the old one is spent.
		if old := s.cli; old != nil {
			go old.Disconnect()
		}
		cli, g := s.newClientLocked(in, in.container.NewDevice())
		s.resetLocked(StatePairing, "")
		ctx, cancel := context.WithCancel(in.ctx)
		s.pairCancel = cancel
		started := s.now()
		in.wg.Go(func() { s.pair(ctx, g, cli, phone, started) })
		return struct{}{}, nil
	})
	return err
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
	_, err := with(s, func(*instance) (struct{}, error) {
		cli, g, state := s.snapshot()
		if state != StateConnected || cli == nil {
			return struct{}{}, ErrNotPaired
		}
		if err := logout(ctx, cli); err != nil {
			return struct{}{}, err
		}
		s.update(g, func() { s.resetLocked(StateLoggedOut, "") })
		return struct{}{}, nil
	})
	return err
}

// Wipe logs out when it can, closes the databases and deletes messages.db, session.db and media/.
// It says whether the phone was told (LogoutDone) or may still list this device (LogoutNotDone).
func (s *Service) Wipe(ctx context.Context) (string, error) {
	s.rw.Lock()
	defer s.rw.Unlock()
	result := LogoutNotPaired
	if cli, _, state := s.snapshot(); state == StateConnected && cli != nil {
		result = LogoutDone
		if err := logout(ctx, cli); err != nil {
			s.opt.Log.Warn("desvincular antes de apagar", "erro", err)
			result = LogoutNotDone
		}
	}
	s.stop()
	// Reopens even when a delete failed: the service must keep answering, and what was left
	// behind is reported.
	errs := []error{
		store.RemoveDB(store.MessagesPath(s.opt.Dir)),
		store.RemoveDB(store.SessionPath(s.opt.Dir)),
		os.RemoveAll(filepath.Join(s.opt.Dir, "media")),
		os.RemoveAll(outboxDir(s.opt.Dir)),
		s.start(),
	}
	return result, errors.Join(errs...)
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

// adoptMessages gives the messages archived before the number was kept to the number paired now.
func (s *Service) adoptMessages(in *instance, cli Client) {
	if err := in.db.AdoptMessages(in.ctx, accountOf(cli)); err != nil {
		s.opt.Log.Error("ligar as mensagens antigas ao número pareado", "erro", err)
	}
}

func (s *Service) Status(ctx context.Context) (Status, error) {
	return with(s, func(in *instance) (Status, error) {
		sum, err := in.db.Totals(ctx)
		if err != nil {
			return Status{}, err
		}
		minute, day, err := in.db.CountSends(ctx, s.now())
		if err != nil {
			return Status{}, err
		}
		locked, err := s.reachoutLocked(ctx, in)
		if err != nil {
			return Status{}, err
		}
		st := Status{API: API, Version: s.opt.Version, TZ: s.tz, Totals: sum.Totals,
			Since: optional(sum.Since), LastMessageAt: optional(sum.LastMessageAt),
			Outgoing:            Outgoing{LastMinute: minute, Today: day, PerMinute: store.SendsPerMinute, PerDay: store.SendsPerDay},
			ReachoutLockedUntil: optional(locked)}
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
	})
}

func (s *Service) Chats(ctx context.Context, q string, limit int) ([]store.Chat, error) {
	return with(s, func(in *instance) ([]store.Chat, error) { return in.db.Chats(ctx, q, limit) })
}

func (s *Service) Messages(ctx context.Context, q store.MessagesQuery) (store.MessagesPage, error) {
	return with(s, func(in *instance) (store.MessagesPage, error) { return in.db.Messages(ctx, q) })
}

func (s *Service) Search(ctx context.Context, q, chat string, limit int) ([]store.Message, error) {
	return with(s, func(in *instance) ([]store.Message, error) { return in.db.Search(ctx, q, chat, limit) })
}

// RequestMedia marks old media for the phone to send; each one is a notification on the phone.
func (s *Service) RequestMedia(ctx context.Context, chat, id, since string) (store.MediaRequest, error) {
	return with(s, func(in *instance) (store.MediaRequest, error) {
		if _, err := s.pairedClient(); err != nil {
			return store.MediaRequest{}, err
		}
		r, err := in.db.RequestMedia(ctx, chat, id, since, s.now())
		if err == nil && r.Requested > 0 {
			in.media.wake()
		}
		return r, err
	})
}
