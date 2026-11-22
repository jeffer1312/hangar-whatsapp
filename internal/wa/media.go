package wa

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
)

// Few at a time: each old media becomes a request to the phone, and a burst of that stands out.
const simultaneousDownloads = 3

const phoneWait = 90 * time.Second

type media struct {
	db     *store.DB
	dir    string
	log    *slog.Logger
	now    func() time.Time
	client func() Client
	wakeup chan struct{}

	mu       sync.Mutex
	waiting  map[string]chan *events.MediaRetry
	inFlight map[[2]string]bool
	// Transient failure (network): not retried in this run, the next run tries again.
	postponed map[[2]string]bool
}

func newMedia(db *store.DB, dir string, log *slog.Logger, now func() time.Time, client func() Client) *media {
	return &media{
		db: db, dir: dir, log: log, now: now, client: client, wakeup: make(chan struct{}, 1),
		waiting: map[string]chan *events.MediaRetry{}, inFlight: map[[2]string]bool{}, postponed: map[[2]string]bool{},
	}
}

func (m *media) wake() {
	select {
	case m.wakeup <- struct{}{}:
	default:
	}
}

func (m *media) run(ctx context.Context) {
	queue := make(chan store.Pending)
	var workers sync.WaitGroup
	for range simultaneousDownloads {
		workers.Go(func() {
			for p := range queue {
				m.download(ctx, p)
				m.mu.Lock()
				delete(m.inFlight, [2]string{p.Chat, p.ID})
				m.mu.Unlock()
			}
		})
	}
	defer workers.Wait()
	defer close(queue)
	for {
		var list []store.Pending
		if m.client() != nil {
			m.mu.Lock()
			skip := map[[2]string]bool{}
			for k := range m.inFlight {
				skip[k] = true
			}
			for k := range m.postponed {
				skip[k] = true
			}
			m.mu.Unlock()
			var err error
			list, err = m.db.Pending(ctx, skip, 50)
			if err != nil && ctx.Err() == nil {
				m.log.Error("ler mídias pendentes", "erro", err)
			}
		}
		for _, p := range list {
			m.mu.Lock()
			m.inFlight[[2]string{p.Chat, p.ID}] = true
			m.mu.Unlock()
			select {
			case queue <- p:
			case <-ctx.Done():
				return
			}
		}
		if len(list) > 0 {
			continue
		}
		select {
		case <-m.wakeup:
		case <-time.After(30 * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

var errPhoneRefused = errors.New("o celular não tem mais esta mídia")

func (m *media) download(ctx context.Context, p store.Pending) {
	cli := m.client()
	if cli == nil {
		return
	}
	key := [2]string{p.Chat, p.ID}
	target, info, ext, err := m.read(p)
	if err != nil {
		m.log.Warn("referência de mídia ilegível", "id", p.ID, "erro", err)
		m.lost(ctx, p, "unreadable")
		return
	}
	rel := store.MediaRel(p.Chat, p.ID, ext)
	err = m.save(ctx, cli, target, rel)
	asked := false
	if expired(err) && p.Requested {
		asked = true
		// Old media leaves the WhatsApp server; the phone, if it still has the file, uploads it again.
		target, err = m.askPhone(ctx, cli, info, target)
		if err == nil {
			err = m.save(ctx, cli, target, rel)
		}
	}
	switch {
	case err == nil:
		if err := m.db.MediaSaved(ctx, p.Chat, p.ID, rel, m.now()); err != nil {
			m.log.Error("gravar caminho da mídia", "erro", err)
		}
	case ctx.Err() != nil:
	case errors.Is(err, errPhoneRefused) || (expired(err) && p.Requested):
		m.log.Info("mídia perdida", "id", p.ID, "erro", err)
		m.lost(ctx, p, "phone_refused")
	case expired(err):
		m.lost(ctx, p, "expired")
	case asked:
		// Left open, every restart would notify the phone again outside the daily cap.
		m.log.Warn("o celular não atendeu o pedido de mídia; peça de novo se precisar", "id", p.ID, "erro", err)
		if err := m.db.CloseRequest(ctx, p.Chat, p.ID, m.now()); err != nil {
			m.log.Error("fechar pedido de mídia", "erro", err)
		}
	default:
		m.log.Warn("mídia adiada para a próxima execução", "id", p.ID, "erro", err)
		m.mu.Lock()
		m.postponed[key] = true
		m.mu.Unlock()
	}
}

func (m *media) lost(ctx context.Context, p store.Pending, code string) {
	if err := m.db.MediaLost(ctx, p.Chat, p.ID, code, m.now()); err != nil {
		m.log.Error("gravar mídia perdida", "erro", err)
	}
}

func (m *media) read(p store.Pending) (whatsmeow.DownloadableMessage, *types.MessageInfo, string, error) {
	var o original
	if err := json.Unmarshal([]byte(p.Raw), &o); err != nil {
		return nil, nil, "", err
	}
	var msg waE2E.Message
	if err := protojson.Unmarshal(o.Message, &msg); err != nil {
		return nil, nil, "", err
	}
	c := contentOf(&msg)
	if c.media == nil {
		return nil, nil, "", errors.New("mensagem sem mídia")
	}
	info := &types.MessageInfo{ID: p.ID, MessageSource: types.MessageSource{IsFromMe: o.FromMe, IsGroup: o.Group}}
	var err error
	if info.Chat, err = types.ParseJID(o.Chat); err != nil {
		return nil, nil, "", err
	}
	if o.Group {
		if info.Sender, err = types.ParseJID(o.Sender); err != nil {
			return nil, nil, "", err
		}
	}
	return c.media, info, c.ext, nil
}

// save downloads to a temporary file in the same folder and renames it: a half file never has
// the final name.
func (m *media) save(ctx context.Context, cli Client, target whatsmeow.DownloadableMessage, rel string) error {
	dest, ok := store.MediaAbs(m.dir, rel)
	if !ok {
		return fmt.Errorf("caminho de mídia fora de media/: %s", rel)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".downloading-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := cli.DownloadToFile(ctx, target, tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dest)
}

// Also without an address: old history messages sometimes come with only the key, and the
// phone sends the address.
func expired(err error) bool {
	return errors.Is(err, whatsmeow.ErrNoURLPresent) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

func (m *media) askPhone(ctx context.Context, cli Client, info *types.MessageInfo, target whatsmeow.DownloadableMessage) (whatsmeow.DownloadableMessage, error) {
	reply := make(chan *events.MediaRetry, 1)
	// Registered before asking: the reply can arrive before the send returns.
	m.mu.Lock()
	m.waiting[info.ID] = reply
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.waiting, info.ID)
		m.mu.Unlock()
	}()
	if err := cli.SendMediaRetryReceipt(ctx, info, target.GetMediaKey()); err != nil {
		return nil, fmt.Errorf("pedir mídia ao celular: %w", err)
	}
	var e *events.MediaRetry
	select {
	case e = <-reply:
	case <-time.After(phoneWait):
		return nil, errors.New("o celular não respondeu ao pedido da mídia")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if e.Error != nil {
		return nil, fmt.Errorf("%w (código %d)", errPhoneRefused, e.Error.Code)
	}
	retry, err := whatsmeow.DecryptMediaRetryNotification(e, target.GetMediaKey())
	if err != nil {
		return nil, fmt.Errorf("decifrar resposta do celular: %w", err)
	}
	if retry.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS || retry.GetDirectPath() == "" {
		return nil, fmt.Errorf("%w (%s)", errPhoneRefused, retry.GetResult())
	}
	// Every media type has directPath; the download uses it, not the URL.
	reflected := target.(interface{ ProtoReflect() protoreflect.Message }).ProtoReflect()
	reflected.Set(reflected.Descriptor().Fields().ByName("directPath"), protoreflect.ValueOfString(retry.GetDirectPath()))
	return target, nil
}

func (m *media) reply(e *events.MediaRetry) {
	m.mu.Lock()
	ch := m.waiting[e.MessageID]
	m.mu.Unlock()
	if ch != nil {
		select {
		case ch <- e:
		default:
		}
	}
}
