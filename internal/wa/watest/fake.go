// Package watest has a stand-in for *whatsmeow.Client: no socket, no phone; sends are only recorded.
package watest

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

var OwnJID = types.NewJID("556411112222", types.DefaultUserServer)

type Client struct {
	mu       sync.Mutex
	device   *wastore.Device
	handlers []whatsmeow.EventHandler
	QR       chan whatsmeow.QRChannelItem
	phones   []string
	logouts  int
	connects int
	log      waLog.Logger
	retries  int
	// Set before the event that triggers them: what DownloadToFile and SendMediaRetryReceipt return.
	downloadErr, retryErr, logoutErr error
	sendErr                          error
	sent                             []Sent
	presences                        []string
	uploads                          [][]byte
}

// Sent is a message handed to SendMessage; nothing leaves the test.
type Sent struct {
	To      types.JID
	Message *waE2E.Message
}

func (f *Client) FailSend(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendErr = err
}

func (f *Client) SendChatPresence(ctx context.Context, jid types.JID, state types.ChatPresence, media types.ChatPresenceMedia) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.presences = append(f.presences, string(state)+" "+jid.String())
	return nil
}

func (f *Client) UploadReader(ctx context.Context, r io.Reader, tmp io.ReadWriteSeeker, mt whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return whatsmeow.UploadResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploads = append(f.uploads, b)
	sum := sha256.Sum256(b)
	return whatsmeow.UploadResponse{URL: "https://mmg.whatsapp.test/d/1", DirectPath: "/v/t62/d1", MediaKey: make([]byte, 32),
		FileSHA256: sum[:], FileEncSHA256: sum[:], FileLength: uint64(len(b))}, nil
}

func (f *Client) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, Sent{To: to, Message: msg})
	if f.sendErr != nil {
		return whatsmeow.SendResponse{}, f.sendErr
	}
	return whatsmeow.SendResponse{ID: fmt.Sprintf("3EB0SENT%d", len(f.sent)), Timestamp: time.Date(2026, 11, 24, 12, 0, 5, 0, time.UTC)}, nil
}

func (f *Client) Sent() []Sent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Sent(nil), f.sent...)
}

func (f *Client) Presences() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.presences...)
}

func (f *Client) Uploads() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.uploads...)
}

func (f *Client) Fail(download, retry error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downloadErr, f.retryErr = download, retry
}

func (f *Client) Retries() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.retries
}

// New takes the logger the service hands to whatsmeow, so a test can write what whatsmeow writes.
func New(device *wastore.Device, log waLog.Logger) *Client {
	return &Client{device: device, log: log, QR: make(chan whatsmeow.QRChannelItem, 8)}
}

func (f *Client) Log() waLog.Logger { return f.log }

func (f *Client) Connects() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connects
}

func (f *Client) Device() *wastore.Device { return f.device }

func (f *Client) AddEventHandler(h whatsmeow.EventHandler) uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, h)
	return uint32(len(f.handlers))
}

func (f *Client) GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return f.QR, nil
}

func (f *Client) ConnectContext(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects++
	return nil
}

func (f *Client) Disconnect() {}

func (f *Client) PairPhone(ctx context.Context, phone string, show bool, ct whatsmeow.PairClientType, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.phones = append(f.phones, phone)
	return "ABCD-EFGH", nil
}

func (f *Client) Logout(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logouts++
	return f.logoutErr
}

func (f *Client) FailLogout(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logoutErr = err
}

func (f *Client) ParseWebMessage(chat types.JID, msg *waWeb.WebMessageInfo) (*events.Message, error) {
	return nil, errors.New("watest: history parsing is not simulated")
}

func (f *Client) DownloadToFile(ctx context.Context, msg whatsmeow.DownloadableMessage, file whatsmeow.File) error {
	f.mu.Lock()
	err := f.downloadErr
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if _, ok := file.(*os.File); !ok {
		return errors.New("watest: download target is not a file")
	}
	_, err = io.WriteString(file, "IMG")
	return err
}

func (f *Client) SendMediaRetryReceipt(ctx context.Context, info *types.MessageInfo, key []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries++
	return f.retryErr
}

func (f *Client) Emit(evt any) {
	f.mu.Lock()
	hs := append([]whatsmeow.EventHandler(nil), f.handlers...)
	f.mu.Unlock()
	for _, h := range hs {
		h(evt)
	}
}

func (f *Client) Phones() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.phones...)
}

func (f *Client) Logouts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logouts
}

// Text is an incoming 1:1 message from 556499990000.
func Text(id, text string, at time.Time) *events.Message {
	chat := types.NewJID("556499990000", types.DefaultUserServer)
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            id, PushName: "Maria", Timestamp: at,
		},
		Message: &waE2E.Message{Conversation: proto.String(text)},
	}
}

// Factory records every client the service creates; paired marks the first device as linked.
type Factory struct {
	mu      sync.Mutex
	paired  bool
	clients []*Client
}

func NewFactory(paired bool) *Factory { return &Factory{paired: paired} }

func (f *Factory) New(d *wastore.Device, log waLog.Logger) *Client {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.paired && len(f.clients) == 0 {
		d.ID = &OwnJID
	}
	c := New(d, log)
	f.clients = append(f.clients, c)
	return c
}

func (f *Factory) Last() *Client {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clients[len(f.clients)-1]
}

func (f *Factory) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.clients)
}

// WaitFor rereads a condition; the ceiling only trips when the service hangs.
func WaitFor(t interface {
	Helper()
	Fatalf(string, ...any)
}, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
