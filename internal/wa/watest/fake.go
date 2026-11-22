// Package watest has a stand-in for *whatsmeow.Client: no socket, no phone, nothing sent.
package watest

import (
	"context"
	"errors"
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
	"google.golang.org/protobuf/proto"
)

var OwnJID = types.NewJID("556411112222", types.DefaultUserServer)

type Client struct {
	mu        sync.Mutex
	device    *wastore.Device
	handlers  []whatsmeow.EventHandler
	QR        chan whatsmeow.QRChannelItem
	phones    []string
	logouts   int
	downloads int
}

func New(device *wastore.Device) *Client {
	return &Client{device: device, QR: make(chan whatsmeow.QRChannelItem, 8)}
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

func (f *Client) ConnectContext(ctx context.Context) error { return nil }

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
	return nil
}

func (f *Client) ParseWebMessage(chat types.JID, msg *waWeb.WebMessageInfo) (*events.Message, error) {
	return nil, errors.New("watest: history parsing is not simulated")
}

func (f *Client) DownloadToFile(ctx context.Context, msg whatsmeow.DownloadableMessage, file whatsmeow.File) error {
	f.mu.Lock()
	f.downloads++
	f.mu.Unlock()
	if _, ok := file.(*os.File); !ok {
		return errors.New("watest: download target is not a file")
	}
	_, err := io.WriteString(file, "IMG")
	return err
}

func (f *Client) SendMediaRetryReceipt(ctx context.Context, info *types.MessageInfo, key []byte) error {
	return nil
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

func (f *Factory) New(d *wastore.Device) *Client {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.paired && len(f.clients) == 0 {
		d.ID = &OwnJID
	}
	c := New(d)
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
