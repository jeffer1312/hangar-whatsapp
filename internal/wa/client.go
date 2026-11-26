package wa

import (
	"context"
	"io"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Client is the part of *whatsmeow.Client the service uses. SendMediaRetryReceipt asks the phone to
// upload old media again; GetProfilePictureInfo only reads; SendChatPresence, UploadReader and SendMessage send a draft the owner
// approved, and only outbox.go may call them (sendlist_test.go).
type Client interface {
	Device() *wastore.Device
	AddEventHandler(handler whatsmeow.EventHandler) uint32
	GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error)
	ConnectContext(ctx context.Context) error
	Disconnect()
	PairPhone(ctx context.Context, phone string, showPushNotification bool, clientType whatsmeow.PairClientType, clientDisplayName string) (string, error)
	Logout(ctx context.Context) error
	ParseWebMessage(chat types.JID, msg *waWeb.WebMessageInfo) (*events.Message, error)
	DownloadToFile(ctx context.Context, msg whatsmeow.DownloadableMessage, file whatsmeow.File) error
	SendMediaRetryReceipt(ctx context.Context, info *types.MessageInfo, mediaKey []byte) error
	GetProfilePictureInfo(ctx context.Context, jid types.JID, params *whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error)
	SendChatPresence(ctx context.Context, jid types.JID, state types.ChatPresence, media types.ChatPresenceMedia) error
	UploadReader(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	SendMessage(ctx context.Context, to types.JID, message *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error)
}

type realClient struct{ *whatsmeow.Client }

func (c realClient) Device() *wastore.Device { return c.Store }

func NewRealClient(device *wastore.Device, log waLog.Logger) Client {
	return realClient{whatsmeow.NewClient(device, log)}
}
