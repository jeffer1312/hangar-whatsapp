package wa

import (
	"context"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waWeb"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Client is the part of *whatsmeow.Client the service uses. Nothing here sends a message: the
// only outgoing call is SendMediaRetryReceipt, which asks the phone to upload old media again.
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
}

type realClient struct{ *whatsmeow.Client }

func (c realClient) Device() *wastore.Device { return c.Store }

func NewRealClient(device *wastore.Device, log waLog.Logger) Client {
	return realClient{whatsmeow.NewClient(device, log)}
}
