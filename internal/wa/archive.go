package wa

import (
	"cmp"
	"context"
	"encoding/json"
	"log/slog"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
)

// original keeps the address as WhatsApp knows it: asking the phone for media needs it, not the
// normalized phone number of the chat.
type original struct {
	Chat    string          `json:"chat"`
	Sender  string          `json:"sender"`
	FromMe  bool            `json:"from_me"`
	Group   bool            `json:"group"`
	Message json.RawMessage `json:"message"`
}

type archiver struct {
	ctx context.Context
	db  *store.DB
	log *slog.Logger
}

func (a *archiver) normalize(cli Client, j types.JID) types.JID {
	j = j.ToNonAD()
	if d := cli.Device(); j.Server == types.HiddenUserServer && d.LIDs != nil {
		if pn, err := d.LIDs.GetPNForLID(a.ctx, j); err == nil && !pn.IsEmpty() {
			return pn.ToNonAD()
		}
	}
	return j
}

func ignored(chat types.JID) bool {
	return chat.Server == types.BroadcastServer || chat.Server == types.NewsletterServer
}

// message gets chat filled in from the history, where the contact's phone comes in the block.
// It reports whether it saved a message with media, the only kind the downloader wants to hear of,
// and false with failed when a message that should be kept was not saved (already logged).
func (a *archiver) message(cli Client, e *events.Message, chat types.JID) (media, failed bool) {
	info := e.Info
	if ignored(info.Chat) {
		return false, false
	}
	if chat.IsEmpty() {
		chat = a.normalize(cli, info.Chat)
	}
	if pm := e.Message.GetProtocolMessage(); pm != nil {
		if pm.GetType() == waE2E.ProtocolMessage_MESSAGE_EDIT {
			if err := a.db.Edit(a.ctx, chat.String(), pm.GetKey().GetID(), contentOf(pm.GetEditedMessage()).text); err != nil {
				a.log.Error("gravar edição", "erro", err)
				return false, true
			}
		}
		return false, false
	}
	c := contentOf(e.Message)
	if c.kind == "" {
		return false, false
	}
	msg, err := protojson.Marshal(e.Message)
	if err != nil {
		a.log.Error("serializar mensagem", "erro", err)
		return false, true
	}
	raw, _ := json.Marshal(original{
		Chat: info.Chat.String(), Sender: info.Sender.String(), FromMe: info.IsFromMe, Group: info.IsGroup, Message: msg,
	})
	r := store.Record{Chat: chat.String(), ID: info.ID, FromMe: info.IsFromMe, At: info.Timestamp,
		Kind: c.kind, Text: c.text, Mime: c.mime, ReplyTo: c.replyTo, Raw: string(raw)}
	if !info.IsFromMe {
		r.Sender = a.normalize(cli, info.Sender).String()
	}
	if err := a.db.Save(a.ctx, r); err != nil {
		a.log.Error("gravar mensagem", "id", info.ID, "erro", err)
		return false, true
	}
	if !info.IsFromMe && !info.IsGroup {
		_ = a.db.Contact(a.ctx, r.Chat, info.PushName, false)
	}
	return c.media != nil, false
}

func (a *archiver) history(cli Client, e *events.HistorySync) {
	total, failures := 0, 0
	for _, conv := range e.Data.GetConversations() {
		jid, err := types.ParseJID(conv.GetID())
		if err != nil || ignored(jid) {
			continue
		}
		chat := a.normalize(cli, jid)
		if pn, err := types.ParseJID(conv.GetPnJID()); err == nil && pn.Server == types.DefaultUserServer {
			chat = pn
		}
		if err := a.db.Contact(a.ctx, chat.String(), cmp.Or(conv.GetName(), conv.GetDisplayName()), true); err != nil {
			a.log.Error("gravar contato", "erro", err)
		}
		for _, item := range conv.GetMessages() {
			web := item.GetMessage()
			if web.GetMessage() == nil {
				continue
			}
			evt, err := cli.ParseWebMessage(jid, web)
			if err != nil {
				a.log.Warn("mensagem do histórico ilegível", "erro", err)
				continue
			}
			if _, failed := a.message(cli, evt, chat); failed {
				failures++
			} else {
				total++
			}
		}
	}
	a.addressBook(cli)
	log := a.log.Info
	if failures > 0 {
		log = a.log.Error
	}
	log("bloco do histórico gravado", "tipo", e.Data.GetSyncType().String(), "progresso", e.Data.GetProgress(),
		"mensagens", total, "falhas", failures)
}

// The phone's address book comes apart; its name wins over the name the person gave themselves.
func (a *archiver) addressBook(cli Client) {
	d := cli.Device()
	if d.Contacts == nil {
		return
	}
	book, err := d.Contacts.GetAllContacts(a.ctx)
	if err != nil {
		a.log.Warn("ler agenda", "erro", err)
		return
	}
	for jid, info := range book {
		name := cmp.Or(info.FullName, info.FirstName, info.BusinessName)
		_ = a.db.Contact(a.ctx, a.normalize(cli, jid).String(), cmp.Or(name, info.PushName), name != "")
	}
}
