package wa

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
)

const maxDraftText = 4096

// Drafts expire on every read and also on this tick, which deletes the copies they leave.
const expiryTick = 30 * time.Second

var (
	ErrBadDraft        = errors.New("rascunho inválido")
	ErrFileRefused     = errors.New("anexo recusado")
	ErrFileTooLarge    = errors.New("anexo acima de 100 MB")
	ErrUnsupportedChat = errors.New("sem envio para lista de transmissão nem canal")
	ErrReachoutLocked  = errors.New("o WhatsApp travou o envio a contatos novos")
)

// DraftRequest is what a session asks to send; Origin and OriginCwd are declared by the Hangar.
type DraftRequest struct {
	Origin, OriginCwd, Chat, Text, ReplyTo, File string
}

func badDraft(msg string) error { return fmt.Errorf("%w: %s", ErrBadDraft, msg) }

func checkText(text string, file bool) error {
	switch {
	case utf8.RuneCountInString(text) > maxDraftText:
		return badDraft(fmt.Sprintf("o texto passa de %d caracteres", maxDraftText))
	case strings.TrimSpace(text) == "" && !file:
		return badDraft("texto vazio sem anexo")
	}
	return nil
}

var newlines = strings.NewReplacer("\r\n", "\n", "\r", "\n")

func outboxDir(dir string) string { return filepath.Join(dir, "outbox") }

// copyDir holds the copy of a draft's attachment.
func (s *Service) copyDir(id string) string { return filepath.Join(outboxDir(s.opt.Dir), id) }

func newDraftID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Service) CreateDraft(ctx context.Context, req DraftRequest) (store.Draft, bool, error) {
	type result struct {
		draft   store.Draft
		created bool
	}
	r, err := with(s, func(in *instance) (result, error) {
		d, created, err := s.createDraft(ctx, in, req)
		return result{d, created}, err
	})
	return r.draft, r.created, err
}

func (s *Service) createDraft(ctx context.Context, in *instance, req DraftRequest) (store.Draft, bool, error) {
	if strings.TrimSpace(req.Origin) == "" {
		return store.Draft{}, false, badDraft("origin é obrigatório")
	}
	if err := checkText(req.Text, req.File != ""); err != nil {
		return store.Draft{}, false, err
	}
	jid, err := types.ParseJID(req.Chat)
	if err != nil {
		return store.Draft{}, false, badDraft("chat inválido: " + err.Error())
	}
	jid = jid.ToNonAD()
	switch jid.Server {
	case types.DefaultUserServer, types.GroupServer, types.HiddenUserServer:
	default:
		return store.Draft{}, false, ErrUnsupportedChat
	}
	cli, err := s.pairedClient()
	if err != nil {
		return store.Draft{}, false, err
	}
	if err := s.expire(ctx, in); err != nil {
		return store.Draft{}, false, err
	}
	info, err := in.db.OutboxChat(ctx, jid.String(), accountOf(cli))
	if err != nil {
		return store.Draft{}, false, err
	}
	d := store.Draft{ID: newDraftID(), Origin: req.Origin, Chat: jid.String(), Address: info.Address, Recent: info.Recent,
		Text: req.Text, ChatKind: "contact", ChatNumber: jid.User}
	if jid.Server == types.GroupServer {
		d.ChatKind, d.ChatNumber = "group", jid.String()
		if info.Name != "" {
			d.ChatName, d.ChatNameSource = info.Name, "group"
		}
	} else {
		if jid.Server != types.DefaultUserServer {
			d.ChatNumber = jid.String()
		}
		d.ChatName, d.ChatNameSource = s.contactName(ctx, cli.Device(), jid, info.Name)
	}
	if req.ReplyTo != "" {
		text, raw, err := in.db.Quoted(ctx, d.Chat, req.ReplyTo)
		if errors.Is(err, store.ErrNotFound) {
			return store.Draft{}, false, badDraft("reply_to não existe nesta conversa")
		} else if err != nil {
			return store.Draft{}, false, err
		}
		d.ReplyTo, d.QuotedText, d.QuotedRaw = &req.ReplyTo, &text, raw
	}
	copyDir := s.copyDir(d.ID)
	if req.File != "" {
		if d.File, err = attach(req.File, req.OriginCwd, s.opt.Dir, copyDir); err != nil {
			return store.Draft{}, false, err
		}
	}
	got, created, err := in.db.CreateDraft(ctx, d, s.now())
	if d.File != nil && (err != nil || !created) {
		os.RemoveAll(copyDir)
	}
	if err != nil {
		return got, created, err
	}
	if created {
		s.fetchAvatar(in, cli, got.Chat)
	}
	return s.withAvatar(ctx, in, got), created, nil
}

// contactName prefers the phone's address book ("contact") to the name people give themselves
// ("push"); the archive's name has no known origin.
func (s *Service) contactName(ctx context.Context, dev *wastore.Device, jid types.JID, archived string) (string, string) {
	if dev.Contacts == nil {
		return archived, ""
	}
	jids := []types.JID{jid}
	if dev.LIDs != nil {
		var other types.JID
		var err error
		if jid.Server == types.DefaultUserServer {
			other, err = dev.LIDs.GetLIDForPN(ctx, jid)
		} else {
			other, err = dev.LIDs.GetPNForLID(ctx, jid)
		}
		if err != nil {
			s.opt.Log.Warn("ligar número e LID do contato", "conversa", jid.String(), "erro", err)
		} else if !other.IsEmpty() {
			jids = append(jids, other)
		}
	}
	push := ""
	for _, j := range jids {
		c, err := dev.Contacts.GetContact(ctx, j)
		if err != nil {
			s.opt.Log.Warn("ler contato na agenda do aparelho", "contato", j.String(), "erro", err)
			continue
		}
		if !c.Found {
			continue
		}
		if name := cmp.Or(c.FullName, c.FirstName); name != "" {
			return name, "contact"
		}
		push = cmp.Or(push, c.PushName, c.BusinessName)
	}
	if push != "" {
		return push, "push"
	}
	return archived, ""
}

// expire closes overdue drafts and deletes the copies of every final draft, also those another
// transaction (a decline, a creation) expired along the way.
func (s *Service) expire(ctx context.Context, in *instance) error {
	_, err := in.db.ExpireDrafts(ctx, s.now())
	s.sweep(ctx, in, false)
	return err
}

// sweep deletes the copies of drafts in a final state. At startup it also deletes folders with no
// draft; later such a folder may belong to a draft being created.
func (s *Service) sweep(ctx context.Context, in *instance, startup bool) {
	entries, err := os.ReadDir(outboxDir(s.opt.Dir))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			s.opt.Log.Error("ler outbox/", "erro", err)
		}
		return
	}
	for _, e := range entries {
		d, err := in.db.Draft(ctx, e.Name())
		if err != nil && !errors.Is(err, store.ErrDraftNotFound) {
			s.opt.Log.Error("ler rascunho da cópia do anexo", "rascunho", e.Name(), "erro", err)
			continue
		}
		final := err == nil && d.State != store.OutboxPending && d.State != store.OutboxSending
		if final || (startup && errors.Is(err, store.ErrDraftNotFound)) {
			if err := os.RemoveAll(s.copyDir(e.Name())); err != nil {
				s.opt.Log.Error("apagar cópia do anexo", "rascunho", e.Name(), "erro", err)
			}
		}
	}
}

// recoverOutbox runs before the API answers: a send the last run left halfway is failed, never
// retried, and copies without a pending draft go.
func (s *Service) recoverOutbox(in *instance) error {
	n, err := in.db.InterruptSending(in.ctx)
	if err != nil {
		return err
	}
	if n > 0 {
		s.opt.Log.Warn("envios interrompidos pela parada anterior marcados como falha, sem reenvio", "rascunhos", n)
	}
	if _, err := in.db.ExpireDrafts(in.ctx, s.now()); err != nil {
		return err
	}
	s.sweep(in.ctx, in, true)
	return nil
}

func (s *Service) expireLoop(in *instance) {
	t := time.NewTicker(expiryTick)
	defer t.Stop()
	for {
		select {
		case <-in.ctx.Done():
			return
		case <-t.C:
			if err := s.expire(in.ctx, in); err != nil && in.ctx.Err() == nil {
				s.opt.Log.Error("expirar rascunhos", "erro", err)
			}
		}
	}
}

func (s *Service) Draft(ctx context.Context, id string) (store.Draft, error) {
	return with(s, func(in *instance) (store.Draft, error) {
		if err := s.expire(ctx, in); err != nil {
			return store.Draft{}, err
		}
		d, err := in.db.Draft(ctx, id)
		if err != nil {
			return d, err
		}
		return s.withAvatar(ctx, in, d), nil
	})
}

var draftStates = map[string]bool{store.OutboxPending: true, store.OutboxSending: true, store.OutboxSent: true,
	store.OutboxDeclined: true, store.OutboxExpired: true, store.OutboxFailed: true}

func (s *Service) Drafts(ctx context.Context, state string) ([]store.Draft, error) {
	if !draftStates[state] {
		return nil, badDraft("state deve ser pending, sending, sent, declined, expired ou failed")
	}
	return with(s, func(in *instance) ([]store.Draft, error) {
		if err := s.expire(ctx, in); err != nil {
			return nil, err
		}
		list, err := in.db.Drafts(ctx, state)
		for i := range list {
			list[i] = s.withAvatar(ctx, in, list[i])
		}
		return list, err
	})
}

func (s *Service) reachoutLocked(ctx context.Context, in *instance) (string, error) {
	until, err := in.db.ReachoutLock(ctx)
	if err != nil || until <= s.now().Format(store.TimeLayout) {
		return "", err
	}
	return until, nil
}

func (s *Service) checkReachout(ctx context.Context, in *instance) error {
	until, err := s.reachoutLocked(ctx, in)
	if err == nil && until != "" {
		err = fmt.Errorf("%w até %s", ErrReachoutLocked, until)
	}
	return err
}

// ApproveDraft approves a pending draft. The send runs after the answer, on the service's context, so
// the request ending does not cut it.
func (s *Service) ApproveDraft(ctx context.Context, id, via string) (store.Draft, error) {
	return s.approve(ctx, id, via, nil)
}

// ApproveEditedDraft approves a pending draft with the owner's text in place of the session's. A text
// equal to the draft's once line breaks are normalized is a plain approval.
func (s *Service) ApproveEditedDraft(ctx context.Context, id, via, text string) (store.Draft, error) {
	text = newlines.Replace(text)
	return s.approve(ctx, id, via, &text)
}

func (s *Service) approve(ctx context.Context, id, via string, text *string) (store.Draft, error) {
	if via != "desktop" && via != "mobile" {
		return store.Draft{}, badDraft(`via deve ser "desktop" ou "mobile"`)
	}
	return with(s, func(in *instance) (store.Draft, error) {
		if err := s.expire(ctx, in); err != nil {
			return store.Draft{}, err
		}
		// The draft's own state answers first: an expired draft says expired even while unpaired.
		d, err := in.db.Draft(ctx, id)
		if err == nil {
			err = store.StateErr(d.State)
		}
		if err != nil {
			return store.Draft{}, err
		}
		if text != nil {
			if err := checkText(*text, d.File != nil); err != nil {
				return store.Draft{}, err
			}
			if *text == newlines.Replace(d.Text) {
				text = nil
			}
		}
		cli, err := s.pairedClient()
		if err != nil {
			return store.Draft{}, err
		}
		// The number paired now sends, maybe not the one the draft was made under.
		if _, err := in.db.OutboxChat(ctx, d.Chat, accountOf(cli)); err != nil {
			return store.Draft{}, err
		}
		if err := s.checkReachout(ctx, in); err != nil {
			return store.Draft{}, err
		}
		if text == nil {
			d, err = in.db.ClaimSend(ctx, id, via, s.now())
		} else {
			d, err = in.db.ClaimEditedSend(ctx, id, via, *text, s.now())
		}
		if errors.Is(err, store.ErrExpired) {
			s.sweep(ctx, in, false)
		}
		if err != nil {
			return d, err
		}
		in.wg.Go(func() { s.deliver(in, cli, d) })
		return s.withAvatar(ctx, in, d), nil
	})
}

func (s *Service) DeclineDraft(ctx context.Context, id string) (store.Draft, error) {
	return with(s, func(in *instance) (store.Draft, error) {
		d, err := in.db.Decline(ctx, id, s.now())
		s.sweep(ctx, in, false)
		if err != nil {
			return d, err
		}
		return s.withAvatar(ctx, in, d), nil
	})
}

// A 100 MB upload on a slow link still fits.
const sendTimeout = 10 * time.Minute

// typingFor is how long "typing…" shows before the message: about a person's pace, capped.
func typingFor(text string) time.Duration {
	return min(time.Second+time.Duration(utf8.RuneCountInString(text))*50*time.Millisecond, 6*time.Second)
}

func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var serverCode = regexp.MustCompile(`(\d+)$`)

// errUnconfirmed marks a send cut after the message may have left: the person may have it.
var errUnconfirmed = errors.New("unconfirmed")

// failure is what the draft records: 463 and 475 mean WhatsApp locked messages to new contacts,
// and insisting after them is what gets an account banned.
func failure(err error) (string, int) {
	switch {
	case errors.Is(err, ErrReachoutLocked):
		return "reachout_locked", 0
	case errors.Is(err, errUnconfirmed):
		return "unconfirmed", 0
	case !errors.Is(err, whatsmeow.ErrServerReturnedError):
		return err.Error(), 0
	}
	code := 0
	if m := serverCode.FindStringSubmatch(err.Error()); m != nil {
		code, _ = strconv.Atoi(m[1])
	}
	if code == 463 || code == 475 {
		return "reachout_locked", code
	}
	return err.Error(), code
}

// deliver sends an approved draft once. Whatever happens it ends in sent or failed, never back in
// pending: a failed draft is not sent again.
func (s *Service) deliver(in *instance, cli Client, d store.Draft) {
	ctx, cancel := context.WithTimeout(in.ctx, sendTimeout)
	defer cancel()
	copyDir := s.copyDir(d.ID)
	defer os.RemoveAll(copyDir)
	to, err := types.ParseJID(d.Address)
	var msg *waE2E.Message
	var resp whatsmeow.SendResponse
	if err != nil {
		err = fmt.Errorf("endereço da conversa ilegível: %w", err)
	} else {
		msg, resp, err = s.transmit(ctx, in, cli, d, to, copyDir)
	}
	// The draft's final state is written even when the service is stopping.
	done := context.WithoutCancel(ctx)
	if err != nil {
		reason, code := failure(err)
		s.opt.Log.Error("envio não saiu; sem reenvio", "rascunho", d.ID, "erro", err)
		if reason == "reachout_locked" && code != 0 {
			s.holdReachout(done, in, s.now().Add(24*time.Hour))
		}
		if err := in.db.FinishFailed(done, d.ID, reason, code); err != nil {
			s.opt.Log.Error("gravar falha do envio", "rascunho", d.ID, "erro", err)
		}
		return
	}
	s.opt.Log.Info("mensagem enviada", "rascunho", d.ID, "origem", d.Origin)
	rec, media := s.archived(cli, d, to, msg, resp, copyDir)
	if err := in.db.FinishSent(done, d.ID, rec, media); err != nil {
		s.opt.Log.Error("arquivar envio feito; o rascunho fica só como enviado", "rascunho", d.ID, "mensagem", resp.ID, "erro", err)
		// No message points at the moved copy: it would stay in media/ for good.
		if p, ok := store.MediaAbs(s.opt.Dir, media); media != "" && ok {
			if err := os.Remove(p); err != nil {
				s.opt.Log.Error("apagar anexo enviado que ficou sem mensagem no arquivo", "caminho", p, "erro", err)
			}
		}
		if err := in.db.MarkSent(done, d.ID, resp.ID); err != nil {
			s.opt.Log.Error("gravar envio feito; a mensagem saiu, mas o rascunho vai aparecer como interrompido",
				"rascunho", d.ID, "mensagem", resp.ID, "erro", err)
		}
	}
}

func (s *Service) transmit(ctx context.Context, in *instance, cli Client, d store.Draft, to types.JID, copyDir string) (*waE2E.Message, whatsmeow.SendResponse, error) {
	var resp whatsmeow.SendResponse
	if err := cli.SendChatPresence(ctx, to, types.ChatPresenceComposing, types.ChatPresenceMediaText); err != nil {
		return nil, resp, fmt.Errorf("mostrar digitando: %w", err)
	}
	if err := s.opt.Wait(ctx, typingFor(d.SentText)); err != nil {
		return nil, resp, err
	}
	if err := cli.SendChatPresence(ctx, to, types.ChatPresencePaused, types.ChatPresenceMediaText); err != nil {
		return nil, resp, fmt.Errorf("parar de mostrar digitando: %w", err)
	}
	quote := s.quoteContext(cli, d)
	var msg *waE2E.Message
	switch {
	case d.File != nil:
		doc, err := upload(ctx, cli, copyDir, d.File.Name)
		if err != nil {
			return nil, resp, err
		}
		if d.SentText != "" {
			doc.Caption = proto.String(d.SentText)
		}
		doc.ContextInfo = quote
		msg = &waE2E.Message{DocumentMessage: doc}
	case quote != nil:
		msg = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String(d.SentText), ContextInfo: quote}}
	default:
		msg = &waE2E.Message{Conversation: proto.String(d.SentText)}
	}
	// A lock may have arrived during the typing wait or the upload.
	if err := s.checkReachout(ctx, in); err != nil {
		return nil, resp, err
	}
	resp, err := cli.SendMessage(ctx, to, msg)
	if err != nil && !errors.Is(err, whatsmeow.ErrServerReturnedError) &&
		(errors.Is(err, whatsmeow.ErrMessageTimedOut) || ctx.Err() != nil || lostAck(resp, err)) {
		err = fmt.Errorf("%w: %w", errUnconfirmed, err)
	}
	return msg, resp, err
}

// lostAck is the connection dropping after the message frame was written. whatsmeow times the wait
// for the server's answer only once the frame went, so any error after it (the resend's own socket
// error included) comes from that stage. The error checks hold without the timing: other requests
// fail with the same type, so the action tells them apart, and a bare ErrNotConnected only comes from
// the resend after a reconnect.
func lostAck(resp whatsmeow.SendResponse, err error) bool {
	var d *whatsmeow.DisconnectedError
	return resp.DebugTimings.Resp > 0 || errors.As(err, &d) && strings.HasPrefix(d.Action, "message send") ||
		err == whatsmeow.ErrNotConnected
}

// holdReachout locks sends until the given time unless a later lock is already recorded: a 463 or 475
// comes without an end, and the next approval must not insist.
func (s *Service) holdReachout(ctx context.Context, in *instance, until time.Time) {
	if err := in.db.ExtendReachoutLock(ctx, until.UTC().Format(store.TimeLayout)); err != nil {
		s.opt.Log.Error("gravar trava depois da recusa do WhatsApp", "erro", err)
	}
}

func upload(ctx context.Context, cli Client, dir, name string) (*waE2E.DocumentMessage, error) {
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("abrir a cópia do anexo: %w", err)
	}
	defer f.Close()
	// The encrypted copy goes next to the plain one: with no file whatsmeow uses the system temp
	// folder, often tmpfs, where 100 MB is memory.
	enc, err := os.CreateTemp(dir, ".upload-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(enc.Name())
	defer enc.Close()
	up, err := cli.UploadReader(ctx, f, enc, whatsmeow.MediaDocument)
	if err != nil {
		return nil, fmt.Errorf("subir o anexo: %w", err)
	}
	mimetype, _, _ := strings.Cut(mime.TypeByExtension(filepath.Ext(name)), ";")
	return &waE2E.DocumentMessage{
		URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
		FileSHA256: up.FileSHA256, FileEncSHA256: up.FileEncSHA256, FileLength: proto.Uint64(up.FileLength),
		Mimetype: proto.String(cmp.Or(strings.TrimSpace(mimetype), "application/octet-stream")), FileName: proto.String(name),
	}, nil
}

// quoteContext cites the replied message as WhatsApp clients do: its id, its author and its content.
func (s *Service) quoteContext(cli Client, d store.Draft) *waE2E.ContextInfo {
	if d.ReplyTo == nil {
		return nil
	}
	ctx := &waE2E.ContextInfo{StanzaID: proto.String(*d.ReplyTo)}
	var o original
	if err := json.Unmarshal([]byte(d.QuotedRaw), &o); err != nil {
		s.opt.Log.Warn("mensagem citada sem original legível; a citação vai só com o id", "rascunho", d.ID, "erro", err)
		return ctx
	}
	author, err := types.ParseJID(o.Sender)
	if err != nil || author.IsEmpty() {
		author, _ = types.ParseJID(o.Chat)
		if o.FromMe {
			if own := cli.Device().ID; own != nil {
				author = *own
			}
		}
	}
	if !author.IsEmpty() {
		ctx.Participant = proto.String(author.ToNonAD().String())
	}
	if ctx.Participant == nil {
		s.opt.Log.Warn("autor da mensagem citada desconhecido", "rascunho", d.ID)
	}
	var quoted waE2E.Message
	if err := protojson.Unmarshal(o.Message, &quoted); err != nil {
		s.opt.Log.Warn("conteúdo da mensagem citada ilegível; a citação vai sem ele", "rascunho", d.ID, "erro", err)
	} else {
		ctx.QuotedMessage = &quoted
	}
	return ctx
}

// archived is the sent message as the archive keeps it; the attachment's copy moves into media/
// instead of being downloaded back.
func (s *Service) archived(cli Client, d store.Draft, to types.JID, msg *waE2E.Message, resp whatsmeow.SendResponse, copyDir string) (store.Record, string) {
	info := types.MessageInfo{MessageSource: types.MessageSource{Chat: to, IsFromMe: true, IsGroup: to.Server == types.GroupServer}}
	if own := cli.Device().ID; own != nil {
		info.Sender = *own
	}
	raw, err := rawOf(info, msg)
	if err != nil {
		s.opt.Log.Error("serializar mensagem enviada", "erro", err)
	}
	c := contentOf(msg)
	at := resp.Timestamp
	if at.IsZero() {
		at = s.now()
	}
	rec := store.Record{Chat: d.Chat, ID: resp.ID, Account: accountOf(cli), FromMe: true, At: at, Kind: c.kind, Text: c.text, Mime: c.mime,
		ReplyTo: c.replyTo, Raw: raw}
	if d.File == nil {
		return rec, ""
	}
	rel := store.MediaRel(d.Chat, resp.ID, c.ext)
	dest, ok := store.MediaAbs(s.opt.Dir, rel)
	if !ok {
		s.opt.Log.Warn("caminho do anexo enviado fora de media/; ele será baixado de volta", "rascunho", d.ID, "caminho", rel)
		return rec, ""
	}
	err = os.MkdirAll(filepath.Dir(dest), 0o700)
	if err == nil {
		err = os.Rename(filepath.Join(copyDir, d.File.Name), dest)
	}
	if err != nil {
		s.opt.Log.Warn("guardar anexo enviado; ele será baixado de volta", "erro", err)
		return rec, ""
	}
	return rec, rel
}

// lockReachout records WhatsApp's lock on messages to new contacts; an active lock without an end
// holds for a day.
func (s *Service) lockReachout(in *instance, e *events.NotifyAccountReachoutTimelock) {
	until := ""
	if e.IsActive {
		ends := e.TimeEnforcementEnds.Time
		if ends.IsZero() {
			ends = s.now().Add(24 * time.Hour)
		}
		until = ends.UTC().Format(store.TimeLayout)
		s.opt.Log.Error("o WhatsApp travou mensagens a contatos novos; envios recusados até o fim da trava",
			"até", until, "tipo", e.EnforcementType)
	} else {
		s.opt.Log.Info("o WhatsApp tirou a trava de mensagens a contatos novos")
	}
	var err error
	if until == "" {
		err = in.db.SetReachoutLock(in.ctx, "")
	} else {
		err = in.db.ExtendReachoutLock(in.ctx, until)
	}
	if err != nil {
		s.opt.Log.Error("gravar trava do WhatsApp", "erro", err)
	}
}
