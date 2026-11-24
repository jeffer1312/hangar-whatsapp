package wa

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"

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

func outboxDir(dir string) string { return filepath.Join(dir, "outbox") }

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
	switch {
	case strings.TrimSpace(req.Origin) == "":
		return store.Draft{}, false, badDraft("origin é obrigatório")
	case utf8.RuneCountInString(req.Text) > maxDraftText:
		return store.Draft{}, false, badDraft(fmt.Sprintf("o texto passa de %d caracteres", maxDraftText))
	case strings.TrimSpace(req.Text) == "" && req.File == "":
		return store.Draft{}, false, badDraft("texto vazio sem anexo")
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
	cli := s.connectedClient()
	if cli == nil {
		return store.Draft{}, false, ErrNotPaired
	}
	if err := s.expire(ctx, in); err != nil {
		return store.Draft{}, false, err
	}
	info, err := in.db.OutboxChat(ctx, jid.String())
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
		d.ChatName, d.ChatNameSource = contactName(ctx, cli.Device(), jid, info.Name)
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
	copyDir := filepath.Join(outboxDir(s.opt.Dir), d.ID)
	if req.File != "" {
		if d.File, err = attach(req.File, req.OriginCwd, s.opt.Dir, copyDir); err != nil {
			return store.Draft{}, false, err
		}
	}
	got, created, err := in.db.CreateDraft(ctx, d, s.now())
	if d.File != nil && (err != nil || !created) {
		os.RemoveAll(copyDir)
	}
	return got, created, err
}

// contactName prefers the phone's address book ("contact") to the name people give themselves
// ("push"); the archive's name has no known origin.
func contactName(ctx context.Context, dev *wastore.Device, jid types.JID, archived string) (string, string) {
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
		if err == nil && !other.IsEmpty() {
			jids = append(jids, other)
		}
	}
	for _, j := range jids {
		c, err := dev.Contacts.GetContact(ctx, j)
		if err != nil || !c.Found {
			continue
		}
		if name := cmp.Or(c.FullName, c.FirstName); name != "" {
			return name, "contact"
		}
		if name := cmp.Or(c.PushName, c.BusinessName); name != "" {
			return name, "push"
		}
	}
	return archived, ""
}

// expire closes overdue drafts and, when any closed, deletes their copies.
func (s *Service) expire(ctx context.Context, in *instance) error {
	n, err := in.db.ExpireDrafts(ctx, s.now())
	if err == nil && n > 0 {
		s.sweep(ctx, in, false)
	}
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
		final := err == nil && d.State != store.OutboxPending && d.State != store.OutboxSending
		if final || (startup && errors.Is(err, store.ErrDraftNotFound)) {
			if err := os.RemoveAll(filepath.Join(outboxDir(s.opt.Dir), e.Name())); err != nil {
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
	if err := s.expire(in.ctx, in); err != nil {
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
		return in.db.Draft(ctx, id)
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
		return in.db.Drafts(ctx, state)
	})
}

func (s *Service) reachoutLocked(ctx context.Context, in *instance) (string, error) {
	until, err := in.db.ReachoutLock(ctx)
	if err != nil || until <= s.now().Format(store.TimeLayout) {
		return "", err
	}
	return until, nil
}

// ApproveDraft approves a pending draft. The send runs after the answer, on the service's context, so
// the request ending does not cut it.
func (s *Service) ApproveDraft(ctx context.Context, id, via string) (store.Draft, error) {
	if via != "desktop" && via != "mobile" {
		return store.Draft{}, badDraft(`via deve ser "desktop" ou "mobile"`)
	}
	return with(s, func(in *instance) (store.Draft, error) {
		if err := s.expire(ctx, in); err != nil {
			return store.Draft{}, err
		}
		d, err := in.db.Draft(ctx, id)
		if err != nil {
			return d, err
		}
		if d.State == store.OutboxExpired {
			return store.Draft{}, store.ErrExpired
		} else if d.State != store.OutboxPending {
			return store.Draft{}, store.ErrAnswered
		}
		cli := s.connectedClient()
		if cli == nil {
			return store.Draft{}, ErrNotPaired
		}
		if until, err := s.reachoutLocked(ctx, in); err != nil {
			return store.Draft{}, err
		} else if until != "" {
			return store.Draft{}, fmt.Errorf("%w até %s", ErrReachoutLocked, until)
		}
		d, err = in.db.ClaimSend(ctx, id, via, s.now())
		if errors.Is(err, store.ErrExpired) {
			s.sweep(ctx, in, false)
		}
		return d, err
	})
}

func (s *Service) DeclineDraft(ctx context.Context, id string) (store.Draft, error) {
	return with(s, func(in *instance) (store.Draft, error) {
		d, err := in.db.Decline(ctx, id, s.now())
		switch {
		case err == nil:
			if err := os.RemoveAll(filepath.Join(outboxDir(s.opt.Dir), id)); err != nil {
				s.opt.Log.Error("apagar cópia do anexo", "rascunho", id, "erro", err)
			}
		case errors.Is(err, store.ErrExpired):
			s.sweep(ctx, in, false)
		}
		return d, err
	})
}
