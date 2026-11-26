package wa

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image/jpeg"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
)

const (
	avatarTimeout  = 30 * time.Second
	maxAvatarBytes = 24 << 10
	maxAvatarSide  = 640
)

// errAvatarRefused is a picture this service will not keep: it is recorded as none.
var errAvatarRefused = errors.New("foto recusada")

func avatarURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || !strings.HasSuffix(u.Hostname(), ".whatsapp.net") {
		return fmt.Errorf("%w: endereço fora do WhatsApp", errAvatarRefused)
	}
	return nil
}

func newAvatarClient(rt http.RoundTripper) *http.Client {
	return &http.Client{Transport: rt, Timeout: avatarTimeout, CheckRedirect: func(r *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("%w: redirecionamentos demais", errAvatarRefused)
		}
		return avatarURL(r.URL.String())
	}}
}

// download fetches a picture preview from WhatsApp's servers; only a small JPEG is kept.
func (s *Service) download(ctx context.Context, raw string) ([]byte, error) {
	if err := avatarURL(raw); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.avatars.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("baixar foto: HTTP %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxAvatarBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxAvatarBytes {
		return nil, fmt.Errorf("%w: acima de %d bytes", errAvatarRefused, maxAvatarBytes)
	}
	if !bytes.HasPrefix(b, []byte{0xFF, 0xD8, 0xFF}) {
		return nil, fmt.Errorf("%w: não é JPEG", errAvatarRefused)
	}
	cfg, err := jpeg.DecodeConfig(bytes.NewReader(b))
	if err != nil || cfg.Width > maxAvatarSide || cfg.Height > maxAvatarSide {
		return nil, fmt.Errorf("%w: JPEG ilegível ou acima de %d px", errAvatarRefused, maxAvatarSide)
	}
	// The headers alone do not prove the image data is whole.
	if _, err := jpeg.Decode(bytes.NewReader(b)); err != nil {
		return nil, fmt.Errorf("%w: JPEG ilegível: %w", errAvatarRefused, err)
	}
	return b, nil
}

// fetchAvatar refreshes the cached picture of chat in the background, one fetch per chat at a time.
// It runs on the instance's context: a wipe cancels it and waits for it, so it never writes after.
func (s *Service) fetchAvatar(in *instance, cli Client, chat string) {
	account := accountOf(cli)
	key := account + " " + chat
	in.avatarMu.Lock()
	if in.avatarBusy[key] {
		in.avatarMu.Unlock()
		return
	}
	in.avatarBusy[key] = true
	in.avatarMu.Unlock()
	in.wg.Go(func() {
		defer func() {
			in.avatarMu.Lock()
			delete(in.avatarBusy, key)
			in.avatarMu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(in.ctx, avatarTimeout)
		defer cancel()
		if err := s.refreshAvatar(ctx, in, cli, account, chat); err != nil && in.ctx.Err() == nil {
			s.opt.Log.Warn("buscar foto da conversa", "conversa", chat, "erro", err)
		}
	})
}

func (s *Service) refreshAvatar(ctx context.Context, in *instance, cli Client, account, chat string) error {
	if s.connectedClient() != cli {
		return nil
	}
	known, err := in.db.ClaimAvatarFetch(ctx, account, chat, s.now())
	switch {
	case errors.Is(err, store.ErrAvatarFresh):
		return nil
	case errors.Is(err, store.ErrRateLimited):
		return fmt.Errorf("teto de %d buscas de foto em 24 h", store.AvatarFetchesPerDay)
	case err != nil:
		return err
	}
	jid, err := types.ParseJID(chat)
	if err != nil {
		return err
	}
	info, err := cli.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{Preview: true, ExistingID: known})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	a := store.Avatar{State: store.AvatarNone}
	switch {
	case errors.Is(err, whatsmeow.ErrProfilePictureNotSet):
	case errors.Is(err, whatsmeow.ErrProfilePictureUnauthorized):
		a.State = store.AvatarHidden
	case err != nil:
		return err
	case info == nil:
		return in.db.RenewAvatar(ctx, account, chat, s.now())
	default:
		img, err := s.download(ctx, info.URL)
		switch {
		case errors.Is(err, errAvatarRefused):
			s.opt.Log.Warn("foto da conversa recusada", "conversa", chat, "erro", err)
		case err != nil:
			return err
		default:
			a = store.Avatar{State: store.AvatarOK, PictureID: info.ID, Image: img}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return in.db.SaveAvatar(ctx, account, chat, a, s.now())
}

func (s *Service) withAvatar(ctx context.Context, in *instance, d store.Draft) store.Draft {
	list := []store.Draft{d}
	s.withAvatars(ctx, in, list)
	return list[0]
}

// withAvatars puts the cached picture of each chat, as the number paired now sees it, in the drafts,
// reading each chat once. The picture is only for show: failing to read it leaves it null.
func (s *Service) withAvatars(ctx context.Context, in *instance, list []store.Draft) {
	cli, _, _ := s.snapshot()
	if cli == nil {
		return
	}
	account := accountOf(cli)
	read := map[string][]byte{}
	for i := range list {
		chat := list[i].Chat
		img, ok := read[chat]
		if !ok {
			var err error
			if img, err = in.db.ChatAvatar(ctx, account, chat); err != nil {
				s.opt.Log.Error("ler foto da conversa", "conversa", chat, "erro", err)
			}
			read[chat] = img
		}
		list[i].ChatAvatar = img
	}
}
