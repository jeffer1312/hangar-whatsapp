package wa

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/jeffer1312/hangar-whatsapp/internal/store"
	"github.com/jeffer1312/hangar-whatsapp/internal/wa/watest"
)

// cdn stands for WhatsApp's picture server: no socket, the files are in memory.
type cdn struct {
	mu        sync.Mutex
	files     map[string][]byte
	redirects map[string]string
	hits      []string
}

func (c *cdn) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hits = append(c.hits, r.URL.String())
	if to, ok := c.redirects[r.URL.String()]; ok {
		return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {to}}, Body: http.NoBody, Request: r}, nil
	}
	status, b := http.StatusOK, c.files[r.URL.String()]
	if b == nil {
		status = http.StatusNotFound
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b)), Request: r}, nil
}

func (c *cdn) put(url string, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files[url] = b
}

func (c *cdn) Hits() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.hits...)
}

type avatarHarness struct {
	*outboxHarness
	cdn *cdn
}

func newAvatarHarness(t *testing.T) *avatarHarness {
	t.Helper()
	clk := &clock{now: time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)}
	c := &cdn{files: map[string][]byte{}, redirects: map[string]string{}}
	h := &harness{t: t, dir: t.TempDir(), clients: watest.NewFactory(true)}
	svc, err := Open(context.Background(), Options{Dir: h.dir, Version: "0.3.0", Now: clk.Now,
		NewClient: func(d *wastore.Device, l waLog.Logger) Client { return h.clients.New(d, l) },
		Wait:      clk.wait, AvatarTransport: c})
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	t.Cleanup(func() { h.svc.Close() })
	a := &avatarHarness{outboxHarness: &outboxHarness{harness: h, clock: clk}, cdn: c}
	a.receive("M1", "olá")
	return a
}

func (h *avatarHarness) idle() bool {
	h.svc.rw.RLock()
	defer h.svc.rw.RUnlock()
	in := h.svc.in
	in.avatarMu.Lock()
	defer in.avatarMu.Unlock()
	return len(in.avatarBusy) == 0
}

// draftFor makes a new draft for chat and waits for the picture fetch it starts.
func (h *avatarHarness) draftFor(chat, text string) store.Draft {
	h.t.Helper()
	d := h.mustDraft(DraftRequest{Chat: chat, Text: text, Origin: "notebook::" + text})
	watest.WaitFor(h.t, "the picture fetch", h.idle)
	return d
}

func (h *avatarHarness) avatar(chat string) (store.Avatar, error) {
	return h.svc.in.db.Avatar(context.Background(), watest.OwnJID.User, chat)
}

func (h *avatarHarness) chatAvatar(id string) []byte {
	h.t.Helper()
	d, err := h.svc.Draft(context.Background(), id)
	if err != nil {
		h.t.Fatal(err)
	}
	return d.ChatAvatar
}

func jpegOf(t *testing.T, w, h int) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := jpeg.Encode(&b, image.NewGray(image.Rect(0, 0, w, h)), nil); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// cutScan is a JPEG whose headers are whole but whose image data stops halfway.
func cutScan(t *testing.T) []byte {
	t.Helper()
	img := image.NewGray(image.Rect(0, 0, 64, 64))
	for i := range img.Pix {
		img.Pix[i] = byte(i * 37)
	}
	var b bytes.Buffer
	if err := jpeg.Encode(&b, img, nil); err != nil {
		t.Fatal(err)
	}
	sos := bytes.Index(b.Bytes(), []byte{0xFF, 0xDA})
	if sos < 0 || sos+40 >= b.Len() {
		t.Fatalf("JPEG without enough image data: %d bytes, scan at %d", b.Len(), sos)
	}
	return b.Bytes()[:sos+40]
}

func picture(id, url string) watest.PictureAnswer {
	return func(ctx context.Context, jid types.JID, p whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		return &types.ProfilePictureInfo{ID: id, URL: url, Type: "preview"}, nil
	}
}

const picURL = "https://pps.whatsapp.net/v/t61/p1.jpg"

func TestDraftFetchesTheChatPicture(t *testing.T) {
	h := newAvatarHarness(t)
	img := jpegOf(t, 96, 96)
	h.cdn.put(picURL, img)
	h.client().AnswerPicture(picture("P1", picURL))

	d := h.draftFor(chatA, "um")
	if got := h.chatAvatar(d.ID); !bytes.Equal(got, img) {
		t.Fatalf("chat_avatar after the fetch = %d bytes, want the %d of the picture", len(got), len(img))
	}
	q := h.client().PictureQueries()
	if len(q) != 1 || q[0].JID.String() != chatA || !q[0].Params.Preview || q[0].Params.ExistingID != "" {
		t.Fatalf("queries = %+v; want one preview query for %s with no known id", q, chatA)
	}
	again := h.draftFor(chatA, "dois")
	if !bytes.Equal(again.ChatAvatar, img) {
		t.Fatalf("draft made with a cached picture = %d bytes, want the picture", len(again.ChatAvatar))
	}
	if q := h.client().PictureQueries(); len(q) != 1 {
		t.Fatalf("queries within 24 h = %d, want 1", len(q))
	}

	group := types.NewJID("120363000000000001", types.GroupServer)
	h.emit(&events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: group, Sender: types.NewJID("556499990000", types.DefaultUserServer), IsGroup: true},
		ID:            "G1", Timestamp: h.clock.Now(),
	}, Message: &waE2E.Message{Conversation: proto.String("oi grupo")}})
	h.draftFor(group.String(), "grupo")
	if q := h.client().PictureQueries(); len(q) != 2 || q[1].JID != group {
		t.Fatalf("group query = %+v; want the group's JID", q)
	}
}

func TestChatPictureIsAskedAgainAfterADay(t *testing.T) {
	h := newAvatarHarness(t)
	first := jpegOf(t, 96, 96)
	h.cdn.put(picURL, first)
	h.client().AnswerPicture(picture("P1", picURL))
	h.draftFor(chatA, "um")

	// Unchanged: WhatsApp answers nothing for the known id, and nothing is downloaded.
	h.clock.Add(25 * time.Hour)
	h.client().AnswerPicture(func(ctx context.Context, jid types.JID, p whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		return nil, nil
	})
	d := h.draftFor(chatA, "dois")
	if q := h.client().PictureQueries(); len(q) != 2 || q[1].Params.ExistingID != "P1" {
		t.Fatalf("queries = %+v; want the second with ExistingID P1", q)
	}
	a, err := h.avatar(chatA)
	if err != nil || a.FetchedAt != "2026-11-25T13:00:00Z" || a.PictureID != "P1" || !bytes.Equal(h.chatAvatar(d.ID), first) {
		t.Fatalf("unchanged picture = %+v, %v; want P1 kept and fetched_at renewed", a, err)
	}
	if hits := h.cdn.Hits(); len(hits) != 1 {
		t.Fatalf("downloads = %v; want only the first", hits)
	}

	h.clock.Add(25 * time.Hour)
	second := jpegOf(t, 64, 64)
	url2 := "https://mmg.whatsapp.net/v/t61/p2.jpg"
	h.cdn.put(url2, second)
	h.client().AnswerPicture(picture("P2", url2))
	d = h.draftFor(chatA, "três")
	if got := h.chatAvatar(d.ID); !bytes.Equal(got, second) {
		t.Fatalf("changed picture = %d bytes, want the new one (%d)", len(got), len(second))
	}
	if a, _ := h.avatar(chatA); a.PictureID != "P2" {
		t.Fatalf("picture id = %q, want P2", a.PictureID)
	}
}

func TestChatPictureNotSetOrHiddenHoldsADay(t *testing.T) {
	for _, c := range []struct {
		err   error
		state string
	}{{whatsmeow.ErrProfilePictureNotSet, store.AvatarNone}, {whatsmeow.ErrProfilePictureUnauthorized, store.AvatarHidden}} {
		t.Run(c.state, func(t *testing.T) {
			h := newAvatarHarness(t)
			h.client().AnswerPicture(func(ctx context.Context, jid types.JID, p whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
				return nil, c.err
			})
			d := h.draftFor(chatA, "um")
			if a, err := h.avatar(chatA); err != nil || a.State != c.state || a.Image != nil {
				t.Fatalf("avatar = %+v, %v; want %s without image", a, err, c.state)
			}
			if got := h.chatAvatar(d.ID); got != nil {
				t.Fatalf("chat_avatar = %v, want null", got)
			}
			h.clock.Add(23 * time.Hour)
			h.draftFor(chatA, "dois")
			if q := h.client().PictureQueries(); len(q) != 1 {
				t.Fatalf("queries within 24 h = %d, want 1", len(q))
			}
			h.clock.Add(2 * time.Hour)
			h.draftFor(chatA, "três")
			if q := h.client().PictureQueries(); len(q) != 2 || q[1].Params.ExistingID != "" {
				t.Fatalf("queries after 24 h = %+v; want a second one with no known id", q)
			}
		})
	}
}

func TestChatPictureErrorWritesNothing(t *testing.T) {
	h := newAvatarHarness(t)
	h.client().AnswerPicture(func(ctx context.Context, jid types.JID, p whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		return nil, errors.New("rede caiu")
	})
	h.draftFor(chatA, "um")
	if a, err := h.avatar(chatA); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("avatar after an error = %+v, %v; want nothing stored", a, err)
	}
	h.cdn.put(picURL, jpegOf(t, 8, 8))
	h.client().AnswerPicture(picture("P1", picURL))
	h.draftFor(chatA, "dois")
	if a, err := h.avatar(chatA); err != nil || a.State != store.AvatarOK {
		t.Fatalf("next draft after an error = %+v, %v; want the picture fetched", a, err)
	}
}

func TestChatPictureDownloadIsChecked(t *testing.T) {
	small := jpegOf(t, 8, 8)
	png := []byte("\x89PNG\r\n\x1a\n" + strings.Repeat("x", 100))
	for _, c := range []struct {
		name, url string
		body      []byte
		hits      int
	}{
		{"other host", "https://pps.whatsapp.net.evil.example/p.jpg", small, 0},
		{"plain http", "http://pps.whatsapp.net/p.jpg", small, 0},
		{"above 24 KB", picURL, append(append([]byte(nil), small...), make([]byte, 24<<10)...), 1},
		{"not a JPEG", picURL, png, 1},
		{"above 640 px", picURL, jpegOf(t, 641, 8), 1},
		{"cut in the image data", picURL, cutScan(t), 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newAvatarHarness(t)
			h.cdn.put(c.url, c.body)
			h.client().AnswerPicture(picture("P1", c.url))
			d := h.draftFor(chatA, "um")
			if a, err := h.avatar(chatA); err != nil || a.State != store.AvatarNone || a.Image != nil {
				t.Fatalf("avatar = %+v, %v; want none without image", a, err)
			}
			if got := h.chatAvatar(d.ID); got != nil {
				t.Fatalf("chat_avatar = %d bytes, want null", len(got))
			}
			if hits := h.cdn.Hits(); len(hits) != c.hits {
				t.Fatalf("downloads = %v, want %d", hits, c.hits)
			}
		})
	}
	h := newAvatarHarness(t)
	h.cdn.put(picURL, jpegOf(t, 640, 640))
	h.client().AnswerPicture(picture("P1", picURL))
	h.draftFor(chatA, "um")
	if a, err := h.avatar(chatA); err != nil || a.State != store.AvatarOK {
		t.Fatalf("640×640 picture = %+v, %v; want ok", a, err)
	}
}

func TestWipeCancelsThePictureFetch(t *testing.T) {
	h := newAvatarHarness(t)
	h.cdn.put(picURL, jpegOf(t, 8, 8))
	asked := make(chan struct{})
	h.client().AnswerPicture(func(ctx context.Context, jid types.JID, p whatsmeow.GetProfilePictureParams) (*types.ProfilePictureInfo, error) {
		close(asked)
		<-ctx.Done()
		return &types.ProfilePictureInfo{ID: "P1", URL: picURL}, nil
	})
	h.mustDraft(DraftRequest{Chat: chatA, Text: "um"})
	select {
	case <-asked:
	case <-time.After(5 * time.Second):
		t.Fatal("the draft did not ask for the picture")
	}
	done := make(chan error, 1)
	go func() {
		_, err := h.svc.Wipe(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wipe waited for a picture fetch that was not cancelled")
	}
	if a, err := h.avatar(chatA); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("avatar after the wipe = %+v, %v; want nothing", a, err)
	}
	if hits := h.cdn.Hits(); len(hits) != 0 {
		t.Fatalf("downloads after the cancel = %v, want none", hits)
	}
}

func TestChatPictureRedirectsStayOnWhatsApp(t *testing.T) {
	h := newAvatarHarness(t)
	evil := "https://evil.example/p.jpg"
	h.cdn.put(evil, jpegOf(t, 8, 8))
	h.cdn.mu.Lock()
	h.cdn.redirects[picURL] = evil
	h.cdn.mu.Unlock()
	h.client().AnswerPicture(picture("P1", picURL))
	h.draftFor(chatA, "um")
	if a, err := h.avatar(chatA); err != nil || a.State != store.AvatarNone {
		t.Fatalf("redirect off WhatsApp = %+v, %v; want none", a, err)
	}
	if hits := h.cdn.Hits(); len(hits) != 1 || hits[0] != picURL {
		t.Fatalf("downloads = %v; want only the WhatsApp address", hits)
	}

	l := newAvatarHarness(t)
	loop := "https://mmg.whatsapp.net/loop"
	l.cdn.mu.Lock()
	l.cdn.redirects[loop] = loop
	l.cdn.mu.Unlock()
	l.client().AnswerPicture(picture("P1", loop))
	l.draftFor(chatA, "um")
	if a, err := l.avatar(chatA); err != nil || a.State != store.AvatarNone {
		t.Fatalf("redirect loop = %+v, %v; want none", a, err)
	}
	if hits := l.cdn.Hits(); len(hits) != 10 {
		t.Fatalf("redirect loop downloads = %d, want 10", len(hits))
	}
}
