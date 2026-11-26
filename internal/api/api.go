// Package api serves contract C2 on loopback, every request and response signed with HMAC.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jeffer1312/hangar-whatsapp/internal/control"
	"github.com/jeffer1312/hangar-whatsapp/internal/store"
	"github.com/jeffer1312/hangar-whatsapp/internal/wa"
)

const (
	defaultLimit = 50
	maxLimit     = 200
	maxBody      = 64 << 10
)

type server struct {
	svc *wa.Service
	key []byte
	now func() time.Time
	log *slog.Logger
}

func New(svc *wa.Service, key []byte, now func() time.Time, log *slog.Logger) http.Handler {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &server{svc: svc, key: key, now: now, log: log}
	mux := http.NewServeMux()
	for pattern, h := range map[string]func(http.ResponseWriter, *http.Request) error{
		"GET /v1/status":                   s.status,
		"POST /v1/pair":                    s.pair,
		"POST /v1/logout":                  s.logout,
		"POST /v1/wipe":                    s.wipe,
		"GET /v1/chats":                    s.chats,
		"GET /v1/messages":                 s.messages,
		"GET /v1/search":                   s.search,
		"POST /v1/media/request":           s.mediaRequest,
		"POST /v1/outbox":                  s.createDraft,
		"GET /v1/outbox":                   s.drafts,
		"GET /v1/outbox/{id}":              s.draft,
		"POST /v1/outbox/{id}/send":        s.approveDraft,
		"POST /v1/outbox/{id}/send-edited": s.approveEditedDraft,
		"POST /v1/outbox/{id}/decline":     s.declineDraft,
	} {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if err := h(w, r); err != nil {
				s.fail(w, err)
			}
		})
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "bad_request", "rota desconhecida")
	})
	return s.signed(mux)
}

type buffered struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (b *buffered) Header() http.Header         { return b.header }
func (b *buffered) Write(p []byte) (int, error) { return b.body.Write(p) }
func (b *buffered) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}

// signed checks the request signature and signs every response, errors included, so whoever takes
// over an old port cannot fake an answer. It gives authenticity, not secrecy: that process still
// reads the plain-text query and body.
func (s *server) signed(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		out := &buffered{header: http.Header{}}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		ts := ""
		if err != nil {
			writeError(out, http.StatusRequestEntityTooLarge, "bad_request", "corpo do pedido grande demais")
		} else if ts, err = control.VerifyRequest(s.key, r.Header.Get("X-Hangar-Signature"), r.Method, r.URL.RequestURI(), body, s.now()); err != nil {
			writeError(out, http.StatusUnauthorized, "unauthorized", "assinatura ausente, inválida ou fora do prazo de 30 s")
		} else {
			r.Body = io.NopCloser(bytes.NewReader(body))
			next.ServeHTTP(out, r)
		}
		if out.status == 0 {
			out.status = http.StatusOK
		}
		for k, v := range out.header {
			w.Header()[k] = v
		}
		w.Header().Set("X-Hangar-Signature", control.SignResponse(s.key, ts, out.status, out.body.Bytes()))
		w.WriteHeader(out.status)
		w.Write(out.body.Bytes())
	})
}

type errorBody struct {
	Detail struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	} `json:"detail"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var e errorBody
	e.Detail.Code, e.Detail.Msg = code, msg
	writeJSON(w, status, e)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

type badRequest string

func (b badRequest) Error() string { return string(b) }

func (s *server) fail(w http.ResponseWriter, err error) {
	var bad badRequest
	switch {
	case errors.As(err, &bad):
		writeError(w, http.StatusBadRequest, "bad_request", string(bad))
	case errors.Is(err, wa.ErrBadPhone), errors.Is(err, wa.ErrBadDraft):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
	case errors.Is(err, wa.ErrFileRefused):
		writeError(w, http.StatusBadRequest, "file_refused", err.Error())
	case errors.Is(err, wa.ErrFileTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "file_too_large", err.Error())
	case errors.Is(err, store.ErrUnknownChat):
		writeError(w, http.StatusConflict, "unknown_chat", "a conversa não está no arquivo")
	case errors.Is(err, store.ErrNewContact):
		writeError(w, http.StatusConflict, "new_contact", "esta pessoa nunca escreveu para você; contato novo não recebe envio")
	case errors.Is(err, wa.ErrUnsupportedChat):
		writeError(w, http.StatusConflict, "unsupported_chat", err.Error())
	case errors.Is(err, store.ErrTooManyDrafts):
		writeError(w, http.StatusTooManyRequests, "too_many_drafts",
			fmt.Sprintf("já há rascunhos abertos demais (%d por sessão, %d no total); responda ou espere expirar", store.DraftsPerOrigin, store.DraftsTotal))
	case errors.Is(err, store.ErrDraftNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, store.ErrExpired):
		writeError(w, http.StatusConflict, "expired", err.Error())
	case errors.Is(err, store.ErrAnswered):
		writeError(w, http.StatusConflict, "answered", err.Error())
	case errors.Is(err, wa.ErrReachoutLocked):
		writeError(w, http.StatusConflict, "reachout_locked", err.Error())
	case errors.Is(err, store.ErrSendLimit):
		writeError(w, http.StatusTooManyRequests, "rate_limited",
			fmt.Sprintf("limite de envios atingido (%d por minuto, %d em 24 h)", store.SendsPerMinute, store.SendsPerDay))
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusBadRequest, "bad_request", "before_id não existe nesta conversa")
	case errors.Is(err, wa.ErrAlreadyPaired):
		writeError(w, http.StatusConflict, "bad_request", err.Error())
	case errors.Is(err, wa.ErrNotPaired):
		writeError(w, http.StatusConflict, "not_paired", "nenhum aparelho pareado: pareie em Configurações > WhatsApp")
	case errors.Is(err, wa.ErrOutdated):
		writeError(w, http.StatusConflict, "outdated", "o WhatsApp recusou esta versão: atualize o hangar-whatsapp")
	case errors.Is(err, store.ErrBusy):
		writeError(w, http.StatusServiceUnavailable, "busy", "a consulta passou de 5 s; restrinja a conversa, o período ou o limite")
	case errors.Is(err, store.ErrRateLimited):
		writeError(w, http.StatusTooManyRequests, "rate_limited", "limite de 200 pedidos de mídia em 24 h atingido")
	case errors.Is(err, wa.ErrUnavailable):
		s.log.Error("pedido sem pasta de dados", "erro", err)
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
	default:
		s.log.Error("pedido falhou", "erro", err)
		writeError(w, http.StatusInternalServerError, "internal", "falha interna do hangar-whatsapp; veja o log do serviço")
	}
}

func decode(r *http.Request, v any) error {
	body, _ := io.ReadAll(r.Body)
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, v); err != nil {
		return badRequest("JSON inválido: " + err.Error())
	}
	return nil
}

func limit(r *http.Request) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, badRequest("limit deve ser um inteiro a partir de 1")
	}
	return min(n, maxLimit), nil
}

// chatJID accepts the bare number with country code, as the prototype did.
func chatJID(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", badRequest("chat é obrigatório")
	}
	if !strings.Contains(v, "@") {
		v += "@s.whatsapp.net"
	}
	return v, nil
}

// moment takes ISO 8601 with offset or a bare date; a bare date ends the day when it is an upper bound.
func moment(name, v string, upper bool) (string, error) {
	if v == "" {
		return "", nil
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.UTC().Format(store.TimeLayout), nil
	}
	t, err := time.Parse(time.DateOnly, v)
	if err != nil {
		return "", badRequest(name + " deve ser ISO 8601 (2026-11-22T10:00:00Z) ou uma data (2026-11-22)")
	}
	if upper {
		t = t.Add(24*time.Hour - time.Second)
	}
	return t.Format(store.TimeLayout), nil
}

// Handlers return their error; the adapter in New answers it in the error format.

func (s *server) status(w http.ResponseWriter, r *http.Request) error {
	st, err := s.svc.Status(r.Context())
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, st)
	return nil
}

func (s *server) pair(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Phone string `json:"phone"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	if err := s.svc.Pair(in.Phone); err != nil {
		return err
	}
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) error {
	if err := s.svc.Logout(r.Context()); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *server) wipe(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Confirm bool `json:"confirm"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	if !in.Confirm {
		return badRequest(`apagar exige {"confirm":true}`)
	}
	logout, err := s.svc.Wipe(r.Context())
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]string{"logout": logout})
	return nil
}

func (s *server) chats(w http.ResponseWriter, r *http.Request) error {
	n, err := limit(r)
	if err != nil {
		return err
	}
	chats, err := s.svc.Chats(r.Context(), r.URL.Query().Get("q"), n)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"chats": chats})
	return nil
}

func (s *server) messages(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	in := store.MessagesQuery{BeforeID: q.Get("before_id")}
	var err error
	if in.Chat, err = chatJID(q.Get("chat")); err != nil {
		return err
	}
	if in.Since, err = moment("since", q.Get("since"), false); err != nil {
		return err
	}
	if in.Until, err = moment("until", q.Get("until"), true); err != nil {
		return err
	}
	if in.Limit, err = limit(r); err != nil {
		return err
	}
	page, err := s.svc.Messages(r.Context(), in)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, page)
	return nil
}

func (s *server) search(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	text := strings.TrimSpace(q.Get("q"))
	if text == "" {
		return badRequest("q é obrigatório")
	}
	chat := ""
	if v := q.Get("chat"); v != "" {
		var err error
		if chat, err = chatJID(v); err != nil {
			return err
		}
	}
	n, err := limit(r)
	if err != nil {
		return err
	}
	found, err := s.svc.Search(r.Context(), text, chat, n)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": found})
	return nil
}

func (s *server) mediaRequest(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Chat  string `json:"chat"`
		ID    string `json:"id"`
		Since string `json:"since"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	chat, err := chatJID(in.Chat)
	if err != nil {
		return err
	}
	since, err := moment("since", in.Since, false)
	if err != nil {
		return err
	}
	res, err := s.svc.RequestMedia(r.Context(), chat, in.ID, since)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, res)
	return nil
}

func (s *server) createDraft(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Chat      string `json:"chat"`
		Text      string `json:"text"`
		ReplyTo   string `json:"reply_to"`
		File      string `json:"file"`
		Origin    string `json:"origin"`
		OriginCwd string `json:"origin_cwd"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	chat, err := chatJID(in.Chat)
	if err != nil {
		return err
	}
	d, created, err := s.svc.CreateDraft(r.Context(), wa.DraftRequest{Origin: in.Origin, OriginCwd: in.OriginCwd, Chat: chat,
		Text: in.Text, ReplyTo: in.ReplyTo, File: in.File})
	if err != nil {
		return err
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, d)
	return nil
}

func (s *server) drafts(w http.ResponseWriter, r *http.Request) error {
	list, err := s.svc.Drafts(r.Context(), r.URL.Query().Get("state"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"drafts": list})
	return nil
}

func (s *server) draft(w http.ResponseWriter, r *http.Request) error {
	d, err := s.svc.Draft(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, d)
	return nil
}

func (s *server) approveDraft(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Via string `json:"via"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	d, err := s.svc.ApproveDraft(r.Context(), r.PathValue("id"), in.Via)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusAccepted, d)
	return nil
}

func (s *server) approveEditedDraft(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Via  string  `json:"via"`
		Text *string `json:"text"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	if in.Text == nil {
		return badRequest("text é obrigatório")
	}
	d, err := s.svc.ApproveEditedDraft(r.Context(), r.PathValue("id"), in.Via, *in.Text)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusAccepted, d)
	return nil
}

func (s *server) declineDraft(w http.ResponseWriter, r *http.Request) error {
	d, err := s.svc.DeclineDraft(r.Context(), r.PathValue("id"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, d)
	return nil
}
