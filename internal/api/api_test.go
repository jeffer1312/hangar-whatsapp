package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	wastore "go.mau.fi/whatsmeow/store"

	"github.com/jeffer1312/hangar-whatsapp/internal/wa"
	"github.com/jeffer1312/hangar-whatsapp/internal/wa/watest"
)

var key = bytes.Repeat([]byte{9}, 32)
var now = time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

type env struct {
	t       *testing.T
	handler http.Handler
	clients *watest.Factory
}

func newEnv(t *testing.T, paired bool) *env {
	t.Helper()
	e := &env{t: t, clients: watest.NewFactory(paired)}
	svc, err := wa.Open(context.Background(), wa.Options{Dir: t.TempDir(), Version: "0.1.0",
		Now: func() time.Time { return now }, NewClient: func(d *wastore.Device) wa.Client { return e.clients.New(d) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	e.handler = New(svc, key, func() time.Time { return now }, nil)
	return e
}

// sign follows the C2 text, not the code under test.
func sign(k []byte, parts ...string) string {
	m := hmac.New(sha256.New, k)
	m.Write([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(m.Sum(nil))
}

func hash(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

type reply struct {
	status int
	body   []byte
	sig    string
}

func (e *env) raw(method, uri, body, header string) reply {
	e.t.Helper()
	r := httptest.NewRequest(method, uri, strings.NewReader(body))
	if header != "" {
		r.Header.Set("X-Hangar-Signature", header)
	}
	w := httptest.NewRecorder()
	e.handler.ServeHTTP(w, r)
	return reply{status: w.Code, body: w.Body.Bytes(), sig: w.Header().Get("X-Hangar-Signature")}
}

func (e *env) do(method, uri, body string) reply {
	e.t.Helper()
	ts := fmt.Sprint(now.Unix())
	rep := e.raw(method, uri, body, ts+"."+sign(key, method, uri, ts, hash([]byte(body))))
	if want := sign(key, ts, fmt.Sprint(rep.status), hash(rep.body)); rep.sig != want {
		e.t.Fatalf("%s %s: response signature %q, want %q", method, uri, rep.sig, want)
	}
	return rep
}

func (r reply) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("body %q: %v", r.body, err)
	}
	return m
}

func (r reply) code(t *testing.T) string {
	t.Helper()
	d, _ := r.json(t)["detail"].(map[string]any)
	if d == nil || d["msg"] == "" {
		t.Fatalf("error body without detail.code/msg: %s", r.body)
	}
	return fmt.Sprint(d["code"])
}

func TestUnsignedAndBadSignaturesAreRefused(t *testing.T) {
	e := newEnv(t, false)
	if r := e.raw("GET", "/v1/status", "", ""); r.status != 401 || r.code(t) != "unauthorized" {
		t.Fatalf("unsigned = %d %s", r.status, r.body)
	}
	old := fmt.Sprint(now.Add(-time.Minute).Unix())
	r := e.raw("GET", "/v1/status", "", old+"."+sign(key, "GET", "/v1/status", old, hash(nil)))
	if r.status != 401 || r.code(t) != "unauthorized" {
		t.Fatalf("expired = %d %s", r.status, r.body)
	}
	if want := sign(key, old, "401", hash(r.body)); r.sig != want {
		t.Fatalf("401 not signed with the request ts: %q", r.sig)
	}
	ts := fmt.Sprint(now.Unix())
	other := bytes.Repeat([]byte{1}, 32)
	if r := e.raw("GET", "/v1/status", "", ts+"."+sign(other, "GET", "/v1/status", ts, hash(nil))); r.status != 401 {
		t.Fatalf("wrong key = %d", r.status)
	}
}

func TestStatusIsSigned(t *testing.T) {
	e := newEnv(t, false)
	r := e.do("GET", "/v1/status", "")
	m := r.json(t)
	if r.status != 200 || m["api"] != 1.0 || m["state"] != "unpaired" || m["version"] != "0.1.0" {
		t.Fatalf("status = %d %s", r.status, r.body)
	}
	for _, k := range []string{"error", "device", "device_name", "qr", "qr_expires_at", "pair_code", "pair_code_expires_at", "tz", "totals", "since", "last_message_at"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("status without %q: %s", k, r.body)
		}
	}
}

func (e *env) seed(n int) {
	e.t.Helper()
	c := e.clients.Last()
	for i := range n {
		c.Emit(watest.Text(fmt.Sprintf("M%d", i+1), fmt.Sprintf("mensagem %d boleto", i+1), now.Add(time.Duration(i-n)*time.Minute)))
	}
	watest.WaitFor(e.t, "the messages", func() bool {
		return e.do("GET", "/v1/status", "").json(e.t)["totals"].(map[string]any)["messages"] == float64(n)
	})
}

func ids(t *testing.T, r reply) []string {
	t.Helper()
	var out []string
	for _, m := range r.json(t)["messages"].([]any) {
		out = append(out, m.(map[string]any)["id"].(string))
	}
	return out
}

func TestMessagesPagination(t *testing.T) {
	e := newEnv(t, true)
	e.seed(3)
	r := e.do("GET", "/v1/messages?chat=556499990000&limit=2", "")
	if r.status != 200 || fmt.Sprint(ids(t, r)) != "[M2 M3]" || r.json(t)["more"] != true || r.json(t)["name"] != "Maria" {
		t.Fatalf("first page = %d %s", r.status, r.body)
	}
	r = e.do("GET", "/v1/messages?chat=556499990000%40s.whatsapp.net&limit=2&before_id=M2", "")
	if fmt.Sprint(ids(t, r)) != "[M1]" || r.json(t)["more"] != false {
		t.Fatalf("before_id page = %s", r.body)
	}
	item := r.json(t)["messages"].([]any)[0].(map[string]any)
	for _, k := range []string{"id", "at", "from_me", "sender", "sender_name", "kind", "text", "media_path", "media_error", "reply_to"} {
		if _, ok := item[k]; !ok {
			t.Fatalf("message without %q: %v", k, item)
		}
	}
	r = e.do("GET", "/v1/messages?chat=556499990000&since=2026-11-22T11:58:00-00:00", "")
	if fmt.Sprint(ids(t, r)) != "[M2 M3]" {
		t.Fatalf("since = %s", r.body)
	}
	for _, uri := range []string{"/v1/messages", "/v1/messages?chat=556499990000&limit=0", "/v1/messages?chat=556499990000&limit=x",
		"/v1/messages?chat=556499990000&since=ontem", "/v1/messages?chat=556499990000&before_id=NOPE"} {
		if r := e.do("GET", uri, ""); r.status != 400 || r.code(t) != "bad_request" {
			t.Fatalf("%s = %d %s", uri, r.status, r.body)
		}
	}
}

func TestSearchAndChats(t *testing.T) {
	e := newEnv(t, true)
	e.seed(3)
	r := e.do("GET", "/v1/search?q=boleto&limit=2", "")
	if r.status != 200 || fmt.Sprint(ids(t, r)) != "[M3 M2]" {
		t.Fatalf("search = %d %s", r.status, r.body)
	}
	if chat := r.json(t)["messages"].([]any)[0].(map[string]any)["chat"]; chat != "556499990000@s.whatsapp.net" {
		t.Fatalf("search item chat = %v", chat)
	}
	for _, uri := range []string{"/v1/search", "/v1/search?q=boleto&chat=%20"} {
		if r := e.do("GET", uri, ""); r.status != 400 || r.code(t) != "bad_request" {
			t.Fatalf("%s = %d %s", uri, r.status, r.body)
		}
	}
	r = e.do("GET", "/v1/chats?q=mari&limit=500", "")
	chats := r.json(t)["chats"].([]any)
	if r.status != 200 || len(chats) != 1 || chats[0].(map[string]any)["messages"] != 3.0 {
		t.Fatalf("chats = %d %s", r.status, r.body)
	}
}

func TestPairLogoutWipe(t *testing.T) {
	e := newEnv(t, false)
	if r := e.do("POST", "/v1/pair", `{"phone":"abc"}`); r.status != 400 || r.code(t) != "bad_request" {
		t.Fatalf("pair bad phone = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/pair", ""); r.status != 202 {
		t.Fatalf("pair = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/logout", ""); r.status != 409 || r.code(t) != "not_paired" {
		t.Fatalf("logout unpaired = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/wipe", `{}`); r.status != 400 || r.code(t) != "bad_request" {
		t.Fatalf("wipe without confirm = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/wipe", `{"confirm":true}`); r.status != 200 || r.json(t)["logout"] != "not_paired" {
		t.Fatalf("wipe = %d %s", r.status, r.body)
	}
}

func TestMediaRequest(t *testing.T) {
	e := newEnv(t, false)
	if r := e.do("POST", "/v1/media/request", `{"chat":"556499990000"}`); r.status != 409 || r.code(t) != "not_paired" {
		t.Fatalf("unpaired = %d %s", r.status, r.body)
	}
	p := newEnv(t, true)
	if r := p.do("POST", "/v1/media/request", `{}`); r.status != 400 {
		t.Fatalf("without chat = %d %s", r.status, r.body)
	}
	r := p.do("POST", "/v1/media/request", `{"chat":"556499990000","since":"2026-11-01"}`)
	m := r.json(t)
	if r.status != 200 || m["requested"] != 0.0 || m["remaining_today"] != 200.0 || m["capped"] != false {
		t.Fatalf("media/request = %d %s", r.status, r.body)
	}
}
