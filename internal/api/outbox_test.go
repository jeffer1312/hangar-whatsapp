package api

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/jeffer1312/hangar-whatsapp/internal/wa/watest"
)

type svcClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *svcClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *svcClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

func body(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

var draftFields = []string{"id", "origin", "chat", "chat_kind", "chat_number", "chat_name", "chat_name_source", "recent", "text",
	"reply_to", "quoted_text", "file", "created_at", "expires_at", "state", "via", "answered_at", "message_id", "error", "wa_code"}

func TestOutboxCreate(t *testing.T) {
	e := newEnv(t, true)
	e.seed(1)
	r := e.do("POST", "/v1/outbox", `{"chat":"556499990000","text":"oi","origin":"notebook::s1","origin_cwd":"/tmp"}`)
	m := r.json(t)
	if r.status != 201 || m["state"] != "pending" || m["chat"] != "556499990000@s.whatsapp.net" || m["file"] != nil || m["chat_kind"] != "contact" {
		t.Fatalf("create = %d %s", r.status, r.body)
	}
	for _, k := range draftFields {
		if _, ok := m[k]; !ok {
			t.Fatalf("draft without %q: %s", k, r.body)
		}
	}
	again := e.do("POST", "/v1/outbox", `{"chat":"556499990000@s.whatsapp.net","text":"oi","origin":"notebook::s1"}`)
	if again.status != 200 || again.json(t)["id"] != m["id"] {
		t.Fatalf("same draft again = %d %s; want 200 with id %v", again.status, again.body, m["id"])
	}

	work := t.TempDir()
	zip := filepath.Join(work, "a.zip")
	if err := os.WriteFile(zip, []byte("PK"), 0o600); err != nil {
		t.Fatal(err)
	}
	r = e.do("POST", "/v1/outbox", body(map[string]string{"chat": "556499990000", "file": zip, "origin": "notebook::s2", "origin_cwd": work}))
	if f, _ := r.json(t)["file"].(map[string]any); r.status != 201 || f["name"] != "a.zip" || f["size"] != 2.0 || f["outside_cwd"] != false || f["source"] != zip {
		t.Fatalf("create with file = %d %s", r.status, r.body)
	}
	hidden := filepath.Join(work, ".ssh", "id")
	if err := os.MkdirAll(filepath.Dir(hidden), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hidden, []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(work, "grande.bin")
	f, err := os.Create(big)
	if err != nil {
		t.Fatal(err)
	}
	f.Truncate(100<<20 + 1)
	f.Close()
	other := types.NewJID("556488887777", types.DefaultUserServer)
	e.clients.Last().Emit(&events.Message{Info: types.MessageInfo{
		MessageSource: types.MessageSource{Chat: other, Sender: watest.OwnJID, IsFromMe: true}, ID: "OUT1", Timestamp: now,
	}, Message: &waE2E.Message{Conversation: proto.String("fui eu")}})
	watest.WaitFor(t, "the outgoing message", func() bool {
		return e.do("GET", "/v1/status", "").json(t)["totals"].(map[string]any)["messages"] == 2.0
	})

	cases := []struct {
		name, body string
		status     int
		code       string
	}{
		{"bad JSON", `{"chat":`, 400, "bad_request"},
		{"no origin", `{"chat":"556499990000","text":"x"}`, 400, "bad_request"},
		{"no chat", `{"text":"x","origin":"n::s"}`, 400, "bad_request"},
		{"relative file", `{"chat":"556499990000","file":"a.zip","origin":"n::s"}`, 400, "bad_request"},
		{"hidden file", body(map[string]string{"chat": "556499990000", "file": hidden, "origin": "n::s"}), 400, "file_refused"},
		{"too large", body(map[string]string{"chat": "556499990000", "file": big, "origin": "n::s"}), 413, "file_too_large"},
		{"unknown chat", `{"chat":"556411110000","text":"x","origin":"n::s"}`, 409, "unknown_chat"},
		{"new contact", `{"chat":"556488887777","text":"x","origin":"n::s"}`, 409, "new_contact"},
		{"broadcast", `{"chat":"status@broadcast","text":"x","origin":"n::s"}`, 409, "unsupported_chat"},
	}
	for _, c := range cases {
		if r := e.do("POST", "/v1/outbox", c.body); r.status != c.status || r.code(t) != c.code {
			t.Errorf("%s = %d %s; want %d %s", c.name, r.status, r.body, c.status, c.code)
		}
	}
	for i := range 2 {
		if r := e.do("POST", "/v1/outbox", fmt.Sprintf(`{"chat":"556499990000","text":"%d","origin":"notebook::s1"}`, i)); r.status != 201 {
			t.Fatalf("draft %d = %d %s", i, r.status, r.body)
		}
	}
	if r := e.do("POST", "/v1/outbox", `{"chat":"556499990000","text":"quarto","origin":"notebook::s1"}`); r.status != 429 || r.code(t) != "too_many_drafts" {
		t.Fatalf("fourth draft of one origin = %d %s", r.status, r.body)
	}
	p := newEnv(t, false)
	if r := p.do("POST", "/v1/outbox", `{"chat":"556499990000","text":"x","origin":"n::s"}`); r.status != 409 || r.code(t) != "not_paired" {
		t.Fatalf("unpaired = %d %s", r.status, r.body)
	}
}

func TestOutboxAnswer(t *testing.T) {
	clk := &svcClock{at: now}
	e := newEnvAt(t, true, clk.now)
	e.seed(1)
	create := func(text string) string {
		t.Helper()
		r := e.do("POST", "/v1/outbox", body(map[string]string{"chat": "556499990000", "text": text, "origin": "n::" + text}))
		if r.status != 201 {
			t.Fatalf("create %s = %d %s", text, r.status, r.body)
		}
		return r.json(t)["id"].(string)
	}
	a, b := create("a"), create("b")
	r := e.do("GET", "/v1/outbox?state=pending", "")
	if list, _ := r.json(t)["drafts"].([]any); r.status != 200 || len(list) != 2 {
		t.Fatalf("list pending = %d %s", r.status, r.body)
	}
	for _, uri := range []string{"/v1/outbox", "/v1/outbox?state=bogus"} {
		if r := e.do("GET", uri, ""); r.status != 400 || r.code(t) != "bad_request" {
			t.Fatalf("%s = %d %s", uri, r.status, r.body)
		}
	}
	if r := e.do("GET", "/v1/outbox/"+a, ""); r.status != 200 || r.json(t)["id"] != a {
		t.Fatalf("get = %d %s", r.status, r.body)
	}
	if r := e.do("GET", "/v1/outbox/nope", ""); r.status != 404 || r.code(t) != "not_found" {
		t.Fatalf("get unknown = %d %s", r.status, r.body)
	}

	if r := e.do("POST", "/v1/outbox/"+a+"/send", `{"via":"tablet"}`); r.status != 400 || r.code(t) != "bad_request" {
		t.Fatalf("send via tablet = %d %s", r.status, r.body)
	}
	r = e.do("POST", "/v1/outbox/"+a+"/send", `{"via":"desktop"}`)
	if m := r.json(t); r.status != 202 || m["state"] != "sending" || m["via"] != "desktop" {
		t.Fatalf("send = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/outbox/"+a+"/send", `{"via":"mobile"}`); r.status != 409 || r.code(t) != "answered" {
		t.Fatalf("second send = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/outbox/nope/send", `{"via":"mobile"}`); r.status != 404 || r.code(t) != "not_found" {
		t.Fatalf("send unknown = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/outbox/"+b+"/decline", ""); r.status != 200 || r.json(t)["state"] != "declined" {
		t.Fatalf("decline = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/outbox/"+b+"/decline", ""); r.status != 409 || r.code(t) != "answered" {
		t.Fatalf("second decline = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/outbox/nope/decline", ""); r.status != 404 || r.code(t) != "not_found" {
		t.Fatalf("decline unknown = %d %s", r.status, r.body)
	}

	c, d := create("c"), create("d")
	clk.add(10 * time.Minute)
	if r := e.do("POST", "/v1/outbox/"+c+"/send", `{"via":"desktop"}`); r.status != 409 || r.code(t) != "expired" {
		t.Fatalf("send expired = %d %s", r.status, r.body)
	}
	if r := e.do("POST", "/v1/outbox/"+d+"/decline", ""); r.status != 409 || r.code(t) != "expired" {
		t.Fatalf("decline expired = %d %s", r.status, r.body)
	}

	ids := []string{create("e"), create("f"), create("g"), create("h")}
	for _, id := range ids[:3] {
		if r := e.do("POST", "/v1/outbox/"+id+"/send", `{"via":"mobile"}`); r.status != 202 {
			t.Fatalf("send %s = %d %s", id, r.status, r.body)
		}
	}
	if r := e.do("POST", "/v1/outbox/"+ids[3]+"/send", `{"via":"mobile"}`); r.status != 429 || r.code(t) != "rate_limited" {
		t.Fatalf("fourth send in a minute = %d %s", r.status, r.body)
	}
	st := e.do("GET", "/v1/status", "").json(t)
	send, _ := st["send"].(map[string]any)
	if send["last_minute"] != 3.0 || send["today"] != 4.0 || send["per_minute"] != 3.0 || send["per_day"] != 20.0 {
		t.Fatalf("status send = %v", st["send"])
	}
	if v, ok := st["reachout_locked_until"]; !ok || v != nil {
		t.Fatalf("status reachout_locked_until = %v (present %v); want null", v, ok)
	}
}
