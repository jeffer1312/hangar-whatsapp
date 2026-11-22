package store

import (
	"database/sql"
	_ "embed"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed testdata/prototype_schema.sql
var prototypeSchema string

const expiredPT = "expirada no servidor do WhatsApp; só o celular tem (whatsapp-arquivo pedir-midia)"

// prototype builds a data folder shaped like ~/Projetos/whatsapp-arquivo/dados.
func prototype(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	db, err := OpenSQLite(filepath.Join(src, "mensagens.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(prototypeSchema); err != nil {
		t.Fatal(err)
	}
	chat := "556499990000@s.whatsapp.net"
	// Named arguments: modernc does not carry positional ones across statements of one Exec.
	_, err = db.Exec(`INSERT INTO contatos (jid, nome) VALUES ($chat, 'Maria');
		INSERT INTO mensagens (conversa, id, remetente, de_mim, horario, tipo, texto, midia, midia_mime, midia_erro, responde_a, origem, bruto) VALUES
		($chat, 'T1', $chat, 0, '2025-11-15T11:18:39+00:00', 'texto', 'oi', NULL, NULL, NULL, NULL, 'prochat', NULL),
		($chat, 'I1', $chat, 0, '2025-11-15T11:19:00+00:00', 'imagem', 'foto', 'midia/556499990000/I1.jpg', 'image/jpeg', NULL, 'T1', 'whatsapp',
			'{"chat":"556499990000@s.whatsapp.net","remetente":"556499990000@s.whatsapp.net","de_mim":false,"grupo":false,"mensagem":{"imageMessage":{"mimetype":"image/jpeg"}}}'),
		($chat, 'A1', '', 1, '2025-11-15T11:20:00+00:00', 'audio', '', NULL, 'audio/ogg', $expired, NULL, 'whatsapp', '{}'),
		($chat, 'D1', $chat, 0, '2025-11-15T11:21:00+00:00', 'documento', 'x.pdf', NULL, 'application/pdf', 'indisponível no WhatsApp quando o ProChat importou', NULL, 'prochat', NULL),
		($chat, 'S1', $chat, 0, '2025-11-15T11:22:00+00:00', 'figurinha', '', NULL, 'image/webp', 'o celular não tem mais esta mídia (código 3)', NULL, 'whatsapp', '{}'),
		($chat, 'R1', $chat, 0, '2025-11-15T11:23:00+00:00', 'reacao', '👍', NULL, NULL, NULL, 'T1', 'whatsapp', '{}'),
		($chat, 'V1', $chat, 0, '2025-11-15T11:24:00+00:00', 'video', '', NULL, 'video/mp4', NULL, NULL, 'whatsapp', '{}'),
		($chat, 'O1', $chat, 0, '2025-11-15T11:25:00+00:00', 'outro', 'enquete', NULL, NULL, NULL, NULL, 'whatsapp', '{}');
		INSERT INTO midia_pedida (conversa, id, pedida_em) VALUES ($chat, 'A1', '2025-11-16 08:00:00');`,
		sql.Named("chat", chat), sql.Named("expired", expiredPT))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "midia", "556499990000"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "midia", "556499990000", "I1.jpg"), []byte("jpeg"), 0o644); err != nil {
		t.Fatal(err)
	}
	session, err := OpenSQLite(filepath.Join(src, "sessao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Exec(`CREATE TABLE whatsmeow_device (jid TEXT); INSERT INTO whatsmeow_device VALUES ('556499990000.0:1@s.whatsapp.net')`); err != nil {
		t.Fatal(err)
	}
	return src
}

func TestImportConvertsPrototype(t *testing.T) {
	src := prototype(t)
	dst := t.TempDir()
	if err := Import(src, dst); err != nil {
		t.Fatalf("Import: %v", err)
	}

	db, err := sql.Open("sqlite", "file:"+filepath.Join(dst, "messages.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want := map[string][5]string{
		// id: kind, source, at, media, media_error
		"T1": {"text", "prochat", "2025-11-15T11:18:39Z", "", ""},
		"I1": {"image", "whatsapp", "2025-11-15T11:19:00Z", "media/556499990000/I1.jpg", ""},
		"A1": {"audio", "whatsapp", "2025-11-15T11:20:00Z", "", "expired"},
		"D1": {"document", "prochat", "2025-11-15T11:21:00Z", "", "unavailable"},
		"S1": {"sticker", "whatsapp", "2025-11-15T11:22:00Z", "", "phone_refused"},
		"R1": {"reaction", "whatsapp", "2025-11-15T11:23:00Z", "", ""},
		"V1": {"video", "whatsapp", "2025-11-15T11:24:00Z", "", ""},
		"O1": {"other", "whatsapp", "2025-11-15T11:25:00Z", "", ""},
	}
	rows, err := db.Query(`SELECT id, kind, source, at, coalesce(media, ''), coalesce(media_error, '') FROM messages`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var id string
		var got [5]string
		if err := rows.Scan(&id, &got[0], &got[1], &got[2], &got[3], &got[4]); err != nil {
			t.Fatal(err)
		}
		if got != want[id] {
			t.Errorf("message %s = %v, want %v", id, got, want[id])
		}
		seen++
	}
	if seen != len(want) {
		t.Fatalf("imported %d messages, want %d", seen, len(want))
	}

	var rawChat, rawMime string
	var rawFromMe int
	if err := db.QueryRow(`SELECT json_extract(raw, '$.chat'), json_extract(raw, '$.from_me'),
		json_extract(raw, '$.message.imageMessage.mimetype') FROM messages WHERE id = 'I1'`).Scan(&rawChat, &rawFromMe, &rawMime); err != nil {
		t.Fatalf("raw of I1 not in the new keys: %v", err)
	}
	if rawChat != "556499990000@s.whatsapp.net" || rawFromMe != 0 || rawMime != "image/jpeg" {
		t.Fatalf("raw of I1 = chat %q from_me %d mime %q", rawChat, rawFromMe, rawMime)
	}

	var name, reqChat, reqID, reqAt string
	if err := db.QueryRow(`SELECT name FROM contacts WHERE jid = '556499990000@s.whatsapp.net'`).Scan(&name); err != nil || name != "Maria" {
		t.Fatalf("contact = %q, %v", name, err)
	}
	if err := db.QueryRow(`SELECT chat, id, requested_at FROM media_requests WHERE done_at IS NULL`).Scan(&reqChat, &reqID, &reqAt); err != nil {
		t.Fatalf("open media request: %v", err)
	}
	if reqID != "A1" || reqAt != "2025-11-16T08:00:00Z" {
		t.Fatalf("media request = %s %s %s", reqChat, reqID, reqAt)
	}

	if b, err := os.ReadFile(filepath.Join(dst, "media", "556499990000", "I1.jpg")); err != nil || string(b) != "jpeg" {
		t.Fatalf("media file = %q, %v", b, err)
	}
	session, err := sql.Open("sqlite", "file:"+filepath.Join(dst, "session.db")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	var jid string
	if err := session.QueryRow(`SELECT jid FROM whatsmeow_device`).Scan(&jid); err != nil || !strings.HasPrefix(jid, "556499990000") {
		t.Fatalf("session.db device = %q, %v", jid, err)
	}
	if _, err := os.Stat(filepath.Join(src, "sessao.db")); !os.IsNotExist(err) {
		t.Fatalf("source sessao.db still in place (%v): two copies of the keys could run", err)
	}
	if _, err := os.Stat(filepath.Join(src, "sessao.db.migrated")); err != nil {
		t.Fatalf("sessao.db.migrated: %v", err)
	}
}

func TestImportRefusesDestinationWithData(t *testing.T) {
	for _, name := range []string{"messages.db", "session.db"} {
		t.Run(name, func(t *testing.T) {
			src := prototype(t)
			dst := t.TempDir()
			if err := os.WriteFile(filepath.Join(dst, name), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := Import(src, dst); err == nil {
				t.Fatalf("Import into a folder with %s succeeded", name)
			}
			if _, err := os.Stat(filepath.Join(src, "sessao.db")); err != nil {
				t.Fatalf("refused import touched the source: %v", err)
			}
		})
	}
}
