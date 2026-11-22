package wa

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestContentOf(t *testing.T) {
	cases := []struct {
		name                              string
		msg                               *waE2E.Message
		kind, text, mime, replyTo, ext    string
		hasMedia                          bool
	}{
		{name: "plain text", msg: &waE2E.Message{Conversation: proto.String("oi")}, kind: "text", text: "oi"},
		{
			name: "reply quoting another",
			msg: &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text: proto.String("sim"), ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("ABC")},
			}},
			kind: "text", text: "sim", replyTo: "ABC",
		},
		{
			name: "image with caption",
			msg: &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
				Caption: proto.String("olha"), Mimetype: proto.String("image/jpeg"),
			}},
			kind: "image", text: "olha", mime: "image/jpeg", ext: ".jpg", hasMedia: true,
		},
		{
			name: "voice note",
			msg:  &waE2E.Message{AudioMessage: &waE2E.AudioMessage{Mimetype: proto.String("audio/ogg; codecs=opus")}},
			kind: "audio", mime: "audio/ogg; codecs=opus", ext: ".ogg", hasMedia: true,
		},
		{
			name: "document uses the file name",
			msg: &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
				FileName: proto.String("Alvará.pdf"), Mimetype: proto.String("application/octet-stream"),
			}},
			kind: "document", text: "Alvará.pdf", mime: "application/octet-stream", ext: ".pdf", hasMedia: true,
		},
		{
			name: "sticker",
			msg:  &waE2E.Message{StickerMessage: &waE2E.StickerMessage{Mimetype: proto.String("image/webp")}},
			kind: "sticker", mime: "image/webp", ext: ".webp", hasMedia: true,
		},
		{
			name: "reaction points at the message",
			msg: &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
				Text: proto.String("👍"), Key: &waCommon.MessageKey{ID: proto.String("XYZ")},
			}},
			kind: "reaction", text: "👍", replyTo: "XYZ",
		},
		{
			name: "control message is not saved",
			msg:  &waE2E.Message{SenderKeyDistributionMessage: &waE2E.SenderKeyDistributionMessage{}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := contentOf(c.msg)
			if got.kind != c.kind || got.text != c.text || got.mime != c.mime || got.replyTo != c.replyTo || got.ext != c.ext {
				t.Fatalf("contentOf = kind %q text %q mime %q replyTo %q ext %q", got.kind, got.text, got.mime, got.replyTo, got.ext)
			}
			if (got.media != nil) != c.hasMedia {
				t.Fatalf("media present = %v, want %v", got.media != nil, c.hasMedia)
			}
		})
	}
}
