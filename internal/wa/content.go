package wa

import (
	"cmp"
	"fmt"
	"mime"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
)

type content struct {
	kind, text, mime, replyTo, ext string
	media                          whatsmeow.DownloadableMessage
}

// The system mime.TypeByExtension gives odd extensions (.jfif, .oga) for the common types.
var extensions = map[string]string{
	"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif",
	"audio/ogg": ".ogg", "audio/mpeg": ".mp3", "audio/mp4": ".m4a", "audio/aac": ".aac",
	"video/mp4": ".mp4", "video/3gpp": ".3gp", "application/pdf": ".pdf",
}

// contentOf returns an empty kind for what is not conversation (control, edit, group key).
func contentOf(m *waE2E.Message) content {
	switch {
	case m.GetConversation() != "":
		return content{kind: "text", text: m.GetConversation()}
	case m.GetExtendedTextMessage() != nil:
		e := m.GetExtendedTextMessage()
		return content{kind: "text", text: e.GetText(), replyTo: e.GetContextInfo().GetStanzaID()}
	case m.GetImageMessage() != nil:
		e := m.GetImageMessage()
		return withMedia("image", e.GetCaption(), e.GetMimetype(), "", e.GetContextInfo(), e)
	case m.GetVideoMessage() != nil:
		e := m.GetVideoMessage()
		return withMedia("video", e.GetCaption(), e.GetMimetype(), "", e.GetContextInfo(), e)
	case m.GetPtvMessage() != nil:
		e := m.GetPtvMessage()
		return withMedia("video", e.GetCaption(), e.GetMimetype(), "", e.GetContextInfo(), e)
	case m.GetAudioMessage() != nil:
		e := m.GetAudioMessage()
		return withMedia("audio", "", e.GetMimetype(), "", e.GetContextInfo(), e)
	case m.GetDocumentMessage() != nil:
		e := m.GetDocumentMessage()
		text := cmp.Or(e.GetCaption(), e.GetFileName(), e.GetTitle())
		return withMedia("document", text, e.GetMimetype(), e.GetFileName(), e.GetContextInfo(), e)
	case m.GetStickerMessage() != nil:
		e := m.GetStickerMessage()
		return withMedia("sticker", "", e.GetMimetype(), "", e.GetContextInfo(), e)
	case m.GetReactionMessage() != nil:
		e := m.GetReactionMessage()
		return content{kind: "reaction", text: e.GetText(), replyTo: e.GetKey().GetID()}
	case m.GetLocationMessage() != nil:
		e := m.GetLocationMessage()
		return content{kind: "other", text: strings.TrimSpace(fmt.Sprintf("localização %f,%f %s %s",
			e.GetDegreesLatitude(), e.GetDegreesLongitude(), e.GetName(), e.GetAddress()))}
	case m.GetContactMessage() != nil:
		e := m.GetContactMessage()
		return content{kind: "other", text: "contato: " + e.GetDisplayName() + "\n" + e.GetVcard()}
	case m.GetPollCreationMessageV3() != nil || m.GetPollCreationMessage() != nil:
		e := cmp.Or(m.GetPollCreationMessageV3(), m.GetPollCreationMessage())
		options := make([]string, 0, len(e.GetOptions()))
		for _, o := range e.GetOptions() {
			options = append(options, o.GetOptionName())
		}
		return content{kind: "other", text: "enquete: " + e.GetName() + " (" + strings.Join(options, " / ") + ")"}
	}
	return content{}
}

func withMedia(kind, text, mimetype, file string, ctx *waE2E.ContextInfo, media whatsmeow.DownloadableMessage) content {
	return content{kind: kind, text: text, mime: mimetype, replyTo: ctx.GetStanzaID(), ext: extension(mimetype, file), media: media}
}

func extension(mimetype, file string) string {
	if ext := filepath.Ext(file); ext != "" && len(ext) <= 6 {
		return strings.ToLower(ext)
	}
	base, _, _ := strings.Cut(mimetype, ";")
	base = strings.TrimSpace(base)
	if ext, ok := extensions[base]; ok {
		return ext
	}
	if exts, _ := mime.ExtensionsByType(base); len(exts) > 0 {
		return exts[0]
	}
	return ".bin"
}
