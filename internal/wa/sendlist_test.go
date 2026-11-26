package wa

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// The service writes to WhatsApp in two ways only: the media retry receipt, and a draft the owner
// approved (typing indicator, upload, message), sent only from outbox.go. The delivery receipts
// whatsmeow sends by itself do not go through our code.
var allowedEverywhere = map[string]bool{"SendMediaRetryReceipt": true}

var draftNames = map[string]bool{"SendMessage": true, "SendChatPresence": true, "UploadReader": true,
	"SendResponse": true, "SendRequestExtra": true}

// The sender calls them; the interface and the stand-in only declare them.
const draftSender = "internal/wa/outbox.go"

var draftFiles = map[string]bool{draftSender: true, "internal/wa/client.go": true, "internal/wa/watest/fake.go": true}

// Types the interface and the stand-in name; the methods appear there only where they are declared.
var draftTypes = map[string]bool{"SendResponse": true, "SendRequestExtra": true}

var uploads = map[string]bool{"Upload": true, "UploadReader": true, "UploadNewsletter": true, "UploadNewsletterReader": true}

// Calls that would show the user as active or tell the sender the message was read, in any file.
var forbidden = map[string]bool{"MarkRead": true, "SendPresence": true, "SubscribePresence": true, "SetStatusMessage": true}

func outgoing(root string) (map[string][]string, error) {
	found := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == ".pmedico" || d.Name() == "dist") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		declared := map[*ast.Ident]bool{}
		ast.Inspect(file, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.FuncDecl:
				declared[d.Name] = true
			case *ast.InterfaceType:
				for _, m := range d.Methods.List {
					for _, id := range m.Names {
						declared[id] = true
					}
				}
			}
			return true
		})
		ast.Inspect(file, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			name := id.Name
			// Send, SendMessage, SendPresence…, but not Sender.
			send := strings.HasPrefix(name, "Send") && (len(name) == 4 || unicode.IsUpper(rune(name[4])))
			allowed := allowedEverywhere[name] || (draftNames[name] && (rel == draftSender || draftFiles[rel] && (draftTypes[name] || declared[id])))
			if forbidden[name] || ((send || uploads[name]) && !allowed) {
				found[name] = append(found[name], rel)
			}
			return true
		})
		return nil
	})
	return found, err
}

func TestOutgoingCallsStayInTheClosedList(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	found, err := outgoing(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) > 0 {
		t.Fatalf("outgoing WhatsApp calls outside the closed list: %v", found)
	}
}

func TestClosedListCatchesStrayCalls(t *testing.T) {
	root := t.TempDir()
	put := func(rel, body string) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("package x\n\nfunc f() {\n"+body+"\n}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	put("internal/wa/outbox.go", "c.SendMessage(); c.UploadReader(); c.MarkRead(); c.SendPresence()")
	put("internal/wa/media.go", "c.SendMediaRetryReceipt(); c.SendMessage()")
	put("internal/api/api.go", "c.Upload(); c.SubscribePresence(); c.SendChatPresence()")
	// The interface may declare the method; only outbox.go may call it.
	put("internal/wa/client.go", "(c.Client.SendMessage)(); up := c.UploadReader; up()\n}\n\ntype C interface {\n\tSendMessage() SendResponse\n\tUploadReader()")
	found, err := outgoing(root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"MarkRead": {"internal/wa/outbox.go"}, "SendPresence": {"internal/wa/outbox.go"},
		"SendMessage": {"internal/wa/client.go", "internal/wa/media.go"}, "UploadReader": {"internal/wa/client.go"}, "Upload": {"internal/api/api.go"}, "SubscribePresence": {"internal/api/api.go"},
		"SendChatPresence": {"internal/api/api.go"}}
	if fmt.Sprint(found) != fmt.Sprint(want) {
		t.Fatalf("found = %v\nwant  %v", found, want)
	}
}
