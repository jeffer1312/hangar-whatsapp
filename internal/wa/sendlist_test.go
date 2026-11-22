package wa

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// The archiver never writes to WhatsApp. The only outgoing call it may name is the media retry
// receipt; the delivery and read receipts whatsmeow sends by itself do not go through our code.
var allowedOutgoing = map[string]bool{"SendMediaRetryReceipt": true}

// Calls that would show the user as active or tell the sender the message was read.
var forbidden = map[string]bool{"MarkRead": true, "SubscribePresence": true, "SetStatusMessage": true}

func TestOnlyTheMediaRetryReceiptIsSent(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]string{}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				name := id.Name
				// Send, SendMessage, SendPresence…, but not Sender.
				send := strings.HasPrefix(name, "Send") && (len(name) == 4 || unicode.IsUpper(rune(name[4])))
				if (send && !allowedOutgoing[name]) || forbidden[name] {
					rel, _ := filepath.Rel(root, path)
					found[name] = append(found[name], rel)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) > 0 {
		t.Fatalf("outgoing WhatsApp calls outside the closed list: %v", found)
	}
}
