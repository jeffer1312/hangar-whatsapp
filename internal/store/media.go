package store

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	safeSegment  = regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`)
	allowedExtension = map[string]bool{
		".jpg": true, ".jpeg": true, ".png": true, ".webp": true, ".gif": true, ".heic": true,
		".ogg": true, ".opus": true, ".mp3": true, ".m4a": true, ".aac": true, ".wav": true,
		".mp4": true, ".3gp": true, ".mov": true, ".pdf": true, ".txt": true, ".csv": true, ".zip": true,
		".doc": true, ".docx": true, ".xls": true, ".xlsx": true, ".ppt": true, ".pptx": true,
		".odt": true, ".ods": true, ".rtf": true,
	}
)

// MediaRel never leaves media/: the id and the number come from whoever sent the message.
func MediaRel(chat, id, ext string) string {
	safe := func(s string) string {
		if safeSegment.MatchString(s) {
			return s
		}
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:16])
	}
	user, _, _ := strings.Cut(chat, "@")
	if !allowedExtension[ext] {
		ext = ".bin"
	}
	return "media/" + safe(user) + "/" + safe(id) + ext
}

// MediaAbs resolves a stored relative path and refuses anything outside dir/media.
func MediaAbs(dir, rel string) (string, bool) {
	root := filepath.Join(dir, "media")
	abs := filepath.Join(dir, filepath.FromSlash(rel))
	inside, err := filepath.Rel(root, abs)
	if err != nil || inside == "." || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return "", false
	}
	return abs, true
}
