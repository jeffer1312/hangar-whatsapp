package wa

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// machineTZ gives an IANA name for the machine's zone; Go only knows "Local".
func machineTZ() string {
	if tz := os.Getenv("TZ"); tz != "" {
		if _, err := time.LoadLocation(strings.TrimPrefix(tz, ":")); err == nil {
			return strings.TrimPrefix(tz, ":")
		}
	}
	if target, err := filepath.EvalSymlinks("/etc/localtime"); err == nil {
		if _, name, ok := strings.Cut(filepath.ToSlash(target), "zoneinfo/"); ok {
			if _, err := time.LoadLocation(name); err == nil {
				return name
			}
		}
	}
	// Windows and machines without the link: the current offset, in the inverted Etc/GMT sign.
	_, offset := time.Now().Zone()
	if offset == 0 || offset%3600 != 0 {
		return "UTC"
	}
	return fmt.Sprintf("Etc/GMT%+d", -offset/3600)
}
