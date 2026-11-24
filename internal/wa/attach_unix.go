//go:build !windows

package wa

import (
	"errors"
	"os"
	"syscall"
)

// /proc/self/environ and the like look like regular files and carry the process's secrets.
func systemPath(path string) bool {
	return within(path, "/proc") || within(path, "/sys") || within(path, "/dev")
}

// openNoFollow refuses a link as the last component; O_NONBLOCK keeps a FIFO from hanging the open
// before the regular-file check.
func openNoFollow(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, syscall.ELOOP) {
		return nil, errSymlink
	}
	return f, err
}
