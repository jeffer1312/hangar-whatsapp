//go:build windows

package wa

import "os"

// Windows has no O_NOFOLLOW: the link check comes right before the open.
func openNoFollow(path string) (*os.File, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if st.Mode()&os.ModeSymlink != 0 {
		return nil, errSymlink
	}
	return os.Open(path)
}
