//go:build windows

package wa

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func systemPath(string) bool { return false }

// A name surrogate (symbolic link, junction) points at another file; other reparse points (OneDrive,
// deduplication) are the file itself.
const reparseTagNameSurrogate = 0x20000000

// openNoFollow opens the last component itself, link or not, and refuses a link; the file is then
// opened for reading and must be that same file, so a swap between the two opens is caught.
func openNoFollow(path string) (*os.File, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	open := func(flags uint32) (windows.Handle, error) {
		h, err := windows.CreateFile(p, windows.GENERIC_READ,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
			flags|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
		if err != nil {
			return 0, &os.PathError{Op: "open", Path: path, Err: err}
		}
		return h, nil
	}
	self, err := open(windows.FILE_FLAG_OPEN_REPARSE_POINT)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(self)
	var tag struct{ FileAttributes, ReparseTag uint32 }
	if err := windows.GetFileInformationByHandleEx(self, windows.FileAttributeTagInfo, (*byte)(unsafe.Pointer(&tag)),
		uint32(unsafe.Sizeof(tag))); err != nil {
		return nil, &os.PathError{Op: "stat", Path: path, Err: err}
	}
	if tag.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 && tag.ReparseTag&reparseTagNameSurrogate != 0 {
		return nil, errSymlink
	}
	h, err := open(0)
	if err != nil {
		return nil, err
	}
	same, err := sameFile(self, h)
	if err != nil || !same {
		windows.CloseHandle(h)
		if err != nil {
			return nil, &os.PathError{Op: "stat", Path: path, Err: err}
		}
		return nil, errSymlink
	}
	return os.NewFile(uintptr(h), path), nil
}

func sameFile(a, b windows.Handle) (bool, error) {
	var x, y windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(a, &x); err != nil {
		return false, err
	}
	if err := windows.GetFileInformationByHandle(b, &y); err != nil {
		return false, err
	}
	return x.VolumeSerialNumber == y.VolumeSerialNumber && x.FileIndexHigh == y.FileIndexHigh &&
		x.FileIndexLow == y.FileIndexLow, nil
}
