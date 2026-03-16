//go:build !linux

package wal

import "os"

// fdatasync falls back to a full fsync on platforms that do not expose
// fdatasync (macOS, Windows, etc.).
func fdatasync(f *os.File) error {
	return f.Sync()
}
