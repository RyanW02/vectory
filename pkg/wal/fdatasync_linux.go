//go:build linux

package wal

import (
	"os"
	"syscall"

	"github.com/RyanW02/vectory/pkg/safemath"
)

// fdatasync flushes file data to stable storage without flushing metadata.
// This is faster than fsync for append-only workloads because the kernel
// can skip the directory entry / inode metadata write.
func fdatasync(f *os.File) error {
	fd, err := safemath.UintptrToInt(f.Fd())
	if err != nil {
		return err
	}

	return syscall.Fdatasync(fd)
}
