package wal

import (
	"bufio"
	"io"
	"os"
)

type fileReader struct {
	f      *os.File
	reader *bufio.Reader
}

func (r *fileReader) Seek(offset int64, whence int) (int64, error) {
	defer r.reader.Reset(r.f)
	return r.f.Seek(offset, whence)
}

var _ io.ReadSeekCloser = (*fileReader)(nil)

func newFileReader(f *os.File, reader *bufio.Reader) *fileReader {
	return &fileReader{
		f:      f,
		reader: reader,
	}
}

func (r *fileReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *fileReader) Close() error {
	return r.f.Close()
}
