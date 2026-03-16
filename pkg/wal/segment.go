package wal

import (
	"bufio"
	"io"
	"os"
)

type segment struct {
	id     int
	size   int64
	file   *os.File
	writer *bufio.Writer
}

var _ io.WriteCloser = (*segment)(nil)

func (s *segment) Reader() (io.ReadSeekCloser, error) {
	f, err := os.Open(s.file.Name())
	if err != nil {
		return nil, err
	}

	return newFileReader(f, bufio.NewReader(f)), nil
}

func (s *segment) Write(p []byte) (n int, err error) {
	n, err = s.writer.Write(p)
	s.size += int64(n)
	return n, err
}

func (s *segment) Flush() error {
	return s.writer.Flush()
}

func (s *segment) Size() int64 {
	return s.size
}

func (s *segment) Close() error {
	if err := s.writer.Flush(); err != nil {
		return err
	}

	if err := s.file.Close(); err != nil {
		return err
	}

	return nil
}
