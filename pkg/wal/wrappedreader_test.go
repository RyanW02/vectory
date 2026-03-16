package wal

import (
	"bufio"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileReader_Read(t *testing.T) {
	content := []byte("hello world")
	f, err := os.CreateTemp(t.TempDir(), "fr-*.bin")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := newFileReader(f, bufio.NewReader(f))
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	got := make([]byte, len(content))
	n, err := io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, len(content), n)
	assert.Equal(t, content, got)
}

func TestFileReader_ReadMultipleChunks(t *testing.T) {
	content := []byte("abcdefghij")
	f, err := os.CreateTemp(t.TempDir(), "fr-*.bin")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := newFileReader(f, bufio.NewReader(f))
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	var got []byte
	buf := make([]byte, 3)
	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	assert.Equal(t, content, got)
}

func TestFileReader_SeekStart(t *testing.T) {
	content := []byte("hello world")
	f, err := os.CreateTemp(t.TempDir(), "fr-*.bin")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := newFileReader(f, bufio.NewReader(f))
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	buf := make([]byte, 5)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)

	pos, err := r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	got := make([]byte, len(content))
	_, err = io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestFileReader_SeekOffset(t *testing.T) {
	content := []byte("abcdefghij")
	f, err := os.CreateTemp(t.TempDir(), "fr-*.bin")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := newFileReader(f, bufio.NewReader(f))
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	pos, err := r.Seek(3, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(3), pos)

	got := make([]byte, 4)
	_, err = io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, content[3:7], got)
}

func TestFileReader_SeekResetsBuffer(t *testing.T) {
	content := make([]byte, 4096+100)
	for i := range content {
		content[i] = byte(i % 256)
	}
	f, err := os.CreateTemp(t.TempDir(), "fr-*.bin")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := newFileReader(f, bufio.NewReader(f))
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	buf := make([]byte, 4096+50)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)

	_, err = r.Seek(10, io.SeekStart)
	require.NoError(t, err)

	got := make([]byte, 5)
	_, err = io.ReadFull(r, got)
	require.NoError(t, err)
	assert.Equal(t, content[10:15], got)
}

func TestFileReader_Close(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fr-*.bin")
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	r := newFileReader(f, bufio.NewReader(f))
	require.NoError(t, r.Close())

	buf := make([]byte, 4)
	_, err = f.Read(buf)
	assert.Error(t, err)
}
