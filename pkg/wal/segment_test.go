package wal

import (
	"bufio"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestSegment(t *testing.T) *segment {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "seg-*.log")
	require.NoError(t, err)
	return &segment{
		id:     1,
		size:   0,
		file:   f,
		writer: bufio.NewWriter(f),
	}
}

func TestSegment_WriteTracksSize(t *testing.T) {
	s := openTestSegment(t)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	data := []byte("hello")
	n, err := s.Write(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, int64(len(data)), s.Size())
}

func TestSegment_MultipleWritesAccumulateSize(t *testing.T) {
	s := openTestSegment(t)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	writes := [][]byte{[]byte("abc"), []byte("defgh"), []byte("ij")}
	total := 0
	for _, w := range writes {
		_, err := s.Write(w)
		require.NoError(t, err)
		total += len(w)
	}
	assert.Equal(t, int64(total), s.Size())
}

func TestSegment_FlushAndReadBack(t *testing.T) {
	s := openTestSegment(t)
	data := []byte("flush test")
	_, err := s.Write(data)
	require.NoError(t, err)
	require.NoError(t, s.Flush())
	name := s.file.Name()
	require.NoError(t, s.Close())

	got, err := os.ReadFile(name)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestSegment_CloseFlushesData(t *testing.T) {
	s := openTestSegment(t)
	name := s.file.Name()
	data := []byte("close flushes")
	_, err := s.Write(data)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	got, err := os.ReadFile(name)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestSegment_ReaderReturnsData(t *testing.T) {
	s := openTestSegment(t)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	data := []byte("reader test data")
	_, err := s.Write(data)
	require.NoError(t, err)
	require.NoError(t, s.Flush())

	r, err := s.Reader()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestSegment_ReaderIndependentOfWriter(t *testing.T) {
	s := openTestSegment(t)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	initialData := []byte("initial")
	_, err := s.Write(initialData)
	require.NoError(t, err)
	require.NoError(t, s.Flush())

	r, err := s.Reader()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	// Write more data but don't flush — reader should not see it
	_, err = s.Write([]byte("extra"))
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, initialData, got)
}
