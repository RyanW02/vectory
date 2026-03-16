package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMinimalWAL creates a bare WAL struct suitable for testing segmentops methods.
// It does NOT call assignCurrentSegment.
func newMinimalWAL(t *testing.T, dir string) *WAL[*testEntry] {
	t.Helper()
	dirFile, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dirFile.Close()) })
	return &WAL[*testEntry]{
		directory:          dirFile,
		logger:             nopLogger(),
		config:             DefaultConfig(),
		applyFn:            func(*testEntry) error { return nil },
		activeTransactions: make(map[uint64][]*testEntry),
	}
}

func TestListSegments_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	w := newMinimalWAL(t, dir)
	segments, err := w.listSegments()
	require.NoError(t, err)
	assert.Empty(t, segments)
}

func TestOpenSegment_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	w := newMinimalWAL(t, dir)

	s, err := w.openSegment(1, true)
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	assert.Equal(t, 1, s.id)
	assert.Equal(t, int64(0), s.Size())
	_, statErr := os.Stat(filepath.Join(dir, "wal-1.log"))
	assert.NoError(t, statErr)
}

func TestOpenSegment_NonExistentNoCreate(t *testing.T) {
	dir := t.TempDir()
	w := newMinimalWAL(t, dir)

	_, err := w.openSegment(99, false)
	assert.Error(t, err)
}

func TestOpenSegment_ExistingFileTracksSize(t *testing.T) {
	dir := t.TempDir()
	content := []byte("some existing content")
	filePath := filepath.Join(dir, "wal-5.log")
	require.NoError(t, os.WriteFile(filePath, content, 0644))

	w := newMinimalWAL(t, dir)
	s, err := w.openSegment(5, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	assert.Equal(t, 5, s.id)
	assert.Equal(t, int64(len(content)), s.Size())
}

func TestRotate_NilActiveSegment(t *testing.T) {
	dir := t.TempDir()
	w := newMinimalWAL(t, dir)
	// activeSegment is nil
	err := w.rotate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active segment")
}

func TestAssignCurrentSegment_AlreadyActive(t *testing.T) {
	dir := t.TempDir()
	w := newMinimalWAL(t, dir)
	require.NoError(t, w.assignCurrentSegment())
	t.Cleanup(func() { require.NoError(t, w.activeSegment.Close()) })

	err := w.assignCurrentSegment()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot open new segment while another segment is active")
}

func TestAssignCurrentSegment_BootstrapCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w := newMinimalWAL(t, dir)
	require.NoError(t, w.assignCurrentSegment())
	require.NoError(t, w.activeSegment.Close())

	filePath := filepath.Join(dir, "wal-1.log")
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	reader := bytes.NewReader(data)

	rec1, _, err := readNextRecord[*testEntry](reader)
	require.NoError(t, err)
	scr, ok := rec1.(*startCheckpointRecord)
	require.True(t, ok, "first record should be startCheckpointRecord")
	assert.Empty(t, scr.activeTransactions)

	rec2, _, err := readNextRecord[*testEntry](reader)
	require.NoError(t, err)
	_, ok = rec2.(*endCheckpointRecord)
	assert.True(t, ok, "second record should be endCheckpointRecord")
}
