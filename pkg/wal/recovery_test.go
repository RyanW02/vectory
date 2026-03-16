package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeRawSegment creates a WAL segment file and writes the given records into it.
func writeRawSegment(t *testing.T, dir string, segmentId int, records ...record) {
	t.Helper()
	path := filepath.Join(dir, segmentFileName(segmentId))
	f, err := os.Create(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	for _, r := range records {
		data, err := r.MarshalBinary()
		require.NoError(t, err)
		_, err = f.Write(data)
		require.NoError(t, err)
	}
}

// writeRawBytes appends raw bytes to an existing WAL segment file.
func writeRawBytes(t *testing.T, dir string, segmentId int, data []byte) {
	t.Helper()
	path := filepath.Join(dir, segmentFileName(segmentId))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	_, err = f.Write(data)
	require.NoError(t, err)
}

func TestFindReplayCheckpoint_NoStartCheckpoint(t *testing.T) {
	dir := t.TempDir()
	writeRawSegment(t, dir, 1, newBeginRecord(1))

	w := newMinimalWAL(t, dir)
	_, err := w.findReplayCheckpoint([]int{1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no start checkpoint record found")
}

func TestFindReplayCheckpoint_NoEndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	writeRawSegment(t, dir, 1, newStartCheckpointRecord([]uint64{}))

	w := newMinimalWAL(t, dir)
	_, err := w.findReplayCheckpoint([]int{1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no end checkpoint record found")
}

func TestFindReplayCheckpoint_IncompleteWithNoPrevious(t *testing.T) {
	// Seg 1: complete bootstrap checkpoint (startCheckpoint + endCheckpoint).
	// Seg 2: incomplete checkpoint (startCheckpoint only, no endCheckpoint).
	// The algorithm finds seg2's startCheckpoint as most recent and seg1's endCheckpoint,
	// but the endCheckpoint is not more recent than the startCheckpoint, and there is no
	// previous startCheckpoint to fall back to.
	dir := t.TempDir()
	writeRawSegment(t, dir, 1,
		newStartCheckpointRecord([]uint64{}),
		newEndCheckpointRecord(),
	)
	writeRawSegment(t, dir, 2,
		newStartCheckpointRecord([]uint64{}),
	)

	w := newMinimalWAL(t, dir)
	_, err := w.findReplayCheckpoint([]int{1, 2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no previous start checkpoint found")
}

func TestRecovery_CommitForUnknownTransaction(t *testing.T) {
	dir := t.TempDir()
	// Valid checkpoints so findReplayCheckpoint succeeds,
	// then a commit for a txn that was never started.
	const unknownTxnID = uint64(999)
	writeRawSegment(t, dir, 1,
		newStartCheckpointRecord([]uint64{}),
		newEndCheckpointRecord(),
		newCommitRecord(unknownTxnID),
	)

	w := newMinimalWAL(t, dir)
	err := w.recover()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "was not active")
}

func TestRecovery_RedoForUnknownTransaction(t *testing.T) {
	dir := t.TempDir()
	const unknownTxnID = uint64(888)
	writeRawSegment(t, dir, 1,
		newStartCheckpointRecord([]uint64{}),
		newEndCheckpointRecord(),
		newRedoRecord[*testEntry](unknownTxnID, entry("data")),
	)

	w := newMinimalWAL(t, dir)
	err := w.recover()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "was not active")
}

func TestRecovery_TruncatedRecordMidSegment(t *testing.T) {
	dir := t.TempDir()
	writeRawSegment(t, dir, 1,
		newStartCheckpointRecord([]uint64{}),
		newEndCheckpointRecord(),
	)
	// Append a partial begin record: type byte + only 4 of the required 8 txnId bytes.
	partial := make([]byte, 5)
	partial[0] = byte(recordTypeBegin)
	binary.LittleEndian.PutUint32(partial[1:], 0xDEADBEEF)
	writeRawBytes(t, dir, 1, partial)

	w := newMinimalWAL(t, dir)
	err := w.recover()
	require.Error(t, err)
}
