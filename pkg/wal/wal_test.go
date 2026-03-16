package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===================== Test Infrastructure =====================

// testEntry is a minimal Data implementation for tests.
type testEntry struct {
	Value []byte
}

func (e *testEntry) MarshalBinary() ([]byte, error) {
	result := make([]byte, len(e.Value))
	copy(result, e.Value)
	return result, nil
}

func (e *testEntry) UnmarshalBinary(data []byte) error {
	e.Value = make([]byte, len(data))
	copy(e.Value, data)
	return nil
}

func (e *testEntry) New() Data {
	return new(testEntry)
}

var _ Data = (*testEntry)(nil)

func entry(s string) *testEntry {
	return &testEntry{Value: []byte(s)}
}

func collectApplyFn(applied *[]*testEntry) func(*testEntry) error {
	return func(e *testEntry) error {
		*applied = append(*applied, e)
		return nil
	}
}

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// openAndRecover opens a WAL in dir, sets a recording applyFn, calls Recover(), and returns the WAL and applied slice.
func openAndRecover(t *testing.T, dir string, opts ...Option) (*WAL[*testEntry], *[]*testEntry) {
	t.Helper()
	w, err := Open[*testEntry](dir, NewConfig(opts...))
	require.NoError(t, err)
	applied := make([]*testEntry, 0)
	w.SetApplyFn(collectApplyFn(&applied))
	require.NoError(t, w.Recover())
	return w, &applied
}

// openFresh opens a WAL in a fresh temp dir, calls Recover(), and returns WAL, applied slice, and dir path.
func openFresh(t *testing.T, opts ...Option) (*WAL[*testEntry], *[]*testEntry, string) {
	t.Helper()
	dir := t.TempDir()
	w, applied := openAndRecover(t, dir, opts...)
	return w, applied, dir
}

// ===================== Config Tests =====================

func TestConfig_Default(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, int64(16*1024*1024), cfg.MaxSegmentBytes)
}

func TestConfig_WithMaxSegmentBytes(t *testing.T) {
	cases := []struct {
		name  string
		bytes int64
	}{
		{"1KB", 1024},
		{"1MB", 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig(WithMaxSegmentBytes(tc.bytes))
			assert.Equal(t, tc.bytes, cfg.MaxSegmentBytes)
		})
	}
}

func TestConfig_NewConfigNoOptions(t *testing.T) {
	cfg := NewConfig()
	assert.Equal(t, DefaultConfig().MaxSegmentBytes, cfg.MaxSegmentBytes)
}

// ===================== Open / Lifecycle Tests =====================

func TestOpen_CreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new-wal-dir")
	w, err := Open[*testEntry](dir, DefaultConfig())
	require.NoError(t, err)

	_, err = os.Stat(dir)
	assert.NoError(t, err, "directory should be created")
	_, err = os.Stat(filepath.Join(dir, "wal-1.log"))
	assert.NoError(t, err, "wal-1.log should be created")

	w.SetApplyFn(collectApplyFn(new([]*testEntry)))
	assert.NoError(t, w.Recover())
}

func TestOpen_ExistingEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	w, err := Open[*testEntry](dir, DefaultConfig())
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "wal-1.log"))
	assert.NoError(t, err)

	w.SetApplyFn(collectApplyFn(new([]*testEntry)))
	assert.NoError(t, w.Recover())
}

func TestOpen_ReopensExistingWAL(t *testing.T) {
	_, _, dir := openFresh(t)

	w2, err := Open[*testEntry](dir, DefaultConfig())
	require.NoError(t, err)
	w2.SetApplyFn(collectApplyFn(new([]*testEntry)))
	assert.NoError(t, w2.Recover())
}

// ===================== Pre-Recovery Guard Tests =====================

func TestBegin_FailsBeforeRecover(t *testing.T) {
	dir := t.TempDir()
	w, err := Open[*testEntry](dir, DefaultConfig())
	require.NoError(t, err)

	_, err = w.Begin()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot begin transaction before recovery")
}

func TestBegin_SucceedsAfterRecover(t *testing.T) {
	w, _, _ := openFresh(t)
	_, err := w.Begin()
	assert.NoError(t, err)
}

// ===================== Basic Transaction Tests =====================

func TestTransaction_BeginCommit(t *testing.T) {
	w, applied, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("hello")))
	require.NoError(t, w.Commit(txId))

	require.Len(t, *applied, 1)
	assert.Equal(t, "hello", string((*applied)[0].Value))
}

func TestTransaction_BeginWriteMultipleCommit(t *testing.T) {
	w, applied, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("a")))
	require.NoError(t, w.Write(txId, entry("b")))
	require.NoError(t, w.Write(txId, entry("c")))
	require.NoError(t, w.Commit(txId))

	require.Len(t, *applied, 3)
	assert.Equal(t, "a", string((*applied)[0].Value))
	assert.Equal(t, "b", string((*applied)[1].Value))
	assert.Equal(t, "c", string((*applied)[2].Value))
}

func TestTransaction_BeginAbort(t *testing.T) {
	w, applied, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("should-not-apply")))
	require.NoError(t, w.Abort(txId))

	assert.Empty(t, *applied)
	assert.Error(t, w.Write(txId, entry("extra")), "txn ID should be invalidated after abort")
}

func TestTransaction_MultipleIndependent(t *testing.T) {
	w, applied, _ := openFresh(t)

	txA, err := w.Begin()
	require.NoError(t, err)
	txB, err := w.Begin()
	require.NoError(t, err)

	require.NoError(t, w.Write(txA, entry("a")))
	require.NoError(t, w.Write(txB, entry("b")))
	require.NoError(t, w.Commit(txA))
	require.NoError(t, w.Commit(txB))

	require.Len(t, *applied, 2)
	assert.Equal(t, "a", string((*applied)[0].Value))
	assert.Equal(t, "b", string((*applied)[1].Value))
}

func TestTransaction_InterleavedOperations(t *testing.T) {
	w, applied, _ := openFresh(t)

	txA, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txA, entry("a1")))

	txB, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txB, entry("b1")))

	require.NoError(t, w.Write(txA, entry("a2")))
	// B commits first
	require.NoError(t, w.Commit(txB))
	require.NoError(t, w.Write(txA, entry("a3")))
	require.NoError(t, w.Commit(txA))

	require.Len(t, *applied, 4)
	assert.Equal(t, "b1", string((*applied)[0].Value))
	assert.Equal(t, "a1", string((*applied)[1].Value))
	assert.Equal(t, "a2", string((*applied)[2].Value))
	assert.Equal(t, "a3", string((*applied)[3].Value))
}

func TestTransaction_CommitNonExistent(t *testing.T) {
	w, _, _ := openFresh(t)
	err := w.Commit(9999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestTransaction_AbortNonExistent(t *testing.T) {
	w, _, _ := openFresh(t)
	err := w.Abort(9999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestTransaction_WriteNonExistent(t *testing.T) {
	w, _, _ := openFresh(t)
	err := w.Write(9999, entry("x"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestTransaction_DoubleCommit(t *testing.T) {
	w, _, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Commit(txId))
	assert.Error(t, w.Commit(txId))
}

func TestTransaction_DoubleAbort(t *testing.T) {
	w, _, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Abort(txId))
	assert.Error(t, w.Abort(txId))
}

func TestTransaction_CommitAfterAbort(t *testing.T) {
	w, _, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Abort(txId))
	assert.Error(t, w.Commit(txId))
}

func TestTransaction_MonotonicIds(t *testing.T) {
	w, _, _ := openFresh(t)

	var ids [5]uint64
	for i := range ids {
		id, err := w.Begin()
		require.NoError(t, err)
		ids[i] = id
	}

	for i := 1; i < len(ids); i++ {
		assert.Greater(t, ids[i], ids[i-1])
	}
}

// ===================== Transaction ID Continuity Tests =====================

func TestTransactionId_ContinuesAfterRestart(t *testing.T) {
	// Commit transactions without an explicit checkpoint so all begin records remain in the
	// replay range.  Recovery must observe them and set nextTransactionId past the highest
	// assigned ID so new IDs never collide with old ones.
	w, _, dir := openFresh(t)

	var prevMaxId uint64
	for i := 0; i < 3; i++ {
		id, err := w.Begin()
		require.NoError(t, err)
		require.NoError(t, w.Commit(id))
		if id > prevMaxId {
			prevMaxId = id
		}
	}

	w2, _ := openAndRecover(t, dir)
	newId, err := w2.Begin()
	require.NoError(t, err)
	assert.Greater(t, newId, prevMaxId, "first transaction ID after restart must exceed highest ID from previous session")
	require.NoError(t, w2.Commit(newId))
}

func TestTransactionId_ContinuesAfterCrash(t *testing.T) {
	// Simulate a crash: commit one transaction, start a second but never commit it.
	// Recovery must observe the begin record for the uncommitted transaction and set
	// nextTransactionId past it, so the abandoned ID is not reissued.
	w, _, dir := openFresh(t)

	id1, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(id1, entry("committed")))
	require.NoError(t, w.Commit(id1))

	id2, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(id2, entry("uncommitted")))
	_ = w // abandon — simulate crash

	w2, _ := openAndRecover(t, dir)
	newId, err := w2.Begin()
	require.NoError(t, err)
	assert.Greater(t, newId, id2, "ID after crash recovery must exceed the highest ID seen before the crash")
	require.NoError(t, w2.Commit(newId))
}

func TestTransactionId_MonotonicAcrossMultipleRestarts(t *testing.T) {
	// Open, commit a few transactions, close, repeat.  IDs must never go backwards.
	dir := t.TempDir()

	var highWatermark uint64
	for session := 0; session < 4; session++ {
		w, _ := openAndRecover(t, dir)
		for i := 0; i < 3; i++ {
			id, err := w.Begin()
			require.NoError(t, err)
			require.NoError(t, w.Commit(id))
			assert.Greater(t, id, highWatermark,
				"session %d, txn %d: ID %d must exceed high-watermark %d", session, i, id, highWatermark)
			highWatermark = id
		}
	}
}

func TestTransactionId_NoCollisionAcrossRestarts(t *testing.T) {
	// Record every ID used in the first session; verify none of them are reissued in the second.
	w, _, dir := openFresh(t)

	seen := make(map[uint64]struct{})
	for i := 0; i < 5; i++ {
		id, err := w.Begin()
		require.NoError(t, err)
		seen[id] = struct{}{}
		require.NoError(t, w.Commit(id))
	}

	w2, _ := openAndRecover(t, dir)
	for i := 0; i < 5; i++ {
		id, err := w2.Begin()
		require.NoError(t, err)
		_, collision := seen[id]
		assert.False(t, collision, "new session must not reuse transaction ID %d from previous session", id)
		seen[id] = struct{}{}
		require.NoError(t, w2.Commit(id))
	}
}

func TestTransactionId_ContinuesAfterRestartWithCheckpointAndActiveTxn(t *testing.T) {
	// A transaction that is active at checkpoint time has its ID recorded in the
	// startCheckpointRecord.  Recovery must seed maxTransactionId from those IDs so
	// nextTransactionId continues past the highest active-at-checkpoint ID.
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("data")))
	require.NoError(t, w.Checkpoint())
	require.NoError(t, w.Write(txId, entry("more-data")))
	require.NoError(t, w.Commit(txId)) // also writes endCheckpoint

	w2, _ := openAndRecover(t, dir)
	newId, err := w2.Begin()
	require.NoError(t, err)
	assert.Greater(t, newId, txId, "ID after restart must exceed ID of transaction active at checkpoint time")
	require.NoError(t, w2.Commit(newId))
}

func TestTransactionId_CleanCheckpointResetsIdBug(t *testing.T) {
	// Known bug: when all transactions are committed *before* a clean checkpoint
	// (checkpoint with no active transactions), their IDs are not recorded in the
	// startCheckpointRecord and their beginRecords are outside the post-checkpoint
	// replay range.  Recovery therefore sets nextTransactionId = 1 regardless of
	// how many IDs were used before the checkpoint, allowing those IDs to be reissued
	// in the next session.
	//
	// When this bug is fixed, change the assertion to:
	//   assert.Greater(t, newId, txId, "...")
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Commit(txId))
	require.NoError(t, w.Checkpoint()) // clean checkpoint: no active transactions

	w2, _ := openAndRecover(t, dir)
	newId, err := w2.Begin()
	require.NoError(t, err)
	// Bug: nextTransactionId was reset to 1, so newId <= txId.
	assert.LessOrEqual(t, newId, txId, "known bug: clean checkpoint causes ID counter to reset, reissuing previously used IDs")
	require.NoError(t, w2.Commit(newId))
}

// ===================== WithTransaction Tests =====================

func TestWithTransaction_Success(t *testing.T) {
	w, applied, _ := openFresh(t)

	err := w.WithTransaction(func(tw *TransactionWriter[*testEntry]) error {
		require.NoError(t, tw.Write(entry("x")))
		require.NoError(t, tw.Write(entry("y")))
		return nil
	})
	require.NoError(t, err)

	require.Len(t, *applied, 2)
	assert.Equal(t, "x", string((*applied)[0].Value))
	assert.Equal(t, "y", string((*applied)[1].Value))
}

func TestWithTransaction_ErrorInCallback(t *testing.T) {
	w, applied, _ := openFresh(t)

	callbackErr := fmt.Errorf("callback error")
	err := w.WithTransaction(func(tw *TransactionWriter[*testEntry]) error {
		require.NoError(t, tw.Write(entry("should-not-apply")))
		return callbackErr
	})

	assert.ErrorIs(t, err, callbackErr)
	assert.Empty(t, *applied)
}

// ===================== ApplyFn Error Tests =====================

func TestCommit_ApplyFnError(t *testing.T) {
	w, _, _ := openFresh(t)

	applyErr := fmt.Errorf("apply error")
	callCount := 0
	w.SetApplyFn(func(e *testEntry) error {
		callCount++
		if callCount >= 2 {
			return applyErr
		}
		return nil
	})

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("a")))
	require.NoError(t, w.Write(txId, entry("b")))

	err = w.Commit(txId)
	assert.Error(t, err)
	assert.ErrorIs(t, err, applyErr)
}

// ===================== Record Serialization Tests =====================

func TestRecord_BeginMarshalRoundTrip(t *testing.T) {
	r := newBeginRecord(42)
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)

	br, ok := rec.(*beginRecord)
	require.True(t, ok, "expected *beginRecord")
	assert.Equal(t, uint64(42), br.TransactionId())
}

func TestRecord_RedoMarshalRoundTrip(t *testing.T) {
	r := newRedoRecord[*testEntry](7, entry("round-trip"))
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)

	rr, ok := rec.(*redoRecord[*testEntry])
	require.True(t, ok, "expected *redoRecord")
	assert.Equal(t, uint64(7), rr.TransactionId())
	assert.Equal(t, "round-trip", string(rr.Data.Value))
}

func TestRecord_CommitMarshalRoundTrip(t *testing.T) {
	r := newCommitRecord(99)
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)

	cr, ok := rec.(*commitRecord)
	require.True(t, ok, "expected *commitRecord")
	assert.Equal(t, uint64(99), cr.TransactionId())
}

func TestRecord_AbortMarshalBug(t *testing.T) {
	// Known bug: abortRecord.MarshalBinary() writes recordTypeCommit (byte 2) instead of
	// recordTypeAbort (byte 3). Abort records are therefore indistinguishable from commit
	// records on disk, causing aborted transactions to be replayed as commits during recovery.
	//
	// When this bug is fixed, update the assertions below to:
	//   assert.Equal(t, byte(recordTypeAbort), data[0])
	//   assert.IsType(t, &abortRecord{}, rec)
	r := newAbortRecord(55)
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	// Bug: type byte is recordTypeCommit (2), not recordTypeAbort (3).
	assert.Equal(t, byte(recordTypeCommit), data[0], "known bug: abort record is serialized with commit type byte")
	assert.NotEqual(t, byte(recordTypeAbort), data[0])

	// Reading back gives a commitRecord, not an abortRecord.
	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)
	_, isCommit := rec.(*commitRecord)
	assert.True(t, isCommit, "known bug: abort record reads back as a commit record")
}

func TestRecord_StartCheckpointMarshalRoundTrip(t *testing.T) {
	r := newStartCheckpointRecord([]uint64{1, 5, 99})
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)

	scr, ok := rec.(*startCheckpointRecord)
	require.True(t, ok, "expected *startCheckpointRecord")
	assert.Equal(t, []uint64{1, 5, 99}, scr.activeTransactions)
}

func TestRecord_EndCheckpointMarshalRoundTrip(t *testing.T) {
	r := newEndCheckpointRecord()
	data, err := r.MarshalBinary()
	require.NoError(t, err)
	assert.Len(t, data, 1)

	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)
	_, ok := rec.(*endCheckpointRecord)
	assert.True(t, ok, "expected *endCheckpointRecord")
}

func TestRecord_InvalidRecordType(t *testing.T) {
	for _, b := range []byte{6, 100, 255} {
		t.Run(fmt.Sprintf("byte_%d", b), func(t *testing.T) {
			_, _, err := readNextRecord[*testEntry](bytes.NewReader([]byte{b}))
			assert.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidRecordType))
		})
	}
}

func TestInitialiseRecord_ValidTypes(t *testing.T) {
	cases := []struct {
		rt       recordType
		expected interface{}
	}{
		{recordTypeBegin, &beginRecord{}},
		{recordTypeRedo, &redoRecord[*testEntry]{}},
		{recordTypeCommit, &commitRecord{}},
		{recordTypeAbort, &abortRecord{}},
		{recordTypeStartCheckpoint, &startCheckpointRecord{}},
		{recordTypeEndCheckpoint, &endCheckpointRecord{}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("type_%d", tc.rt), func(t *testing.T) {
			rec, err := initialiseRecord[*testEntry](tc.rt)
			require.NoError(t, err)
			assert.IsType(t, tc.expected, rec)
		})
	}
}

func TestInitialiseRecord_InvalidTypes(t *testing.T) {
	for _, rt := range []recordType{6, 100, 255} {
		t.Run(fmt.Sprintf("type_%d", rt), func(t *testing.T) {
			_, err := initialiseRecord[*testEntry](rt)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidRecordType))
		})
	}
}

// ===================== Recovery Tests =====================

func TestRecovery_Empty(t *testing.T) {
	_, applied, _ := openFresh(t)
	assert.Empty(t, *applied)
}

func TestRecovery_CommittedTransactionReplayed(t *testing.T) {
	// Commit a transaction without a subsequent checkpoint so recovery must replay it.
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("hello")))
	require.NoError(t, w.Write(txId, entry("world")))
	require.NoError(t, w.Commit(txId))

	_, applied := openAndRecover(t, dir)
	require.Len(t, *applied, 2)
	assert.Equal(t, "hello", string((*applied)[0].Value))
	assert.Equal(t, "world", string((*applied)[1].Value))
}

func TestRecovery_CommittedBeforeCheckpoint_NotReplayed(t *testing.T) {
	// Transactions committed before a checkpoint should not be replayed on recovery,
	// since their data has already been applied to the store.
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("pre-checkpoint")))
	require.NoError(t, w.Commit(txId))
	require.NoError(t, w.Checkpoint())

	_, applied := openAndRecover(t, dir)
	assert.Empty(t, *applied)
}

func TestRecovery_UncommittedTransactionNotReplayed(t *testing.T) {
	// A transaction started but never committed (simulating a crash) should not be replayed.
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("uncommitted")))
	// No Commit or Abort - simulate crash by abandoning w.
	_ = w

	_, applied := openAndRecover(t, dir)
	assert.Empty(t, *applied)
}

func TestRecovery_AbortedTransactionBug(t *testing.T) {
	// Known bug: abortRecord.MarshalBinary() serializes using recordTypeCommit instead of
	// recordTypeAbort. On recovery the abort record is read as a commit, causing the aborted
	// transaction's data to be applied.
	//
	// When this bug is fixed, update the assertion to: assert.Empty(t, *applied)
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("aborted-data")))
	require.NoError(t, w.Abort(txId))

	_, applied := openAndRecover(t, dir)
	// Bug: the aborted transaction is replayed as committed during recovery.
	assert.Len(t, *applied, 1, "known bug: aborted transaction replayed due to abort record serialization bug")
}

func TestRecovery_MultipleCommittedTransactions(t *testing.T) {
	w, _, dir := openFresh(t)

	for _, val := range []string{"first", "second", "third"} {
		txId, err := w.Begin()
		require.NoError(t, err)
		require.NoError(t, w.Write(txId, entry(val)))
		require.NoError(t, w.Commit(txId))
	}

	_, applied := openAndRecover(t, dir)
	require.Len(t, *applied, 3)
	assert.Equal(t, "first", string((*applied)[0].Value))
	assert.Equal(t, "second", string((*applied)[1].Value))
	assert.Equal(t, "third", string((*applied)[2].Value))
}

func TestRecovery_CheckpointWithActiveTxns(t *testing.T) {
	// Begin a txn, checkpoint with it active, write more to it, then commit.
	// Only redo records written after the checkpoint offset are replayed on recovery.
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("before-checkpoint")))
	require.NoError(t, w.Checkpoint())
	require.NoError(t, w.Write(txId, entry("after-checkpoint")))
	require.NoError(t, w.Commit(txId)) // also triggers endCheckpoint

	_, applied := openAndRecover(t, dir)
	// "before-checkpoint" was already applied in the first session and is before the checkpoint offset.
	// "after-checkpoint" is after the checkpoint offset and must be replayed.
	require.Len(t, *applied, 1)
	assert.Equal(t, "after-checkpoint", string((*applied)[0].Value))
}

func TestRecovery_IncompleteCheckpointFallback(t *testing.T) {
	// When the most recent checkpoint is incomplete (startCheckpoint written but endCheckpoint never written),
	// recovery falls back to the previous complete checkpoint.
	w, _, dir := openFresh(t)

	// Commit a txn before the second checkpoint (will NOT be replayed after fallback).
	txC, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txC, entry("before-second-checkpoint")))
	require.NoError(t, w.Commit(txC))

	// Second complete checkpoint.
	require.NoError(t, w.Checkpoint())

	// Commit a txn between the second checkpoint and the incomplete one (WILL be replayed).
	txD, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txD, entry("between-checkpoints")))
	require.NoError(t, w.Commit(txD))

	// Begin a txn but don't commit; checkpoint with it active so endCheckpoint is never written.
	txB, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txB, entry("never-committed")))
	require.NoError(t, w.Checkpoint()) // writes startCheckpoint([txB]); endCheckpoint never written
	// Simulate crash by abandoning w.

	_, applied := openAndRecover(t, dir)
	// Falls back to the second complete checkpoint:
	//   txC (committed before second checkpoint): NOT replayed
	//   txD (committed after second checkpoint, in replay range): IS replayed
	//   txB (uncommitted): NOT applied
	require.Len(t, *applied, 1)
	assert.Equal(t, "between-checkpoints", string((*applied)[0].Value))
}

func TestRecovery_ApplyFnError(t *testing.T) {
	w, _, dir := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, entry("data")))
	require.NoError(t, w.Commit(txId))

	w2, err := Open[*testEntry](dir, DefaultConfig())
	require.NoError(t, err)
	applyErr := fmt.Errorf("apply error")
	w2.SetApplyFn(func(*testEntry) error { return applyErr })

	err = w2.Recover()
	assert.Error(t, err)
	assert.ErrorIs(t, err, applyErr)
}

func TestRecovery_AcrossSegments(t *testing.T) {
	// Use a small segment size to force multiple segment rotations, then verify that
	// recovery correctly replays committed transactions spanning multiple segment files.
	const maxSegBytes int64 = 50
	w, _, dir := openFresh(t, WithMaxSegmentBytes(maxSegBytes))

	var committed []string
	for i := 0; i < 5; i++ {
		val := fmt.Sprintf("entry-%d", i)
		txId, err := w.Begin()
		require.NoError(t, err)
		require.NoError(t, w.Write(txId, entry(val)))
		require.NoError(t, w.Commit(txId))
		committed = append(committed, val)
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Greater(t, len(entries), 1, "expected rotation to produce multiple segment files")

	_, applied := openAndRecover(t, dir, WithMaxSegmentBytes(maxSegBytes))
	require.Len(t, *applied, len(committed))
	for i, val := range committed {
		assert.Equal(t, val, string((*applied)[i].Value))
	}
}

// ===================== Checkpoint Tests =====================

func TestCheckpoint_NoActiveTransactions(t *testing.T) {
	w, _, dir := openFresh(t)
	require.NoError(t, w.Checkpoint())

	_, applied := openAndRecover(t, dir)
	assert.Empty(t, *applied)
}

func TestCheckpoint_WithActiveTransactions_CompletedByCommit(t *testing.T) {
	w, _, dir := openFresh(t)

	txA, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txA, entry("before-checkpoint")))
	require.NoError(t, w.Checkpoint())

	require.NoError(t, w.Write(txA, entry("after-checkpoint")))
	// Commit writes commitRecord then endCheckpoint (via checkpointEnd).
	require.NoError(t, w.Commit(txA))

	_, applied := openAndRecover(t, dir)
	// Only redo records after the checkpoint offset are in the replay range.
	require.Len(t, *applied, 1)
	assert.Equal(t, "after-checkpoint", string((*applied)[0].Value))
}

func TestCheckpoint_WithActiveTransactions_CompletedByAbort(t *testing.T) {
	w, _, dir := openFresh(t)

	txA, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txA, entry("should-abort")))
	require.NoError(t, w.Checkpoint())

	// Abort writes an abort record (serialized as commit due to the known bug) then endCheckpoint.
	require.NoError(t, w.Abort(txA))

	_, applied := openAndRecover(t, dir)
	// The redo record was written before the checkpoint offset, so it is not in the replay range.
	// Recovery initializes activeTransactions[txA]=[] from the checkpoint record,
	// then encounters the "commit" (the misencoded abort) and applies zero buffered records.
	assert.Empty(t, *applied)
}

func TestCheckpoint_MultipleActiveTransactions(t *testing.T) {
	w, _, dir := openFresh(t)

	txA, err := w.Begin()
	require.NoError(t, err)
	txB, err := w.Begin()
	require.NoError(t, err)

	require.NoError(t, w.Write(txA, entry("a-before")))
	require.NoError(t, w.Write(txB, entry("b-before")))
	require.NoError(t, w.Checkpoint())

	require.NoError(t, w.Write(txA, entry("a-after")))
	require.NoError(t, w.Write(txB, entry("b-after")))
	// Commit A: endCheckpoint not written yet (B still active).
	require.NoError(t, w.Commit(txA))
	// Commit B: endCheckpoint now written (last active txn from checkpoint completes).
	require.NoError(t, w.Commit(txB))

	_, applied := openAndRecover(t, dir)
	// Only post-checkpoint redo records are replayed; A commits before B.
	require.Len(t, *applied, 2)
	assert.Equal(t, "a-after", string((*applied)[0].Value))
	assert.Equal(t, "b-after", string((*applied)[1].Value))
}

// ===================== Segment Tests =====================

func TestSegment_RotationOccursAtThreshold(t *testing.T) {
	// Record sizes with MaxSegmentBytes=50:
	//   Bootstrap: startCheckpoint(9) + endCheckpoint(1) = 10 bytes in wal-1.log
	//   Txn 1: begin(9) + redo("hello"=18) + commit(9) = 36 bytes → total 46 ≤ 50, stays in wal-1.log
	//   Txn 2: begin(9) → 46+9=55 > 50 → rotate to wal-2.log
	const maxSegBytes int64 = 50
	w, _, dir := openFresh(t, WithMaxSegmentBytes(maxSegBytes))

	for i := 0; i < 2; i++ {
		txId, err := w.Begin()
		require.NoError(t, err)
		require.NoError(t, w.Write(txId, entry("hello")))
		require.NoError(t, w.Commit(txId))
	}

	_, err := os.Stat(filepath.Join(dir, "wal-2.log"))
	assert.NoError(t, err, "wal-2.log should exist after segment rotation")
}

func TestSegment_MultipleRotations(t *testing.T) {
	const maxSegBytes int64 = 50
	w, _, dir := openFresh(t, WithMaxSegmentBytes(maxSegBytes))

	for i := 0; i < 6; i++ {
		txId, err := w.Begin()
		require.NoError(t, err)
		require.NoError(t, w.Write(txId, entry("hello")))
		require.NoError(t, w.Commit(txId))
	}

	_, err := os.Stat(filepath.Join(dir, "wal-3.log"))
	assert.NoError(t, err, "wal-3.log should exist after multiple rotations")
}

func TestSegment_FileName(t *testing.T) {
	cases := []struct {
		id       int
		expected string
	}{
		{1, "wal-1.log"},
		{42, "wal-42.log"},
		{100, "wal-100.log"},
	}
	for _, tc := range cases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, segmentFileName(tc.id))
		})
	}
}

func TestSegment_ListSegments_SortedNumerically(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"wal-10.log", "wal-1.log", "wal-3.log"} {
		f, err := os.Create(filepath.Join(dir, name))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	dirFile, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dirFile.Close()) })

	w := &WAL[*testEntry]{directory: dirFile, logger: nopLogger()}
	segments, err := w.listSegments()
	require.NoError(t, err)
	assert.Equal(t, []int{1, 3, 10}, segments)
}

func TestSegment_ListSegments_IgnoresNonSegmentFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"wal-1.log", "wal-2.log", "readme.txt", ".gitkeep", "other.log"} {
		f, err := os.Create(filepath.Join(dir, name))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	dirFile, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dirFile.Close()) })

	w := &WAL[*testEntry]{directory: dirFile, logger: nopLogger()}
	segments, err := w.listSegments()
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, segments)
}

// ===================== Concurrency Tests =====================

func TestConcurrency_ParallelTransactions(t *testing.T) {
	// Run with: go test -race ./pkg/wal/
	w, _, _ := openFresh(t)

	var totalApplied atomic.Int64
	w.SetApplyFn(func(*testEntry) error {
		totalApplied.Add(1)
		return nil
	})

	const goroutines = 10
	const recordsPerGoroutine = 5

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := w.WithTransaction(func(tw *TransactionWriter[*testEntry]) error {
				for j := 0; j < recordsPerGoroutine; j++ {
					if err := tw.Write(entry(fmt.Sprintf("g%d-r%d", i, j))); err != nil {
						return err
					}
				}
				return nil
			})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(goroutines*recordsPerGoroutine), totalApplied.Load())
}

func TestConcurrency_CheckpointDuringTransactions(t *testing.T) {
	// Verify no deadlock or panic when Checkpoint races with concurrent transactions.
	// Run with: go test -race ./pkg/wal/
	w, _, _ := openFresh(t)

	var totalApplied atomic.Int64
	w.SetApplyFn(func(*testEntry) error {
		totalApplied.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := w.WithTransaction(func(tw *TransactionWriter[*testEntry]) error {
				return tw.Write(entry(fmt.Sprintf("entry-%d", i)))
			})
			assert.NoError(t, err)
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = w.Checkpoint()
	}()

	wg.Wait()
	assert.Equal(t, int64(5), totalApplied.Load())
}

// ===================== Edge Case Tests =====================

func TestEdge_EmptyTransaction(t *testing.T) {
	w, applied, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Commit(txId))

	assert.Empty(t, *applied)
}

func TestEdge_EmptyTransactionAbort(t *testing.T) {
	w, _, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)
	assert.NoError(t, w.Abort(txId))
}

func TestEdge_LargeData(t *testing.T) {
	w, applied, _ := openFresh(t)

	large := make([]byte, 1*1024*1024)
	for i := range large {
		large[i] = byte(i % 256)
	}

	txId, err := w.Begin()
	require.NoError(t, err)
	require.NoError(t, w.Write(txId, &testEntry{Value: large}))
	require.NoError(t, w.Commit(txId))

	require.Len(t, *applied, 1)
	assert.Equal(t, large, (*applied)[0].Value)
}

func TestEdge_ManyTransactions(t *testing.T) {
	w, applied, _ := openFresh(t)

	const count = 500
	for i := 0; i < count; i++ {
		txId, err := w.Begin()
		require.NoError(t, err)
		require.NoError(t, w.Write(txId, entry(fmt.Sprintf("entry-%d", i))))
		require.NoError(t, w.Commit(txId))
	}

	assert.Len(t, *applied, count)
}
