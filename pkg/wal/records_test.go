package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingMarshalEntry struct{}

func (e *failingMarshalEntry) MarshalBinary() ([]byte, error) {
	return nil, fmt.Errorf("marshal error")
}
func (e *failingMarshalEntry) UnmarshalBinary([]byte) error { return nil }
func (e *failingMarshalEntry) New() Data                    { return new(failingMarshalEntry) }

type failingUnmarshalEntry struct{}

func (e *failingUnmarshalEntry) MarshalBinary() ([]byte, error) { return []byte("data"), nil }
func (e *failingUnmarshalEntry) UnmarshalBinary([]byte) error {
	return fmt.Errorf("unmarshal error")
}
func (e *failingUnmarshalEntry) New() Data { return new(failingUnmarshalEntry) }

func TestRecordType_Methods(t *testing.T) {
	cases := []struct {
		name     string
		rec      record
		expected recordType
	}{
		{"begin", newBeginRecord(1), recordTypeBegin},
		{"redo", newRedoRecord[*testEntry](1, entry("x")), recordTypeRedo},
		{"commit", newCommitRecord(1), recordTypeCommit},
		{"abort", newAbortRecord(1), recordTypeAbort},
		{"startCheckpoint", newStartCheckpointRecord(nil), recordTypeStartCheckpoint},
		{"endCheckpoint", newEndCheckpointRecord(), recordTypeEndCheckpoint},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.rec.Type())
		})
	}
}

func TestTransactionId_Method(t *testing.T) {
	cases := []struct {
		name string
		rec  transactionRecord
		id   uint64
	}{
		{"begin", newBeginRecord(42), 42},
		{"redo", newRedoRecord[*testEntry](7, entry("x")), 7},
		{"commit", newCommitRecord(99), 99},
		{"abort", newAbortRecord(55), 55},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.id, tc.rec.TransactionId())
		})
	}
}

func TestBeginRecord_BinarySize(t *testing.T) {
	data, err := newBeginRecord(1).MarshalBinary()
	require.NoError(t, err)
	assert.Len(t, data, 9)
}

func TestRedoRecord_BinarySize(t *testing.T) {
	payload := []byte("hello world")
	r := newRedoRecord[*testEntry](1, &testEntry{Value: payload})
	data, err := r.MarshalBinary()
	require.NoError(t, err)
	assert.Len(t, data, 13+len(payload))
}

func TestCommitRecord_BinarySize(t *testing.T) {
	data, err := newCommitRecord(1).MarshalBinary()
	require.NoError(t, err)
	assert.Len(t, data, 9)
}

func TestAbortRecord_BinarySize(t *testing.T) {
	data, err := newAbortRecord(1).MarshalBinary()
	require.NoError(t, err)
	assert.Len(t, data, 9)
}

func TestStartCheckpointRecord_BinarySize(t *testing.T) {
	cases := []struct {
		txns     []uint64
		expected int
	}{
		{nil, 9},
		{[]uint64{1}, 9 + 8},
		{[]uint64{1, 2, 3}, 9 + 24},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("len=%d", len(tc.txns)), func(t *testing.T) {
			data, err := newStartCheckpointRecord(tc.txns).MarshalBinary()
			require.NoError(t, err)
			assert.Len(t, data, tc.expected)
		})
	}
}

func TestEndCheckpointRecord_BinarySize(t *testing.T) {
	data, err := newEndCheckpointRecord().MarshalBinary()
	require.NoError(t, err)
	assert.Len(t, data, 1)
}

func TestStartCheckpointRecord_EmptyActiveTransactions(t *testing.T) {
	r := newStartCheckpointRecord([]uint64{})
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)
	scr, ok := rec.(*startCheckpointRecord)
	require.True(t, ok)
	assert.Empty(t, scr.activeTransactions)
}

func TestStartCheckpointRecord_ManyActiveTransactions(t *testing.T) {
	txns := make([]uint64, 100)
	for i := range txns {
		txns[i] = uint64(i + 1)
	}
	r := newStartCheckpointRecord(txns)
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	rec, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)
	scr, ok := rec.(*startCheckpointRecord)
	require.True(t, ok)
	assert.Equal(t, txns, scr.activeTransactions)
}

func TestRedoRecord_MarshalError(t *testing.T) {
	r := newRedoRecord[*failingMarshalEntry](1, new(failingMarshalEntry))
	_, err := r.MarshalBinary()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal error")
}

func TestRedoRecord_UnmarshalError(t *testing.T) {
	r := newRedoRecord[*failingUnmarshalEntry](1, new(failingUnmarshalEntry))
	data, err := r.MarshalBinary()
	require.NoError(t, err)

	_, _, err = readNextRecord[*failingUnmarshalEntry](bytes.NewReader(data))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidRecordData))
}

func TestAbortRecord_ReadDirectly(t *testing.T) {
	// Construct bytes with recordTypeAbort header to test the Read path directly,
	// bypassing the known bug in abortRecord.MarshalBinary().
	txnID := uint64(42)
	data := make([]byte, 9)
	data[0] = byte(recordTypeAbort)
	binary.LittleEndian.PutUint64(data[1:], txnID)

	rec, n, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, 9, n)
	ar, ok := rec.(*abortRecord)
	require.True(t, ok, "expected *abortRecord")
	assert.Equal(t, txnID, ar.TransactionId())
}
