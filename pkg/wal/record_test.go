package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadNextRecord_EmptyReader(t *testing.T) {
	_, _, err := readNextRecord[*testEntry](bytes.NewReader(nil))
	require.Error(t, err)
	assert.True(t, errors.Is(err, io.EOF))
}

func TestReadNextRecord_TruncatedBeginRecord(t *testing.T) {
	// type byte 0 (begin) + only 4 bytes; need 8 for txnId
	data := []byte{byte(recordTypeBegin), 0, 0, 0, 0}
	_, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.Error(t, err)
	assert.False(t, errors.Is(err, io.EOF), "should be ErrUnexpectedEOF, not EOF")
}

func TestReadNextRecord_TruncatedRedoHeader(t *testing.T) {
	// type byte 1 (redo) + only 6 bytes; need 12 for header
	data := []byte{byte(recordTypeRedo), 0, 0, 0, 0, 0, 0}
	_, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.Error(t, err)
	assert.False(t, errors.Is(err, io.EOF))
}

func TestReadNextRecord_TruncatedRedoData(t *testing.T) {
	// type byte + txnId (8 bytes) + dataLen=100 (4 bytes) + only 5 bytes of data
	data := make([]byte, 1+8+4+5)
	data[0] = byte(recordTypeRedo)
	binary.LittleEndian.PutUint64(data[1:9], 1)
	binary.LittleEndian.PutUint32(data[9:13], 100)
	_, _, err := readNextRecord[*testEntry](bytes.NewReader(data))
	require.Error(t, err)
	assert.False(t, errors.Is(err, io.EOF))
}

func TestReadNextRecord_ByteCount(t *testing.T) {
	cases := []struct {
		name string
		rec  record
	}{
		{"begin", newBeginRecord(1)},
		{"redo", newRedoRecord[*testEntry](1, entry("x"))},
		{"commit", newCommitRecord(1)},
		{"startCheckpoint", newStartCheckpointRecord([]uint64{1, 2})},
		{"endCheckpoint", newEndCheckpointRecord()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.rec.MarshalBinary()
			require.NoError(t, err)
			_, n, err := readNextRecord[*testEntry](bytes.NewReader(data))
			require.NoError(t, err)
			assert.Equal(t, len(data), n)
		})
	}
}

func TestReadNextRecord_MultipleRecords(t *testing.T) {
	r1 := newBeginRecord(1)
	r2 := newCommitRecord(1)
	data1, err := r1.MarshalBinary()
	require.NoError(t, err)
	data2, err := r2.MarshalBinary()
	require.NoError(t, err)

	reader := bytes.NewReader(append(data1, data2...))

	rec1, n1, err := readNextRecord[*testEntry](reader)
	require.NoError(t, err)
	assert.IsType(t, &beginRecord{}, rec1)
	assert.Equal(t, len(data1), n1)

	rec2, n2, err := readNextRecord[*testEntry](reader)
	require.NoError(t, err)
	assert.IsType(t, &commitRecord{}, rec2)
	assert.Equal(t, len(data2), n2)

	_, _, err = readNextRecord[*testEntry](reader)
	assert.True(t, errors.Is(err, io.EOF))
}
