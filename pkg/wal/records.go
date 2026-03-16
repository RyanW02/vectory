package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RyanW02/vectory/internal/utils"
	"github.com/RyanW02/vectory/pkg/safemath"
)

type (
	baseTransactionRecord struct {
		transactionId uint64
	}

	beginRecord struct {
		baseTransactionRecord
	}

	redoRecord[T Data] struct {
		baseTransactionRecord
		Data T
	}

	commitRecord struct {
		baseTransactionRecord
	}

	abortRecord struct {
		baseTransactionRecord
	}

	startCheckpointRecord struct {
		activeTransactions []uint64
	}

	endCheckpointRecord struct{}
)

const (
	recordTypeBegin recordType = iota
	recordTypeRedo
	recordTypeCommit
	recordTypeAbort
	recordTypeStartCheckpoint
	recordTypeEndCheckpoint
)

type testData struct{}

func (t *testData) MarshalBinary() ([]byte, error) { return nil, nil }
func (t *testData) UnmarshalBinary([]byte) error   { return nil }
func (t *testData) New() Data                      { return new(testData) }

var (
	_ transactionRecord = (*beginRecord)(nil)
	_ transactionRecord = (*redoRecord[*testData])(nil)
	_ transactionRecord = (*commitRecord)(nil)
	_ transactionRecord = (*abortRecord)(nil)
	_ record            = (*startCheckpointRecord)(nil)
	_ record            = (*endCheckpointRecord)(nil)
)

func newBeginRecord(transactionId uint64) *beginRecord {
	return &beginRecord{
		baseTransactionRecord: newBaseTransactionRecord(transactionId),
	}
}

func newRedoRecord[T Data](transactionId uint64, data T) *redoRecord[T] {
	return &redoRecord[T]{
		baseTransactionRecord: newBaseTransactionRecord(transactionId),
		Data:                  data,
	}
}

func newCommitRecord(transactionId uint64) *commitRecord {
	return &commitRecord{
		baseTransactionRecord: newBaseTransactionRecord(transactionId),
	}
}

func newAbortRecord(transactionId uint64) *abortRecord {
	return &abortRecord{
		baseTransactionRecord: newBaseTransactionRecord(transactionId),
	}
}

func newStartCheckpointRecord(activeTransactions []uint64) *startCheckpointRecord {
	return &startCheckpointRecord{
		activeTransactions: activeTransactions,
	}
}

func newEndCheckpointRecord() *endCheckpointRecord {
	return new(endCheckpointRecord)
}

func newBaseTransactionRecord(transactionId uint64) baseTransactionRecord {
	return baseTransactionRecord{
		transactionId: transactionId,
	}
}

func (r baseTransactionRecord) TransactionId() uint64 {
	return r.transactionId
}

func (r beginRecord) Type() recordType {
	return recordTypeBegin
}

func (r redoRecord[T]) Type() recordType {
	return recordTypeRedo
}

func (r commitRecord) Type() recordType {
	return recordTypeCommit
}

func (r abortRecord) Type() recordType {
	return recordTypeAbort
}

func (r startCheckpointRecord) Type() recordType {
	return recordTypeStartCheckpoint
}

func (r endCheckpointRecord) Type() recordType {
	return recordTypeEndCheckpoint
}

func (r beginRecord) MarshalBinary() ([]byte, error) {
	data := make([]byte, 9)
	data[0] = byte(recordTypeBegin)
	binary.LittleEndian.PutUint64(data[1:9], r.transactionId)
	return data, nil
}

func (r *beginRecord) Read(reader io.Reader) (int, error) {
	data, n, err := utils.ReadN(reader, 8)
	if err != nil {
		return n, fmt.Errorf("failed to read begin record: %w", err)
	}

	r.transactionId = binary.LittleEndian.Uint64(data)
	return n, nil
}

func (r redoRecord[T]) MarshalBinary() ([]byte, error) {
	writeData, err := r.Data.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal redo record data: %w", err)
	}

	dataLen, err := safemath.IntToUint32(len(writeData))
	if err != nil {
		return nil, fmt.Errorf("redo record data too large: %w", err)
	}

	data := make([]byte, 13+len(writeData))
	data[0] = byte(recordTypeRedo)
	binary.LittleEndian.PutUint64(data[1:9], r.transactionId)
	binary.LittleEndian.PutUint32(data[9:13], dataLen)
	copy(data[13:], writeData)

	return data, nil
}

func (r *redoRecord[T]) Read(reader io.Reader) (int, error) {
	header, nHeader, err := utils.ReadN(reader, 12)
	if err != nil {
		return nHeader, fmt.Errorf("failed to read redo record header: %w", err)
	}

	r.transactionId = binary.LittleEndian.Uint64(header[:8])
	dataSize := binary.LittleEndian.Uint32(header[8:12])

	data, nData, err := utils.ReadN(reader, int(dataSize))
	if err != nil {
		return nHeader + nData, fmt.Errorf("failed to read redo record data: %w", err)
	}

	var zero T
	r.Data = zero.New().(T)

	if err := r.Data.UnmarshalBinary(data); err != nil {
		return nHeader + nData, fmt.Errorf("%w: failed to unmarshal redo record data: %w", ErrInvalidRecordData, err)
	}

	return nHeader + nData, nil
}

func (r commitRecord) MarshalBinary() ([]byte, error) {
	data := make([]byte, 9)
	data[0] = byte(recordTypeCommit)
	binary.LittleEndian.PutUint64(data[1:9], r.transactionId)
	return data, nil
}

func (r *commitRecord) Read(reader io.Reader) (int, error) {
	data, n, err := utils.ReadN(reader, 8)
	if err != nil {
		return n, fmt.Errorf("failed to read commit record: %w", err)
	}

	r.transactionId = binary.LittleEndian.Uint64(data)
	return n, nil
}

func (r abortRecord) MarshalBinary() ([]byte, error) {
	data := make([]byte, 9)
	data[0] = byte(recordTypeCommit)
	binary.LittleEndian.PutUint64(data[1:9], r.transactionId)
	return data, nil
}

func (r *abortRecord) Read(reader io.Reader) (int, error) {
	data, n, err := utils.ReadN(reader, 8)
	if err != nil {
		return n, fmt.Errorf("failed to read abort record: %w", err)
	}

	r.transactionId = binary.LittleEndian.Uint64(data)
	return n, nil
}

func (r startCheckpointRecord) MarshalBinary() ([]byte, error) {
	data := make([]byte, 1+8+8*len(r.activeTransactions))
	data[0] = byte(recordTypeStartCheckpoint)
	binary.LittleEndian.PutUint64(data[1:9], uint64(len(r.activeTransactions)))
	for i, transactionId := range r.activeTransactions {
		binary.LittleEndian.PutUint64(data[9+i*8:17+i*8], transactionId)
	}

	return data, nil
}

func (r *startCheckpointRecord) Read(reader io.Reader) (int, error) {
	header, nHeader, err := utils.ReadN(reader, 8)
	if err != nil {
		return nHeader, fmt.Errorf("failed to read start checkpoint record header: %w", err)
	}

	numActiveTransactions := binary.LittleEndian.Uint64(header)

	var sumN int
	r.activeTransactions = make([]uint64, numActiveTransactions)
	for i := uint64(0); i < numActiveTransactions; i++ {
		data, n, err := utils.ReadN(reader, 8)
		if err != nil {
			return nHeader + sumN, fmt.Errorf("failed to read active transaction ID for start checkpoint record: %w", err)
		}

		sumN += n

		r.activeTransactions[i] = binary.LittleEndian.Uint64(data)
	}

	return nHeader + sumN, nil
}

func (r endCheckpointRecord) MarshalBinary() ([]byte, error) {
	data := make([]byte, 1)
	data[0] = byte(recordTypeEndCheckpoint)
	return data, nil
}

func (r *endCheckpointRecord) Read(reader io.Reader) (int, error) {
	return 0, nil
}
