package wal

import (
	"encoding"
	"fmt"
	"io"
)

type (
	Data interface {
		encoding.BinaryMarshaler
		encoding.BinaryUnmarshaler
		New() Data
	}

	record interface {
		encoding.BinaryMarshaler
		binaryReader
		Type() recordType
	}

	transactionRecord interface {
		record
		TransactionId() uint64
	}

	binaryReader interface {
		Read(r io.Reader) (int, error)
	}
)

type recordType uint8

var (
	ErrInvalidRecordType = fmt.Errorf("invalid record type")
	ErrInvalidRecordData = fmt.Errorf("invalid record data")
)

func readNextRecord[T Data](r io.Reader) (record, int, error) {
	// Read the record type first to determine how much data to read
	header := make([]byte, 1)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, 0, fmt.Errorf("failed to read record header: %w", err)
	}

	rt := recordType(header[0])

	record, err := initialiseRecord[T](rt)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to initialise record: %w", err)
	}

	n, err := record.Read(r)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal record: %w", err)
	}

	return record, 1 + n, nil
}

func initialiseRecord[T Data](rt recordType) (record, error) {
	switch rt {
	case recordTypeBegin:
		return new(beginRecord), nil
	case recordTypeRedo:
		return new(redoRecord[T]), nil
	case recordTypeCommit:
		return new(commitRecord), nil
	case recordTypeAbort:
		return new(abortRecord), nil
	case recordTypeStartCheckpoint:
		return new(startCheckpointRecord), nil
	case recordTypeEndCheckpoint:
		return new(endCheckpointRecord), nil
	default:
		return nil, fmt.Errorf("%w: unknown record type %d", ErrInvalidRecordType, rt)
	}
}
