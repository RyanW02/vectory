package main

import (
	"encoding/binary"

	"github.com/RyanW02/vectory/pkg/safemath"
	"github.com/RyanW02/vectory/pkg/wal"
)

type KeyValueUpdate struct {
	Key   string
	Value string
}

var _ wal.Data = (*KeyValueUpdate)(nil)

func (k KeyValueUpdate) MarshalBinary() ([]byte, error) {
	keyLen, err := safemath.IntToUint32(len(k.Key))
	if err != nil {
		return nil, err
	}

	valueLen, err := safemath.IntToUint32(len(k.Value))
	if err != nil {
		return nil, err
	}

	data := make([]byte, 4+len(k.Key)+4+len(k.Value))
	binary.LittleEndian.PutUint32(data[0:4], keyLen)
	copy(data[4:4+len(k.Key)], k.Key)
	binary.LittleEndian.PutUint32(data[4+len(k.Key):8+len(k.Key)], valueLen)
	copy(data[8+len(k.Key):], k.Value)
	return data, nil
}

func (k *KeyValueUpdate) New() wal.Data { return new(KeyValueUpdate) }

func (k *KeyValueUpdate) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return wal.ErrInvalidRecordData
	}

	keyLen := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < int(8+keyLen) {
		return wal.ErrInvalidRecordData
	}

	k.Key = string(data[4 : 4+keyLen])

	valueLen := binary.LittleEndian.Uint32(data[4+keyLen : 8+keyLen])
	if len(data) < int(8+keyLen+valueLen) {
		return wal.ErrInvalidRecordData
	}

	k.Value = string(data[8+keyLen : 8+keyLen+valueLen])
	return nil
}
