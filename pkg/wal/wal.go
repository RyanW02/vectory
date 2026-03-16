package wal

import (
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/RyanW02/vectory/internal/utils"
)

type WAL[T Data] struct {
	mu     sync.Mutex
	config *Config
	logger *slog.Logger

	applyFn func(data T) error

	directory *os.Root

	nextTransactionId      uint64
	activeSegment          *segment
	activeTransactions     map[uint64][]T
	checkpointTransactions *[]uint64
	recovered              bool
}

func Open[T Data](directoryPath string, config *Config) (*WAL[T], error) {
	f, err := os.OpenRoot(directoryPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(directoryPath, 0750); err != nil {
				return nil, err
			}

			return Open[T](directoryPath, config)
		}

		return nil, err
	}

	return OpenWithDirectory[T](f, config)
}

func OpenWithDirectory[T Data](directory *os.Root, config *Config) (*WAL[T], error) {
	wal := &WAL[T]{
		mu:     sync.Mutex{},
		config: config,
		logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),

		directory: directory,

		applyFn: func(data T) error {
			fmt.Println(data)
			return nil
		},

		activeTransactions: make(map[uint64][]T),
	}
	if err := wal.assignCurrentSegment(); err != nil {
		return nil, err
	}

	return wal, nil
}

func (w *WAL[T]) Begin() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.recovered {
		return 0, fmt.Errorf("cannot begin transaction before recovery")
	}

	transactionId := w.allocateTransactionId()
	if err := w.append(newBeginRecord(transactionId)); err != nil {
		return 0, fmt.Errorf("failed to append begin record: %w", err)
	}

	if err := w.flush(); err != nil {
		return 0, fmt.Errorf("failed to flush after appending begin record: %w", err)
	}

	w.activeTransactions[transactionId] = make([]T, 0)
	return transactionId, nil
}

func (w *WAL[T]) Commit(transactionId uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	records, ok := w.activeTransactions[transactionId]
	if !ok {
		return fmt.Errorf("transaction with id %d does not exist", transactionId)
	}

	if err := w.append(newCommitRecord(transactionId)); err != nil {
		return fmt.Errorf("failed to append commit record: %w", err)
	}

	if err := w.flush(); err != nil {
		return fmt.Errorf("failed to flush after appending commit record: %w", err)
	}

	delete(w.activeTransactions, transactionId)

	// If there is an active checkpoint, we need to check if we have completed it
	if w.checkpointTransactions != nil {
		checkpointTransactions, updated := utils.RemoveElement(*w.checkpointTransactions, transactionId)
		if updated {
			*w.checkpointTransactions = checkpointTransactions
		}

		if len(checkpointTransactions) == 0 {
			if err := w.checkpointEnd(); err != nil {
				return fmt.Errorf("failed to write end checkpoint record: %w", err)
			}
		}
	}

	for _, r := range records {
		if err := w.applyFn(r); err != nil {
			return fmt.Errorf("failed to apply record during commit: %w", err)
		}
	}

	return nil
}

func (w *WAL[T]) Abort(transactionId uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.activeTransactions[transactionId]; !ok {
		return fmt.Errorf("transaction with id %d does not exist", transactionId)
	}

	if err := w.append(newAbortRecord(transactionId)); err != nil {
		return fmt.Errorf("failed to append abort record: %w", err)
	}

	if err := w.flush(); err != nil {
		return fmt.Errorf("failed to flush after appending abort record: %w", err)
	}

	delete(w.activeTransactions, transactionId)

	// If there is an active checkpoint, we need to check if we have completed it
	if w.checkpointTransactions != nil {
		checkpointTransactions, updated := utils.RemoveElement(*w.checkpointTransactions, transactionId)
		if updated {
			*w.checkpointTransactions = checkpointTransactions
		}

		if len(checkpointTransactions) == 0 {
			if err := w.checkpointEnd(); err != nil {
				return fmt.Errorf("failed to write end checkpoint record: %w", err)
			}
		}
	}

	return nil
}

func (w *WAL[T]) Write(transactionId uint64, data T) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	records, ok := w.activeTransactions[transactionId]
	if !ok {
		return fmt.Errorf("transaction with id %d does not exist", transactionId)
	}

	if err := w.append(newRedoRecord(transactionId, data)); err != nil {
		return fmt.Errorf("failed to append redo record: %w", err)
	}

	if err := w.flush(); err != nil {
		return fmt.Errorf("failed to flush after appending redo record: %w", err)
	}

	records = append(records, data)
	w.activeTransactions[transactionId] = records

	return nil
}

func (w *WAL[T]) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Copy the map keys to a slice to avoid holding the lock while performing checkpointing logic
	transactionIds := make([]uint64, 0, len(w.activeTransactions))
	for transactionId := range w.activeTransactions {
		transactionIds = append(transactionIds, transactionId)
	}

	// Write start checkpoint record with active transaction IDs
	if err := w.append(newStartCheckpointRecord(transactionIds)); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("failed to append start checkpoint record: %w", err)
	}

	if err := w.flush(); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("failed to flush after appending start checkpoint record: %w", err)
	}

	if len(transactionIds) == 0 {
		// If there are no active transactions, we can immediately write the end checkpoint record
		return w.checkpointEnd()
	}

	// Otherwise, we need to wait for the active transactions to complete before writing the end checkpoint record
	w.checkpointTransactions = &transactionIds
	return nil
}

func (w *WAL[T]) checkpointEnd() error {
	if err := w.append(newEndCheckpointRecord()); err != nil {
		return fmt.Errorf("failed to append end checkpoint record: %w", err)
	}

	if err := w.flush(); err != nil {
		return fmt.Errorf("failed to flush after appending end checkpoint record: %w", err)
	}

	w.checkpointTransactions = nil
	return nil
}

func (w *WAL[T]) SetApplyFn(fn func(data T) error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.applyFn = fn
}

func (w *WAL[T]) SetLogger(logger *slog.Logger) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logger = logger
}

func (w *WAL[T]) Recover() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.recovered {
		return fmt.Errorf("WAL has already been recovered")
	}

	if err := w.recover(); err != nil {
		return fmt.Errorf("failed to recover WAL: %w", err)
	}

	w.recovered = true
	return nil
}

func (w *WAL[T]) append(record record) error {
	data, err := record.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if w.activeSegment.Size()+int64(len(data)) > w.config.MaxSegmentBytes {
		if err := w.rotate(); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	if _, err := w.activeSegment.Write(data); err != nil {
		return fmt.Errorf("failed to write record to active segment: %w", err)
	}

	return nil
}

func (w *WAL[T]) flush() error {
	if w.activeSegment == nil {
		return fmt.Errorf("cannot flush when there is no active segment")
	}

	if err := w.activeSegment.Flush(); err != nil {
		return fmt.Errorf("failed to flush active segment: %w", err)
	}

	return fdatasync(w.activeSegment.file)
}

func (w *WAL[T]) allocateTransactionId() uint64 {
	id := w.nextTransactionId
	w.nextTransactionId++
	return id
}
