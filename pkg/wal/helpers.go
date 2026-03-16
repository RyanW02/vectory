package wal

import "fmt"

type TransactionWriter[T Data] struct {
	wal           *WAL[T]
	transactionId uint64
}

func (w *TransactionWriter[T]) Write(data T) error {
	return w.wal.Write(w.transactionId, data)
}

func (w *WAL[T]) WithTransaction(fn func(*TransactionWriter[T]) error) error {
	transactionId, err := w.Begin()
	if err != nil {
		return err
	}

	transactionWriter := &TransactionWriter[T]{
		wal:           w,
		transactionId: transactionId,
	}

	if err := fn(transactionWriter); err != nil {
		if abortErr := w.Abort(transactionId); abortErr != nil {
			return fmt.Errorf("failed to abort transaction after error: %w; original error: %v", abortErr, err)
		}

		return err
	}

	return w.Commit(transactionId)
}
