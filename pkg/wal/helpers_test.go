package wal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithTransaction_BeginFails(t *testing.T) {
	dir := t.TempDir()
	w, err := Open[*testEntry](dir, DefaultConfig())
	require.NoError(t, err)
	// Deliberately skip Recover() so Begin() returns an error.
	err = w.WithTransaction(func(tw *TransactionWriter[*testEntry]) error {
		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot begin transaction before recovery")
}

func TestWithTransaction_CommitFails(t *testing.T) {
	w, _, _ := openFresh(t)
	applyErr := fmt.Errorf("commit apply error")
	w.SetApplyFn(func(*testEntry) error { return applyErr })

	err := w.WithTransaction(func(tw *TransactionWriter[*testEntry]) error {
		return tw.Write(entry("data"))
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, applyErr)
}

func TestTransactionWriter_DelegatesWrite(t *testing.T) {
	w, _, _ := openFresh(t)

	txId, err := w.Begin()
	require.NoError(t, err)

	tw := &TransactionWriter[*testEntry]{wal: w, transactionId: txId}
	require.NoError(t, tw.Write(entry("delegated")))

	w.mu.Lock()
	records := w.activeTransactions[txId]
	w.mu.Unlock()

	require.Len(t, records, 1)
	assert.Equal(t, "delegated", string(records[0].Value))
}
