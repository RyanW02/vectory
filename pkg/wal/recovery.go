package wal

import (
	"errors"
	"fmt"
	"io"
)

type recordIndex[T record] struct {
	segmentId int
	offset    int64
	record    T
}

func (w *WAL[T]) recover() error {
	segments, err := w.listSegments()
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	checkpointIndex, err := w.findReplayCheckpoint(segments)
	if err != nil {
		return fmt.Errorf("failed to find replay checkpoint: %w", err)
	}

	var maxTransactionId uint64

	activeTransactions := make(map[uint64][]redoRecord[T])
	for _, transactionId := range checkpointIndex.record.activeTransactions {
		activeTransactions[transactionId] = make([]redoRecord[T], 0)

		if transactionId > maxTransactionId {
			maxTransactionId = transactionId
		}
	}

	for segmentId := checkpointIndex.segmentId; segmentId <= segments[len(segments)-1]; segmentId++ {
		s, err := w.openSegment(segmentId, false)
		if err != nil {
			return fmt.Errorf("failed to open segment %d: %w", segmentId, err)
		}

		reader, err := s.Reader()
		if err != nil {
			return fmt.Errorf("failed to create reader for segment %d: %w", segmentId, err)
		}

		if segmentId == checkpointIndex.segmentId {
			// If this is the checkpoint segment, we need to start reading from the checkpoint offset
			if _, err := reader.Seek(checkpointIndex.offset, io.SeekStart); err != nil {
				return fmt.Errorf("failed to seek to checkpoint offset in segment %d: %w", segmentId, err)
			}
		}

		for {
			record, _, err := readNextRecord[T](reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					return fmt.Errorf("failed to read record from segment %d: %w", segmentId, err)
				}
			}

			switch r := record.(type) {
			case *beginRecord:
				activeTransactions[r.TransactionId()] = make([]redoRecord[T], 0)

				if r.TransactionId() > maxTransactionId {
					maxTransactionId = r.TransactionId()
				}
			case *commitRecord:
				redoRecords, ok := activeTransactions[r.TransactionId()]
				if !ok {
					return fmt.Errorf("encountered commit record for transaction %d that was not active during recovery from segment %d", r.TransactionId(), segmentId)
				}

				for _, redoRecord := range redoRecords {
					if err := w.applyFn(redoRecord.Data); err != nil {
						return fmt.Errorf("failed to apply redo record for transaction %d during recovery from segment %d: %w", r.TransactionId(), segmentId, err)
					}
				}

				delete(activeTransactions, r.TransactionId())
			case *abortRecord:
				delete(activeTransactions, r.TransactionId())
			case *redoRecord[T]:
				redoRecords, ok := activeTransactions[r.TransactionId()]
				if !ok {
					return fmt.Errorf("encountered redo record for transaction %d that was not active during recovery from segment %d", r.TransactionId(), segmentId)
				}

				activeTransactions[r.TransactionId()] = append(redoRecords, *r)
			case *startCheckpointRecord, *endCheckpointRecord:
				// no-op
			default:
				return fmt.Errorf("encountered unknown record type during recovery from segment %d: %T", segmentId, r)
			}
		}
	}

	w.nextTransactionId = maxTransactionId + 1

	if len(activeTransactions) > 0 {
		activeTransactionIds := make([]uint64, 0, len(activeTransactions))
		for transactionId := range activeTransactions {
			activeTransactionIds = append(activeTransactionIds, transactionId)
		}

		w.logger.Warn("recovery complete, found active transactions that were not committed before the crash, aborting them",
			"activeTransactionIds", activeTransactionIds)

		for _, transactionId := range activeTransactionIds {
			if err := w.append(newAbortRecord(transactionId)); err != nil {
				return fmt.Errorf("failed to append abort record for active transaction %d during recovery: %w", transactionId, err)
			}
		}

		return w.flush()
	}

	return nil
}

func (w *WAL[T]) findReplayCheckpoint(segments []int) (recordIndex[*startCheckpointRecord], error) {
	/*
	 * Iterate backwards through the segments, to find the segment with the most recent checkpoint.
	 * If we see an <end checkpoint> first, the writes performed by the transactions named in the corresponding
	 * <start checkpoint> record, plus any transactions that started after the <start checkpoint> record, must be
	 * replayed.
	 *
	 * If we see a <start checkpoint> record first, then it is not possible to tell whether committed transactions were
	 * written to disk before the crash, so we must go back to the previous <end checkpoint> record and repeat the above
	 * process from its corresponding <start checkpoint> record.
	 */

	// First, go back and find the most recent checkpoint, by iterating backwards through the segments and looking for
	// checkpoint records
	var previousStartCheckpointIndex, startCheckpointIndex *recordIndex[*startCheckpointRecord]
	var endCheckpointIndex *recordIndex[*endCheckpointRecord]
	for i := len(segments) - 1; i >= 0; i-- {
		segmentId := segments[i]
		s, err := w.openSegment(segmentId, false)
		if err != nil {
			return recordIndex[*startCheckpointRecord]{}, fmt.Errorf("failed to open segment %d: %w", segmentId, err)
		}

		//b, err := io.ReadAll(s.file)
		//w.logger.Warn("read segment file during checkpoint search", "segmentId", segmentId, "bytes", hex.EncodeToString(b))

		reader, err := s.Reader()
		if err != nil {
			return recordIndex[*startCheckpointRecord]{}, fmt.Errorf("failed to create reader for segment %d: %w", segmentId, err)
		}

		var offset int
		for {
			record, n, err := readNextRecord[T](reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					return recordIndex[*startCheckpointRecord]{}, fmt.Errorf("failed to read record from segment %d: %w", segmentId, err)
				}
			}

			switch r := record.(type) {
			case *startCheckpointRecord:
				if startCheckpointIndex == nil || segmentId >= startCheckpointIndex.segmentId {
					previousStartCheckpointIndex = startCheckpointIndex

					startCheckpointIndex = &recordIndex[*startCheckpointRecord]{
						segmentId: segmentId,
						offset:    int64(offset),
						record:    r,
					}
				}
			case *endCheckpointRecord:
				if endCheckpointIndex == nil || segmentId >= endCheckpointIndex.segmentId {
					endCheckpointIndex = &recordIndex[*endCheckpointRecord]{
						segmentId: segmentId,
						offset:    int64(offset),
						record:    r,
					}
				}
			}

			offset += n
		}

		if startCheckpointIndex != nil && endCheckpointIndex != nil {
			// If we have found both a start and end checkpoint, we can stop searching for checkpoints
			break
		}
	}

	// If no checkpoints were found, then we have a corrupted WAL, and we cannot recover
	if startCheckpointIndex == nil {
		return recordIndex[*startCheckpointRecord]{}, fmt.Errorf("no start checkpoint record found during recovery, cannot recover WAL")
	}

	if endCheckpointIndex == nil {
		return recordIndex[*startCheckpointRecord]{}, fmt.Errorf("no end checkpoint record found during recovery, cannot recover WAL")
	}

	endCheckpointIsMostRecent := endCheckpointIndex.segmentId > startCheckpointIndex.segmentId ||
		(endCheckpointIndex.segmentId == startCheckpointIndex.segmentId && endCheckpointIndex.offset > startCheckpointIndex.offset)

	if endCheckpointIsMostRecent {
		return *startCheckpointIndex, nil
	}

	if previousStartCheckpointIndex == nil {
		return recordIndex[*startCheckpointRecord]{}, fmt.Errorf("most recent checkpoint is a start checkpoint, " +
			"but no previous start checkpoint found during recovery, cannot recover WAL")
	}

	return *previousStartCheckpointIndex, nil
}
