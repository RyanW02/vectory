package wal

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strconv"
)

var segmentNamePattern = regexp.MustCompile(`^wal-(\d+)\.log$`)

func (w *WAL[T]) listSegments() ([]int, error) {
	var segmentIds []int
	if err := fs.WalkDir(w.directory.FS(), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.Name() == "." {
			return nil
		}

		if d.IsDir() {
			w.logger.Warn("skipping subdirectory in WAL directory", "subdirectoryName", path)
			return nil
		}

		match := segmentNamePattern.FindStringSubmatch(d.Name())
		if len(match) != 2 {
			// Don't hard fail if we encounter other files in the directory, just log and skip them
			w.logger.Warn("skipping file with invalid name", "fileName", d.Name())
			return nil
		}

		segmentId, err := strconv.Atoi(match[1])
		if err != nil {
			return fmt.Errorf("invalid segment file name: %s", d.Name())
		}

		if segmentId < 0 {
			return fmt.Errorf("invalid segment ID %d in file name: %s", segmentId, d.Name())
		}

		segmentIds = append(segmentIds, segmentId)
		return nil
	}); err != nil {
		return nil, err
	}

	sort.Ints(segmentIds)
	return segmentIds, nil
}

func (w *WAL[T]) assignCurrentSegment() error {
	if w.activeSegment != nil {
		return fmt.Errorf("cannot open new segment while another segment is active")
	}

	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	// Create new segment if there are no existing segments
	var segmentId int
	var initialiseRootSegment bool
	if len(segments) == 0 {
		segmentId = 1
		initialiseRootSegment = true
	} else {
		segmentId = segments[len(segments)-1]
	}

	activeSegment, err := w.openSegment(segmentId, initialiseRootSegment)
	if err != nil {
		return err
	}

	w.activeSegment = activeSegment

	if initialiseRootSegment {
		if err := w.append(newStartCheckpointRecord(make([]uint64, 0))); err != nil {
			return fmt.Errorf("failed to write initial start checkpoint record to new segment: %w", err)
		}

		if err := w.append(newEndCheckpointRecord()); err != nil {
			return fmt.Errorf("failed to write initial end checkpoint record to new segment: %w", err)
		}

		if err := w.flush(); err != nil {
			return fmt.Errorf("failed to flush after writing initial start checkpoint record to new segment: %w", err)
		}
	}

	return nil
}

func (w *WAL[T]) openSegment(id int, createIfNotExist bool) (s *segment, err error) {
	fileName := segmentFileName(id)

	flags := os.O_WRONLY | os.O_APPEND
	if createIfNotExist {
		flags |= os.O_CREATE
	}

	f, err := w.directory.OpenFile(fileName, flags, 0600)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &segment{
		id:     id,
		size:   stat.Size(),
		file:   f,
		writer: bufio.NewWriter(f),
	}, nil
}

func (w *WAL[T]) rotate() error {
	if w.activeSegment == nil {
		return fmt.Errorf("cannot rotate segments when there is no active segment")
	}

	nextId := w.activeSegment.id + 1
	if err := w.activeSegment.Close(); err != nil {
		return err
	}

	segmentFile, err := w.openSegment(nextId, true)
	if err != nil {
		return err
	}

	w.activeSegment = segmentFile
	return nil
}

func segmentFileName(id int) string {
	return fmt.Sprintf("wal-%d.log", id)
}
