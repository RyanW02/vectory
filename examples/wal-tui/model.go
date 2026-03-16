package main

import (
	"fmt"

	"github.com/RyanW02/vectory/pkg/wal"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/bubbletea"
)

type appState int

const (
	stateSetup appState = iota
	stateReady
	stateSelectTx
	stateWriteKey
	stateWriteValue
	stateConfirmQuit
	stateError
)

type EventKind int

const (
	EventInfo EventKind = iota
	EventBegin
	EventWrite
	EventCommit
	EventAbort
	EventApplied
	EventCheckpoint
	EventError
	EventRecovery
)

type LogEntry struct {
	kind    EventKind
	message string
	// for write/applied events
	txID  uint64
	key   string
	value string
}

func (e LogEntry) Render() string {
	switch e.kind {
	case EventInfo:
		return styleEventInfo.Render("[INFO] " + e.message)
	case EventBegin:
		return styleEventBegin.Render(fmt.Sprintf("[BEGIN] tx#%d", e.txID))
	case EventWrite:
		return styleEventWrite.Render(fmt.Sprintf("[WRITE] tx#%d key=%s val=%s", e.txID, e.key, e.value))
	case EventCommit:
		return styleEventCommit.Render(fmt.Sprintf("[COMMIT] tx#%d", e.txID))
	case EventAbort:
		return styleEventAbort.Render(fmt.Sprintf("[ABORT] tx#%d", e.txID))
	case EventApplied:
		return styleEventApplied.Render(fmt.Sprintf("[APPLIED] key=%s val=%s", e.key, e.value))
	case EventCheckpoint:
		return styleEventCheckpoint.Render("[CHECKPOINT] " + e.message)
	case EventError:
		return styleEventError.Render("[ERR] " + e.message)
	case EventRecovery:
		return styleEventRecovery.Render("[RECOVERY] " + e.message)
	default:
		return e.message
	}
}

// txInfo tracks TUI-side metadata about active transactions.
type txInfo struct {
	id     uint64
	writes int
}

type model struct {
	w      *wal.WAL[*KeyValueUpdate]
	walDir string
	state  appState

	// Inputs
	pathInput  textinput.Model
	keyInput   textinput.Model
	valueInput textinput.Model

	// Transaction tracking (TUI side)
	activeTxs     []txInfo
	selectedTxIdx int
	pendingAction string // "write", "commit", "abort"
	pendingTxID   uint64
	pendingKey    string

	// Pointer to the slice that the apply function appends to
	pendingApplyPtr *[]LogEntry

	// Event log
	events   []LogEntry
	viewport viewport.Model

	width, height int
	fatalErr      error
}

func initialModel(w *wal.WAL[*KeyValueUpdate], walDir string, recoveryEvents []LogEntry, pendingApplyPtr *[]LogEntry) model {
	pathIn := textinput.New()
	pathIn.Placeholder = "/tmp/my-wal"
	pathIn.CharLimit = 256

	keyIn := textinput.New()
	keyIn.Placeholder = "key"
	keyIn.CharLimit = 128

	valueIn := textinput.New()
	valueIn.Placeholder = "value"
	valueIn.CharLimit = 256

	vp := viewport.New(0, 0)

	var events []LogEntry
	if w != nil {
		events = append(events, LogEntry{kind: EventInfo, message: "WAL opened at " + walDir})
		events = append(events, recoveryEvents...)
	}

	state := stateReady
	if w == nil {
		state = stateSetup
		pathIn.Focus()
	}

	return model{
		w:               w,
		walDir:          walDir,
		state:           state,
		pathInput:       pathIn,
		keyInput:        keyIn,
		valueInput:      valueIn,
		activeTxs:       []txInfo{},
		pendingApplyPtr: pendingApplyPtr,
		events:          events,
		viewport:        vp,
	}
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}
