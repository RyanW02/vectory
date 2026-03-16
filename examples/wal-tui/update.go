package main

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/RyanW02/vectory/pkg/wal"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbletea"
)

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m = m.resizeViewport()
		return m, nil

	case tea.KeyMsg:
		switch m.state {
		case stateSetup:
			return m.updateSetup(msg)
		case stateReady:
			return m.updateReady(msg)
		case stateSelectTx:
			return m.updateSelectTx(msg)
		case stateWriteKey:
			return m.updateWriteKey(msg)
		case stateWriteValue:
			return m.updateWriteValue(msg)
		case stateConfirmQuit:
			return m.updateConfirmQuit(msg)
		case stateError:
			return m.updateError(msg)
		}
	}

	// Pass scroll events to viewport in ready state
	if m.state == stateReady {
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m model) updateSetup(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		dir := m.pathInput.Value()
		if dir == "" {
			dir = m.pathInput.Placeholder
		}

		// Open the WAL
		var recoveryEvents []LogEntry
		pendingApply := make([]LogEntry, 0)

		w, err := wal.Open[*KeyValueUpdate](dir, wal.DefaultConfig())
		if err != nil {
			m.fatalErr = fmt.Errorf("failed to open WAL: %w", err)
			m.state = stateError
			return m, nil
		}

		w.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
		w.SetApplyFn(func(kv *KeyValueUpdate) error {
			recoveryEvents = append(recoveryEvents, LogEntry{kind: EventRecovery, message: fmt.Sprintf("applied key=%s val=%s", kv.Key, kv.Value)})
			return nil
		})

		if err := w.Recover(); err != nil {
			m.fatalErr = fmt.Errorf("recovery failed: %w", err)
			m.state = stateError
			return m, nil
		}

		w.SetApplyFn(func(kv *KeyValueUpdate) error {
			pendingApply = append(pendingApply, LogEntry{kind: EventApplied, key: kv.Key, value: kv.Value})
			return nil
		})

		m.w = w
		m.walDir = dir
		m.pendingApplyPtr = &pendingApply
		m.state = stateReady

		m.events = append(m.events, LogEntry{kind: EventInfo, message: "WAL opened at " + dir})
		if len(recoveryEvents) == 0 {
			m.events = append(m.events, LogEntry{kind: EventInfo, message: "No recovery needed"})
		} else {
			m.events = append(m.events, LogEntry{kind: EventInfo, message: fmt.Sprintf("Recovery: replayed %d event(s)", len(recoveryEvents))})
			m.events = append(m.events, recoveryEvents...)
		}

		m = m.syncViewport()
		return m, nil

	case tea.KeyCtrlC:
		return m, tea.Quit

	default:
		var cmd tea.Cmd
		m.pathInput, cmd = m.pathInput.Update(msg)
		return m, cmd
	}
}

func (m model) updateReady(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit

	case "q":
		if len(m.activeTxs) > 0 {
			m.state = stateConfirmQuit
			return m, nil
		}
		return m, tea.Quit

	case "b":
		txID, err := m.w.Begin()
		if err != nil {
			m.events = append(m.events, LogEntry{kind: EventError, message: fmt.Sprintf("begin failed: %v", err)})
		} else {
			m.activeTxs = append(m.activeTxs, txInfo{id: txID, writes: 0})
			m.events = append(m.events, LogEntry{kind: EventBegin, txID: txID})
		}
		m = m.syncViewport()
		return m, nil

	case "w":
		return m.initiateAction("write")

	case "c":
		return m.initiateAction("commit")

	case "a":
		return m.initiateAction("abort")

	case "p":
		if err := m.w.Checkpoint(); err != nil {
			m.events = append(m.events, LogEntry{kind: EventError, message: fmt.Sprintf("checkpoint failed: %v", err)})
		} else {
			m.events = append(m.events, LogEntry{kind: EventCheckpoint, message: "triggered"})
		}
		m = m.syncViewport()
		return m, nil

	default:
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(msg)
		return m, cmd
	}
}

func (m model) initiateAction(action string) (tea.Model, tea.Cmd) {
	if len(m.activeTxs) == 0 {
		m.events = append(m.events, LogEntry{kind: EventError, message: fmt.Sprintf("no active transactions for %s", action)})
		m = m.syncViewport()
		return m, nil
	}

	if len(m.activeTxs) == 1 {
		m.pendingTxID = m.activeTxs[0].id
		m.pendingAction = action
		return m.executePendingAction()
	}

	// Multiple transactions: go to selection
	m.pendingAction = action
	m.selectedTxIdx = 0
	m.state = stateSelectTx
	return m, nil
}

func (m model) executePendingAction() (tea.Model, tea.Cmd) {
	switch m.pendingAction {
	case "write":
		m.keyInput.SetValue("")
		m.keyInput.Focus()
		m.state = stateWriteKey
		return m, textinput.Blink

	case "commit":
		if err := m.w.Commit(m.pendingTxID); err != nil {
			m.events = append(m.events, LogEntry{kind: EventError, message: fmt.Sprintf("commit tx#%d failed: %v", m.pendingTxID, err)})
		} else {
			m.events = append(m.events, LogEntry{kind: EventCommit, txID: m.pendingTxID})
			// Drain apply events
			if m.pendingApplyPtr != nil {
				m.events = append(m.events, *m.pendingApplyPtr...)
				*m.pendingApplyPtr = (*m.pendingApplyPtr)[:0]
			}
			m.activeTxs = removeTx(m.activeTxs, m.pendingTxID)
		}
		m.state = stateReady
		m = m.syncViewport()
		return m, nil

	case "abort":
		if err := m.w.Abort(m.pendingTxID); err != nil {
			m.events = append(m.events, LogEntry{kind: EventError, message: fmt.Sprintf("abort tx#%d failed: %v", m.pendingTxID, err)})
		} else {
			m.events = append(m.events, LogEntry{kind: EventAbort, txID: m.pendingTxID})
			m.activeTxs = removeTx(m.activeTxs, m.pendingTxID)
		}
		m.state = stateReady
		m = m.syncViewport()
		return m, nil
	}

	m.state = stateReady
	return m, nil
}

func (m model) updateSelectTx(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "esc":
		m.state = stateReady
		return m, nil

	case "up", "k":
		if m.selectedTxIdx > 0 {
			m.selectedTxIdx--
		}
		return m, nil

	case "down", "j":
		if m.selectedTxIdx < len(m.activeTxs)-1 {
			m.selectedTxIdx++
		}
		return m, nil

	case "enter":
		if m.selectedTxIdx < len(m.activeTxs) {
			m.pendingTxID = m.activeTxs[m.selectedTxIdx].id
			return m.executePendingAction()
		}
		return m, nil
	}

	return m, nil
}

func (m model) updateWriteKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		m.pendingKey = m.keyInput.Value()
		m.valueInput.SetValue("")
		m.valueInput.Focus()
		m.keyInput.Blur()
		m.state = stateWriteValue
		return m, textinput.Blink

	case tea.KeyEsc:
		m.keyInput.Blur()
		m.state = stateReady
		return m, nil

	default:
		var cmd tea.Cmd
		m.keyInput, cmd = m.keyInput.Update(msg)
		return m, cmd
	}
}

func (m model) updateWriteValue(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		key := m.pendingKey
		value := m.valueInput.Value()
		m.valueInput.Blur()

		if err := m.w.Write(m.pendingTxID, &KeyValueUpdate{Key: key, Value: value}); err != nil {
			m.events = append(m.events, LogEntry{kind: EventError, message: fmt.Sprintf("write tx#%d failed: %v", m.pendingTxID, err)})
		} else {
			m.events = append(m.events, LogEntry{kind: EventWrite, txID: m.pendingTxID, key: key, value: value})
			m.activeTxs = incrementWrites(m.activeTxs, m.pendingTxID)
		}

		m.state = stateReady
		m = m.syncViewport()
		return m, nil

	case tea.KeyEsc:
		m.valueInput.Blur()
		m.state = stateReady
		return m, nil

	default:
		var cmd tea.Cmd
		m.valueInput, cmd = m.valueInput.Update(msg)
		return m, cmd
	}
}

func (m model) updateConfirmQuit(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "y", "Y":
		return m, tea.Quit
	case "n", "N", "esc":
		m.state = stateReady
		return m, nil
	case "ctrl+c":
		return m, tea.Quit
	}
	return m, nil
}

func (m model) updateError(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	}
	return m, nil
}

func removeTx(txs []txInfo, id uint64) []txInfo {
	result := make([]txInfo, 0, len(txs))
	for _, tx := range txs {
		if tx.id != id {
			result = append(result, tx)
		}
	}
	return result
}

func incrementWrites(txs []txInfo, id uint64) []txInfo {
	for i, tx := range txs {
		if tx.id == id {
			txs[i].writes++
			return txs
		}
	}
	return txs
}
