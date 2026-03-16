package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/RyanW02/vectory/pkg/wal"
	"github.com/charmbracelet/bubbletea"
)

func main() {
	// Allow optional directory argument for convenience
	var walDir string
	if len(os.Args) > 1 {
		walDir = os.Args[1]
	}

	var w *wal.WAL[*KeyValueUpdate]
	var recoveryEvents []LogEntry
	pendingApply := make([]LogEntry, 0)

	if walDir != "" {
		var err error
		w, err = wal.Open[*KeyValueUpdate](walDir, wal.DefaultConfig())
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open WAL: %v\n", err)
			os.Exit(1)
		}

		w.SetLogger(slog.New(slog.NewTextHandler(os.Stdout, nil)))

		// Capture recovery apply events before launching TUI
		w.SetApplyFn(func(kv *KeyValueUpdate) error {
			recoveryEvents = append(recoveryEvents, LogEntry{
				kind:    EventRecovery,
				message: fmt.Sprintf("applied key=%s val=%s", kv.Key, kv.Value),
			})
			return nil
		})

		if err := w.Recover(); err != nil {
			fmt.Fprintf(os.Stderr, "recovery failed: %v\n", err)
			os.Exit(1)
		}

		// Switch to runtime apply function
		w.SetApplyFn(func(kv *KeyValueUpdate) error {
			pendingApply = append(pendingApply, LogEntry{
				kind:  EventApplied,
				key:   kv.Key,
				value: kv.Value,
			})
			return nil
		})
	}

	m := initialModel(w, walDir, recoveryEvents, &pendingApply)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error running TUI: %v\n", err)
		os.Exit(1)
	}
}
