package main

import "github.com/charmbracelet/lipgloss"

var (
	styleHeader = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205")).
			Background(lipgloss.Color("235")).
			Padding(0, 1)

	styleTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205"))

	stylePanelTitle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("244"))

	styleHelp = lipgloss.NewStyle().
			Foreground(lipgloss.Color("244")).
			Padding(0, 1)

	styleHelpKey = lipgloss.NewStyle().
			Foreground(lipgloss.Color("205")).
			Bold(true)

	styleTxItem = lipgloss.NewStyle().
			Foreground(lipgloss.Color("252"))

	styleTxSelected = lipgloss.NewStyle().
			Foreground(lipgloss.Color("205")).
			Bold(true)

	styleEventInfo = lipgloss.NewStyle().
			Foreground(lipgloss.Color("39"))

	styleEventBegin = lipgloss.NewStyle().
			Foreground(lipgloss.Color("82"))

	styleEventWrite = lipgloss.NewStyle().
			Foreground(lipgloss.Color("220"))

	styleEventCommit = lipgloss.NewStyle().
				Foreground(lipgloss.Color("46"))

	styleEventAbort = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196"))

	styleEventApplied = lipgloss.NewStyle().
				Foreground(lipgloss.Color("51"))

	styleEventCheckpoint = lipgloss.NewStyle().
				Foreground(lipgloss.Color("213"))

	styleEventError = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			Bold(true)

	styleEventRecovery = lipgloss.NewStyle().
				Foreground(lipgloss.Color("208"))

	styleInput = lipgloss.NewStyle().
			Foreground(lipgloss.Color("252"))

	styleLabel = lipgloss.NewStyle().
			Foreground(lipgloss.Color("244"))

	styleError = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			Bold(true)

	styleCursor = lipgloss.NewStyle().
			Foreground(lipgloss.Color("205")).
			Bold(true)
)
