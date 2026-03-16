package main

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

func (m model) View() string {
	switch m.state {
	case stateSetup:
		return m.viewSetup()
	case stateError:
		return m.viewError()
	default:
		return m.viewMain()
	}
}

func (m model) viewSetup() string {
	title := styleTitle.Render("WAL TUI")
	prompt := styleLabel.Render("Enter WAL directory path:")
	input := styleInput.Render(m.pathInput.View())
	hint := styleHelp.Render("Press Enter to open, Ctrl+C to quit")
	return lipgloss.JoinVertical(lipgloss.Left,
		"",
		"  "+title,
		"",
		"  "+prompt,
		"  "+input,
		"",
		"  "+hint,
	)
}

func (m model) viewError() string {
	title := styleError.Render("Fatal Error")
	msg := ""
	if m.fatalErr != nil {
		msg = m.fatalErr.Error()
	}
	return lipgloss.JoinVertical(lipgloss.Left,
		"",
		"  "+title,
		"",
		"  "+msg,
		"",
		"  "+styleHelp.Render("Press q to quit"),
	)
}

func (m model) viewMain() string {
	if m.width == 0 || m.height == 0 {
		return ""
	}

	header := m.renderHeader()
	help := m.renderHelp()
	body := m.renderBody()

	return lipgloss.JoinVertical(lipgloss.Left, header, body, help)
}

func (m model) renderHeader() string {
	title := styleTitle.Render("WAL TUI")
	dir := styleHelp.Render("· " + m.walDir)
	content := title + "  " + dir
	return styleHeader.Width(m.width).Render(content)
}

func (m model) renderHelp() string {
	keys := []struct{ key, desc string }{
		{"b", "begin"},
		{"w", "write"},
		{"c", "commit"},
		{"a", "abort"},
		{"p", "checkpoint"},
		{"q", "quit"},
	}
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = styleHelpKey.Render(k.key) + styleHelp.Render("="+k.desc)
	}
	line := strings.Join(parts, styleHelp.Render("  "))
	return styleHelp.Width(m.width).Render(line)
}

func (m model) renderBody() string {
	bodyHeight := m.height - 2 // header + help

	leftWidth := m.width / 3
	rightWidth := m.width - leftWidth - 1

	left := m.renderLeft(leftWidth, bodyHeight)
	right := m.renderRight(rightWidth, bodyHeight)

	return lipgloss.JoinHorizontal(lipgloss.Top, left, right)
}

func (m model) renderLeft(w, h int) string {
	title := stylePanelTitle.Render("Active Txs")
	divider := stylePanelTitle.Render(strings.Repeat("─", w-2))

	var lines []string
	lines = append(lines, title)
	lines = append(lines, divider)

	switch m.state {
	case stateSelectTx:
		for i, tx := range m.activeTxs {
			line := fmt.Sprintf("tx#%d (%d writes)", tx.id, tx.writes)
			if i == m.selectedTxIdx {
				lines = append(lines, styleCursor.Render("> ")+styleTxSelected.Render(line))
			} else {
				lines = append(lines, "  "+styleTxItem.Render(line))
			}
		}

	case stateWriteKey:
		lines = append(lines, styleLabel.Render("Writing to tx#"+fmt.Sprintf("%d", m.pendingTxID)))
		lines = append(lines, "")
		lines = append(lines, styleLabel.Render("Key: ")+m.keyInput.View())

	case stateWriteValue:
		lines = append(lines, styleLabel.Render("Writing to tx#"+fmt.Sprintf("%d", m.pendingTxID)))
		lines = append(lines, "")
		lines = append(lines, styleLabel.Render("Key:   ")+styleInput.Render(m.pendingKey))
		lines = append(lines, styleLabel.Render("Value: ")+m.valueInput.View())

	case stateConfirmQuit:
		lines = append(lines, styleError.Render("Uncommitted transactions remain on disk."))
		lines = append(lines, "")
		lines = append(lines, styleTxItem.Render("Quit anyway? (y/n)"))

	default:
		if len(m.activeTxs) == 0 {
			lines = append(lines, styleHelp.Render("(none)"))
		} else {
			for _, tx := range m.activeTxs {
				line := fmt.Sprintf("tx#%d (%d writes)", tx.id, tx.writes)
				lines = append(lines, "  "+styleTxItem.Render(line))
			}
		}
	}

	content := strings.Join(lines, "\n")
	// Pad to fill height
	lineCount := strings.Count(content, "\n") + 1
	for i := lineCount; i < h; i++ {
		content += "\n"
	}

	return lipgloss.NewStyle().
		Width(w).
		Height(h).
		BorderRight(true).
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("238")).
		Render(content)
}

func (m model) renderRight(w, h int) string {
	title := stylePanelTitle.Render("Event Log")
	divider := stylePanelTitle.Render(strings.Repeat("─", w-2))
	header := title + "\n" + divider + "\n"

	vp := m.viewport
	vp.Width = w - 2
	vp.Height = h - 3 // title + divider + padding

	return lipgloss.NewStyle().
		Width(w).
		Height(h).
		Padding(0, 1).
		Render(header + vp.View())
}

func (m model) syncViewport() model {
	lines := make([]string, len(m.events))
	for i, e := range m.events {
		lines[i] = e.Render()
	}
	m.viewport.SetContent(strings.Join(lines, "\n"))
	m.viewport.GotoBottom()
	return m
}

func (m model) resizeViewport() model {
	if m.height < 3 || m.width < 10 {
		return m
	}
	bodyHeight := m.height - 2
	rightWidth := m.width - m.width/3 - 3
	m.viewport.Width = rightWidth
	m.viewport.Height = bodyHeight - 3
	return m
}
