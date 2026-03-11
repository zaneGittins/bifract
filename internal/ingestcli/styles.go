package ingestcli

import "github.com/charmbracelet/lipgloss"

var (
	Purple = lipgloss.Color("#9c6ade")
	Green  = lipgloss.Color("#6bcf7f")
	Cyan   = lipgloss.Color("#8be9fd")
	Gray   = lipgloss.Color("#6c6c6c")
	White  = lipgloss.Color("#ffffff")
	Red    = lipgloss.Color("#ff5555")
	Yellow = lipgloss.Color("#f1fa8c")
	Dim    = lipgloss.Color("#44475a")

	TitleStyle = lipgloss.NewStyle().
			Foreground(Purple).
			Bold(true)

	SuccessStyle = lipgloss.NewStyle().
			Foreground(Green)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(Red)

	WarningStyle = lipgloss.NewStyle().
			Foreground(Yellow)

	DimStyle = lipgloss.NewStyle().
			Foreground(Gray)

	ValueStyle = lipgloss.NewStyle().
			Foreground(Cyan)

	BoldStyle = lipgloss.NewStyle().
			Bold(true)

	BarFilledStyle = lipgloss.NewStyle().
			Foreground(Purple)

	BarEmptyStyle = lipgloss.NewStyle().
			Foreground(Dim)
)
