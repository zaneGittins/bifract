package setup

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

	SubtitleStyle = lipgloss.NewStyle().
			Foreground(Gray)

	SuccessStyle = lipgloss.NewStyle().
			Foreground(Green)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(Red)

	WarningStyle = lipgloss.NewStyle().
			Foreground(Yellow)

	HighlightStyle = lipgloss.NewStyle().
			Foreground(Cyan)

	DimStyle = lipgloss.NewStyle().
			Foreground(Gray)

	PromptStyle = lipgloss.NewStyle().
			Foreground(Purple).
			Bold(true)

	ValueStyle = lipgloss.NewStyle().
			Foreground(Green)

	BoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(Purple).
			Padding(1, 2)

	// Panel wraps each wizard step's content
	PanelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(Dim).
			Padding(1, 3).
			MarginTop(1)

	// Active step in progress bar
	StepActiveStyle = lipgloss.NewStyle().
			Foreground(Purple).
			Bold(true)

	// Completed step in progress bar
	StepDoneStyle = lipgloss.NewStyle().
			Foreground(Green)

	// Pending step in progress bar
	StepPendingStyle = lipgloss.NewStyle().
			Foreground(Gray)

	// Label for input fields
	LabelStyle = lipgloss.NewStyle().
			Foreground(Cyan).
			MarginBottom(1)

	// Hint text at bottom of panels
	HintStyle = lipgloss.NewStyle().
			Foreground(Gray).
			Italic(true).
			MarginTop(1)

	// Selected item in a list
	SelectedStyle = lipgloss.NewStyle().
			Foreground(Cyan).
			Bold(true)

	// Unselected item in a list
	UnselectedStyle = lipgloss.NewStyle().
			Foreground(Gray)
)
