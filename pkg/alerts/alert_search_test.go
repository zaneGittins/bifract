package alerts

import "testing"

func TestFilterAlertsByName(t *testing.T) {
	all := []*Alert{{Name: "Encoded PowerShell"}, {Name: "New service installed"}, {Name: "powershell download"}}
	tests := []struct {
		search string
		want   int
	}{
		{"", 3},
		{"  ", 3},
		{"powershell", 2},
		{"POWERSHELL", 2},
		{"service", 1},
		{"absent", 0},
	}
	for _, tt := range tests {
		if got := len(filterAlertsByName(all, tt.search)); got != tt.want {
			t.Errorf("filterAlertsByName(%q) = %d alerts, want %d", tt.search, got, tt.want)
		}
	}
}
