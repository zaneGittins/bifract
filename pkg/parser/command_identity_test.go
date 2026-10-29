package parser

import "testing"

// Tables keyed by command name must use the canonical name, or an alias of the
// command slips past them (model_lookup was renamed modelLookup and the scan-level
// join silently stopped applying to the new spelling).
func TestCommandTablesUseCanonicalNames(t *testing.T) {
	for name := range caseDisallowedCommands {
		if getCommandHandler(name) == nil {
			t.Errorf("caseDisallowedCommands: %q is not a registered command", name)
		} else if c := canonicalCommandName(name); c != name {
			t.Errorf("caseDisallowedCommands: %q is an alias of %q", name, c)
		}
	}
}

func TestCommandIndexMatchesAnyAlias(t *testing.T) {
	cmds := []CommandNode{{Name: "head"}, {Name: "modellookup"}}
	for _, name := range []string{"modelLookup", "model_lookup", "modellookup"} {
		if got := commandIndex(cmds, name); got != 1 {
			t.Errorf("commandIndex(%q) = %d, want 1", name, got)
		}
	}
	if got := commandIndex(cmds, "groupby"); got != -1 {
		t.Errorf("commandIndex(groupby) = %d, want -1", got)
	}
}

func TestCommandDisplayNameUsesTheRegisteredSpelling(t *testing.T) {
	for in, want := range map[string]string{"model_lookup": "modelLookup", "modellookup": "modelLookup", "modifiedz": "modifiedzscore", "nosuch": "nosuch"} {
		if got := CommandDisplayName(in); got != want {
			t.Errorf("CommandDisplayName(%q) = %q, want %q", in, got, want)
		}
	}
}
