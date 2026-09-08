package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// DefaultTLSHThreshold is the TLSH convention for "similar". 50 is the usual
// looser setting; beyond that a match stops meaning much.
const DefaultTLSHThreshold = 30

// MaxTLSHThreshold bounds the threshold. TLSH distances run to several hundred,
// but past ~100 a "match" is noise, and a very loose threshold turns the emitted
// IN list into most of the index.
const MaxTLSHThreshold = 200

// TLSHParams is a parsed tlsh() invocation. The server resolves these into
// concrete digest matches before the query is rendered; see QueryOptions.TLSHMatches.
type TLSHParams struct {
	Field     string   // log field holding the digest
	Hashes    []string // literal needles from hash=
	Dict      string   // dictionary name from dict=
	Threshold int
}

// ExtractTLSHParams returns the tlsh() invocation in a pipeline, if any.
//
// A second tlsh() is rejected rather than ignored. Resolution produces one match
// set, so a second command would filter its own field using the first field's
// digests and overwrite the first's tlsh_distance, both silently.
func ExtractTLSHParams(pipeline *PipelineNode) (TLSHParams, bool, error) {
	var p TLSHParams
	var found bool
	var firstErr error

	ForEachCommand(pipeline, func(cmd CommandNode) {
		if strings.ToLower(cmd.Name) != "tlsh" {
			return
		}
		if found {
			if firstErr == nil {
				firstErr = fmt.Errorf("tlsh(): only one similarity filter per query; combine the digests into a single dict= or hash= list")
			}
			return
		}
		found = true
		p.Threshold = DefaultTLSHThreshold

		for _, arg := range cmd.Arguments {
			arg = strings.TrimSpace(arg)
			switch {
			case strings.HasPrefix(arg, "field="):
				p.Field = unquoteArg(strings.TrimPrefix(arg, "field="))
			case strings.HasPrefix(arg, "hash="):
				for _, h := range strings.Split(strings.TrimPrefix(arg, "hash="), ",") {
					if h = unquoteArg(h); h != "" {
						p.Hashes = append(p.Hashes, h)
					}
				}
			case strings.HasPrefix(arg, "dict="):
				p.Dict = unquoteArg(strings.TrimPrefix(arg, "dict="))
			case strings.HasPrefix(arg, "threshold="):
				raw := unquoteArg(strings.TrimPrefix(arg, "threshold="))
				n, err := strconv.Atoi(raw)
				if err != nil {
					if firstErr == nil {
						firstErr = fmt.Errorf("tlsh(): threshold must be a number, got %q", raw)
					}
					return
				}
				p.Threshold = n
			case arg == "":
			default:
				// A bare first argument is the field, so tlsh(tlsh, hash="...") works.
				if p.Field == "" && !strings.Contains(arg, "=") {
					p.Field = unquoteArg(arg)
					continue
				}
				// Anything else is a mistake worth reporting. Dropping it silently
				// turned a typo'd `treshold=50` into a query that ran at the default,
				// and an unquoted `hash=A,B` into a hunt for A alone.
				if firstErr == nil {
					if name, _, ok := strings.Cut(arg, "="); ok {
						firstErr = fmt.Errorf("tlsh(): unknown argument %q; expected field=, hash=, dict= or threshold=", name)
					} else {
						firstErr = fmt.Errorf("tlsh(): unexpected argument %q; quote a multi-value hash list as hash=\"a,b\"", arg)
					}
				}
			}
		}

		if firstErr == nil {
			firstErr = p.validate()
		}
	})

	if firstErr != nil {
		return TLSHParams{}, found, firstErr
	}
	return p, found, nil
}

// tlshFieldNamePattern is the shape a log field name may take. tlsh() is the one
// command whose field name reaches a hand-built SQL identifier (the fallback probe
// for an unindexed fractal), and a quoted argument passes the lexer verbatim, so
// the name is validated here rather than trusted. Identifier quoting is applied on
// top of this, not instead of it.
var tlshFieldNamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.\-]*$`)

func (p TLSHParams) validate() error {
	if p.Field == "" {
		return fmt.Errorf("tlsh(): field= is required, naming the log field that holds the digest")
	}
	if !tlshFieldNamePattern.MatchString(p.Field) {
		return fmt.Errorf("tlsh(): %q is not a valid field name; use letters, digits, underscore, dot or hyphen", p.Field)
	}
	if len(p.Hashes) == 0 && p.Dict == "" {
		return fmt.Errorf("tlsh(): needs hash=\"<digest>\" or dict=\"<name>\" to compare against")
	}
	if len(p.Hashes) > 0 && p.Dict != "" {
		return fmt.Errorf("tlsh(): use hash= or dict=, not both")
	}
	if p.Threshold < 0 {
		return fmt.Errorf("tlsh(): threshold must not be negative")
	}
	if p.Threshold > MaxTLSHThreshold {
		return fmt.Errorf("tlsh(): threshold %d is above the maximum of %d; beyond that a match carries no signal",
			p.Threshold, MaxTLSHThreshold)
	}
	return nil
}

func unquoteArg(s string) string {
	return strings.Trim(strings.TrimSpace(s), `"'`)
}
