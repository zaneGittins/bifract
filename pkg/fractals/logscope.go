package fractals

import (
	"context"

	"bifract/pkg/prisms"
)

// LogScope answers which fractals a request may read logs from, and whether a
// given log is one of them. Both halves are needed: a lookup by log_id alone
// matches across every fractal, so the id set prunes the query and Allows is the
// authority on the result.
type LogScope struct {
	Fractals *Manager
	Prisms   *prisms.Manager
}

// AccessibleFractalIDs returns the fractals the session is scoped to: one for a
// fractal session, the member fractals for a prism, the default fractal when no
// scope is set. An empty result means the caller may read no logs at all.
func (s LogScope) AccessibleFractalIDs(ctx context.Context) ([]string, error) {
	if prismID, _ := ctx.Value("selected_prism").(string); prismID != "" {
		if s.Prisms == nil {
			return nil, nil
		}
		return s.Prisms.GetMemberFractalIDs(ctx, prismID)
	}
	if fractalID, _ := ctx.Value("selected_fractal").(string); fractalID != "" {
		return []string{fractalID}, nil
	}
	if s.Fractals == nil {
		return nil, nil
	}
	def, err := s.Fractals.GetDefaultFractal(ctx)
	if err != nil {
		return nil, err
	}
	return []string{def.ID}, nil
}

// ReadFilterIDs is AccessibleFractalIDs widened for a `fractal_id IN (...)`
// filter, so rows written before fractal_id existed still match: they carry ”
// and belong to the default fractal.
func (s LogScope) ReadFilterIDs(ctx context.Context) ([]string, error) {
	ids, err := s.AccessibleFractalIDs(ctx)
	if err != nil || len(ids) == 0 {
		return ids, err
	}
	if s.Allows(ctx, "", ids) {
		ids = append(ids, "")
	}
	return ids, nil
}

// Allows reports whether a log carrying logFractalID is inside accessible.
// Legacy rows have an empty fractal_id and belong to the default fractal, which
// is resolved here rather than failing open.
func (s LogScope) Allows(ctx context.Context, logFractalID string, accessible []string) bool {
	if logFractalID == "" && s.Fractals != nil {
		if def, err := s.Fractals.GetDefaultFractal(ctx); err == nil {
			logFractalID = def.ID
		}
	}
	for _, id := range accessible {
		if id == logFractalID {
			return true
		}
	}
	return false
}
