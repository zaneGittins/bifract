package alerts

import (
	"context"
	"fmt"

	"bifract/pkg/models"
	"bifract/pkg/parser"
)

// This file adapts *Manager to models.LinkedAlertManager so an analytics model
// can own a normal alert (its detection query) without the models package
// importing pkg/alerts (which would be an import cycle, since pkg/alerts already
// imports pkg/models for model_lookup support).
//
// Ownership split: the model owns the alert's name, description, and generated
// detection query. The operator owns everything else (actions, throttle,
// severity, enabled state) from the Alerts page, and updates preserve those.

var _ models.LinkedAlertManager = (*Manager)(nil)

// CreateLinkedAlert creates the paused alert backing a model and returns its ID.
func (m *Manager) CreateLinkedAlert(ctx context.Context, spec models.LinkedAlertSpec) (string, error) {
	req := AlertCreateRequest{
		Name:        spec.Name,
		Description: spec.Description,
		QueryString: spec.QueryString,
		AlertType:   "event",
		Severity:    spec.Severity,
		Enabled:     spec.Enabled,
	}
	alert, err := m.CreateAlert(ctx, req, spec.CreatedBy, spec.FractalID, spec.PrismID)
	if err != nil {
		return "", err
	}
	return alert.ID, nil
}

// UpdateLinkedAlert refreshes ONLY the fields a model owns: name, description,
// and the generated detection query. It updates those columns directly rather
// than round-tripping through UpdateAlert, which would re-derive action
// associations from GetAlert -- and GetAlert only returns enabled actions, so a
// model edit could silently drop the alert's link to a globally-disabled action.
// Everything else (actions, throttle, severity, enabled state, scheduling,
// labels) is left exactly as configured on the Alerts page.
func (m *Manager) UpdateLinkedAlert(ctx context.Context, alertID string, spec models.LinkedAlertSpec) error {
	// Guard against ever writing a query the engine cannot parse (which would
	// break alert evaluation on the next cache refresh).
	if _, err := parser.ParseQuery(spec.QueryString); err != nil {
		return fmt.Errorf("generated query is invalid: %w", err)
	}
	result, err := m.pg.Exec(ctx,
		`UPDATE alerts SET name = $1, description = $2, query_string = $3, updated_at = NOW()
		 WHERE id = $4`,
		spec.Name, spec.Description, spec.QueryString, alertID)
	if err != nil {
		return fmt.Errorf("update linked alert: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return fmt.Errorf("linked alert not found")
	}
	m.engine.RefreshAlerts(ctx)
	return nil
}

// DeleteLinkedAlert removes the alert backing a model.
func (m *Manager) DeleteLinkedAlert(ctx context.Context, alertID string) error {
	return m.DeleteAlert(ctx, alertID)
}

// SetLinkedAlertEnabled toggles the backing alert's enabled state and refreshes
// the engine cache so the change takes effect immediately.
func (m *Manager) SetLinkedAlertEnabled(ctx context.Context, alertID string, enabled bool) error {
	result, err := m.pg.Exec(ctx,
		"UPDATE alerts SET enabled = $1, disabled_reason = '' WHERE id = $2", enabled, alertID)
	if err != nil {
		return fmt.Errorf("toggle linked alert: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return fmt.Errorf("linked alert not found")
	}
	m.engine.RefreshAlerts(ctx)
	return nil
}
