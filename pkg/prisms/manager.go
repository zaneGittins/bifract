package prisms

import (
	"context"
	"fmt"

	"bifract/pkg/storage"
)

// Manager handles prism CRUD and member management.
type Manager struct {
	pg *storage.PostgresClient
}

// NewManager creates a new prism manager.
func NewManager(pg *storage.PostgresClient) *Manager {
	return &Manager{pg: pg}
}

// CreatePrism creates a new prism.
func (m *Manager) CreatePrism(ctx context.Context, name, description, createdBy string) (*Prism, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	p := &Prism{}
	err := m.pg.QueryRow(ctx, `
		INSERT INTO prisms (name, description, created_by)
		VALUES ($1, $2, $3)
		RETURNING id, name, description, created_by, created_at, updated_at
	`, name, description, createdBy).Scan(
		&p.ID, &p.Name, &p.Description, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("create prism: %w", err)
	}
	p.Members = []PrismMember{}
	return p, nil
}

// GetPrism retrieves a prism by ID, including its members.
func (m *Manager) GetPrism(ctx context.Context, id string) (*Prism, error) {
	p := &Prism{}
	err := m.pg.QueryRow(ctx, `
		SELECT p.id, p.name, p.description, COALESCE(p.created_by, ''), p.created_at, p.updated_at,
		       COUNT(pm.fractal_id) AS member_count
		FROM prisms p
		LEFT JOIN prism_members pm ON pm.prism_id = p.id
		WHERE p.id = $1
		GROUP BY p.id
	`, id).Scan(&p.ID, &p.Name, &p.Description, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt, &p.MemberCount)
	if err != nil {
		return nil, fmt.Errorf("prism not found: %w", err)
	}

	members, err := m.getMembers(ctx, id)
	if err != nil {
		return nil, err
	}
	p.Members = members
	return p, nil
}

// ListPrisms returns all prisms with member counts.
func (m *Manager) ListPrisms(ctx context.Context) ([]*Prism, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT p.id, p.name, p.description, COALESCE(p.created_by, ''), p.created_at, p.updated_at,
		       COUNT(pm.fractal_id) AS member_count
		FROM prisms p
		LEFT JOIN prism_members pm ON pm.prism_id = p.id
		GROUP BY p.id
		ORDER BY p.name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list prisms: %w", err)
	}
	defer rows.Close()

	var prisms []*Prism
	for rows.Next() {
		p := &Prism{}
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt, &p.MemberCount); err != nil {
			return nil, fmt.Errorf("scan prism: %w", err)
		}
		p.Members = []PrismMember{}
		prisms = append(prisms, p)
	}
	return prisms, rows.Err()
}

// UpdatePrism updates a prism's name and description.
func (m *Manager) UpdatePrism(ctx context.Context, id, name, description string) (*Prism, error) {
	p := &Prism{}
	err := m.pg.QueryRow(ctx, `
		UPDATE prisms SET name = $2, description = $3, updated_at = NOW()
		WHERE id = $1
		RETURNING id, name, description, COALESCE(created_by, ''), created_at, updated_at
	`, id, name, description).Scan(
		&p.ID, &p.Name, &p.Description, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update prism: %w", err)
	}

	members, err := m.getMembers(ctx, id)
	if err != nil {
		return nil, err
	}
	p.Members = members
	p.MemberCount = len(members)
	return p, nil
}

// DeletePrism deletes a prism (members cascade automatically).
func (m *Manager) DeletePrism(ctx context.Context, id string) error {
	res, err := m.pg.Exec(ctx, `DELETE FROM prisms WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete prism: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("prism not found")
	}
	return nil
}

// AddMember adds a fractal to a prism. Duplicate entries are silently ignored.
func (m *Manager) AddMember(ctx context.Context, prismID, fractalID string) error {
	_, err := m.pg.Exec(ctx, `
		INSERT INTO prism_members (prism_id, fractal_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING
	`, prismID, fractalID)
	if err != nil {
		return fmt.Errorf("add member: %w", err)
	}
	return nil
}

// RemoveMember removes a fractal from a prism.
func (m *Manager) RemoveMember(ctx context.Context, prismID, fractalID string) error {
	_, err := m.pg.Exec(ctx, `
		DELETE FROM prism_members WHERE prism_id = $1 AND fractal_id = $2
	`, prismID, fractalID)
	return err
}

// GetMemberFractalIDs returns the fractal UUIDs that belong to a prism.
func (m *Manager) GetMemberFractalIDs(ctx context.Context, prismID string) ([]string, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT fractal_id FROM prism_members WHERE prism_id = $1
	`, prismID)
	if err != nil {
		return nil, fmt.Errorf("get member fractal IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// getMembers returns the full member list for a prism including fractal names.
func (m *Manager) getMembers(ctx context.Context, prismID string) ([]PrismMember, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT pm.fractal_id, f.name, pm.added_at
		FROM prism_members pm
		JOIN fractals f ON f.id = pm.fractal_id
		WHERE pm.prism_id = $1
		ORDER BY f.name ASC
	`, prismID)
	if err != nil {
		return nil, fmt.Errorf("get members: %w", err)
	}
	defer rows.Close()

	var members []PrismMember
	for rows.Next() {
		var mem PrismMember
		if err := rows.Scan(&mem.FractalID, &mem.FractalName, &mem.AddedAt); err != nil {
			return nil, err
		}
		members = append(members, mem)
	}
	if members == nil {
		members = []PrismMember{}
	}
	return members, rows.Err()
}
