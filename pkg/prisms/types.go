package prisms

import "time"

// Prism is a virtual view that queries across multiple fractals.
type Prism struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Description string        `json:"description,omitempty"`
	MemberCount int           `json:"member_count"`
	Members     []PrismMember `json:"members,omitempty"`
	CreatedBy   string        `json:"created_by,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

// PrismMember represents one fractal that is part of a prism.
type PrismMember struct {
	FractalID   string    `json:"fractal_id"`
	FractalName string    `json:"fractal_name,omitempty"`
	AddedAt     time.Time `json:"added_at"`
}
