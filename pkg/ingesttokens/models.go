package ingesttokens

import (
	"time"

	"bifract/pkg/normalizers"
)

// IngestToken represents a per-fractal token used to authenticate log ingestion.
type IngestToken struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	Description     string     `json:"description,omitempty"`
	TokenPrefix     string     `json:"token_prefix"`
	TokenValue      string     `json:"token_value,omitempty"`
	FractalID       string     `json:"fractal_id"`
	ParserType      string     `json:"parser_type"`
	NormalizerID    *string    `json:"normalizer_id,omitempty"`
	NormalizerName  string     `json:"normalizer_name,omitempty"`
	TimestampFields []TsField  `json:"timestamp_fields"`
	IsActive        bool       `json:"is_active"`
	IsDefault       bool       `json:"is_default"`
	CreatedBy       string     `json:"created_by"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
	UsageCount      int64      `json:"usage_count"`
	LogCount        int64      `json:"log_count"`
}

// TsField defines a timestamp field name and its expected format.
type TsField struct {
	Field  string `json:"field"`
	Format string `json:"format"`
}

// ValidatedToken is the hot-path result returned after token validation.
type ValidatedToken struct {
	TokenID         string
	FractalID       string
	ParserType      string
	Normalizer      *normalizers.CompiledNormalizer
	TimestampFields []TsField
}

type CreateTokenRequest struct {
	Name            string    `json:"name"`
	Description     string    `json:"description,omitempty"`
	ParserType      string    `json:"parser_type,omitempty"`
	NormalizerID    *string   `json:"normalizer_id,omitempty"`
	TimestampFields []TsField `json:"timestamp_fields,omitempty"`
}

type UpdateTokenRequest struct {
	Name            *string   `json:"name,omitempty"`
	Description     *string   `json:"description,omitempty"`
	ParserType      *string   `json:"parser_type,omitempty"`
	NormalizerID    *string   `json:"normalizer_id,omitempty"`
	ClearNormalizer bool      `json:"clear_normalizer,omitempty"`
	TimestampFields []TsField `json:"timestamp_fields,omitempty"`
}

type CreateTokenResponse struct {
	Token       string      `json:"token"`
	TokenPrefix string      `json:"token_prefix"`
	IngestToken IngestToken `json:"ingest_token"`
}
