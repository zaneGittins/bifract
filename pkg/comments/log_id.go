package comments

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateLogID creates a deterministic hash-based ID for a log entry
// Uses SHA256(timestamp + raw_log) to ensure the same log always gets the same ID
func GenerateLogID(timestamp time.Time, rawLog string) string {
	data := fmt.Sprintf("%d:%s", timestamp.UnixNano(), rawLog)
	hash := sha256.Sum256([]byte(data))
	// Use first 16 bytes (32 hex chars) for reasonable uniqueness
	return hex.EncodeToString(hash[:16])
}
