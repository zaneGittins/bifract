package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateLogID creates a deterministic hash ID for a log entry
// This matches the implementation used in the comments system
func GenerateLogID(timestamp time.Time, rawLog string) string {
	// Convert timestamp to nanoseconds
	nanos := timestamp.UnixNano()

	// Create hash input: "timestamp_nanos:raw_log"
	data := fmt.Sprintf("%d:%s", nanos, rawLog)

	// Compute SHA256 hash
	hash := sha256.Sum256([]byte(data))

	// Return first 32 hex characters (16 bytes)
	return hex.EncodeToString(hash[:16])
}
