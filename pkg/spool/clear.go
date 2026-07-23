package spool

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ClearGenerationSettingKey is the Postgres settings key holding the global,
// monotonically increasing "clear archive spool" generation. An admin action
// bumps it; each ingest pod compares it against the generation its own spool has
// already applied (the marker file below) and, while archiving is disabled,
// resets its spool to catch up. Shared here so the server (ingest, the spool
// Writer) and archiver (the spool Reader) binaries agree on the key.
const ClearGenerationSettingKey = "spool_clear_generation"

// clearMarkerFile records, inside the spool directory, the clear generation this
// spool has actually applied. It lives on the same volume as the segments, so the
// co-located archiver (sharing the volume) can tell when a requested clear has
// completed and re-sync its Reader. It is deliberately not a seg-*.spool file, so
// the segment machinery ignores it and Writer.Reset does not delete it.
const clearMarkerFile = "clear_gen"

// ReadClearGeneration returns the clear generation last applied to the spool in
// dir, or 0 when none has been (no marker, empty, or unreadable).
func ReadClearGeneration(dir string) int64 {
	b, err := os.ReadFile(filepath.Join(dir, clearMarkerFile))
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// WriteClearGeneration records that the spool in dir has been cleared up to gen.
// Written atomically (temp + rename) so a crash never leaves a torn marker.
func WriteClearGeneration(dir string, gen int64) error {
	tmp := filepath.Join(dir, clearMarkerFile+".tmp")
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(gen, 10)), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, clearMarkerFile))
}
