package archive

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
)

// versionHintFile is Iceberg's filesystem-catalog convention for recording the
// current metadata version. Bifract commits through the Postgres-backed SQL
// catalog, which tracks metadata_location itself and never reads this file, so
// the hint exists purely to make the bucket self-describing: any Iceberg reader
// resolves the live snapshot without the catalog, and losing Postgres no longer
// obscures which metadata is current.
const versionHintFile = "version-hint.text"

// hintWarned suppresses repeat warnings per hint path. The write sits on the
// commit path of every archiver roll, so a deployment whose object storage
// credentials exclude the metadata prefix would otherwise log on every flush.
var hintWarned sync.Map

// versionHintTarget derives the hint path and contents from a table's metadata
// location. Returns false when the file name does not follow the
// "<version>-<uuid>.metadata.json" convention, in which case there is nothing
// safe to publish.
//
// The hint holds the metadata file's stem ("00002-<uuid>"), not the bare version
// number the Hadoop-catalog convention specifies. Readers resolve a hint by
// substituting its contents into "<hint>.metadata.json", and iceberg-go's SQL
// catalog embeds a UUID in the file name, so a bare "2" resolves to
// "2.metadata.json" and misses. The stem is a deviation that costs nothing: a
// strict Hadoop-catalog reader expects "v2.metadata.json" and could not have
// resolved these file names from any hint contents.
//
// Splits on the last separator rather than using path.Dir, which collapses the
// "//" in a URI scheme.
func versionHintTarget(metadataLocation string) (hintPath, hint string, ok bool) {
	i := strings.LastIndex(metadataLocation, "/")
	if i < 0 {
		return "", "", false
	}
	dir, base := metadataLocation[:i+1], metadataLocation[i+1:]
	stem, found := strings.CutSuffix(base, ".metadata.json")
	if !found || stem == "" {
		return "", "", false
	}
	// Confirm the leading version component so a non-Iceberg file never yields a
	// hint, even though the version itself is not what gets written.
	digits, _, found := strings.Cut(stem, "-")
	if !found || digits == "" {
		return "", "", false
	}
	if v, err := strconv.Atoi(digits); err != nil || v < 0 {
		return "", "", false
	}
	return dir + versionHintFile, stem, true
}

// writeVersionHint publishes the table's current metadata version to
// metadata/version-hint.text. Advisory only: failures are logged once per path
// and never propagate, since no Bifract read path depends on the hint.
//
// Concurrent archivers may race and leave the hint behind the true current
// version. A reader following a stale hint gets an older snapshot, or a
// missing-file error once that metadata has aged past
// write.metadata.previous-versions-max. Both self-heal on the next commit, which
// is cheaper than a read-modify-write on every commit to prevent.
func writeVersionHint(ctx context.Context, tbl *icetable.Table) {
	if tbl == nil {
		return
	}
	hintPath, hint, ok := versionHintTarget(tbl.MetadataLocation())
	if !ok {
		return
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		warnVersionHint(hintPath, err)
		return
	}
	w, ok := fsys.(icebergio.WriteFileIO)
	if !ok {
		return
	}
	if err := w.WriteFile(hintPath, []byte(hint)); err != nil {
		warnVersionHint(hintPath, err)
	}
}

func warnVersionHint(hintPath string, err error) {
	if _, seen := hintWarned.LoadOrStore(hintPath, struct{}{}); seen {
		return
	}
	log.Printf("[Archive] version hint write failed for %s (external Iceberg readers will need an explicit metadata path): %v", hintPath, err)
}
