// Package tlshresolve turns a tlsh() invocation into the concrete digest matches a
// query renders as an IN filter.
//
// It lives apart from the query handler because both interactive search and alert
// evaluation need it. When it lived in pkg/query only, an alert whose BQL used
// tlsh() failed on every tick with "requires server-side pre-processing", forever.
package tlshresolve

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"

	"bifract/pkg/dictionaries"
	"bifract/pkg/models"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
	"bifract/pkg/tlsh"
)

// tlshMaxIndexRows caps how many distinct digests a probe will pull back.
//
// The limit is applied as a SQL LIMIT, so it bounds the result set ClickHouse
// materializes rather than being checked after the fact. Sized against memory, not
// against what the matcher could theoretically chew through: each row costs on the
// order of 270 bytes by the time it is a parsed candidate, so this is ~135MB, which
// a 2GB container survives. Still far past a sane index, since a fleet's distinct
// executables number in the thousands and a field whose distinct count approaches
// its row count is not a fuzzy-hash field.
const tlshMaxIndexRows = 500_000

// tlshMaxNeedles caps the needle list. Cost is distinct x needles, so a corpus
// dump (MalwareBazaar publishes over a million digests) would take minutes per
// query. Curated, family-representative lists are the workable shape.
const tlshMaxNeedles = 50_000

// tlshMaxWork caps candidates x needles, the product that actually sets the
// sweep's cost. The individual caps allow 2,000,000 x 50,000 = 10^11 comparisons,
// which is minutes of wall clock; this refuses that up front with a message naming
// both numbers instead of appearing to hang.
const tlshMaxWork = 2_000_000_000

// tlshMaxMatches caps the resolved match set. Past this the emitted IN list stops
// being a filter and becomes a copy of the index. Sized to stay inside the
// translator's literal-payload backstop (parser.tlshMaxLiteralBytes) so this is the
// limit that binds, and truncation is reported rather than silent.
const tlshMaxMatches = 25_000

// resolveTLSH turns a tlsh() invocation into the concrete digest matches the
// translator renders as an IN filter.
//
// The work is proportional to the number of DISTINCT digests, not to row count,
// which is what makes similarity matching viable over billions of rows. That
// collapse is why a tlsh model is required rather than optional: without its index
// the probe degrades to a DISTINCT scan of the whole window, which on a real fleet
// is slow enough that the feature would look broken instead of unconfigured.
// ModelIndexer resolves the per-fractal tlsh model indexes. Satisfied by *models.Manager.
type ModelIndexer interface {
	ListTLSHIndexes(ctx context.Context, fractalIDs []string, keyField string) (map[string]models.TLSHIndex, map[string]string, error)
}

// DictReader reads the needle list from a dictionary. Satisfied by *dictionaries.Manager.
type DictReader interface {
	GetDictionaryByName(ctx context.Context, fractalID, prismID, name string) (*dictionaries.Dictionary, error)
	GetKeys(ctx context.Context, id string, limit int) ([]string, error)
}

// LogQuerier runs the index probe. Satisfied by *storage.ClickHouseClient.
type LogQuerier interface {
	Query(ctx context.Context, query string) ([]map[string]interface{}, error)
}

// Resolver holds the three dependencies the resolution needs. Any may be nil; the
// resolution then fails with a message naming what is unavailable rather than
// panicking.
type Resolver struct {
	Models ModelIndexer
	Dicts  DictReader
	DB     LogQuerier
}

// Resolve returns the digests within threshold of a needle.
func (h *Resolver) Resolve(ctx context.Context, p parser.TLSHParams, fractalIDs []string, prismID string) ([]parser.TLSHMatch, error) {
	if h == nil || h.Models == nil {
		return nil, fmt.Errorf("tlsh(): analytics models are not available in this deployment")
	}
	if h.DB == nil {
		return nil, fmt.Errorf("tlsh(): no log store available in this deployment")
	}
	if len(fractalIDs) == 0 {
		return nil, fmt.Errorf("tlsh(): no fractal in scope")
	}

	indexes, filtered, err := h.Models.ListTLSHIndexes(ctx, fractalIDs, p.Field)
	if err != nil {
		return nil, fmt.Errorf("tlsh(): resolve digest index: %w", err)
	}

	// Every fractal in scope must be indexed. A partially indexed prism would
	// return a subset of the digests present and silently drop real matches, so
	// this fails loudly instead, naming what to create.
	var missing []string
	for _, fid := range fractalIDs {
		if _, ok := indexes[fid]; !ok {
			missing = append(missing, fid)
		}
	}
	if len(missing) > 0 {
		return nil, tlshMissingIndexError(p.Field, missing, filtered, len(fractalIDs), prismID)
	}

	needles, err := h.loadTLSHNeedles(ctx, p, fractalIDs, prismID)
	if err != nil {
		return nil, err
	}
	if len(needles) == 0 {
		return nil, fmt.Errorf("tlsh(): no valid digests to compare against")
	}

	candidates, err := h.probeTLSHIndex(ctx, indexes, fractalIDs)
	if err != nil {
		return nil, err
	}

	if work := len(candidates) * len(needles); work > tlshMaxWork {
		return nil, fmt.Errorf(
			"tlsh(): %d indexed digests against %d needles is %d comparisons, past the %d limit; narrow the needle list or the preceding filters",
			len(candidates), len(needles), work, tlshMaxWork)
	}

	matches, err := matchTLSH(ctx, candidates, needles, p.Threshold)
	if err != nil {
		return nil, err
	}
	if len(matches) > tlshMaxMatches {
		// Ordered closest-first, so the tail is the weakest. Say so: the result is
		// a different answer from the one asked for, and a lower threshold or a
		// narrower needle list is the fix.
		log.Printf("[tlsh] %d digests matched within distance %d; keeping the %d closest. Lower the threshold or narrow the needle list for a complete answer.",
			len(matches), p.Threshold, tlshMaxMatches)
		matches = matches[:tlshMaxMatches]
	}
	return matches, nil
}

// tlshCandidate is one distinct digest from the index, carried alongside its
// verbatim string because that string is what the log filter must match.
type tlshCandidate struct {
	raw    string
	digest tlsh.Digest
}

// probeTLSHIndex reads the distinct digests indexed for every fractal in scope.
func (h *Resolver) probeTLSHIndex(ctx context.Context, indexes map[string]models.TLSHIndex, fractalIDs []string) ([]tlshCandidate, error) {
	// One SELECT per index table, unioned: each fractal's index is its own model
	// and therefore its own table.
	seen := make(map[string]struct{})
	var out []tlshCandidate

	for _, fid := range fractalIDs {
		idx := indexes[fid]
		sql := fmt.Sprintf(
			"SELECT DISTINCT digest FROM `%s` WHERE fractal_id = '%s' LIMIT %d",
			storage.EscCHStr(idx.TableName), storage.EscCHStr(fid), tlshMaxIndexRows+1)

		rows, err := h.DB.Query(ctx, sql)
		if err != nil {
			return nil, fmt.Errorf("tlsh(): probe digest index %q: %w", idx.Name, err)
		}
		if len(rows) > tlshMaxIndexRows || len(out)+len(rows) > tlshMaxIndexRows {
			// Checked against the running total, not just this table: a prism spanning
			// several fractals would otherwise accumulate the per-index limit once per
			// member before anything complained.
			return nil, fmt.Errorf(
				"tlsh(): digest index %q pushes the candidate set past %d distinct values; a field this varied is not a fuzzy-hash field",
				idx.Name, tlshMaxIndexRows)
		}
		for _, r := range rows {
			raw, _ := r["digest"].(string)
			if raw == "" {
				continue
			}
			if _, dup := seen[raw]; dup {
				continue // same digest in two fractals: compare it once
			}
			// The model's guard admits only well-formed digests, so a parse failure
			// here means the index and the parser have drifted apart. Skip rather
			// than fail the query; TestTLSHGuardMatchesParser is what prevents it.
			d, err := tlsh.Parse(raw)
			if err != nil {
				continue
			}
			seen[raw] = struct{}{}
			out = append(out, tlshCandidate{raw: raw, digest: d})
		}
	}
	return out, nil
}

// tlshNeedle is a parsed comparison target plus the label reported on a match.
type tlshNeedle struct {
	label  string
	digest tlsh.Digest
}

func (h *Resolver) loadTLSHNeedles(ctx context.Context, p parser.TLSHParams, fractalIDs []string, prismID string) ([]tlshNeedle, error) {
	var raw []string

	switch {
	case len(p.Hashes) > 0:
		// A literal needle is the user's own input, so a malformed one is a mistake
		// worth reporting rather than skipping.
		for _, hsh := range p.Hashes {
			if !tlsh.Valid(hsh) {
				return nil, fmt.Errorf("tlsh(): %q is not a valid TLSH digest (expected %d hex characters, optionally prefixed with T1)",
					hsh, tlsh.DigestHexLen)
			}
		}
		raw = p.Hashes

	case p.Dict != "":
		if h.Dicts == nil {
			return nil, fmt.Errorf("tlsh(): dictionaries are not available in this deployment")
		}
		fid := ""
		if prismID == "" && len(fractalIDs) > 0 {
			fid = fractalIDs[0]
		}
		dict, err := h.Dicts.GetDictionaryByName(ctx, fid, prismID, p.Dict)
		if err != nil {
			return nil, fmt.Errorf("tlsh(): dictionary %q: %w", p.Dict, err)
		}
		raw, err = h.Dicts.GetKeys(ctx, dict.ID, tlshMaxNeedles+1)
		if err != nil {
			return nil, fmt.Errorf("tlsh(): read dictionary %q: %w", p.Dict, err)
		}
		if len(raw) > tlshMaxNeedles {
			return nil, fmt.Errorf(
				"tlsh(): dictionary %q holds more than %d digests; comparing every log digest against a corpus that large takes minutes, so use a curated list",
				p.Dict, tlshMaxNeedles)
		}
	}

	needles := make([]tlshNeedle, 0, len(raw))
	skipped := 0
	for _, s := range raw {
		d, err := tlsh.Parse(strings.TrimSpace(s))
		if err != nil {
			// A dictionary is shared, editable data: one bad row must not poison
			// every query that uses it.
			skipped++
			continue
		}
		needles = append(needles, tlshNeedle{label: s, digest: d})
	}
	if len(needles) == 0 && skipped > 0 {
		return nil, fmt.Errorf("tlsh(): dictionary %q holds no valid TLSH digests (%d unusable rows)", p.Dict, skipped)
	}
	return needles, nil
}

// matchTLSH keeps every candidate within threshold of some needle, recording its
// closest one. Parallel across candidates: the comparison is pure and the work is
// candidates x needles, which is the only part that grows with the index.
func matchTLSH(ctx context.Context, candidates []tlshCandidate, needles []tlshNeedle, threshold int) ([]parser.TLSHMatch, error) {
	if len(candidates) == 0 || len(needles) == 0 {
		return nil, nil
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(candidates) {
		workers = len(candidates)
	}
	if workers < 1 {
		workers = 1
	}

	partial := make([][]parser.TLSHMatch, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < len(candidates); i += workers {
				// An abandoned search must not leave every core pinned until the
				// sweep finishes. Checked per stride step, not per comparison: the
				// inner loop is nanoseconds and a ctx read would dominate it.
				if i%1024 == w%1024 && ctx.Err() != nil {
					return
				}
				c := candidates[i]
				best := -1
				bestLabel := ""
				for _, n := range needles {
					// The bound never exceeds the true distance, so a candidate it
					// rejects cannot be a match. It costs a fraction of the full
					// comparison, which is what makes a large needle list workable.
					if c.digest.LowerBound(n.digest) > threshold {
						continue
					}
					d := c.digest.Distance(n.digest)
					if d <= threshold && (best < 0 || d < best) {
						best, bestLabel = d, n.label
						if d == 0 {
							break
						}
					}
				}
				if best >= 0 {
					partial[w] = append(partial[w], parser.TLSHMatch{
						Digest: c.raw, Distance: best, Needle: bestLabel,
					})
				}
			}
		}(w)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var matches []parser.TLSHMatch
	for _, p := range partial {
		matches = append(matches, p...)
	}
	// Closest first, so a cap keeps the strongest matches rather than an arbitrary slice.
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Distance != matches[j].Distance {
			return matches[i].Distance < matches[j].Distance
		}
		return matches[i].Digest < matches[j].Digest
	})
	return matches, nil
}

func tlshMissingIndexError(field string, missing []string, filtered map[string]string, total int, prismID string) error {
	// A model that exists but carries a definition filter indexes only part of its
	// fractal, so it cannot answer for a query whose filters differ. Saying "no
	// index" about a model visible in the UI would send someone in circles.
	var unusable []string
	for _, fid := range missing {
		if name, ok := filtered[fid]; ok {
			unusable = append(unusable, name)
		}
	}
	if len(unusable) > 0 {
		return fmt.Errorf(
			"tlsh(): the TLSH model(s) on field %q (%s) carry a definition filter, so they index only part of their fractal and cannot answer this query. "+
				"Create an unfiltered tlsh model on %q instead",
			field, strings.Join(unusable, ", "), field)
	}
	if prismID != "" && total > 1 {
		return fmt.Errorf(
			"tlsh(): %d of %d fractals in this prism have no TLSH index on %q (%s). "+
				"Create a tlsh analytics model on %q in each, or run the query against a single fractal",
			len(missing), total, field, strings.Join(missing, ", "), field)
	}
	return fmt.Errorf(
		"tlsh(): no TLSH index on field %q. Create a tlsh analytics model keyed on %q "+
			"under Analytics Models, then seed it with a backfill to cover existing history",
		field, field)
}
