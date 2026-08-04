// bifract-attackgen slims MITRE's ATT&CK STIX bundle into the compact, gzipped
// matrix that pkg/attack embeds. It is a development tool: run it when ATT&CK
// publishes a new version, then commit the regenerated data file.
//
//	go run ./cmd/bifract-attackgen -in enterprise-attack.json -out pkg/attack/data/enterprise-attack.json.gz
//	go run ./cmd/bifract-attackgen -url -out pkg/attack/data/enterprise-attack.json.gz
package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"bifract/pkg/attack"
)

const defaultURL = "https://raw.githubusercontent.com/mitre-attack/attack-stix-data/master/enterprise-attack/enterprise-attack.json"

// Minimal STIX shapes: only the fields the slim matrix needs.
type stixBundle struct {
	Objects []stixObject `json:"objects"`
}

type stixObject struct {
	Type               string       `json:"type"`
	ID                 string       `json:"id"`
	Name               string       `json:"name"`
	Revoked            bool         `json:"revoked"`
	ExternalReferences []stixExtRef `json:"external_references"`
	KillChainPhases    []stixPhase  `json:"kill_chain_phases"`
	SourceRef          string       `json:"source_ref"`
	TargetRef          string       `json:"target_ref"`
	RelationshipType   string       `json:"relationship_type"`
	TacticRefs         []string     `json:"tactic_refs"`
	AnalyticRefs       []string     `json:"x_mitre_analytic_refs"`
	LogSourceRefs      []stixLogSrc `json:"x_mitre_log_source_references"`
	Platforms          []string     `json:"x_mitre_platforms"`
	Shortname          string       `json:"x_mitre_shortname"`
	IsSubtechnique     bool         `json:"x_mitre_is_subtechnique"`
	Deprecated         bool         `json:"x_mitre_deprecated"`
	Domains            []string     `json:"x_mitre_domains"`
	Version            string       `json:"x_mitre_version"`
	DataSources        []string     `json:"x_mitre_data_sources"` // removed in ATT&CK v17+, kept for older bundles
}

type stixExtRef struct {
	SourceName string `json:"source_name"`
	ExternalID string `json:"external_id"`
}

type stixPhase struct {
	KillChainName string `json:"kill_chain_name"`
	PhaseName     string `json:"phase_name"`
}

type stixLogSrc struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
}

func main() {
	in := flag.String("in", "", "path to a local enterprise-attack.json STIX bundle")
	url := flag.String("url", "", "download the bundle from this URL (use -url= for the MITRE default)")
	out := flag.String("out", "pkg/attack/data/enterprise-attack.json.gz", "output path")
	pretty := flag.Bool("pretty", false, "also write an uncompressed .json next to the output for inspection")
	flag.Parse()

	if *in == "" && *url == "" {
		*url = defaultURL
	}
	if *in != "" && *url != "" {
		fatal(fmt.Errorf("-in and -url are mutually exclusive"))
	}

	raw, err := loadBundle(*in, *url)
	if err != nil {
		fatal(err)
	}

	var bundle stixBundle
	if err := json.Unmarshal(raw, &bundle); err != nil {
		fatal(fmt.Errorf("parse STIX bundle: %w", err))
	}

	matrix, err := build(&bundle)
	if err != nil {
		fatal(err)
	}

	encoded, err := json.Marshal(matrix)
	if err != nil {
		fatal(fmt.Errorf("encode matrix: %w", err))
	}

	if err := writeGzip(*out, encoded); err != nil {
		fatal(err)
	}
	if *pretty {
		indented, _ := json.MarshalIndent(matrix, "", "  ")
		if err := os.WriteFile(strings.TrimSuffix(*out, ".gz"), indented, 0o644); err != nil {
			fatal(err)
		}
	}

	info, _ := os.Stat(*out)
	fmt.Printf("ATT&CK %s (%s): %d tactics, %d techniques (%d sub), %d revoked aliases -> %s (%d bytes gz, %d raw)\n",
		matrix.Version, matrix.Domain, len(matrix.Tactics), len(matrix.Techniques),
		countSub(matrix), len(matrix.RevokedBy), *out, info.Size(), len(encoded))
}

func loadBundle(path, url string) ([]byte, error) {
	if path != "" {
		return os.ReadFile(path)
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("download bundle: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download bundle: HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func build(bundle *stixBundle) (*attack.Matrix, error) {
	byID := make(map[string]*stixObject, len(bundle.Objects))
	for i := range bundle.Objects {
		byID[bundle.Objects[i].ID] = &bundle.Objects[i]
	}

	m := &attack.Matrix{Domain: "enterprise-attack"}

	// Version comes from the collection object; the bundle itself carries none.
	for _, o := range bundle.Objects {
		if o.Type == "x-mitre-collection" && o.Version != "" {
			m.Version = o.Version
			break
		}
	}
	if m.Version == "" {
		return nil, fmt.Errorf("no x-mitre-collection version in bundle")
	}

	// Tactic order comes from the matrix object's tactic_refs, which is the
	// left-to-right column order on attack.mitre.org. Sorting any other way
	// would scramble the kill chain.
	var matrixObj *stixObject
	for _, o := range bundle.Objects {
		if o.Type == "x-mitre-matrix" {
			matrixObj = byID[o.ID]
			break
		}
	}
	if matrixObj == nil {
		return nil, fmt.Errorf("no x-mitre-matrix object in bundle")
	}
	for _, ref := range matrixObj.TacticRefs {
		t := byID[ref]
		if t == nil || t.Deprecated || t.Revoked {
			continue
		}
		m.Tactics = append(m.Tactics, attack.Tactic{
			ID:    mitreID(t),
			Short: t.Shortname,
			Name:  t.Name,
		})
	}
	if len(m.Tactics) == 0 {
		return nil, fmt.Errorf("matrix object listed no usable tactics")
	}

	// Revoked techniques are dropped from the grid but kept as aliases so a rule
	// still tagged with a retired ID (Sigma rule sets are full of them) resolves
	// to its replacement instead of silently counting as no coverage.
	m.RevokedBy = map[string]string{}
	parentOf := map[string]string{}
	for _, o := range bundle.Objects {
		if o.Type != "relationship" {
			continue
		}
		src, dst := byID[o.SourceRef], byID[o.TargetRef]
		if src == nil || dst == nil || src.Type != "attack-pattern" {
			continue
		}
		switch o.RelationshipType {
		case "revoked-by":
			if dst.Type == "attack-pattern" {
				m.RevokedBy[mitreID(src)] = mitreID(dst)
			}
		case "subtechnique-of":
			parentOf[mitreID(src)] = mitreID(dst)
		}
	}

	platforms := newStringTable()
	logSources := newStringTable()

	// ATT&CK v17 moved detection guidance out of the technique and into
	// detection-strategy -> analytic -> log source. Walk it back so each
	// technique still knows which telemetry MITRE expects it to be found in.
	techLogSources := collectLogSources(bundle, byID)

	for _, o := range bundle.Objects {
		if o.Type != "attack-pattern" || !inDomain(&o, "enterprise-attack") {
			continue
		}
		id := mitreID(&o)
		if id == "" || o.Revoked {
			continue
		}

		tech := attack.Technique{
			ID:         id,
			Name:       o.Name,
			Sub:        o.IsSubtechnique,
			Parent:     parentOf[id],
			Deprecated: o.Deprecated,
		}
		if tech.Sub && tech.Parent == "" {
			// Fall back to the ID prefix when the relationship is missing.
			if dot := strings.IndexByte(id, '.'); dot > 0 {
				tech.Parent = id[:dot]
			}
		}
		for _, p := range o.KillChainPhases {
			if p.KillChainName == "mitre-attack" {
				tech.Tactics = append(tech.Tactics, p.PhaseName)
			}
		}
		for _, p := range o.Platforms {
			tech.Platforms = append(tech.Platforms, platforms.index(p))
		}
		for _, ls := range techLogSources[o.ID] {
			tech.LogSources = append(tech.LogSources, logSources.index(ls))
		}
		// Pre-v17 bundles carry data sources on the technique itself.
		for _, ds := range o.DataSources {
			tech.LogSources = append(tech.LogSources, logSources.index(ds))
		}
		sort.Ints(tech.Platforms)
		sort.Ints(tech.LogSources)
		tech.LogSources = dedupeInts(tech.LogSources)

		m.Techniques = append(m.Techniques, tech)
	}

	sort.Slice(m.Techniques, func(i, j int) bool { return m.Techniques[i].ID < m.Techniques[j].ID })
	m.Platforms = platforms.values()
	m.LogSources = logSources.values()

	if err := validate(m); err != nil {
		return nil, err
	}
	return m, nil
}

// collectLogSources maps an attack-pattern STIX id to the sorted, deduplicated
// log source names of every analytic that detects it.
func collectLogSources(bundle *stixBundle, byID map[string]*stixObject) map[string][]string {
	out := map[string]map[string]struct{}{}
	for _, o := range bundle.Objects {
		if o.Type != "relationship" || o.RelationshipType != "detects" {
			continue
		}
		strategy, technique := byID[o.SourceRef], byID[o.TargetRef]
		if strategy == nil || technique == nil || technique.Type != "attack-pattern" {
			continue
		}
		if strategy.Type != "x-mitre-detection-strategy" || strategy.Deprecated || strategy.Revoked {
			continue
		}
		for _, aref := range strategy.AnalyticRefs {
			analytic := byID[aref]
			if analytic == nil || analytic.Deprecated || analytic.Revoked {
				continue
			}
			for _, ls := range analytic.LogSourceRefs {
				if ls.Name == "" {
					continue
				}
				if out[technique.ID] == nil {
					out[technique.ID] = map[string]struct{}{}
				}
				out[technique.ID][ls.Name] = struct{}{}
			}
		}
	}

	flat := make(map[string][]string, len(out))
	for tid, set := range out {
		names := make([]string, 0, len(set))
		for n := range set {
			names = append(names, n)
		}
		sort.Strings(names)
		flat[tid] = names
	}
	return flat
}

// validate catches a bad regeneration before it is committed.
func validate(m *attack.Matrix) error {
	tactics := make(map[string]bool, len(m.Tactics))
	for _, t := range m.Tactics {
		if t.ID == "" || t.Short == "" || t.Name == "" {
			return fmt.Errorf("tactic %+v is missing a field", t)
		}
		tactics[t.Short] = true
	}

	ids := make(map[string]bool, len(m.Techniques))
	for _, t := range m.Techniques {
		ids[t.ID] = true
	}
	for _, t := range m.Techniques {
		if len(t.Tactics) == 0 && !t.Deprecated {
			return fmt.Errorf("technique %s (%s) belongs to no tactic", t.ID, t.Name)
		}
		for _, short := range t.Tactics {
			if !tactics[short] {
				return fmt.Errorf("technique %s references unknown tactic %q", t.ID, short)
			}
		}
		if t.Sub && !ids[t.Parent] {
			return fmt.Errorf("sub-technique %s has unknown parent %q", t.ID, t.Parent)
		}
		for _, i := range t.Platforms {
			if i < 0 || i >= len(m.Platforms) {
				return fmt.Errorf("technique %s has out-of-range platform index %d", t.ID, i)
			}
		}
		for _, i := range t.LogSources {
			if i < 0 || i >= len(m.LogSources) {
				return fmt.Errorf("technique %s has out-of-range log source index %d", t.ID, i)
			}
		}
	}
	for from, to := range m.RevokedBy {
		if from == to {
			return fmt.Errorf("technique %s is revoked by itself", from)
		}
	}
	return nil
}

type stringTable struct {
	index_ map[string]int
	list   []string
}

func newStringTable() *stringTable {
	return &stringTable{index_: map[string]int{}}
}

func (s *stringTable) index(v string) int {
	if i, ok := s.index_[v]; ok {
		return i
	}
	i := len(s.list)
	s.index_[v] = i
	s.list = append(s.list, v)
	return i
}

func (s *stringTable) values() []string { return s.list }

func dedupeInts(in []int) []int {
	if len(in) < 2 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func mitreID(o *stixObject) string {
	for _, ref := range o.ExternalReferences {
		if ref.SourceName == "mitre-attack" {
			return ref.ExternalID
		}
	}
	return ""
}

func inDomain(o *stixObject, domain string) bool {
	if len(o.Domains) == 0 {
		return true
	}
	for _, d := range o.Domains {
		if d == domain {
			return true
		}
	}
	return false
}

func countSub(m *attack.Matrix) int {
	n := 0
	for _, t := range m.Techniques {
		if t.Sub {
			n++
		}
	}
	return n
}

func writeGzip(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	zw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return err
	}
	if _, err := zw.Write(data); err != nil {
		return err
	}
	return zw.Close()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "bifract-attackgen:", err)
	os.Exit(1)
}
