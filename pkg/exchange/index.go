// Package exchange lists shareable content (lookups, detections, normalizers,
// policies, models, saved searches, dashboards and bundles of them) from an index
// file, so an install can browse and add it from the UI.
package exchange

import (
	"bytes"
	"fmt"
	"log"
	"path"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"bifract/pkg/egress"
	"bifract/pkg/parser"
)

// Item kinds. Each but a bundle maps onto an import the server already has.
const (
	KindLookup     = "lookup"
	KindDetection  = "detection"
	KindNormalizer = "normalizer"
	KindPolicy     = "policy"
	KindModel      = "model"
	KindSearch     = "search"
	KindDashboard  = "dashboard"
	// KindBundle groups other items so they can be added together.
	KindBundle = "bundle"
)

var kinds = map[string]bool{
	KindLookup: true, KindDetection: true, KindNormalizer: true, KindPolicy: true,
	KindModel: true, KindSearch: true, KindDashboard: true, KindBundle: true,
}

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// Lookups and models are installed under their id, which BQL names bare in dict=
// and model=.
var nameIDPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Index is the list of items an exchange offers.
type Index struct {
	Items []Item `yaml:"items" json:"items"`
}

// Item is one installable piece of content, or a bundle of them.
type Item struct {
	ID          string   `yaml:"id" json:"id"`
	Kind        string   `yaml:"kind" json:"kind"`
	Name        string   `yaml:"name" json:"name"`
	Description string   `yaml:"description" json:"description"`
	Author      string   `yaml:"author,omitempty" json:"author,omitempty"`
	Homepage    string   `yaml:"homepage,omitempty" json:"homepage,omitempty"`
	Tags        []string `yaml:"tags,omitempty" json:"tags,omitempty"`
	// Path is the item's file, relative to the index.
	Path string `yaml:"path,omitempty" json:"path,omitempty"`
	// Requires lists the items this one needs present first. A Sigma detection's
	// fields map through the normalizer it requires.
	Requires []string `yaml:"requires,omitempty" json:"requires,omitempty"`
	// Items are a bundle's members.
	Items  []string `yaml:"items,omitempty" json:"items,omitempty"`
	Lookup *Lookup  `yaml:"lookup,omitempty" json:"lookup,omitempty"`
}

// Lookup is a URL-filled lookup. The data stays upstream; only this is installed.
type Lookup struct {
	// ListKind is the lookup kind: value (the default), network or pattern.
	ListKind string `yaml:"list_kind,omitempty" json:"list_kind,omitempty"`
	Source   Source `yaml:"source" json:"source"`
}

// Source matches the source a lookup is created with.
type Source struct {
	URL            string   `yaml:"url" json:"url"`
	Format         string   `yaml:"format,omitempty" json:"format,omitempty"`
	KeyField       string   `yaml:"key_field" json:"key_field"`
	Columns        []string `yaml:"columns,omitempty" json:"columns,omitempty"`
	SplitKey       string   `yaml:"split_key,omitempty" json:"split_key,omitempty"`
	KeyFormat      string   `yaml:"key_format,omitempty" json:"key_format,omitempty"`
	RefreshMinutes int      `yaml:"refresh_minutes,omitempty" json:"refresh_minutes,omitempty"`
}

// SavedSearch is a search item's file, in the shape a saved query is created with.
type SavedSearch struct {
	Name        string   `yaml:"name" json:"name"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	Query       string   `yaml:"query" json:"query_text"`
	Tags        []string `yaml:"tags,omitempty" json:"tags,omitempty"`
	TimeRange   string   `yaml:"time_range,omitempty" json:"time_range,omitempty"`
}

// ParseSearch reads a search item's file.
func ParseSearch(data []byte) (*SavedSearch, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var s SavedSearch
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("parse search: %w", err)
	}
	s.Query = strings.TrimSpace(s.Query)
	if strings.TrimSpace(s.Name) == "" || s.Query == "" {
		return nil, fmt.Errorf("a search needs a name and a query")
	}
	if _, err := parser.ParseQuery(s.Query); err != nil {
		return nil, fmt.Errorf("search query: %w", err)
	}
	return &s, nil
}

// ParseIndex reads an index strictly: an unknown field or an invalid item fails
// the whole file. The index in this repository is held to it.
func ParseIndex(data []byte) (*Index, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var idx Index
	if err := dec.Decode(&idx); err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}
	if errs := idx.check(); len(errs) > 0 {
		return nil, errs[0]
	}
	return &idx, nil
}

// ReadIndex reads an index served to an install, keeping only the items this
// version can use. Installs read the index from main, so one written for a newer
// Bifract still lists everything an older one understands.
func ReadIndex(data []byte) (*Index, error) {
	var idx Index
	if err := yaml.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}
	for _, err := range idx.check() {
		log.Printf("[Exchange] Skipping %v", err)
	}
	return &idx, nil
}

// Find returns the item with the given id, or nil.
func (idx *Index) Find(id string) *Item {
	for i := range idx.Items {
		if idx.Items[i].ID == id {
			return &idx.Items[i]
		}
	}
	return nil
}

// Normalizer returns the normalizer item a detection requires, or nil.
func (idx *Index) Normalizer(it Item) *Item {
	for _, r := range it.Requires {
		if req := idx.Find(r); req != nil && req.Kind == KindNormalizer {
			return req
		}
	}
	return nil
}

// InstallOrder lists what adding an item adds: its requirements first, each once,
// then the item. A bundle adds each member that way.
func (idx *Index) InstallOrder(id string) []Item {
	var order []Item
	seen := map[string]bool{}
	var visit func(id string)
	visit = func(id string) {
		it := idx.Find(id)
		if it == nil || seen[id] {
			return
		}
		seen[id] = true
		for _, r := range it.Requires {
			visit(r)
		}
		if it.Kind == KindBundle {
			for _, m := range it.Items {
				visit(m)
			}
			return
		}
		order = append(order, *it)
	}
	visit(id)
	return order
}

// check drops every item that is invalid, repeats an earlier id, or refers to an
// item the index does not keep, and returns why each was dropped.
func (idx *Index) check() []error {
	var errs []error
	kept := make([]Item, 0, len(idx.Items))
	seen := make(map[string]bool, len(idx.Items))
	for _, it := range idx.Items {
		var err error
		switch {
		case !idPattern.MatchString(it.ID):
			err = fmt.Errorf("item id %q must be lowercase letters, digits, - or _", it.ID)
		case seen[it.ID]:
			err = fmt.Errorf("duplicate item id %q", it.ID)
		default:
			if err = it.validate(); err != nil {
				err = fmt.Errorf("item %q: %w", it.ID, err)
			}
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		seen[it.ID] = true
		kept = append(kept, it)
	}
	idx.Items = kept

	// Dropping an item can strand what refers to it, so repeat until stable.
	for {
		dropped := false
		kept = make([]Item, 0, len(idx.Items))
		for _, it := range idx.Items {
			if err := idx.checkRefs(&it); err != nil {
				errs = append(errs, fmt.Errorf("item %q: %w", it.ID, err))
				dropped = true
				continue
			}
			kept = append(kept, it)
		}
		idx.Items = kept
		if !dropped {
			break
		}
	}
	for _, it := range idx.Items {
		if idx.cycles(it.ID, map[string]bool{}) {
			errs = append(errs, fmt.Errorf("item %q: its requirements form a cycle", it.ID))
			idx.Items = nil
			break
		}
	}
	return errs
}

// checkRefs checks what an item refers to. A bundle keeps the members still listed
// and fails only when none are left; anything else fails on a missing requirement.
func (idx *Index) checkRefs(it *Item) error {
	normalizers := 0
	for _, r := range it.Requires {
		req := idx.Find(r)
		switch {
		case r == it.ID:
			return fmt.Errorf("requires itself")
		case req == nil:
			return fmt.Errorf("requires %q, which is not in this index", r)
		case req.Kind == KindBundle:
			return fmt.Errorf("requires the bundle %q; require its items instead", r)
		case req.Kind == KindNormalizer:
			normalizers++
		}
	}
	if it.Kind == KindDetection && normalizers > 1 {
		return fmt.Errorf("a detection maps its fields through one normalizer; it requires %d", normalizers)
	}
	if it.Kind != KindBundle {
		return nil
	}
	var members []string
	var missing error
	for _, m := range it.Items {
		switch mem := idx.Find(m); {
		case mem == nil:
			missing = fmt.Errorf("bundle member %q is not in this index", m)
		case mem.Kind == KindBundle:
			return fmt.Errorf("bundle member %q is a bundle; list its items instead", m)
		default:
			members = append(members, m)
		}
	}
	if len(members) == 0 {
		return missing
	}
	if missing != nil {
		log.Printf("[Exchange] Bundle %q: %v", it.ID, missing)
	}
	it.Items = members
	return nil
}

// cycles reports whether following an item's requirements leads back to it.
func (idx *Index) cycles(id string, path map[string]bool) bool {
	if path[id] {
		return true
	}
	it := idx.Find(id)
	if it == nil {
		return false
	}
	path[id] = true
	defer delete(path, id)
	for _, r := range it.Requires {
		if idx.cycles(r, path) {
			return true
		}
	}
	return false
}

func (it Item) validate() error {
	if !kinds[it.Kind] {
		return fmt.Errorf("unknown kind %q", it.Kind)
	}
	if strings.TrimSpace(it.Name) == "" {
		return fmt.Errorf("name is required")
	}
	// The UI links the homepage, so it must not be a script URL.
	if it.Homepage != "" {
		if err := egress.ValidateURL(it.Homepage); err != nil {
			return fmt.Errorf("homepage: %w", err)
		}
	}
	if (it.Kind == KindLookup || it.Kind == KindModel) && !nameIDPattern.MatchString(it.ID) {
		return fmt.Errorf("a %s is installed under its id, so %q must start with a letter and use only letters, digits and _", it.Kind, it.ID)
	}
	if len(it.Items) > 0 && it.Kind != KindBundle {
		return fmt.Errorf("only a bundle lists items")
	}
	if it.Lookup != nil && it.Kind != KindLookup {
		return fmt.Errorf("only a lookup takes a lookup section")
	}
	switch it.Kind {
	case KindBundle:
		if it.Path != "" || len(it.Requires) > 0 {
			return fmt.Errorf("a bundle has no path or requirements; its items carry them")
		}
		if len(it.Items) == 0 {
			return fmt.Errorf("a bundle needs items")
		}
		return nil
	case KindLookup:
		if it.Path != "" {
			return fmt.Errorf("a lookup has no path; its data comes from lookup.source.url")
		}
		if it.Lookup == nil {
			return fmt.Errorf("a lookup needs a lookup section")
		}
		if err := egress.ValidateURL(it.Lookup.Source.URL); err != nil {
			return fmt.Errorf("lookup source url: %w", err)
		}
		if strings.TrimSpace(it.Lookup.Source.KeyField) == "" {
			return fmt.Errorf("lookup source needs a key_field")
		}
		return nil
	}
	return validPath(it.Path)
}

// pathPattern admits plain file paths only: no escapes a server could decode
// into another location, no query and no fragment.
var pathPattern = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._/-]*$`)

// validPath keeps a file inside the directory the index is served from.
func validPath(p string) error {
	if p == "" {
		return fmt.Errorf("path is required")
	}
	ok := pathPattern.MatchString(p) && path.Clean(p) == p
	for _, seg := range strings.Split(p, "/") {
		ok = ok && seg != "." && seg != ".."
	}
	if !ok {
		return fmt.Errorf("path %q must be a plain relative path inside the exchange", p)
	}
	return nil
}
