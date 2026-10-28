// Package exchange lists shareable content (lookups, detections, normalizers and
// policies) from an index file, so an install can browse and add it from the UI.
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
)

// Item kinds. Each maps onto an import the server already has.
const (
	KindLookup     = "lookup"
	KindDetection  = "detection"
	KindNormalizer = "normalizer"
	KindPolicy     = "policy"
)

var kinds = map[string]bool{KindLookup: true, KindDetection: true, KindNormalizer: true, KindPolicy: true}

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// A lookup is installed under its id, which BQL names bare in dict=.
var lookupIDPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Index is the list of items an exchange offers.
type Index struct {
	Items []Item `yaml:"items" json:"items"`
}

// Item is one installable piece of content.
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
	// Normalizer is the id of the normalizer item a Sigma detection's fields map through.
	Normalizer string  `yaml:"normalizer,omitempty" json:"normalizer,omitempty"`
	Lookup     *Lookup `yaml:"lookup,omitempty" json:"lookup,omitempty"`
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

// check drops every item that is invalid, repeats an earlier id or names a
// normalizer the index does not keep, and returns why each was dropped.
func (idx *Index) check() []error {
	var errs []error
	kept := idx.Items[:0]
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

	kept = idx.Items[:0]
	for _, it := range idx.Items {
		if it.Normalizer != "" {
			if n := idx.Find(it.Normalizer); n == nil || n.Kind != KindNormalizer {
				errs = append(errs, fmt.Errorf("item %q: normalizer %q is not a normalizer in this index", it.ID, it.Normalizer))
				continue
			}
		}
		kept = append(kept, it)
	}
	idx.Items = kept
	return errs
}

func (it Item) validate() error {
	if !kinds[it.Kind] {
		return fmt.Errorf("unknown kind %q; expected lookup, detection, normalizer or policy", it.Kind)
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
	if it.Normalizer != "" && it.Kind != KindDetection {
		return fmt.Errorf("only a detection takes a normalizer")
	}
	if it.Kind == KindLookup {
		if !lookupIDPattern.MatchString(it.ID) {
			return fmt.Errorf("lookup id %q must start with a letter and use only letters, digits and _", it.ID)
		}
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
	if it.Lookup != nil {
		return fmt.Errorf("only a lookup takes a lookup section")
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
