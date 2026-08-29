package aitools

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// maxDictionaryRows caps a read. A dictionary can hold far more, so the answer to
// a wide question is a narrower search, not a bigger page.
const maxDictionaryRows = 200

func registerDictionaryTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "list_dictionaries",
		Annotations: readOnly(),
		Description: "List the dictionaries available in this fractal.\n\n" +
			"A dictionary is a keyed lookup table held alongside the logs: known-bad " +
			"indicators, asset inventories, user-to-team mappings, allow lists. Detections " +
			"consult them instead of hard-coding values, so a watchlist changes without " +
			"touching a query.\n\n" +
			"Read one before writing a detection that refers to it: the key column is what a " +
			"lookup matches on, and the other columns are what a match returns.\n\n" +
			"Returns each dictionary's id, name, key column and row count.",
	}, listDictionaries)

	add(d, &mcp.Tool{
		Name:        "get_dictionary",
		Annotations: readOnly(),
		Description: "Get one dictionary's definition: its columns, key, and how it is populated.\n\n" +
			"This is the definition only; use search_dictionary to read the rows.",
	}, getDictionary)

	add(d, &mcp.Tool{
		Name:        "search_dictionary",
		Annotations: readOnly(),
		Description: "Read rows from a dictionary, optionally filtered.\n\n" +
			"Use this to answer \"is this indicator already on a watchlist\" before " +
			"escalating, and to see what a detection's lookup would return for a given key.\n\n" +
			"Returns the matching rows, each with its key and field values.",
	}, searchDictionary)

	add(d, &mcp.Tool{
		Name:        "add_dictionary_rows",
		Annotations: mutates(),
		Description: "Insert or update rows in a dictionary.\n\n" +
			"This changes what live detections match on, so treat it as a production edit. " +
			"Each row is written by column name, so call get_dictionary first if the columns " +
			"are not already known; a row missing the key column, or naming a column that " +
			"does not exist, is rejected here rather than stored and never matched.\n\n" +
			"Returns how many rows were written.",
	}, addDictionaryRows)
}

func listDictionaries(ctx context.Context, c Client, _ noArgs) (any, error) {
	payload, err := c.Get(ctx, "/dictionaries", nil)
	if err != nil {
		return nil, err
	}
	if _, ok := payload.([]any); !ok {
		return payload, nil
	}
	summaries := summarize(payload, "id", "name", "description", "key_column", "row_count", "is_global")
	return map[string]any{"count": len(summaries), "dictionaries": summaries}, nil
}

type dictionaryIDArgs struct {
	DictionaryID string `json:"dictionary_id" jsonschema:"The dictionary UUID, from list_dictionaries."`
}

func getDictionary(ctx context.Context, c Client, in dictionaryIDArgs) (any, error) {
	return c.Get(ctx, "/dictionaries/"+url.PathEscape(in.DictionaryID), nil)
}

type searchDictionaryArgs struct {
	DictionaryID string `json:"dictionary_id" jsonschema:"The dictionary UUID, from list_dictionaries."`
	Search       string `json:"search,omitempty" jsonschema:"Match rows containing this text. Empty returns the first rows as-is."`
	Limit        int    `json:"limit,omitempty" jsonschema:"How many rows to return, capped at 200. Default 50. A dictionary can hold far more, so filter rather than paging through it."`
}

func searchDictionary(ctx context.Context, c Client, in searchDictionaryArgs) (any, error) {
	query := url.Values{"limit": {strconv.Itoa(clamp(in.Limit, 50, 1, maxDictionaryRows))}}
	if needle := strings.TrimSpace(in.Search); needle != "" {
		query.Set("search", needle)
	}
	return c.Get(ctx, "/dictionaries/"+url.PathEscape(in.DictionaryID)+"/data", query)
}

type addDictionaryRowsArgs struct {
	DictionaryID string              `json:"dictionary_id" jsonschema:"The dictionary UUID, from list_dictionaries."`
	Rows         []map[string]string `json:"rows" jsonschema:"The rows to write. Each row maps column name to value and must include the dictionary's key column; a row already carrying that key is overwritten, not duplicated."`
}

func addDictionaryRows(ctx context.Context, c Client, in addDictionaryRowsArgs) (any, error) {
	if len(in.Rows) == 0 {
		return nil, errors.New("no rows given, so there is nothing to write")
	}

	// The write is by column name, and the API silently drops an unknown column
	// and stores a keyless row that can never match. Caught here instead.
	definition, err := c.Get(ctx, "/dictionaries/"+url.PathEscape(in.DictionaryID), nil)
	if err != nil {
		return nil, err
	}
	keyColumn := Field[string](definition, "key_column")
	if keyColumn == "" {
		return nil, errors.New("this dictionary declares no key column, so a row cannot be keyed")
	}
	known := map[string]bool{}
	var columnNames []string
	for _, column := range Field[[]any](definition, "columns") {
		if name := Field[string](column, "name"); name != "" {
			known[name] = true
			columnNames = append(columnNames, name)
		}
	}

	rows := make([]map[string]any, 0, len(in.Rows))
	for i, row := range in.Rows {
		if strings.TrimSpace(row[keyColumn]) == "" {
			return nil, fmt.Errorf(
				"row %d has no value for the key column %q, so it would be stored but never matched. Columns: %s",
				i, keyColumn, strings.Join(columnNames, ", "))
		}
		for name := range row {
			if !known[name] {
				return nil, fmt.Errorf("row %d sets %q, which this dictionary has no column for. Columns: %s",
					i, name, strings.Join(columnNames, ", "))
			}
		}
		// The API writes from fields alone and ignores key on write, but it reads
		// rows back with it, so both are sent and always agree.
		rows = append(rows, map[string]any{"key": row[keyColumn], "fields": row})
	}
	return c.Post(ctx, "/dictionaries/"+url.PathEscape(in.DictionaryID)+"/data", map[string]any{"rows": rows})
}
