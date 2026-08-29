package aitools

import (
	"context"
	"net/url"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerInstructionTools(d *set) {
	add(d, &mcp.Tool{
		Name:        "list_instruction_libraries",
		Annotations: readOnly(),
		Description: "List this fractal's instruction libraries.\n\n" +
			"Libraries hold the operator's own guidance: playbooks, triage rules, escalation " +
			"procedures. Worth reading at the start of an investigation, since it is where " +
			"environment-specific context lives that no query will tell you.\n\n" +
			"Returns libraries with names, descriptions, page counts, and sync status.",
	}, listInstructionLibraries)

	add(d, &mcp.Tool{
		Name:        "get_instruction_library",
		Annotations: readOnly(),
		Description: "Get a library and the index of its pages.\n\n" +
			"Page content is not included; load pages selectively with read_instruction_page.",
	}, getInstructionLibrary)

	add(d, &mcp.Tool{
		Name:        "read_instruction_page",
		Annotations: readOnly(),
		Description: "Read one instruction page in full: its name, description, and instruction text.",
	}, readInstructionPage)

	add(d, &mcp.Tool{
		Name:        "create_instruction_library",
		Annotations: mutates(),
		Description: "Create an instruction library.",
	}, createInstructionLibrary)

	add(d, &mcp.Tool{
		Name:        "create_instruction_page",
		Annotations: mutates(),
		Description: "Add a page to an instruction library.\n\n" +
			"Pages marked always_include go into every AI conversation's system prompt; the " +
			"rest appear in an index and are loaded on demand. Reserve always_include for " +
			"guidance that applies to every investigation, since it costs context on all of them.",
	}, createInstructionPage)

	add(d, &mcp.Tool{
		Name:        "update_instruction_page",
		Annotations: mutates(),
		Description: "Replace an instruction page's contents.\n\n" +
			"Every field is written, so read the page first unless you intend to overwrite it.",
	}, updateInstructionPage)
}

type libraryIDArgs struct {
	LibraryID string `json:"library_id" jsonschema:"The library UUID."`
}

func listInstructionLibraries(ctx context.Context, c Client, _ noArgs) (any, error) {
	return c.Get(ctx, "/instruction-libraries", nil)
}

func getInstructionLibrary(ctx context.Context, c Client, in libraryIDArgs) (any, error) {
	return c.Get(ctx, "/instruction-libraries/"+url.PathEscape(in.LibraryID), nil)
}

type readInstructionPageArgs struct {
	LibraryID string `json:"library_id" jsonschema:"The library UUID holding the page."`
	PageID    string `json:"page_id" jsonschema:"The page UUID."`
}

func readInstructionPage(ctx context.Context, c Client, in readInstructionPageArgs) (any, error) {
	return c.Get(ctx, "/instruction-libraries/"+url.PathEscape(in.LibraryID)+
		"/pages/"+url.PathEscape(in.PageID), nil)
}

type createInstructionLibraryArgs struct {
	Name        string `json:"name" jsonschema:"Library name, for example 'SOC Playbooks'."`
	Description string `json:"description,omitempty" jsonschema:"What this library covers."`
	IsDefault   bool   `json:"is_default,omitempty" jsonschema:"Make it the fractal's default library. Only one can be default."`
}

func createInstructionLibrary(ctx context.Context, c Client, in createInstructionLibraryArgs) (any, error) {
	return c.Post(ctx, "/instruction-libraries", map[string]any{
		"name":        in.Name,
		"description": in.Description,
		"is_default":  in.IsDefault,
		// A library created here is edited in the UI, never re-synced from a repo.
		"source": "manual",
	})
}

// instructionPage is the body both page writes send. The update endpoint replaces
// the page, so create and update take the same complete set of fields.
type instructionPage struct {
	Name          string `json:"name" jsonschema:"Page name, for example 'Incident Response'."`
	Content       string `json:"content" jsonschema:"The instruction text, plain text or markdown."`
	Description   string `json:"description,omitempty" jsonschema:"Summary shown in the page index."`
	AlwaysInclude bool   `json:"always_include,omitempty" jsonschema:"Always inject this page into AI context."`
	SortOrder     int    `json:"sort_order,omitempty" jsonschema:"Display order; lower sorts first."`
}

func (p instructionPage) body() map[string]any {
	return map[string]any{
		"name":           p.Name,
		"description":    p.Description,
		"content":        p.Content,
		"always_include": p.AlwaysInclude,
		"sort_order":     p.SortOrder,
	}
}

type createInstructionPageArgs struct {
	LibraryID string `json:"library_id" jsonschema:"The library UUID."`
	instructionPage
}

func createInstructionPage(ctx context.Context, c Client, in createInstructionPageArgs) (any, error) {
	return c.Post(ctx, "/instruction-libraries/"+url.PathEscape(in.LibraryID)+"/pages", in.body())
}

type updateInstructionPageArgs struct {
	LibraryID string `json:"library_id" jsonschema:"The library UUID holding the page."`
	PageID    string `json:"page_id" jsonschema:"The page UUID."`
	instructionPage
}

func updateInstructionPage(ctx context.Context, c Client, in updateInstructionPageArgs) (any, error) {
	return c.Put(ctx, "/instruction-libraries/"+url.PathEscape(in.LibraryID)+
		"/pages/"+url.PathEscape(in.PageID), in.body())
}
