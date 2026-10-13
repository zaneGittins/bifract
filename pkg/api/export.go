package api

// The log export contract, shared by the server and the bifract CLI.
const (
	// ExportPath is the export route under /api/v1. The /stream suffix exempts it
	// from the request timeout and proxy buffering.
	ExportPath = "/logs/export/stream"
	// ExportRowsTrailer carries the row count once an export completes. A
	// response without it was cut short.
	ExportRowsTrailer = "Bifract-Export-Rows"
)
