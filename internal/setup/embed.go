package setup

import "embed"

//go:embed templates/*
var TemplateFS embed.FS
