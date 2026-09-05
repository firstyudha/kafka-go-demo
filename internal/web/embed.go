// Package web bundles the producer's HTML/CSS/JS assets via //go:embed
// so the binary is self-contained. StaticFS() returns a sub-FS rooted at
// the static/ directory to prevent path traversal to the templates.
package web

import (
	"embed"
	"io/fs"
	"text/template"
)

//go:embed templates static
var FS embed.FS

// Templates is parsed at package init. Panics on malformed template — visible at startup.
var Templates = template.Must(template.ParseFS(FS, "templates/*.html"))

// StaticFS returns an fs.FS rooted at the static/ directory.
// Used by http.FileServer so requests cannot reach templates/.
func StaticFS() fs.FS {
	sub, _ := fs.Sub(FS, "static")
	return sub
}
