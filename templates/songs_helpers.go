package templates

import "github.com/a-h/templ"

// jsonString encodes v as a JSON string literal for safe embedding in a
// JavaScript expression. It returns an empty JSON string on encoding failure.
func jsonString(v any) string {
	s, err := templ.JSONString(v)
	if err != nil {
		return `""`
	}
	return s
}
