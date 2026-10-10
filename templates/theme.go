package templates

import "context"

type themeContextKeyType struct{}

var themeContextKey = themeContextKeyType{}

// WithTheme associates a theme preference ("light", "dark", "system") with the context.
func WithTheme(ctx context.Context, theme string) context.Context {
	return context.WithValue(ctx, themeContextKey, theme)
}

// ThemeFromContext retrieves the theme from context, defaulting to "system".
// Only known values pass through: anything else falls back to "system" so a
// crafted context value can never reach the data-signals JS expression in
// Layout (defense in depth alongside the cookie/handler whitelists).
func ThemeFromContext(ctx context.Context) string {
	if ctx == nil {
		return "system"
	}
	if v, ok := ctx.Value(themeContextKey).(string); ok {
		switch v {
		case "light", "dark", "system":
			return v
		}
	}
	return "system"
}
