package templates

import "context"

type themeContextKeyType struct{}

var themeContextKey = themeContextKeyType{}

// WithTheme associates a theme preference ("light", "dark", "system") with the context.
func WithTheme(ctx context.Context, theme string) context.Context {
	return context.WithValue(ctx, themeContextKey, theme)
}

// ThemeFromContext retrieves the theme from context, defaulting to "system".
func ThemeFromContext(ctx context.Context) string {
	if ctx == nil {
		return "system"
	}
	if v, ok := ctx.Value(themeContextKey).(string); ok && v != "" {
		return v
	}
	return "system"
}
