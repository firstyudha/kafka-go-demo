// Package logger provides a single shared slog.Logger configured for the demo.
// Text format (not JSON) so logs are readable on screen during the webinar.
package logger

import (
	"log/slog"
	"os"
)

// New returns a slog.Logger writing to stdout in text format at Info level.
func New() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}
