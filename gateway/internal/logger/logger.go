package logger

import (
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
)

func New(env string) *slog.Logger {
	var handler slog.Handler
	if env == "dev" {
		handler = tint.NewTextHandler(
			os.Stdout,
			&tint.Options{
				Level:      slog.LevelDebug,
				TimeFormat: time.Kitchen,
				AddSource:  true,
			},
		)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})
	}
	return slog.New(handler)
}
