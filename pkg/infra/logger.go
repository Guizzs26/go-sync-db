package infra

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

func SetupLogger() *slog.Logger {
	var level slog.Level
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	// MultiWriter: Stdout (Produção/12-Factor) + Arquivo (Estudo Local)
	// Nota: Em produção real, você removeria o logFile e usaria apenas os.Stdout
	logFile, _ := os.OpenFile("relay.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	multiWriter := io.MultiWriter(os.Stdout, logFile)

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}

	if strings.ToUpper(os.Getenv("LOG_FORMAT")) == "JSON" {
		handler = slog.NewJSONHandler(multiWriter, opts)
	} else {
		handler = slog.NewTextHandler(multiWriter, opts)
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)

	return logger
}
