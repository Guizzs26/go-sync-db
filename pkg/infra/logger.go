package infra

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/Guizzs26/go-sync-db/internal/config"
)

var logFile *os.File

func SetupLogger(cfg *config.Config) *slog.Logger {
	level := parseLogLevel(cfg.LogLevel)

	if logFile != nil {
		logFile.Close()
	}

	var err error
	logFile, err = os.OpenFile("relay.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	var writer io.Writer = os.Stdout
	if err == nil {
		writer = io.MultiWriter(os.Stdout, logFile)
	} else {
		slog.Error("Failed to open log file, using stdout only", "error", err)
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.ToUpper(cfg.LogFormat) == "JSON" {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}

	return slog.New(handler)
}

func CloseLogger() {
	if logFile != nil {
		logFile.Sync()
		logFile.Close()
	}
}

func parseLogLevel(lvl string) slog.Level {
	switch strings.ToUpper(lvl) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
