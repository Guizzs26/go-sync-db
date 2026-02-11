package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

const (
	MinBatchSize = 1
	MaxBatchSize = 1000
)

type Config struct {
	DatabaseURL         string
	RabbitMQURL         string
	LogLevel            string
	LogFormat           string
	BatchSize           int
	PollInterval        time.Duration
	MaintenanceInterval time.Duration
}

func Load() *Config {
	_ = godotenv.Load()

	batchSize := getEnvInt("BATCH_SIZE", 100)

	if batchSize > MaxBatchSize {
		slog.Warn("BATCH_SIZE exceeds safety limit. Clamping to maximum", "requested", batchSize, "limit", MaxBatchSize)
		batchSize = MaxBatchSize
	} else if batchSize < MinBatchSize {
		batchSize = MinBatchSize

	}

	return &Config{
		DatabaseURL:         getEnv("DATABASE_URL", "postgres://admin:password@localhost:5432/modern_pax_db"),
		RabbitMQURL:         getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		LogLevel:            getEnv("LOG_LEVEL", "INFO"),
		LogFormat:           getEnv("LOG_FORMAT", "TEXT"),
		BatchSize:           batchSize,
		PollInterval:        time.Duration(getEnvInt("POLL_INTERVAL_SEC", 1)) * time.Second,
		MaintenanceInterval: time.Duration(getEnvInt("MAINTENANCE_INTERVAL_MIN", 5)) * time.Minute,
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}
