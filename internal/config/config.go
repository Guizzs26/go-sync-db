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
	PostgresURL         string // (Relay)
	FirebirdURL         string // (Consumer)
	RabbitMQURL         string
	UnitID              int
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
		PostgresURL:         getEnv("POSTGRES_URL", getEnv("DATABASE_URL", "postgres://test-user:test-pass@localhost:5301/test-db?sslmode=disable")),
		FirebirdURL:         getEnv("FIREBIRD_URL", "SYSDBA:masterkey@localhost:3051/firebird/data/pax.fdb"),
		RabbitMQURL:         getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		UnitID:              getEnvInt("UNIT_ID", 1),
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
