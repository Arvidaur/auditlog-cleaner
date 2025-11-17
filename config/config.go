package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds the application configuration DB and Timing settings taken from .env file
type Config struct {
	Database DatabaseConfig
	Timing   TimingConfig
}

type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

type TimingConfig struct {
	CleanupIntervalSeconds float64
	MaxLogAgeSeconds       int
}

// Load reads configuration from .env file and environment variables
func Load() (*Config, error) {
	// Load .env file (optional - environment variables take precedence)
	_ = godotenv.Load()

	cleanupInterval, err := getEnvAsFloat("CLEANUP_INTERVAL_SECONDS")
	if err != nil {
		return nil, fmt.Errorf("invalid CLEANUP_INTERVAL_SECONDS: %v", err)
	}

	maxLogAge, err := getEnvAsInt("MAX_LOG_AGE_SECONDS")
	if err != nil {
		return nil, fmt.Errorf("invalid MAX_LOG_AGE_SECONDS: %v", err)
	}

	port, err := getEnvAsInt("POSTGRES_PORT")
	if err != nil {
		return nil, fmt.Errorf("invalid POSTGRES_PORT: %v", err)
	}

	config := &Config{
		Database: DatabaseConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     port,
			User:     getEnv("POSTGRES_USER", "user"),
			Password: getEnv("POSTGRES_PASSWORD", "password"),
			DBName:   getEnv("POSTGRES_DB", "auditlogs"),
			SSLMode:  getEnv("POSTGRES_SSL_MODE", "disable"),
		},
		Timing: TimingConfig{
			CleanupIntervalSeconds: cleanupInterval,
			MaxLogAgeSeconds:       maxLogAge,
		},
	}

	return config, nil
}

// ConnectionString returns PostgreSQL connection string
func (c *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode,
	)
}

// Print displays the current configuration
func (c *Config) Print() {
	fmt.Printf("Configuration:\n")
	fmt.Printf("  Database: %s@%s:%d/%s\n", c.Database.User, c.Database.Host, c.Database.Port, c.Database.DBName)
	fmt.Printf("  Cleanup interval: %.1f seconds\n", c.Timing.CleanupIntervalSeconds)
	fmt.Printf("  Max log age: %d seconds\n\n", c.Timing.MaxLogAgeSeconds)
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string) (int, error) {
	valueStr := os.Getenv(key)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value, nil
	}
	return 0, fmt.Errorf("environment variable %s is not a valid int", key)
}

func getEnvAsFloat(key string) (float64, error) {
	valueStr := os.Getenv(key)
	if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return value, nil
	}
	return 0, fmt.Errorf("environment variable %s is not a valid float", key)
}
