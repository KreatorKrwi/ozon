package configs

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DB struct {
		Host     string
		Port     string
		User     string
		Password string
		Name     string
		SSLMode  string
	}
	Server struct {
		Port string
	}
}

func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
		return nil, err
	}

	cfg := &Config{}

	cfg.DB.Host = getEnv("DB_HOST", "db")
	cfg.DB.Port = getEnv("DB_PORT", "5432")
	cfg.DB.User = getEnv("DB_USER", "user")
	cfg.DB.Password = getEnv("DB_PASSWORD", "password")
	cfg.DB.Name = getEnv("DB_NAME", "wb")
	cfg.DB.SSLMode = getEnv("DB_SSLMODE", "disable")

	cfg.Server.Port = getEnv("SERVER_PORT", "8080")

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
