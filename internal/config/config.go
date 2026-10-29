package config

import (
	"os"
	"strings"
)

func Brokers() []string {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		raw = "localhost:9092"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func EnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
