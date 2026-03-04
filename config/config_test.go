package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigWithEnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
providers:
  stripe:
    mode: webhook
    secret: ${STRIPE_WEBHOOK_SECRET}
nats:
  url: nats://localhost:4222
  stream: ENSEMBLE_TAP
  subject_prefix: ensemble.tap
  max_age: 168h
  dedup_window: 2m
server:
  port: 8080
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	t.Setenv("STRIPE_WEBHOOK_SECRET", "whsec_123")
	t.Setenv("TAP_SERVER_PORT", "9091")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got := cfg.Server.Port; got != 9091 {
		t.Fatalf("expected env override port 9091, got %d", got)
	}
	if cfg.Providers["stripe"].Secret != "whsec_123" {
		t.Fatalf("expected secret expansion")
	}
	if cfg.NATS.MaxAge != 168*time.Hour {
		t.Fatalf("expected max_age 168h, got %s", cfg.NATS.MaxAge)
	}
}
