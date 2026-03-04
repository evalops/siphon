package config

import (
	"os"
	"path/filepath"
	"strings"
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

func TestLoadConfigMissingFileAppliesDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("load missing config file: %v", err)
	}

	if cfg.NATS.URL != "nats://localhost:4222" {
		t.Fatalf("expected default nats url, got %q", cfg.NATS.URL)
	}
	if cfg.NATS.Stream != "ENSEMBLE_TAP" {
		t.Fatalf("expected default stream, got %q", cfg.NATS.Stream)
	}
	if cfg.Server.Port != 8080 {
		t.Fatalf("expected default server port, got %d", cfg.Server.Port)
	}
	if cfg.Server.BasePath != "/webhooks" {
		t.Fatalf("expected default base path, got %q", cfg.Server.BasePath)
	}
	if cfg.Server.AdminReplayMaxLimit != 2000 {
		t.Fatalf("expected default admin replay max limit 2000, got %d", cfg.Server.AdminReplayMaxLimit)
	}
	if cfg.Server.AdminReplayJobTTL != 24*time.Hour {
		t.Fatalf("expected default admin replay job ttl 24h, got %s", cfg.Server.AdminReplayJobTTL)
	}
	if cfg.Server.AdminReplayJobMaxJobs != 512 {
		t.Fatalf("expected default admin replay job max jobs 512, got %d", cfg.Server.AdminReplayJobMaxJobs)
	}
	if cfg.Server.AdminReplayJobTimeout != 5*time.Minute {
		t.Fatalf("expected default admin replay job timeout 5m, got %s", cfg.Server.AdminReplayJobTimeout)
	}
	if cfg.Server.AdminReplayMaxConcurrent != 2 {
		t.Fatalf("expected default admin replay max concurrent jobs 2, got %d", cfg.Server.AdminReplayMaxConcurrent)
	}
	if cfg.Server.AdminReplayStoreBackend != "memory" {
		t.Fatalf("expected default admin replay store backend memory, got %q", cfg.Server.AdminReplayStoreBackend)
	}
	if cfg.Server.AdminReplaySQLitePath != "tap-admin-replay.db" {
		t.Fatalf("expected default admin replay sqlite path, got %q", cfg.Server.AdminReplaySQLitePath)
	}
	if cfg.Server.AdminReplayReasonMinLen != 12 {
		t.Fatalf("expected default admin replay reason min length 12, got %d", cfg.Server.AdminReplayReasonMinLen)
	}
	if cfg.Server.AdminReplayMaxQueuedPerIP != 100 {
		t.Fatalf("expected default admin replay max queued per ip 100, got %d", cfg.Server.AdminReplayMaxQueuedPerIP)
	}
	if cfg.Server.AdminReplayMaxQueuedToken != 20 {
		t.Fatalf("expected default admin replay max queued per token 20, got %d", cfg.Server.AdminReplayMaxQueuedToken)
	}
	if cfg.Server.AdminRateLimitPerSec != 5.0 {
		t.Fatalf("expected default admin rate limit per sec 5.0, got %v", cfg.Server.AdminRateLimitPerSec)
	}
	if cfg.Server.AdminRateLimitBurst != 20 {
		t.Fatalf("expected default admin rate limit burst 20, got %d", cfg.Server.AdminRateLimitBurst)
	}
	if cfg.Server.AdminMTLSClientCertHeader != "X-Forwarded-Client-Cert" {
		t.Fatalf("expected default mTLS client cert header, got %q", cfg.Server.AdminMTLSClientCertHeader)
	}
}

func TestLoadConfigSnakeCaseEnvOverrides(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")

	t.Setenv("TAP_NATS_SUBJECT_PREFIX", "ensemble.tap.custom")
	t.Setenv("TAP_SERVER_MAX_BODY_SIZE", "2097152")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_LIMIT", "1234")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_JOB_TTL", "12h")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_JOB_MAX_JOBS", "777")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_JOB_TIMEOUT", "2m")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_CONCURRENT_JOBS", "6")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_STORE_BACKEND", "sqlite")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_SQLITE_PATH", "/tmp/tap-admin-replay-test.db")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_REQUIRE_REASON", "true")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_REASON_MIN_LENGTH", "9")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_QUEUED_PER_IP", "55")
	t.Setenv("TAP_SERVER_ADMIN_REPLAY_MAX_QUEUED_PER_TOKEN", "12")
	t.Setenv("TAP_SERVER_ADMIN_RATE_LIMIT_PER_SEC", "2.5")
	t.Setenv("TAP_SERVER_ADMIN_RATE_LIMIT_BURST", "9")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN", "current-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_SECONDARY", "next-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_READ", "read-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_REPLAY", "replay-admin-token")
	t.Setenv("TAP_SERVER_ADMIN_TOKEN_CANCEL", "cancel-admin-token")
	t.Setenv("TAP_CLICKHOUSE_FLUSH_INTERVAL", "3s")
	t.Setenv("TAP_PROVIDERS_STRIPE_SECRET", "whsec_env")
	t.Setenv("TAP_PROVIDERS_HUBSPOT_CLIENT_SECRET", "hs_client_secret")

	cfg, err := Load(missing)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.NATS.SubjectPrefix != "ensemble.tap.custom" {
		t.Fatalf("expected nats.subject_prefix override, got %q", cfg.NATS.SubjectPrefix)
	}
	if cfg.Server.MaxBodySize != 2097152 {
		t.Fatalf("expected server.max_body_size override, got %d", cfg.Server.MaxBodySize)
	}
	if cfg.Server.AdminReplayMaxLimit != 1234 {
		t.Fatalf("expected server.admin_replay_max_limit override, got %d", cfg.Server.AdminReplayMaxLimit)
	}
	if cfg.Server.AdminReplayJobTTL != 12*time.Hour {
		t.Fatalf("expected server.admin_replay_job_ttl override, got %s", cfg.Server.AdminReplayJobTTL)
	}
	if cfg.Server.AdminReplayJobMaxJobs != 777 {
		t.Fatalf("expected server.admin_replay_job_max_jobs override, got %d", cfg.Server.AdminReplayJobMaxJobs)
	}
	if cfg.Server.AdminReplayJobTimeout != 2*time.Minute {
		t.Fatalf("expected server.admin_replay_job_timeout override, got %s", cfg.Server.AdminReplayJobTimeout)
	}
	if cfg.Server.AdminReplayMaxConcurrent != 6 {
		t.Fatalf("expected server.admin_replay_max_concurrent_jobs override, got %d", cfg.Server.AdminReplayMaxConcurrent)
	}
	if cfg.Server.AdminReplayStoreBackend != "sqlite" {
		t.Fatalf("expected server.admin_replay_store_backend override, got %q", cfg.Server.AdminReplayStoreBackend)
	}
	if cfg.Server.AdminReplaySQLitePath != "/tmp/tap-admin-replay-test.db" {
		t.Fatalf("expected server.admin_replay_sqlite_path override, got %q", cfg.Server.AdminReplaySQLitePath)
	}
	if !cfg.Server.AdminReplayRequireReason {
		t.Fatalf("expected server.admin_replay_require_reason override")
	}
	if cfg.Server.AdminReplayReasonMinLen != 9 {
		t.Fatalf("expected server.admin_replay_reason_min_length override, got %d", cfg.Server.AdminReplayReasonMinLen)
	}
	if cfg.Server.AdminReplayMaxQueuedPerIP != 55 {
		t.Fatalf("expected server.admin_replay_max_queued_per_ip override, got %d", cfg.Server.AdminReplayMaxQueuedPerIP)
	}
	if cfg.Server.AdminReplayMaxQueuedToken != 12 {
		t.Fatalf("expected server.admin_replay_max_queued_per_token override, got %d", cfg.Server.AdminReplayMaxQueuedToken)
	}
	if cfg.Server.AdminRateLimitPerSec != 2.5 {
		t.Fatalf("expected server.admin_rate_limit_per_sec override, got %v", cfg.Server.AdminRateLimitPerSec)
	}
	if cfg.Server.AdminRateLimitBurst != 9 {
		t.Fatalf("expected server.admin_rate_limit_burst override, got %d", cfg.Server.AdminRateLimitBurst)
	}
	if cfg.Server.AdminToken != "current-admin-token" {
		t.Fatalf("expected server.admin_token override")
	}
	if cfg.Server.AdminTokenSecondary != "next-admin-token" {
		t.Fatalf("expected server.admin_token_secondary override")
	}
	if cfg.Server.AdminTokenRead != "read-admin-token" {
		t.Fatalf("expected server.admin_token_read override")
	}
	if cfg.Server.AdminTokenReplay != "replay-admin-token" {
		t.Fatalf("expected server.admin_token_replay override")
	}
	if cfg.Server.AdminTokenCancel != "cancel-admin-token" {
		t.Fatalf("expected server.admin_token_cancel override")
	}
	if cfg.ClickHouse.FlushInterval != 3*time.Second {
		t.Fatalf("expected clickhouse.flush_interval override, got %s", cfg.ClickHouse.FlushInterval)
	}
	if cfg.Providers["stripe"].Secret != "whsec_env" {
		t.Fatalf("expected providers.stripe.secret override")
	}
	if cfg.Providers["hubspot"].ClientSecret != "hs_client_secret" {
		t.Fatalf("expected providers.hubspot.client_secret override")
	}
}

func TestConfigValidateAdminTokenAndReplayRules(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantErrSub string
	}{
		{
			name: "secondary token requires primary",
			cfg: Config{
				Server: ServerConfig{
					AdminTokenSecondary: "next-token",
				},
			},
			wantErrSub: "admin_token_secondary requires",
		},
		{
			name: "primary and secondary token must differ",
			cfg: Config{
				Server: ServerConfig{
					AdminToken:          "same-token",
					AdminTokenSecondary: "same-token",
				},
			},
			wantErrSub: "must differ",
		},
		{
			name: "replay max limit must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxLimit: -1,
				},
			},
			wantErrSub: "must be in range",
		},
		{
			name: "replay max limit upper bound enforced",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxLimit: 100001,
				},
			},
			wantErrSub: "must be in range",
		},
		{
			name: "replay job ttl must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayJobTTL: -1 * time.Second,
				},
			},
			wantErrSub: "admin_replay_job_ttl",
		},
		{
			name: "replay job max jobs upper bound enforced",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayJobMaxJobs: 100001,
				},
			},
			wantErrSub: "admin_replay_job_max_jobs",
		},
		{
			name: "replay job timeout must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayJobTimeout: -1 * time.Second,
				},
			},
			wantErrSub: "admin_replay_job_timeout",
		},
		{
			name: "replay max concurrent jobs upper bound enforced",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxConcurrent: 101,
				},
			},
			wantErrSub: "admin_replay_max_concurrent_jobs",
		},
		{
			name: "replay store backend must be valid",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayStoreBackend: "file",
				},
			},
			wantErrSub: "admin_replay_store_backend",
		},
		{
			name: "sqlite replay backend accepts default sqlite path",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayStoreBackend: "sqlite",
				},
			},
		},
		{
			name: "replay reason min length must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayReasonMinLen: -1,
				},
			},
			wantErrSub: "admin_replay_reason_min_length",
		},
		{
			name: "replay queued per ip must be in range",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxQueuedPerIP: -1,
				},
			},
			wantErrSub: "admin_replay_max_queued_per_ip",
		},
		{
			name: "replay queued per token must be in range",
			cfg: Config{
				Server: ServerConfig{
					AdminReplayMaxQueuedToken: -1,
				},
			},
			wantErrSub: "admin_replay_max_queued_per_token",
		},
		{
			name: "admin rate limit per sec must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminRateLimitPerSec: -1,
				},
			},
			wantErrSub: "admin_rate_limit_per_sec",
		},
		{
			name: "admin rate limit burst must be positive",
			cfg: Config{
				Server: ServerConfig{
					AdminRateLimitBurst: -1,
				},
			},
			wantErrSub: "admin_rate_limit_burst",
		},
		{
			name: "admin allowlist CIDR must be valid",
			cfg: Config{
				Server: ServerConfig{
					AdminAllowedCIDRs: []string{"not-a-cidr"},
				},
			},
			wantErrSub: "admin_allowed_cidrs",
		},
		{
			name: "valid token rotation and replay max",
			cfg: Config{
				Server: ServerConfig{
					AdminToken:                "primary-token",
					AdminTokenSecondary:       "next-token",
					AdminReplayMaxLimit:       5000,
					AdminReplayJobTTL:         12 * time.Hour,
					AdminReplayJobMaxJobs:     2048,
					AdminReplayJobTimeout:     2 * time.Minute,
					AdminReplayMaxConcurrent:  4,
					AdminReplayStoreBackend:   "sqlite",
					AdminReplaySQLitePath:     "/tmp/tap-admin-replay.db",
					AdminReplayRequireReason:  true,
					AdminReplayReasonMinLen:   16,
					AdminReplayMaxQueuedPerIP: 80,
					AdminReplayMaxQueuedToken: 10,
					AdminRateLimitPerSec:      3.5,
					AdminRateLimitBurst:       11,
					AdminAllowedCIDRs:         []string{"203.0.113.0/24"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.ApplyDefaults()
			err := cfg.Validate()
			if tt.wantErrSub == "" {
				if err != nil {
					t.Fatalf("unexpected validation error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected validation error containing %q", tt.wantErrSub)
			}
			if !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("expected validation error containing %q, got %q", tt.wantErrSub, err.Error())
			}
		})
	}
}

func TestConfigValidateProviderModesAndCredentials(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		wantErrSub string
	}{
		{
			name: "webhook provider requires secret",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"stripe": {Mode: "webhook"},
				},
			},
			wantErrSub: "providers.stripe webhook mode requires secret",
		},
		{
			name: "hubspot webhook accepts client secret",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"hubspot": {Mode: "webhook", ClientSecret: "hs-client-secret"},
				},
			},
		},
		{
			name: "webhook provider accepts tenant secret override",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"acme": {
						Mode: "webhook",
						Tenants: map[string]ProviderTenantConfig{
							"tenant-a": {Secret: "tenant-secret"},
						},
					},
				},
			},
		},
		{
			name: "unsupported poll provider fails fast",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"stripe": {Mode: "poll", AccessToken: "tok", BaseURL: "https://api.example.com"},
				},
			},
			wantErrSub: "providers.stripe poll mode is not supported",
		},
		{
			name: "salesforce poll requires base url",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"salesforce": {Mode: "poll", AccessToken: "tok"},
				},
			},
			wantErrSub: "providers.salesforce poll target base is invalid: base_url is required",
		},
		{
			name: "quickbooks poll requires realm id",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"quickbooks": {Mode: "poll", AccessToken: "tok", BaseURL: "https://quickbooks.api.intuit.com"},
				},
			},
			wantErrSub: "realm_id is required",
		},
		{
			name: "hubspot poll supports tenant-only credentials",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"hubspot": {
						Mode: "poll",
						Tenants: map[string]ProviderTenantConfig{
							"tenant-a": {
								AccessToken: "tenant-token",
								BaseURL:     "https://api.hubapi.com",
							},
						},
					},
				},
			},
		},
		{
			name: "hubspot poll validates base target when base credentials present",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"hubspot": {
						Mode:        "poll",
						AccessToken: "base-token",
						Tenants: map[string]ProviderTenantConfig{
							"tenant-a": {
								AccessToken: "tenant-token",
								BaseURL:     "https://api.hubapi.com",
							},
						},
					},
				},
			},
			wantErrSub: "providers.hubspot poll target base is invalid: base_url is required",
		},
		{
			name: "notion poll accepts base secret",
			cfg: Config{
				Providers: map[string]ProviderConfig{
					"notion": {Mode: "poll", Secret: "notion-token", BaseURL: "https://api.notion.com"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.ApplyDefaults()
			err := cfg.Validate()
			if tt.wantErrSub == "" {
				if err != nil {
					t.Fatalf("unexpected validation error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected validation error containing %q", tt.wantErrSub)
			}
			if !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("expected validation error containing %q, got %q", tt.wantErrSub, err.Error())
			}
		})
	}
}
