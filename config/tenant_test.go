package config

import "testing"

func TestApplyProviderTenantOverridesFields(t *testing.T) {
	base := ProviderConfig{
		Secret:       "default-secret",
		AccessToken:  "default-token",
		ClientSecret: "default-client-secret",
		APIKey:       "default-api-key",
		BaseURL:      "https://default.example.com",
		RealmID:      "realm-default",
		RefreshToken: "refresh-default",
		TenantID:     "base-tenant",
		Tenants: map[string]ProviderTenantConfig{
			"tenant-a": {
				TenantID:     "tenant-a-id",
				Secret:       "tenant-secret",
				AccessToken:  "tenant-token",
				ClientSecret: "tenant-client-secret",
				APIKey:       "tenant-api-key",
				BaseURL:      "https://tenant.example.com",
				RealmID:      "realm-tenant",
				RefreshToken: "refresh-tenant",
			},
		},
	}

	merged := ApplyProviderTenant(base, "tenant-a")
	if merged.TenantID != "tenant-a-id" {
		t.Fatalf("unexpected tenant id: %q", merged.TenantID)
	}
	if merged.Secret != "tenant-secret" || merged.AccessToken != "tenant-token" || merged.ClientSecret != "tenant-client-secret" {
		t.Fatalf("tenant auth values were not applied: %+v", merged)
	}
	if merged.APIKey != "tenant-api-key" || merged.BaseURL != "https://tenant.example.com" || merged.RealmID != "realm-tenant" {
		t.Fatalf("tenant endpoint values were not applied: %+v", merged)
	}
	if merged.RefreshToken != "refresh-tenant" {
		t.Fatalf("unexpected refresh token: %q", merged.RefreshToken)
	}
	if merged.Tenants != nil {
		t.Fatalf("merged config should not carry tenant map")
	}
}

func TestApplyProviderTenantUsesFallbackWhenTenantNotFound(t *testing.T) {
	base := ProviderConfig{
		Secret:   "default-secret",
		Tenants:  map[string]ProviderTenantConfig{"tenant-a": {Secret: "tenant-secret"}},
		TenantID: "base-tenant",
	}

	merged := ApplyProviderTenant(base, "tenant-missing")
	if merged.Secret != "default-secret" {
		t.Fatalf("unexpected secret override: %q", merged.Secret)
	}
	if merged.TenantID != "tenant-missing" {
		t.Fatalf("tenant id should fall back to requested key, got %q", merged.TenantID)
	}
	if merged.Tenants != nil {
		t.Fatalf("merged config should not carry tenant map")
	}
}
