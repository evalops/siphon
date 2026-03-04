package providers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProviderVerifierHex(t *testing.T) {
	secret := "test-secret"
	body := []byte(`{"ok":true}`)

	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	sig := hex.EncodeToString(h.Sum(nil))

	r := httptest.NewRequest("POST", "/webhooks/generic", nil)
	r.Header.Set("X-Signature", "sha256="+sig)

	v := ProviderVerifier{
		Algorithm:   "sha256",
		SigHeader:   "X-Signature",
		SigEncoding: "prefixed-hex",
		SigPrefix:   "sha256=",
	}
	if err := v.Verify(r, body, secret); err != nil {
		t.Fatalf("expected valid signature, got %v", err)
	}
}

func TestProviderVerifierBase64WithTimestamp(t *testing.T) {
	secret := "test-secret"
	body := []byte("payload")
	now := time.Now().UTC()

	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	sig := base64.StdEncoding.EncodeToString(h.Sum(nil))

	r := httptest.NewRequest("POST", "/webhooks/hubspot", nil)
	r.Header.Set("X-Signature", sig)
	r.Header.Set("X-Timestamp", now.Format(time.RFC3339))

	v := ProviderVerifier{
		Algorithm:       "sha256",
		SigHeader:       "X-Signature",
		SigEncoding:     "base64",
		TimestampHeader: "X-Timestamp",
		MaxAge:          5 * time.Minute,
	}
	if err := v.Verify(r, body, secret); err != nil {
		t.Fatalf("expected valid signature, got %v", err)
	}
}

func TestProviderVerifierRejectOldTimestamp(t *testing.T) {
	secret := "test-secret"
	body := []byte("payload")

	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	sig := base64.StdEncoding.EncodeToString(h.Sum(nil))

	r := httptest.NewRequest("POST", "/webhooks/hubspot", nil)
	r.Header.Set("X-Signature", sig)
	r.Header.Set("X-Timestamp", time.Now().Add(-20*time.Minute).Format(time.RFC3339))

	v := ProviderVerifier{
		Algorithm:       "sha256",
		SigHeader:       "X-Signature",
		SigEncoding:     "base64",
		TimestampHeader: "X-Timestamp",
		MaxAge:          5 * time.Minute,
	}
	if err := v.Verify(r, body, secret); err == nil {
		t.Fatalf("expected timestamp error")
	}
}
