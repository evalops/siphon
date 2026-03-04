package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/dlq"
)

func TestRunStartsAndStopsWithReadyLifecycle(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		Providers: map[string]config.ProviderConfig{
			"acme": {Mode: "webhook", Secret: "webhook-secret", TenantID: "tenant-1"},
		},
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "ENSEMBLE_TAP_CMD_TEST",
			SubjectPrefix: "ensemble.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{Port: port, BasePath: "/webhooks", MaxBodySize: 1 << 20},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	body := []byte(`{"id":"42","timestamp":"2026-03-03T14:22:00Z"}`)
	req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+intToString(port)+"/webhooks/acme", bytes.NewReader(body))
	req.Header.Set("X-Signature", signGeneric(body, "webhook-secret"))
	req.Header.Set("X-Event-Type", "deal.updated")
	req.Header.Set("X-Event-Id", "evt_cmd_1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected webhook accepted, got %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunReadinessReflectsNATSDisconnect(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "ENSEMBLE_TAP_CMD_TEST_READY",
			SubjectPrefix: "ensemble.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{Port: port, BasePath: "/webhooks", MaxBodySize: 1 << 20},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	s.Shutdown()
	s.WaitForShutdown()

	if err := waitForStatusOrError(readyURL, http.StatusServiceUnavailable, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint did not reflect nats disconnect: %v", err)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestSecureTokenEqual(t *testing.T) {
	if secureTokenEqual("", "token") {
		t.Fatalf("expected empty actual token to fail")
	}
	if secureTokenEqual("token", "") {
		t.Fatalf("expected empty expected token to fail")
	}
	if !secureTokenEqual("token", "token") {
		t.Fatalf("expected matching tokens to pass")
	}
	if !secureTokenEqual(" token ", "token") {
		t.Fatalf("expected trimmed tokens to pass")
	}
	if secureTokenEqual("token-1", "token-2") {
		t.Fatalf("expected non-matching tokens to fail")
	}
}

func TestParseReplayDLQLimit(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantReq    int
		wantEff    int
		wantCapped bool
		wantErr    bool
		wantErrSub string
	}{
		{name: "default when empty", raw: "", wantReq: 0, wantEff: defaultReplayDLQLimit, wantCapped: false},
		{name: "custom valid", raw: "25", wantReq: 25, wantEff: 25, wantCapped: false},
		{name: "cap large limit", raw: "99999", wantReq: 99999, wantEff: maxReplayDLQLimit, wantCapped: true},
		{name: "invalid zero", raw: "0", wantErr: true, wantErrSub: "greater than 0"},
		{name: "invalid negative", raw: "-3", wantErr: true, wantErrSub: "greater than 0"},
		{name: "invalid text", raw: "abc", wantErr: true, wantErrSub: "positive integer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, eff, capped, err := parseReplayDLQLimit(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.wantErrSub != "" && !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Fatalf("unexpected error %q", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if req != tt.wantReq || eff != tt.wantEff || capped != tt.wantCapped {
				t.Fatalf("unexpected parse result: req=%d eff=%d capped=%v", req, eff, capped)
			}
		})
	}
}

func TestPollerStatusRegistrySnapshotFiltered(t *testing.T) {
	registry := newPollerStatusRegistry()
	registry.upsert("notion", "tenant-a", 25*time.Millisecond, 9.0, 3, 5, 30*time.Second, 0.2)
	registry.upsert("hubspot", "tenant-b", 50*time.Millisecond, 4.0, 1, 7, 45*time.Second, 0.1)

	if got := registry.SnapshotFiltered("", ""); len(got) != 2 {
		t.Fatalf("expected 2 pollers without filter, got %d", len(got))
	}
	if got := registry.SnapshotFiltered("NOTION", ""); len(got) != 1 || got[0].Provider != "notion" {
		t.Fatalf("expected provider filter to be case-insensitive, got %+v", got)
	}
	if got := registry.SnapshotFiltered("", "tenant-b"); len(got) != 1 || got[0].TenantID != "tenant-b" {
		t.Fatalf("expected tenant filter to match tenant-b, got %+v", got)
	}
	if got := registry.SnapshotFiltered("notion", "tenant-b"); len(got) != 0 {
		t.Fatalf("expected empty result for mismatched provider+tenant filter, got %+v", got)
	}
}

func TestRunAdminReplayEndpointRequiresToken(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)
	cfg := config.Config{
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "ENSEMBLE_TAP_CMD_TEST_REPLAY",
			SubjectPrefix: "ensemble.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:        port,
			BasePath:    "/webhooks",
			MaxBodySize: 1 << 20,
			AdminToken:  "test-admin-token",
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	replayBaseURL := "http://127.0.0.1:" + intToString(port) + "/admin/replay-dlq"
	replayURL := replayBaseURL + "?limit=1"
	reqNoToken, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	respNoToken, err := http.DefaultClient.Do(reqNoToken)
	if err != nil {
		t.Fatalf("request replay without token: %v", err)
	}
	_ = respNoToken.Body.Close()
	if respNoToken.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d", respNoToken.StatusCode)
	}
	reqInvalidLimit, _ := http.NewRequest(http.MethodPost, replayBaseURL+"?limit=invalid", nil)
	reqInvalidLimit.Header.Set("X-Admin-Token", "test-admin-token")
	respInvalidLimit, err := http.DefaultClient.Do(reqInvalidLimit)
	if err != nil {
		t.Fatalf("request replay with invalid limit: %v", err)
	}
	_ = respInvalidLimit.Body.Close()
	if respInvalidLimit.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid replay limit, got %d", respInvalidLimit.StatusCode)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}

	subject := "ensemble.tap.replay.test.updated"
	payload := []byte(`{"id":"replay_1"}`)
	rec := dlq.Record{
		Stage:           "publish",
		Provider:        "test",
		Reason:          "manual replay test",
		OriginalSubject: subject,
		OriginalDedupID: "replay_1",
		OriginalPayload: payload,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal dlq record: %v", err)
	}
	msg := &nats.Msg{
		Subject: "ensemble.dlq.publish.test",
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set(nats.MsgIdHdr, "dlq_test_replay_1")
	if _, err := js.PublishMsg(msg); err != nil {
		t.Fatalf("publish dlq message: %v", err)
	}

	replayedSub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe replay subject: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush nats connection: %v", err)
	}

	reqWithToken, _ := http.NewRequest(http.MethodPost, replayURL, nil)
	reqWithToken.Header.Set("X-Admin-Token", "test-admin-token")
	respWithToken, err := http.DefaultClient.Do(reqWithToken)
	if err != nil {
		t.Fatalf("request replay with token: %v", err)
	}
	if respWithToken.StatusCode != http.StatusOK {
		_ = respWithToken.Body.Close()
		t.Fatalf("expected 200 with admin token, got %d", respWithToken.StatusCode)
	}
	type replayResponse struct {
		Replayed       int  `json:"replayed"`
		RequestedLimit int  `json:"requested_limit"`
		EffectiveLimit int  `json:"effective_limit"`
		MaxLimit       int  `json:"max_limit"`
		Capped         bool `json:"capped"`
	}
	var replayResult replayResponse
	replayBody, err := io.ReadAll(respWithToken.Body)
	_ = respWithToken.Body.Close()
	if err != nil {
		t.Fatalf("read replay response body: %v", err)
	}
	if err := json.Unmarshal(replayBody, &replayResult); err != nil {
		t.Fatalf("decode replay response body: %v", err)
	}
	if replayResult.Replayed != 1 ||
		replayResult.RequestedLimit != 1 ||
		replayResult.EffectiveLimit != 1 ||
		replayResult.MaxLimit != maxReplayDLQLimit ||
		replayResult.Capped {
		t.Fatalf("unexpected replay response: %+v", replayResult)
	}

	got, err := replayedSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected replayed message: %v", err)
	}
	if string(got.Data) != string(payload) {
		t.Fatalf("unexpected replay payload: %s", string(got.Data))
	}
	reqWithCap, _ := http.NewRequest(http.MethodPost, replayBaseURL+"?limit=99999", nil)
	reqWithCap.Header.Set("X-Admin-Token", "test-admin-token")
	respWithCap, err := http.DefaultClient.Do(reqWithCap)
	if err != nil {
		t.Fatalf("request replay with capped limit: %v", err)
	}
	if respWithCap.StatusCode != http.StatusOK {
		_ = respWithCap.Body.Close()
		t.Fatalf("expected 200 with capped limit, got %d", respWithCap.StatusCode)
	}
	var cappedResult replayResponse
	cappedBody, err := io.ReadAll(respWithCap.Body)
	_ = respWithCap.Body.Close()
	if err != nil {
		t.Fatalf("read capped replay response body: %v", err)
	}
	if err := json.Unmarshal(cappedBody, &cappedResult); err != nil {
		t.Fatalf("decode capped replay response body: %v", err)
	}
	if cappedResult.RequestedLimit != 99999 ||
		cappedResult.EffectiveLimit != maxReplayDLQLimit ||
		cappedResult.MaxLimit != maxReplayDLQLimit ||
		!cappedResult.Capped {
		t.Fatalf("unexpected capped replay response: %+v", cappedResult)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func TestRunAdminPollerStatusEndpoint(t *testing.T) {
	s := runNATSServer(t)
	port := freePort(t)

	notionAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/search" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[],"has_more":false,"next_cursor":""}`))
	}))
	defer notionAPI.Close()

	cfg := config.Config{
		Providers: map[string]config.ProviderConfig{
			"notion": {
				Mode:                "poll",
				BaseURL:             notionAPI.URL,
				AccessToken:         "notion-token",
				TenantID:            "tenant-ops",
				PollInterval:        25 * time.Millisecond,
				PollRateLimitPerSec: 9.0,
				PollBurst:           3,
			},
		},
		NATS: config.NATSConfig{
			URL:           s.ClientURL(),
			Stream:        "ENSEMBLE_TAP_CMD_TEST_POLLER_STATUS",
			SubjectPrefix: "ensemble.tap",
			MaxAge:        time.Hour,
			DedupWindow:   time.Minute,
		},
		Server: config.ServerConfig{
			Port:        port,
			BasePath:    "/webhooks",
			MaxBodySize: 1 << 20,
			AdminToken:  "test-admin-token",
		},
	}
	cfg.ApplyDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()

	readyURL := "http://127.0.0.1:" + intToString(port) + "/readyz"
	if err := waitForStatusOrError(readyURL, http.StatusOK, 10*time.Second, errCh); err != nil {
		t.Fatalf("ready endpoint never became healthy: %v", err)
	}

	statusURL := "http://127.0.0.1:" + intToString(port) + "/admin/poller-status"
	reqNoToken, _ := http.NewRequest(http.MethodGet, statusURL, nil)
	respNoToken, err := http.DefaultClient.Do(reqNoToken)
	if err != nil {
		t.Fatalf("request poller status without token: %v", err)
	}
	_ = respNoToken.Body.Close()
	if respNoToken.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without admin token, got %d", respNoToken.StatusCode)
	}

	type pollerStatus struct {
		Provider        string    `json:"provider"`
		TenantID        string    `json:"tenant_id"`
		Interval        string    `json:"interval"`
		RateLimitPerSec float64   `json:"rate_limit_per_sec"`
		Burst           int       `json:"burst"`
		FailureBudget   int       `json:"failure_budget"`
		CircuitBreak    string    `json:"circuit_break_duration"`
		JitterRatio     float64   `json:"jitter_ratio"`
		LastRunAt       time.Time `json:"last_run_at"`
		LastSuccessAt   time.Time `json:"last_success_at"`
		LastError       string    `json:"last_error"`
	}
	type pollerStatusResponse struct {
		Provider string         `json:"provider"`
		Tenant   string         `json:"tenant"`
		Count    int            `json:"count"`
		Pollers  []pollerStatus `json:"pollers"`
	}
	readStatus := func(url string) pollerStatusResponse {
		t.Helper()
		reqWithToken, _ := http.NewRequest(http.MethodGet, url, nil)
		reqWithToken.Header.Set("X-Admin-Token", "test-admin-token")
		respWithToken, err := http.DefaultClient.Do(reqWithToken)
		if err != nil {
			t.Fatalf("request poller status with token: %v", err)
		}
		if respWithToken.StatusCode != http.StatusOK {
			_ = respWithToken.Body.Close()
			t.Fatalf("expected 200 with admin token, got %d", respWithToken.StatusCode)
		}
		body, err := io.ReadAll(respWithToken.Body)
		_ = respWithToken.Body.Close()
		if err != nil {
			t.Fatalf("read status response body: %v", err)
		}
		var out pollerStatusResponse
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("decode status response body: %v", err)
		}
		return out
	}

	var got pollerStatusResponse
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got = readStatus(statusURL)
		if len(got.Pollers) > 0 && !got.Pollers[0].LastRunAt.IsZero() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if got.Count != 1 || len(got.Pollers) != 1 {
		t.Fatalf("expected one poller status, got count=%d len=%d", got.Count, len(got.Pollers))
	}
	status := got.Pollers[0]
	if status.Provider != "notion" || status.TenantID != "tenant-ops" {
		t.Fatalf("unexpected poller identity: %+v", status)
	}
	if status.Interval != "25ms" || status.RateLimitPerSec != 9.0 || status.Burst != 3 {
		t.Fatalf("unexpected poller config status: %+v", status)
	}
	if status.FailureBudget != 5 || status.CircuitBreak != "30s" || status.JitterRatio != 0 {
		t.Fatalf("unexpected poller resilience status: %+v", status)
	}
	if status.LastRunAt.IsZero() || status.LastSuccessAt.IsZero() {
		t.Fatalf("expected poller to have run successfully, got %+v", status)
	}
	if status.LastError != "" {
		t.Fatalf("expected no poller error, got %q", status.LastError)
	}

	filteredProvider := readStatus(statusURL + "?provider=NOTION")
	if filteredProvider.Provider != "NOTION" || filteredProvider.Count != 1 || len(filteredProvider.Pollers) != 1 {
		t.Fatalf("unexpected provider-filter response: %+v", filteredProvider)
	}
	filteredTenant := readStatus(statusURL + "?tenant=missing-tenant")
	if filteredTenant.Tenant != "missing-tenant" || filteredTenant.Count != 0 || len(filteredTenant.Pollers) != 0 {
		t.Fatalf("unexpected tenant-filter response: %+v", filteredTenant)
	}
	filteredCombo := readStatus(statusURL + "?provider=notion&tenant=tenant-ops")
	if filteredCombo.Count != 1 || len(filteredCombo.Pollers) != 1 {
		t.Fatalf("unexpected combined-filter response: %+v", filteredCombo)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run did not stop after cancel")
	}
}

func waitForStatusOrError(url string, want int, timeout time.Duration, errCh <-chan error) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			if err == nil {
				return context.Canceled
			}
			return err
		default:
		}
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func runNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir()}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("nats server not ready")
	}
	t.Cleanup(func() {
		if s.Running() {
			s.Shutdown()
			s.WaitForShutdown()
		}
	})
	return s
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func intToString(v int) string {
	return fmt.Sprintf("%d", v)
}

func signGeneric(body []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
