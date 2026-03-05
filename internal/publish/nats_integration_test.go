package publish

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/evalops/ensemble-tap/config"
	"github.com/evalops/ensemble-tap/internal/health"
	"github.com/evalops/ensemble-tap/internal/normalize"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNATSPublisherPublishesAndDeduplicates(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_TEST",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "stripe",
		EntityType:      "invoice",
		EntityID:        "in_123",
		Action:          "paid",
		ProviderEventID: "evt_123",
		ProviderTime:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	subject, err := pub.Publish(ctx, evt, "dup_123")
	if err != nil {
		t.Fatalf("publish first message: %v", err)
	}
	if subject != "ensemble.tap.stripe.invoice.paid" {
		t.Fatalf("unexpected subject: %q", subject)
	}

	if _, err := pub.Publish(ctx, evt, "dup_123"); err != nil {
		t.Fatalf("publish duplicate message: %v", err)
	}

	info, err := pub.js.StreamInfo(cfg.Stream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("expected 1 stored message after dedup, got %d", info.State.Msgs)
	}

	if err := pub.Ready(); err != nil {
		t.Fatalf("publisher should be ready: %v", err)
	}
}

func TestNATSPublisherEnsureStreamIsIdempotent(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_IDEMPOTENT",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   time.Minute,
	}

	ctx := context.Background()
	first, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("first publisher: %v", err)
	}
	defer first.Close()

	second, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("second publisher against existing stream: %v", err)
	}
	defer second.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:     "github",
		EntityType:   "issues",
		EntityID:     "7",
		Action:       "opened",
		ProviderTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	if _, err := second.Publish(ctx, evt, "idempotent_1"); err != nil {
		t.Fatalf("publish from second publisher: %v", err)
	}
}

func TestNATSPublisherTenantScopedSubject(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:                  s.ClientURL(),
		Stream:               "ENSEMBLE_TAP_TENANT_SUBJECT",
		SubjectPrefix:        "ensemble.tap",
		TenantScopedSubjects: true,
		MaxAge:               time.Hour,
		DedupWindow:          2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:     "stripe",
		EntityType:   "invoice",
		EntityID:     "in_1",
		Action:       "paid",
		ProviderTime: time.Now().UTC(),
		TenantID:     "tenant-1",
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	subject, err := pub.Publish(ctx, evt, "tenant_subject_1")
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if subject != "ensemble.tap.tenant_1.stripe.invoice.paid" {
		t.Fatalf("unexpected subject: %q", subject)
	}
}

func TestNATSPublisherSetsRequestIDHeader(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_REQ_ID_HEADER",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "stripe",
		EntityType:      "invoice",
		EntityID:        "in_123",
		Action:          "paid",
		ProviderEventID: "evt_123",
		ProviderTime:    time.Now().UTC(),
		RequestID:       "req-nats-1",
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}

	if _, err := pub.Publish(ctx, evt, "req_id_header_1"); err != nil {
		t.Fatalf("publish message: %v", err)
	}

	stored, err := pub.js.GetMsg(cfg.Stream, 1)
	if err != nil {
		t.Fatalf("get stored message: %v", err)
	}
	if got := strings.TrimSpace(stored.Header.Get(natsRequestIDHeader)); got != "req-nats-1" {
		t.Fatalf("expected request id header req-nats-1, got %q", got)
	}
}

func TestNATSPublisherRawInfersRequestIDHeaderFromPayload(t *testing.T) {
	s := runNATSServer(t)
	cfg := config.NATSConfig{
		URL:           s.ClientURL(),
		Stream:        "ENSEMBLE_TAP_RAW_REQ_ID_HEADER",
		SubjectPrefix: "ensemble.tap",
		MaxAge:        time.Hour,
		DedupWindow:   2 * time.Minute,
	}

	ctx := context.Background()
	pub, err := NewNATSPublisher(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	evt, err := normalize.ToCloudEvent(normalize.NormalizedEvent{
		Provider:        "github",
		EntityType:      "issues",
		EntityID:        "7",
		Action:          "opened",
		ProviderEventID: "evt_gh_7",
		ProviderTime:    time.Now().UTC(),
		RequestID:       "req-raw-1",
	})
	if err != nil {
		t.Fatalf("build cloud event: %v", err)
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal cloud event: %v", err)
	}

	if err := pub.PublishRaw(ctx, "ensemble.tap.github.issues.opened", payload, "raw_req_id_1", ""); err != nil {
		t.Fatalf("publish raw message: %v", err)
	}

	stored, err := pub.js.GetMsg(cfg.Stream, 1)
	if err != nil {
		t.Fatalf("get stored message: %v", err)
	}
	if got := strings.TrimSpace(stored.Header.Get(natsRequestIDHeader)); got != "req-raw-1" {
		t.Fatalf("expected request id header req-raw-1, got %q", got)
	}
}

func runNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("create nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("nats server not ready")
	}
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	return s
}

func TestNormalizeNATSRuntimeConfigDefaults(t *testing.T) {
	cfg := normalizeNATSRuntimeConfig(config.NATSConfig{})

	if cfg.ConnectTimeout != 5*time.Second {
		t.Fatalf("expected default connect timeout, got %s", cfg.ConnectTimeout)
	}
	if cfg.ReconnectWait != 2*time.Second {
		t.Fatalf("expected default reconnect wait, got %s", cfg.ReconnectWait)
	}
	if cfg.MaxReconnects != -1 {
		t.Fatalf("expected default max reconnects -1, got %d", cfg.MaxReconnects)
	}
	if cfg.PublishTimeout != 5*time.Second {
		t.Fatalf("expected default publish timeout, got %s", cfg.PublishTimeout)
	}
	if cfg.PublishMaxRetries != 3 {
		t.Fatalf("expected default publish max retries 3, got %d", cfg.PublishMaxRetries)
	}
	if cfg.PublishRetryBackoff != 100*time.Millisecond {
		t.Fatalf("expected default publish retry backoff 100ms, got %s", cfg.PublishRetryBackoff)
	}
	if cfg.StreamReplicas != 1 {
		t.Fatalf("expected default stream replicas 1, got %d", cfg.StreamReplicas)
	}
	if cfg.StreamStorage != "file" {
		t.Fatalf("expected default stream storage file, got %q", cfg.StreamStorage)
	}
	if cfg.StreamDiscard != "old" {
		t.Fatalf("expected default stream discard old, got %q", cfg.StreamDiscard)
	}
}

func TestStreamStorageAndDiscardPolicyMapping(t *testing.T) {
	if got := streamStorageType("memory"); got != nats.MemoryStorage {
		t.Fatalf("expected memory storage mapping, got %v", got)
	}
	if got := streamStorageType("file"); got != nats.FileStorage {
		t.Fatalf("expected file storage mapping, got %v", got)
	}
	if got := streamDiscardPolicy("new"); got != nats.DiscardNew {
		t.Fatalf("expected discard new mapping, got %v", got)
	}
	if got := streamDiscardPolicy("old"); got != nats.DiscardOld {
		t.Fatalf("expected discard old mapping, got %v", got)
	}
}

func TestNATSAuthOptionsSelection(t *testing.T) {
	if opts := natsAuthOptions(config.NATSConfig{}); len(opts) != 0 {
		t.Fatalf("expected no auth options, got %d", len(opts))
	}
	if opts := natsAuthOptions(config.NATSConfig{Username: "u", Password: "p"}); len(opts) != 1 {
		t.Fatalf("expected one user/pass auth option, got %d", len(opts))
	}
	if opts := natsAuthOptions(config.NATSConfig{Token: "token"}); len(opts) != 1 {
		t.Fatalf("expected one token auth option, got %d", len(opts))
	}
	if opts := natsAuthOptions(config.NATSConfig{CredsFile: "/tmp/nats.creds", Token: "token"}); len(opts) != 1 {
		t.Fatalf("expected creds_file auth to take precedence, got %d options", len(opts))
	}
}

func TestNATSTLSOptionsSelection(t *testing.T) {
	opts, err := natsTLSOptions(config.NATSConfig{})
	if err != nil {
		t.Fatalf("expected no tls option error, got %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("expected no tls options, got %d", len(opts))
	}

	opts, err = natsTLSOptions(config.NATSConfig{Secure: true})
	if err != nil {
		t.Fatalf("expected secure tls options without error, got %v", err)
	}
	if len(opts) != 1 {
		t.Fatalf("expected one tls option, got %d", len(opts))
	}

	_, err = natsTLSOptions(config.NATSConfig{Secure: true, CAFile: "/tmp/not-found-ca.pem"})
	if err == nil {
		t.Fatalf("expected missing ca_file to error")
	}

	badCA := t.TempDir() + "/bad-ca.pem"
	if writeErr := os.WriteFile(badCA, []byte("not-a-cert"), 0o600); writeErr != nil {
		t.Fatalf("write bad ca file: %v", writeErr)
	}
	_, err = natsTLSOptions(config.NATSConfig{Secure: true, CAFile: badCA})
	if err == nil {
		t.Fatalf("expected malformed ca_file to error")
	}
}

func TestShouldRetryNATSPublish(t *testing.T) {
	if shouldRetryNATSPublish(nil) {
		t.Fatalf("expected nil error to be non-retryable")
	}
	if shouldRetryNATSPublish(context.Canceled) {
		t.Fatalf("expected canceled context error to be non-retryable")
	}
	if shouldRetryNATSPublish(context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded to be non-retryable")
	}
	if !shouldRetryNATSPublish(errors.New("temporary network")) {
		t.Fatalf("expected generic error to be retryable")
	}
	if shouldRetryNATSPublish(&nats.APIError{Code: 400}) {
		t.Fatalf("expected 4xx API error to be non-retryable")
	}
	if !shouldRetryNATSPublish(&nats.APIError{Code: 503}) {
		t.Fatalf("expected 5xx API error to be retryable")
	}
}

func TestStreamCompressionTypeMapping(t *testing.T) {
	if got := streamCompressionType("s2"); got != nats.S2Compression {
		t.Fatalf("expected s2 compression mapping, got %v", got)
	}
	if got := streamCompressionType("none"); got != nats.NoCompression {
		t.Fatalf("expected none compression mapping, got %v", got)
	}
	if got := streamCompressionType("unknown"); got != nats.NoCompression {
		t.Fatalf("expected default no compression mapping, got %v", got)
	}
}

func TestPublishRetryReason(t *testing.T) {
	if got := publishRetryReason(nil); got != "unknown" {
		t.Fatalf("expected unknown for nil error, got %q", got)
	}
	if got := publishRetryReason(context.Canceled); got != "context" {
		t.Fatalf("expected context classification, got %q", got)
	}
	if got := publishRetryReason(context.DeadlineExceeded); got != "context" {
		t.Fatalf("expected context classification, got %q", got)
	}
	if got := publishRetryReason(&nats.APIError{Code: 503}); got != "api_5xx" {
		t.Fatalf("expected api_5xx classification, got %q", got)
	}
	if got := publishRetryReason(&nats.APIError{Code: 404}); got != "api_4xx" {
		t.Fatalf("expected api_4xx classification, got %q", got)
	}
	if got := publishRetryReason(&nats.APIError{Code: 302}); got != "api_other" {
		t.Fatalf("expected api_other classification, got %q", got)
	}
	if got := publishRetryReason(errors.New("transport failure")); got != "transport" {
		t.Fatalf("expected transport classification, got %q", got)
	}
}

func TestAdvisoryKindFromSubject(t *testing.T) {
	if got := advisoryKindFromSubject("$JS.EVENT.ADVISORY.CONSUMER.CREATED.ENSEMBLE_TAP"); got != "consumer.created" {
		t.Fatalf("expected advisory kind consumer.created, got %q", got)
	}
	if got := advisoryKindFromSubject("$JS.EVENT.ADVISORY.STREAM..ENSEMBLE_TAP"); got != "stream" {
		t.Fatalf("expected advisory kind stream, got %q", got)
	}
	if got := advisoryKindFromSubject("invalid"); got != "unknown" {
		t.Fatalf("expected advisory kind unknown for malformed subject, got %q", got)
	}
}

func TestSubscribeJetStreamAdvisoriesIncrementsMetric(t *testing.T) {
	s := runNATSServer(t)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()

	metrics := health.NewMetrics()
	pub := &NATSPublisher{nc: nc, metrics: metrics}
	before := testutil.ToFloat64(metrics.JetStreamAdvisoriesTotal.WithLabelValues("consumer.created"))

	pub.subscribeJetStreamAdvisories()
	if pub.advSub == nil {
		t.Fatalf("expected advisory subscription to be created")
	}
	defer pub.Close()

	if err := nc.Publish("$JS.EVENT.ADVISORY.CONSUMER.CREATED.ENSEMBLE_TAP", []byte("{}")); err != nil {
		t.Fatalf("publish advisory message: %v", err)
	}
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("flush advisory message: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		after := testutil.ToFloat64(metrics.JetStreamAdvisoriesTotal.WithLabelValues("consumer.created"))
		if after > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected advisory metric to increment for consumer.created")
}

func TestPublishMsgWithRetryReturnsErrorWhenNoStreamMatchesSubject(t *testing.T) {
	s := runNATSServer(t)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream context: %v", err)
	}
	metrics := health.NewMetrics()
	pub := &NATSPublisher{
		cfg: config.NATSConfig{
			PublishMaxRetries:   3,
			PublishRetryBackoff: 10 * time.Millisecond,
		},
		nc:      nc,
		js:      js,
		metrics: metrics,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = pub.publishMsgWithRetry(ctx, &nats.Msg{Subject: "ensemble.tap.unmatched.subject", Data: []byte("payload")})
	if err == nil {
		t.Fatalf("expected publish error when no stream matches subject")
	}
	nc.Close()
}

func TestWaitForClosed(t *testing.T) {
	s := runNATSServer(t)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}

	pub := &NATSPublisher{nc: nc}
	start := time.Now()
	pub.WaitForClosed(40 * time.Millisecond)
	if waited := time.Since(start); waited < 30*time.Millisecond {
		t.Fatalf("expected wait loop to honor timeout for open connection, waited %s", waited)
	}

	nc.Close()
	pub.WaitForClosed(0)
}
