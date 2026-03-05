package health

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewMetricsIncludesTransportAndAdvisoryCollectors(t *testing.T) {
	metrics := NewMetrics()
	if metrics == nil {
		t.Fatalf("expected metrics instance")
	}

	metrics.NATSPublishRetriesTotal.WithLabelValues("transport").Inc()
	if got := testutil.ToFloat64(metrics.NATSPublishRetriesTotal.WithLabelValues("transport")); got < 1 {
		t.Fatalf("expected nats publish retries counter to increment, got %f", got)
	}

	metrics.NATSPublishRetryDelaySeconds.Observe(0.1)

	metrics.JetStreamAdvisoriesTotal.WithLabelValues("consumer.created").Inc()
	if got := testutil.ToFloat64(metrics.JetStreamAdvisoriesTotal.WithLabelValues("consumer.created")); got < 1 {
		t.Fatalf("expected jetstream advisory counter to increment, got %f", got)
	}

	metrics.ClickHouseDedupSkippedTotal.Inc()
	if got := testutil.ToFloat64(metrics.ClickHouseDedupSkippedTotal); got < 1 {
		t.Fatalf("expected clickhouse dedupe skipped counter to increment, got %f", got)
	}

	metrics.NATSConnected.Set(1)
	if got := testutil.ToFloat64(metrics.NATSConnected); got != 1 {
		t.Fatalf("expected nats connected gauge to be set, got %f", got)
	}
}
