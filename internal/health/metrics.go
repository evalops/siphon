package health

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	WebhooksReceivedTotal            *prometheus.CounterVec
	WebhookVerificationFailuresTotal *prometheus.CounterVec
	WebhookProcessingDurationSeconds *prometheus.HistogramVec
	EventsPublishedTotal             *prometheus.CounterVec
	EventPublishFailuresTotal        *prometheus.CounterVec
	EventsDedupHitsTotal             *prometheus.CounterVec
	NATSPublishRetriesTotal          *prometheus.CounterVec
	NATSPublishRetryDelaySeconds     prometheus.Histogram
	JetStreamAdvisoriesTotal         *prometheus.CounterVec
	ClickHouseDedupSkippedTotal      prometheus.Counter
	AdminRequestsTotal               *prometheus.CounterVec
	AdminRequestDurationSeconds      *prometheus.HistogramVec
	AdminReplayJobsTotal             *prometheus.CounterVec
	AdminReplayJobsInFlight          prometheus.Gauge
	PollerStuck                      *prometheus.GaugeVec
	PollerConsecutiveFailures        *prometheus.GaugeVec
	PollerFetchRequestsTotal         *prometheus.CounterVec
	PollerFetchPagesTotal            *prometheus.CounterVec
	PollerFetchTruncatedTotal        *prometheus.CounterVec
	ProviderHealth                   *prometheus.GaugeVec
	NATSConnected                    prometheus.Gauge
}

var (
	metricsOnce sync.Once
	metricsInst *Metrics
)

func NewMetrics() *Metrics {
	metricsOnce.Do(func() {
		metricsInst = &Metrics{
			WebhooksReceivedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_webhooks_received_total",
				Help: "Received webhooks by provider and status.",
			}, []string{"provider", "status"}),
			WebhookVerificationFailuresTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_webhooks_verification_failures_total",
				Help: "Webhook signature verification failures by provider.",
			}, []string{"provider"}),
			WebhookProcessingDurationSeconds: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "tap_webhooks_processing_duration_seconds",
				Help:    "Webhook processing latency by provider.",
				Buckets: prometheus.DefBuckets,
			}, []string{"provider"}),
			EventsPublishedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_events_published_total",
				Help: "Events published to NATS by provider/entity/action.",
			}, []string{"provider", "entity_type", "action"}),
			EventPublishFailuresTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_events_publish_failures_total",
				Help: "NATS publish failures by provider.",
			}, []string{"provider"}),
			EventsDedupHitsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_events_dedup_hits_total",
				Help: "Events deduplicated by NATS in the dedup window.",
			}, []string{"provider"}),
			NATSPublishRetriesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_nats_publish_retries_total",
				Help: "NATS publish retries by retry classification.",
			}, []string{"reason"}),
			NATSPublishRetryDelaySeconds: promauto.NewHistogram(prometheus.HistogramOpts{
				Name:    "tap_nats_publish_retry_delay_seconds",
				Help:    "Applied retry delay before a NATS publish retry attempt.",
				Buckets: prometheus.DefBuckets,
			}),
			JetStreamAdvisoriesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_jetstream_advisories_total",
				Help: "JetStream advisory events observed by advisory kind.",
			}, []string{"kind"}),
			ClickHouseDedupSkippedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "tap_clickhouse_dedup_skipped_total",
				Help: "Rows skipped by ClickHouse sink due to idempotency dedupe checks.",
			}),
			AdminRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_admin_requests_total",
				Help: "Admin endpoint requests by endpoint and outcome.",
			}, []string{"endpoint", "outcome"}),
			AdminRequestDurationSeconds: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "tap_admin_request_duration_seconds",
				Help:    "Admin endpoint request latency by endpoint and outcome.",
				Buckets: prometheus.DefBuckets,
			}, []string{"endpoint", "outcome"}),
			AdminReplayJobsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_admin_replay_jobs_total",
				Help: "Admin replay job lifecycle transitions by stage.",
			}, []string{"stage"}),
			AdminReplayJobsInFlight: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "tap_admin_replay_jobs_in_flight",
				Help: "Current number of replay jobs actively executing.",
			}),
			PollerStuck: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "tap_poller_stuck",
				Help: "Poller stuck state (1 stuck, 0 healthy) by provider and tenant.",
			}, []string{"provider", "tenant"}),
			PollerConsecutiveFailures: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "tap_poller_consecutive_failures",
				Help: "Current consecutive poller failures by provider and tenant.",
			}, []string{"provider", "tenant"}),
			PollerFetchRequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_poller_fetch_requests_total",
				Help: "Upstream API requests issued by pollers by provider and tenant.",
			}, []string{"provider", "tenant"}),
			PollerFetchPagesTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_poller_fetch_pages_total",
				Help: "Poller response pages processed by provider and tenant.",
			}, []string{"provider", "tenant"}),
			PollerFetchTruncatedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "tap_poller_fetch_truncated_total",
				Help: "Poll cycles truncated due to configured fetch page/request budgets.",
			}, []string{"provider", "tenant"}),
			ProviderHealth: promauto.NewGaugeVec(prometheus.GaugeOpts{
				Name: "tap_provider_health",
				Help: "Provider health state (1 healthy, 0 unhealthy).",
			}, []string{"provider"}),
			NATSConnected: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "tap_nats_connected",
				Help: "NATS connection status (1 connected, 0 disconnected).",
			}),
		}
	})
	return metricsInst
}
