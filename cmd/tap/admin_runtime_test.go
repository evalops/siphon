package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

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

func TestAuthorizeAdminToken(t *testing.T) {
	if ok, slot := authorizeAdminToken("token-a", "token-a", "token-b"); !ok || slot != "primary" {
		t.Fatalf("expected primary token match, got ok=%v slot=%q", ok, slot)
	}
	if ok, slot := authorizeAdminToken("token-b", "token-a", "token-b"); !ok || slot != "secondary" {
		t.Fatalf("expected secondary token match, got ok=%v slot=%q", ok, slot)
	}
	if ok, slot := authorizeAdminToken("token-c", "token-a", "token-b"); ok || slot != "" {
		t.Fatalf("expected unauthorized token, got ok=%v slot=%q", ok, slot)
	}
	if ok, slot := authorizeAdminToken("token-b", "token-a", ""); ok || slot != "" {
		t.Fatalf("expected no secondary match when secondary token not configured, got ok=%v slot=%q", ok, slot)
	}
}

func TestAuthorizeAdminTokenForScope(t *testing.T) {
	if ok, slot := authorizeAdminTokenForScope("primary-token", adminScopeReplay, "primary-token", "next-token", "read-token", "replay-token", "cancel-token"); !ok || slot != "primary" {
		t.Fatalf("expected global primary token to authorize replay scope, got ok=%v slot=%q", ok, slot)
	}
	if ok, slot := authorizeAdminTokenForScope("read-token", adminScopeRead, "", "", "read-token", "replay-token", "cancel-token"); !ok || slot != "read" {
		t.Fatalf("expected read token to authorize read scope, got ok=%v slot=%q", ok, slot)
	}
	if ok, _ := authorizeAdminTokenForScope("read-token", adminScopeReplay, "", "", "read-token", "replay-token", "cancel-token"); ok {
		t.Fatalf("expected read token to be denied for replay scope")
	}
	if ok, slot := authorizeAdminTokenForScope("cancel-token", adminScopeRead, "", "", "read-token", "replay-token", "cancel-token"); !ok || slot != "cancel" {
		t.Fatalf("expected cancel token to authorize read scope, got ok=%v slot=%q", ok, slot)
	}
	if ok, _ := authorizeAdminTokenForScope("replay-token", adminScopeCancel, "", "", "read-token", "replay-token", "cancel-token"); ok {
		t.Fatalf("expected replay token to be denied for cancel scope")
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
			req, eff, capped, err := parseReplayDLQLimit(tt.raw, maxReplayDLQLimit)
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

	req, eff, capped, err := parseReplayDLQLimit("99999", 500)
	if err != nil {
		t.Fatalf("unexpected error with custom max limit: %v", err)
	}
	if req != 99999 || eff != 500 || !capped {
		t.Fatalf("expected custom max cap to apply, got req=%d eff=%d capped=%v", req, eff, capped)
	}

	req, eff, capped, err = parseReplayDLQLimit("", 50)
	if err != nil {
		t.Fatalf("unexpected error with empty limit and low max: %v", err)
	}
	if req != 0 || eff != 50 || !capped {
		t.Fatalf("expected empty limit to respect max cap, got req=%d eff=%d capped=%v", req, eff, capped)
	}
}

func TestParseReplayJobListLimit(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		want       int
		wantErr    bool
		wantErrSub string
	}{
		{name: "default empty", raw: "", want: defaultReplayListLimit},
		{name: "explicit valid", raw: "25", want: 25},
		{name: "capped", raw: "99999", want: maxReplayListLimit},
		{name: "invalid zero", raw: "0", wantErr: true, wantErrSub: "greater than 0"},
		{name: "invalid text", raw: "oops", wantErr: true, wantErrSub: "positive integer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseReplayJobListLimit(tt.raw)
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
			if got != tt.want {
				t.Fatalf("unexpected list limit: got %d want %d", got, tt.want)
			}
		})
	}
}

func TestParseReplayJobStatusFilter(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		want       string
		wantErr    bool
		wantErrSub string
	}{
		{name: "empty all", raw: "", want: ""},
		{name: "queued", raw: "queued", want: adminReplayJobStatusQueued},
		{name: "running uppercase", raw: "RUNNING", want: adminReplayJobStatusRunning},
		{name: "cancelled", raw: "cancelled", want: adminReplayJobStatusCancelled},
		{name: "invalid", raw: "done", wantErr: true, wantErrSub: "invalid status"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseReplayJobStatusFilter(tt.raw)
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
			if got != tt.want {
				t.Fatalf("unexpected status filter: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestParseReplayJobCursor(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	cursor := encodeReplayJobCursor(adminReplayJobSnapshot{
		JobID:     "replay_cursor_1",
		CreatedAt: now,
	})
	createdAt, jobID, err := parseReplayJobCursor(cursor)
	if err != nil {
		t.Fatalf("expected valid replay cursor, got error: %v", err)
	}
	if !createdAt.Equal(now) || jobID != "replay_cursor_1" {
		t.Fatalf("unexpected parsed cursor values: created_at=%s job_id=%s", createdAt, jobID)
	}

	if _, _, err := parseReplayJobCursor("%%%"); err == nil {
		t.Fatalf("expected malformed cursor encoding to fail")
	}
	invalidPayload := base64.RawURLEncoding.EncodeToString([]byte("oops"))
	if _, _, err := parseReplayJobCursor(invalidPayload); err == nil {
		t.Fatalf("expected malformed cursor payload to fail")
	}
	invalidTimestamp := base64.RawURLEncoding.EncodeToString([]byte("not-a-number|replay_1"))
	if _, _, err := parseReplayJobCursor(invalidTimestamp); err == nil {
		t.Fatalf("expected malformed cursor timestamp to fail")
	}
	missingJobID := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d|", now.UnixNano())))
	if _, _, err := parseReplayJobCursor(missingJobID); err == nil {
		t.Fatalf("expected missing cursor job id to fail")
	}
}

func TestParseAdminReason(t *testing.T) {
	if reason, err := parseAdminReason("  investigate dlq drift  ", true, 10); err != nil || reason != "investigate dlq drift" {
		t.Fatalf("expected valid admin reason, got reason=%q err=%v", reason, err)
	}
	if _, err := parseAdminReason("", true, 5); err == nil {
		t.Fatalf("expected required admin reason to fail when missing")
	}
	if reason, err := parseAdminReason("", false, 5); err != nil || reason != "" {
		t.Fatalf("expected optional missing admin reason to pass, got reason=%q err=%v", reason, err)
	}
	if _, err := parseAdminReason("short", true, 10); err == nil {
		t.Fatalf("expected short admin reason to fail min length")
	}
}

func TestAdminRetryAfterSeconds(t *testing.T) {
	tests := []struct {
		limit float64
		want  int
	}{
		{limit: 0, want: 1},
		{limit: 5, want: 1},
		{limit: 1, want: 1},
		{limit: 0.5, want: 2},
		{limit: 0.1, want: 10},
	}
	for _, tt := range tests {
		got := adminRetryAfterSeconds(tt.limit)
		if got != tt.want {
			t.Fatalf("adminRetryAfterSeconds(%v): want %d, got %d", tt.limit, tt.want, got)
		}
	}
}

func TestAdminRateLimiterRegistryIsCallerScoped(t *testing.T) {
	registry := newAdminRateLimiterRegistry(rate.Limit(1), 1)
	if ok, _ := registry.Allow(adminEndpointReplayDLQ, "203.0.113.10", "token-a"); !ok {
		t.Fatalf("expected first replay request to pass")
	}
	if ok, scope := registry.Allow(adminEndpointReplayDLQ, "203.0.113.10", "token-a"); ok || scope != "ip" {
		t.Fatalf("expected second replay request from same ip to be limited by ip scope, got ok=%v scope=%q", ok, scope)
	}
	if ok, _ := registry.Allow(adminEndpointReplayDLQ, "203.0.113.11", "token-b"); !ok {
		t.Fatalf("expected replay request from a different ip to pass")
	}
	if ok, _ := registry.Allow(adminEndpointPollerStatus, "203.0.113.10", "token-a"); !ok {
		t.Fatalf("expected different endpoint key space to pass")
	}
}

func TestAdminAllowlistAndClientCertHelpers(t *testing.T) {
	cidrs, err := parseAdminAllowedCIDRs([]string{"203.0.113.0/24", "198.51.100.10/32"})
	if err != nil {
		t.Fatalf("parse cidrs: %v", err)
	}
	if !adminRequesterAllowed("203.0.113.42", cidrs) {
		t.Fatalf("expected ip in allowlist to be accepted")
	}
	if adminRequesterAllowed("192.0.2.5", cidrs) {
		t.Fatalf("expected ip outside allowlist to be rejected")
	}
	req := httptest.NewRequest(http.MethodGet, "http://example.com/admin/poller-status", nil)
	req.Header.Set("X-Forwarded-Client-Cert", "subject=client-a")
	if !adminHasClientCert(req, "X-Forwarded-Client-Cert") {
		t.Fatalf("expected forwarded client cert header to satisfy mTLS helper")
	}
	if adminHasClientCert(req, "X-Other-Cert") {
		t.Fatalf("expected missing configured cert header to fail mTLS helper")
	}
}

func TestAdminReplayJobRegistryGetOrCreateIdempotent(t *testing.T) {
	registry := newAdminReplayJobRegistry(128, time.Hour)
	base := adminReplayJobSnapshot{
		RequestedLimit: 10,
		EffectiveLimit: 10,
		MaxLimit:       2000,
		Capped:         false,
		DryRun:         true,
	}

	const workers = 24
	results := make([]adminReplayJobSnapshot, workers)
	reused := make([]bool, workers)
	conflicts := make([]bool, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, wasReused, wasConflict := registry.GetOrCreate(base, "idem-replay-atomic")
			results[i] = job
			reused[i] = wasReused
			conflicts[i] = wasConflict
		}()
	}
	wg.Wait()

	firstID := strings.TrimSpace(results[0].JobID)
	if firstID == "" {
		t.Fatalf("expected first replay job id to be populated")
	}

	createdCount := 0
	reusedCount := 0
	for i := 0; i < workers; i++ {
		if conflicts[i] {
			t.Fatalf("expected no idempotency conflict for equivalent requests")
		}
		if strings.TrimSpace(results[i].JobID) != firstID {
			t.Fatalf("expected all equivalent idempotent requests to return same job id")
		}
		if reused[i] {
			reusedCount++
		} else {
			createdCount++
		}
	}
	if createdCount != 1 || reusedCount != workers-1 {
		t.Fatalf("unexpected idempotency counts: created=%d reused=%d", createdCount, reusedCount)
	}

	job, found := registry.GetByIdempotencyKey("idem-replay-atomic")
	if !found || job.JobID != firstID {
		t.Fatalf("expected idempotency key lookup to return created job")
	}
}

func TestAdminReplayJobRegistryGetOrCreateConflict(t *testing.T) {
	registry := newAdminReplayJobRegistry(128, time.Hour)
	baseDryRun := adminReplayJobSnapshot{
		RequestedLimit: 10,
		EffectiveLimit: 10,
		MaxLimit:       2000,
		Capped:         false,
		DryRun:         true,
	}
	initial, reused, conflict := registry.GetOrCreate(baseDryRun, "idem-replay-conflict")
	if reused || conflict {
		t.Fatalf("expected first idempotency call to create job; reused=%v conflict=%v", reused, conflict)
	}

	baseReplay := baseDryRun
	baseReplay.DryRun = false
	next, reused, conflict := registry.GetOrCreate(baseReplay, "idem-replay-conflict")
	if !conflict || reused {
		t.Fatalf("expected conflicting idempotency key use; reused=%v conflict=%v", reused, conflict)
	}
	if next.JobID != initial.JobID {
		t.Fatalf("expected conflict response to reference original job id")
	}

	stored, found := registry.Get(initial.JobID)
	if !found {
		t.Fatalf("expected original job to remain present")
	}
	if !stored.DryRun {
		t.Fatalf("expected original job parameters to remain unchanged")
	}
}

func TestAdminReplayJobRegistryCleansExpiredJobsOnRead(t *testing.T) {
	registry := newAdminReplayJobRegistry(32, 20*time.Millisecond)
	base := adminReplayJobSnapshot{
		RequestedLimit: 1,
		EffectiveLimit: 1,
		MaxLimit:       2000,
		DryRun:         true,
	}
	old, reused, conflict := registry.GetOrCreate(base, "idem-replay-expired")
	if reused || conflict {
		t.Fatalf("expected initial job creation")
	}

	time.Sleep(80 * time.Millisecond)
	if _, found := registry.Get(old.JobID); found {
		t.Fatalf("expected expired replay job lookup by id to be pruned")
	}
	if _, found := registry.GetByIdempotencyKey("idem-replay-expired"); found {
		t.Fatalf("expected expired idempotency mapping to be pruned")
	}

	newJob, reused, conflict := registry.GetOrCreate(base, "idem-replay-expired")
	if reused || conflict {
		t.Fatalf("expected new idempotency create after TTL expiry")
	}
	if newJob.JobID == old.JobID {
		t.Fatalf("expected new replay job id after expiry")
	}
}

func TestAdminReplayJobRegistryList(t *testing.T) {
	registry := newAdminReplayJobRegistry(128, time.Hour)
	base := adminReplayJobSnapshot{
		RequestedLimit: 5,
		EffectiveLimit: 5,
		MaxLimit:       2000,
	}
	first, _, _ := registry.GetOrCreate(base, "idem-list-1")
	second, _, _ := registry.GetOrCreate(base, "idem-list-2")
	registry.MarkSucceeded(first.JobID, 3)
	registry.MarkFailed(second.JobID, "replay error")

	allJobs, summary, nextCursor := registry.List("", 10, time.Time{}, "")
	if len(allJobs) != 2 {
		t.Fatalf("expected list all jobs count=2, got %d", len(allJobs))
	}
	if nextCursor != "" {
		t.Fatalf("expected no next cursor when limit exceeds result set, got %q", nextCursor)
	}
	if summary[adminReplayJobStatusSucceeded] != 1 || summary[adminReplayJobStatusFailed] != 1 {
		t.Fatalf("unexpected replay list summary: %+v", summary)
	}

	succeededJobs, succeededSummary, _ := registry.List(adminReplayJobStatusSucceeded, 10, time.Time{}, "")
	if len(succeededJobs) != 1 || succeededJobs[0].JobID != first.JobID {
		t.Fatalf("expected only succeeded job %q, got %+v", first.JobID, succeededJobs)
	}
	if succeededSummary[adminReplayJobStatusSucceeded] != 1 || succeededSummary[adminReplayJobStatusFailed] != 1 {
		t.Fatalf("expected summary to reflect all statuses, got %+v", succeededSummary)
	}

	limitedJobs, _, limitedNextCursor := registry.List("", 1, time.Time{}, "")
	if len(limitedJobs) != 1 {
		t.Fatalf("expected replay list limit=1 to return 1 job, got %d", len(limitedJobs))
	}
	if strings.TrimSpace(limitedNextCursor) == "" {
		t.Fatalf("expected replay list limit=1 to return next cursor")
	}
	cursorCreatedAt, cursorJobID, err := parseReplayJobCursor(limitedNextCursor)
	if err != nil {
		t.Fatalf("parse list cursor: %v", err)
	}
	secondPage, _, secondNextCursor := registry.List("", 1, cursorCreatedAt, cursorJobID)
	if len(secondPage) != 1 {
		t.Fatalf("expected second replay list page count=1, got %d", len(secondPage))
	}
	if secondPage[0].JobID == limitedJobs[0].JobID {
		t.Fatalf("expected second page job to differ from first page")
	}
	if secondNextCursor != "" {
		t.Fatalf("expected no further cursor on second page, got %q", secondNextCursor)
	}
}

func TestAdminReplayJobRegistryCancelQueued(t *testing.T) {
	registry := newAdminReplayJobRegistry(128, time.Hour)
	base := adminReplayJobSnapshot{
		RequestedLimit: 2,
		EffectiveLimit: 2,
		MaxLimit:       2000,
	}
	queued, _, _ := registry.GetOrCreate(base, "idem-cancel-queued")
	cancelledJob, found, cancelled := registry.CancelQueued(queued.JobID, "manual abort")
	if !found || !cancelled {
		t.Fatalf("expected queued replay job cancellation to succeed, found=%v cancelled=%v", found, cancelled)
	}
	if cancelledJob.Status != adminReplayJobStatusCancelled || !strings.Contains(cancelledJob.Error, "cancelled") {
		t.Fatalf("unexpected cancelled replay job snapshot: %+v", cancelledJob)
	}
	if registry.MarkRunning(queued.JobID) {
		t.Fatalf("expected cancelled replay job to not transition to running")
	}

	running, _, _ := registry.GetOrCreate(base, "idem-cancel-running")
	if !registry.MarkRunning(running.JobID) {
		t.Fatalf("expected running replay job transition")
	}
	runningJob, found, cancelled := registry.CancelQueued(running.JobID, "manual abort")
	if !found || cancelled {
		t.Fatalf("expected running replay job cancel attempt to conflict, found=%v cancelled=%v", found, cancelled)
	}
	if runningJob.Status != adminReplayJobStatusRunning {
		t.Fatalf("expected running replay job status to remain running, got %+v", runningJob)
	}

	_, found, cancelled = registry.CancelQueued("missing-job-id", "manual abort")
	if found || cancelled {
		t.Fatalf("expected missing replay job cancellation to report not found")
	}
}

func TestAdminReplayJobRegistryGetOrCreateWithGuards(t *testing.T) {
	base := adminReplayJobSnapshot{
		RequestedLimit: 5,
		EffectiveLimit: 5,
		MaxLimit:       2000,
	}

	registryIP := newAdminReplayJobRegistry(128, time.Hour)
	var (
		queueScope string
		queueCount int
	)
	_, reused, conflict, queueConflict, _, _ := registryIP.GetOrCreateWithGuards(
		base,
		adminReplayJobCreateMeta{CreatorIP: "203.0.113.10", CreatorTokenFingerprint: "tok-a"},
		"",
		1,
		0,
	)
	if reused || conflict || queueConflict {
		t.Fatalf("expected first guarded create to succeed, reused=%v conflict=%v queueConflict=%v", reused, conflict, queueConflict)
	}
	_, _, _, queueConflict, queueScope, queueCount = registryIP.GetOrCreateWithGuards(
		base,
		adminReplayJobCreateMeta{CreatorIP: "203.0.113.10", CreatorTokenFingerprint: "tok-b"},
		"",
		1,
		0,
	)
	if !queueConflict || queueScope != "ip" || queueCount != 1 {
		t.Fatalf("expected ip queue guard conflict scope=ip count=1, got conflict=%v scope=%q count=%d", queueConflict, queueScope, queueCount)
	}

	registryToken := newAdminReplayJobRegistry(128, time.Hour)
	_, reused, conflict, queueConflict, _, _ = registryToken.GetOrCreateWithGuards(
		base,
		adminReplayJobCreateMeta{CreatorIP: "198.51.100.20", CreatorTokenFingerprint: "tok-z"},
		"",
		0,
		1,
	)
	if reused || conflict || queueConflict {
		t.Fatalf("expected first guarded create for token scope to succeed, reused=%v conflict=%v queueConflict=%v", reused, conflict, queueConflict)
	}
	_, _, _, queueConflict, queueScope, queueCount = registryToken.GetOrCreateWithGuards(
		base,
		adminReplayJobCreateMeta{CreatorIP: "198.51.100.21", CreatorTokenFingerprint: "tok-z"},
		"",
		0,
		1,
	)
	if !queueConflict || queueScope != "token" || queueCount != 1 {
		t.Fatalf("expected token queue guard conflict scope=token count=1, got conflict=%v scope=%q count=%d", queueConflict, queueScope, queueCount)
	}
}

func TestAdminReplayJobRegistrySQLitePersistsAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "admin-replay.db")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	registry, err := newAdminReplayJobRegistryWithBackend(128, time.Hour, "sqlite", dbPath, logger)
	if err != nil {
		t.Fatalf("create sqlite replay registry: %v", err)
	}

	base := adminReplayJobSnapshot{
		RequestedLimit: 3,
		EffectiveLimit: 3,
		MaxLimit:       2000,
	}
	queued, reused, conflict, queueConflict, _, _ := registry.GetOrCreateWithGuards(
		base,
		adminReplayJobCreateMeta{
			OperatorReason:          "replay after incident review",
			CreatorIP:               "203.0.113.15",
			CreatorTokenFingerprint: "token-fp-a",
		},
		"sqlite-idem-1",
		0,
		0,
	)
	if reused || conflict || queueConflict {
		t.Fatalf("expected initial sqlite replay job creation, reused=%v conflict=%v queueConflict=%v", reused, conflict, queueConflict)
	}
	if !registry.MarkRunning(queued.JobID) {
		t.Fatalf("expected queued job to transition to running")
	}
	registry.MarkSucceeded(queued.JobID, 3)

	cancelBase := adminReplayJobSnapshot{
		RequestedLimit: 2,
		EffectiveLimit: 2,
		MaxLimit:       2000,
		DryRun:         true,
	}
	cancelQueued, reused, conflict, queueConflict, _, _ := registry.GetOrCreateWithGuards(
		cancelBase,
		adminReplayJobCreateMeta{
			OperatorReason:          "queue sanity check",
			CreatorIP:               "203.0.113.16",
			CreatorTokenFingerprint: "token-fp-b",
		},
		"sqlite-idem-2",
		0,
		0,
	)
	if reused || conflict || queueConflict {
		t.Fatalf("expected second sqlite replay job creation, reused=%v conflict=%v queueConflict=%v", reused, conflict, queueConflict)
	}
	cancelledSnapshot, found, cancelled := registry.CancelQueued(cancelQueued.JobID, "operator aborted queue")
	if !found || !cancelled {
		t.Fatalf("expected queued replay job cancellation to persist, found=%v cancelled=%v", found, cancelled)
	}
	if cancelledSnapshot.CancelReason != "operator aborted queue" {
		t.Fatalf("expected cancel reason to be set, got %q", cancelledSnapshot.CancelReason)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("close sqlite replay registry: %v", err)
	}

	restored, err := newAdminReplayJobRegistryWithBackend(128, time.Hour, "sqlite", dbPath, logger)
	if err != nil {
		t.Fatalf("restore sqlite replay registry: %v", err)
	}
	defer func() {
		if err := restored.Close(); err != nil {
			t.Fatalf("close restored sqlite replay registry: %v", err)
		}
	}()

	loaded, found := restored.Get(queued.JobID)
	if !found {
		t.Fatalf("expected succeeded replay job to be restored")
	}
	if loaded.Status != adminReplayJobStatusSucceeded || loaded.Replayed != 3 {
		t.Fatalf("unexpected restored succeeded replay job snapshot: %+v", loaded)
	}
	if loaded.OperatorReason != "replay after incident review" {
		t.Fatalf("expected restored operator reason, got %q", loaded.OperatorReason)
	}
	idemLoaded, found := restored.GetByIdempotencyKey("sqlite-idem-1")
	if !found || idemLoaded.JobID != queued.JobID {
		t.Fatalf("expected idempotency lookup to be restored")
	}

	cancelledLoaded, found := restored.Get(cancelQueued.JobID)
	if !found {
		t.Fatalf("expected cancelled replay job to be restored")
	}
	if cancelledLoaded.Status != adminReplayJobStatusCancelled {
		t.Fatalf("expected restored cancelled status, got %q", cancelledLoaded.Status)
	}
	if cancelledLoaded.CancelReason != "operator aborted queue" {
		t.Fatalf("expected restored cancel reason, got %q", cancelledLoaded.CancelReason)
	}
	if !strings.Contains(cancelledLoaded.Error, "cancelled") {
		t.Fatalf("expected restored cancelled error message, got %q", cancelledLoaded.Error)
	}

	next, reused, conflict, queueConflict, _, _ := restored.GetOrCreateWithGuards(
		base,
		adminReplayJobCreateMeta{
			OperatorReason:          "follow-up replay",
			CreatorIP:               "203.0.113.17",
			CreatorTokenFingerprint: "token-fp-c",
		},
		"sqlite-idem-3",
		0,
		0,
	)
	if reused || conflict || queueConflict {
		t.Fatalf("expected new replay job creation after restore, reused=%v conflict=%v queueConflict=%v", reused, conflict, queueConflict)
	}
	if next.JobID == queued.JobID || next.JobID == cancelQueued.JobID {
		t.Fatalf("expected new replay job id after restore, got %q", next.JobID)
	}
}
