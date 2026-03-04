package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type adminReplayJobSQLiteStore struct {
	db *sql.DB
}

func newAdminReplayJobSQLiteStore(path string) (*adminReplayJobSQLiteStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("admin replay sqlite path is required")
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create admin replay sqlite directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open admin replay sqlite database: %w", err)
	}
	store := &adminReplayJobSQLiteStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *adminReplayJobSQLiteStore) init() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("admin replay sqlite store is not initialized")
	}
	const schema = `
CREATE TABLE IF NOT EXISTS admin_replay_jobs (
	job_id TEXT PRIMARY KEY,
	status TEXT NOT NULL,
	requested_limit INTEGER NOT NULL,
	effective_limit INTEGER NOT NULL,
	max_limit INTEGER NOT NULL,
	capped INTEGER NOT NULL,
	dry_run INTEGER NOT NULL,
	created_at TEXT NOT NULL,
	started_at TEXT,
	completed_at TEXT,
	replayed INTEGER NOT NULL,
	operator_reason TEXT,
	cancel_reason TEXT,
	error TEXT,
	idempotency_key TEXT,
	creator_ip TEXT,
	creator_token_fingerprint TEXT,
	updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_admin_replay_jobs_status ON admin_replay_jobs(status);
CREATE INDEX IF NOT EXISTS idx_admin_replay_jobs_updated_at ON admin_replay_jobs(updated_at);
CREATE INDEX IF NOT EXISTS idx_admin_replay_jobs_idempotency ON admin_replay_jobs(idempotency_key);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("initialize admin replay sqlite schema: %w", err)
	}
	return nil
}

func (s *adminReplayJobSQLiteStore) Load() ([]*adminReplayJob, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("admin replay sqlite store is not initialized")
	}
	rows, err := s.db.Query(`
SELECT
	job_id,
	status,
	requested_limit,
	effective_limit,
	max_limit,
	capped,
	dry_run,
	created_at,
	started_at,
	completed_at,
	replayed,
	operator_reason,
	cancel_reason,
	error,
	idempotency_key,
	creator_ip,
	creator_token_fingerprint,
	updated_at
FROM admin_replay_jobs`)
	if err != nil {
		return nil, fmt.Errorf("query admin replay jobs: %w", err)
	}
	defer rows.Close()

	out := make([]*adminReplayJob, 0)
	for rows.Next() {
		var (
			jobID          string
			status         string
			requestedLimit int
			effectiveLimit int
			maxLimit       int
			capped         int
			dryRun         int
			createdAtRaw   string
			startedAtRaw   sql.NullString
			completedAtRaw sql.NullString
			replayed       int
			operatorReason sql.NullString
			cancelReason   sql.NullString
			errMsg         sql.NullString
			idempotencyKey sql.NullString
			creatorIP      sql.NullString
			creatorTokenFP sql.NullString
			updatedAtRaw   string
		)
		if err := rows.Scan(
			&jobID,
			&status,
			&requestedLimit,
			&effectiveLimit,
			&maxLimit,
			&capped,
			&dryRun,
			&createdAtRaw,
			&startedAtRaw,
			&completedAtRaw,
			&replayed,
			&operatorReason,
			&cancelReason,
			&errMsg,
			&idempotencyKey,
			&creatorIP,
			&creatorTokenFP,
			&updatedAtRaw,
		); err != nil {
			return nil, fmt.Errorf("scan admin replay job: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(createdAtRaw))
		if err != nil {
			return nil, fmt.Errorf("parse created_at for replay job %q: %w", jobID, err)
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(updatedAtRaw))
		if err != nil {
			return nil, fmt.Errorf("parse updated_at for replay job %q: %w", jobID, err)
		}
		startedAt, err := parseOptionalTimestamp(startedAtRaw)
		if err != nil {
			return nil, fmt.Errorf("parse started_at for replay job %q: %w", jobID, err)
		}
		completedAt, err := parseOptionalTimestamp(completedAtRaw)
		if err != nil {
			return nil, fmt.Errorf("parse completed_at for replay job %q: %w", jobID, err)
		}
		out = append(out, &adminReplayJob{
			snapshot: adminReplayJobSnapshot{
				JobID:          strings.TrimSpace(jobID),
				Status:         strings.TrimSpace(status),
				RequestedLimit: requestedLimit,
				EffectiveLimit: effectiveLimit,
				MaxLimit:       maxLimit,
				Capped:         capped != 0,
				DryRun:         dryRun != 0,
				CreatedAt:      createdAt.UTC(),
				StartedAt:      startedAt.UTC(),
				CompletedAt:    completedAt.UTC(),
				Replayed:       replayed,
				OperatorReason: strings.TrimSpace(operatorReason.String),
				CancelReason:   strings.TrimSpace(cancelReason.String),
				Error:          strings.TrimSpace(errMsg.String),
			},
			idempotencyKey:          strings.TrimSpace(idempotencyKey.String),
			creatorIP:               strings.TrimSpace(creatorIP.String),
			creatorTokenFingerprint: strings.TrimSpace(creatorTokenFP.String),
			updatedAt:               updatedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate admin replay jobs: %w", err)
	}
	return out, nil
}

func (s *adminReplayJobSQLiteStore) Upsert(job *adminReplayJob) error {
	if s == nil || s.db == nil || job == nil {
		return nil
	}
	snapshot := job.snapshot
	_, err := s.db.Exec(`
INSERT INTO admin_replay_jobs (
	job_id,
	status,
	requested_limit,
	effective_limit,
	max_limit,
	capped,
	dry_run,
	created_at,
	started_at,
	completed_at,
	replayed,
	operator_reason,
	cancel_reason,
	error,
	idempotency_key,
	creator_ip,
	creator_token_fingerprint,
	updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO UPDATE SET
	status=excluded.status,
	requested_limit=excluded.requested_limit,
	effective_limit=excluded.effective_limit,
	max_limit=excluded.max_limit,
	capped=excluded.capped,
	dry_run=excluded.dry_run,
	created_at=excluded.created_at,
	started_at=excluded.started_at,
	completed_at=excluded.completed_at,
	replayed=excluded.replayed,
	operator_reason=excluded.operator_reason,
	cancel_reason=excluded.cancel_reason,
	error=excluded.error,
	idempotency_key=excluded.idempotency_key,
	creator_ip=excluded.creator_ip,
	creator_token_fingerprint=excluded.creator_token_fingerprint,
	updated_at=excluded.updated_at
`,
		strings.TrimSpace(snapshot.JobID),
		strings.TrimSpace(snapshot.Status),
		snapshot.RequestedLimit,
		snapshot.EffectiveLimit,
		snapshot.MaxLimit,
		boolToInt(snapshot.Capped),
		boolToInt(snapshot.DryRun),
		snapshot.CreatedAt.UTC().Format(time.RFC3339Nano),
		optionalTimestamp(snapshot.StartedAt),
		optionalTimestamp(snapshot.CompletedAt),
		snapshot.Replayed,
		nullableString(snapshot.OperatorReason),
		nullableString(snapshot.CancelReason),
		nullableString(snapshot.Error),
		nullableString(job.idempotencyKey),
		nullableString(job.creatorIP),
		nullableString(job.creatorTokenFingerprint),
		job.updatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("upsert admin replay job %q: %w", snapshot.JobID, err)
	}
	return nil
}

func (s *adminReplayJobSQLiteStore) Delete(jobID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil
	}
	if _, err := s.db.Exec(`DELETE FROM admin_replay_jobs WHERE job_id = ?`, jobID); err != nil {
		return fmt.Errorf("delete admin replay job %q: %w", jobID, err)
	}
	return nil
}

func (s *adminReplayJobSQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func parseOptionalTimestamp(raw sql.NullString) (time.Time, error) {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw.String))
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func optionalTimestamp(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func nullableString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
