package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const sqliteStateSchemaVersion = 1

var sqliteStateMigrations = map[int][]string{
	1: {
		`CREATE TABLE IF NOT EXISTS checkpoints (
			provider TEXT PRIMARY KEY,
			checkpoint TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			provider TEXT NOT NULL,
			entity_type TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			snapshot_json TEXT NOT NULL,
			PRIMARY KEY (provider, entity_type, entity_id)
		)`,
	},
}

type SQLiteStateStore struct {
	db          *sql.DB
	Checkpoints *SQLiteCheckpointStore
	Snapshots   *SQLiteSnapshotStore
}

type SQLiteCheckpointStore struct {
	db *sql.DB
}

type SQLiteSnapshotStore struct {
	db *sql.DB
}

func NewSQLiteStateStore(path string) (*SQLiteStateStore, error) {
	if path == "" {
		path = "tap-state.db"
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)

	store := &SQLiteStateStore{
		db:          db,
		Checkpoints: &SQLiteCheckpointStore{db: db},
		Snapshots:   &SQLiteSnapshotStore{db: db},
	}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStateStore) init() error {
	if err := s.applyPragmas(); err != nil {
		return err
	}
	if err := s.applyMigrations(); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStateStore) applyPragmas() error {
	pragmas := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA foreign_keys = ON`,
	}
	for _, stmt := range pragmas {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("apply sqlite pragma %q: %w", stmt, err)
		}
	}
	return nil
}

func (s *SQLiteStateStore) applyMigrations() error {
	var currentVersion int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&currentVersion); err != nil {
		return fmt.Errorf("read sqlite schema version: %w", err)
	}
	if currentVersion >= sqliteStateSchemaVersion {
		return nil
	}

	for version := currentVersion + 1; version <= sqliteStateSchemaVersion; version++ {
		stmts, ok := sqliteStateMigrations[version]
		if !ok {
			return fmt.Errorf("missing sqlite migration for version %d", version)
		}
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("begin sqlite migration %d: %w", version, err)
		}
		rolledBack := false
		rollback := func(cause error) error {
			if !rolledBack {
				_ = tx.Rollback()
				rolledBack = true
			}
			return cause
		}
		for _, stmt := range stmts {
			if _, err := tx.Exec(stmt); err != nil {
				return rollback(fmt.Errorf("apply sqlite migration %d statement: %w", version, err))
			}
		}
		if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
			return rollback(fmt.Errorf("set sqlite schema version %d: %w", version, err))
		}
		if err := tx.Commit(); err != nil {
			return rollback(fmt.Errorf("commit sqlite migration %d: %w", version, err))
		}
	}
	return nil
}

func (s *SQLiteStateStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteCheckpointStore) Get(provider string) (string, bool) {
	var checkpoint string
	err := s.db.QueryRow(`SELECT checkpoint FROM checkpoints WHERE provider = ?`, provider).Scan(&checkpoint)
	if err != nil {
		return "", false
	}
	return checkpoint, true
}

func (s *SQLiteCheckpointStore) Set(provider, checkpoint string) error {
	_, err := s.db.Exec(`INSERT INTO checkpoints(provider, checkpoint) VALUES(?, ?)
		ON CONFLICT(provider) DO UPDATE SET checkpoint=excluded.checkpoint`, provider, checkpoint)
	if err != nil {
		return fmt.Errorf("upsert checkpoint for provider %q: %w", provider, err)
	}
	return nil
}

func (s *SQLiteSnapshotStore) Get(provider, entityType, entityID string) (map[string]any, bool) {
	var raw string
	err := s.db.QueryRow(`SELECT snapshot_json FROM snapshots WHERE provider=? AND entity_type=? AND entity_id=?`, provider, entityType, entityID).Scan(&raw)
	if err != nil {
		return nil, false
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, false
	}
	return out, true
}

func (s *SQLiteSnapshotStore) Put(provider, entityType, entityID string, snapshot map[string]any) error {
	if snapshot == nil {
		snapshot = map[string]any{}
	}
	b, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot for provider=%q entity_type=%q entity_id=%q: %w", provider, entityType, entityID, err)
	}
	_, err = s.db.Exec(`INSERT INTO snapshots(provider, entity_type, entity_id, snapshot_json) VALUES(?, ?, ?, ?)
		ON CONFLICT(provider, entity_type, entity_id) DO UPDATE SET snapshot_json=excluded.snapshot_json`,
		provider, entityType, entityID, string(b))
	if err != nil {
		return fmt.Errorf("upsert snapshot for provider=%q entity_type=%q entity_id=%q: %w", provider, entityType, entityID, err)
	}
	return nil
}
