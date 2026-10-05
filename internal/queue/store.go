package queue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close()                         { s.Pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// API and workers may start together; serialize their idempotent bootstrap.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('workq-schema:' || current_schema()))`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const columns = `id::text, kind, payload, state, attempts, max_attempts, run_at,
    lease_until, last_error, result, created_at, updated_at, COALESCE(lease_token::text, '')`

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Kind, &j.Payload, &j.State, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LeaseUntil, &j.LastError, &j.Result, &j.CreatedAt, &j.UpdatedAt, &j.LeaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

func (s *Store) Enqueue(ctx context.Context, key string, req Request) (Job, bool, error) {
	if len(key) == 0 || len(key) > 128 {
		return Job{}, false, errors.New("idempotency key must contain 1 to 128 bytes")
	}
	if req.MaxAttempts < 1 || req.MaxAttempts > 10 || req.Kind == "" || !json.Valid(req.Payload) {
		return Job{}, false, errors.New("invalid job request")
	}
	hash, err := Fingerprint(req)
	if err != nil {
		return Job{}, false, err
	}
	id, err := NewID()
	if err != nil {
		return Job{}, false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `INSERT INTO jobs (id, idempotency_key, fingerprint, kind, payload, max_attempts)
        VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (idempotency_key) DO NOTHING`, id, key, hash, req.Kind, req.Payload, req.MaxAttempts)
	if err != nil {
		return Job{}, false, err
	}
	var existingHash string
	if err := tx.QueryRow(ctx, `SELECT fingerprint FROM jobs WHERE idempotency_key=$1`, key).Scan(&existingHash); err != nil {
		return Job{}, false, err
	}
	if existingHash != hash {
		return Job{}, false, ErrConflict
	}
	job, err := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE idempotency_key=$1`, key))
	if err != nil {
		return Job{}, false, err
	}
	return job, result.RowsAffected() == 1, tx.Commit(ctx)
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	return scanJob(s.Pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1`, id))
}

func (s *Store) List(ctx context.Context, state string, limit int) ([]Job, error) {
	if limit < 1 || limit > 100 {
		limit = 50
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+columns+` FROM jobs WHERE ($1='' OR state=$1) ORDER BY created_at DESC, id LIMIT $2`, state, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) Stats(ctx context.Context) (map[string]int64, error) {
	result := map[string]int64{"queued": 0, "running": 0, "succeeded": 0, "dead": 0}
	rows, err := s.Pool.Query(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		result[state] = count
	}
	return result, rows.Err()
}

// A claim changes state and installs a fresh token in one statement. SKIP LOCKED
// lets other workers claim different rows while this transaction holds its lock.
func (s *Store) Claim(ctx context.Context, lease time.Duration) (Job, error) {
	token, err := NewID()
	if err != nil {
		return Job{}, err
	}
	return scanJob(s.Pool.QueryRow(ctx, `WITH candidate AS (
        SELECT id FROM jobs WHERE state='queued' AND run_at <= now()
        ORDER BY run_at, created_at, id FOR UPDATE SKIP LOCKED LIMIT 1
    ) UPDATE jobs SET state='running', attempts=attempts+1, lease_token=$1,
        lease_until=now()+($2 * interval '1 millisecond'), updated_at=now()
        WHERE id=(SELECT id FROM candidate) RETURNING `+columns, token, lease.Milliseconds()))
}

func leaseResult(affected int64, err error) error {
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context, j Job, lease time.Duration) error {
	result, err := s.Pool.Exec(ctx, `UPDATE jobs SET lease_until=now()+($3 * interval '1 millisecond'), updated_at=now()
        WHERE id=$1 AND lease_token=$2 AND state='running' AND lease_until > now()`, j.ID, j.LeaseToken, lease.Milliseconds())
	return leaseResult(result.RowsAffected(), err)
}

func (s *Store) Complete(ctx context.Context, j Job, resultJSON json.RawMessage) error {
	result, err := s.Pool.Exec(ctx, `UPDATE jobs SET state='succeeded', result=$3, last_error=NULL,
        lease_token=NULL, lease_until=NULL, updated_at=now()
        WHERE id=$1 AND lease_token=$2 AND state='running' AND lease_until > now()`, j.ID, j.LeaseToken, resultJSON)
	return leaseResult(result.RowsAffected(), err)
}

func (s *Store) Fail(ctx context.Context, j Job, reason string, delay time.Duration) error {
	if len(reason) > 1024 {
		reason = reason[:1024]
	}
	result, err := s.Pool.Exec(ctx, `UPDATE jobs SET state=CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
        run_at=now()+($4 * interval '1 millisecond'), last_error=$3, lease_token=NULL, lease_until=NULL, updated_at=now()
        WHERE id=$1 AND lease_token=$2 AND state='running' AND lease_until > now()`, j.ID, j.LeaseToken, reason, delay.Milliseconds())
	return leaseResult(result.RowsAffected(), err)
}

// Recovery invalidates the previous token before making a job visible again.
// A worker whose old process resumes cannot acknowledge the new owner's work.
func (s *Store) RecoverExpired(ctx context.Context) (int64, error) {
	result, err := s.Pool.Exec(ctx, `UPDATE jobs SET state=CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
        run_at=now(), last_error='worker lease expired', lease_token=NULL, lease_until=NULL, updated_at=now()
        WHERE state='running' AND lease_until <= now()`)
	return result.RowsAffected(), err
}

func (s *Store) SaveReport(ctx context.Context, jobID string, resultJSON json.RawMessage) (json.RawMessage, error) {
	_, err := s.Pool.Exec(ctx, `INSERT INTO report_results (job_id, result) VALUES ($1, $2) ON CONFLICT (job_id) DO NOTHING`, jobID, resultJSON)
	if err != nil {
		return nil, err
	}
	var saved json.RawMessage
	err = s.Pool.QueryRow(ctx, `SELECT result FROM report_results WHERE job_id=$1`, jobID).Scan(&saved)
	return saved, err
}
