package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/scheduler"
	"github.com/ethan/smart-route/internal/store"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type DB struct{ db *sql.DB }
type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func Open(path string) (*DB, error) {
	dsn := path
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		dsn = "file:" + path
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite: %w", err)
	}
	s := &DB{db: db}
	if err = s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}
func (s *DB) Close() error { return s.db.Close() }
func (s *DB) migrate(ctx context.Context) error {
	b, e := migrationFS.ReadFile("migrations/001_initial.sql")
	if e != nil {
		return e
	}
	if _, e = s.db.ExecContext(ctx, string(b)); e != nil {
		return fmt.Errorf("sqlite migration: %w", e)
	}
	_, e = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO schema_migrations VALUES(1,?)`, time.Now().UTC())
	if e != nil {
		return e
	}
	for version := 2; version <= 10; version++ {
		var applied int
		if e = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=?`, version).Scan(&applied); e != nil {
			return e
		}
		if applied != 0 {
			continue
		}
		b, e = migrationFS.ReadFile(fmt.Sprintf("migrations/%03d_%s.sql", version, map[int]string{2: "worker_protocol", 3: "worker_identity_capacity", 4: "durable_results", 5: "controllers", 6: "pending_sandboxes", 7: "worker_auth", 8: "sessions", 9: "session_recovery", 10: "recovery_ack_events"}[version]))
		if e != nil {
			return e
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(b)); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlite migration %d: %w", version, err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations VALUES(?,?)`, version, time.Now().UTC()); err != nil {
			tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
func enc(v any) (string, error) { b, e := json.Marshal(v); return string(b), e }
func dec(s string, v any) error { return json.Unmarshal([]byte(s), v) }
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}
func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func mapErr(e error) error {
	if errors.Is(e, sql.ErrNoRows) {
		return store.ErrNotFound
	}
	if e != nil && strings.Contains(e.Error(), "UNIQUE constraint failed") {
		return store.ErrConflict
	}
	return e
}

func (s *DB) CreateJob(ctx context.Context, j domain.Job) (domain.Job, error) {
	if j.IdempotencyKey != "" {
		old, e := s.GetJobByIdempotencyKey(ctx, j.IdempotencyKey)
		if e == nil {
			return old, nil
		}
		if !errors.Is(e, store.ErrNotFound) {
			return domain.Job{}, e
		}
	}
	now := time.Now().UTC()
	if j.CreatedAt.IsZero() {
		j.CreatedAt = now
	}
	if j.UpdatedAt.IsZero() {
		j.UpdatedAt = j.CreatedAt
	}
	c, e := enc(j.Constraints)
	if e != nil {
		return domain.Job{}, e
	}
	payload := j.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	deps, _ := enc(j.DependsOn)
	if len(j.DependsOn) > 0 {
		j.State = domain.JobWaiting
	}
	if e = s.validateDependencies(ctx, j); e != nil {
		return domain.Job{}, e
	}
	if len(j.DependsOn) > 0 {
		allSucceeded := true
		for _, id := range j.DependsOn {
			var state domain.JobState
			if e = s.db.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id=?`, id).Scan(&state); e != nil {
				return domain.Job{}, mapErr(e)
			}
			if state != domain.JobSucceeded {
				allSucceeded = false
			}
			if state.Terminal() && state != domain.JobSucceeded {
				return domain.Job{}, fmt.Errorf("%w: dependency is terminal without success", store.ErrConflict)
			}
		}
		if allSucceeded {
			j.State = domain.JobQueued
		}
	}
	_, e = s.db.ExecContext(ctx, `INSERT INTO jobs(id,idempotency_key,kind,payload_json,state,constraints_json,retry_max_attempts,retry_backoff_ns,retry_max_backoff_ns,retry_max_elapsed_ns,timeout_at,next_attempt_at,created_at,updated_at,session_id,depends_on_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, j.ID, nullString(j.IdempotencyKey), j.Kind, []byte(payload), j.State, c, j.RetryPolicy.MaxAttempts, int64(j.RetryPolicy.Backoff), int64(j.RetryPolicy.MaxBackoff), int64(j.RetryPolicy.MaxElapsed), nullTime(j.TimeoutAt), nil, j.CreatedAt.UTC(), j.UpdatedAt.UTC(), nullString(string(j.SessionID)), deps)
	if mapErr(e) == store.ErrConflict && j.IdempotencyKey != "" {
		return s.GetJobByIdempotencyKey(ctx, j.IdempotencyKey)
	}
	return j, mapErr(e)
}
func scanJob(row *sql.Row) (domain.Job, error) {
	var j domain.Job
	var st, c string
	var payload []byte
	var idempotencyKey sql.NullString
	var timeout sql.NullTime
	var b, mb, me int64
	var session sql.NullString
	var deps string
	e := row.Scan(&j.ID, &idempotencyKey, &j.Kind, &payload, &st, &c, &j.RetryPolicy.MaxAttempts, &b, &mb, &me, &timeout, &j.CreatedAt, &j.UpdatedAt, &session, &deps)
	if e != nil {
		return j, mapErr(e)
	}
	j.State = domain.JobState(st)
	j.IdempotencyKey = idempotencyKey.String
	j.Payload = append(json.RawMessage(nil), payload...)
	j.SessionID = domain.SessionID(session.String)
	if e = dec(deps, &j.DependsOn); e != nil {
		return j, e
	}
	j.RetryPolicy.Backoff = time.Duration(b)
	j.RetryPolicy.MaxBackoff = time.Duration(mb)
	j.RetryPolicy.MaxElapsed = time.Duration(me)
	if timeout.Valid {
		j.TimeoutAt = timeout.Time
	}
	return j, dec(c, &j.Constraints)
}

const jobColumns = `id,idempotency_key,kind,payload_json,state,constraints_json,retry_max_attempts,retry_backoff_ns,retry_max_backoff_ns,retry_max_elapsed_ns,timeout_at,created_at,updated_at,session_id,depends_on_json`

func getJob(ctx context.Context, q queryer, clause string, arg any) (domain.Job, error) {
	return scanJob(q.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE `+clause, arg))
}
func (s *DB) GetJob(ctx context.Context, id domain.JobID) (domain.Job, error) {
	j, e := getJob(ctx, s.db, "id=?", id)
	if e != nil {
		return j, e
	}
	j.Attempts, e = listAttempts(ctx, s.db, id)
	if e != nil {
		return j, e
	}
	j.Events, e = listEvents(ctx, s.db, id, 0, 10000)
	return j, e
}
func (s *DB) GetJobByIdempotencyKey(ctx context.Context, k string) (domain.Job, error) {
	j, e := getJob(ctx, s.db, "idempotency_key=?", k)
	if e != nil {
		return j, e
	}
	return s.GetJob(ctx, j.ID)
}
func (s *DB) ListQueuedJobs(ctx context.Context, q store.QueueQuery) ([]domain.Job, error) {
	if q.Limit <= 0 {
		q.Limit = 100
	}
	rows, e := s.db.QueryContext(ctx, `SELECT id FROM jobs WHERE state='queued' AND (next_attempt_at IS NULL OR next_attempt_at<=CURRENT_TIMESTAMP) ORDER BY created_at,id LIMIT ?`, q.Limit)
	if e != nil {
		return nil, e
	}
	var out []domain.Job
	var ids []domain.JobID
	for rows.Next() {
		var id domain.JobID
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, id)
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return nil, e
	}
	rows.Close()
	for _, id := range ids {
		j, e := s.GetJob(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, j)
	}
	return out, nil
}
func (s *DB) CancelJob(ctx context.Context, id domain.JobID, at time.Time) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	r, e := tx.ExecContext(ctx, `UPDATE jobs SET state='canceled',next_attempt_at=NULL,updated_at=? WHERE id=? AND state NOT IN ('succeeded','failed','canceled','timed_out','dependency_failed','session_lost')`, at.UTC(), id)
	if e != nil {
		return e
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		var exists int
		if queryErr := tx.QueryRowContext(ctx, `SELECT 1 FROM jobs WHERE id=?`, id).Scan(&exists); errors.Is(queryErr, sql.ErrNoRows) {
			return store.ErrNotFound
		} else if queryErr != nil {
			return queryErr
		}
		return store.ErrConflict
	}
	var attempt domain.AttemptID
	queryErr := tx.QueryRowContext(ctx, `SELECT id FROM job_attempts WHERE job_id=? AND state IN ('leased','running')`, id).Scan(&attempt)
	if queryErr == nil {
		failure, _ := enc(domain.Failure{Code: "job_canceled", Message: "job canceled by client", Class: domain.FailureNonRetryable})
		if _, e = tx.ExecContext(ctx, `UPDATE job_attempts SET state='canceled',failure_json=?,ended_at=? WHERE id=?`, failure, at.UTC(), attempt); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `DELETE FROM leases WHERE attempt_id=?`, attempt); e != nil {
			return e
		}
	} else if !errors.Is(queryErr, sql.ErrNoRows) {
		return queryErr
	}
	if e = propagateDependencyFailure(ctx, tx, id, at); e != nil {
		return e
	}
	var seq uint64
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM job_events WHERE job_id=?`, id).Scan(&seq); e != nil {
		return e
	}
	data, _ := enc(map[string]string{"job_state": "canceled", "reason": "client_request"})
	if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(id,sequence,type,job_id,attempt_id,occurred_at,data_json) VALUES(?,?,?,?,?,?,?)`, fmt.Sprintf("canceled-%s", id), seq, domain.EventJobTransition, id, nullString(string(attempt)), at.UTC(), data); e != nil {
		return e
	}
	return tx.Commit()
}

func (s *DB) UpsertWorker(ctx context.Context, w domain.Worker) error {
	c, e := enc(w.Capabilities)
	if e != nil {
		return e
	}
	if w.LastSeenAt.IsZero() {
		w.LastSeenAt = time.Now().UTC()
	}
	if w.RegisteredAt.IsZero() {
		w.RegisteredAt = w.LastSeenAt
	}
	a, _ := enc(w.ActiveAttempts)
	m, _ := enc(w.SandboxMetadata)
	h, _ := enc(w.Health)
	u, _ := enc(w.UpstreamStatus)
	_, e = s.db.ExecContext(ctx, `INSERT INTO workers(id,capabilities_json,last_seen_at,session_id,session_token_hash,worker_version,protocol_version,slots,active_attempts_json,sandbox_metadata_json,health_json,upstream_status_json,registered_at,instance_id,sandbox_id,sandbox_provider,max_concurrency,available_slots,session_expires_at,reserved_session_id,session_epoch) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET capabilities_json=excluded.capabilities_json,last_seen_at=excluded.last_seen_at,session_id=excluded.session_id,session_token_hash=excluded.session_token_hash,worker_version=excluded.worker_version,protocol_version=excluded.protocol_version,active_attempts_json=excluded.active_attempts_json,sandbox_metadata_json=excluded.sandbox_metadata_json,health_json=excluded.health_json,upstream_status_json=excluded.upstream_status_json,instance_id=excluded.instance_id,sandbox_id=excluded.sandbox_id,sandbox_provider=excluded.sandbox_provider,max_concurrency=excluded.max_concurrency,available_slots=excluded.available_slots,session_expires_at=excluded.session_expires_at,reserved_session_id=excluded.reserved_session_id,session_epoch=excluded.session_epoch`, w.ID, c, w.LastSeenAt.UTC(), w.SessionID, w.SessionTokenHash, w.WorkerVersion, w.ProtocolVersion, w.MaxConcurrency, a, m, h, u, nullTime(w.RegisteredAt), w.InstanceID, w.SandboxID, w.SandboxProvider, w.MaxConcurrency, w.AvailableSlots, nullTime(w.SessionExpiresAt), w.ReservedSessionID, w.SessionEpoch)
	return e
}
func (s *DB) GetWorker(ctx context.Context, id domain.WorkerID) (domain.Worker, error) {
	var w domain.Worker
	var c, a, m, h, u string
	var registered, sessionExpires sql.NullTime
	e := s.db.QueryRowContext(ctx, `SELECT id,capabilities_json,last_seen_at,session_id,session_token_hash,worker_version,protocol_version,active_attempts_json,sandbox_metadata_json,health_json,upstream_status_json,registered_at,instance_id,sandbox_id,sandbox_provider,max_concurrency,available_slots,session_expires_at,reserved_session_id,session_epoch FROM workers WHERE id=?`, id).Scan(&w.ID, &c, &w.LastSeenAt, &w.SessionID, &w.SessionTokenHash, &w.WorkerVersion, &w.ProtocolVersion, &a, &m, &h, &u, &registered, &w.InstanceID, &w.SandboxID, &w.SandboxProvider, &w.MaxConcurrency, &w.AvailableSlots, &sessionExpires, &w.ReservedSessionID, &w.SessionEpoch)
	if e != nil {
		return w, mapErr(e)
	}
	if registered.Valid {
		w.RegisteredAt = registered.Time
	}
	if sessionExpires.Valid {
		w.SessionExpiresAt = sessionExpires.Time
	}
	if e = dec(c, &w.Capabilities); e != nil {
		return w, e
	}
	if e = dec(a, &w.ActiveAttempts); e != nil {
		return w, e
	}
	if e = dec(m, &w.SandboxMetadata); e != nil {
		return w, e
	}
	if e = dec(h, &w.Health); e != nil {
		return w, e
	}
	e = dec(u, &w.UpstreamStatus)
	return w, e
}
func (s *DB) GetWorkerByInstanceID(ctx context.Context, instanceID string) (domain.Worker, error) {
	var id domain.WorkerID
	if e := s.db.QueryRowContext(ctx, `SELECT id FROM workers WHERE instance_id=?`, instanceID).Scan(&id); e != nil {
		return domain.Worker{}, mapErr(e)
	}
	return s.GetWorker(ctx, id)
}
func (s *DB) UpsertSandbox(ctx context.Context, v domain.Sandbox) error {
	c, e := enc(v.Capabilities)
	if e != nil {
		return e
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	if v.UpdatedAt.IsZero() {
		v.UpdatedAt = v.CreatedAt
	}
	_, e = s.db.ExecContext(ctx, `INSERT INTO sandboxes(id,worker_id,capabilities_json,state,created_at,provider,external_id,updated_at,drain_at,reserved_session_id) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET worker_id=excluded.worker_id,capabilities_json=excluded.capabilities_json,state=excluded.state,provider=excluded.provider,external_id=excluded.external_id,updated_at=excluded.updated_at,drain_at=excluded.drain_at`, v.ID, nullString(string(v.WorkerID)), c, v.State, v.CreatedAt.UTC(), v.Provider, v.ExternalID, v.UpdatedAt.UTC(), nullTime(v.DrainAt), v.ReservedSessionID)
	return e
}
func (s *DB) GetSandbox(ctx context.Context, id domain.SandboxID) (domain.Sandbox, error) {
	return getSandbox(ctx, s.db, id)
}
func getSandbox(ctx context.Context, q queryer, id domain.SandboxID) (domain.Sandbox, error) {
	var v domain.Sandbox
	var c string
	var worker sql.NullString
	var updated, drain sql.NullTime
	e := q.QueryRowContext(ctx, `SELECT id,worker_id,capabilities_json,state,created_at,provider,external_id,updated_at,drain_at,reserved_session_id FROM sandboxes WHERE id=?`, id).Scan(&v.ID, &worker, &c, &v.State, &v.CreatedAt, &v.Provider, &v.ExternalID, &updated, &drain, &v.ReservedSessionID)
	if e != nil {
		return v, mapErr(e)
	}
	if updated.Valid {
		v.UpdatedAt = updated.Time
	}
	v.WorkerID = domain.WorkerID(worker.String)
	if drain.Valid {
		v.DrainAt = drain.Time
	}
	return v, dec(c, &v.Capabilities)
}

func (s *DB) GetAttempt(ctx context.Context, id domain.AttemptID) (domain.Attempt, error) {
	return getAttempt(ctx, s.db, id)
}
func getAttempt(ctx context.Context, q queryer, id domain.AttemptID) (domain.Attempt, error) {
	var a domain.Attempt
	var st string
	var fail sql.NullString
	var started, ended, expiry sql.NullTime
	var lease, sandbox sql.NullString
	e := q.QueryRowContext(ctx, `SELECT a.id,a.job_id,a.number,a.state,a.worker_id,a.sandbox_id,a.failure_json,a.started_at,a.ended_at,l.id,l.expires_at FROM job_attempts a LEFT JOIN leases l ON l.attempt_id=a.id WHERE a.id=?`, id).Scan(&a.ID, &a.JobID, &a.Number, &st, &a.WorkerID, &sandbox, &fail, &started, &ended, &lease, &expiry)
	if e != nil {
		return a, mapErr(e)
	}
	a.State = domain.AttemptState(st)
	a.SandboxID = domain.SandboxID(sandbox.String)
	if fail.Valid {
		a.Failure = &domain.Failure{}
		if e = dec(fail.String, a.Failure); e != nil {
			return a, e
		}
	}
	if started.Valid {
		a.StartedAt = &started.Time
	}
	if ended.Valid {
		a.EndedAt = &ended.Time
	}
	if lease.Valid {
		a.Lease = domain.Lease{ID: domain.LeaseID(lease.String), WorkerID: a.WorkerID, AttemptID: a.ID, ExpiresAt: expiry.Time}
	}
	return a, nil
}
func listAttempts(ctx context.Context, q queryer, id domain.JobID) ([]domain.Attempt, error) {
	rows, e := q.QueryContext(ctx, `SELECT id FROM job_attempts WHERE job_id=? ORDER BY number`, id)
	if e != nil {
		return nil, e
	}
	var ids []domain.AttemptID
	for rows.Next() {
		var x domain.AttemptID
		if e = rows.Scan(&x); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, x)
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return nil, e
	}
	rows.Close()
	var out []domain.Attempt
	for _, x := range ids {
		a, e := getAttempt(ctx, q, x)
		if e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, nil
}
func (s *DB) AppendEvent(ctx context.Context, v domain.Event) error {
	d, e := enc(v.Data)
	if e != nil {
		return e
	}
	_, e = s.db.ExecContext(ctx, `INSERT INTO job_events(id,sequence,type,job_id,attempt_id,occurred_at,data_json,worker_sequence,idempotency_key) VALUES(?,?,?,?,?,?,?,?,?)`, v.ID, v.Sequence, v.Type, v.JobID, v.AttemptID, v.OccurredAt.UTC(), d, v.WorkerSequence, v.IdempotencyKey)
	return mapErr(e)
}
func (s *DB) AppendAttemptEvent(ctx context.Context, attempt domain.AttemptID, worker domain.WorkerID, v domain.Event) (domain.Event, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return v, e
	}
	defer tx.Rollback()
	var state string
	if e = tx.QueryRowContext(ctx, `SELECT job_id,state FROM job_attempts WHERE id=? AND worker_id=?`, attempt, worker).Scan(&v.JobID, &state); e != nil {
		return v, mapErr(e)
	}
	if !domain.AttemptState(state).Active() {
		return v, store.ErrConflict
	}
	v.AttemptID = attempt
	if v.IdempotencyKey != "" {
		var old domain.Event
		var typ, data string
		err := tx.QueryRowContext(ctx, `SELECT id,sequence,type,job_id,attempt_id,occurred_at,data_json,worker_sequence,idempotency_key FROM job_events WHERE attempt_id=? AND idempotency_key=?`, attempt, v.IdempotencyKey).Scan(&old.ID, &old.Sequence, &typ, &old.JobID, &old.AttemptID, &old.OccurredAt, &data, &old.WorkerSequence, &old.IdempotencyKey)
		if err == nil {
			old.Type = domain.EventType(typ)
			_ = dec(data, &old.Data)
			return old, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return v, err
		}
	}
	if v.ID == "" {
		v.ID = domain.EventID(fmt.Sprintf("event-%s-%d", attempt, time.Now().UnixNano()))
	}
	if v.OccurredAt.IsZero() {
		v.OccurredAt = time.Now().UTC()
	}
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM job_events WHERE job_id=?`, v.JobID).Scan(&v.Sequence); e != nil {
		return v, e
	}
	d, e := enc(v.Data)
	if e != nil {
		return v, e
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(id,sequence,type,job_id,attempt_id,occurred_at,data_json,worker_sequence,idempotency_key) VALUES(?,?,?,?,?,?,?,?,?)`, v.ID, v.Sequence, v.Type, v.JobID, v.AttemptID, v.OccurredAt.UTC(), d, v.WorkerSequence, v.IdempotencyKey); e != nil {
		return v, mapErr(e)
	}
	return v, tx.Commit()
}
func (s *DB) ListEvents(ctx context.Context, id domain.JobID, after uint64, limit int) ([]domain.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	return listEvents(ctx, s.db, id, after, limit)
}
func listEvents(ctx context.Context, q queryer, id domain.JobID, after uint64, limit int) ([]domain.Event, error) {
	rows, e := q.QueryContext(ctx, `SELECT id,sequence,type,job_id,attempt_id,occurred_at,data_json,worker_sequence,idempotency_key FROM job_events WHERE job_id=? AND sequence>? ORDER BY sequence LIMIT ?`, id, after, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var v domain.Event
		var typ, d string
		var attempt sql.NullString
		if e = rows.Scan(&v.ID, &v.Sequence, &typ, &v.JobID, &attempt, &v.OccurredAt, &d, &v.WorkerSequence, &v.IdempotencyKey); e != nil {
			return nil, e
		}
		v.AttemptID = domain.AttemptID(attempt.String)
		v.Type = domain.EventType(typ)
		if e = dec(d, &v.Data); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *DB) ClaimNextJob(ctx context.Context, r store.ClaimRequest) (domain.Attempt, domain.Job, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return domain.Attempt{}, domain.Job{}, e
	}
	defer tx.Rollback()
	if r.Capacity > 0 {
		var active, available, maximum int
		if e = tx.QueryRowContext(ctx, `SELECT COUNT(*),(SELECT available_slots FROM workers WHERE id=?),(SELECT max_concurrency FROM workers WHERE id=?) FROM job_attempts WHERE worker_id=? AND state IN ('leased','running')`, r.Worker.ID, r.Worker.ID, r.Worker.ID).Scan(&active, &available, &maximum); e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		r.Worker.AvailableSlots = available
		r.Worker.MaxConcurrency = min(maximum, r.Capacity)
	}
	rows, e := tx.QueryContext(ctx, `SELECT id FROM jobs WHERE state='queued' AND (timeout_at IS NULL OR timeout_at>?) AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY created_at,id`, r.Now.UTC(), r.Now.UTC())
	if e != nil {
		return domain.Attempt{}, domain.Job{}, e
	}
	var ids []domain.JobID
	for rows.Next() {
		var id domain.JobID
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return domain.Attempt{}, domain.Job{}, e
		}
		ids = append(ids, id)
	}
	rows.Close()
	jobs := make([]domain.Job, 0, len(ids))
	for _, id := range ids {
		j, e := getJob(ctx, tx, "id=?", id)
		if e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		jobs = append(jobs, j)
	}
	policy := r.Scheduler
	if policy == nil {
		policy = scheduler.New(scheduler.Config{})
	}
	sandbox, e := getSandbox(ctx, tx, r.SandboxID)
	if e != nil {
		return domain.Attempt{}, domain.Job{}, e
	}
	var active int
	if e = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_attempts WHERE worker_id=? AND state IN ('leased','running')`, r.Worker.ID).Scan(&active); e != nil {
		return domain.Attempt{}, domain.Job{}, e
	}
	for _, j := range policy.Rank(scheduler.Request{Jobs: jobs, Worker: r.Worker, Sandbox: sandbox, Now: r.Now, Active: active}).Ranked {
		var reserved string
		if e = tx.QueryRowContext(ctx, `SELECT reserved_session_id FROM workers WHERE id=?`, r.Worker.ID).Scan(&reserved); e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		if j.SessionID == "" && reserved != "" {
			continue
		}
		if j.SessionID != "" {
			var state, worker, box, capsJSON, provider string
			var idle int64
			var epoch uint64
			if e = tx.QueryRowContext(ctx, `SELECT state,COALESCE(worker_id,''),COALESCE(sandbox_id,''),capabilities_json,preferred_provider,idle_ttl_ns,epoch FROM sessions WHERE id=?`, j.SessionID).Scan(&state, &worker, &box, &capsJSON, &provider, &idle, &epoch); e != nil {
				return domain.Attempt{}, domain.Job{}, mapErr(e)
			}
			var caps domain.Capabilities
			if e = dec(capsJSON, &caps); e != nil {
				return domain.Attempt{}, domain.Job{}, e
			}
			required := domain.RoutingConstraints{Capabilities: caps.Capabilities, Labels: caps.Labels, Architecture: caps.Architecture, Region: caps.Region, PreferredProvider: provider}
			if !r.Worker.Capabilities.Satisfies(required) || (provider != "" && r.Worker.SandboxProvider != provider) {
				continue
			}
			if state == string(domain.SessionActive) && (worker != string(r.Worker.ID) || box != string(r.SandboxID)) {
				continue
			}
			if state == string(domain.SessionPending) {
				if reserved != "" && reserved != string(j.SessionID) {
					continue
				}
				var expiry any
				if idle > 0 {
					expiry = r.Now.Add(time.Duration(idle)).UTC()
				}
				res, err := tx.ExecContext(ctx, `UPDATE sessions SET worker_id=?,sandbox_id=?,state='active',last_activity=?,idle_expires_at=? WHERE id=? AND state='pending'`, r.Worker.ID, r.SandboxID, r.Now, expiry, j.SessionID)
				if err != nil {
					return domain.Attempt{}, domain.Job{}, err
				}
				n, _ := res.RowsAffected()
				if n != 1 {
					continue
				}
				if _, e = tx.ExecContext(ctx, `UPDATE workers SET reserved_session_id=?,session_epoch=? WHERE id=?`, j.SessionID, epoch, r.Worker.ID); e != nil {
					return domain.Attempt{}, domain.Job{}, e
				}
				if _, e = tx.ExecContext(ctx, `UPDATE sandboxes SET reserved_session_id=? WHERE id=?`, j.SessionID, r.SandboxID); e != nil {
					return domain.Attempt{}, domain.Job{}, e
				}
			} else if state == string(domain.SessionRecovering) {
				prefix := fmt.Sprintf("rebuild-%s-%d-", j.SessionID, epoch)
				if reserved != string(j.SessionID) || r.Worker.SessionEpoch != epoch || !strings.HasPrefix(string(j.ID), prefix) {
					continue
				}
			} else if state != string(domain.SessionActive) {
				continue
			}
		}
		id := j.ID
		var n int
		if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(number),0)+1 FROM job_attempts WHERE job_id=?`, id).Scan(&n); e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		a := domain.Attempt{ID: domain.AttemptID(fmt.Sprintf("%s-%d", id, n)), JobID: id, Number: n, State: domain.AttemptLeased, WorkerID: r.Worker.ID, SandboxID: r.SandboxID}
		a.Lease = domain.Lease{ID: domain.LeaseID("lease-" + string(a.ID)), WorkerID: r.Worker.ID, AttemptID: a.ID, ExpiresAt: r.Now.Add(r.LeaseDuration)}
		res, e := tx.ExecContext(ctx, `UPDATE jobs SET state='leased',updated_at=? WHERE id=? AND state='queued'`, r.Now.UTC(), id)
		if e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		changed, _ := res.RowsAffected()
		if changed == 0 {
			continue
		}
		if r.Capacity > 0 {
			res, e = tx.ExecContext(ctx, `UPDATE workers SET available_slots=available_slots-1 WHERE id=? AND available_slots>0`, r.Worker.ID)
			if e != nil {
				return domain.Attempt{}, domain.Job{}, e
			}
			changed, _ = res.RowsAffected()
			if changed != 1 {
				return domain.Attempt{}, domain.Job{}, store.ErrConflict
			}
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO job_attempts(id,job_id,number,state,worker_id,sandbox_id,started_at) VALUES(?,?,?,?,?,?,?)`, a.ID, id, n, a.State, a.WorkerID, nullString(string(a.SandboxID)), r.Now.UTC()); e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		if _, e = tx.ExecContext(ctx, `INSERT INTO leases VALUES(?,?,?,?)`, a.Lease.ID, a.WorkerID, a.ID, a.Lease.ExpiresAt.UTC()); e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		if j.SessionID != "" {
			var idle int64
			if e = tx.QueryRowContext(ctx, `SELECT idle_ttl_ns FROM sessions WHERE id=?`, j.SessionID).Scan(&idle); e != nil {
				return domain.Attempt{}, domain.Job{}, e
			}
			var expiry any
			if idle > 0 {
				expiry = r.Now.Add(time.Duration(idle)).UTC()
			}
			if _, e = tx.ExecContext(ctx, `UPDATE sessions SET last_activity=?,idle_expires_at=? WHERE id=? AND state='active'`, r.Now, expiry, j.SessionID); e != nil {
				return domain.Attempt{}, domain.Job{}, e
			}
		}
		if e = tx.Commit(); e != nil {
			return domain.Attempt{}, domain.Job{}, e
		}
		j.State = domain.JobLeased
		j.Attempts = []domain.Attempt{a}
		return a, j, nil
	}
	return domain.Attempt{}, domain.Job{}, store.ErrNotFound
}
func (s *DB) RenewLease(ctx context.Context, a domain.AttemptID, w domain.WorkerID, until time.Time) error {
	now := time.Now().UTC()
	res, e := s.db.ExecContext(ctx, `UPDATE leases SET expires_at=? WHERE attempt_id=? AND worker_id=? AND expires_at>? AND EXISTS (SELECT 1 FROM job_attempts WHERE id=? AND state IN ('leased','running'))`, until.UTC(), a, w, now, a)
	if e != nil {
		return e
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrNotFound
	}
	return nil
}
func (s *DB) CompleteAttempt(ctx context.Context, c store.Completion) error {
	if !c.AttemptState.Terminal() || (c.JobState != "" && !c.JobState.Terminal()) {
		return store.ErrConflict
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var jid domain.JobID
	var old string
	var number, maxAttempts int
	var timeout sql.NullTime
	var leaseExpiry sql.NullTime
	e = tx.QueryRowContext(ctx, `SELECT a.job_id,a.state,a.number,j.retry_max_attempts,j.timeout_at,l.expires_at FROM job_attempts a JOIN jobs j ON j.id=a.job_id LEFT JOIN leases l ON l.attempt_id=a.id WHERE a.id=? AND a.worker_id=?`, c.AttemptID, c.WorkerID).Scan(&jid, &old, &number, &maxAttempts, &timeout, &leaseExpiry)
	if e != nil {
		return mapErr(e)
	}
	if !domain.CanAttemptTransition(domain.AttemptState(old), c.AttemptState) {
		return store.ErrConflict
	}
	if !leaseExpiry.Valid || !c.At.Before(leaseExpiry.Time) {
		return store.ErrConflict
	}
	if c.JobState == "" {
		c.JobState = domain.JobFailed
		if c.Failure != nil && c.Failure.Retryable() && number < maxAttempts && (!timeout.Valid || c.At.Before(timeout.Time)) {
			c.JobState = domain.JobQueued
		}
	}
	var f any
	if c.Failure != nil {
		f, _ = enc(c.Failure)
	}
	if _, e = tx.ExecContext(ctx, `UPDATE job_attempts SET state=?,failure_json=?,ended_at=? WHERE id=?`, c.AttemptState, f, c.At.UTC(), c.AttemptID); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `DELETE FROM leases WHERE attempt_id=?`, c.AttemptID); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE workers SET available_slots=MIN(max_concurrency,available_slots+1) WHERE id=?`, c.WorkerID); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `UPDATE jobs SET state=?,updated_at=? WHERE id=?`, c.JobState, c.At.UTC(), jid); e != nil {
		return e
	}
	if c.JobState == domain.JobSucceeded {
		if _, e = tx.ExecContext(ctx, `UPDATE jobs SET state='queued',updated_at=? WHERE state='waiting' AND EXISTS(SELECT 1 FROM json_each(jobs.depends_on_json) d WHERE d.value=?) AND NOT EXISTS(SELECT 1 FROM json_each(jobs.depends_on_json) d JOIN jobs p ON p.id=d.value WHERE p.state!='succeeded')`, c.At, jid); e != nil {
			return e
		}
	} else if c.JobState.Terminal() {
		if e = propagateDependencyFailure(ctx, tx, jid, c.At); e != nil {
			return e
		}
	}
	var sessionID sql.NullString
	var idle int64
	if e = tx.QueryRowContext(ctx, `SELECT session_id FROM jobs WHERE id=?`, jid).Scan(&sessionID); e != nil {
		return e
	}
	if sessionID.Valid {
		if e = tx.QueryRowContext(ctx, `SELECT idle_ttl_ns FROM sessions WHERE id=?`, sessionID.String).Scan(&idle); e != nil {
			return e
		}
		var expiry any
		if idle > 0 {
			expiry = c.At.Add(time.Duration(idle)).UTC()
		}
		if _, e = tx.ExecContext(ctx, `UPDATE sessions SET last_activity=?,idle_expires_at=? WHERE id=? AND state='active'`, c.At, expiry, sessionID.String); e != nil {
			return e
		}
	}
	c.Event.JobID = jid
	c.Event.AttemptID = c.AttemptID
	if c.Event.OccurredAt.IsZero() {
		c.Event.OccurredAt = c.At
	}
	if c.Event.ID == "" {
		c.Event.ID = domain.EventID(fmt.Sprintf("event-%s-%d", c.AttemptID, c.At.UnixNano()))
	}
	if c.Event.Sequence == 0 {
		if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM job_events WHERE job_id=?`, jid).Scan(&c.Event.Sequence); e != nil {
			return e
		}
	}
	d, _ := enc(c.Event.Data)
	if _, e = tx.ExecContext(ctx, `INSERT INTO job_events(id,sequence,type,job_id,attempt_id,occurred_at,data_json,worker_sequence,idempotency_key) VALUES(?,?,?,?,?,?,?,?,?)`, c.Event.ID, c.Event.Sequence, c.Event.Type, c.Event.JobID, c.Event.AttemptID, c.Event.OccurredAt.UTC(), d, c.Event.WorkerSequence, c.Event.IdempotencyKey); e != nil {
		return mapErr(e)
	}
	return tx.Commit()
}

func (s *DB) SaveResult(ctx context.Context, attempt domain.AttemptID, worker domain.WorkerID, result domain.JobResult) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var job domain.JobID
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT job_id,state FROM job_attempts WHERE id=? AND worker_id=?`, attempt, worker).Scan(&job, &state); err != nil {
		return mapErr(err)
	}
	if !domain.AttemptState(state).Active() {
		return store.ErrConflict
	}
	metadata, err := enc(result.Metadata)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO job_results(job_id,attempt_id,status_code,data,artifact_key,metadata_json,created_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET attempt_id=excluded.attempt_id,status_code=excluded.status_code,data=excluded.data,artifact_key=excluded.artifact_key,metadata_json=excluded.metadata_json,created_at=excluded.created_at`, job, attempt, result.StatusCode, result.Data, result.ArtifactKey, metadata, result.CreatedAt.UTC())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *DB) GetResult(ctx context.Context, job domain.JobID) (domain.JobResult, error) {
	var result domain.JobResult
	var metadata string
	err := s.db.QueryRowContext(ctx, `SELECT job_id,attempt_id,status_code,data,artifact_key,metadata_json,created_at FROM job_results WHERE job_id=?`, job).Scan(&result.JobID, &result.AttemptID, &result.StatusCode, &result.Data, &result.ArtifactKey, &metadata, &result.CreatedAt)
	if err != nil {
		return result, mapErr(err)
	}
	err = dec(metadata, &result.Metadata)
	return result, err
}
func (s *DB) ExpireLeases(ctx context.Context, now time.Time) ([]domain.AttemptID, error) {
	return s.expireLeases(ctx, `expires_at<=?`, now.UTC(), now)
}
func (s *DB) ExpireWorkerLeases(ctx context.Context, worker domain.WorkerID, now time.Time) ([]domain.AttemptID, error) {
	return s.expireLeases(ctx, `worker_id=?`, worker, now)
}
func (s *DB) expireLeases(ctx context.Context, where string, arg any, now time.Time) ([]domain.AttemptID, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	rows, e := tx.QueryContext(ctx, `SELECT attempt_id FROM leases WHERE `+where+` ORDER BY expires_at`, arg)
	if e != nil {
		return nil, e
	}
	var ids []domain.AttemptID
	for rows.Next() {
		var id domain.AttemptID
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return nil, e
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		var job domain.JobID
		var worker domain.WorkerID
		var number, maxAttempts int
		var base, maximum, maxElapsed int64
		var created time.Time
		var timeout sql.NullTime
		if e = tx.QueryRowContext(ctx, `SELECT a.job_id,a.worker_id,a.number,j.retry_max_attempts,j.retry_backoff_ns,j.retry_max_backoff_ns,j.retry_max_elapsed_ns,j.created_at,j.timeout_at FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=? AND a.state IN ('leased','running')`, id).Scan(&job, &worker, &number, &maxAttempts, &base, &maximum, &maxElapsed, &created, &timeout); errors.Is(e, sql.ErrNoRows) {
			continue
		} else if e != nil {
			return nil, e
		}
		failure, _ := enc(domain.Failure{Code: "lease_expired", Message: "worker lease expired", Class: domain.FailureRetryable})
		if _, e = tx.ExecContext(ctx, `UPDATE job_attempts SET state='lease_expired',failure_json=?,ended_at=? WHERE id=? AND state IN ('leased','running')`, failure, now.UTC(), id); e != nil {
			return nil, e
		}
		retry := number < maxAttempts && (!timeout.Valid || now.Before(timeout.Time)) && (maxElapsed <= 0 || now.Sub(created) < time.Duration(maxElapsed))
		state := domain.JobFailed
		var next any
		if timeout.Valid && !now.Before(timeout.Time) {
			state = domain.JobTimedOut
		} else if retry {
			state = domain.JobQueued
			delay := retryDelay(time.Duration(base), time.Duration(maximum), number, id)
			next = now.Add(delay).UTC()
		}
		if _, e = tx.ExecContext(ctx, `UPDATE jobs SET state=?,next_attempt_at=?,updated_at=? WHERE id=? AND state IN ('leased','running')`, state, next, now.UTC(), job); e != nil {
			return nil, e
		}
		if state.Terminal() {
			if e = propagateDependencyFailure(ctx, tx, job, now); e != nil {
				return nil, e
			}
		}
		if _, e = tx.ExecContext(ctx, `DELETE FROM leases WHERE attempt_id=?`, id); e != nil {
			return nil, e
		}
		if _, e = tx.ExecContext(ctx, `UPDATE workers SET available_slots=MIN(max_concurrency,available_slots+1) WHERE id=?`, worker); e != nil {
			return nil, e
		}
		var sequence uint64
		if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM job_events WHERE job_id=?`, job).Scan(&sequence); e != nil {
			return nil, e
		}
		data, _ := enc(map[string]string{"attempt_state": string(domain.AttemptExpired), "job_state": string(state), "reason": "lease_expired"})
		eventID := fmt.Sprintf("lease-expired-%s", id)
		if _, e = tx.ExecContext(ctx, `INSERT OR IGNORE INTO job_events(id,sequence,type,job_id,attempt_id,occurred_at,data_json) VALUES(?,?,?,?,?,?,?)`, eventID, sequence, domain.EventAttemptTransition, job, id, now.UTC(), data); e != nil {
			return nil, e
		}
	}
	return ids, tx.Commit()
}

func retryDelay(base, maximum time.Duration, attempt int, id domain.AttemptID) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for i := 1; i < attempt && (maximum <= 0 || d < maximum); i++ {
		if d > time.Duration(1<<62) {
			break
		}
		d *= 2
	}
	if maximum > 0 && d > maximum {
		d = maximum
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	// Stable 0..25% jitter keeps retries dispersed while making tests reproducible.
	return d + time.Duration(uint64(d)*uint64(h.Sum32()%251)/1000)
}

func (s *DB) TimeoutJobs(ctx context.Context, now time.Time) ([]domain.JobID, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM jobs WHERE timeout_at IS NOT NULL AND timeout_at<=? AND state NOT IN ('succeeded','failed','canceled','timed_out','dependency_failed','session_lost') ORDER BY id`, now.UTC())
	if err != nil {
		return nil, err
	}
	var ids []domain.JobID
	for rows.Next() {
		var id domain.JobID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		var attempt domain.AttemptID
		var worker domain.WorkerID
		err = tx.QueryRowContext(ctx, `SELECT id,worker_id FROM job_attempts WHERE job_id=? AND state IN ('leased','running')`, id).Scan(&attempt, &worker)
		if err == nil {
			failure, _ := enc(domain.Failure{Code: "job_timeout", Message: "job deadline exceeded", Class: domain.FailureNonRetryable})
			if _, err = tx.ExecContext(ctx, `UPDATE job_attempts SET state='canceled',failure_json=?,ended_at=? WHERE id=?`, failure, now.UTC(), attempt); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM leases WHERE attempt_id=?`, attempt); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE workers SET available_slots=MIN(max_concurrency,available_slots+1) WHERE id=?`, worker); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE jobs SET state='timed_out',next_attempt_at=NULL,updated_at=? WHERE id=?`, now.UTC(), id); err != nil {
			return nil, err
		}
		if err = propagateDependencyFailure(ctx, tx, id, now); err != nil {
			return nil, err
		}
		var seq uint64
		if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM job_events WHERE job_id=?`, id).Scan(&seq); err != nil {
			return nil, err
		}
		data, _ := enc(map[string]string{"job_state": "timed_out", "reason": "deadline_exceeded"})
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO job_events(id,sequence,type,job_id,attempt_id,occurred_at,data_json) VALUES(?,?,?,?,?,?,?)`, fmt.Sprintf("timeout-%s", id), seq, domain.EventJobTransition, id, nullString(string(attempt)), now.UTC(), data); err != nil {
			return nil, err
		}
	}
	return ids, tx.Commit()
}

func (s *DB) SetWorkerHealth(ctx context.Context, id domain.WorkerID, health domain.WorkerHealth, at time.Time) error {
	w, err := s.GetWorker(ctx, id)
	if err != nil {
		return err
	}
	if w.Health == nil {
		w.Health = map[string]string{}
	}
	w.Health["status"] = string(health)
	h, err := enc(w.Health)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE workers SET health_json=?,session_token_hash=CASE WHEN ?='dead' THEN '' ELSE session_token_hash END,session_expires_at=CASE WHEN ?='dead' THEN NULL ELSE session_expires_at END WHERE id=?`, h, health, health, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrNotFound
	}
	if health == domain.WorkerDead && w.ReservedSessionID != "" {
		session, e := s.GetSession(ctx, w.ReservedSessionID)
		if e != nil {
			return e
		}
		if session.RecoveryPolicy != domain.RecoveryNone {
			return s.RequestRecovery(ctx, session.ID, at)
		}
		return s.finishSession(ctx, session.ID, domain.SessionLost, "session_lost", at)
	}
	return nil
}

func (s *DB) SetSandboxState(ctx context.Context, id domain.SandboxID, state string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE sandboxes SET state=?,updated_at=?,drain_at=CASE WHEN ?='draining' THEN COALESCE(drain_at,?) ELSE drain_at END WHERE id=?`, state, at.UTC(), state, at.UTC(), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrNotFound
	}
	if state == "terminated" || state == "failed" || state == "stopped" {
		return s.RevokeSandboxCredentials(ctx, id)
	}
	return nil
}
func (s *DB) DeleteSandbox(ctx context.Context, id domain.SandboxID) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM sandboxes WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *DB) CreateBootstrapToken(ctx context.Context, token store.BootstrapToken) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO bootstrap_tokens(token_hash,sandbox_id,sandbox_provider,pool,capability_hash,expires_at) VALUES(?,?,?,?,?,?)`, token.TokenHash, token.SandboxID, token.SandboxProvider, token.Pool, token.CapabilityHash, token.ExpiresAt.UTC())
	return mapErr(err)
}

func (s *DB) ConsumeBootstrapToken(ctx context.Context, hash string, sandboxID domain.SandboxID, provider, pool, capabilityHash string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE bootstrap_tokens SET consumed_at=? WHERE token_hash=? AND sandbox_id=? AND sandbox_provider=? AND pool=? AND capability_hash=? AND consumed_at IS NULL AND expires_at>?`, now.UTC(), hash, sandboxID, provider, pool, capabilityHash, now.UTC())
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return store.ErrNotFound
	}
	return nil
}

func (s *DB) RevokeSandboxCredentials(ctx context.Context, sandboxID domain.SandboxID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE bootstrap_tokens SET consumed_at=COALESCE(consumed_at,?) WHERE sandbox_id=?`, time.Now().UTC(), sandboxID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE workers SET session_token_hash='',session_expires_at=NULL WHERE sandbox_id=?`, sandboxID); err != nil {
		return err
	}
	return tx.Commit()
}

var _ store.Store = (*DB)(nil)
