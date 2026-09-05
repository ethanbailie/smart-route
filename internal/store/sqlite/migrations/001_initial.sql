PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE, kind TEXT NOT NULL DEFAULT '', payload_json BLOB NOT NULL DEFAULT 'null', state TEXT NOT NULL, constraints_json TEXT NOT NULL,
 retry_max_attempts INTEGER NOT NULL, retry_backoff_ns INTEGER NOT NULL, retry_max_backoff_ns INTEGER NOT NULL,
 timeout_at TIMESTAMP, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_queued_idx ON jobs(state, created_at, id);
CREATE TABLE IF NOT EXISTS workers (id TEXT PRIMARY KEY, capabilities_json TEXT NOT NULL, last_seen_at TIMESTAMP NOT NULL);
CREATE INDEX IF NOT EXISTS workers_heartbeat_idx ON workers(last_seen_at);
CREATE TABLE IF NOT EXISTS sandboxes (id TEXT PRIMARY KEY, worker_id TEXT NOT NULL REFERENCES workers(id), capabilities_json TEXT NOT NULL, state TEXT NOT NULL, created_at TIMESTAMP NOT NULL);
CREATE INDEX IF NOT EXISTS sandboxes_state_idx ON sandboxes(state);
CREATE TABLE IF NOT EXISTS job_attempts (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id), number INTEGER NOT NULL, state TEXT NOT NULL,
 worker_id TEXT NOT NULL REFERENCES workers(id), sandbox_id TEXT REFERENCES sandboxes(id), failure_json TEXT,
 started_at TIMESTAMP, ended_at TIMESTAMP, UNIQUE(job_id, number)
);
CREATE UNIQUE INDEX IF NOT EXISTS attempts_active_idx ON job_attempts(job_id) WHERE state IN ('leased','running');
CREATE TABLE IF NOT EXISTS leases (id TEXT PRIMARY KEY, worker_id TEXT NOT NULL REFERENCES workers(id), attempt_id TEXT NOT NULL UNIQUE REFERENCES job_attempts(id), expires_at TIMESTAMP NOT NULL);
CREATE INDEX IF NOT EXISTS leases_expiration_idx ON leases(expires_at);
CREATE TABLE IF NOT EXISTS job_events (id TEXT PRIMARY KEY, sequence INTEGER NOT NULL, type TEXT NOT NULL, job_id TEXT NOT NULL REFERENCES jobs(id), attempt_id TEXT, occurred_at TIMESTAMP NOT NULL, data_json TEXT NOT NULL, UNIQUE(job_id, sequence));
CREATE INDEX IF NOT EXISTS events_sequence_idx ON job_events(job_id, sequence);
CREATE TABLE IF NOT EXISTS upstreams (id TEXT PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS credential_refs (id TEXT PRIMARY KEY, provider TEXT NOT NULL, reference TEXT NOT NULL, metadata_json TEXT NOT NULL DEFAULT '{}', UNIQUE(provider, reference));
