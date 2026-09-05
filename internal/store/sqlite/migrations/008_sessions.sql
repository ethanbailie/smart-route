CREATE TABLE sessions (
 id TEXT PRIMARY KEY, pool TEXT NOT NULL, capabilities_json TEXT NOT NULL, preferred_provider TEXT NOT NULL DEFAULT '', labels_json TEXT NOT NULL,
 sandbox_id TEXT, worker_id TEXT, state TEXT NOT NULL, idle_ttl_ns INTEGER NOT NULL, max_lifetime_ns INTEGER NOT NULL,
 created_at TIMESTAMP NOT NULL, last_activity TIMESTAMP NOT NULL, idle_expires_at TIMESTAMP, closed_at TIMESTAMP, failure_json TEXT
);
CREATE INDEX sessions_state_idx ON sessions(state);
CREATE INDEX sessions_worker_sandbox_idx ON sessions(worker_id, sandbox_id);
CREATE INDEX sessions_activity_idx ON sessions(last_activity);
CREATE INDEX sessions_idle_expiry_idx ON sessions(idle_expires_at) WHERE state IN ('pending','active');
ALTER TABLE jobs ADD COLUMN session_id TEXT REFERENCES sessions(id);
ALTER TABLE jobs ADD COLUMN depends_on_json TEXT NOT NULL DEFAULT '[]';
CREATE INDEX jobs_session_idx ON jobs(session_id, created_at, id);
ALTER TABLE workers ADD COLUMN reserved_session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE sandboxes ADD COLUMN reserved_session_id TEXT NOT NULL DEFAULT '';
