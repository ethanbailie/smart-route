ALTER TABLE sessions ADD COLUMN recovery_policy TEXT NOT NULL DEFAULT 'none';
ALTER TABLE sessions ADD COLUMN checkpoint_mode TEXT NOT NULL DEFAULT 'explicit';
ALTER TABLE sessions ADD COLUMN rebuild_plan_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE sessions ADD COLUMN epoch INTEGER NOT NULL DEFAULT 1;
ALTER TABLE sessions ADD COLUMN recovery_state TEXT NOT NULL DEFAULT 'idle';
ALTER TABLE sessions ADD COLUMN recovery_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN recovery_after TIMESTAMP;
ALTER TABLE sessions ADD COLUMN recovery_error TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN latest_checkpoint_id TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN session_epoch INTEGER NOT NULL DEFAULT 0;
CREATE TABLE session_checkpoints (
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id), epoch INTEGER NOT NULL,
 sequence INTEGER NOT NULL, adapter TEXT NOT NULL, location TEXT NOT NULL DEFAULT '', checksum TEXT NOT NULL DEFAULT '',
 size INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL, created_at TIMESTAMP NOT NULL, expires_at TIMESTAMP, error TEXT NOT NULL DEFAULT '',
 UNIQUE(session_id,sequence)
);
CREATE INDEX checkpoints_session_idx ON session_checkpoints(session_id,sequence DESC);
CREATE INDEX checkpoints_expiry_idx ON session_checkpoints(expires_at) WHERE expires_at IS NOT NULL;
