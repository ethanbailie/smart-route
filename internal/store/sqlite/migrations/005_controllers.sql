ALTER TABLE jobs ADD COLUMN retry_max_elapsed_ns INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN next_attempt_at TIMESTAMP;
CREATE INDEX jobs_retry_idx ON jobs(state,next_attempt_at,created_at,id);
ALTER TABLE sandboxes ADD COLUMN provider TEXT NOT NULL DEFAULT '';
ALTER TABLE sandboxes ADD COLUMN external_id TEXT NOT NULL DEFAULT '';
ALTER TABLE sandboxes ADD COLUMN updated_at TIMESTAMP;
ALTER TABLE sandboxes ADD COLUMN drain_at TIMESTAMP;
