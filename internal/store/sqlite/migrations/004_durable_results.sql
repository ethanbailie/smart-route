ALTER TABLE job_events ADD COLUMN worker_sequence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE job_events ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX events_attempt_idempotency_idx ON job_events(attempt_id,idempotency_key) WHERE idempotency_key<>'';
CREATE UNIQUE INDEX events_attempt_worker_sequence_idx ON job_events(attempt_id,worker_sequence) WHERE worker_sequence>0;
CREATE TABLE job_results (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id), attempt_id TEXT NOT NULL REFERENCES job_attempts(id),
 status_code INTEGER NOT NULL, data BLOB, artifact_key TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL,
 created_at TIMESTAMP NOT NULL
);
