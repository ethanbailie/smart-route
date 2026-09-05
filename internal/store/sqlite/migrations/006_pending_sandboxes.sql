CREATE TABLE sandboxes_new (
 id TEXT PRIMARY KEY,
 worker_id TEXT,
 capabilities_json TEXT NOT NULL,
 state TEXT NOT NULL,
 created_at TIMESTAMP NOT NULL,
 provider TEXT NOT NULL DEFAULT '',
 external_id TEXT NOT NULL DEFAULT '',
 updated_at TIMESTAMP,
 drain_at TIMESTAMP
);
INSERT INTO sandboxes_new SELECT id,worker_id,capabilities_json,state,created_at,provider,external_id,updated_at,drain_at FROM sandboxes;
DROP TABLE sandboxes;
ALTER TABLE sandboxes_new RENAME TO sandboxes;
CREATE INDEX sandboxes_state_idx ON sandboxes(state);
