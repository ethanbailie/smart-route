ALTER TABLE sessions ADD COLUMN restore_acknowledged INTEGER NOT NULL DEFAULT 0;
CREATE TABLE session_recovery_events (
 id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id), epoch INTEGER NOT NULL,
 stage TEXT NOT NULL, message TEXT NOT NULL DEFAULT '', occurred_at TIMESTAMP NOT NULL
);
CREATE INDEX recovery_events_session_idx ON session_recovery_events(session_id,occurred_at,id);
