ALTER TABLE workers ADD COLUMN session_expires_at TIMESTAMP;
CREATE TABLE bootstrap_tokens (
 token_hash TEXT PRIMARY KEY,
 sandbox_id TEXT NOT NULL,
 sandbox_provider TEXT NOT NULL,
 pool TEXT NOT NULL DEFAULT '',
 capability_hash TEXT NOT NULL DEFAULT '',
 expires_at TIMESTAMP NOT NULL,
 consumed_at TIMESTAMP
);
CREATE INDEX bootstrap_tokens_sandbox_idx ON bootstrap_tokens(sandbox_id);
