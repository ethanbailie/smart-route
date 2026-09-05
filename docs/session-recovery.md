# Session recovery

Sessions default to `recovery_policy: none`: worker loss fails the session and unfinished jobs closed. This fail-closed default prevents accidental replay.

`checkpoint` restores the newest checksum-valid durable checkpoint, falling back to an older valid checkpoint if necessary. Checkpoints can be explicit or after each successful job. The `application` strategy exports configured worker paths as a portable archive while excluding credentials; the `provider_snapshot` strategy invokes the selected provider's native snapshot capability and startup rejects incompatible providers. A replacement remains non-active until its worker durably acknowledges successful restore. `rebuild` provisions a clean replacement and runs only the declared replay-safe steps in order. Every step requires an idempotency key, and activation waits for successful validation; failure leaves the session lost and blocks remaining work.

Every recovery increments a durable epoch. Workers bind to exactly one epoch, and every authenticated worker mutation rejects a stale epoch. The durable controller uses deterministic replacement IDs, adopts a registered replacement after restart, provisions at most one replacement per epoch, and applies bounded exponential backoff.

Recovery stages are stored as append-only session events and exposed by the recovery-events API. Exhausting the configured attempt limit records a terminal `recovery_failed` result instead of retrying indefinitely.

Checkpoint garbage collection always removes corrupt and partial records, sweeps orphaned files, and applies `checkpoint_ttl`. `retain_latest` protects the newest usable checkpoints from TTL deletion, while the conservative default `delete_on_close: false` retains them when a session closes. Enabling delete-on-close removes all checkpoints for permanently closed or lost sessions and transactionally repairs the session's latest-checkpoint pointer before backing objects are deleted.

The filesystem adapter writes partial artifacts atomically, validates SHA-256 on restore, and garbage-collects expired, corrupt, or partial records. Archive producers must exclude `.env`, SSH/AWS credentials, secret directories, provider tokens, and injected runtime credentials. Restore injects fresh credentials separately.
