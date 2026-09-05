# Deployment and recovery

## Release artifacts

`make images VERSION=v0.1.0` reproducibly builds two independent, minimal images:

- `smart-route:v0.1.0` is the control plane.
- `smart-route-worker:v0.1.0` is the untrusted job worker used by sandbox providers.

Both images run as uid/gid `65532`, contain only the binary and CA certificates, and accept a read-only root filesystem. `VERSION`, `GIT_SHA`, and `BUILT_AT` are embedded with build arguments. Inspect control-plane metadata with `smart-route version` or `GET /versionz`; worker registrations include the version, Git SHA, and protocol version.

## Local quickstart

From `deployments/`, run `docker compose up --build`. The control plane listens on port 8080, stores SQLite state in the named `smart-route-data` volume, and reports liveness at `/healthz` and readiness at `/readyz`. Use a separately versioned worker image in a provider configuration when executing jobs.

## Production topology

The supported durable store is currently SQLite only. Run exactly one control-plane replica with its database and WAL files on one persistent, ReadWriteOnce volume. SQLite is not a shared-database HA design; horizontal control-plane replicas and PostgreSQL are not currently supported. Put TLS and authentication at the ingress (or configure the built-in TLS/auth options), keep the API reachable from cloud workers, and pin worker images by digest or immutable tag.

`deployments/kubernetes.yaml` demonstrates the required `Recreate` rollout and persistent volume. Replace the image and provide a `smart-route` ConfigMap containing a production configuration whose database DSN is `/var/lib/smart-route/smart-route.db`. Supply provider and API credentials through environment-backed secret references, never the ConfigMap.

## Startup and recovery contract

On each start the control plane opens the same SQLite file, applies embedded migrations transactionally, then performs one provider reconciliation before binding the HTTP server and starting autoscaling. Consequently `/readyz` is unavailable until migration and reconciliation succeed. Normal controllers then run continuously.

Destroying and recreating the control-plane container does not destroy queued jobs, attempts, results, worker identities, leases, or sandbox records when the volume is retained. Workers retry HTTP operations and register again after their session expires or the server becomes reachable. Worker health follows the configured suspect/dead thresholds; dead-worker leases are expired and may retry, while stale completions are rejected transactionally. Provider reconciliation terminates or adopts discovered orphans according to `controllers.orphans`, updates known sandbox state, and removes completed terminating records.

Back up SQLite safely (including WAL state) using SQLite's online backup mechanism or by stopping the process and copying the database together with `-wal` and `-shm` files. Restore while the control plane is stopped, then start a single replica and allow reconciliation to complete before sending traffic.

## CI and release gates

CI enforces formatting, vet, unit tests, race tests, both container builds, and the credential-free Docker integration/recovery suite. The Fly credential test is deliberately opt-in and isolated in a protected environment; default CI never requires cloud credentials.
