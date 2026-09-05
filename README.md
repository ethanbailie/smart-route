# smart-route

smart-route is a provider-agnostic Go control plane and worker runtime for
routing generic command, HTTP, and upstream jobs to ephemeral sandboxes. Jobs
are claimed over an outbound long-poll protocol, so workers need no inbound
ports. SQLite durably records jobs, attempts, leases, events, and results across
control-plane restarts.

Routing is based on declared capabilities, health, region, cost, and documented
upstream availability. Upstream credentials are referenced by logical ID and
resolved only inside the selected worker; they are not stored in job payloads or
used to evade provider quotas, access controls, or terms of service.

Release packaging, a Docker Compose quickstart, Kubernetes topology,
persistence limitations, and recovery behavior are documented in
[docs/deployment.md](docs/deployment.md).

## Integration and chaos suite

Docker must be available locally. One command builds a uniquely versioned worker image, starts the real orchestrator against SQLite, runs the multi-worker and failure scenarios, and removes its containers/image:

```sh
SMART_ROUTE_INTEGRATION=1 go test -race -tags=integration ./internal/integration -count=1 -timeout=3m
```

The default `go test ./...` suite does not require Docker or cloud credentials.
