# Fly Machines provider

This adapter targets the public Fly Machines API at `https://api.machines.dev`.
It follows Fly's current documented API assumptions: every Machine belongs to an
existing app; bearer-token authentication is required; create/list/get/wait and
forced delete are under `/v1/apps/{app}/machines`; Machine metadata is filterable;
`config.env`, `config.init.exec`, `config.guest`, and `config.restart` configure a
Machine; and `started`, `stopped`, `suspended`, and `destroyed` are wait/lifecycle
states. Fly documents per-action rate limits, so transient, capacity, timeout,
rate-limit, and 5xx responses are retried with bounded exponential backoff.

References (checked 2026-09-03):

- https://fly.io/docs/machines/api/working-with-machines-api/
- https://fly.io/docs/machines/api/machines-resource/
- https://fly.io/docs/machines/runtime-environment/

Configuration keys are `app` (required), `token_env` (default
`FLY_API_TOKEN`), `api_url`, `image`, `request_timeout`, `startup_timeout`,
`max_retries`, and `retry_backoff`. `image` must use a digest or non-`latest`
tag so workers are reproducible. The app must already exist and its registry
must permit pulling that image. Fly Machines have outbound networking by
default; this adapter intentionally publishes no inbound service.

Bootstrap credentials are only placed in the Machine environment. They are
never copied to metadata, returned sandbox data, or provider error messages.
Use a narrowly scoped, short-lived bootstrap token and HTTPS control-plane URL.

## Live end-to-end test

The build-tagged test starts a real control plane, mints a production bootstrap
credential, provisions the configured worker image, observes registration, runs
`/bin/echo`, checks the stored result, then drains and terminates the Machine
through `SandboxReaper` and verifies it is gone. It is never part of default CI.

Prerequisites:

1. Create a Fly app and publish a versioned image containing
   `smart-route-worker` (with `/bin/echo`) to a registry that app can pull.
2. Choose a fixed local listen address and route a public **HTTPS** tunnel to it.
   The tunnel must preserve worker HTTP requests for the duration of the test.
3. Export `FLY_API_TOKEN` (app-scoped deploy token), `FLY_INTEGRATION_APP`,
   `FLY_INTEGRATION_IMAGE`, `FLY_INTEGRATION_LISTEN` (for example
   `0.0.0.0:18080`), and `FLY_INTEGRATION_CONTROL_PLANE_URL` (the tunnel URL).
4. Run `go test -tags=integration ./internal/sandbox/fly -run TestLiveWorkerE2E -v`.

This creates a billable Fly Machine. Cleanup is registered immediately after
creation, and the Machine also carries a ten-minute maximum-lifetime marker, but
operators should still verify the app has no `smart-route-managed` Machines if
the test process or tunnel is forcibly interrupted.
