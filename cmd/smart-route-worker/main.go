package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethan/smart-route/internal/buildinfo"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/worker"
)

func main() {
	cfg, control, err := load()
	if err != nil {
		log.Fatal(err)
	}
	runner, err := worker.New(control, cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := runner.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func load() (worker.Config, worker.ControlPlane, error) {
	base := os.Getenv("SMART_ROUTE_CONTROL_PLANE_URL")
	if base == "" {
		return worker.Config{}, nil, fmt.Errorf("SMART_ROUTE_CONTROL_PLANE_URL is required")
	}
	control, err := worker.NewHTTPControlPlane(base, nil)
	if err != nil {
		return worker.Config{}, nil, err
	}
	max := intEnv("SMART_ROUTE_MAX_CONCURRENCY", 1)
	labels := stringMap(os.Getenv("SMART_ROUTE_LABELS"))
	capabilities := listEnv("SMART_ROUTE_CAPABILITIES")
	upstreams := listEnv("SMART_ROUTE_UPSTREAMS")
	allow := listEnv("SMART_ROUTE_COMMAND_ALLOWLIST")
	if len(allow) == 0 {
		allow = []string{"/bin/sh", "/bin/echo"}
	}
	secrets := secretValues()
	executors := map[string]worker.Executor{
		"command": worker.NewCommandExecutor(worker.CommandConfig{Allowlist: allow, MaxOutputBytes: int64(intEnv("SMART_ROUTE_MAX_OUTPUT_BYTES", 1<<20)), EmitChunks: boolEnv("SMART_ROUTE_EMIT_CHUNKS"), Secrets: secrets}),
		"http":    worker.NewHTTPExecutor(worker.HTTPConfig{MaxResponseBytes: int64(intEnv("SMART_ROUTE_MAX_HTTP_RESPONSE_BYTES", 1<<20))}),
	}
	caps := domain.Capabilities{Capabilities: capabilities, Labels: labels, Architecture: domain.Architecture(runtime.GOARCH), Region: os.Getenv("SMART_ROUTE_REGION"), ExecutorKinds: []domain.ExecutorKind{domain.ExecutorProcess, domain.ExecutorRemote}, Upstreams: upstreams}
	registration := worker.RegistrationRequest{BootstrapToken: os.Getenv("SMART_ROUTE_BOOTSTRAP_TOKEN"), InstanceID: instanceID(), SandboxID: envDefault("SMART_ROUTE_SANDBOX_ID", hostname()), SandboxProvider: envDefault("SMART_ROUTE_SANDBOX_PROVIDER", "standalone"), Version: buildinfo.Version, Capabilities: caps, MaxConcurrency: max, SandboxMetadata: map[string]string{"runtime": "worker", "hostname": hostname(), "git_sha": buildinfo.GitSHA, "protocol_version": buildinfo.ProtocolVersion}}
	cfg := worker.Config{Registration: registration, Executors: executors, ClaimWait: durationEnv("SMART_ROUTE_CLAIM_WAIT", 20*time.Second), ShutdownTimeout: durationEnv("SMART_ROUTE_SHUTDOWN_TIMEOUT", 30*time.Second), CancelOnShutdown: boolEnv("SMART_ROUTE_CANCEL_ON_SHUTDOWN"), Secrets: secrets, EventRetryBuffer: intEnv("SMART_ROUTE_EVENT_RETRY_BUFFER", 64)}
	if roots := listEnv("SMART_ROUTE_CHECKPOINT_PATHS"); len(roots) > 0 {
		strategy := worker.FilesystemCheckpoint{Roots: roots}
		cfg.CheckpointExport, cfg.CheckpointRestore = strategy.Export, strategy.Restore
	}
	return cfg, control, nil
}

func hostname() string {
	v, _ := os.Hostname()
	if v == "" {
		return "unknown"
	}
	return v
}
func instanceID() string {
	if v := os.Getenv("SMART_ROUTE_INSTANCE_ID"); v != "" {
		return v
	}
	sum := sha256.Sum256([]byte(hostname()))
	h := hex.EncodeToString(sum[:16])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
func envDefault(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func intEnv(k string, d int) int {
	v, err := strconv.Atoi(os.Getenv(k))
	if err != nil || v <= 0 {
		return d
	}
	return v
}
func boolEnv(k string) bool { v, _ := strconv.ParseBool(os.Getenv(k)); return v }
func durationEnv(k string, d time.Duration) time.Duration {
	v, err := time.ParseDuration(os.Getenv(k))
	if err != nil || v <= 0 {
		return d
	}
	return v
}
func listEnv(k string) []string {
	var out []string
	for _, v := range strings.Split(os.Getenv(k), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}
func stringMap(v string) map[string]string {
	out := map[string]string{}
	if v == "" {
		return out
	}
	if json.Unmarshal([]byte(v), &out) == nil {
		return out
	}
	for _, item := range strings.Split(v, ",") {
		p := strings.SplitN(item, "=", 2)
		if len(p) == 2 {
			out[strings.TrimSpace(p[0])] = strings.TrimSpace(p[1])
		}
	}
	return out
}
func secretValues() []string {
	var out []string
	for _, entry := range os.Environ() {
		p := strings.SplitN(entry, "=", 2)
		if len(p) == 2 && (strings.HasPrefix(p[0], "SMART_ROUTE_SECRET_") || strings.HasPrefix(p[0], "SMART_ROUTE_CREDENTIAL_")) {
			out = append(out, p[1])
		}
	}
	return out
}
