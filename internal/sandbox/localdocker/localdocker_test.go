package localdocker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
)

type response struct {
	output string
	err    error
}
type recordingRunner struct {
	calls     [][]string
	responses []response
}

func (r *recordingRunner) Run(_ context.Context, binary string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{binary}, args...))
	result := r.responses[0]
	r.responses = r.responses[1:]
	return []byte(result.output), result.err
}

func TestProvisionAndExternalTermination(t *testing.T) {
	runner := &recordingRunner{responses: []response{{output: "container-id\n"}, {output: "Error: No such container", err: errors.New("exit 1")}, {output: "Error: No such container", err: errors.New("exit 1")}}}
	provider, err := newProvider(Config{Image: "worker:v1"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	created, err := provider.Create(context.Background(), sandbox.CreateSpec{WorkerID: "worker-1", ControlPlaneURL: "https://control.example", BootstrapToken: "token", WorkerMaxConcurrency: 3, Image: "override:v2", CPUClass: "2", MemoryClass: "1g", Architecture: "arm64", Environment: map[string]domain.CredentialRefID{"API_KEY": "credential-1"}, BootstrapCommand: []string{"worker", "start"}, BootstrapArtifact: "artifact-1", MaxLifetime: time.Hour, Labels: map[string]string{"pool": "dev"}, Capabilities: domain.Capabilities{Capabilities: []string{"shell"}, Labels: map[string]string{"pool": "dev"}, Upstreams: []string{"llm"}, Region: "west"}})
	if err != nil {
		t.Fatal(err)
	}
	command := strings.Join(runner.calls[0], " ")
	for _, wanted := range []string{"docker run --detach", "smart-route.managed=true", "SMART_ROUTE_CONTROL_PLANE_URL=https://control.example", "SMART_ROUTE_BOOTSTRAP_TOKEN=token", "SMART_ROUTE_CAPABILITIES=shell", "SMART_ROUTE_LABELS={\"pool\":\"dev\"}", "SMART_ROUTE_UPSTREAMS=llm", "SMART_ROUTE_REGION=west", "SMART_ROUTE_MAX_CONCURRENCY=3", "smart-route.labels={\"pool\":\"dev\"}", "--platform linux/arm64", "--cpus 2", "--memory 1g", "SMART_ROUTE_ENV_REF_API_KEY=credential-1", "SMART_ROUTE_BOOTSTRAP_ARTIFACT=artifact-1", "override:v2 worker start"} {
		if !strings.Contains(command, wanted) {
			t.Errorf("command %q does not contain %q", command, wanted)
		}
	}
	observed, err := provider.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State != sandbox.StateTerminated {
		t.Fatalf("state = %q, want terminated", observed.State)
	}
	if err = provider.Terminate(context.Background(), created.ID); err != nil {
		t.Fatalf("idempotent terminate: %v", err)
	}
}
