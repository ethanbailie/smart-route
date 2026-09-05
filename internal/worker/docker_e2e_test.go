package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/httpapi"
	"github.com/ethan/smart-route/internal/store/sqlite"
	"github.com/ethan/smart-route/pkg/client"
)

func TestDockerWorkerE2E(t *testing.T) {
	if os.Getenv("SMART_ROUTE_DOCKER_E2E") != "1" {
		t.Skip("set SMART_ROUTE_DOCKER_E2E=1 after building smart-route-worker:bai-11")
	}
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "e2e.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: httpapi.New(db, httpapi.Config{RequestTimeout: 5 * time.Second, MaxClaimWait: 2 * time.Second, HeartbeatInterval: time.Second, LeaseDuration: 4 * time.Second}).Handler()}
	go func() { _ = server.Serve(listener) }()
	defer server.Shutdown(context.Background())
	port := listener.Addr().(*net.TCPAddr).Port
	name := fmt.Sprintf("smart-route-worker-e2e-%d", time.Now().UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "host", "--name", name,
		"-e", fmt.Sprintf("SMART_ROUTE_CONTROL_PLANE_URL=http://127.0.0.1:%d", port),
		"-e", "SMART_ROUTE_INSTANCE_ID=550e8400-e29b-41d4-a716-446655440011",
		"-e", "SMART_ROUTE_SANDBOX_ID=docker-e2e", "smart-route-worker:bai-11")
	if err = command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { cancel(); _ = exec.Command("docker", "stop", "--time", "1", name).Run(); _ = command.Wait() }()
	api, err := client.New(fmt.Sprintf("http://127.0.0.1:%d", port), nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		workers, e := api.ListWorkers(context.Background())
		if e == nil && len(workers) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not register: %v", e)
		}
		time.Sleep(100 * time.Millisecond)
	}
	payload, _ := json.Marshal(map[string]any{"command": "/bin/echo", "args": []string{"docker-e2e"}})
	job, err := api.SubmitJob(context.Background(), client.SubmitJob{IdempotencyKey: name, Kind: "command", Payload: payload, Constraints: client.Constraints{ExecutorKind: "process"}, TimeoutSeconds: 10})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()
	job, err = api.WaitTerminal(waitCtx, job.ID, 100*time.Millisecond)
	if err != nil || job.State != "succeeded" {
		t.Fatalf("job state=%s err=%v", job.State, err)
	}
	events, err := api.ListEvents(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Type == "result" && event.Data["body"] == "docker-e2e\n" {
			found = true
		}
	}
	if !found {
		t.Fatal("worker result event was not recorded")
	}
}
