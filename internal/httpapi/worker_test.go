package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/store/sqlite"
)

func workerRequest(t *testing.T, client *http.Client, method, url, worker, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if worker != "" {
		req.Header.Set("X-Smart-Route-Worker-ID", worker)
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func responseData(t *testing.T, res *http.Response) map[string]any {
	t.Helper()
	defer res.Body.Close()
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Data
}

func TestWorkerCancelsSessionLostAttempt(t *testing.T) {
	if !workerShouldCancel(domain.JobSessionLost) {
		t.Fatal("session_lost attempt was not canceled")
	}
}

func TestWorkerProtocolLifecycle(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	api := New(db, Config{RequestTimeout: 5 * time.Second, MaxClaimWait: 3 * time.Second, LeaseDuration: time.Minute, MaxResultBytes: 4})
	bootstrap, err := api.MintBootstrapToken(context.Background(), "sandbox-1", "localdocker", "main", domain.Capabilities{Capabilities: []string{"build"}, Labels: map[string]string{"pool": "main"}, Architecture: domain.ArchitectureAMD64})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	client := server.Client()

	registrationBody := `{"bootstrap_token":"` + bootstrap + `","instance_id":"550e8400-e29b-41d4-a716-446655440000","sandbox_id":"sandbox-1","sandbox_provider":"localdocker","worker_version":"1.2.3","protocol_version":"1","max_concurrency":1,"capabilities":{"capabilities":["build"],"labels":{"pool":"main"},"architecture":"amd64"},"sandbox_metadata":{"runtime":"docker"}}`
	res := workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/register", "", "", registrationBody)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d", res.StatusCode)
	}
	registration := responseData(t, res)
	worker, token := registration["worker_id"].(string), registration["session_token"].(string)
	res = workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/register", "", "", registrationBody)
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("replayed bootstrap status = %d", res.StatusCode)
	}

	bad := workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/claim", worker, "wrong", `{}`)
	bad.Body.Close()
	if bad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad session status = %d", bad.StatusCode)
	}

	claimed := make(chan *http.Response, 1)
	go func() {
		claimed <- workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/claim", worker, token, `{"wait_seconds":2}`)
	}()
	time.Sleep(50 * time.Millisecond)
	submit := workerRequest(t, client, http.MethodPost, server.URL+"/v1/jobs", "", "", `{"idempotency_key":"worker-flow-1","kind":"build","payload":{},"constraints":{"capabilities":["build"],"labels":{"pool":"main"},"architecture":"amd64"},"timeout_seconds":60,"retry":{"max_attempts":2}}`)
	if submit.StatusCode != http.StatusCreated {
		t.Fatalf("submit status = %d", submit.StatusCode)
	}
	submit.Body.Close()
	res = <-claimed
	if res.StatusCode != http.StatusOK {
		t.Fatalf("claim status = %d", res.StatusCode)
	}
	claim := responseData(t, res)
	attempt := claim["attempt"].(map[string]any)
	attemptID := attempt["id"].(string)
	jobID := claim["job"].(map[string]any)["id"].(string)

	heartbeat := workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/heartbeat", worker, token, `{"active_attempts":["`+attemptID+`"],"available_slots":0,"sandbox_metadata":{"runtime":"docker"},"health":{"status":"ok"},"upstreams":{"origin":"healthy"}}`)
	if heartbeat.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat status = %d", heartbeat.StatusCode)
	}
	heartbeatData := responseData(t, heartbeat)
	token = heartbeatData["session_token"].(string)
	for action, step := range map[string]struct {
		body string
		want int
	}{"renew": {`{}`, 200}, "events": {`{"type":"progress","data":{"percent":"50"}}`, 202}} {
		res = workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/attempts/"+attemptID+"/"+action, worker, token, step.body)
		if res.StatusCode != step.want {
			t.Fatalf("%s status = %d", action, res.StatusCode)
		}
		res.Body.Close()
	}
	res = workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/attempts/"+attemptID+"/result", worker, token, `{"status_code":200,"data":"dG9vIGxvbmc="}`)
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized result status = %d", res.StatusCode)
	}
	res.Body.Close()
	res = workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/attempts/"+attemptID+"/result", worker, token, `{"status_code":200,"data":"b2s="}`)
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("result status = %d", res.StatusCode)
	}
	res.Body.Close()
	res = workerRequest(t, client, http.MethodPost, server.URL+"/v1/worker/attempts/"+attemptID+"/complete", worker, token, `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("complete status = %d", res.StatusCode)
	}
	res.Body.Close()
	res, err = client.Get(server.URL + "/v1/jobs/" + jobID + "/result")
	if err != nil {
		t.Fatal(err)
	}
	result := responseData(t, res)
	if result["data"] != "b2s=" || result["status_code"] != float64(200) {
		t.Fatalf("result = %#v", result)
	}
}
