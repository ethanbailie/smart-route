package fly

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/sandbox/providertest"
)

func TestProviderContract(t *testing.T) {
	var mu sync.Mutex
	machines := map[string]machine{}
	next := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/v1/apps/test/machines")
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && path == "":
			var input createRequest
			_ = json.NewDecoder(r.Body).Decode(&input)
			if input.Config.Env["SMART_ROUTE_BOOTSTRAP_TOKEN"] != "secret" {
				t.Errorf("bootstrap token missing from env")
			}
			for _, value := range input.Config.Metadata {
				if strings.Contains(value, "secret") {
					t.Errorf("bootstrap token leaked into metadata")
				}
			}
			next++
			id := string(rune('0' + next))
			m := machine{ID: "machine-" + id, State: "created", Region: input.Region, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
			m.Config.Image = input.Config.Image
			m.Config.Metadata = input.Config.Metadata
			machines[m.ID] = m
			_ = json.NewEncoder(w).Encode(m)
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/wait"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/wait")
			m := machines[id]
			m.State = "started"
			machines[id] = m
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && path == "":
			out := []machine{}
			for _, m := range machines {
				match := true
				for key, values := range r.URL.Query() {
					if strings.HasPrefix(key, "metadata.") && m.Config.Metadata[strings.TrimPrefix(key, "metadata.")] != values[0] {
						match = false
					}
				}
				if match {
					out = append(out, m)
				}
			}
			_ = json.NewEncoder(w).Encode(out)
		case r.Method == http.MethodDelete:
			id := strings.TrimPrefix(path, "/")
			m, ok := machines[id]
			if !ok {
				http.NotFound(w, r)
				return
			}
			m.State = "destroyed"
			machines[id] = m
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	providertest.Run(t, func(t *testing.T) sandbox.Provider {
		p, err := newProvider(Config{App: "test", Token: "test-token", APIURL: server.URL, Image: "worker:v1", RequestTimeout: time.Second, StartupTimeout: time.Second}, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}
