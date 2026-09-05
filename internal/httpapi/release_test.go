package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethan/smart-route/internal/buildinfo"
)

func TestVersionEndpoint(t *testing.T) {
	response := httptest.NewRecorder()
	New(nil, Config{}).Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/versionz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var envelope struct {
		Data buildinfo.Info `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data != buildinfo.Current() || envelope.Data.ProtocolVersion == "" {
		t.Fatalf("metadata = %#v", envelope.Data)
	}
}
