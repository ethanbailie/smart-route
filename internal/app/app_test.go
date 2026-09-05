package app

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethan/smart-route/internal/config"
)

func TestRunShutsDownAndClosesDatabase(t *testing.T) {
	c := config.Default()
	c.Database.DSN = filepath.Join(t.TempDir(), "route.db")
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	c.HTTP.Listen = ln.Addr().String()
	ln.Close()
	c.HTTP.PublicURL = "http://" + c.HTTP.Listen
	c.HTTP.ShutdownTimeout = config.Duration(time.Second)
	a, e := Build(c)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case e = <-done:
		if e != nil && !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete")
	}
	if _, e = a.DB.ListWorkers(context.Background()); e == nil {
		t.Fatal("database remained open")
	}
}
func TestDoctorRejectsMissingReferencesWithoutJobs(t *testing.T) {
	c := config.Default()
	c.Database.DSN = filepath.Join(t.TempDir(), "route.db")
	c.Upstreams = map[string]config.Upstream{"llm": {Enabled: true, CredentialRef: "missing"}}
	if e := Doctor(context.Background(), c); e == nil {
		t.Fatal("doctor accepted a missing credential reference")
	}
}
