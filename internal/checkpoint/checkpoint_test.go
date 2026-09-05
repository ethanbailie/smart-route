package checkpoint

import (
	"context"
	"errors"
	"github.com/ethan/smart-route/internal/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFilesystemIntegrityAndSecretExclusions(t *testing.T) {
	a := Filesystem{Root: t.TempDir()}
	cp := domain.Checkpoint{ID: "cp"}
	location, sum, size, err := a.Save(context.Background(), cp, strings.NewReader("state"))
	if err != nil || size != 5 {
		t.Fatal(location, sum, size, err)
	}
	cp.Location, cp.Checksum = location, sum
	r, err := a.Open(context.Background(), cp)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err = os.WriteFile(location, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if r, err = a.Open(context.Background(), cp); !errors.Is(err, ErrCorrupt) {
		if r != nil {
			r.Close()
		}
		t.Fatalf("corruption=%v", err)
	}
	for _, name := range []string{"repo/.env", "home/.ssh/id_rsa", "x/.aws/credentials", "run/secrets/token"} {
		if !UnsafePath(name) {
			t.Errorf("did not exclude %s", name)
		}
	}
	orphan := filepath.Join(a.Root, "orphan.checkpoint")
	if err = os.WriteFile(orphan, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err = os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}
	if err = a.SweepOrphans(context.Background(), map[string]bool{}, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan retained: %v", err)
	}
}
