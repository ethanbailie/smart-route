package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemCheckpointRoundTripExcludesCredentials(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "state.db"), []byte("state"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, ".env"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	archive, err := (FilesystemCheckpoint{Roots: []string{src}}).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = (FilesystemCheckpoint{Roots: []string{dst}}).Restore(context.Background(), archive); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dst, "state.db"))
	if err != nil || string(data) != "state" {
		t.Fatalf("state=%q %v", data, err)
	}
	if _, err = os.Stat(filepath.Join(dst, ".env")); !os.IsNotExist(err) {
		t.Fatalf("credential restored: %v", err)
	}
}
