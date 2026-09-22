package artifact

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Filesystem struct {
	Root string
}

func (f *Filesystem) resolve(key string) (string, error) {
	if key == "" || strings.Contains(key, "\x00") || strings.Contains(key, "..") {
		return "", fmt.Errorf("invalid artifact key")
	}
	root, err := filepath.Abs(f.Root)
	if err != nil {
		return "", err
	}
	p, err := filepath.Abs(filepath.Join(root, key))
	if err != nil {
		return "", err
	}
	if p == root || !strings.HasPrefix(p, root+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid artifact key")
	}
	return p, nil
}

func (f *Filesystem) Put(_ context.Context, key string, data []byte) error {
	p, err := f.resolve(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0750); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0640)
}

func (f *Filesystem) Get(_ context.Context, key string) ([]byte, error) {
	p, err := f.resolve(key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}
