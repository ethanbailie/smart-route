// Package checkpoint stores portable session snapshots outside provider APIs.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethan/smart-route/internal/domain"
)

var ErrCorrupt = errors.New("checkpoint: corrupt")

type Adapter interface {
	Name() string
	Save(context.Context, domain.Checkpoint, io.Reader) (location, checksum string, size int64, err error)
	Open(context.Context, domain.Checkpoint) (io.ReadCloser, error)
	Delete(context.Context, domain.Checkpoint) error
}
type Strategy string

const (
	StrategyApplication      Strategy = "application"
	StrategyProviderSnapshot Strategy = "provider_snapshot"
)

type Strategic interface {
	Adapter
	Strategy() Strategy
}

// Filesystem is suitable for a shared durable volume. The caller supplies an
// already-filtered archive; unsafe filenames are rejected to prevent accidental
// persistence of common credentials.
type Filesystem struct{ Root string }

func (f Filesystem) Name() string       { return "filesystem" }
func (f Filesystem) Strategy() Strategy { return StrategyApplication }
func safeName(id string) bool {
	return id != "" && filepath.Base(id) == id && !strings.ContainsAny(id, "/\\")
}
func (f Filesystem) Save(ctx context.Context, c domain.Checkpoint, r io.Reader) (string, string, int64, error) {
	if !safeName(c.ID) {
		return "", "", 0, fmt.Errorf("checkpoint: unsafe id")
	}
	if err := ctx.Err(); err != nil {
		return "", "", 0, err
	}
	if err := os.MkdirAll(f.Root, 0700); err != nil {
		return "", "", 0, err
	}
	final := filepath.Join(f.Root, c.ID+".checkpoint")
	partial := final + ".partial"
	out, err := os.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", "", 0, err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, h), r)
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(partial)
		return "", "", 0, errors.Join(copyErr, closeErr)
	}
	if err = os.Rename(partial, final); err != nil {
		_ = os.Remove(partial)
		return "", "", 0, err
	}
	return final, hex.EncodeToString(h.Sum(nil)), n, nil
}
func (f Filesystem) Open(ctx context.Context, c domain.Checkpoint) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := os.Open(c.Location)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	if _, err = io.Copy(h, r); err != nil {
		r.Close()
		return nil, err
	}
	if hex.EncodeToString(h.Sum(nil)) != c.Checksum {
		r.Close()
		return nil, ErrCorrupt
	}
	if _, err = r.Seek(0, 0); err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}
func (f Filesystem) Delete(ctx context.Context, c domain.Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(c.Location)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
func (f Filesystem) SweepOrphans(ctx context.Context, live map[string]bool, before time.Time) error {
	items, e := os.ReadDir(f.Root)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	for _, item := range items {
		if e = ctx.Err(); e != nil {
			return e
		}
		path := filepath.Join(f.Root, item.Name())
		if live[path] {
			continue
		}
		if !strings.HasSuffix(item.Name(), ".partial") && !strings.HasSuffix(item.Name(), ".checkpoint") {
			continue
		}
		info, e := item.Info()
		if e != nil {
			return e
		}
		if info.ModTime().After(before) {
			continue
		}
		if e = os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	return nil
}

// UnsafePath reports paths that archive producers must omit. This deliberately
// errs toward exclusion; applications should inject credentials again at restore.
func UnsafePath(name string) bool {
	n := strings.ToLower(filepath.ToSlash(name))
	base := filepath.Base(n)
	return base == ".env" || base == "credentials" || base == "id_rsa" || base == "id_ed25519" || strings.Contains(n, "/.ssh/") || strings.Contains(n, "/.aws/") || strings.Contains(n, "/secrets/")
}

// ProviderSnapshot persists provider-native snapshot bytes using a durable
// backing adapter while selecting provider-side restoration.
type ProviderSnapshot struct{ Backing Adapter }

func (p ProviderSnapshot) Name() string       { return "provider_snapshot" }
func (p ProviderSnapshot) Strategy() Strategy { return StrategyProviderSnapshot }
func (p ProviderSnapshot) Save(ctx context.Context, c domain.Checkpoint, r io.Reader) (string, string, int64, error) {
	if p.Backing == nil {
		return "", "", 0, fmt.Errorf("checkpoint: backing adapter required")
	}
	return p.Backing.Save(ctx, c, r)
}
func (p ProviderSnapshot) Open(ctx context.Context, c domain.Checkpoint) (io.ReadCloser, error) {
	if p.Backing == nil {
		return nil, fmt.Errorf("checkpoint: backing adapter required")
	}
	return p.Backing.Open(ctx, c)
}
func (p ProviderSnapshot) Delete(ctx context.Context, c domain.Checkpoint) error {
	if p.Backing == nil {
		return fmt.Errorf("checkpoint: backing adapter required")
	}
	return p.Backing.Delete(ctx, c)
}
func (p ProviderSnapshot) SweepOrphans(ctx context.Context, live map[string]bool, before time.Time) error {
	if s, ok := p.Backing.(interface {
		SweepOrphans(context.Context, map[string]bool, time.Time) error
	}); ok {
		return s.SweepOrphans(ctx, live, before)
	}
	return nil
}
