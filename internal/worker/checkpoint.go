package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ethan/smart-route/internal/checkpoint"
)

// FilesystemCheckpoint exports configured application state roots. It excludes
// credentials, symlinks, devices and sockets and restores only below those roots.
type FilesystemCheckpoint struct{ Roots []string }

func (f FilesystemCheckpoint) Export(ctx context.Context) ([]byte, error) {
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for i, root := range f.Roots {
		root = filepath.Clean(root)
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if err = ctx.Err(); err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			name := filepath.ToSlash(filepath.Join("root-"+strconv.Itoa(i), rel))
			if checkpoint.UnsafePath("/"+name) || info.Mode()&os.ModeSymlink != 0 {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if info.IsDir() {
				return tw.WriteHeader(&tar.Header{Name: name, Mode: 0700, Typeflag: tar.TypeDir})
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			h, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			h.Name = name
			h.Mode &= 0700
			r, err := os.Open(path)
			if err != nil {
				return err
			}
			if err = tw.WriteHeader(h); err != nil {
				r.Close()
				return err
			}
			_, copyErr := io.Copy(tw, r)
			return errors.Join(copyErr, r.Close())
		})
		if err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (f FilesystemCheckpoint) Restore(ctx context.Context, data []byte) error {
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(h.Name), "/")
		if len(parts) == 0 || !strings.HasPrefix(parts[0], "root-") {
			return fmt.Errorf("checkpoint: invalid root")
		}
		i, err := strconv.Atoi(strings.TrimPrefix(parts[0], "root-"))
		if err != nil || i < 0 || i >= len(f.Roots) {
			return fmt.Errorf("checkpoint: unknown root")
		}
		if len(parts) == 1 {
			continue
		}
		rel := filepath.Join(parts[1:]...)
		if checkpoint.UnsafePath("/" + filepath.ToSlash(rel)) {
			continue
		}
		root := filepath.Clean(f.Roots[i])
		target := filepath.Join(root, rel)
		if target != root && !strings.HasPrefix(target, root+string(filepath.Separator)) {
			return fmt.Errorf("checkpoint: path traversal")
		}
		if h.Typeflag == tar.TypeDir {
			if err = os.MkdirAll(target, 0700); err != nil {
				return err
			}
			continue
		}
		if h.Typeflag != tar.TypeReg {
			return fmt.Errorf("checkpoint: unsupported entry")
		}
		if err = os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(h.Mode)&0700)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, tr)
		if err = errors.Join(copyErr, out.Close()); err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
	}
}
