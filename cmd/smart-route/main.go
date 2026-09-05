package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/ethan/smart-route/internal/app"
	"github.com/ethan/smart-route/internal/buildinfo"
	"github.com/ethan/smart-route/internal/config"
	"github.com/ethan/smart-route/internal/store/sqlite"
	"github.com/ethan/smart-route/pkg/client"
)

func main() {
	if e := run(os.Args[1:], os.Stdout, os.Stderr); e != nil {
		fmt.Fprintln(os.Stderr, "smart-route:", e)
		os.Exit(1)
	}
}
func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: smart-route [--config file] <serve|migrate|doctor|version|job|worker|sandbox|pool> ...")
}
func run(args []string, out, errOut io.Writer) error {
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		return json.NewEncoder(out).Encode(buildinfo.Current())
	}
	root := flag.NewFlagSet("smart-route", flag.ContinueOnError)
	root.SetOutput(errOut)
	path := root.String("config", env("SMART_ROUTE_CONFIG", "smart-route.yaml"), "YAML or TOML configuration file")
	if e := root.Parse(args); errors.Is(e, flag.ErrHelp) {
		return nil
	} else if e != nil {
		return e
	}
	rest := root.Args()
	if len(rest) == 0 {
		usage(out)
		return nil
	}
	configPath := *path
	if configPath == "smart-route.yaml" {
		if _, e := os.Stat(configPath); errors.Is(e, os.ErrNotExist) {
			configPath = ""
		}
	}
	c, e := config.Load(configPath)
	if e != nil {
		return e
	}
	ctx := context.Background()
	switch rest[0] {
	case "serve":
		return app.Serve(c)
	case "migrate":
		db, e := sqlite.Open(c.Database.DSN)
		if e != nil {
			return e
		}
		return db.Close()
	case "doctor":
		e = app.Doctor(ctx, c)
		if e == nil {
			fmt.Fprintln(out, "ok")
		}
		return e
	}
	cl, e := client.New(c.HTTP.PublicURL, &http.Client{Timeout: time.Duration(c.HTTP.RequestTimeout)})
	if e != nil {
		return e
	}
	cl.SetBearerToken(c.AuthToken())
	write := func(v any) error { return json.NewEncoder(out).Encode(v) }
	switch rest[0] {
	case "job":
		if len(rest) < 2 {
			return errors.New("job requires submit|get|events|cancel")
		}
		switch rest[1] {
		case "submit":
			fs := flag.NewFlagSet("job submit", flag.ContinueOnError)
			file := fs.String("file", "-", "JSON request file")
			if e = fs.Parse(rest[2:]); e != nil {
				return e
			}
			var r io.Reader = os.Stdin
			if *file != "-" {
				f, x := os.Open(*file)
				if x != nil {
					return x
				}
				defer f.Close()
				r = f
			}
			var req client.SubmitJob
			if e = json.NewDecoder(r).Decode(&req); e != nil {
				return e
			}
			v, e := cl.SubmitJob(ctx, req)
			if e != nil {
				return e
			}
			return write(v)
		case "get", "events", "cancel":
			if len(rest) < 3 {
				return errors.New("job id is required")
			}
			var v any
			if rest[1] == "get" {
				v, e = cl.GetJob(ctx, rest[2])
			} else if rest[1] == "events" {
				v, e = cl.ListEvents(ctx, rest[2])
			} else {
				v, e = cl.CancelJob(ctx, rest[2])
			}
			if e != nil {
				return e
			}
			return write(v)
		default:
			return errors.New("unknown job command")
		}
	case "worker":
		if len(rest) != 2 || rest[1] != "list" {
			return errors.New("usage: worker list")
		}
		v, e := cl.ListWorkers(ctx)
		if e != nil {
			return e
		}
		return write(v)
	case "sandbox":
		if len(rest) != 2 || rest[1] != "list" {
			return errors.New("usage: sandbox list")
		}
		v, e := cl.ListSandboxes(ctx)
		if e != nil {
			return e
		}
		return write(v)
	case "pool":
		if len(rest) != 2 || rest[1] != "status" {
			return errors.New("usage: pool status")
		}
		v, e := cl.AdminStatus(ctx)
		if e != nil {
			return e
		}
		return write(v.Pools)
	default:
		usage(errOut)
		return fmt.Errorf("unknown command %q", rest[0])
	}
}
func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
