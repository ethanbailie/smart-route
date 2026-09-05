package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethan/smart-route/internal/checkpoint"
	"github.com/ethan/smart-route/internal/config"
	"github.com/ethan/smart-route/internal/controller"
	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/httpapi"
	"github.com/ethan/smart-route/internal/sandbox"
	"github.com/ethan/smart-route/internal/sandbox/fly"
	"github.com/ethan/smart-route/internal/sandbox/localdocker"
	"github.com/ethan/smart-route/internal/store/sqlite"
	"github.com/ethan/smart-route/internal/telemetry"
	"golang.org/x/sync/errgroup"
)

type Application struct {
	Config      config.Config
	DB          *sqlite.DB
	Server      *http.Server
	controllers []func(context.Context) error
	startup     []func(context.Context) error
}

func Build(c config.Config) (*Application, error) {
	db, e := sqlite.Open(c.Database.DSN)
	if e != nil {
		return nil, fmt.Errorf("open database: %w", e)
	}
	tel := telemetry.New(telemetry.Config{Enabled: c.Telemetry.Enabled, Metrics: c.Telemetry.Metrics, Tracing: c.Telemetry.Tracing, Logger: slog.Default()})
	providerConfig := make(map[string]sandbox.ProviderConfig, len(c.Providers))
	for n, p := range c.Providers {
		providerConfig[n] = sandbox.ProviderConfig{Type: p.Type, Config: p.Config}
	}
	registry, e := sandbox.NewRegistry(providerConfig, providerFactories())
	if e != nil {
		db.Close()
		return nil, e
	}
	if c.Recovery.Strategy == "provider_snapshot" {
		for _, pool := range c.Pools {
			provider, x := registry.Get(pool.Provider)
			if x != nil {
				db.Close()
				return nil, x
			}
			if _, ok := provider.(sandbox.Snapshotter); !ok {
				db.Close()
				return nil, fmt.Errorf("provider %s does not support configured native snapshot recovery", pool.Provider)
			}
		}
	}
	filesystem := checkpoint.Filesystem{Root: c.Recovery.CheckpointDirectory}
	var cp checkpoint.Adapter = filesystem
	if c.Recovery.Strategy == "provider_snapshot" {
		cp = checkpoint.ProviderSnapshot{Backing: filesystem}
	}
	apiConfig := httpapi.Config{RequestTimeout: time.Duration(c.HTTP.RequestTimeout), ReadTimeout: time.Duration(c.HTTP.ReadTimeout), WriteTimeout: time.Duration(c.HTTP.WriteTimeout), IdleTimeout: time.Duration(c.HTTP.IdleTimeout), ShutdownTimeout: time.Duration(c.HTTP.ShutdownTimeout), HeartbeatInterval: time.Duration(c.Jobs.HeartbeatInterval), LeaseDuration: time.Duration(c.Jobs.LeaseDuration), WorkerTimeout: time.Duration(c.Jobs.WorkerTimeout), MaxClaimWait: time.Duration(c.Jobs.MaxClaimWait), BootstrapTokenTTL: time.Duration(c.Auth.BootstrapTokenTTL), WorkerSessionTTL: time.Duration(c.Auth.WorkerSessionTTL), PublicAuthToken: c.AuthToken(), RequireTLS: c.TLS.Required, InsecureLocalMode: c.Auth.InsecureLocal, InlineResultBytes: c.Jobs.InlineResultBytes, MaxResultBytes: c.Jobs.MaxResultBytes, MaxEvents: c.Jobs.MaxEvents, Telemetry: tel, CheckpointAdapter: cp, CheckpointTTL: time.Duration(c.Recovery.CheckpointTTL), RecoveryBackoff: time.Duration(c.Recovery.BackoffBase), Providers: registry}
	api := httpapi.New(db, apiConfig)
	server := api.HTTPServer(c.HTTP.Listen, apiConfig)
	a := &Application{Config: c, DB: db, Server: server}
	add := func(fn func(context.Context) error) { a.controllers = append(a.controllers, fn) }
	lease := controller.NewLeaseReaper(db, nil)
	lease.Observe = tel.LeaseExpired
	add(func(ctx context.Context) error { return lease.Start(ctx, time.Duration(c.Controllers.LeaseReaper)) })
	timeouts := controller.NewJobTimeouts(db, nil)
	add(func(ctx context.Context) error { return timeouts.Start(ctx, time.Duration(c.Controllers.JobTimeouts)) })
	sessions := controller.NewSessionExpiry(db, nil)
	add(func(ctx context.Context) error { return sessions.Start(ctx, time.Duration(c.Controllers.JobTimeouts)) })
	health := controller.NewWorkerHealth(db, controller.WorkerHealthConfig{SuspectAfter: time.Duration(c.Controllers.WorkerSuspectAfter), DeadAfter: time.Duration(c.Controllers.WorkerDeadAfter)}, nil)
	add(func(ctx context.Context) error { return health.Start(ctx, time.Duration(c.Controllers.WorkerHealth)) })
	reconcile := controller.NewSandboxReconciler(db, registry, controller.ReconcileConfig{OwnerLabel: c.Controllers.OwnerLabel, OwnerValue: c.Controllers.OwnerValue, Orphans: controller.OrphanPolicy(c.Controllers.Orphans), MaxLifetime: time.Duration(c.Controllers.MaxLifetime), DrainGrace: time.Duration(c.Controllers.DrainGrace)}, nil)
	a.startup = append(a.startup, reconcile.Run)
	add(func(ctx context.Context) error { return reconcile.Start(ctx, time.Duration(c.Controllers.Reconciler)) })
	reaper := controller.NewSandboxReaper(db, registry, controller.ReaperConfig{IdleAfter: time.Duration(c.Controllers.IdleAfter), DrainGrace: time.Duration(c.Controllers.DrainGrace), MinimumWarm: c.Controllers.MinimumWarm}, nil)
	add(func(ctx context.Context) error { return reaper.Start(ctx, time.Duration(c.Controllers.Reaper)) })
	pools := make([]controller.SandboxPool, 0, len(c.Pools))
	for _, p := range c.Pools {
		caps := domain.Capabilities{Capabilities: append([]string(nil), p.Capabilities...), Labels: p.Labels, Architecture: domain.Architecture(p.Architecture), Region: p.Region, Upstreams: append([]string(nil), p.Upstreams...), ExecutorKinds: executorKinds(p.ExecutorKinds)}
		env := map[string]domain.CredentialRefID{}
		for k, v := range p.Environment {
			env[k] = domain.CredentialRefID(v)
		}
		pools = append(pools, controller.SandboxPool{Name: p.Name, Provider: p.Provider, Create: sandbox.CreateSpec{ControlPlaneURL: c.HTTP.PublicURL, WorkerMaxConcurrency: p.WorkerConcurrency, Image: p.Image, Template: p.Template, CPUClass: p.CPUClass, MemoryClass: p.MemoryClass, Architecture: domain.Architecture(p.Architecture), Region: p.Region, Environment: env, BootstrapCommand: p.BootstrapCommand, BootstrapArtifact: p.BootstrapArtifact, MaxLifetime: time.Duration(p.MaxLifetime)}, Capabilities: caps, Labels: p.Labels, MinReplicas: p.MinReplicas, MaxReplicas: p.MaxReplicas, WorkerConcurrency: p.WorkerConcurrency, IdleTTL: time.Duration(p.IdleTTL), StartupTimeout: time.Duration(p.StartupTimeout), ScaleUpCooldown: time.Duration(p.ScaleUpCooldown), ScaleDownCooldown: time.Duration(p.ScaleDownCooldown), ScaleDownStabilize: time.Duration(p.ScaleDownStabilize), Cost: p.Cost, Region: p.Region})
	}
	auto := controller.NewQueueAutoscaler(db, registry, pools, tel.ObserveAutoscaler, nil)
	auto.BootstrapTokens = api
	auto.BootstrapTokenTTL = time.Duration(c.Auth.BootstrapTokenTTL)
	auto.Limits = controller.AutoscalerLimits{MaxScaleUpPerRun: c.Controllers.MaxScaleUpPerRun, ProvisioningConcurrency: c.Controllers.ProvisioningConcurrency, MaxTotalSandboxes: c.Controllers.MaxTotalSandboxes, MaxSandboxesByProvider: c.Controllers.MaxSandboxesByProvider, ProviderBackoffBase: time.Duration(c.Controllers.ProviderBackoffBase), ProviderBackoffMax: time.Duration(c.Controllers.ProviderBackoffMax)}
	add(func(ctx context.Context) error { return auto.Start(ctx, time.Duration(c.Controllers.Autoscaler)) })
	recovery := controller.NewRecoveryController(db, registry, pools, controller.RecoveryConfig{BackoffBase: time.Duration(c.Recovery.BackoffBase), BackoffMax: time.Duration(c.Recovery.BackoffMax), MaxAttempts: c.Recovery.MaxAttempts}, nil)
	recovery.Checkpoints = map[string]checkpoint.Adapter{cp.Name(): cp}
	recovery.Tokens = api
	a.startup = append(a.startup, recovery.Run)
	add(func(ctx context.Context) error { return recovery.Start(ctx, time.Duration(c.Recovery.Interval)) })
	gc := &controller.CheckpointGC{Store: db, Adapters: recovery.Checkpoints, RetainLatest: c.Recovery.RetainLatest, DeleteOnClose: c.Recovery.DeleteOnClose}
	add(func(ctx context.Context) error { return gc.Start(ctx, time.Duration(c.Recovery.Interval)) })
	return a, nil
}

func (a *Application) Run(ctx context.Context) error {
	// Migrations completed in Build. Recover provider state before advertising
	// readiness or allowing autoscaling: migrate -> reconcile -> serve.
	for _, initialize := range a.startup {
		if e := initialize(ctx); e != nil {
			_ = a.DB.Close()
			return fmt.Errorf("startup reconciliation: %w", e)
		}
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, run := range a.controllers {
		run := run
		g.Go(func() error {
			e := run(ctx)
			if errors.Is(e, context.Canceled) {
				return nil
			}
			return e
		})
	}
	g.Go(func() error {
		var e error
		if a.Config.TLS.CertFile != "" {
			e = a.Server.ListenAndServeTLS(a.Config.TLS.CertFile, a.Config.TLS.KeyFile)
		} else {
			e = a.Server.ListenAndServe()
		}
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	})
	g.Go(func() error {
		<-ctx.Done()
		drain, cancel := context.WithTimeout(context.Background(), time.Duration(a.Config.HTTP.ShutdownTimeout))
		defer cancel()
		return a.Server.Shutdown(drain)
	})
	e := g.Wait()
	closeErr := a.DB.Close()
	return errors.Join(e, closeErr)
}

func executorKinds(values []string) []domain.ExecutorKind {
	out := make([]domain.ExecutorKind, len(values))
	for i, value := range values {
		out[i] = domain.ExecutorKind(value)
	}
	return out
}

func Serve(c config.Config) error {
	a, e := Build(c)
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return a.Run(ctx)
}

// Doctor checks configuration references and dependencies without creating jobs or sandboxes.
func Doctor(ctx context.Context, c config.Config) error {
	if e := c.Validate(); e != nil {
		return e
	}
	db, e := sqlite.Open(c.Database.DSN)
	if e != nil {
		return fmt.Errorf("database: %w", e)
	}
	defer db.Close()
	providerConfig := map[string]sandbox.ProviderConfig{}
	for n, p := range c.Providers {
		providerConfig[n] = sandbox.ProviderConfig{Type: p.Type, Config: p.Config}
	}
	r, e := sandbox.NewRegistry(providerConfig, providerFactories())
	if e != nil {
		return e
	}
	for _, name := range r.Names() {
		p, _ := r.Get(name)
		if _, e = p.List(ctx, sandbox.Filter{}); e != nil {
			return fmt.Errorf("provider %s: %w", name, e)
		}
	}
	for ref, names := range c.Secrets.Environment {
		for key, variable := range names {
			if _, ok := os.LookupEnv(variable); !ok {
				return fmt.Errorf("secret reference %s key %s: environment variable is not set", ref, key)
			}
		}
	}
	return nil
}

func providerFactories() map[string]sandbox.Factory {
	return map[string]sandbox.Factory{"localdocker": localdocker.Factory, "fly": fly.Factory}
}
