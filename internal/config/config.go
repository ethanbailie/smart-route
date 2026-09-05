package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v3"
)

// Duration accepts Go duration strings in YAML, TOML and environment values.
type Duration time.Duration

func (d *Duration) UnmarshalText(b []byte) error {
	v, e := time.ParseDuration(string(b))
	if e != nil {
		return e
	}
	*d = Duration(v)
	return nil
}
func (d Duration) MarshalText() ([]byte, error) { return []byte(time.Duration(d).String()), nil }

type Config struct {
	HTTP        HTTP                `yaml:"http" toml:"http" json:"http"`
	Database    Database            `yaml:"database" toml:"database" json:"database"`
	Jobs        Jobs                `yaml:"jobs" toml:"jobs" json:"jobs"`
	Providers   map[string]Provider `yaml:"providers" toml:"providers" json:"providers"`
	Pools       []Pool              `yaml:"pools" toml:"pools" json:"pools"`
	Secrets     Secrets             `yaml:"secrets" toml:"secrets" json:"secrets"`
	Upstreams   map[string]Upstream `yaml:"upstreams" toml:"upstreams" json:"upstreams"`
	Auth        Auth                `yaml:"auth" toml:"auth" json:"auth"`
	TLS         TLS                 `yaml:"tls" toml:"tls" json:"tls"`
	Telemetry   Telemetry           `yaml:"telemetry" toml:"telemetry" json:"telemetry"`
	Controllers Controllers         `yaml:"controllers" toml:"controllers" json:"controllers"`
	Recovery    Recovery            `yaml:"recovery" toml:"recovery" json:"recovery"`
}
type HTTP struct {
	Listen, PublicURL                                                       string
	RequestTimeout, ReadTimeout, WriteTimeout, IdleTimeout, ShutdownTimeout Duration
}
type Database struct{ DSN string }
type Jobs struct {
	HeartbeatInterval, LeaseDuration, WorkerTimeout, MaxClaimWait Duration
	MaxEvents, InlineResultBytes, MaxResultBytes, MaxAttempts     int
	RetryBackoff, RetryMaxBackoff, RetryMaxElapsed                Duration
}
type Provider struct {
	Type   string
	Config map[string]string
}
type Pool struct {
	Name, Provider, Image, Template, CPUClass, MemoryClass, Architecture, Region, BootstrapArtifact string
	Capabilities                                                                                    []string
	ExecutorKinds, Upstreams                                                                        []string
	Labels                                                                                          map[string]string
	Environment                                                                                     map[string]string
	BootstrapCommand                                                                                []string
	MinReplicas, MaxReplicas, WorkerConcurrency                                                     int
	IdleTTL, StartupTimeout, ScaleUpCooldown, ScaleDownCooldown, ScaleDownStabilize, MaxLifetime    Duration
	Cost                                                                                            *float64
}
type Secrets struct{ Environment map[string]map[string]string }
type Upstream struct {
	Enabled              bool
	Capabilities, Models []string
	CredentialRef        string
}
type Auth struct {
	Token                               string
	TokenEnv                            string
	InsecureLocal                       bool
	BootstrapTokenTTL, WorkerSessionTTL Duration
}
type TLS struct {
	CertFile, KeyFile string
	Required          bool
}
type Telemetry struct{ Enabled, Metrics, Tracing bool }
type Controllers struct {
	LeaseReaper, JobTimeouts, WorkerHealth, Reconciler, Reaper, Autoscaler    Duration
	WorkerSuspectAfter, WorkerDeadAfter, IdleAfter, DrainGrace, MaxLifetime   Duration
	Orphans                                                                   string
	OwnerLabel, OwnerValue                                                    string
	MinimumWarm, MaxScaleUpPerRun, ProvisioningConcurrency, MaxTotalSandboxes int
	MaxSandboxesByProvider                                                    map[string]int
	ProviderBackoffBase, ProviderBackoffMax                                   Duration
}
type Recovery struct {
	CheckpointDirectory string   `yaml:"checkpoint_directory" toml:"checkpoint_directory" json:"checkpoint_directory"`
	Strategy            string   `yaml:"strategy" toml:"strategy" json:"strategy"`
	CheckpointTTL       Duration `yaml:"checkpoint_ttl" toml:"checkpoint_ttl" json:"checkpoint_ttl"`
	Interval            Duration `yaml:"interval" toml:"interval" json:"interval"`
	BackoffBase         Duration `yaml:"backoff_base" toml:"backoff_base" json:"backoff_base"`
	BackoffMax          Duration `yaml:"backoff_max" toml:"backoff_max" json:"backoff_max"`
	MaxAttempts         int      `yaml:"max_attempts" toml:"max_attempts" json:"max_attempts"`
	RetainLatest        int      `yaml:"retain_latest" toml:"retain_latest" json:"retain_latest"`
	DeleteOnClose       bool     `yaml:"delete_on_close" toml:"delete_on_close" json:"delete_on_close"`
}

func Default() Config {
	return Config{
		HTTP:     HTTP{Listen: "127.0.0.1:8080", PublicURL: "http://127.0.0.1:8080", RequestTimeout: Duration(30 * time.Second), ReadTimeout: Duration(15 * time.Second), WriteTimeout: Duration(30 * time.Second), IdleTimeout: Duration(60 * time.Second), ShutdownTimeout: Duration(10 * time.Second)},
		Database: Database{DSN: "smart-route.db"}, Jobs: Jobs{HeartbeatInterval: Duration(10 * time.Second), LeaseDuration: Duration(30 * time.Second), WorkerTimeout: Duration(30 * time.Second), MaxClaimWait: Duration(20 * time.Second), MaxEvents: 100, InlineResultBytes: 64 << 10, MaxResultBytes: 8 << 20, MaxAttempts: 3, RetryBackoff: Duration(time.Second), RetryMaxBackoff: Duration(time.Minute)},
		Providers: map[string]Provider{}, Secrets: Secrets{Environment: map[string]map[string]string{}}, Upstreams: map[string]Upstream{},
		Auth: Auth{BootstrapTokenTTL: Duration(5 * time.Minute), WorkerSessionTTL: Duration(5 * time.Minute)}, Controllers: Controllers{LeaseReaper: Duration(5 * time.Second), JobTimeouts: Duration(5 * time.Second), WorkerHealth: Duration(10 * time.Second), Reconciler: Duration(30 * time.Second), Reaper: Duration(30 * time.Second), Autoscaler: Duration(10 * time.Second), WorkerSuspectAfter: Duration(30 * time.Second), WorkerDeadAfter: Duration(time.Minute), DrainGrace: Duration(30 * time.Second), Orphans: "terminate", ProviderBackoffBase: Duration(time.Second), ProviderBackoffMax: Duration(time.Minute)}, Recovery: Recovery{CheckpointDirectory: "checkpoints", Strategy: "application", CheckpointTTL: Duration(24 * time.Hour), Interval: Duration(5 * time.Second), BackoffBase: Duration(time.Second), BackoffMax: Duration(time.Minute), MaxAttempts: 5, RetainLatest: 3},
	}
}

func Load(path string) (Config, error) {
	c := Default()
	if path != "" {
		b, e := os.ReadFile(path)
		if e != nil {
			return c, e
		}
		switch {
		case strings.HasSuffix(strings.ToLower(path), ".toml"):
			_, e = toml.Decode(string(b), &c)
		default:
			e = yaml.Unmarshal(b, &c)
		}
		if e != nil {
			return c, fmt.Errorf("decode config: %w", e)
		}
	}
	if e := applyEnv(&c); e != nil {
		return c, e
	}
	if e := c.Validate(); e != nil {
		return c, e
	}
	return c, nil
}

func applyEnv(c *Config) error {
	// JSON env overrides make maps/lists typed; scalar aliases cover common deployment settings.
	aliases := map[string]*string{"SMART_ROUTE_HTTP_LISTEN": &c.HTTP.Listen, "SMART_ROUTE_HTTP_PUBLIC_URL": &c.HTTP.PublicURL, "SMART_ROUTE_DATABASE_DSN": &c.Database.DSN, "SMART_ROUTE_AUTH_TOKEN": &c.Auth.Token, "SMART_ROUTE_AUTH_TOKEN_ENV": &c.Auth.TokenEnv, "SMART_ROUTE_TLS_CERT_FILE": &c.TLS.CertFile, "SMART_ROUTE_TLS_KEY_FILE": &c.TLS.KeyFile}
	for k, p := range aliases {
		if v, ok := os.LookupEnv(k); ok {
			*p = v
		}
	}
	for k, d := range map[string]*Duration{"SMART_ROUTE_HEARTBEAT_INTERVAL": &c.Jobs.HeartbeatInterval, "SMART_ROUTE_LEASE_DURATION": &c.Jobs.LeaseDuration, "SMART_ROUTE_HTTP_SHUTDOWN_TIMEOUT": &c.HTTP.ShutdownTimeout} {
		if v, ok := os.LookupEnv(k); ok {
			if e := d.UnmarshalText([]byte(v)); e != nil {
				return fmt.Errorf("%s: %w", k, e)
			}
		}
	}
	for k, target := range map[string]any{"SMART_ROUTE_PROVIDERS": &c.Providers, "SMART_ROUTE_POOLS": &c.Pools, "SMART_ROUTE_UPSTREAMS": &c.Upstreams, "SMART_ROUTE_SECRET_ENVIRONMENT": &c.Secrets.Environment} {
		if v, ok := os.LookupEnv(k); ok {
			if e := json.Unmarshal([]byte(v), target); e != nil {
				return fmt.Errorf("%s: %w", k, e)
			}
		}
	}
	return nil
}

func (c Config) Validate() error {
	var es []error
	add := func(path, msg string) { es = append(es, fmt.Errorf("%s: %s", path, msg)) }
	if _, e := net.ResolveTCPAddr("tcp", c.HTTP.Listen); e != nil {
		add("http.listen", "must be host:port")
	}
	u, e := url.Parse(c.HTTP.PublicURL)
	if e != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		add("http.public_url", "must be an absolute HTTP(S) URL")
	}
	if strings.TrimSpace(c.Database.DSN) == "" {
		add("database.dsn", "is required")
	}
	positive := func(path string, d Duration) {
		if d <= 0 {
			add(path, "must be positive")
		}
	}
	positive("jobs.heartbeat_interval", c.Jobs.HeartbeatInterval)
	positive("jobs.lease_duration", c.Jobs.LeaseDuration)
	if c.Jobs.LeaseDuration <= c.Jobs.HeartbeatInterval {
		add("jobs.lease_duration", "must exceed heartbeat_interval")
	}
	positive("jobs.worker_timeout", c.Jobs.WorkerTimeout)
	if c.Jobs.MaxEvents < 1 || c.Jobs.InlineResultBytes < 1 || c.Jobs.MaxResultBytes < c.Jobs.InlineResultBytes {
		add("jobs", "event/result limits are invalid")
	}
	if c.Jobs.MaxAttempts < 1 {
		add("jobs.max_attempts", "must be at least 1")
	}
	if c.Auth.Token != "" && c.Auth.TokenEnv != "" {
		add("auth", "set token or token_env, not both")
	}
	if c.Auth.TokenEnv != "" {
		if _, ok := os.LookupEnv(c.Auth.TokenEnv); !ok {
			add("auth.token_env", "referenced environment variable is not set")
		}
	}
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		add("tls", "cert_file and key_file must be set together")
	}
	if c.TLS.Required && c.TLS.CertFile == "" {
		add("tls.cert_file", "is required when TLS is required")
	}
	for name, p := range c.Providers {
		if strings.TrimSpace(name) == "" || p.Type == "" {
			add("providers."+name, "name and type are required")
		}
		if p.Type != "localdocker" && p.Type != "fly" {
			add("providers."+name+".type", "unsupported provider "+strconv.Quote(p.Type))
		}
	}
	seen := map[string]bool{}
	for i, p := range c.Pools {
		base := fmt.Sprintf("pools[%d]", i)
		if p.Name == "" || seen[p.Name] {
			add(base+".name", "must be non-empty and unique")
		}
		seen[p.Name] = true
		if _, ok := c.Providers[p.Provider]; !ok {
			add(base+".provider", "references unknown provider")
		}
		if p.MinReplicas < 0 || p.MaxReplicas < p.MinReplicas || p.WorkerConcurrency < 1 {
			add(base, "replica/concurrency limits are invalid")
		}
		for _, ref := range p.Environment {
			if _, ok := c.Secrets.Environment[ref]; !ok {
				add(base+".environment", "references unknown secret "+strconv.Quote(ref))
			}
		}
	}
	for name, u := range c.Upstreams {
		if u.CredentialRef != "" {
			if _, ok := c.Secrets.Environment[u.CredentialRef]; !ok {
				add("upstreams."+name+".credential_ref", "references unknown secret")
			}
		}
	}
	for _, v := range []struct {
		n string
		d Duration
	}{{"controllers.lease_reaper", c.Controllers.LeaseReaper}, {"controllers.job_timeouts", c.Controllers.JobTimeouts}, {"controllers.worker_health", c.Controllers.WorkerHealth}, {"controllers.reconciler", c.Controllers.Reconciler}, {"controllers.reaper", c.Controllers.Reaper}, {"controllers.autoscaler", c.Controllers.Autoscaler}} {
		positive(v.n, v.d)
	}
	if c.Controllers.WorkerDeadAfter < c.Controllers.WorkerSuspectAfter {
		add("controllers.worker_dead_after", "must be >= worker_suspect_after")
	}
	if c.Controllers.Orphans != "terminate" && c.Controllers.Orphans != "adopt" {
		add("controllers.orphans", "must be terminate or adopt")
	}
	positive("recovery.interval", c.Recovery.Interval)
	positive("recovery.backoff_base", c.Recovery.BackoffBase)
	positive("recovery.backoff_max", c.Recovery.BackoffMax)
	if c.Recovery.CheckpointDirectory == "" {
		add("recovery.checkpoint_directory", "is required")
	}
	if c.Recovery.Strategy != "application" && c.Recovery.Strategy != "provider_snapshot" {
		add("recovery.strategy", "must be application or provider_snapshot")
	}
	if c.Recovery.MaxAttempts < 1 {
		add("recovery.max_attempts", "must be at least 1")
	}
	return errors.Join(es...)
}

func (c Config) AuthToken() string {
	if c.Auth.TokenEnv != "" {
		return os.Getenv(c.Auth.TokenEnv)
	}
	return c.Auth.Token
}
