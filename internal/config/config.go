package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		return d.UnmarshalText([]byte(s))
	}
	var n int64
	if e := json.Unmarshal(b, &n); e != nil {
		return e
	}
	*d = Duration(n)
	return nil
}

type Config struct {
	HTTP        HTTP                `yaml:"http" toml:"http" json:"http"`
	Database    Database            `yaml:"database" toml:"database" json:"database"`
	Jobs        Jobs                `yaml:"jobs" toml:"jobs" json:"jobs"`
	Providers   map[string]Provider `yaml:"providers" toml:"providers" json:"providers"`
	Pools       []Pool              `yaml:"pools" toml:"pools" json:"pools"`
	Secrets     Secrets             `yaml:"secrets" toml:"secrets" json:"secrets"`
	Artifacts   Artifacts           `yaml:"artifacts" toml:"artifacts" json:"artifacts"`
	Auth        Auth                `yaml:"auth" toml:"auth" json:"auth"`
	TLS         TLS                 `yaml:"tls" toml:"tls" json:"tls"`
	Telemetry   Telemetry           `yaml:"telemetry" toml:"telemetry" json:"telemetry"`
	Controllers Controllers         `yaml:"controllers" toml:"controllers" json:"controllers"`
	Recovery    Recovery            `yaml:"recovery" toml:"recovery" json:"recovery"`
}
type HTTP struct {
	Listen          string   `yaml:"listen" toml:"listen" json:"listen"`
	PublicURL       string   `yaml:"public_url" toml:"public_url" json:"public_url"`
	RequestTimeout  Duration `yaml:"request_timeout" toml:"request_timeout" json:"request_timeout"`
	ReadTimeout     Duration `yaml:"read_timeout" toml:"read_timeout" json:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout" toml:"write_timeout" json:"write_timeout"`
	IdleTimeout     Duration `yaml:"idle_timeout" toml:"idle_timeout" json:"idle_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout" toml:"shutdown_timeout" json:"shutdown_timeout"`
}
type Database struct {
	DSN string `yaml:"dsn" toml:"dsn" json:"dsn"`
}
type Jobs struct {
	HeartbeatInterval Duration `yaml:"heartbeat_interval" toml:"heartbeat_interval" json:"heartbeat_interval"`
	LeaseDuration     Duration `yaml:"lease_duration" toml:"lease_duration" json:"lease_duration"`
	WorkerTimeout     Duration `yaml:"worker_timeout" toml:"worker_timeout" json:"worker_timeout"`
	MaxClaimWait      Duration `yaml:"max_claim_wait" toml:"max_claim_wait" json:"max_claim_wait"`
	MaxEvents         int      `yaml:"max_events" toml:"max_events" json:"max_events"`
	InlineResultBytes int      `yaml:"inline_result_bytes" toml:"inline_result_bytes" json:"inline_result_bytes"`
	MaxResultBytes    int      `yaml:"max_result_bytes" toml:"max_result_bytes" json:"max_result_bytes"`
	MaxAttempts       int      `yaml:"max_attempts" toml:"max_attempts" json:"max_attempts"`
	RetryBackoff      Duration `yaml:"retry_backoff" toml:"retry_backoff" json:"retry_backoff"`
	RetryMaxBackoff   Duration `yaml:"retry_max_backoff" toml:"retry_max_backoff" json:"retry_max_backoff"`
	RetryMaxElapsed   Duration `yaml:"retry_max_elapsed" toml:"retry_max_elapsed" json:"retry_max_elapsed"`
}
type Provider struct {
	Type   string            `yaml:"type" toml:"type" json:"type"`
	Config map[string]string `yaml:"config" toml:"config" json:"config"`
}
type Pool struct {
	Name               string            `yaml:"name" toml:"name" json:"name"`
	Provider           string            `yaml:"provider" toml:"provider" json:"provider"`
	Image              string            `yaml:"image" toml:"image" json:"image"`
	Template           string            `yaml:"template" toml:"template" json:"template"`
	CPUClass           string            `yaml:"cpu_class" toml:"cpu_class" json:"cpu_class"`
	MemoryClass        string            `yaml:"memory_class" toml:"memory_class" json:"memory_class"`
	Architecture       string            `yaml:"architecture" toml:"architecture" json:"architecture"`
	Region             string            `yaml:"region" toml:"region" json:"region"`
	BootstrapArtifact  string            `yaml:"bootstrap_artifact" toml:"bootstrap_artifact" json:"bootstrap_artifact"`
	Capabilities       []string          `yaml:"capabilities" toml:"capabilities" json:"capabilities"`
	ExecutorKinds      []string          `yaml:"executor_kinds" toml:"executor_kinds" json:"executor_kinds"`
	Labels             map[string]string `yaml:"labels" toml:"labels" json:"labels"`
	Environment        map[string]string `yaml:"environment" toml:"environment" json:"environment"`
	BootstrapCommand   []string          `yaml:"bootstrap_command" toml:"bootstrap_command" json:"bootstrap_command"`
	MinReplicas        int               `yaml:"min_replicas" toml:"min_replicas" json:"min_replicas"`
	MaxReplicas        int               `yaml:"max_replicas" toml:"max_replicas" json:"max_replicas"`
	WorkerConcurrency  int               `yaml:"worker_concurrency" toml:"worker_concurrency" json:"worker_concurrency"`
	IdleTTL            Duration          `yaml:"idle_ttl" toml:"idle_ttl" json:"idle_ttl"`
	StartupTimeout     Duration          `yaml:"startup_timeout" toml:"startup_timeout" json:"startup_timeout"`
	ScaleUpCooldown    Duration          `yaml:"scale_up_cooldown" toml:"scale_up_cooldown" json:"scale_up_cooldown"`
	ScaleDownCooldown  Duration          `yaml:"scale_down_cooldown" toml:"scale_down_cooldown" json:"scale_down_cooldown"`
	ScaleDownStabilize Duration          `yaml:"scale_down_stabilize" toml:"scale_down_stabilize" json:"scale_down_stabilize"`
	MaxLifetime        Duration          `yaml:"max_lifetime" toml:"max_lifetime" json:"max_lifetime"`
	Cost               *float64          `yaml:"cost" toml:"cost" json:"cost"`
}
type Secrets struct {
	Environment map[string]map[string]string `yaml:"environment" toml:"environment" json:"environment"`
}
type Artifacts struct {
	Directory string `yaml:"directory" toml:"directory" json:"directory"`
}
type Auth struct {
	Token             string   `yaml:"token" toml:"token" json:"token"`
	TokenEnv          string   `yaml:"token_env" toml:"token_env" json:"token_env"`
	InsecureLocal     bool     `yaml:"insecure_local" toml:"insecure_local" json:"insecure_local"`
	BootstrapTokenTTL Duration `yaml:"bootstrap_token_ttl" toml:"bootstrap_token_ttl" json:"bootstrap_token_ttl"`
	WorkerSessionTTL  Duration `yaml:"worker_session_ttl" toml:"worker_session_ttl" json:"worker_session_ttl"`
}
type TLS struct {
	CertFile string `yaml:"cert_file" toml:"cert_file" json:"cert_file"`
	KeyFile  string `yaml:"key_file" toml:"key_file" json:"key_file"`
	Required bool   `yaml:"required" toml:"required" json:"required"`
}
type Telemetry struct {
	Enabled bool `yaml:"enabled" toml:"enabled" json:"enabled"`
	Metrics bool `yaml:"metrics" toml:"metrics" json:"metrics"`
}
type Controllers struct {
	LeaseReaper             Duration       `yaml:"lease_reaper" toml:"lease_reaper" json:"lease_reaper"`
	JobTimeouts             Duration       `yaml:"job_timeouts" toml:"job_timeouts" json:"job_timeouts"`
	SessionExpiry           Duration       `yaml:"session_expiry" toml:"session_expiry" json:"session_expiry"`
	WorkerHealth            Duration       `yaml:"worker_health" toml:"worker_health" json:"worker_health"`
	Reconciler              Duration       `yaml:"reconciler" toml:"reconciler" json:"reconciler"`
	Reaper                  Duration       `yaml:"reaper" toml:"reaper" json:"reaper"`
	Autoscaler              Duration       `yaml:"autoscaler" toml:"autoscaler" json:"autoscaler"`
	WorkerSuspectAfter      Duration       `yaml:"worker_suspect_after" toml:"worker_suspect_after" json:"worker_suspect_after"`
	WorkerDeadAfter         Duration       `yaml:"worker_dead_after" toml:"worker_dead_after" json:"worker_dead_after"`
	IdleAfter               Duration       `yaml:"idle_after" toml:"idle_after" json:"idle_after"`
	DrainGrace              Duration       `yaml:"drain_grace" toml:"drain_grace" json:"drain_grace"`
	MaxLifetime             Duration       `yaml:"max_lifetime" toml:"max_lifetime" json:"max_lifetime"`
	Orphans                 string         `yaml:"orphans" toml:"orphans" json:"orphans"`
	OwnerLabel              string         `yaml:"owner_label" toml:"owner_label" json:"owner_label"`
	OwnerValue              string         `yaml:"owner_value" toml:"owner_value" json:"owner_value"`
	MinimumWarm             int            `yaml:"minimum_warm" toml:"minimum_warm" json:"minimum_warm"`
	MaxScaleUpPerRun        int            `yaml:"max_scale_up_per_run" toml:"max_scale_up_per_run" json:"max_scale_up_per_run"`
	ProvisioningConcurrency int            `yaml:"provisioning_concurrency" toml:"provisioning_concurrency" json:"provisioning_concurrency"`
	MaxTotalSandboxes       int            `yaml:"max_total_sandboxes" toml:"max_total_sandboxes" json:"max_total_sandboxes"`
	MaxSandboxesByProvider  map[string]int `yaml:"max_sandboxes_by_provider" toml:"max_sandboxes_by_provider" json:"max_sandboxes_by_provider"`
	ProviderBackoffBase     Duration       `yaml:"provider_backoff_base" toml:"provider_backoff_base" json:"provider_backoff_base"`
	ProviderBackoffMax      Duration       `yaml:"provider_backoff_max" toml:"provider_backoff_max" json:"provider_backoff_max"`
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
		Providers: map[string]Provider{}, Secrets: Secrets{Environment: map[string]map[string]string{}}, Artifacts: Artifacts{Directory: "artifacts"},
		Auth: Auth{BootstrapTokenTTL: Duration(5 * time.Minute), WorkerSessionTTL: Duration(5 * time.Minute)}, Controllers: Controllers{LeaseReaper: Duration(5 * time.Second), JobTimeouts: Duration(5 * time.Second), SessionExpiry: Duration(5 * time.Second), WorkerHealth: Duration(10 * time.Second), Reconciler: Duration(30 * time.Second), Reaper: Duration(30 * time.Second), Autoscaler: Duration(10 * time.Second), WorkerSuspectAfter: Duration(30 * time.Second), WorkerDeadAfter: Duration(time.Minute), DrainGrace: Duration(30 * time.Second), Orphans: "terminate", ProviderBackoffBase: Duration(time.Second), ProviderBackoffMax: Duration(time.Minute)}, Recovery: Recovery{CheckpointDirectory: "checkpoints", Strategy: "application", CheckpointTTL: Duration(24 * time.Hour), Interval: Duration(5 * time.Second), BackoffBase: Duration(time.Second), BackoffMax: Duration(time.Minute), MaxAttempts: 5, RetainLatest: 3},
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
			var md toml.MetaData
			if md, e = toml.Decode(string(b), &c); e == nil && len(md.Undecoded()) > 0 {
				undecoded := make([]string, len(md.Undecoded()))
				for i, k := range md.Undecoded() {
					undecoded[i] = k.String()
				}
				e = fmt.Errorf("unknown configuration field(s): %s", strings.Join(undecoded, ", "))
			}
		default:
			decoder := yaml.NewDecoder(bytes.NewReader(b))
			decoder.KnownFields(true)
			if e = decoder.Decode(&c); errors.Is(e, io.EOF) {
				e = nil
			}
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
	for k, target := range map[string]any{"SMART_ROUTE_PROVIDERS": &c.Providers, "SMART_ROUTE_POOLS": &c.Pools, "SMART_ROUTE_SECRET_ENVIRONMENT": &c.Secrets.Environment} {
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
	for _, v := range []struct {
		n string
		d Duration
	}{{"controllers.lease_reaper", c.Controllers.LeaseReaper}, {"controllers.job_timeouts", c.Controllers.JobTimeouts}, {"controllers.session_expiry", c.Controllers.SessionExpiry}, {"controllers.worker_health", c.Controllers.WorkerHealth}, {"controllers.reconciler", c.Controllers.Reconciler}, {"controllers.reaper", c.Controllers.Reaper}, {"controllers.autoscaler", c.Controllers.Autoscaler}} {
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
