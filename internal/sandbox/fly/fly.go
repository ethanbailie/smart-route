// Package fly implements sandbox.Provider with the Fly Machines API.
package fly

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
)

const (
	ProviderName = "fly"
	defaultAPI   = "https://api.machines.dev"
	managedKey   = "smart-route-managed"
	sandboxKey   = "smart-route-sandbox-id"
	workerKey    = "smart-route-worker-id"
	labelsKey    = "smart-route-labels"
	capsKey      = "smart-route-capabilities"
	expiresKey   = "smart-route-expires-at"
)

type Config struct {
	App, Token, TokenEnv, APIURL, Image string
	RequestTimeout, StartupTimeout      time.Duration
	MaxRetries                          int
	RetryBackoff                        time.Duration
}

type Provider struct {
	config Config
	client *http.Client
	mu     sync.Mutex
	known  map[domain.SandboxID]sandbox.Sandbox
}

func New(config Config) (*Provider, error) { return newProvider(config, nil) }

func Factory(values map[string]string) (sandbox.Provider, error) {
	c := Config{App: values["app"], Token: values["token"], TokenEnv: values["token_env"], APIURL: values["api_url"], Image: values["image"]}
	var err error
	if c.RequestTimeout, err = duration(values["request_timeout"], 15*time.Second); err != nil {
		return nil, err
	}
	if c.StartupTimeout, err = duration(values["startup_timeout"], 90*time.Second); err != nil {
		return nil, err
	}
	if c.RetryBackoff, err = duration(values["retry_backoff"], 500*time.Millisecond); err != nil {
		return nil, err
	}
	c.MaxRetries = 3
	if values["max_retries"] != "" {
		c.MaxRetries, err = strconv.Atoi(values["max_retries"])
		if err != nil {
			return nil, fmt.Errorf("max_retries: %w", err)
		}
	}
	return New(c)
}

func newProvider(config Config, client *http.Client) (*Provider, error) {
	if config.TokenEnv == "" {
		config.TokenEnv = "FLY_API_TOKEN"
	}
	if config.Token == "" {
		config.Token = os.Getenv(config.TokenEnv)
	}
	if config.APIURL == "" {
		config.APIURL = defaultAPI
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 15 * time.Second
	}
	if config.StartupTimeout <= 0 {
		config.StartupTimeout = 90 * time.Second
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = 500 * time.Millisecond
	}
	if config.MaxRetries < 0 {
		return nil, invalid("configure", "max_retries cannot be negative")
	}
	u, err := url.Parse(config.APIURL)
	if err != nil || u.Scheme == "" || u.Host == "" || strings.TrimSpace(config.App) == "" {
		return nil, invalid("configure", "app and an absolute api_url are required")
	}
	if config.Token == "" {
		return nil, &sandbox.ProviderError{Provider: ProviderName, Operation: "configure", Code: sandbox.CodeAuthentication, Err: errors.New("API token is required via token_env")}
	}
	if client == nil {
		client = &http.Client{}
	}
	return &Provider{config: config, client: client, known: map[domain.SandboxID]sandbox.Sandbox{}}, nil
}

type machine struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	State      string `json:"state"`
	Region     string `json:"region"`
	InstanceID string `json:"instance_id"`
	CreatedAt  string `json:"created_at"`
	Config     struct {
		Image    string            `json:"image"`
		Metadata map[string]string `json:"metadata"`
	} `json:"config"`
}
type createRequest struct {
	Name   string        `json:"name,omitempty"`
	Region string        `json:"region,omitempty"`
	Config machineConfig `json:"config"`
}
type machineConfig struct {
	Image    string            `json:"image"`
	Env      map[string]string `json:"env"`
	Metadata map[string]string `json:"metadata"`
	Init     *struct {
		Exec []string `json:"exec"`
	} `json:"init,omitempty"`
	Guest   guest `json:"guest"`
	Restart struct {
		Policy string `json:"policy"`
	} `json:"restart"`
}
type guest struct {
	CPUKind  string `json:"cpu_kind"`
	CPUs     int    `json:"cpus"`
	MemoryMB int    `json:"memory_mb"`
}

func (p *Provider) Create(ctx context.Context, spec sandbox.CreateSpec) (sandbox.Sandbox, error) {
	if err := validateSpec(spec); err != nil {
		return sandbox.Sandbox{}, sandbox.NewError(ProviderName, "create", sandbox.CodeInvalid, err)
	}
	id := spec.SandboxID
	if id == "" {
		generated, err := newID()
		if err != nil {
			return sandbox.Sandbox{}, sandbox.NewError(ProviderName, "create", sandbox.CodeInternal, err)
		}
		id = generated
	}
	image := spec.Image
	if image == "" {
		image = spec.Template
	}
	if image == "" {
		image = p.config.Image
	}
	if image == "" || strings.HasSuffix(image, ":latest") {
		return sandbox.Sandbox{}, invalid("create", "a versioned worker image (digest or non-latest tag) is required")
	}
	cpuKind, cpus, err := cpu(spec.CPUClass)
	if err != nil {
		return sandbox.Sandbox{}, invalid("create", err.Error())
	}
	memory, err := memoryMB(spec.MemoryClass)
	if err != nil {
		return sandbox.Sandbox{}, invalid("create", err.Error())
	}
	labels, _ := json.Marshal(spec.Labels)
	caps, _ := json.Marshal(spec.Capabilities)
	metadata := map[string]string{managedKey: "true", sandboxKey: string(id), workerKey: string(spec.WorkerID), labelsKey: string(labels), capsKey: string(caps)}
	if spec.MaxLifetime > 0 {
		metadata[expiresKey] = time.Now().UTC().Add(spec.MaxLifetime).Format(time.RFC3339Nano)
	}
	providerName := spec.SandboxProvider
	if providerName == "" {
		providerName = ProviderName
	}
	env := map[string]string{"SMART_ROUTE_CONTROL_PLANE_URL": spec.ControlPlaneURL, "SMART_ROUTE_BOOTSTRAP_TOKEN": spec.BootstrapToken, "SMART_ROUTE_WORKER_ID": string(spec.WorkerID), "SMART_ROUTE_SANDBOX_ID": string(id), "SMART_ROUTE_SANDBOX_PROVIDER": providerName, "SMART_ROUTE_MAX_CONCURRENCY": strconv.Itoa(max(1, spec.WorkerMaxConcurrency)), "SMART_ROUTE_CAPABILITIES": strings.Join(spec.Capabilities.Capabilities, ","), "SMART_ROUTE_UPSTREAMS": strings.Join(spec.Capabilities.Upstreams, ","), "SMART_ROUTE_REGION": spec.Capabilities.Region}
	workerLabels, _ := json.Marshal(spec.Capabilities.Labels)
	env["SMART_ROUTE_LABELS"] = string(workerLabels)
	for key, ref := range spec.Environment {
		env["SMART_ROUTE_ENV_REF_"+key] = string(ref)
	}
	if spec.BootstrapArtifact != "" {
		env["SMART_ROUTE_BOOTSTRAP_ARTIFACT"] = spec.BootstrapArtifact
	}
	req := createRequest{Name: "smart-route-" + string(id), Region: spec.Region, Config: machineConfig{Image: image, Env: env, Metadata: metadata, Guest: guest{CPUKind: cpuKind, CPUs: cpus, MemoryMB: memory}}}
	req.Config.Restart.Policy = "on-failure"
	if len(spec.BootstrapCommand) > 0 {
		req.Config.Init = &struct {
			Exec []string `json:"exec"`
		}{Exec: append([]string(nil), spec.BootstrapCommand...)}
	}
	var created machine
	if err = p.do(ctx, "create", id, http.MethodPost, p.machinesPath(), nil, req, &created); err != nil {
		return sandbox.Sandbox{}, err
	}
	if err = p.waitStarted(ctx, id, created.ID); err != nil {
		_ = p.delete(context.Background(), id, created.ID)
		return sandbox.Sandbox{}, err
	}
	created.State = "started"
	item := fromMachine(created)
	p.remember(item)
	return item, nil
}

func (p *Provider) Get(ctx context.Context, id domain.SandboxID) (sandbox.Sandbox, error) {
	machines, err := p.listManaged(ctx, id)
	if err != nil {
		return sandbox.Sandbox{}, err
	}
	if len(machines) == 0 {
		p.mu.Lock()
		known, ok := p.known[id]
		p.mu.Unlock()
		if ok {
			known.State = sandbox.StateTerminated
			p.remember(known)
			return known, nil
		}
		return sandbox.Sandbox{}, &sandbox.ProviderError{Provider: ProviderName, Operation: "get", SandboxID: string(id), Code: sandbox.CodeNotFound}
	}
	return fromMachine(machines[0]), nil
}

func (p *Provider) List(ctx context.Context, filter sandbox.Filter) ([]sandbox.Sandbox, error) {
	machines, err := p.listManaged(ctx, "")
	if err != nil {
		return nil, err
	}
	result := make([]sandbox.Sandbox, 0, len(machines))
	for _, m := range machines {
		item := fromMachine(m)
		if item.ID != "" && sandbox.Matches(item, filter) {
			result = append(result, item)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (p *Provider) Terminate(ctx context.Context, id domain.SandboxID) error {
	machines, err := p.listManaged(ctx, id)
	if err != nil {
		return err
	}
	for _, m := range machines {
		if err = p.delete(ctx, id, m.ID); err != nil && !errors.Is(err, sandbox.ErrNotFound) {
			return err
		}
	}
	p.mu.Lock()
	if item, ok := p.known[id]; ok {
		item.State = sandbox.StateTerminated
		p.known[id] = item
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) listManaged(ctx context.Context, id domain.SandboxID) ([]machine, error) {
	q := url.Values{"metadata." + managedKey: {"true"}}
	if id != "" {
		q.Set("metadata."+sandboxKey, string(id))
	}
	var result []machine
	err := p.do(ctx, "list", id, http.MethodGet, p.machinesPath(), q, nil, &result)
	return result, err
}
func (p *Provider) delete(ctx context.Context, id domain.SandboxID, external string) error {
	return p.do(ctx, "terminate", id, http.MethodDelete, p.machinesPath()+"/"+url.PathEscape(external), url.Values{"force": {"true"}}, nil, nil)
}
func (p *Provider) waitStarted(ctx context.Context, id domain.SandboxID, external string) error {
	ctx, cancel := context.WithTimeout(ctx, p.config.StartupTimeout)
	defer cancel()
	q := url.Values{"state": {"started"}, "timeout": {strconv.Itoa(max(1, int(p.config.StartupTimeout.Seconds())))}}
	err := p.do(ctx, "wait", id, http.MethodGet, p.machinesPath()+"/"+url.PathEscape(external)+"/wait", q, nil, nil)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &sandbox.ProviderError{Provider: ProviderName, Operation: "wait", SandboxID: string(id), Code: sandbox.CodeUnavailable, Err: errors.New("startup deadline exceeded")}
	}
	return err
}
func (p *Provider) machinesPath() string {
	return "/v1/apps/" + url.PathEscape(p.config.App) + "/machines"
}

func (p *Provider) do(ctx context.Context, op string, id domain.SandboxID, method, path string, q url.Values, body, out any) error {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return sandbox.NewError(ProviderName, op, sandbox.CodeInternal, err)
		}
	}
	for attempt := 0; ; attempt++ {
		requestCtx, cancel := context.WithTimeout(ctx, p.config.RequestTimeout)
		u := strings.TrimRight(p.config.APIURL, "/") + path
		if len(q) > 0 {
			u += "?" + q.Encode()
		}
		req, e := http.NewRequestWithContext(requestCtx, method, u, bytes.NewReader(encoded))
		if e != nil {
			cancel()
			return sandbox.NewError(ProviderName, op, sandbox.CodeInternal, e)
		}
		req.Header.Set("Authorization", "Bearer "+p.config.Token)
		req.Header.Set("Content-Type", "application/json")
		resp, e := p.client.Do(req)
		var data []byte
		if e == nil {
			data, e = io.ReadAll(io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
		}
		cancel()
		code := classify(resp, e)
		if e == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(data) > 0 {
				if e = json.Unmarshal(data, out); e != nil {
					return sandbox.NewError(ProviderName, op, sandbox.CodeInternal, fmt.Errorf("decode response: %w", e))
				}
			}
			return nil
		}
		if method == http.MethodDelete && resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil
		}
		if attempt >= p.config.MaxRetries || (code != sandbox.CodeUnavailable && code != sandbox.CodeCapacity) {
			return &sandbox.ProviderError{Provider: ProviderName, Operation: op, SandboxID: string(id), Code: code, Err: safeAPIError(resp, e)}
		}
		delay := p.config.RetryBackoff * time.Duration(1<<attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &sandbox.ProviderError{Provider: ProviderName, Operation: op, SandboxID: string(id), Code: sandbox.CodeUnavailable, Err: ctx.Err()}
		case <-timer.C:
		}
	}
}

func classify(resp *http.Response, err error) sandbox.ErrorCode {
	if err != nil {
		return sandbox.CodeUnavailable
	}
	switch resp.StatusCode {
	case 400, 422:
		return sandbox.CodeInvalid
	case 401, 403:
		return sandbox.CodeAuthentication
	case 404:
		return sandbox.CodeNotFound
	case 408, 429:
		return sandbox.CodeCapacity
	default:
		if resp.StatusCode >= 500 {
			return sandbox.CodeUnavailable
		}
		return sandbox.CodeInternal
	}
}
func safeAPIError(resp *http.Response, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("Machines API status %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode))
}
func fromMachine(m machine) sandbox.Sandbox {
	var labels map[string]string
	var caps domain.Capabilities
	_ = json.Unmarshal([]byte(m.Config.Metadata[labelsKey]), &labels)
	_ = json.Unmarshal([]byte(m.Config.Metadata[capsKey]), &caps)
	created, _ := time.Parse(time.RFC3339, m.CreatedAt)
	return sandbox.Sandbox{ID: domain.SandboxID(m.Config.Metadata[sandboxKey]), Provider: ProviderName, ExternalID: m.ID, WorkerID: domain.WorkerID(m.Config.Metadata[workerKey]), State: normalize(m.State), Capabilities: caps, Labels: labels, Metadata: map[string]string{"image": m.Config.Image, "region": m.Region, "instance_id": m.InstanceID}, CreatedAt: created}
}
func normalize(state string) sandbox.State {
	switch state {
	case "created", "starting":
		return sandbox.StateCreating
	case "started":
		return sandbox.StateRunning
	case "stopped", "suspended":
		return sandbox.StateStopped
	case "destroyed", "destroying":
		return sandbox.StateTerminated
	case "failed":
		return sandbox.StateFailed
	default:
		return sandbox.StateUnknown
	}
}
func validateSpec(s sandbox.CreateSpec) error {
	if s.WorkerID == "" {
		return errors.New("worker ID is required")
	}
	u, e := url.ParseRequestURI(s.ControlPlaneURL)
	if e != nil || u.Scheme == "" || u.Host == "" {
		return errors.New("valid control-plane URL is required")
	}
	if s.BootstrapToken == "" {
		return errors.New("bootstrap token is required")
	}
	if s.Architecture != "" && s.Architecture != domain.ArchitectureAMD64 {
		return errors.New("Fly Machines adapter currently supports amd64")
	}
	return nil
}
func cpu(v string) (string, int, error) {
	if v == "" {
		return "shared", 1, nil
	}
	parts := strings.Split(v, "-")
	kind := parts[0]
	if kind != "shared" && kind != "performance" {
		return "", 0, fmt.Errorf("cpu_class must be shared[-N] or performance[-N]")
	}
	n := 1
	if len(parts) == 2 {
		var e error
		n, e = strconv.Atoi(parts[1])
		if e != nil || n < 1 {
			return "", 0, fmt.Errorf("invalid cpu_class %q", v)
		}
	} else if len(parts) > 2 {
		return "", 0, fmt.Errorf("invalid cpu_class %q", v)
	}
	return kind, n, nil
}
func memoryMB(v string) (int, error) {
	if v == "" {
		return 256, nil
	}
	lower := strings.ToLower(strings.TrimSpace(v))
	mult := 1
	if strings.HasSuffix(lower, "gb") {
		mult = 1024
		lower = strings.TrimSuffix(lower, "gb")
	} else if strings.HasSuffix(lower, "g") {
		mult = 1024
		lower = strings.TrimSuffix(lower, "g")
	} else if strings.HasSuffix(lower, "mb") {
		lower = strings.TrimSuffix(lower, "mb")
	} else if strings.HasSuffix(lower, "m") {
		lower = strings.TrimSuffix(lower, "m")
	}
	n, e := strconv.Atoi(lower)
	if e != nil || n < 1 {
		return 0, fmt.Errorf("invalid memory_class %q", v)
	}
	return n * mult, nil
}
func duration(v string, fallback time.Duration) (time.Duration, error) {
	if v == "" {
		return fallback, nil
	}
	d, e := time.ParseDuration(v)
	if e != nil || d <= 0 {
		return 0, fmt.Errorf("invalid duration %q", v)
	}
	return d, nil
}
func invalid(op, message string) error {
	return sandbox.NewError(ProviderName, op, sandbox.CodeInvalid, errors.New(message))
}
func (p *Provider) remember(item sandbox.Sandbox) {
	p.mu.Lock()
	p.known[item.ID] = item
	p.mu.Unlock()
}
func newID() (domain.SandboxID, error) {
	b := make([]byte, 12)
	if _, e := rand.Read(b); e != nil {
		return "", e
	}
	return domain.SandboxID(hex.EncodeToString(b)), nil
}
