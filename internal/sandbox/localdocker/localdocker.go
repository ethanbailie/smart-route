// Package localdocker implements sandbox.Provider using local Docker containers.
// Docker command syntax, labels, and lifecycle values are contained in this package.
package localdocker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethan/smart-route/internal/domain"
	"github.com/ethan/smart-route/internal/sandbox"
)

const (
	ProviderName      = "localdocker"
	defaultImage      = "smart-route-worker:latest"
	managedLabel      = "smart-route.managed"
	sandboxLabel      = "smart-route.sandbox-id"
	workerLabel       = "smart-route.worker-id"
	capabilitiesLabel = "smart-route.capabilities"
	labelsLabel       = "smart-route.labels"
	regionLabel       = "smart-route.region"
	expiresLabel      = "smart-route.expires-at"
	templateLabel     = "smart-route.template"
)

type Config struct {
	Image        string
	DockerBinary string
	Network      string
}

type commandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, binary, args...).CombinedOutput()
}

type Provider struct {
	config Config
	runner commandRunner
	mu     sync.Mutex
	known  map[domain.SandboxID]sandbox.Sandbox
}

func New(config Config) (*Provider, error) { return newProvider(config, execRunner{}) }

func Factory(values map[string]string) (sandbox.Provider, error) {
	return New(Config{Image: values["image"], DockerBinary: values["docker_binary"], Network: values["network"]})
}

func newProvider(config Config, runner commandRunner) (*Provider, error) {
	if config.Image == "" {
		config.Image = defaultImage
	}
	if config.DockerBinary == "" {
		config.DockerBinary = "docker"
	}
	if strings.TrimSpace(config.Image) == "" || strings.HasPrefix(config.Image, "-") {
		return nil, sandbox.NewError(ProviderName, "configure", sandbox.CodeInvalid, fmt.Errorf("invalid image"))
	}
	return &Provider{config: config, runner: runner, known: make(map[domain.SandboxID]sandbox.Sandbox)}, nil
}

func (p *Provider) Create(ctx context.Context, spec sandbox.CreateSpec) (sandbox.Sandbox, error) {
	if err := validateSpec(spec); err != nil {
		return sandbox.Sandbox{}, sandbox.NewError(ProviderName, "create", sandbox.CodeInvalid, err)
	}
	id := spec.SandboxID
	var err error
	if id == "" {
		id, err = newID()
	}
	if err != nil {
		return sandbox.Sandbox{}, sandbox.NewError(ProviderName, "create", sandbox.CodeInternal, err)
	}
	name := containerName(id)
	concurrency := spec.WorkerMaxConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	providerName := spec.SandboxProvider
	if providerName == "" {
		providerName = ProviderName
	}
	args := []string{"run", "--detach", "--name", name,
		"--label", managedLabel + "=true", "--label", sandboxLabel + "=" + string(id),
		"--label", workerLabel + "=" + string(spec.WorkerID),
		"--env", "SMART_ROUTE_CONTROL_PLANE_URL=" + spec.ControlPlaneURL,
		"--env", "SMART_ROUTE_BOOTSTRAP_TOKEN=" + spec.BootstrapToken,
		"--env", "SMART_ROUTE_WORKER_ID=" + string(spec.WorkerID),
		"--env", "SMART_ROUTE_INSTANCE_ID=" + instanceID(id),
		"--env", "SMART_ROUTE_SANDBOX_ID=" + string(id),
		"--env", "SMART_ROUTE_SANDBOX_PROVIDER=" + providerName,
		"--env", fmt.Sprintf("SMART_ROUTE_MAX_CONCURRENCY=%d", concurrency),
	}
	if p.config.Network != "" {
		args = append(args, "--network", p.config.Network)
	}
	capabilities, _ := json.Marshal(spec.Capabilities)
	workerLabels, _ := json.Marshal(spec.Capabilities.Labels)
	args = append(args, "--env", "SMART_ROUTE_CAPABILITIES="+strings.Join(spec.Capabilities.Capabilities, ","), "--env", "SMART_ROUTE_LABELS="+string(workerLabels), "--env", "SMART_ROUTE_UPSTREAMS="+strings.Join(spec.Capabilities.Upstreams, ","), "--env", "SMART_ROUTE_REGION="+spec.Capabilities.Region)
	labels, _ := json.Marshal(spec.Labels)
	args = append(args, "--label", capabilitiesLabel+"="+string(capabilities), "--label", labelsLabel+"="+string(labels))
	if spec.Template != "" {
		args = append(args, "--label", templateLabel+"="+spec.Template)
	}
	region := spec.Region
	if region == "" {
		region = spec.Capabilities.Region
	}
	if region != "" {
		args = append(args, "--label", regionLabel+"="+region)
	}
	if spec.MaxLifetime > 0 {
		args = append(args, "--label", expiresLabel+"="+time.Now().UTC().Add(spec.MaxLifetime).Format(time.RFC3339Nano))
	}
	architecture := spec.Architecture
	if architecture == "" {
		architecture = spec.Capabilities.Architecture
	}
	if architecture != "" {
		args = append(args, "--platform", "linux/"+string(architecture))
	}
	if spec.CPUClass != "" {
		args = append(args, "--cpus", spec.CPUClass)
	}
	if spec.MemoryClass != "" {
		args = append(args, "--memory", spec.MemoryClass)
	}
	for _, key := range sortedCredentialKeys(spec.Environment) {
		args = append(args, "--env", "SMART_ROUTE_ENV_REF_"+key+"="+string(spec.Environment[key]))
	}
	if spec.BootstrapArtifact != "" {
		args = append(args, "--env", "SMART_ROUTE_BOOTSTRAP_ARTIFACT="+spec.BootstrapArtifact)
	}
	image := spec.Image
	if image == "" {
		image = spec.Template
	}
	if image == "" {
		image = p.config.Image
	}
	args = append(args, image)
	args = append(args, spec.BootstrapCommand...)
	output, err := p.runner.Run(ctx, p.config.DockerBinary, args...)
	if err != nil {
		return sandbox.Sandbox{}, p.commandError("create", id, output, err)
	}
	item := sandbox.Sandbox{ID: id, Provider: ProviderName, ExternalID: strings.TrimSpace(string(output)), WorkerID: spec.WorkerID, State: sandbox.StateRunning, Capabilities: spec.Capabilities, Labels: cloneStrings(spec.Labels), Metadata: map[string]string{"image": image, "template": spec.Template}, CreatedAt: time.Now().UTC()}
	p.mu.Lock()
	p.known[id] = item
	p.mu.Unlock()
	return item, nil
}

func (p *Provider) Get(ctx context.Context, id domain.SandboxID) (sandbox.Sandbox, error) {
	p.mu.Lock()
	item, known := p.known[id]
	p.mu.Unlock()
	output, err := p.runner.Run(ctx, p.config.DockerBinary, "inspect", "--format", "{{json .}}", containerName(id))
	if err != nil {
		if missing(output) {
			if !known {
				return sandbox.Sandbox{}, &sandbox.ProviderError{Provider: ProviderName, Operation: "get", SandboxID: string(id), Code: sandbox.CodeNotFound}
			}
			item.State = sandbox.StateTerminated
			p.remember(item)
			return item, nil
		}
		return sandbox.Sandbox{}, p.commandError("get", id, output, err)
	}
	var container dockerContainer
	if err := json.Unmarshal(output, &container); err != nil {
		return sandbox.Sandbox{}, sandbox.NewError(ProviderName, "get", sandbox.CodeInternal, fmt.Errorf("decode docker state: %w", err))
	}
	if expired(container, time.Now()) {
		if output, err := p.runner.Run(ctx, p.config.DockerBinary, "rm", "--force", containerName(id)); err != nil && !missing(output) {
			return sandbox.Sandbox{}, p.commandError("expire", id, output, err)
		}
		container.State = dockerState{Status: "removing"}
	}
	item = sandboxFromContainer(container, item)
	p.remember(item)
	return item, nil
}

func (p *Provider) List(ctx context.Context, filter sandbox.Filter) ([]sandbox.Sandbox, error) {
	output, err := p.runner.Run(ctx, p.config.DockerBinary, "ps", "--all", "--quiet", "--filter", "label="+managedLabel+"=true")
	if err != nil {
		return nil, p.commandError("list", "", output, err)
	}
	items := make([]sandbox.Sandbox, 0)
	for _, externalID := range strings.Fields(string(output)) {
		inspect, inspectErr := p.runner.Run(ctx, p.config.DockerBinary, "inspect", "--format", "{{json .}}", externalID)
		if inspectErr != nil {
			if missing(inspect) {
				continue
			}
			return nil, p.commandError("list", "", inspect, inspectErr)
		}
		var container dockerContainer
		if err := json.Unmarshal(inspect, &container); err != nil {
			return nil, sandbox.NewError(ProviderName, "list", sandbox.CodeInternal, err)
		}
		if expired(container, time.Now()) {
			if removed, err := p.runner.Run(ctx, p.config.DockerBinary, "rm", "--force", externalID); err != nil && !missing(removed) {
				return nil, p.commandError("expire", "", removed, err)
			}
			container.State = dockerState{Status: "removing"}
		}
		item := sandboxFromContainer(container, sandbox.Sandbox{})
		if item.ID != "" && sandbox.Matches(item, filter) {
			p.remember(item)
			items = append(items, item)
		}
	}
	return items, nil
}

func (p *Provider) Terminate(ctx context.Context, id domain.SandboxID) error {
	output, err := p.runner.Run(ctx, p.config.DockerBinary, "rm", "--force", containerName(id))
	if err != nil && !missing(output) {
		return p.commandError("terminate", id, output, err)
	}
	p.mu.Lock()
	if item, ok := p.known[id]; ok {
		item.State = sandbox.StateTerminated
		p.known[id] = item
	}
	p.mu.Unlock()
	return nil
}

type dockerState struct {
	Status    string `json:"Status"`
	Running   bool   `json:"Running"`
	Dead      bool   `json:"Dead"`
	OOMKilled bool   `json:"OOMKilled"`
	Error     string `json:"Error"`
}

type dockerContainer struct {
	ID      string      `json:"Id"`
	Name    string      `json:"Name"`
	Created string      `json:"Created"`
	State   dockerState `json:"State"`
	Config  struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

func sandboxFromContainer(container dockerContainer, fallback sandbox.Sandbox) sandbox.Sandbox {
	item := fallback
	item.Provider = ProviderName
	item.ExternalID = container.ID
	item.State = normalizeState(container.State)
	if value := container.Config.Labels[sandboxLabel]; value != "" {
		item.ID = domain.SandboxID(value)
	}
	if value := container.Config.Labels[workerLabel]; value != "" {
		item.WorkerID = domain.WorkerID(value)
	}
	_ = json.Unmarshal([]byte(container.Config.Labels[capabilitiesLabel]), &item.Capabilities)
	_ = json.Unmarshal([]byte(container.Config.Labels[labelsLabel]), &item.Labels)
	item.Metadata = map[string]string{"image": container.Config.Image, "name": strings.TrimPrefix(container.Name, "/"), "template": container.Config.Labels[templateLabel]}
	if created, err := time.Parse(time.RFC3339Nano, container.Created); err == nil {
		item.CreatedAt = created
	}
	return item
}

func expired(container dockerContainer, now time.Time) bool {
	value := container.Config.Labels[expiresLabel]
	deadline, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && !now.Before(deadline)
}

func normalizeState(state dockerState) sandbox.State {
	if state.Dead || state.OOMKilled || state.Error != "" {
		return sandbox.StateFailed
	}
	if state.Running || state.Status == "running" {
		return sandbox.StateRunning
	}
	switch state.Status {
	case "created", "restarting":
		return sandbox.StateCreating
	case "exited", "paused":
		return sandbox.StateStopped
	case "removing":
		return sandbox.StateTerminated
	default:
		return sandbox.StateUnknown
	}
}

func validateSpec(spec sandbox.CreateSpec) error {
	if spec.WorkerID == "" {
		return fmt.Errorf("worker ID is required")
	}
	parsed, err := url.ParseRequestURI(spec.ControlPlaneURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("valid control-plane URL is required")
	}
	if spec.BootstrapToken == "" {
		return fmt.Errorf("bootstrap token is required")
	}
	return nil
}

func newID() (domain.SandboxID, error) {
	buffer := make([]byte, 12)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return domain.SandboxID(hex.EncodeToString(buffer)), nil
}

func containerName(id domain.SandboxID) string { return "smart-route-worker-" + string(id) }
func instanceID(id domain.SandboxID) string {
	sum := sha256.Sum256([]byte(id))
	sum[6] = (sum[6] & 0x0f) | 0x40
	sum[8] = (sum[8] & 0x3f) | 0x80
	hexID := hex.EncodeToString(sum[:16])
	return hexID[:8] + "-" + hexID[8:12] + "-" + hexID[12:16] + "-" + hexID[16:20] + "-" + hexID[20:]
}
func missing(output []byte) bool {
	message := strings.ToLower(string(output))
	return strings.Contains(message, "no such container") || strings.Contains(message, "no such object")
}

func sortedCredentialKeys(values map[string]domain.CredentialRefID) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func cloneStrings(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (p *Provider) remember(item sandbox.Sandbox) {
	p.mu.Lock()
	p.known[item.ID] = item
	p.mu.Unlock()
}

func (p *Provider) commandError(operation string, id domain.SandboxID, output []byte, cause error) error {
	code := sandbox.CodeUnavailable
	var exitError *exec.ExitError
	if errors.As(cause, &exitError) {
		code = sandbox.CodeInternal
	}
	detail := strings.TrimSpace(string(output))
	if detail == "" {
		detail = cause.Error()
	}
	return &sandbox.ProviderError{Provider: ProviderName, Operation: operation, SandboxID: string(id), Code: code, Err: errors.New(detail)}
}
