// Package buildinfo exposes metadata injected into release binaries at link time.
package buildinfo

const ProtocolVersion = "1"

var (
	Version = "dev"
	GitSHA  = "unknown"
	BuiltAt = "unknown"
)

type Info struct {
	Version         string `json:"version"`
	GitSHA          string `json:"git_sha"`
	BuiltAt         string `json:"built_at"`
	ProtocolVersion string `json:"protocol_version"`
}

func Current() Info {
	return Info{Version: Version, GitSHA: GitSHA, BuiltAt: BuiltAt, ProtocolVersion: ProtocolVersion}
}
