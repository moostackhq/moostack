// Package hostlayout is where Moostack keeps its files on a node's host,
// and the files that mooctl writes there for the node components.
package hostlayout

import (
	"errors"
	"fmt"
	"net/netip"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/identity"
	"github.com/moostackhq/moostack/internal/strictyaml"
)

// The directories Moostack keeps on a host. Nothing else of Moostack's is
// on the host, apart from its systemd units.
const (
	Root      = "/opt/moostack"
	ConfigDir = "/etc/moostack"
	StateDir  = "/var/lib/moostack"
	RunDir    = "/run/moostack"
)

// Dirs are the directories Moostack keeps on a host.
var Dirs = []string{Root, ConfigDir, StateDir, RunDir}

const (
	// BinDir holds the node components' binaries.
	BinDir = Root + "/bin"
	// Agent is the agent's binary.
	Agent = BinDir + "/agent"
	// AgentSocket is where the agent serves the node API, to root only.
	AgentSocket = RunDir + "/agent.sock"
	// Updater is the updater's binary.
	Updater = BinDir + "/updater"
	// UpdaterSocket is where the updater serves its node API, to root
	// only.
	UpdaterSocket = RunDir + "/updater.sock"
	// NodeFile is the node's NodeConfig.
	NodeFile = ConfigDir + "/node.yml"
	// ContainerdConfigFile is containerd's config, ContainerdConfig.
	ContainerdConfigFile = ConfigDir + "/containerd.toml"
	// ContainerdSocket is where containerd serves its API.
	ContainerdSocket = RunDir + "/containerd/containerd.sock"
	// GenerationFile holds the generation of the cluster directory that
	// the node last applied, with the directory's nodes, as JSON.
	GenerationFile = StateDir + "/generation"
	// PeerPort is the port the agent listens for peers on, at the node's
	// private address.
	PeerPort = 7443
)

// Identity is where the node keeps its node identity, which the agent
// and the updater share. The key is made on the node, in a directory of
// root's alone, and never leaves it.
var Identity = identity.Files{
	Key:  StateDir + "/identity/node.key",
	Cert: ConfigDir + "/node.crt",
	CA:   ConfigDir + "/ca.crt",
}

// Server is the binary of the program that serves the node API on
// socket, if one does.
func Server(socket string) (string, bool) {
	switch socket {
	case AgentSocket:
		return Agent, true
	case UpdaterSocket:
		return Updater, true
	}
	return "", false
}

// ContainerdConfig keeps containerd's files and sockets under Moostack's
// directories, and reads no other config, so it doesn't clash with
// another containerd on the host. Kubernetes' CRI and NRI are off.
const ContainerdConfig = `version = 4
root = "` + StateDir + `/containerd"
state = "` + RunDir + `/containerd"
imports = []
disabled_plugins = [
  "io.containerd.grpc.v1.cri",
  "io.containerd.cri.v1.images",
  "io.containerd.cri.v1.runtime",
  "io.containerd.podsandbox.controller.v1.podsandbox",
  "io.containerd.nri.v1.nri",
]

[plugins."io.containerd.server.v1.grpc"]
  address = "` + ContainerdSocket + `"
  uid = 0
  gid = 0

[plugins."io.containerd.server.v1.ttrpc"]
  address = "` + ContainerdSocket + `.ttrpc"
  uid = 0
  gid = 0

[plugins."io.containerd.internal.v1.opt"]
  path = "` + StateDir + `/containerd-opt"
`

// ModeDirect is the mode of a node operated by mooctl over SSH.
const ModeDirect = "direct"

// NodeConfig is what the node knows about itself.
type NodeConfig struct {
	// Cluster is the cluster's name.
	Cluster string `yaml:"cluster"`
	// Node is the node's name.
	Node string `yaml:"node"`
	// Mode is the mode of the node's cluster, ModeDirect for now.
	Mode string `yaml:"mode"`
	// PrivateAddress is the address other nodes reach the node at, where
	// the agent listens for them.
	PrivateAddress netip.Addr `yaml:"private_address"`
}

// Marshal is c as the node file holds it.
func (c NodeConfig) Marshal() ([]byte, error) { return yaml.Marshal(c) }

// ParseNodeConfig parses a node file.
func ParseNodeConfig(data []byte) (NodeConfig, error) {
	var c NodeConfig
	if err := strictyaml.Unmarshal(data, &c); err != nil {
		return NodeConfig{}, fmt.Errorf("node file: %w", err)
	}
	if c.Cluster == "" || c.Node == "" {
		return NodeConfig{}, errors.New("node file: no cluster or node name")
	}
	if !c.PrivateAddress.IsValid() {
		return NodeConfig{}, errors.New("node file: no private address")
	}
	if c.Mode != ModeDirect {
		return NodeConfig{}, fmt.Errorf("node file: unknown mode %q", c.Mode)
	}
	return c, nil
}
