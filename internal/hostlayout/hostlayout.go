// Package hostlayout is where Moostack keeps its files on a node's host,
// and the files that mooctl writes there for the node components.
package hostlayout

import (
	"encoding/json"
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
	// Agent is the agent's binary: a symbolic link, relative, to the
	// active one of AgentSlots.
	Agent = BinDir + "/agent"
	// AgentSocket is where the agent serves the node API, to root only.
	AgentSocket = RunDir + "/agent.sock"
	// Updater is the updater's binary: a symbolic link, relative, to the
	// active one of UpdaterSlots.
	Updater = BinDir + "/updater"
	// Runc is runc's binary, which containerd's shim runs each container
	// with.
	Runc = BinDir + "/runc"
	// ContainerdPath is containerd's PATH, where it finds its shim and
	// the shim finds runc: BinDir first.
	ContainerdPath = BinDir + ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
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
	// LockFile keeps the node's operation lock across restarts of the
	// agent.
	LockFile = StateDir + "/lock"
	// RemovedIdentitiesFile keeps the fingerprints of the node identities
	// removed from the cluster, which the agent refuses as peers, as
	// MarshalRemovedIdentities writes them.
	RemovedIdentitiesFile = StateDir + "/removed-identities"
	// PeerPort is the port the agent listens for peers on, at the node's
	// private address.
	PeerPort = 7443
	// ServicesFile holds the cluster services the node runs, as JSON,
	// which are the agent's routes.
	ServicesFile = StateDir + "/services"
	// ServicesDir holds a directory for each cluster service, named for
	// it, which the agent mounts in the service's container, where the
	// service serves its API. It is root's alone, so that no other user
	// of the host reaches a service around the agent.
	ServicesDir = RunDir + "/services"
	// ServiceLogDir holds each cluster service's output, as <name>.log.
	ServiceLogDir = StateDir + "/logs"
	// ContainerdNamespace is the containerd namespace of Moostack's
	// containers and images.
	ContainerdNamespace = "moostack"
	// RuncRoot is where runc keeps the state of Moostack's containers.
	RuncRoot = RunDir + "/runc"
	// CgroupParent is the cgroup, under the host's cgroup root, of the
	// cluster services' containers, each in one of its own.
	CgroupParent = "/moostack"
	// CgroupDir is CgroupParent in the host's cgroup filesystem.
	CgroupDir = "/sys/fs/cgroup" + CgroupParent
	// ServiceUser is the user and group, by ID, that the cluster services
	// run as: nobody.
	ServiceUser = 65534
)

// ContainerdShim is the shim that containerd runs each container
// through, in BinDir.
const ContainerdShim = "containerd-shim-runc-v2"

// ContainerdFiles are containerd's binaries in BinDir, by name:
// containerd and its shim.
var ContainerdFiles = []string{"containerd", ContainerdShim}

// AgentSlots are the agent's A/B slots: the binary the agent runs from,
// which Agent links to, and the one the updater writes the next agent
// into.
var AgentSlots = [2]string{Agent + ".a", Agent + ".b"}

// AgentSwapFile records a swap of the agent that the updater has
// switched to but not yet seen healthy: the A/B slot it switched from,
// which it switches back to if it starts again with the file there.
const AgentSwapFile = StateDir + "/agent-swap"

// UpdaterSlots are the updater's A/B slots, as AgentSlots are the
// agent's, which the agent swaps.
var UpdaterSlots = [2]string{Updater + ".a", Updater + ".b"}

// UpdaterSwapFile records a swap of the updater that the agent has
// switched to but not yet seen healthy, as AgentSwapFile does the
// agent's.
const UpdaterSwapFile = StateDir + "/updater-swap"

// Slots are the A/B slots of the binary at path, if it runs from them:
// the agent's or the updater's.
func Slots(path string) ([2]string, bool) {
	switch path {
	case Agent:
		return AgentSlots, true
	case Updater:
		return UpdaterSlots, true
	}
	return [2]string{}, false
}

// The systemd units that run the node components.
const (
	ContainerdUnit = "moostack-containerd.service"
	UpdaterUnit    = "moostack-updater.service"
	AgentUnit      = "moostack-agent.service"
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
// directories, and its shims' sockets too, and reads no other config, so
// it doesn't clash with another containerd on the host. Kubernetes' CRI
// and NRI are off.
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

[plugins."io.containerd.shim.v1.manager"]
  socket_dir = "` + RunDir + `/containerd/s"
`

// MarshalRemovedIdentities is the fingerprints of removed identities as
// RemovedIdentitiesFile holds them: a JSON list.
func MarshalRemovedIdentities(fingerprints []string) []byte {
	data, err := json.Marshal(fingerprints)
	if err != nil {
		panic(err)
	}
	return append(data, '\n')
}

// ParseRemovedIdentities reads what MarshalRemovedIdentities wrote.
func ParseRemovedIdentities(data []byte) ([]string, error) {
	var fingerprints []string
	err := json.Unmarshal(data, &fingerprints)
	return fingerprints, err
}

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
