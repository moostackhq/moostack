package k3s

// Role is what a node is in its cluster: the control plane, or a node
// that joins it.
type Role struct {
	ControlPlane bool
	// name is how the operator sees the role.
	name string
	// service is k3s's systemd unit.
	service string
	// exec is what k3s's install script installs.
	exec string
	// uninstall is k3s's uninstall script.
	uninstall string
	// reservedMiB is the memory the kubelet keeps from pods for k3s.
	reservedMiB int
	// memLimitPercent is k3s's GOMEMLIMIT, as a share of the node's
	// memory.
	memLimitPercent int
}

var (
	ControlPlane = Role{ControlPlane: true, name: "control plane", service: "k3s", exec: "server", uninstall: "k3s-uninstall.sh", reservedMiB: 650, memLimitPercent: 25}
	Agent        = Role{name: "node", service: "k3s-agent", exec: "agent", uninstall: "k3s-agent-uninstall.sh", reservedMiB: 250, memLimitPercent: 10}
)

// RoleOf is ControlPlane or Agent.
func RoleOf(controlPlane bool) Role {
	if controlPlane {
		return ControlPlane
	}
	return Agent
}

// String is the role as the operator sees it.
func (r Role) String() string { return r.name }

// Service is k3s's systemd unit on a node of the role.
func (r Role) Service() string { return r.service }
