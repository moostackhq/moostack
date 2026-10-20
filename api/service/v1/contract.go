package servicev1

// How a cluster service's container meets the agent. The agent mounts a
// directory of the host's at SocketDir in the container, and the service
// serves its API on the unix socket APISocket there.
const (
	SocketDir = "/run/moostack/service"
	APISocket = "api.sock"
)

// The metadata keys under which the agent tells a service who called:
// CallerKindKey holds the CallerKind's name, and CallerNodeKey the
// calling node's name, for a peer node. The agent drops any metadata
// whose key starts with CallerKeyPrefix from the call it receives, so a
// caller can't set them.
const (
	CallerKeyPrefix = "moostack-caller-"
	CallerKindKey   = CallerKeyPrefix + "kind"
	CallerNodeKey   = CallerKeyPrefix + "node"
)
