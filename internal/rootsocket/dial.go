package rootsocket

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// Reconnect has a connection to the agent's or the updater's socket
// connect again about a second apart at most, so that one that waits for
// the program after it was down for long, as across a restart, reaches it
// soon after it serves. Each attempt gets as long as gRPC gives it by
// default, so that a program slow to answer is reached all the same.
var Reconnect = grpc.WithConnectParams(grpc.ConnectParams{
	Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
	MinConnectTimeout: 20 * time.Second,
})

// Dial is a connection to the node API on the unix socket at path, as
// the agent and the updater make to each other's.
func Dial(path string) (*grpc.ClientConn, error) {
	return grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()), Reconnect)
}
