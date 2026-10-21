package nodeclient_test

import (
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// An updater from a release before UpdateAgent serves the Updater
// service without it.
func TestUpdateAgentSaysAnUpdaterWithoutItCantUpdateTheAgent(t *testing.T) {
	desc := nodev1.Updater_ServiceDesc
	desc.Streams = slices.DeleteFunc(slices.Clone(desc.Streams), func(s grpc.StreamDesc) bool { return s.StreamName == "UpdateAgent" })
	server := grpc.NewServer()
	server.RegisterService(&desc, nodev1.UnimplementedUpdaterServer{})
	socket := filepath.Join(t.TempDir(), "updater.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(l)
	defer server.Stop()
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = nodeclient.UpdateAgent(t.Context(), nodev1.NewUpdaterClient(conn), "2.0.0", []byte("agent"), time.Second)
	if err == nil || !strings.Contains(err.Error(), "the node's updater is from a release that can't update the agent") {
		t.Errorf("UpdateAgent on an updater without it: %v, want it to say the updater can't update the agent", err)
	}
}
