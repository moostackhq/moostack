package nodeclient

import (
	"bytes"
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// recordingAgent records the requests of the calls it answers.
type recordingAgent struct {
	nodev1.UnimplementedAgentServer
	mu      sync.Mutex
	imports []*nodev1.ImportImageRequest
	removes []*nodev1.RemoveRequest
}

func (a *recordingAgent) ImportImage(stream grpc.ClientStreamingServer[nodev1.ImportImageRequest, nodev1.ImportImageResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&nodev1.ImportImageResponse{})
		}
		if err != nil {
			return err
		}
		a.mu.Lock()
		a.imports = append(a.imports, req)
		a.mu.Unlock()
	}
}

func (a *recordingAgent) Remove(_ context.Context, req *nodev1.RemoveRequest) (*nodev1.RemoveResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.removes = append(a.removes, req)
	return &nodev1.RemoveResponse{}, nil
}

// serveRecording serves a over a socket, and returns a client of it.
func serveRecording(t *testing.T, a *recordingAgent) nodev1.AgentClient {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "agent.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	nodev1.RegisterAgentServer(s, a)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	conn, err := dial(socket, func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return nodev1.NewAgentClient(conn)
}

// The lease travels in an import's first request alone, even for an
// empty archive, and the chunks, none larger than importChunk, carry the
// archive whole.
func TestImportImageSendsTheLeaseInTheFirstRequestAlone(t *testing.T) {
	for _, size := range []int{0, 1, importChunk, importChunk + 1, 3*importChunk + 7} {
		a := &recordingAgent{}
		data := bytes.Repeat([]byte{'x'}, size)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		_, err := ImportImage(ctx, serveRecording(t, a), data, "the-lease")
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if len(a.imports) == 0 || a.imports[0].Lease != "the-lease" {
			t.Errorf("%d bytes: the first request is %v, want the lease in it", size, a.imports)
			continue
		}
		var got []byte
		for i, r := range a.imports {
			if i > 0 && r.Lease != "" {
				t.Errorf("%d bytes: request %d carries the lease too", size, i)
			}
			if len(r.Chunk) > importChunk {
				t.Errorf("%d bytes: request %d carries %d bytes", size, i, len(r.Chunk))
			}
			got = append(got, r.Chunk...)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("%d bytes: the agent got %d bytes", size, len(got))
		}
	}
}

// Remove sends the container's name and the lease, each in its place.
func TestRemoveSendsTheNameAndTheLease(t *testing.T) {
	a := &recordingAgent{}
	n := &Node{Agent: serveRecording(t, a)}
	if err := n.Remove(t.Context(), "dns", "the-lease"); err != nil {
		t.Fatal(err)
	}
	if len(a.removes) != 1 || a.removes[0].Name != "dns" || a.removes[0].Lease != "the-lease" {
		t.Errorf("Remove sent %v, want dns for the-lease", a.removes)
	}
}
