package nodeclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"path/filepath"
	"strings"
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
	// ended says an import's stream ended as one that sent it all does,
	// and ends is told how each import's stream ended.
	ended bool
	ends  chan error
}

func (a *recordingAgent) ImportImage(stream grpc.ClientStreamingServer[nodev1.ImportImageRequest, nodev1.ImportImageResponse]) error {
	for {
		req, err := stream.Recv()
		if a.ends != nil && err != nil {
			a.ends <- err
		}
		if err == io.EOF {
			a.mu.Lock()
			a.ended = true
			a.mu.Unlock()
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
	return &nodev1.RemoveResponse{Removed: req.Name == "dns"}, nil
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
		// Bytes of their own in each place, so that a chunk sent out of
		// order, or overwritten, shows.
		data := make([]byte, size)
		rand.NewChaCha8([32]byte{byte(size), byte(size >> 8), byte(size >> 16)}).Read(data)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		_, err := ImportImage(ctx, serveRecording(t, a), bytes.NewReader(data), "the-lease")
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
	if removed, err := n.Remove(t.Context(), "dns", "the-lease"); err != nil || !removed {
		t.Fatalf("Remove = %v, %v, want the agent's answer that it removed dns", removed, err)
	}
	if removed, err := n.Remove(t.Context(), "web", "the-lease"); err != nil || removed {
		t.Fatalf("Remove of web = %v, %v, want the agent's answer that it kept no web", removed, err)
	}
	if len(a.removes) != 2 || a.removes[0].Name != "dns" || a.removes[0].Lease != "the-lease" {
		t.Errorf("Remove sent %v, want dns for the-lease", a.removes)
	}
}

// failingReader reads n bytes, then fails with err.
type failingReader struct {
	n   int
	err error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.n == 0 {
		return 0, r.err
	}
	n := min(len(p), r.n)
	clear(p[:n])
	r.n -= n
	return n, nil
}

// An archive that fails to be read partway fails the import, saying
// why, and ends the call as one cut off, so that the agent imports
// nothing of it.
func TestAnArchiveThatCantBeReadEndsTheImport(t *testing.T) {
	a := &recordingAgent{ends: make(chan error, 1)}
	broken := errors.New("disk read error")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := ImportImage(ctx, serveRecording(t, a), &failingReader{n: 2*importChunk + 5, err: broken}, "the-lease")
	if !errors.Is(err, broken) || !strings.Contains(err.Error(), "reading the image's archive") {
		t.Errorf("ImportImage of an archive that fails to be read = %v, want why", err)
	}
	select {
	case end := <-a.ends:
		if end == io.EOF {
			t.Error("the agent's stream ended as one that sent the whole archive")
		}
	case <-ctx.Done():
		t.Fatal("the agent's stream didn't end")
	}
}

// gatedReader reads chunks of a chunk each, the first at once and each
// after only once the agent got the one before, so that a client that
// read the whole archive before it sent any would wait for ever.
type gatedReader struct {
	a      *recordingAgent
	chunks int
	read   int
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if r.read == r.chunks {
		return 0, io.EOF
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		r.a.mu.Lock()
		got := len(r.a.imports)
		r.a.mu.Unlock()
		if got >= r.read {
			break
		}
		if time.Now().After(deadline) {
			return 0, errors.New("the agent got nothing of what was read before")
		}
	}
	n := min(len(p), importChunk)
	clear(p[:n])
	r.read++
	return n, nil
}

// An import streams the archive as it reads it, a chunk at a time,
// rather than holding it whole first.
func TestAnImportStreamsTheArchiveAsItReadsIt(t *testing.T) {
	a := &recordingAgent{}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := ImportImage(ctx, serveRecording(t, a), &gatedReader{a: a, chunks: 8}, "the-lease"); err != nil {
		t.Fatalf("ImportImage of an archive read a chunk at a time: %v", err)
	}
	var size int
	for _, r := range a.imports {
		size += len(r.Chunk)
	}
	if size != 8*importChunk || !a.ended {
		t.Errorf("the agent got %d bytes, ended: %v, want the whole archive", size, a.ended)
	}
}
