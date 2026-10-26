//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	remoteerrors "github.com/containerd/containerd/v2/core/remotes/errors"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// Pull pulls the public image ref, a normalized reference, from its
// registry, and unpacks it, and returns the name and the digest of the
// image pulled. As Import does, it waits for a run of a container to
// end, and pulls only if fence lets it once no run waits. It fails, as
// pullError says, for an image of another platform than the node's, at
// the registry's first answer that the node is rate limited, and with
// the other failures told apart. A pull that fails, as one cut off,
// leaves what it fetched to its lease, which expires after leaseExpiry,
// so that a pull again meanwhile fetches only what is missing.
func (s *containers) Pull(ctx context.Context, ref string, fence func() error) (string, string, error) {
	if err := nodev1.CheckReference(ref); err != nil {
		return "", "", err
	}
	s.running.Lock()
	defer s.running.Unlock()
	if err := fence(); err != nil {
		return "", "", err
	}
	// A lease holds what the pull writes until the image holds it, and
	// past a pull that fails until it expires.
	ctx, _, release, err := s.ownLease(namespaced(ctx))
	if err != nil {
		return "", "", err
	}
	node := platforms.Default()
	img, err := s.client.Pull(ctx, ref,
		containerd.WithResolver(newResolver(pullTransport())),
		containerd.WithPlatformMatcher(node),
		containerd.WithImageHandlerWrapper(platformChecked(s.client.ContentStore(), node)),
		containerd.WithPullUnpack,
		containerd.WithPullSnapshotter(snapshotter),
		// The layers are synced before their snapshots are committed, as
		// unpack syncs them.
		containerd.WithUnpackOpts([]containerd.UnpackOpt{containerd.WithUnpackApplyOpts(diff.WithSyncFs(true))}))
	if err != nil {
		return "", "", pullError(ref, err)
	}
	release()
	return img.Name(), img.Target().Digest.String(), nil
}

// pullTransport is the transport a pull reaches registries through:
// containerd's, which trusts the system's CAs alone.
func pullTransport() *http.Transport { return docker.DefaultHTTPTransport(nil) }

// newResolver is the resolver a pull reaches registries with, through
// transport, anonymously, which answers a registry's 429 as an error,
// as failFast does.
func newResolver(transport http.RoundTripper) remotes.Resolver {
	return docker.NewResolver(docker.ResolverOptions{Client: &http.Client{Transport: failFast{transport}}})
}

// failFast is a transport that answers a 429 as a *rateLimitedError,
// which containerd's resolver doesn't retry, as it retries a 429 response
// at once, so that a pull fails at the registry's first answer that the
// node is rate limited, and spends no more of what the registry allows
// it.
type failFast struct{ http.RoundTripper }

func (t failFast) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		return resp, err
	}
	resp.Body.Close()
	return nil, &rateLimitedError{host: req.URL.Host}
}

// rateLimitedError says the registry at host answered that the node is
// rate limited.
type rateLimitedError struct{ host string }

func (e *rateLimitedError) Error() string { return e.host + " answered 429 Too Many Requests" }

// platformChecked wraps a pull's handler so that the config of the image
// it pulls is checked against node, the node's platform, and the pull
// fails with a *platformError for an image of another, as containerd
// checks only an index's manifests, not a single one's: an image for
// another architecture would otherwise pull and unpack, and fail only
// once its process starts. store holds what the pull fetched.
func platformChecked(store content.Provider, node platforms.MatchComparer) func(images.Handler) images.Handler {
	return func(h images.Handler) images.Handler {
		return images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
			children, err := h.Handle(ctx, desc)
			if err != nil || !images.IsConfigType(desc.MediaType) {
				return children, err
			}
			data, err := content.ReadBlob(ctx, store, desc)
			if err != nil {
				return nil, err
			}
			var config ocispec.Image
			if err := json.Unmarshal(data, &config); err != nil {
				return nil, fmt.Errorf("reading the image's config: %w", err)
			}
			if p := platforms.Normalize(config.Platform); !node.Match(p) {
				return nil, &platformError{image: p}
			}
			return children, nil
		})
	}
}

// platformError says the image is for another platform than the node's.
type platformError struct{ image ocispec.Platform }

func (e *platformError) Error() string {
	return fmt.Sprintf("the image is for %s, not for this node's %s", platforms.Format(e.image), platforms.DefaultString())
}

// pullError is err, why the pull of ref failed, as a status whose code
// tells what failed: FAILED_PRECONDITION for an image of another platform
// than the node's, RESOURCE_EXHAUSTED for a registry that rate limits
// the node, PERMISSION_DENIED for one that denies access to the image, as
// Docker Hub does for a private repository and for one that doesn't
// exist, NOT_FOUND for an image the registry doesn't have, UNAVAILABLE
// for a registry that can't be reached or answers that it is down,
// CANCELED or DEADLINE_EXCEEDED for a pull whose call ended, and UNKNOWN
// for anything else.
func pullError(ref string, err error) error {
	var (
		platform   *platformError
		limited    *rateLimitedError
		unexpected remoteerrors.ErrUnexpectedStatus
	)
	code := codes.Unknown
	switch {
	case errors.As(err, &platform):
		code = codes.FailedPrecondition
	case strings.Contains(err.Error(), "no match for platform in manifest"):
		// An index without the node's platform; containerd says so only
		// in its text.
		code, err = codes.FailedPrecondition, fmt.Errorf("it has no image for this node's %s: %w", platforms.DefaultString(), err)
	case errors.As(err, &limited):
		code = codes.ResourceExhausted
	case errors.Is(err, docker.ErrInvalidAuthorization),
		errors.As(err, &unexpected) && (unexpected.StatusCode == http.StatusUnauthorized || unexpected.StatusCode == http.StatusForbidden):
		code = codes.PermissionDenied
	case errdefs.IsNotFound(err):
		code = codes.NotFound
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.As(err, &unexpected) && unexpected.StatusCode >= http.StatusInternalServerError,
		errors.As(err, new(net.Error)):
		// A registry that answers it is down, which containerd retries once,
		// or that can't be reached at all: a name that doesn't resolve, a
		// refused connection or a certificate the node doesn't trust.
		code = codes.Unavailable
	}
	return status.Error(code, err.Error())
}
