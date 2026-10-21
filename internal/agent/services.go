//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/api/types/runc/options"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/gateway"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

// The labels of a cluster service's container, which say what it was
// run as.
const (
	versionLabel = "moostack.version"
	imageLabel   = "moostack.image"
	apiLabel     = "moostack.api"
	// digestLabel is the digest of the image the container was made
	// from, so that a rebuild of an image under the same name, as of a
	// release built again, is told apart.
	digestLabel = "moostack.image-digest"
)

// serviceName is what a cluster service may be called: its name names
// its container, directory and log.
var serviceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// services runs the cluster services in containerd, each in a container
// named for it, and keeps the services it runs in a file, which are the
// gateway's routes.
type services struct {
	client  *containerd.Client
	gateway *gateway.Gateway
	// file holds the services, dir the directory of each one's socket,
	// and logDir their output.
	file, dir, logDir string

	// running serializes changing the services' containers.
	running sync.Mutex

	mu   sync.Mutex
	list []*nodev1.Service
	// problems are why each service by name last failed to start, until
	// it starts.
	problems map[string]string
	// routed are the APIs routed for each service, by name: the one its
	// container was made for, and the one it is stored with. It changes
	// under running.
	routed map[string][]string
	// unknown are the services whose containers couldn't be read when
	// they were last routed, so that the API each was made for isn't
	// known, until each is routed. It changes under running.
	unknown map[string]bool
}

// loadServices reads the services the node runs from file, and routes
// their APIs through g, as route does, also those whose containers can't
// be read, as while containerd is down. A missing file is no services.
func loadServices(ctx context.Context, client *containerd.Client, g *gateway.Gateway, file, dir, logDir string) (*services, error) {
	s := &services{client: client, gateway: g, file: file, dir: dir, logDir: logDir, problems: map[string]string{}, routed: map[string][]string{}, unknown: map[string]bool{}}
	data, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		var stored []storedService
		if err := json.Unmarshal(data, &stored); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		for _, st := range stored {
			s.list = append(s.list, st.service())
		}
	}
	for _, svc := range s.list {
		// A service whose container can't be read is routed as unknown.
		_ = s.route(namespaced(ctx), svc)
	}
	return s, nil
}

// storedService is a service as the file keeps it.
type storedService struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	API     string `json:"api"`
	Image   string `json:"image"`
}

// service is the service st keeps.
func (st storedService) service() *nodev1.Service {
	return &nodev1.Service{Name: st.Name, Version: st.Version, Api: st.API, Image: st.Image}
}

// socket is the API socket of the service called name, on the host.
func (s *services) socket(name string) string {
	return filepath.Join(s.dir, name, servicev1.APISocket)
}

// namespaced is ctx in Moostack's containerd namespace.
func namespaced(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, hostlayout.ContainerdNamespace)
}

// Import imports the images of the OCI image layout archive r, and
// returns their names.
func (s *services) Import(ctx context.Context, r io.Reader) ([]string, error) {
	images, err := s.client.Import(namespaced(ctx), r)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, img := range images {
		names = append(names, img.Name)
	}
	return names, nil
}

// Run runs svc, replacing the service of its name, stores it, and routes
// its API to it once it runs.
func (s *services) Run(ctx context.Context, svc *nodev1.Service) error {
	if !serviceName.MatchString(svc.Name) || svc.Version == "" || svc.Api == "" || svc.Image == "" {
		return status.Errorf(codes.InvalidArgument, "a service needs a name of lowercase letters, digits and hyphens, a version, an API and an image: %v", svc)
	}
	s.running.Lock()
	defer s.running.Unlock()
	if _, err := s.client.GetImage(namespaced(ctx), svc.Image); errdefs.IsNotFound(err) {
		return status.Errorf(codes.FailedPrecondition, "the node has no image %s", svc.Image)
	} else if err != nil {
		return status.Errorf(codes.Unavailable, "reading the image %s: %v", svc.Image, err)
	}
	s.mu.Lock()
	var replaced *nodev1.Service
	if i := slices.IndexFunc(s.list, func(other *nodev1.Service) bool { return other.Name == svc.Name }); i >= 0 {
		replaced = s.list[i]
	}
	list := slices.DeleteFunc(slices.Clone(s.list), func(other *nodev1.Service) bool { return other.Name == svc.Name })
	s.mu.Unlock()
	// An API is another service's while it is routed for it, stored or
	// still running. A service whose container the agent couldn't read
	// when it started is routed first, so that the APIs of all are known,
	// and so is the one run again, so that whether it answers is known.
	if replaced != nil && s.unknown[replaced.Name] {
		s.route(namespaced(ctx), replaced)
	}
	for _, other := range list {
		if s.unknown[other.Name] {
			if err := s.route(namespaced(ctx), other); err != nil {
				return status.Errorf(codes.Unavailable, "the APIs of the service %s aren't known: %v", other.Name, err)
			}
		}
		if slices.Contains(s.routed[other.Name], svc.Api) {
			return status.Errorf(codes.FailedPrecondition, "the service %s serves %s already", other.Name, svc.Api)
		}
	}
	list = append(list, svc)
	slices.SortFunc(list, func(a, b *nodev1.Service) int { return strings.Compare(a.Name, b.Name) })
	if err := s.store(list); err != nil {
		return err
	}
	s.mu.Lock()
	s.list = list
	s.mu.Unlock()
	if err := s.start(ctx, svc, true); err != nil {
		return err
	}
	// The image of the service replaced goes, unless another service
	// runs it, so that a node keeps no image per release. Failing to
	// delete it leaves it, which doesn't affect the service.
	if replaced != nil && replaced.Image != svc.Image && !slices.ContainsFunc(list, func(other *nodev1.Service) bool { return other.Image == replaced.Image }) {
		s.client.ImageService().Delete(namespaced(ctx), replaced.Image)
	}
	return nil
}

// store writes list to the file.
func (s *services) store(list []*nodev1.Service) error {
	stored := []storedService{}
	for _, svc := range list {
		stored = append(stored, storedService{Name: svc.Name, Version: svc.Version, API: svc.Api, Image: svc.Image})
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	return writeFile(s.file, append(data, '\n'))
}

// StartAll starts every stored service that isn't running, as after the
// node boots, trying again every few seconds for those that fail, until
// all run or ctx ends. Each try starts the service as it is stored then,
// so that a try never undoes a Run in between.
func (s *services) StartAll(ctx context.Context) {
	var pending []string
	s.mu.Lock()
	for _, svc := range s.list {
		pending = append(pending, svc.Name)
	}
	s.mu.Unlock()
	for len(pending) > 0 {
		var failed []string
		for _, name := range pending {
			s.running.Lock()
			s.mu.Lock()
			i := slices.IndexFunc(s.list, func(svc *nodev1.Service) bool { return svc.Name == name })
			var svc *nodev1.Service
			if i >= 0 {
				svc = s.list[i]
			}
			s.mu.Unlock()
			var err error
			if svc != nil {
				err = s.start(ctx, svc, false)
			}
			s.running.Unlock()
			if err != nil {
				failed = append(failed, name)
			}
		}
		pending = failed
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// start makes sure svc's container runs, as svc says, and records why
// if it can't. Either way it routes what runs, as route does. The caller
// holds s.running. With repair, as when svc is run again, a service as
// svc says that doesn't answer its health check is made anew.
func (s *services) start(ctx context.Context, svc *nodev1.Service, repair bool) error {
	ctx = namespaced(ctx)
	err := s.ensure(ctx, svc, repair)
	if routeErr := s.route(ctx, svc); err == nil {
		err = routeErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.problems[svc.Name] = fmt.Sprintf("starting it: %v", err)
	} else {
		delete(s.problems, svc.Name)
	}
	return err
}

// ensure starts svc's container, first making it anew from svc's image
// if there is none, or the one there is runs another version, image or
// API, or another build of the image. A container made before the agent
// recorded its image's digest is taken to run the image there is, so
// that a new agent doesn't restart every service, and so is one whose
// image is gone, which it can run without. The new image is found
// and unpacked before the old container goes, so that a new image the
// node lacks, or can't unpack, leaves the old service running. One that
// unpacks but doesn't run doesn't. With repair, a container as svc says
// whose service doesn't answer its health check is made anew too, so that
// running a service again repairs it.
func (s *services) ensure(ctx context.Context, svc *nodev1.Service, repair bool) error {
	c, labels, err := s.container(ctx, svc.Name)
	if err != nil {
		return err
	}
	// The image is looked up once, so that the container is labelled with
	// the digest of the image it is made from, whatever is imported meanwhile.
	var digest string
	img, imgErr := s.client.GetImage(ctx, svc.Image)
	if imgErr == nil {
		digest = img.Target().Digest.String()
	} else if !errdefs.IsNotFound(imgErr) {
		return imgErr
	}
	if c != nil && labels[versionLabel] == svc.Version && labels[imageLabel] == svc.Image && labels[apiLabel] == svc.Api &&
		(labels[digestLabel] == "" || digest == "" || labels[digestLabel] == digest) &&
		(!repair || s.gateway.Health(ctx, svc.Api) == nil) {
		return s.startTask(ctx, c, svc)
	}
	if imgErr != nil {
		return imgErr
	}
	if err := unpack(ctx, img); err != nil {
		return err
	}
	if c != nil {
		// What the old service described mustn't be taken for the new
		// one's, so its API is routed again once the new one runs.
		s.gateway.Hold(svc.Api)
		if err := remove(ctx, c); err != nil {
			return err
		}
	}
	// A container that was removed only in part leaves its snapshot.
	if err := s.client.SnapshotService("").Remove(ctx, svc.Name); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	dir := filepath.Join(s.dir, svc.Name)
	c, err = s.client.NewContainer(ctx, svc.Name,
		containerd.WithImage(img),
		containerd.WithNewSnapshot(svc.Name, img),
		containerd.WithRuntime(plugins.RuntimeRuncV2, &options.Options{Root: hostlayout.RuncRoot}),
		containerd.WithContainerLabels(map[string]string{versionLabel: svc.Version, imageLabel: svc.Image, apiLabel: svc.Api, digestLabel: digest}),
		containerd.WithNewSpec(
			oci.WithImageConfig(img),
			oci.WithUIDGID(hostlayout.ServiceUser, hostlayout.ServiceUser),
			oci.WithCapabilities(nil),
			oci.WithNoNewPrivileges,
			oci.WithRootFSReadonly(),
			oci.WithCgroup(hostlayout.CgroupParent+"/"+svc.Name),
			oci.WithMounts([]specs.Mount{{Type: "bind", Source: dir, Destination: servicev1.SocketDir, Options: []string{"rbind", "rw"}}}),
		))
	if err != nil {
		return err
	}
	return s.startTask(ctx, c, svc)
}

// unpack unpacks img into snapshots, unless it is already.
func unpack(ctx context.Context, img containerd.Image) error {
	if unpacked, err := img.IsUnpacked(ctx, ""); err != nil {
		return err
	} else if !unpacked {
		// The layers are synced before their snapshots are committed, so
		// that a crash never leaves a service's files half written.
		return img.Unpack(ctx, "", containerd.WithUnpackApplyOpts(diff.WithSyncFs(true)))
	}
	return nil
}

// startTask starts c's task, unless it runs already, with a fresh
// directory for the service's socket.
func (s *services) startTask(ctx context.Context, c containerd.Container, svc *nodev1.Service) error {
	task, err := c.Task(ctx, nil)
	switch {
	case errdefs.IsNotFound(err):
	case err != nil:
		return err
	default:
		st, err := task.Status(ctx)
		if err != nil {
			return err
		}
		if st.Status == containerd.Running {
			return nil
		}
		if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil {
			return err
		}
	}
	if err := s.prepareDir(svc.Name); err != nil {
		return err
	}
	if err := os.MkdirAll(s.logDir, 0o700); err != nil {
		return err
	}
	task, err = c.NewTask(ctx, cio.LogFile(filepath.Join(s.logDir, svc.Name+".log")))
	if err != nil {
		return err
	}
	if err := task.Start(ctx); err != nil {
		task.Delete(ctx, containerd.WithProcessKill)
		return err
	}
	return nil
}

// prepareDir makes the directory of the service called name, which only
// the service's user can enter, inside s.dir, which only root can, and
// removes a socket an earlier run left.
func (s *services) prepareDir(name string) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(s.dir, 0o700); err != nil {
		return err
	}
	dir := filepath.Join(s.dir, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chown(dir, hostlayout.ServiceUser, hostlayout.ServiceUser); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, servicev1.APISocket)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// remove kills c's task, if it has one, and removes c with its snapshot.
func remove(ctx context.Context, c containerd.Container) error {
	if task, err := c.Task(ctx, nil); err == nil {
		if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil {
			return err
		}
	} else if !errdefs.IsNotFound(err) {
		return err
	}
	return c.Delete(ctx, containerd.WithSnapshotCleanup)
}

// route routes the API that svc's container was made for to svc's
// socket, so that a service that still runs after a failed replacement
// is reached at its API. svc's own API answers 'unavailable' until a
// container made for it runs, and an API routed for svc before, which
// is neither, is routed no more. If svc's container can't be read, svc
// is unknown: its own API answers 'unavailable' and is routed for it,
// next to what was before, which the container may still serve. ctx is
// in Moostack's namespace, and the caller holds s.running.
func (s *services) route(ctx context.Context, svc *nodev1.Service) error {
	_, labels, err := s.container(ctx, svc.Name)
	if err != nil {
		s.gateway.Hold(svc.Api)
		if !slices.Contains(s.routed[svc.Name], svc.Api) {
			s.routed[svc.Name] = append(s.routed[svc.Name], svc.Api)
		}
		s.unknown[svc.Name] = true
		return fmt.Errorf("reading its container: %w", err)
	}
	madeFor := labels[apiLabel]
	apis := []string{svc.Api}
	if madeFor != "" && madeFor != svc.Api {
		apis = append(apis, madeFor)
	}
	for _, before := range s.routed[svc.Name] {
		if !slices.Contains(apis, before) {
			s.gateway.Remove(before)
		}
	}
	s.routed[svc.Name] = apis
	delete(s.unknown, svc.Name)
	if madeFor != svc.Api {
		s.gateway.Hold(svc.Api)
	}
	if madeFor == "" {
		return nil
	}
	return s.gateway.Set(madeFor, s.socket(svc.Name))
}

// container is the container of the service called name, and its
// labels, or none if the service has none.
func (s *services) container(ctx context.Context, name string) (containerd.Container, map[string]string, error) {
	c, err := s.client.LoadContainer(ctx, name)
	if errdefs.IsNotFound(err) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	labels, err := c.Labels(ctx)
	if err != nil {
		return nil, nil, err
	}
	return c, labels, nil
}

// Status is each service the node runs, at the version its container
// was made for, none if it has none. A service is healthy if it answers
// its health call without a problem, and its last start didn't fail. All
// are asked at once.
func (s *services) Status(ctx context.Context) []*nodev1.Component {
	s.mu.Lock()
	list := slices.Clone(s.list)
	problems := maps.Clone(s.problems)
	s.mu.Unlock()
	components := make([]*nodev1.Component, len(list))
	var wg sync.WaitGroup
	for i, svc := range list {
		wg.Go(func() {
			_, labels, err := s.container(namespaced(ctx), svc.Name)
			c := &nodev1.Component{Name: svc.Name, Version: labels[versionLabel], Build: labels[digestLabel], Health: nodev1.Health_HEALTH_HEALTHY}
			if err != nil {
				c.Health, c.Problem = nodev1.Health_HEALTH_UNHEALTHY, fmt.Sprintf("reading its container: %v", err)
			} else if p := problems[svc.Name]; p != "" {
				c.Health, c.Problem = nodev1.Health_HEALTH_UNHEALTHY, p
			} else if err := s.gateway.Health(ctx, svc.Api); err != nil {
				c.Health, c.Problem = nodev1.Health_HEALTH_UNHEALTHY, err.Error()
			}
			components[i] = c
		})
	}
	wg.Wait()
	return components
}
