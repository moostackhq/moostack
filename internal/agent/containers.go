//go:build linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/containerd/containerd/api/types/runc/options"
	containerd "github.com/containerd/containerd/v2/client"
	containerdcontainers "github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/diff"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/defaults"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	godigest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	servicev1 "github.com/moostackhq/moostack/api/service/v1"
	"github.com/moostackhq/moostack/internal/gateway"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

// The labels of a container, which say what it was run as.
const (
	imageLabel = "moostack.image"
	apiLabel   = "moostack.api"
	// digestLabel is the digest of the image the container was made
	// from, so that a rebuild of an image under the same name, as of a
	// release built again, is told apart.
	digestLabel = "moostack.image-digest"
	// propertiesLabel is the container's properties, as JSON.
	propertiesLabel = "moostack.properties"
)

// snapshotter is the snapshotter the containers' images are unpacked
// into and their containers made on. Each call names it, so that all
// agree, whatever default the namespace names.
const snapshotter = defaults.DefaultSnapshotter

// The leases of ownLease. One that isn't let go, as by an agent killed
// while it holds it, expires after leaseExpiry. leaseReleaseTimeout
// bounds letting one go, which goes on once the context it was taken in
// ends, so that a containerd that doesn't answer holds the containers no
// longer than it would any other call.
const (
	leaseExpiry         = 24 * time.Hour
	leaseReleaseTimeout = 10 * time.Second
)

// container is a container as the agent runs it, and as the file keeps
// it: its name, the image it runs and its properties.
type container struct {
	Name  string `json:"name"`
	Image string `json:"image"`
	properties
	// API is the gRPC service name of the API that the container serves
	// on its socket, which the agent routes calls to it by, empty if it
	// serves none.
	//
	// Temporary: the routed API is kept apart from the properties, which
	// say how a container runs, until it gets a design of its own.
	API string `json:"api,omitempty"`
}

// properties are how a container runs, as its run says them.
type properties struct {
	// User is the uid to run as, with a gid after a colon, the image's
	// user if nil.
	User *string `json:"user,omitempty"`
	// ReadOnlyRoot mounts the container's root read-only.
	ReadOnlyRoot bool `json:"read_only_root"`
	// Capabilities are the capabilities to run with, containerd's default
	// set if nil.
	Capabilities *[]string `json:"capabilities,omitempty"`
}

// containerOf is the container that Run runs as req says, with its
// capabilities in order, so that the same set in another order is the
// same container.
func containerOf(req *nodev1.Container) *container {
	c := &container{Name: req.Name, Image: req.Image, API: req.Api, properties: properties{User: req.User, ReadOnlyRoot: req.ReadOnlyRoot}}
	if req.Capabilities != nil {
		// Set but empty is no capabilities, unlike none set.
		caps := append([]string{}, req.Capabilities.Names...)
		slices.Sort(caps)
		c.Capabilities = &caps
	}
	return c
}

// label is p as propertiesLabel says it.
func (p properties) label() string {
	data, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// containers runs containers in containerd, each from its image with its
// properties, and keeps the containers it runs in a file. The APIs that
// they serve are the gateway's routes.
type containers struct {
	client  *containerd.Client
	gateway *gateway.Gateway
	// file holds the containers, dir the directory of each one's socket,
	// and logDir their output.
	file, dir, logDir string
	// runc is the runc binary that containerd's shim runs them with.
	runc string

	// running serializes changing the containers.
	running sync.Mutex

	mu   sync.Mutex
	list []*container
	// unused are the images that containers ran from and no longer do,
	// which are deleted unless a container runs from them or they were
	// imported since, kept until they are, so that a deletion that
	// failed, as while containerd didn't answer, is tried again by the
	// next run or removal. It changes under running.
	unused []unusedImage
	// problems are why each container by name last failed to start, until
	// it starts.
	problems map[string]string
	// routed are the APIs routed for each container, by name: the one it
	// was made for, and the one it is stored with. It changes under
	// running.
	routed map[string][]string
	// unknown are the containers that couldn't be read when they were
	// last routed, so that the API each was made for isn't known, until
	// each is routed. It changes under running.
	unknown map[string]bool
}

// loadContainers reads the containers the node runs from the file that
// cfg names, and routes their APIs through g, as route does, also those
// that can't be read, as while containerd is down. A missing file is no
// containers.
func loadContainers(ctx context.Context, client *containerd.Client, g *gateway.Gateway, cfg Config) (*containers, error) {
	s := &containers{client: client, gateway: g, file: cfg.ContainersFile, dir: cfg.ContainersDir, logDir: cfg.ContainerLogDir, runc: cfg.Runc, problems: map[string]string{}, routed: map[string][]string{}, unknown: map[string]bool{}}
	data, err := os.ReadFile(s.file)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		var st listFile
		if err := json.Unmarshal(data, &st); err != nil {
			return nil, fmt.Errorf("%s: %w", s.file, err)
		}
		s.list, s.unused = st.Containers, st.Unused
	}
	for _, c := range s.list {
		// A container that can't be read is routed as unknown, and why is
		// in its status.
		_ = s.route(namespaced(ctx), c)
	}
	return s, nil
}

// listFile is what the file keeps: the containers, and the images to
// delete once no container runs from them.
type listFile struct {
	Containers []*container  `json:"containers"`
	Unused     []unusedImage `json:"unused,omitempty"`
}

// unusedImage is an image that no container runs from, and when
// containerd last updated it, as an import does, when it became unused.
type unusedImage struct {
	Image   string    `json:"image"`
	Updated time.Time `json:"updated"`
}

// markUnused is unused with each of images unused from now on, as it is
// now in containerd, but for one containerd no longer holds. It fails
// with UNAVAILABLE if an image can't be read, since one marked without
// its time of update couldn't be told from one imported again, before
// the change it is part of changes anything. ctx is in Moostack's
// namespace.
func (s *containers) markUnused(ctx context.Context, unused []unusedImage, images ...string) ([]unusedImage, error) {
	next := slices.DeleteFunc(slices.Clone(unused), func(u unusedImage) bool { return slices.Contains(images, u.Image) })
	for _, image := range images {
		img, err := s.client.ImageService().Get(ctx, image)
		if errdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "reading the image %s, which the container ran from, changing nothing: %v", image, err)
		}
		next = append(next, unusedImage{Image: image, Updated: img.UpdatedAt})
	}
	return next, nil
}

// socket is the API socket of the container called name, on the host.
func (s *containers) socket(name string) string {
	return filepath.Join(s.dir, name, servicev1.APISocket)
}

// log is the file that the container called name writes its output to.
func (s *containers) log(name string) string {
	return filepath.Join(s.logDir, name+".log")
}

// namespaced is ctx in Moostack's containerd namespace.
func namespaced(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, hostlayout.ContainerdNamespace)
}

// Import imports the images of the OCI image layout archive r, and
// returns their names. It waits for a run of a container to end, as one
// that goes on after its call ended, so that the run's deletion of the
// container's other images doesn't take the one imported. An image
// imported again, as to go back to it, is no longer unused, as its time
// of update in containerd tells. It imports only if fence lets it once
// no run waits.
func (s *containers) Import(ctx context.Context, r io.Reader, fence func() error) ([]string, error) {
	s.running.Lock()
	defer s.running.Unlock()
	if err := fence(); err != nil {
		return nil, err
	}
	// A lease holds what the import writes until the images hold it.
	ctx, _, release, err := s.ownLease(namespaced(ctx))
	if err != nil {
		return nil, err
	}
	defer release()
	images, err := s.client.Import(ctx, r)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, img := range images {
		names = append(names, img.Name)
	}
	return names, nil
}

// checkContainer fails with INVALID_ARGUMENT unless c can be run, as
// nodev1.CheckContainer says, so that a container that can't be made is
// refused before the one it would replace goes.
func checkContainer(c *container) error {
	req := &nodev1.Container{Name: c.Name, Image: c.Image, User: c.User, ReadOnlyRoot: c.ReadOnlyRoot, Api: c.API}
	if c.Capabilities != nil {
		req.Capabilities = &nodev1.Capabilities{Names: *c.Capabilities}
	}
	return nodev1.CheckContainer(req)
}

// changeTimeout bounds a run or a removal of a container, which goes on
// once its call ends, from when it no longer waits for another: unpacking
// an image, and stopping, making and starting containers.
const changeTimeout = 5 * time.Minute

// change runs f as a change of the containers, within changeTimeout,
// once no other change runs, and only if fence lets it then.
func (s *containers) change(ctx context.Context, fence func() error, f func(ctx context.Context) error) error {
	s.running.Lock()
	defer s.running.Unlock()
	if err := fence(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, changeTimeout)
	defer cancel()
	return f(namespaced(ctx))
}

// Run runs c, replacing the container of its name, stores it, and routes
// its API to it once it runs, as a change, which fence lets happen.
func (s *containers) Run(ctx context.Context, c *container, fence func() error) error {
	if err := checkContainer(c); err != nil {
		return err
	}
	return s.change(ctx, fence, func(ctx context.Context) error {
		if _, err := s.client.GetImage(ctx, c.Image); errdefs.IsNotFound(err) {
			return status.Errorf(codes.FailedPrecondition, "the node has no image %s", c.Image)
		} else if err != nil {
			return status.Errorf(codes.Unavailable, "reading the image %s: %v", c.Image, err)
		}
		replaced := s.stored(c.Name)
		list := s.without(c.Name)
		// An API is another container's while it is routed for it, stored or
		// still running. A container that the agent couldn't read when it
		// started is routed first, so that the APIs of all are known, and so
		// is the one run again, so that whether it answers is known.
		if replaced != nil && s.unknown[replaced.Name] {
			// Why it still can't be read, if it can't, is the start's
			// to say.
			_ = s.route(ctx, replaced)
		}
		for _, other := range list {
			if s.unknown[other.Name] {
				if err := s.route(ctx, other); err != nil {
					return status.Errorf(codes.Unavailable, "the APIs of the container %s aren't known: %v", other.Name, err)
				}
			}
			if c.API != "" && slices.Contains(s.routed[other.Name], c.API) {
				return status.Errorf(codes.FailedPrecondition, "the container %s serves %s already", other.Name, c.API)
			}
		}
		// The list keeps a container of its own, rather than the caller's.
		c = &container{Name: c.Name, Image: c.Image, properties: c.properties, API: c.API}
		list = append(list, c)
		slices.SortFunc(list, func(a, b *container) int { return strings.Compare(a.Name, b.Name) })
		// The image of the container it replaces is unused once the new one
		// runs, which deleting it waits for.
		unused := s.unused
		if replaced != nil {
			var err error
			if unused, err = s.markUnused(ctx, unused, replaced.Image); err != nil {
				return err
			}
		}
		if err := s.store(list, unused); err != nil {
			return err
		}
		s.mu.Lock()
		s.list = list
		s.mu.Unlock()
		s.unused = unused
		if err := s.start(ctx, c, true); err != nil {
			return err
		}
		// The other tags of its image go too, which an update imported but
		// never ran, as when it stopped before the run, so that a node
		// keeps no image per release.
		s.deleteUnused(ctx, otherTags(ctx, s.client, c.Image))
		return s.deleteAllUnused(ctx)
	})
}

// without is the stored containers but the one called name, in a list
// of their own.
func (s *containers) without(name string) []*container {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(s.list), func(c *container) bool { return c.Name == name })
}

// all are the stored containers, in a list of their own.
func (s *containers) all() []*container {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.list)
}

// deleteAllUnused deletes the unused images, as deleteUnused does, and
// keeps those that are left. ctx is in Moostack's namespace, and the
// caller holds s.running.
func (s *containers) deleteAllUnused(ctx context.Context) error {
	if len(s.unused) == 0 {
		return nil
	}
	left := s.deleteUnused(ctx, s.unused)
	if slices.Equal(left, s.unused) {
		return nil
	}
	if err := s.store(s.all(), left); err != nil {
		return err
	}
	s.unused = left
	return nil
}

// deleteUnused deletes each of images that no container runs from and
// that containerd didn't update since it became unused, as an import
// again does, and returns those still to delete: those it failed to
// delete, and those a container that the node no longer keeps runs
// from, as the one a failed replacement left running. An image a kept
// container runs from, or imported since, isn't unused, and isn't
// returned. If containerd's containers can't be read, none are deleted.
// ctx is in Moostack's namespace.
func (s *containers) deleteUnused(ctx context.Context, images []unusedImage) []unusedImage {
	kept := s.all()
	made, err := s.client.ContainerService().List(ctx)
	if err != nil {
		return slices.Clone(images)
	}
	var left []unusedImage
	for _, u := range images {
		if slices.ContainsFunc(kept, func(c *container) bool { return c.Image == u.Image }) {
			continue
		}
		if slices.ContainsFunc(made, func(c containerdcontainers.Container) bool { return c.Labels[imageLabel] == u.Image }) {
			left = append(left, u)
			continue
		}
		img, err := s.client.ImageService().Get(ctx, u.Image)
		switch {
		case errdefs.IsNotFound(err):
			continue
		case err != nil:
			left = append(left, u)
			continue
		case !img.UpdatedAt.Equal(u.Updated):
			continue
		}
		if err := s.client.ImageService().Delete(ctx, u.Image); err != nil && !errdefs.IsNotFound(err) {
			left = append(left, u)
		}
	}
	return left
}

// otherTags are the images that client holds of image's repository under
// another tag, as they are now, none if image has no tag or they can't
// be listed.
func otherTags(ctx context.Context, client *containerd.Client, image string) []unusedImage {
	i := strings.LastIndex(image, ":")
	if i < 0 || i < strings.LastIndex(image, "/") {
		return nil
	}
	images, err := client.ImageService().List(ctx)
	if err != nil {
		return nil
	}
	var others []unusedImage
	for _, img := range images {
		if img.Name != image && strings.HasPrefix(img.Name, image[:i+1]) {
			others = append(others, unusedImage{Image: img.Name, Updated: img.UpdatedAt})
		}
	}
	return others
}

// store writes list and unused to the file.
func (s *containers) store(list []*container, unused []unusedImage) error {
	data, err := json.Marshal(listFile{Containers: append([]*container{}, list...), Unused: unused})
	if err != nil {
		return err
	}
	return writeFile(s.file, append(data, '\n'))
}

// StartAll starts every stored container that isn't running, as after the
// node boots, trying again every few seconds for those that fail, until
// all run or ctx ends. Each try starts the container as it is stored
// then, so that a try never undoes a Run in between.
func (s *containers) StartAll(ctx context.Context) {
	var pending []string
	s.mu.Lock()
	for _, c := range s.list {
		pending = append(pending, c.Name)
	}
	s.mu.Unlock()
	for len(pending) > 0 {
		var failed []string
		for _, name := range pending {
			s.running.Lock()
			c := s.stored(name)
			var err error
			if c != nil {
				err = s.start(ctx, c, false)
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

// stored is the stored container called name, nil if none is.
func (s *containers) stored(name string) *container {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := slices.IndexFunc(s.list, func(c *container) bool { return c.Name == name }); i >= 0 {
		return s.list[i]
	}
	return nil
}

// start makes sure c's container runs, as c says, and records why if it
// can't. Either way it routes what runs, as route does. The caller holds
// s.running. With repair, as when c is run again, a container as c says
// that doesn't answer its API's health check is made anew.
func (s *containers) start(ctx context.Context, c *container, repair bool) error {
	ctx = namespaced(ctx)
	err := s.ensure(ctx, c, repair)
	if routeErr := s.route(ctx, c); err == nil {
		err = routeErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.problems[c.Name] = fmt.Sprintf("starting it: %v", err)
	} else {
		delete(s.problems, c.Name)
	}
	return err
}

// ensure starts c's container, first making it anew from c's image if
// there is none, or the one there is was made from another image, build
// of the image, API or properties. One whose image is gone is taken to
// run the image there was, which it can run without. The new image is
// found and unpacked before the old container goes, so that a new image
// the node lacks, or can't unpack, leaves the old container running,
// started again if it isn't, as after the node rebooted. One that
// unpacks but doesn't run doesn't. With repair, a container as c says
// whose API doesn't answer its health check is made anew too, so that
// running a container again repairs it.
func (s *containers) ensure(ctx context.Context, c *container, repair bool) error {
	existing, labels, err := s.load(ctx, c.Name)
	if err != nil {
		return err
	}
	// The image is looked up once, so that the container is labelled
	// with the digest of the image it is made from, whatever is
	// imported meanwhile.
	var digest string
	img, imgErr := s.client.GetImage(ctx, c.Image)
	if imgErr == nil {
		digest = img.Target().Digest.String()
	} else if !errdefs.IsNotFound(imgErr) {
		return imgErr
	}
	same := existing != nil && labels[imageLabel] == c.Image && labels[apiLabel] == c.API &&
		labels[propertiesLabel] == c.properties.label() && (digest == "" || labels[digestLabel] == digest)
	if same && (!repair || c.API == "" || s.gateway.Health(ctx, c.API) == nil) {
		return s.startTask(ctx, existing, c)
	}
	// While the new container can't be made, the old one runs on, started
	// again if it doesn't, as after the node rebooted.
	keepOld := func(err error) error {
		if existing != nil {
			if startErr := s.startTask(ctx, existing, &container{Name: c.Name, API: labels[apiLabel]}); startErr != nil {
				return errors.Join(err, startErr)
			}
		}
		return err
	}
	if imgErr != nil {
		return keepOld(imgErr)
	}
	var config oci.Image = img
	if c.User != nil {
		if config, err = imageWithoutUser(ctx, img); err != nil {
			return keepOld(err)
		}
	}
	// A lease holds what the new container is made on until it holds it.
	leased, lease, release, err := s.ownLease(ctx)
	if err != nil {
		return keepOld(err)
	}
	defer release()
	if err := unpack(leased, s.client, lease, img); err != nil {
		return keepOld(err)
	}
	if existing != nil {
		// What the old container described mustn't be taken for the new
		// one's, so its API is routed again once the new one runs.
		if c.API != "" {
			s.gateway.Hold(c.API)
		}
		if err := deleteContainer(leased, existing); err != nil {
			return err
		}
	}
	if err := s.removeSnapshot(leased, c.Name); err != nil {
		return err
	}
	// The log of a container replaced by another starts empty; one made
	// anew as it was, to repair it, keeps what says why it failed.
	if !same {
		if err := truncate(s.log(c.Name)); err != nil {
			return err
		}
	}
	made, err := s.client.NewContainer(leased, c.Name,
		containerd.WithImage(img),
		containerd.WithSnapshotter(snapshotter),
		containerd.WithNewSnapshot(c.Name, img),
		containerd.WithRuntime(plugins.RuntimeRuncV2, &options.Options{Root: hostlayout.RuncRoot}),
		containerd.WithContainerLabels(map[string]string{imageLabel: c.Image, apiLabel: c.API, digestLabel: digest, propertiesLabel: c.properties.label()}),
		containerd.WithNewSpec(s.specOf(c, config)...))
	if err != nil {
		return err
	}
	return s.startTask(leased, made, c)
}

// specOf is how c's container runs, from config, the image's config:
// as c's properties say, always without new privileges, in a cgroup of
// its own, and with its socket directory mounted if it serves an API.
func (s *containers) specOf(c *container, config oci.Image) []oci.SpecOpts {
	spec := []oci.SpecOpts{oci.WithImageConfig(config)}
	if c.User != nil {
		spec = append(spec, oci.WithUser(*c.User))
	}
	if c.Capabilities != nil {
		spec = append(spec, oci.WithCapabilities(*c.Capabilities), withAmbientCapabilities(*c.Capabilities))
	}
	spec = append(spec, oci.WithNoNewPrivileges, oci.WithCgroup(hostlayout.CgroupParent+"/"+c.Name))
	if c.ReadOnlyRoot {
		spec = append(spec, oci.WithRootFSReadonly())
	}
	if c.API != "" {
		dir := filepath.Join(s.dir, c.Name)
		spec = append(spec, oci.WithMounts([]specs.Mount{{Type: "bind", Source: dir, Destination: servicev1.SocketDir, Options: []string{"rbind", "rw"}}}))
	}
	return spec
}

// imageWithoutUser is img as WithImageConfig reads it, with its
// config's user left out, for a container given a user of its own:
// WithImageConfig looks the image's user up in the image, refusing one
// the image lacks, before the given one replaces it.
func imageWithoutUser(ctx context.Context, img containerd.Image) (oci.Image, error) {
	desc, err := img.Config(ctx)
	if err != nil {
		return nil, err
	}
	data, err := content.ReadBlob(ctx, img.ContentStore(), desc)
	if err != nil {
		return nil, err
	}
	if data, err = configWithoutUser(data); err != nil {
		return nil, err
	}
	desc.Digest, desc.Size = godigest.FromBytes(data), int64(len(data))
	return imageWithConfig{img, desc, data}, nil
}

// configWithoutUser is the image config data with its user left out,
// under whatever case of its keys, as encoding/json matches them when
// WithImageConfig reads it. Every other field stays as it is.
func configWithoutUser(data []byte) ([]byte, error) {
	var config map[string]json.RawMessage
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	for key, raw := range config {
		if !strings.EqualFold(key, "config") || string(raw) == "null" {
			continue
		}
		var run map[string]json.RawMessage
		if err := json.Unmarshal(raw, &run); err != nil {
			return nil, err
		}
		for field := range run {
			if strings.EqualFold(field, "User") {
				delete(run, field)
			}
		}
		edited, err := json.Marshal(run)
		if err != nil {
			return nil, err
		}
		config[key] = edited
	}
	return json.Marshal(config)
}

// imageWithConfig is an image whose config is data, at desc.
type imageWithConfig struct {
	containerd.Image
	desc ocispec.Descriptor
	data []byte
}

// Config is the descriptor of i's config, data.
func (i imageWithConfig) Config(context.Context) (ocispec.Descriptor, error) { return i.desc, nil }

// ContentStore is the image's content store, which holds i's config too.
func (i imageWithConfig) ContentStore() content.Store {
	return configStore{Store: i.Image.ContentStore(), desc: i.desc, data: i.data}
}

// configStore is a content store that holds data at desc besides what
// Store holds.
type configStore struct {
	content.Store
	desc ocispec.Descriptor
	data []byte
}

// ReaderAt reads data for desc, and the rest from Store.
func (s configStore) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	if desc.Digest != s.desc.Digest {
		return s.Store.ReaderAt(ctx, desc)
	}
	return blob{bytes.NewReader(s.data)}, nil
}

// blob is a content.ReaderAt of bytes in memory.
type blob struct{ *bytes.Reader }

// Close closes nothing: the bytes are in memory.
func (blob) Close() error { return nil }

// withAmbientCapabilities keeps caps across the exec of a process that
// runs as a user other than root, which WithCapabilities alone loses: it
// sets them inheritable and ambient too.
func withAmbientCapabilities(caps []string) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containerdcontainers.Container, s *specs.Spec) error {
		s.Process.Capabilities.Inheritable = slices.Clone(caps)
		s.Process.Capabilities.Ambient = slices.Clone(caps)
		return nil
	}
}

// removeSnapshot removes the snapshot of the container called name, which
// one that was removed only in part leaves. ctx is in Moostack's
// namespace.
func (s *containers) removeSnapshot(ctx context.Context, name string) error {
	if err := s.client.SnapshotService(snapshotter).Remove(ctx, name); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// ownLease creates a lease of the agent's own, and returns ctx with it
// set, the lease, and a function that lets go of it. containerd's clients
// take the lease set on ctx for theirs, rather than one that ctx's caller
// names in its call's header. Letting go of it goes on once ctx ends.
func (s *containers) ownLease(ctx context.Context) (context.Context, leases.Lease, func(), error) {
	lease, err := s.client.LeasesService().Create(ctx, leases.WithRandomID(), leases.WithExpiration(leaseExpiry))
	if err != nil {
		return nil, leases.Lease{}, nil, err
	}
	release := func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseReleaseTimeout)
		defer cancel()
		// One not let go of expires after leaseExpiry.
		_ = s.client.LeasesService().Delete(ctx, lease)
	}
	return leases.WithLease(ctx, lease.ID), lease, release, nil
}

// unpack unpacks img into snapshots, unless it is already, holding them
// in lease, which ctx carries, from before it finds them there. Once
// the import of a rebuild that changes only the config replaced the
// image they were unpacked for, nothing but a container holds them: the
// old one, which goes before the new one is made on them, if there is
// one. Without the lease, garbage collection could take them in
// between.
func unpack(ctx context.Context, client *containerd.Client, lease leases.Lease, img containerd.Image) error {
	diffIDs, err := img.RootFS(ctx)
	if err != nil {
		return err
	}
	if err := client.LeasesService().AddResource(ctx, lease,
		leases.Resource{ID: identity.ChainID(diffIDs).String(), Type: "snapshots/" + snapshotter}); err != nil {
		return err
	}
	if unpacked, err := img.IsUnpacked(ctx, snapshotter); err != nil {
		return err
	} else if !unpacked {
		// The layers are synced before their snapshots are committed, so
		// that a crash never leaves a container's files half written.
		return img.Unpack(ctx, snapshotter, containerd.WithUnpackApplyOpts(diff.WithSyncFs(true)))
	}
	return nil
}

// startTask starts made's task, the container of c, unless it runs
// already, with a fresh directory for its socket if it serves an API,
// and none otherwise. If that fails, what an agent that crashed while
// containerd made or deleted a task of that name may have left is
// cleaned up, as cleanTask does, and the start is tried again.
func (s *containers) startTask(ctx context.Context, made containerd.Container, c *container) error {
	err := s.startTaskOnce(ctx, made, c)
	if err == nil {
		return nil
	}
	if cleanErr := s.cleanTask(ctx, made.ID()); cleanErr != nil {
		return fmt.Errorf("%w; cleaning up what a crash may have left: %v", err, cleanErr)
	}
	return s.startTaskOnce(ctx, made, c)
}

// cleanTask deletes the task of the container id, if it has one that
// isn't running, and otherwise, once containerd says it holds no task of
// that name, runc's state of one and its bundle, which containerd itself
// cleans up only when it starts, as one whose making or deleting stopped
// when the agent that asked for it crashed leaves them. ctx is in
// Moostack's namespace.
func (s *containers) cleanTask(ctx context.Context, id string) error {
	_, err := s.client.TaskService().Delete(ctx, &tasks.DeleteTaskRequest{ContainerID: id})
	if err == nil {
		return nil
	}
	if !errdefs.IsNotFound(errgrpc.ToNative(err)) {
		return err
	}
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}
	if out, err := exec.CommandContext(ctx, s.runc, "--root", filepath.Join(hostlayout.RuncRoot, ns), "delete", "--force", id).CombinedOutput(); err != nil && !strings.Contains(string(out), "does not exist") {
		return fmt.Errorf("deleting runc's state of %s: %v: %s", id, err, out)
	}
	return deleteBundle(filepath.Join(hostlayout.ContainerdState, "io.containerd.runtime.v2.task", ns, id))
}

// deleteBundle deletes the bundle of a task that containerd no longer
// holds, at path, as containerd deletes one when it starts: its root is
// unmounted, and removed only once empty, so that nothing of the
// snapshot mounted there goes.
func deleteBundle(path string) error {
	work, workErr := os.Readlink(filepath.Join(path, "work"))
	rootfs := filepath.Join(path, "rootfs")
	if err := mount.UnmountRecursive(rootfs, 0); err != nil {
		return err
	}
	if err := os.Remove(rootfs); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if workErr == nil {
		return os.RemoveAll(work)
	}
	return nil
}

// startTaskOnce is startTask, failing with ALREADY_EXISTS if containerd
// holds a task it can't load.
func (s *containers) startTaskOnce(ctx context.Context, made containerd.Container, c *container) error {
	task, err := made.Task(ctx, nil)
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
	if c.API != "" {
		spec, err := made.Spec(ctx)
		if err != nil {
			return err
		}
		if err := s.prepareDir(c.Name, spec.Process.User); err != nil {
			return err
		}
	} else if err := os.RemoveAll(filepath.Join(s.dir, c.Name)); err != nil {
		// One that served an API before leaves its socket directory.
		return err
	}
	if err := os.MkdirAll(s.logDir, 0o700); err != nil {
		return err
	}
	task, err = made.NewTask(ctx, cio.LogFile(s.log(c.Name)))
	if err != nil {
		return err
	}
	if err := task.Start(ctx); err != nil {
		// A task left behind is cleaned up by the next start, as cleanTask
		// does.
		_, _ = task.Delete(ctx, containerd.WithProcessKill)
		return err
	}
	return nil
}

// prepareDir makes the socket directory of the container called name,
// which only the container's user can enter, inside s.dir, which only
// root can, and removes a socket an earlier run left.
func (s *containers) prepareDir(name string, user specs.User) error {
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
	if err := os.Chown(dir, int(user.UID), int(user.GID)); err != nil {
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

// truncate empties the file at path, if there is one.
func truncate(path string) error {
	if err := os.Truncate(path, 0); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// Remove stops and deletes the container called name, if the node keeps
// one, with its snapshot, empties its log, and deletes its image and the
// other unused images unless a container runs from them, as a change,
// which fence lets happen. It deletes the container before it stores the
// list without it, so that a crash in between leaves it kept, for a
// Remove again to finish.
func (s *containers) Remove(ctx context.Context, name string, fence func() error) error {
	if err := nodev1.CheckName(name); err != nil {
		return err
	}
	return s.change(ctx, fence, func(ctx context.Context) error {
		if c := s.stored(name); c != nil {
			if err := s.remove(ctx, c); err != nil {
				return err
			}
		}
		return s.deleteAllUnused(ctx)
	})
}

// remove removes the stored container c, as Remove says, but for the
// unused images, its own among them. ctx is in Moostack's namespace, and
// the caller holds s.running.
func (s *containers) remove(ctx context.Context, c *container) error {
	// Its image is read first, so that a removal that can't changes
	// nothing.
	unused, err := s.markUnused(ctx, s.unused, c.Image)
	if err != nil {
		return err
	}
	existing, _, err := s.load(ctx, c.Name)
	if err != nil {
		return err
	}
	if existing != nil {
		if err := deleteContainer(ctx, existing); err != nil {
			return err
		}
	}
	if err := s.removeSnapshot(ctx, c.Name); err != nil {
		return err
	}
	if err := truncate(s.log(c.Name)); err != nil {
		return err
	}
	list := s.without(c.Name)
	if err := s.store(list, unused); err != nil {
		return err
	}
	s.mu.Lock()
	s.list = list
	delete(s.problems, c.Name)
	s.mu.Unlock()
	s.unused = unused
	for _, api := range s.routed[c.Name] {
		s.gateway.Remove(api)
	}
	delete(s.routed, c.Name)
	delete(s.unknown, c.Name)
	// What a container left in its socket directory goes with it; what
	// can't go stays in /run until the node reboots, as the container is
	// gone already.
	_ = os.RemoveAll(filepath.Join(s.dir, c.Name))
	return nil
}

// deleteContainer kills c's task, if it has one, and deletes c with its
// snapshot.
func deleteContainer(ctx context.Context, c containerd.Container) error {
	if task, err := c.Task(ctx, nil); err == nil {
		if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil {
			return err
		}
	} else if !errdefs.IsNotFound(err) {
		return err
	}
	return c.Delete(ctx, containerd.WithSnapshotCleanup)
}

// route routes the API that c's container was made for to c's socket, so
// that a container that still runs after a failed replacement is reached
// at its API. c's own API answers 'unavailable' until a container made
// for it runs, and an API routed for c before, which is neither, is
// routed no more. If c's container can't be read, c is unknown: its own
// API answers 'unavailable' and is routed for it, next to what was
// before, which the container may still serve. ctx is in Moostack's
// namespace, and the caller holds s.running.
func (s *containers) route(ctx context.Context, c *container) error {
	_, labels, err := s.load(ctx, c.Name)
	if err != nil {
		if c.API != "" {
			s.gateway.Hold(c.API)
			if !slices.Contains(s.routed[c.Name], c.API) {
				s.routed[c.Name] = append(s.routed[c.Name], c.API)
			}
		}
		s.unknown[c.Name] = true
		return fmt.Errorf("reading its container: %w", err)
	}
	madeFor := labels[apiLabel]
	var apis []string
	if c.API != "" {
		apis = append(apis, c.API)
	}
	if madeFor != "" && madeFor != c.API {
		apis = append(apis, madeFor)
	}
	for _, before := range s.routed[c.Name] {
		if !slices.Contains(apis, before) {
			s.gateway.Remove(before)
		}
	}
	s.routed[c.Name] = apis
	delete(s.unknown, c.Name)
	if c.API != "" && madeFor != c.API {
		s.gateway.Hold(c.API)
	}
	if madeFor == "" {
		return nil
	}
	return s.gateway.Set(madeFor, s.socket(c.Name))
}

// load is the containerd container called name, and its labels, or none
// if there is none.
func (s *containers) load(ctx context.Context, name string) (containerd.Container, map[string]string, error) {
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

// Report is each container the node keeps, each container read and each
// API asked for its health once, all at once, so that status takes no
// longer than the slowest of them.
//
// A container is reported with the image it was made from, unless its
// container can't be read or is gone, whether its process runs,
// unspecified if that can't be read, and for one with an API, whether the
// API answers its health call. The problem is why its last start failed,
// if it did, or else why it failed or its API is unhealthy.
func (s *containers) Report(ctx context.Context) []*nodev1.ContainerStatus {
	s.mu.Lock()
	list := slices.Clone(s.list)
	problems := maps.Clone(s.problems)
	s.mu.Unlock()
	statuses := make([]*nodev1.ContainerStatus, len(list))
	var wg sync.WaitGroup
	for i, c := range list {
		wg.Go(func() {
			statuses[i] = s.status(ctx, c, problems[c.Name])
		})
	}
	wg.Wait()
	return statuses
}

// status is c as it runs, whose last start failed with problem unless
// that is empty.
func (s *containers) status(ctx context.Context, c *container, problem string) *nodev1.ContainerStatus {
	st := &nodev1.ContainerStatus{Name: c.Name, State: nodev1.TaskState_TASK_STATE_FAILED, Problem: problem}
	made, labels, err := s.load(namespaced(ctx), c.Name)
	switch {
	case err != nil:
		st.State, st.Problem = nodev1.TaskState_TASK_STATE_UNSPECIFIED, fmt.Sprintf("reading its container: %v", err)
	case made == nil:
		if st.Problem == "" {
			st.Problem = "it has no container"
		}
	default:
		st.Image, st.Digest = labels[imageLabel], labels[digestLabel]
		st.State, st.ExitCode, err = taskState(namespaced(ctx), made)
		if err != nil && st.Problem == "" {
			st.Problem = err.Error()
		}
	}
	if c.API != "" {
		st.Health = nodev1.Health_HEALTH_HEALTHY
		if st.Problem != "" {
			st.Health = nodev1.Health_HEALTH_UNHEALTHY
		} else if err := s.gateway.Health(ctx, c.API); err != nil {
			st.Health, st.Problem = nodev1.Health_HEALTH_UNHEALTHY, err.Error()
		}
	}
	return st
}

// taskState is whether made's process runs, the code it exited with if
// it did, and why it failed if it neither runs nor exited, or why it
// can't tell.
func taskState(ctx context.Context, made containerd.Container) (nodev1.TaskState, uint32, error) {
	task, err := made.Task(ctx, nil)
	if errdefs.IsNotFound(err) {
		return nodev1.TaskState_TASK_STATE_FAILED, 0, errors.New("it has no process")
	}
	if err != nil {
		return nodev1.TaskState_TASK_STATE_UNSPECIFIED, 0, fmt.Errorf("reading its process: %w", err)
	}
	st, err := task.Status(ctx)
	if err != nil {
		return nodev1.TaskState_TASK_STATE_UNSPECIFIED, 0, fmt.Errorf("reading its process: %w", err)
	}
	switch st.Status {
	case containerd.Running:
		return nodev1.TaskState_TASK_STATE_RUNNING, 0, nil
	case containerd.Stopped:
		return nodev1.TaskState_TASK_STATE_EXITED, st.ExitStatus, nil
	}
	return nodev1.TaskState_TASK_STATE_FAILED, 0, fmt.Errorf("its process is %s", st.Status)
}
