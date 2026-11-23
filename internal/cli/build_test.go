package cli

import (
	"archive/tar"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/moostackhq/moostack/internal/k3s"
)

// ociArchive writes an OCI image layout archive of a random image to
// path, as buildx writes one.
func ociArchive(t *testing.T, path string) {
	t.Helper()
	img, err := random.Image(256, 1)
	if err != nil {
		t.Fatal(err)
	}
	lay := filepath.Join(t.TempDir(), "layout")
	p, err := layout.Write(lay, empty.Index)
	if err == nil {
		err = p.AppendImage(img)
	}
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	err = filepath.WalkDir(lay, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(lay, p)
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err = tw.Write(data)
		return err
	})
	if err == nil {
		err = tw.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
}

// fakeDocker is a docker command on the containerd image store that
// builds by extracting an archive of its own for the platform asked, and
// logs the platforms it built for in dir's builds.
func fakeDocker(t *testing.T) (path, dir string) {
	t.Helper()
	dir = t.TempDir()
	for _, arch := range []string{"amd64", "arm64"} {
		ociArchive(t, filepath.Join(dir, arch+".tar"))
	}
	script := `#!/bin/sh
fake='` + dir + `'
case "$1" in
info) echo '29.7.2|x86_64|io.containerd.snapshotter.v1' ;;
context) echo default ;;
buildx)
	if [ "$2" = version ]; then echo "github.com/docker/buildx v0.36.1"; exit 0; fi
	if [ "$2" = inspect ]; then echo "Driver: docker"; exit 0; fi
	while [ $# -gt 0 ]; do
		case "$1" in
		--platform) arch=${2#linux/} ;;
		--output) dest=${2#type=oci,dest=}; dest=${dest%%,*} ;;
		esac
		shift
	done
	echo "$arch" >> "$fake/builds"
	mkdir "$dest" && tar -xf "$fake/$arch.tar" -C "$dest"
	;;
esac
`
	path = filepath.Join(dir, "docker")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path, dir
}

func TestDeployRunsTheBuildByDigestOnEveryArchitecture(t *testing.T) {
	c, home := deployingProd(t)
	// A node Kubernetes has that the cluster directory doesn't yet.
	c.nodes["node9"] = fakeKubeNode{ip: "10.0.0.9", arch: "arm64"}
	docker, dir := fakeDocker(t)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers: {web: {build: {dockerfile: .}, port: 80}}\n"), 0o644)
	t.Chdir(app)

	var out strings.Builder
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr, env.Docker = &out, &out, docker
	if err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "deploy"}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}

	builds, _ := os.ReadFile(filepath.Join(dir, "builds"))
	if got := strings.Fields(string(builds)); !slices.Equal(got, []string{"amd64", "arm64"}) {
		t.Errorf("built for %v", got)
	}
	repo, _ := name.NewRepository(c.registry+"/web/web", name.Insecure)
	tags, err := ggcrremote.List(repo)
	tags = slices.DeleteFunc(tags, func(tag string) bool { return tag == k3s.LiveTag })
	if err != nil || len(tags) != 1 {
		t.Fatalf("zot has %v, %v", tags, err)
	}
	want := "moostack.local/web/web@sha256:" + tags[0]
	if !strings.Contains(out.String(), "Image: "+want) {
		t.Errorf("output:\n%s", out.String())
	}
	if !slices.ContainsFunc(c.manifests, func(m string) bool { return strings.Contains(m, "image: "+want) }) {
		t.Errorf("no object runs %s:\n%s", want, strings.Join(c.manifests, "---\n"))
	}
}

// imageOf is the image that the Deployment called name runs in the
// manifests applied to c.
func imageOf(t *testing.T, c *fakeCluster, name string) string {
	t.Helper()
	for _, m := range c.manifests {
		for _, doc := range strings.Split(m, "---\n") {
			var d struct {
				Kind     string
				Metadata struct{ Name string }
				Spec     struct {
					Template struct {
						Spec struct{ Containers []struct{ Image string } }
					}
				}
			}
			if err := yaml.Unmarshal([]byte(doc), &d); err != nil || d.Kind != "Deployment" || d.Metadata.Name != name {
				continue
			}
			return d.Spec.Template.Spec.Containers[0].Image
		}
	}
	t.Fatalf("no Deployment %s was applied", name)
	return ""
}

func TestDeployBuildsEachBuildOfItsContainersOnce(t *testing.T) {
	c, home := deployingProd(t)
	docker, dir := fakeDocker(t)
	app := t.TempDir()
	os.MkdirAll(filepath.Join(app, "api"), 0o755)
	os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "api", "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte(`name: shop
cluster: prod
containers:
  api: {build: {dockerfile: api}, port: 3000, host: api.example.com}
  cache: {image: "valkey/valkey:8", port: 6379}
  jobs: {build: {dockerfile: .}, command: [bin/jobs]}
  web: {build: {dockerfile: .}, port: 8080, host: shop.example.com}
`), 0o644)
	t.Chdir(app)
	c.unissued = "api-tls"

	out := deployWith(t, c, home, docker)
	builds, _ := os.ReadFile(filepath.Join(dir, "builds"))
	if got := strings.Fields(string(builds)); !slices.Equal(got, []string{"amd64", "amd64"}) {
		t.Errorf("built for %v, want two builds", got)
	}
	ref := func(container string) string {
		repo, _ := name.NewRepository(c.registry+"/shop/"+container, name.Insecure)
		tags, err := ggcrremote.List(repo)
		tags = slices.DeleteFunc(tags, func(tag string) bool { return tag == k3s.LiveTag })
		if err != nil || len(tags) != 1 {
			t.Fatalf("the registry has %v of %s, %v", tags, container, err)
		}
		return "moostack.local/shop/" + container + "@sha256:" + tags[0]
	}
	api, jobs := ref("api"), ref("jobs")
	for _, container := range []string{"cache", "web"} {
		repo, _ := name.NewRepository(c.registry+"/shop/"+container, name.Insecure)
		if tags, err := ggcrremote.List(repo); err == nil {
			t.Errorf("the registry has %v of %s", tags, container)
		}
	}
	for container, want := range map[string]string{"api": api, "cache": "valkey/valkey:8", "jobs": jobs, "web": jobs} {
		if got := imageOf(t, c, container); got != want {
			t.Errorf("%s runs %s, not %s", container, got, want)
		}
	}
	for _, line := range []string{
		"Building api for linux/amd64...\n", "Pushing api...\n", "Building jobs for linux/amd64...\n", "Pushing jobs...\n",
		"Deployed shop to prod\nImage: " + api + "\nImage: " + jobs + "\nWarning: api.example.com has no certificate yet: exit 1: the challenge is pending\nURL: https://api.example.com\nURL: https://shop.example.com\n",
	} {
		if !strings.Contains(out, line) {
			t.Errorf("no %q in:\n%s", line, out)
		}
	}
	if strings.Contains(out, "Building web") || strings.Contains(out, "Building cache") {
		t.Errorf("built web or cache:\n%s", out)
	}
	for _, cert := range []string{"certificate/api-tls", "certificate/web-tls"} {
		if !c.ran("root@h1", cert) {
			t.Errorf("didn't wait for %s", cert)
		}
	}
	if c.ran("root@h1", "certificate/jobs-tls") || c.ran("root@h1", "certificate/cache-tls") || c.ran("root@h1", "certificate/shop-tls") {
		t.Error("waited for the certificate of a container without a host")
	}
}

func TestDeployOfImagesOnlyBuildsNothing(t *testing.T) {
	c, home := deployingProd(t)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("name: shop\ncluster: prod\ncontainers: {cache: {image: \"valkey/valkey:8\", port: 6379}}\n"), 0o644)
	t.Chdir(app)
	env := fakeEnvFor(c, home, nil)
	var out strings.Builder
	env.Stdout, env.Stderr, env.Docker = &out, &out, filepath.Join(t.TempDir(), "no-docker")
	if err := New(env).Run(t.Context(), []string{"mooctl", "deploy"}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if got := imageOf(t, c, "cache"); got != "valkey/valkey:8" {
		t.Errorf("cache runs %s", got)
	}
	if strings.Contains(out.String(), "Image:") || strings.Contains(out.String(), "URL:") {
		t.Errorf("output:\n%s", out.String())
	}
	if c.ran("root@h1", "nodeInfo.architecture") {
		t.Error("asked for the nodes' architectures with nothing to build")
	}
}

func TestDeployRemoveDeletesTheImagesOfEveryContainer(t *testing.T) {
	c, home := deployingProd(t)
	docker, _ := fakeDocker(t)
	for _, name := range []string{"shop", "shopping"} {
		app := t.TempDir()
		os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
		os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("name: "+name+"\ncluster: prod\ncontainers: {web: {build: {dockerfile: .}, port: 80}}\n"), 0o644)
		t.Chdir(app)
		deployWith(t, c, home, docker)
	}
	mustRun(t, c, home, "deploy", "--remove", "shop")
	left := func(repo string) int {
		r, _ := name.NewRepository(c.registry+"/"+repo, name.Insecure)
		tags, _ := ggcrremote.List(r)
		n := 0
		for _, tag := range tags {
			if _, err := ggcrremote.Head(r.Tag(tag)); err == nil {
				n++
			}
		}
		return n
	}
	if n := left("shop/web"); n != 0 {
		t.Errorf("%d images of shop/web are left", n)
	}
	if n := left("shopping/web"); n != 2 {
		t.Errorf("%d tags of shopping/web are left, want its image and the mark", n)
	}
}

func TestARailpackWithoutAPinnedFrontendIsRefused(t *testing.T) {
	c, home := deployingProd(t)
	docker, _ := fakeDocker(t)
	railpack := filepath.Join(t.TempDir(), "railpack")
	os.WriteFile(railpack, []byte("#!/bin/sh\necho 'railpack 0.44.0'\n"), 0o755)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers: {web: {build: {railpack: .}, port: 80}}\n"), 0o644)
	t.Chdir(app)
	var out strings.Builder
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr, env.Docker, env.Railpack = &out, &out, docker, railpack
	err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "deploy"})
	if err == nil || !strings.Contains(err.Error(), "railpack is 0.44.0; mooctl builds with Railpack 0.43.0") {
		t.Errorf("deploy = %v\n%s", err, out.String())
	}
}

// deployWith runs mooctl deploy with args in the current directory with
// the docker, and returns what it printed.
func deployWith(t *testing.T, c *fakeCluster, home, docker string, args ...string) string {
	t.Helper()
	var out strings.Builder
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr, env.Docker = &out, &out, docker
	if err := New(env).Run(t.Context(), append([]string{"mooctl", "-c", "prod", "deploy"}, args...)); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	return out.String()
}

// liveOf is the digest of the image the registry of c keeps as repo's
// running one, empty if it keeps none.
func liveOf(t *testing.T, c *fakeCluster, repo string) string {
	t.Helper()
	ref, err := name.NewTag(c.registry+"/"+repo+":"+k3s.LiveTag, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := ggcrremote.Head(ref)
	if err != nil {
		return ""
	}
	return desc.Digest.String()
}

func TestTheRegistryKeepsTheImageThatRuns(t *testing.T) {
	c, home := deployingProd(t)
	docker, dir := fakeDocker(t)
	app := t.TempDir()
	os.WriteFile(filepath.Join(app, "Dockerfile"), []byte("FROM scratch\n"), 0o644)
	os.WriteFile(filepath.Join(app, "deploy.yaml"), []byte("cluster: prod\nname: web\ncontainers: {web: {build: {dockerfile: .}, port: 80}}\n"), 0o644)
	t.Chdir(app)
	deployWith(t, c, home, docker)
	_, running, _ := strings.Cut(imageOf(t, c, "web"), "@")
	if got := liveOf(t, c, "web/web"); got == "" || got != running {
		t.Fatalf("the registry keeps %q, and %s runs", got, running)
	}

	// A deploy whose app doesn't become ready pushes its image and leaves
	// the mark where it was.
	ociArchive(t, filepath.Join(dir, "amd64.tar"))
	c.rolloutFails = true
	var out strings.Builder
	env := fakeEnvFor(c, home, nil)
	env.Stdout, env.Stderr, env.Docker = &out, &out, docker
	if err := New(env).Run(t.Context(), []string{"mooctl", "-c", "prod", "deploy"}); err == nil {
		t.Fatalf("a deploy that doesn't become ready passed:\n%s", out.String())
	}
	repo, _ := name.NewRepository(c.registry+"/web/web", name.Insecure)
	if tags, err := ggcrremote.List(repo); err != nil || len(tags) != 3 {
		t.Fatalf("the registry has %v, %v, want both images and the mark", tags, err)
	}
	if got := liveOf(t, c, "web/web"); got != running {
		t.Errorf("after a failed deploy the registry keeps %q, and %s runs", got, running)
	}
}
