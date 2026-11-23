package k3s_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/moostackhq/moostack/internal/console"
	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/objects"
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

// These tests read a real Kubernetes API: a k3s server with no agent, in
// a container of the machine's Docker, which must be there. Nothing runs
// its pods, so each test sets their status itself.

const image = "rancher/k3s:v1.37.2-k3s1"

var (
	// admin is the cluster's administrator, which sets up what the tests
	// read.
	admin *kubernetes.Clientset
	// reader is how the backend connects: as an account that may only
	// list pods.
	reader *rest.Config
)

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	name := fmt.Sprintf("moostack-orchestrator-test-%d", os.Getpid())
	if out, err := exec.Command("docker", "run", "-d", "--rm", "--name", name, "--memory", "1g", "-p", "127.0.0.1::6443", image,
		"server", "--disable-agent", "--disable=traefik,servicelb,metrics-server,local-storage,coredns").CombinedOutput(); err != nil {
		return 0, fmt.Errorf("starting %s: %v\n%s", image, err, out)
	}
	defer exec.Command("docker", "rm", "-f", name).Run()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := connect(ctx, name); err != nil {
		logs, _ := exec.Command("docker", "logs", "--tail", "30", name).CombinedOutput()
		return 0, fmt.Errorf("%v\n%s", err, logs)
	}
	return m.Run(), nil
}

// connect sets admin and reader for the k3s server in the container
// name, once its API answers.
func connect(ctx context.Context, name string) error {
	port, err := exec.Command("docker", "port", name, "6443/tcp").Output()
	if err != nil {
		return fmt.Errorf("docker port: %w", err)
	}
	host := "https://" + strings.TrimSpace(strings.SplitN(string(port), "\n", 2)[0])

	var config *rest.Config
	err = retry(ctx, func() error {
		kubeconfig, err := exec.Command("docker", "exec", name, "cat", "/etc/rancher/k3s/k3s.yaml").Output()
		if err != nil {
			return fmt.Errorf("reading the kubeconfig: %w", err)
		}
		if config, err = clientcmd.RESTConfigFromKubeConfig(kubeconfig); err != nil {
			return err
		}
		config.Host = host
		config.QPS, config.Burst = 500, 500
		if admin, err = kubernetes.NewForConfig(config); err != nil {
			return err
		}
		_, err = admin.CoreV1().ServiceAccounts("kube-system").Get(ctx, "default", metav1.GetOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("the API at %s: %w", host, err)
	}

	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "kube-system"}}
	if _, err := admin.CoreV1().ServiceAccounts("kube-system").Create(ctx, account, metav1.CreateOptions{}); err != nil {
		return err
	}
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "reader"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"list"}}},
	}
	if _, err := admin.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{}); err != nil {
		return err
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "reader"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "reader"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "reader", Namespace: "kube-system"}},
	}
	if _, err := admin.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{}); err != nil {
		return err
	}
	token, err := admin.CoreV1().ServiceAccounts("kube-system").CreateToken(ctx, "reader", &authenticationv1.TokenRequest{}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	reader = &rest.Config{Host: host, BearerToken: token.Status.Token, TLSClientConfig: rest.TLSClientConfig{CAData: config.CAData}}
	return nil
}

func retry(ctx context.Context, f func() error) error {
	for {
		err := f()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func backend(t *testing.T) *k3s.Backend {
	t.Helper()
	b, err := k3s.New(reader)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// container is a container of a pod the tests make, and the state they
// give it.
type container struct {
	name, image string
	state       state
}

type state int

const (
	waiting state = iota
	running
	terminated
)

func status(containers []container) []corev1.ContainerStatus {
	var out []corev1.ContainerStatus
	for _, c := range containers {
		s := corev1.ContainerStatus{Name: c.name, Image: "resolved.test/" + c.name, ImageID: "resolved.test/" + c.name + "@sha256:0000"}
		switch c.state {
		case running:
			s.State.Running = &corev1.ContainerStateRunning{StartedAt: metav1.Now()}
		case waiting:
			s.State.Waiting = &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}
		case terminated:
			s.State.Terminated = &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}
		}
		out = append(out, s)
	}
	return out
}

func createPod(t *testing.T, pod *corev1.Pod, st corev1.PodStatus) {
	t.Helper()
	if err := addPod(pod, st); err != nil {
		t.Fatal(err)
	}
}

// addPod creates pod and gives it the status st.
func addPod(pod *corev1.Pod, st corev1.PodStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var created *corev1.Pod
	err := retry(ctx, func() (err error) {
		created, err = admin.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		return err
	})
	if err != nil {
		return err
	}
	created.Status = st
	_, err = admin.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, created, metav1.UpdateOptions{})
	return err
}

// unique is name with a random suffix.
func unique(name string) string {
	return name + "-" + strings.ToLower(rand.Text()[:8])
}

func createNamespace(t *testing.T, name string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if _, err := admin.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
}

// pod creates the pod name in namespace, which it creates too, with
// containers in their states. app, if not empty, is the app the pod is
// labelled as of.
func pod(t *testing.T, namespace, name, app string, containers ...container) {
	t.Helper()
	createNamespace(t, namespace)
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if app != "" {
		p.Labels = map[string]string{objects.AppLabel: app}
	}
	for _, c := range containers {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: c.name, Image: c.image})
	}
	createPod(t, p, corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: status(containers)})
}

// runningOf is what the backend lists as running, cut to the replicas
// whose names start with prefix.
func runningOf(t *testing.T, prefix string) []model.Running {
	t.Helper()
	all, err := backend(t).Running(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var own []model.Running
	for _, r := range all {
		if strings.HasPrefix(r.Replica, prefix) {
			own = append(own, r)
		}
	}
	return own
}

func TestARunningContainerIsListedWithItsAppReplicaNameAndImage(t *testing.T) {
	app := unique("shop")
	pod(t, app, app+"-web-1", app, container{name: "web", image: "example.test/web:1", state: running})
	want := []model.Running{{App: app, Replica: app + "-web-1", Container: "web", Image: "example.test/web:1"}}
	if got := runningOf(t, app); !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
}

func TestAContainerOfAPodWithoutAnAppsLabelIsTheClustersOwn(t *testing.T) {
	id := unique("own")
	pod(t, id, id+"-dns", "", container{name: "dns", image: "example.test/dns:1", state: running})
	// A namespace's label doesn't make its pods an app's.
	labelled := unique("labelled")
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: labelled, Labels: map[string]string{objects.AppLabel: labelled}}}
	if _, err := admin.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod(t, labelled, id+"-job", "", container{name: "job", image: "example.test/job:1", state: running})
	want := []model.Running{
		{Replica: id + "-dns", Container: "dns", Image: "example.test/dns:1"},
		{Replica: id + "-job", Container: "job", Image: "example.test/job:1"},
	}
	if got := runningOf(t, id); !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
}

func TestAPodLabelledWithNoAppsNameIsTheClustersOwn(t *testing.T) {
	id := unique("empty")
	createNamespace(t, id)
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: id, Labels: map[string]string{objects.AppLabel: ""}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "example.test/web:1"}}},
	}
	createPod(t, p, corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: status([]container{{name: "web", state: running}})})
	want := []model.Running{{Replica: id, Container: "web", Image: "example.test/web:1"}}
	if got := runningOf(t, id); !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
}

func TestARunningContainerThePodDoesntAskForIsListedWithoutAnImage(t *testing.T) {
	id := unique("stray")
	createNamespace(t, id)
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: id, Namespace: id},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "example.test/web:1"}}},
	}
	createPod(t, p, corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: status([]container{
		{name: "web", state: running},
		{name: "other", state: running},
	})})
	want := []model.Running{
		{Replica: id, Container: "other"},
		{Replica: id, Container: "web", Image: "example.test/web:1"},
	}
	if got := runningOf(t, id); !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
}

func TestTheBackendTakesAnAppsOwnManifestsOrChart(t *testing.T) {
	if got := backend(t).Can(); got != (orchestrator.Capabilities{KubernetesObjects: true}) {
		t.Errorf("Can: %+v, want an app's own manifests or chart", got)
	}
}

func TestWaitingAndTerminatedContainersAreLeftOut(t *testing.T) {
	id := unique("states")
	pod(t, id, id+"-mixed", id,
		container{name: "up", image: "example.test/up:1", state: running},
		container{name: "pulling", image: "example.test/pulling:1", state: waiting},
		container{name: "done", image: "example.test/done:1", state: terminated},
	)
	pod(t, id, id+"-pending", id, container{name: "pulling", image: "example.test/pulling:1", state: waiting})
	pod(t, id+"-idle", id+"-finished", "", container{name: "job", image: "example.test/job:1", state: terminated})
	want := []model.Running{{App: id, Replica: id + "-mixed", Container: "up", Image: "example.test/up:1"}}
	if got := runningOf(t, id); !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
}

func TestContainersAreOrderedByAppThenReplicaThenContainer(t *testing.T) {
	id := unique("order")
	one := container{name: "c", image: "example.test/c:1", state: running}
	// The namespaces sort the other way round than the apps.
	pod(t, id+"-ns-a", id+"-a", id+"-z", one)
	pod(t, id+"-ns-b", id+"-b", id+"-m",
		container{name: "z", image: "example.test/z:1", state: running},
		container{name: "a", image: "example.test/a:1", state: running},
	)
	pod(t, id+"-ns-b", id+"-a2", id+"-m", one)
	pod(t, id+"-ns-z", id+"-z", "", one)
	pod(t, id+"-ns-c", id+"-c", id+"-a", one)
	want := []model.Running{
		{App: "", Replica: id + "-z", Container: "c", Image: "example.test/c:1"},
		{App: id + "-a", Replica: id + "-c", Container: "c", Image: "example.test/c:1"},
		{App: id + "-m", Replica: id + "-a2", Container: "c", Image: "example.test/c:1"},
		{App: id + "-m", Replica: id + "-b", Container: "a", Image: "example.test/a:1"},
		{App: id + "-m", Replica: id + "-b", Container: "z", Image: "example.test/z:1"},
		{App: id + "-z", Replica: id + "-a", Container: "c", Image: "example.test/c:1"},
	}
	if got := runningOf(t, id); !slices.Equal(got, want) {
		t.Errorf("Running:\n got %+v\nwant %+v", got, want)
	}
}

func TestRunningInitAndEphemeralContainersAreListed(t *testing.T) {
	ctx := context.Background()
	ns := unique("sidecars")
	createNamespace(t, ns)
	always := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: ns, Namespace: ns, Labels: map[string]string{objects.AppLabel: ns}},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{Name: "migrate", Image: "example.test/migrate:1"},
				{Name: "proxy", Image: "example.test/proxy:1", RestartPolicy: &always},
			},
			Containers: []corev1.Container{{Name: "app", Image: "example.test/app:1"}},
		},
	}
	createPod(t, pod, corev1.PodStatus{
		Phase: corev1.PodRunning,
		InitContainerStatuses: status([]container{
			{name: "migrate", state: terminated},
			{name: "proxy", state: running},
		}),
		ContainerStatuses: status([]container{{name: "app", state: running}}),
	})
	pods := admin.CoreV1().Pods(ns)
	created, err := pods.Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created.Spec.EphemeralContainers = []corev1.EphemeralContainer{{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "example.test/debug:1"},
	}}
	created, err = pods.UpdateEphemeralContainers(ctx, ns, created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created.Status.EphemeralContainerStatuses = status([]container{{name: "debug", state: running}})
	if _, err := pods.UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	want := []model.Running{
		{App: ns, Replica: ns, Container: "app", Image: "example.test/app:1"},
		{App: ns, Replica: ns, Container: "debug", Image: "example.test/debug:1"},
		{App: ns, Replica: ns, Container: "proxy", Image: "example.test/proxy:1"},
	}
	if got := runningOf(t, ns); !slices.Equal(got, want) {
		t.Errorf("Running in %s:\n got %+v\nwant %+v", ns, got, want)
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPodsBeyondOnePageAreListed(t *testing.T) {
	ns := unique("pages")
	createNamespace(t, ns)
	const count = 501
	want := make([]model.Running, count)
	var wg sync.WaitGroup
	slots := make(chan struct{}, 16)
	for i := range count {
		name := fmt.Sprintf("%s-%03d", ns, i)
		want[i] = model.Running{Replica: name, Container: "app", Image: "example.test/app:1"}
		wg.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example.test/app:1"}}},
			}
			st := corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: status([]container{{name: "app", state: running}})}
			if err := addPod(pod, st); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	var limits []string
	config := rest.CopyConfig(reader)
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(r *http.Request) (*http.Response, error) {
			limits = append(limits, r.URL.Query().Get("limit"))
			return rt.RoundTrip(r)
		})
	})
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	all, err := b.Running(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(limits) < 2 || slices.ContainsFunc(limits, func(limit string) bool { return limit != "500" }) {
		t.Errorf("the API was asked with the limits %q, want 2 calls or more, each for 500 pods", limits)
	}
	var got []model.Running
	for _, r := range all {
		if strings.HasPrefix(r.Replica, ns) {
			got = append(got, r)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("Running listed %d of the %d containers in %s", len(got), count, ns)
	}
}

func TestAMethodTheBackendDoesntHaveYetSaysSo(t *testing.T) {
	ctx := context.Background()
	b := backend(t)
	_, apps := b.Apps(ctx)
	_, nodes := b.Nodes(ctx)
	_, preflight := b.Preflight(ctx, nil, orchestrator.NodeRequest{})
	_, token := b.ConsoleToken(ctx)
	for method, err := range map[string]error{
		"Check":        b.Check(model.App{Name: "shop"}),
		"Deploy":       b.Deploy(ctx, model.App{Name: "shop"}, func(orchestrator.Step) {}),
		"Certificate":  b.Certificate(ctx, "shop", "web"),
		"Remove":       b.Remove(ctx, "shop"),
		"Logs":         b.Logs(ctx, "shop", orchestrator.LogOptions{}, io.Discard),
		"Apps":         apps,
		"Preflight":    preflight,
		"AddNode":      b.AddNode(ctx, nil, model.Node{}),
		"RemoveNode":   b.RemoveNode(ctx, "node-1", nil, orchestrator.RemoveOptions{}),
		"Uninstall":    b.Uninstall(ctx, nil, model.Node{}),
		"Nodes":        nodes,
		"RunConsole":   b.RunConsole(ctx, "example.test/console:1", nil),
		"ConsoleToken": token,
	} {
		if want := method + ": the k3s backend can't do this yet"; err == nil || err.Error() != want {
			t.Errorf("%s: %v, want %q", method, err, want)
		}
	}
}

func TestARefusedTokenIsAnError(t *testing.T) {
	config := rest.CopyConfig(reader)
	config.BearerToken = "not-a-token"
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Running(context.Background()); err == nil || !strings.Contains(err.Error(), "listing pods") {
		t.Errorf("Running with a refused token: %v, want an error from listing pods", err)
	}
}

func TestAnAccountWithoutTheRightToListPodsIsAnError(t *testing.T) {
	ctx := context.Background()
	name := unique("no-rights")
	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kube-system"}}
	if _, err := admin.CoreV1().ServiceAccounts("kube-system").Create(ctx, account, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	token, err := admin.CoreV1().ServiceAccounts("kube-system").CreateToken(ctx, name, &authenticationv1.TokenRequest{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	config := rest.CopyConfig(reader)
	config.BearerToken = token.Status.Token
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Running(ctx); !apierrors.IsForbidden(err) {
		t.Errorf("Running without the right to list pods: %v, want a forbidden error", err)
	}
}

func TestTheConsolesPageShowsTheCluster(t *testing.T) {
	app := unique("page")
	pod(t, app, app+"-web-1", app, container{name: "web", image: "example.test/web:1", state: running})
	pod(t, app+"-system", app+"-dns-1", "", container{name: "dns", image: "example.test/dns:1", state: running})
	o := backend(t)
	dir, err := os.MkdirTemp("", "console")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "console.sock")
	tokens, err := console.ListenTokenSocket(socket)
	if err != nil {
		t.Fatal(err)
	}
	web, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- console.New(o).Serve(ctx, web, tokens) }()
	defer func() {
		cancel()
		<-done
	}()
	base := "http://" + web.Addr().String()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	token, err := console.RequestToken(ctx, socket)
	if err != nil {
		t.Fatal(err)
	}
	login, err := client.PostForm(base+"/console/login", url.Values{"token": {token}})
	if err != nil {
		t.Fatal(err)
	}
	login.Body.Close()
	if len(login.Cookies()) != 1 {
		t.Fatalf("logging in: status %d, no session", login.StatusCode)
	}
	req, err := http.NewRequest(http.MethodGet, base+"/console/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(login.Cookies()[0])
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200:\n%s", res.StatusCode, body)
	}
	for _, want := range []string{
		"<h2>" + app + "</h2>", "<tr><td>" + app + "-web-1</td><td>web</td><td>example.test/web:1</td></tr>",
		"<h2>The cluster's own</h2>", "<tr><td>" + app + "-dns-1</td><td>dns</td><td>example.test/dns:1</td></tr>",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("the page lacks %s:\n%s", want, body)
		}
	}
}
