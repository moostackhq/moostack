package k3s_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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
	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
	"github.com/moostackhq/moostack/internal/orchestrator/orchestratortest"
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

func status(containers []orchestratortest.PodContainer) []corev1.ContainerStatus {
	var out []corev1.ContainerStatus
	for _, c := range containers {
		s := corev1.ContainerStatus{Name: c.Name, Image: "resolved.test/" + c.Name, ImageID: "resolved.test/" + c.Name + "@sha256:0000"}
		switch c.Status {
		case orchestratortest.Running:
			s.State.Running = &corev1.ContainerStateRunning{StartedAt: metav1.Now()}
		case orchestratortest.Waiting:
			s.State.Waiting = &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}
		case orchestratortest.Terminated:
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

// unique is name with a suffix of this call's own, so that a test run
// again finds nothing a run before it left.
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

func setup(t *testing.T, pods []orchestratortest.Pod) orchestrator.Orchestrator {
	t.Helper()
	for _, p := range pods {
		createNamespace(t, p.Namespace)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace}}
		for _, c := range p.Containers {
			pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: c.Name, Image: c.Image})
		}
		createPod(t, pod, corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: status(p.Containers)})
	}
	return backend(t)
}

func TestTheBackendKeepsThePortsPromises(t *testing.T) {
	orchestratortest.Contract(t, setup)
}

func TestRunningInitAndEphemeralContainersAreListed(t *testing.T) {
	ctx := context.Background()
	ns := unique("sidecars")
	createNamespace(t, ns)
	always := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns},
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
		InitContainerStatuses: status([]orchestratortest.PodContainer{
			{Name: "migrate", Status: orchestratortest.Terminated},
			{Name: "proxy", Status: orchestratortest.Running},
		}),
		ContainerStatuses: status([]orchestratortest.PodContainer{{Name: "app", Status: orchestratortest.Running}}),
	})
	pods := admin.CoreV1().Pods(ns)
	created, err := pods.Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created.Spec.EphemeralContainers = []corev1.EphemeralContainer{{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "example.test/debug:1"},
	}}
	created, err = pods.UpdateEphemeralContainers(ctx, "web", created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created.Status.EphemeralContainerStatuses = status([]orchestratortest.PodContainer{{Name: "debug", Status: orchestratortest.Running}})
	if _, err := pods.UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	all, err := backend(t).Running(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []orchestrator.Container
	for _, c := range all {
		if c.Namespace == ns {
			got = append(got, c)
		}
	}
	want := []orchestrator.Container{
		{Namespace: ns, Pod: "web", Name: "app", Image: "example.test/app:1"},
		{Namespace: ns, Pod: "web", Name: "debug", Image: "example.test/debug:1"},
		{Namespace: ns, Pod: "web", Name: "proxy", Image: "example.test/proxy:1"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("Running in %s:\n got %+v\nwant %+v", ns, got, want)
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPodsBeyondOnePageAreListed(t *testing.T) {
	ns := unique("pages")
	createNamespace(t, ns)
	const count = 501
	want := make([]orchestrator.Container, count)
	var wg sync.WaitGroup
	slots := make(chan struct{}, 16)
	for i := range count {
		name := fmt.Sprintf("pod-%03d", i)
		want[i] = orchestrator.Container{Namespace: ns, Pod: name, Name: "app", Image: "example.test/app:1"}
		wg.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example.test/app:1"}}},
			}
			st := corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: status([]orchestratortest.PodContainer{
				{Name: "app", Status: orchestratortest.Running},
			})}
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
	var got []orchestrator.Container
	for _, c := range all {
		if c.Namespace == ns {
			got = append(got, c)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("Running listed %d of the %d containers in %s", len(got), count, ns)
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
	ns := unique("page")
	o := setup(t, []orchestratortest.Pod{{Namespace: ns, Name: "web-1", Containers: []orchestratortest.PodContainer{
		{Name: "web", Image: "example.test/web:1", Status: orchestratortest.Running},
	}}})
	srv := httptest.NewServer(console.New(o))
	defer srv.Close()
	res, err := http.Get(srv.URL + "/console/")
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
	for _, want := range []string{"<h2>" + ns + "</h2>", "<tr><td>web-1</td><td>web</td><td>example.test/web:1</td></tr>"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("the page lacks %s:\n%s", want, body)
		}
	}
}
