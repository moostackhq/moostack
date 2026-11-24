package k3s_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/moostackhq/moostack/internal/orchestrator"
	"github.com/moostackhq/moostack/internal/orchestrator/k3s"
)

// The test cluster has no kubelet to read a container's log from, so
// the tests answer the requests for one themselves, and the API
// answers every other.

// logRequest matches the path of a request for a pod's log.
var logRequest = regexp.MustCompile(`^/api/v1/namespaces/([^/]+)/pods/([^/]+)/log$`)

// kubeletLogs are the logs of containers, by "pod/container", and the
// requests made for them, as "pod/container tail=2 follow=false", with
// no tail for a request of the whole log. A request for the log of a
// pod its namespace doesn't have is answered as the API does, with 404.
type kubeletLogs struct {
	mu       sync.Mutex
	logs     map[string]containerLog
	requests []string
	// before, if set, runs before each request for a log is answered.
	before func()
}

// containerLog is what a request for a container's log is answered
// with: text, of which a request gets its last lines, then what then
// gives, then what follows gives, if the request follows; or the
// refusal, if set.
type containerLog struct {
	text          string
	then, follows io.Reader
	refusal       string
}

// answer answers r if it asks for a container's log.
func (k *kubeletLogs) answer(r *http.Request) (*http.Response, bool) {
	m := logRequest.FindStringSubmatch(r.URL.Path)
	if m == nil {
		return nil, false
	}
	q := r.URL.Query()
	key := m[2] + "/" + q.Get("container")
	request := key
	if q.Has("tailLines") {
		request += " tail=" + q.Get("tailLines")
	}
	request += " follow=" + strconv.FormatBool(q.Get("follow") == "true")
	k.mu.Lock()
	k.requests = append(k.requests, request)
	l, ok := k.logs[key]
	before := k.before
	k.mu.Unlock()
	if before != nil {
		before()
	}
	respond := func(code int, contentType string, body io.Reader) (*http.Response, bool) {
		return &http.Response{
			StatusCode: code, Status: http.StatusText(code), Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(body), Request: r,
		}, true
	}
	if _, err := admin.CoreV1().Pods(m[1]).Get(context.Background(), m[2], metav1.GetOptions{}); apierrors.IsNotFound(err) {
		status, _ := json.Marshal(metav1.Status{
			TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status:   metav1.StatusFailure, Message: `pods "` + m[2] + `" not found`, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound,
		})
		return respond(http.StatusNotFound, "application/json", bytes.NewReader(status))
	}
	if !ok || l.refusal != "" {
		message := l.refusal
		if !ok {
			message = "the test has no log of " + key
		}
		status, _ := json.Marshal(metav1.Status{
			TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status:   metav1.StatusFailure, Message: message, Reason: metav1.StatusReasonBadRequest, Code: http.StatusBadRequest,
		})
		return respond(http.StatusBadRequest, "application/json", bytes.NewReader(status))
	}
	text := l.text
	if q.Has("tailLines") {
		n, _ := strconv.Atoi(q.Get("tailLines"))
		lines := strings.SplitAfter(text, "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
		text = strings.Join(lines[max(0, len(lines)-n):], "")
	}
	var log io.Reader = strings.NewReader(text)
	if l.then != nil {
		log = io.MultiReader(log, l.then)
	}
	if q.Get("follow") == "true" && l.follows != nil {
		log = io.MultiReader(log, l.follows)
	}
	// The answer ends with the request's context, as one of the API does.
	body, to := io.Pipe()
	go func() {
		_, err := io.Copy(to, log)
		to.CloseWithError(err)
	}()
	context.AfterFunc(r.Context(), func() { body.CloseWithError(context.Cause(r.Context())) })
	return respond(http.StatusOK, "text/plain", body)
}

func (k *kubeletLogs) asked() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	asked := slices.Clone(k.requests)
	slices.Sort(asked)
	return asked
}

// logging is a backend whose requests for a container's log logs
// answers.
func logging(t *testing.T, logs map[string]containerLog) (*k3s.Backend, *kubeletLogs) {
	t.Helper()
	k := &kubeletLogs{logs: logs}
	config := rest.CopyConfig(adminConfig)
	config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripper(func(r *http.Request) (*http.Response, error) {
			if res, ok := k.answer(r); ok {
				// A request whose context has ended fails, as in the
				// transport.
				if r.Context().Err() != nil {
					res.Body.Close()
					return nil, r.Context().Err()
				}
				return res, nil
			}
			return rt.RoundTrip(r)
		})
	})
	b, err := k3s.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return b, k
}

// appOfPods makes an app's namespace called name, removed when the test
// ends, with a pod for each name of pods, each of the containers named.
func appOfPods(t *testing.T, name string, pods []string, containers ...string) {
	t.Helper()
	cleared(t, name)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{appLabel: name}}}
	if _, err := admin.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, p := range pods {
		var cs []container
		for _, c := range containers {
			cs = append(cs, container{name: c, image: "example.test/" + c + ":1", state: running})
		}
		pod(t, name, p, name, cs...)
	}
}

func logsOf(t *testing.T, b *k3s.Backend, app string, o orchestrator.LogOptions) string {
	t.Helper()
	var w bytes.Buffer
	if err := b.Logs(within(t, time.Minute), app, o, &w); err != nil {
		t.Fatalf("Logs: %v", err)
	}
	return w.String()
}

func TestLogsWritesTheLastLinesOfEachContainerAfterItsReplicaAndContainer(t *testing.T) {
	name := unique("logs")
	appOfPods(t, name, []string{"web-b", "web-a"}, "web", "proxy")
	b, kubelet := logging(t, map[string]containerLog{
		"web-a/web":   {text: "a1\na2\na3\n"},
		"web-a/proxy": {text: "p1\n"},
		"web-b/web":   {text: "b1\nb2\nno line end"},
		"web-b/proxy": {text: ""},
	})
	for _, c := range []struct {
		tail      int
		want      string
		requested string
	}{
		{2, "[pod/web-a/web] a2\n[pod/web-a/web] a3\n[pod/web-a/proxy] p1\n[pod/web-b/web] b2\n[pod/web-b/web] no line end\n", " tail=2"},
		{0, "", " tail=0"},
		{-1, "[pod/web-a/web] a1\n[pod/web-a/web] a2\n[pod/web-a/web] a3\n[pod/web-a/proxy] p1\n[pod/web-b/web] b1\n[pod/web-b/web] b2\n[pod/web-b/web] no line end\n", ""},
	} {
		kubelet.requests = nil
		if got := logsOf(t, b, name, orchestrator.LogOptions{Tail: c.tail}); got != c.want {
			t.Errorf("Logs with the tail %d wrote\n%s, want\n%s", c.tail, got, c.want)
		}
		var want []string
		for _, key := range []string{"web-a/proxy", "web-a/web", "web-b/proxy", "web-b/web"} {
			want = append(want, key+c.requested+" follow=false")
		}
		if got := kubelet.asked(); !slices.Equal(got, want) {
			t.Errorf("with the tail %d the logs asked for were %q, want %q", c.tail, got, want)
		}
	}
}

func TestLogsHasTheInitAndEphemeralContainersOfAReplica(t *testing.T) {
	ctx := context.Background()
	name := unique("sidelogs")
	appOfPods(t, name, nil)
	always := corev1.ContainerRestartPolicyAlways
	createPod(t, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: name},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{Name: "migrate", Image: "example.test/migrate:1"},
				{Name: "proxy", Image: "example.test/proxy:1", RestartPolicy: &always},
			},
			Containers: []corev1.Container{{Name: "web", Image: "example.test/web:1"}},
		},
	}, corev1.PodStatus{Phase: corev1.PodRunning})
	pods := admin.CoreV1().Pods(name)
	err := retry(within(t, time.Minute), func() error {
		created, err := pods.Get(ctx, "web-1", metav1.GetOptions{})
		if err != nil {
			return err
		}
		created.Spec.EphemeralContainers = []corev1.EphemeralContainer{{
			EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "example.test/debug:1"},
		}}
		_, err = pods.UpdateEphemeralContainers(ctx, "web-1", created, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := logging(t, map[string]containerLog{
		"web-1/migrate": {text: "migrated\n"},
		"web-1/proxy":   {text: "proxying\n"},
		"web-1/web":     {text: "serving\n"},
		"web-1/debug":   {text: "debugging\n"},
	})
	want := "[pod/web-1/migrate] migrated\n[pod/web-1/proxy] proxying\n[pod/web-1/web] serving\n[pod/web-1/debug] debugging\n"
	if got := logsOf(t, b, name, orchestrator.LogOptions{Tail: 10}); got != want {
		t.Errorf("Logs wrote\n%s, want\n%s", got, want)
	}
}

func TestLogsWritesWhyAContainersLogCantBeReadAndGoesOn(t *testing.T) {
	name := unique("refused")
	appOfPods(t, name, []string{"web-a", "web-b"}, "web")
	b, _ := logging(t, map[string]containerLog{
		"web-a/web": {refusal: `container "web" in pod "web-a" is waiting to start: ContainerCreating`},
		"web-b/web": {text: "b1\n"},
	})
	refused, read := "error: container \"web\" in pod \"web-a\" is waiting to start: ContainerCreating\n", "[pod/web-b/web] b1\n"
	if got := logsOf(t, b, name, orchestrator.LogOptions{Tail: 10}); got != refused+read {
		t.Errorf("Logs wrote\n%s, want\n%s", got, refused+read)
	}
	if got := logsOf(t, b, name, orchestrator.LogOptions{Tail: 10, Follow: true}); got != refused+read && got != read+refused {
		t.Errorf("Logs, following, wrote\n%s, want the two lines\n%s", got, refused+read)
	}
}

func TestLogsOfAnAppTheClusterDoesntHaveIsErrNoAppAndOfOneWithoutReplicasErrNoReplicas(t *testing.T) {
	b, kubelet := logging(t, nil)
	for _, name := range []string{unique("absent"), "kube-system"} {
		if err := b.Logs(within(t, time.Minute), name, orchestrator.LogOptions{Tail: 10}, io.Discard); !errors.Is(err, orchestrator.ErrNoApp) {
			t.Errorf("Logs of %s, which is no app: %v, want ErrNoApp", name, err)
		}
	}
	name := unique("idle")
	appOfPods(t, name, nil)
	for _, follow := range []bool{false, true} {
		if err := b.Logs(within(t, time.Minute), name, orchestrator.LogOptions{Tail: 10, Follow: follow}, io.Discard); !errors.Is(err, orchestrator.ErrNoReplicas) {
			t.Errorf("Logs of an app without replicas, following %t: %v, want ErrNoReplicas", follow, err)
		}
	}
	if asked := kubelet.asked(); len(asked) != 0 {
		t.Errorf("the logs asked for were %q, want none", asked)
	}
}

// lineWriter sends each write it gets.
type lineWriter chan string

func (w lineWriter) Write(p []byte) (int, error) {
	w <- string(p)
	return len(p), nil
}

// next is the next write to w, within ten seconds.
func (w lineWriter) next(t *testing.T) string {
	t.Helper()
	select {
	case line := <-w:
		return line
	case <-time.After(10 * time.Second):
		t.Fatal("Logs wrote nothing in ten seconds")
		return ""
	}
}

func TestLogsFollowedWritesEachLineAsItComesUntilTheReplicasEnd(t *testing.T) {
	name := unique("followed")
	appOfPods(t, name, []string{"web-a", "web-b"}, "web")
	a, toA := io.Pipe()
	bb, toB := io.Pipe()
	b, kubelet := logging(t, map[string]containerLog{
		"web-a/web": {text: "old a\nlast a\n", follows: a},
		"web-b/web": {text: "", follows: bb},
	})
	w := make(lineWriter, 16)
	done := make(chan error, 1)
	go func() {
		done <- b.Logs(within(t, time.Minute), name, orchestrator.LogOptions{Tail: 1, Follow: true}, w)
	}()
	if got, want := w.next(t), "[pod/web-a/web] last a\n"; got != want {
		t.Errorf("the first write was %q, want %q", got, want)
	}
	// A line comes whole, however it is sent.
	io.WriteString(toB, "half ")
	io.WriteString(toA, "new a\n")
	if got, want := w.next(t), "[pod/web-a/web] new a\n"; got != want {
		t.Errorf("the write after a line of web-a was %q, want %q", got, want)
	}
	io.WriteString(toB, "a line\n")
	if got, want := w.next(t), "[pod/web-b/web] half a line\n"; got != want {
		t.Errorf("the write after the line of web-b ended was %q, want %q", got, want)
	}
	toA.Close()
	blocked(t, done, 2*time.Second, "with a replica still followed")
	io.WriteString(toB, "bye")
	toB.Close()
	if got, want := w.next(t), "[pod/web-b/web] bye\n"; got != want {
		t.Errorf("the last write was %q, want %q", got, want)
	}
	if err := returned(t, done); err != nil {
		t.Errorf("Logs: %v", err)
	}
	if got, want := kubelet.asked(), []string{"web-a/web tail=1 follow=true", "web-b/web tail=1 follow=true"}; !slices.Equal(got, want) {
		t.Errorf("the logs asked for were %q, want %q", got, want)
	}
}

func TestLogsFollowedEndsWithItsContextsCause(t *testing.T) {
	name := unique("cutlogs")
	appOfPods(t, name, []string{"web-a"}, "web")
	r, to := io.Pipe()
	defer to.Close()
	b, _ := logging(t, map[string]containerLog{"web-a/web": {text: "a\n", follows: r}})
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	w := make(lineWriter, 16)
	done := make(chan error, 1)
	go func() { done <- b.Logs(ctx, name, orchestrator.LogOptions{Tail: 1, Follow: true}, w) }()
	w.next(t)
	cancel(cause)
	if err := returned(t, done); err == nil || err.Error() != cause.Error() {
		t.Errorf("Logs: %v, want the context's cause alone", err)
	}
	select {
	case line := <-w:
		t.Errorf("an interrupted Logs wrote %q", line)
	default:
	}
}

func TestLogsInterruptedBeforeALogAnswersFailsWithTheCauseAndWritesNothing(t *testing.T) {
	name := unique("cutearly")
	appOfPods(t, name, []string{"web-a", "web-b"}, "web")
	for _, follow := range []bool{false, true} {
		b, kubelet := logging(t, map[string]containerLog{"web-a/web": {text: "a\n"}, "web-b/web": {text: "b\n"}})
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("interrupted")
		kubelet.before = func() { cancel(cause) }
		var w bytes.Buffer
		err := b.Logs(ctx, name, orchestrator.LogOptions{Tail: 1, Follow: follow}, &w)
		if err == nil || err.Error() != cause.Error() {
			t.Errorf("Logs interrupted at its first request for a log, following %t: %v, want the cause alone", follow, err)
		}
		if w.Len() != 0 {
			t.Errorf("Logs interrupted at its first request for a log, following %t, wrote %q", follow, w.String())
		}
	}
}

// cancelling cancels at the write of the line.
type cancelling struct {
	bytes.Buffer
	line   string
	cancel context.CancelFunc
}

func (w *cancelling) Write(p []byte) (int, error) {
	if string(p) == w.line {
		w.cancel()
	}
	return w.Buffer.Write(p)
}

func TestLogsWhoseContextEndsBetweenTwoLogsFailsAndLeavesTheSecondUnwritten(t *testing.T) {
	name := unique("cutbetween")
	appOfPods(t, name, []string{"web-a", "web-b"}, "web")
	b, _ := logging(t, map[string]containerLog{"web-a/web": {text: "a\n"}, "web-b/web": {text: "b\n"}})
	ctx, cancel := context.WithCancel(context.Background())
	w := &cancelling{line: "[pod/web-a/web] a\n", cancel: cancel}
	if err := b.Logs(ctx, name, orchestrator.LogOptions{Tail: 1}, w); !errors.Is(err, context.Canceled) {
		t.Errorf("Logs whose context ended with a log unread: %v, want it to fail", err)
	}
	if got, want := w.String(), "[pod/web-a/web] a\n"; got != want {
		t.Errorf("Logs wrote %q, want %q", got, want)
	}
}

func TestLogsFollowsFiftyContainersAndRefusesOneMore(t *testing.T) {
	name := unique("many")
	var pods []string
	logs := map[string]containerLog{}
	for i := range 25 {
		p := fmt.Sprintf("web-%02d", i)
		pods = append(pods, p)
		logs[p+"/a"], logs[p+"/b"] = containerLog{text: "line\n"}, containerLog{text: "line\n"}
	}
	appOfPods(t, name, pods, "a", "b")
	b, kubelet := logging(t, logs)
	if got := strings.Count(logsOf(t, b, name, orchestrator.LogOptions{Tail: 1, Follow: true}), "\n"); got != 50 {
		t.Errorf("Logs following 50 containers wrote %d lines", got)
	}
	pod(t, name, "web-25", name, container{name: "a", image: "example.test/a:1", state: running})
	logs["web-25/a"] = containerLog{text: "line\n"}
	kubelet.requests = nil
	err := b.Logs(within(t, time.Minute), name, orchestrator.LogOptions{Tail: 1, Follow: true}, io.Discard)
	if want := name + " has 51 containers, and the logs of at most 50 are followed at once"; err == nil || err.Error() != want {
		t.Errorf("Logs following 51 containers: %v, want %q", err, want)
	}
	if asked := kubelet.asked(); len(asked) != 0 {
		t.Errorf("the refused Logs asked for %d logs", len(asked))
	}
	if got := strings.Count(logsOf(t, b, name, orchestrator.LogOptions{Tail: 1}), "\n"); got != 51 {
		t.Errorf("Logs without following wrote %d lines of the 51 containers", got)
	}
}

// broken reads text and then fails.
type broken struct{ text io.Reader }

func (b broken) Read(p []byte) (int, error) {
	n, err := b.text.Read(p)
	if err == io.EOF {
		err = errors.New("the connection is lost")
	}
	return n, err
}

func TestLogsFailsIfALogBreaksOffAndWritesWhatItReadOfIt(t *testing.T) {
	name := unique("broken")
	appOfPods(t, name, []string{"web-a", "web-b"}, "web")
	for _, follow := range []bool{false, true} {
		b, _ := logging(t, map[string]containerLog{
			"web-a/web": {then: broken{strings.NewReader("a1\na2")}},
			"web-b/web": {text: "b1\n"},
		})
		var w bytes.Buffer
		err := b.Logs(within(t, time.Minute), name, orchestrator.LogOptions{Tail: 5, Follow: follow}, &w)
		if want := "reading the log of web-a's web: the connection is lost"; err == nil || err.Error() != want {
			t.Errorf("Logs, following %t: %v, want %q", follow, err, want)
		}
		got := strings.Split(strings.TrimSuffix(w.String(), "\n"), "\n")
		slices.Sort(got)
		if want := []string{"[pod/web-a/web] a1", "[pod/web-a/web] a2", "[pod/web-b/web] b1"}; !slices.Equal(got, want) {
			t.Errorf("Logs, following %t, wrote %q, want %q", follow, got, want)
		}
	}
}

// failingWriter fails every write.
type failingWriter struct{ writes int }

func (w *failingWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, errors.New("the pipe is closed")
}

func TestLogsFailsOnceItCantWrite(t *testing.T) {
	name := unique("unwritten")
	appOfPods(t, name, []string{"web-a", "web-b", "web-c"}, "web")
	r, to := io.Pipe()
	defer to.Close()
	for _, follow := range []bool{false, true} {
		// Followed, the log of web-c never has a line and never ends.
		b, _ := logging(t, map[string]containerLog{
			"web-a/web": {text: "a1\na2\n", follows: r},
			"web-b/web": {text: "b1\n", follows: r},
			"web-c/web": {follows: r},
		})
		w := &failingWriter{}
		done := make(chan error, 1)
		go func() {
			done <- b.Logs(within(t, time.Minute), name, orchestrator.LogOptions{Tail: 5, Follow: follow}, w)
		}()
		if err := returned(t, done); err == nil || err.Error() != "the pipe is closed" {
			t.Errorf("Logs to a writer that fails, following %t: %v, want the writer's error", follow, err)
		}
		if w.writes != 1 {
			t.Errorf("Logs wrote %d times to a writer that failed, following %t", w.writes, follow)
		}
	}
}

func TestLogsOnAnEndedContextFailsAndAsksForNothing(t *testing.T) {
	name := unique("endedlogs")
	appOfPods(t, name, []string{"web-a"}, "web")
	b, kubelet := logging(t, map[string]containerLog{"web-a/web": {text: "a\n"}})
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("interrupted")
	cancel(cause)
	var w bytes.Buffer
	if err := b.Logs(ctx, name, orchestrator.LogOptions{Tail: 1}, &w); !errors.Is(err, cause) {
		t.Errorf("Logs on an ended context: %v, want its cause", err)
	}
	if w.Len() != 0 || len(kubelet.asked()) != 0 {
		t.Errorf("Logs on an ended context wrote %q and asked for %q", w.String(), kubelet.asked())
	}
}

// A request through a host has a minute. A followed log goes on past
// it.
func TestALogFollowedThroughAHostGoesOnPastTheTimeARequestHas(t *testing.T) {
	if testing.Short() {
		t.Skip("takes 70 seconds")
	}
	t.Parallel()
	later := make(chan struct{})
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/shop":
			json.NewEncoder(w).Encode(corev1.Namespace{
				TypeMeta:   metav1.TypeMeta{Kind: "Namespace", APIVersion: "v1"},
				ObjectMeta: metav1.ObjectMeta{Name: "shop", Labels: map[string]string{appLabel: "shop"}},
			})
		case "/api/v1/namespaces/shop/pods":
			json.NewEncoder(w).Encode(corev1.PodList{
				TypeMeta: metav1.TypeMeta{Kind: "PodList", APIVersion: "v1"},
				Items: []corev1.Pod{{
					ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "shop"},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web"}}},
				}},
			})
		case "/api/v1/namespaces/shop/pods/web-1/log":
			w.Header().Set("Content-Type", "text/plain")
			io.WriteString(w, "early\n")
			w.(http.Flusher).Flush()
			select {
			case <-later:
				io.WriteString(w, "late\n")
			case <-r.Context().Done():
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: api.Certificate().Raw})
	config := clientcmdapi.NewConfig()
	config.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:6443", CertificateAuthorityData: ca}
	config.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	config.Contexts["x"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	config.CurrentContext = "x"
	text, err := clientcmd.Write(*config)
	if err != nil {
		t.Fatal(err)
	}
	b := k3s.Through(hostTo{&controlPlane{kubeconfig: string(text)}, api.Listener.Addr().String()}, k3s.Cluster{})
	w := make(lineWriter, 4)
	done := make(chan error, 1)
	go func() {
		done <- b.Logs(context.Background(), "shop", orchestrator.LogOptions{Tail: 1, Follow: true}, w)
	}()
	if got := w.next(t); got != "[pod/web-1/web] early\n" {
		t.Errorf("the first write was %q", got)
	}
	select {
	case err := <-done:
		t.Fatalf("Logs returned %v within 70 seconds, with the log still followed", err)
	case line := <-w:
		t.Fatalf("Logs wrote %q, which the log doesn't have yet", line)
	case <-time.After(70 * time.Second):
	}
	close(later)
	if got := w.next(t); got != "[pod/web-1/web] late\n" {
		t.Errorf("the write after 70 seconds was %q", got)
	}
	if err := returned(t, done); err != nil {
		t.Errorf("Logs: %v", err)
	}
}
