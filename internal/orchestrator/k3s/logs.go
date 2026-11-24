package k3s

import (
	"bufio"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

// maxFollowed is how many containers' logs are followed at once.
const maxFollowed = 50

// Logs writes the logs of every container of every pod of the app's
// namespace, init and ephemeral ones as well, each line as
// "[pod/web-1/web] line". Without o.Follow it writes them one after
// another, by pod's name, then container as the pod lists them. With
// it, it writes each line as it comes, and refuses more than
// maxFollowed containers. A log that can't be read is a line
// "error: ..." in its place, and Logs goes on. It fails, once the other
// logs have ended, if a log breaks off, and at once if w does. If ctx
// ends with a log unread or not ended, it fails, with ctx's cause
// unless its deadline ended it, and writes nothing more.
func (b *Backend) Logs(ctx context.Context, app string, o orchestrator.LogOptions, w io.Writer) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return causeIfEnded(ctx, b.logs(ctx, app, o, w))
}

func (b *Backend) logs(ctx context.Context, app string, o orchestrator.LogOptions, w io.Writer) error {
	a, err := b.connect(ctx)
	if err != nil {
		return err
	}
	ns, err := a.namespace(ctx, app)
	if err != nil {
		return err
	}
	pods, err := a.client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing %s's pods: %w", app, err)
	}
	if len(pods.Items) == 0 {
		return orchestrator.ErrNoReplicas
	}
	slices.SortFunc(pods.Items, func(x, y corev1.Pod) int { return cmp.Compare(x.Name, y.Name) })
	var logs []containerLog
	for _, p := range pods.Items {
		for _, c := range p.Spec.InitContainers {
			logs = append(logs, containerLog{ns, p.Name, c.Name})
		}
		for _, c := range p.Spec.Containers {
			logs = append(logs, containerLog{ns, p.Name, c.Name})
		}
		for _, c := range p.Spec.EphemeralContainers {
			logs = append(logs, containerLog{ns, p.Name, c.Name})
		}
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	out := &lines{w: w, failed: stop}
	if !o.Follow {
		for _, l := range logs {
			if ctx.Err() != nil {
				out.broke(context.Cause(ctx))
				break
			}
			a.copyLog(ctx, l, o, out)
		}
		return out.result()
	}
	if len(logs) > maxFollowed {
		return fmt.Errorf("%s has %d containers, and the logs of at most %d are followed at once", app, len(logs), maxFollowed)
	}
	var wg sync.WaitGroup
	for _, l := range logs {
		wg.Go(func() { a.copyLog(ctx, l, o, out) })
	}
	wg.Wait()
	return out.result()
}

// containerLog is the log of a container of a pod.
type containerLog struct {
	namespace, pod, container string
}

// copyLog writes l's lines to out, each after l's pod and container: its
// last o.Tail lines, or all of them if o.Tail is negative, and with
// o.Follow those that come until the log ends. It tells out a log that
// can't be read, and one that breaks off or that ctx's end keeps from
// being read.
func (a *api) copyLog(ctx context.Context, l containerLog, o orchestrator.LogOptions, out *lines) {
	opts := &corev1.PodLogOptions{Container: l.container, Follow: o.Follow}
	if o.Tail >= 0 {
		tail := int64(o.Tail)
		opts.TailLines = &tail
	}
	stream, err := a.streams.CoreV1().Pods(l.namespace).GetLogs(l.pod, opts).Stream(ctx)
	if err != nil {
		if ctx.Err() != nil {
			out.broke(context.Cause(ctx))
			return
		}
		out.write("", []byte("error: "+err.Error()))
		return
	}
	defer stream.Close()
	prefix := "[pod/" + l.pod + "/" + l.container + "] "
	r := bufio.NewReader(stream)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 && !out.write(prefix, line) {
			return
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			out.broke(fmt.Errorf("reading the log of %s's %s: %w", l.pod, l.container, err))
			return
		}
	}
}

// lines writes lines to w, one writer at a time. Once a write fails, it
// calls failed and writes no more.
type lines struct {
	w      io.Writer
	failed func()

	mu sync.Mutex
	// writeErr is the error of the write that failed, and broken that of
	// the first log that broke off or wasn't read.
	writeErr, broken error
}

// write writes prefix and line, which it ends with a line break if it
// has none, in one write, and reports whether it wrote them.
func (l *lines) write(prefix string, line []byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writeErr != nil {
		return false
	}
	text := append([]byte(prefix), line...)
	if text[len(text)-1] != '\n' {
		text = append(text, '\n')
	}
	if _, err := l.w.Write(text); err != nil {
		l.writeErr = err
		l.failed()
		return false
	}
	return true
}

func (l *lines) broke(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken == nil {
		l.broken = err
	}
}

// result is the error of the write that failed, or else of the first
// log that broke off or wasn't read.
func (l *lines) result() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writeErr != nil {
		return l.writeErr
	}
	return l.broken
}
