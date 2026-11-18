package sshclient_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/sshclient"
)

// Close returns on a connection that just went silent.
func TestCloseReturnsOnAConnectionThatJustWentSilent(t *testing.T) {
	t.Parallel()
	signer, file, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	proxy := newSilentProxy(t, "127.0.0.1:"+d.port)
	viaProxy := newHome(t, strings.Replace(nodeConfig(d, "root"), "Port "+d.port, "Port "+proxy.port(), 1))
	viaProxy.write(t, ".ssh/key", file)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	a, err := sshclient.Dial(ctx, d.node(), viaProxy.options())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	held, err := a.Hold(ctx, k3s.LockScript, k3s.LockReady)
	if err != nil {
		t.Fatal(err)
	}
	proxy.silence.Store(true)
	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- held.Close() }()
	select {
	case <-closed:
		t.Logf("Close returned after %v", time.Since(start))
	case <-time.After(30 * time.Second):
		t.Error("Close on a connection that just went silent hasn't returned in 30 s")
	}
}

// A command RunStoppable started is killed on the node once its
// connection goes silent, after its context ended.
func TestRunStoppableEndsTheCommandOverASilentConnection(t *testing.T) {
	t.Parallel()
	signer, file, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	proxy := newSilentProxy(t, "127.0.0.1:"+d.port)
	viaProxy := newHome(t, strings.Replace(nodeConfig(d, "root"), "Port "+d.port, "Port "+proxy.port(), 1))
	viaProxy.write(t, ".ssh/key", file)
	a, err := sshclient.Dial(t.Context(), d.node(), viaProxy.options())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(2 * time.Second)
		proxy.silence.Store(true)
		time.Sleep(time.Second)
		cancel()
	}()
	if _, err := a.RunStoppable(ctx, "sleep 70; touch /tmp/after-silence"); err == nil {
		t.Fatal("RunStoppable outlived its context")
	}
	time.Sleep(80 * time.Second)
	if out := d.exec(t, "sh", "-c", "test -e /tmp/after-silence && echo there || echo gone"); strings.TrimSpace(out) != "gone" {
		t.Error("the command went on after its context ended on a silent connection")
	}
}

// Taking the lock gives up when the node goes silent before it answers.
func TestHoldGivesUpOnASilentNode(t *testing.T) {
	defer sshclient.SetReadyTimeout(3 * time.Second)()
	signer, file, _ := newKey(t)
	d := startSSHD(t, signer.PublicKey())
	proxy := newSilentProxy(t, "127.0.0.1:"+d.port)
	viaProxy := newHome(t, strings.Replace(nodeConfig(d, "root"), "Port "+d.port, "Port "+proxy.port(), 1))
	viaProxy.write(t, ".ssh/key", file)
	a, err := sshclient.Dial(t.Context(), d.node(), viaProxy.options())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	proxy.silence.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := a.Hold(t.Context(), k3s.LockScript, k3s.LockReady)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Error("Hold took a lock on a silent node")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Hold on a silent node didn't end")
	}
}
