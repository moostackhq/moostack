// Package k3s is the orchestrator backend for k3s, over the Kubernetes
// API.
package k3s

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	"github.com/moostackhq/moostack/internal/orchestrator"
)

// kubeconfigFile is k3s's kubeconfig of the cluster's administrator, on
// the control plane.
const kubeconfigFile = "/etc/rancher/k3s/k3s.yaml"

// The Kubernetes client logs nothing.
func init() {
	klog.SetLogger(logr.Discard())
}

// requestTimeout bounds each request to the API made through a host.
const requestTimeout = time.Minute

type Backend struct {
	// host is the control plane the API is reached through, nil if it
	// isn't reached through one.
	host orchestrator.Host

	mu  sync.Mutex
	api *api
}

// api is a connection to the cluster's API.
type api struct {
	client  kubernetes.Interface
	dynamic dynamic.Interface
	mapper  *restmapper.DeferredDiscoveryRESTMapper
}

// New returns a Backend that connects with config.
func New(config *rest.Config) (*Backend, error) {
	a, err := newAPI(config)
	if err != nil {
		return nil, err
	}
	return &Backend{api: a}, nil
}

// InCluster returns a Backend that connects as the service account of
// the pod the process runs in.
func InCluster() (*Backend, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return New(config)
}

// Through returns a Backend that reaches the API through host, the
// cluster's control plane, as the cluster's administrator. It connects
// at the first call that needs the API: it reads the kubeconfig on
// host, keeps it in memory, and dials the API through host. It reads no
// file and runs no command that the kubeconfig names. Its own
// errors hold nothing of the kubeconfig; a failed read's error is
// host's. With a nil host, every call that
// needs the API fails.
func Through(host orchestrator.Host) *Backend {
	return &Backend{host: host}
}

func newAPI(config *rest.Config) (*api, error) {
	config = rest.CopyConfig(config)
	config.WarningHandler = rest.NoWarnings{}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &api{
		client:  client,
		dynamic: dyn,
		mapper:  restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(client.Discovery())),
	}, nil
}

// connect returns the connection to the API, which it makes if there is
// none yet.
func (b *Backend) connect(ctx context.Context) (*api, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.api != nil {
		return b.api, nil
	}
	if b.host == nil {
		return nil, errors.New("the cluster has no control plane to reach it through")
	}
	kubeconfig, err := b.host.Run(ctx, "cat "+kubeconfigFile, nil)
	if err != nil {
		return nil, fmt.Errorf("reading the control plane's %s: %w", kubeconfigFile, err)
	}
	config, ok := restConfig([]byte(kubeconfig))
	if !ok {
		return nil, fmt.Errorf("the control plane's %s isn't a kubeconfig", kubeconfigFile)
	}
	host := b.host
	config.Dial = func(ctx context.Context, _, addr string) (net.Conn, error) { return host.DialTCP(ctx, addr) }
	config.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	config.Timeout = requestTimeout
	a, err := newAPI(config)
	if err != nil {
		return nil, fmt.Errorf("the control plane's %s can't be connected with", kubeconfigFile)
	}
	b.api = a
	return a, nil
}

// restConfig is how to connect as kubeconfig's current context says,
// with what the kubeconfig itself holds: the server, its CA, and the
// user's certificate and key, token, or name and password. A
// certificate without its key, or a key without its certificate, is no
// kubeconfig. It takes
// nothing the kubeconfig names on this machine: no file, and no command
// or provider of credentials.
func restConfig(kubeconfig []byte) (*rest.Config, bool) {
	c, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return nil, false
	}
	current := c.Contexts[c.CurrentContext]
	if current == nil {
		return nil, false
	}
	cluster, user := c.Clusters[current.Cluster], c.AuthInfos[current.AuthInfo]
	if cluster == nil || user == nil || cluster.Server == "" {
		return nil, false
	}
	if (len(user.ClientCertificateData) == 0) != (len(user.ClientKeyData) == 0) {
		return nil, false
	}
	return &rest.Config{
		Host:        cluster.Server,
		BearerToken: user.Token,
		Username:    user.Username,
		Password:    user.Password,
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: cluster.TLSServerName,
			CAData:     cluster.CertificateAuthorityData,
			CertData:   user.ClientCertificateData,
			KeyData:    user.ClientKeyData,
		},
	}, true
}

// mapping is how the API serves the kind gk. A kind it doesn't know is
// looked up once more, after reading the API's kinds again.
func (a *api) mapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	m, err := a.mapper.RESTMapping(gk, versions...)
	if meta.IsNoMatchError(err) {
		a.mapper.Reset()
		m, err = a.mapper.RESTMapping(gk, versions...)
	}
	return m, err
}

// errNotInTime is poll's error when the time is up.
var errNotInTime = errors.New("not in time")

// poll asks done at once and then every interval until it says yes or
// fails. It fails with errNotInTime once timeout has passed or ctx's
// deadline has, and with ctx's cause if ctx ends otherwise.
func poll(ctx context.Context, timeout, interval time.Duration, done func(ctx context.Context) (bool, error)) error {
	timed, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		ok, err := done(timed)
		if err == nil && ok {
			return nil
		}
		if err != nil && timed.Err() == nil {
			return err
		}
		if timed.Err() == nil {
			select {
			case <-timed.Done():
			case <-time.After(interval):
				continue
			}
		}
		if ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return context.Cause(ctx)
		}
		return errNotInTime
	}
}

func (b *Backend) Can() orchestrator.Capabilities {
	return orchestrator.Capabilities{KubernetesObjects: true}
}

var _ orchestrator.Orchestrator = (*Backend)(nil)
