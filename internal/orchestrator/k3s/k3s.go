// Package k3s is the orchestrator backend for k3s, over the Kubernetes
// API.
package k3s

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
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

// requestTimeout bounds each request to the API made through a host,
// but for a container's log.
const requestTimeout = time.Minute

type Backend struct {
	mu sync.Mutex
	// host is the control plane the API is reached through, nil if it
	// isn't reached through one.
	host    orchestrator.Host
	cluster Cluster
	api     *api
}

// Cluster is what the backend is told of a cluster to make hosts its
// nodes.
type Cluster struct {
	// Version is the k3s version of the cluster's nodes.
	Version string
	// Email is the email of the ACME account that certificates are
	// issued to, empty for self-signed ones.
	Email string
	// Registry is the name nodes pull the cluster's own images from, and
	// RegistryPort the port of that registry on the control plane's
	// private address.
	Registry     string
	RegistryPort uint16
	// ControlPlane is the control plane's private address, invalid for a
	// cluster without one.
	ControlPlane netip.Addr
}

// api is a connection to the cluster's API.
type api struct {
	// config is how the connection was made.
	config *rest.Config
	client kubernetes.Interface
	// streams is client without a bound on how long an answer takes.
	streams kubernetes.Interface
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
// needs the API fails, until AddNode has made a host the control plane.
func Through(host orchestrator.Host, cluster Cluster) *Backend {
	return &Backend{host: host, cluster: cluster}
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
	unbounded := rest.CopyConfig(config)
	unbounded.Timeout = 0
	streams, err := kubernetes.NewForConfig(unbounded)
	if err != nil {
		return nil, err
	}
	return &api{
		config:  config,
		client:  client,
		streams: streams,
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
		return nil, errNoControlPlane
	}
	a, err := connectThrough(ctx, b.host)
	if err != nil {
		return nil, err
	}
	b.api = a
	return a, nil
}

// connectThrough connects to the API through host, a control plane.
func connectThrough(ctx context.Context, host orchestrator.Host) (*api, error) {
	kubeconfig, err := host.Run(ctx, "cat "+kubeconfigFile, nil)
	if err != nil {
		return nil, fmt.Errorf("reading the control plane's %s: %w", kubeconfigFile, err)
	}
	config, ok := restConfig([]byte(kubeconfig))
	if !ok {
		return nil, fmt.Errorf("the control plane's %s isn't a kubeconfig", kubeconfigFile)
	}
	config.Dial = func(ctx context.Context, _, addr string) (net.Conn, error) { return host.DialTCP(ctx, addr) }
	config.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	config.Timeout = requestTimeout
	a, err := newAPI(config)
	if err != nil {
		return nil, fmt.Errorf("the control plane's %s can't be connected with", kubeconfigFile)
	}
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
