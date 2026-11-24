package k3s

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// issuer is the ClusterIssuer that apps' ingresses get their
// certificates from.
const issuer = "moostack"

// servicesTimeout bounds waiting for the cluster's own services.
const servicesTimeout = 5 * time.Minute

var clusterIssuers = schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "clusterissuers"}

// missingServices names the cluster's own services that aren't ready
// for apps: the issuer, cert-manager's webhook, Traefik and its ports.
func (a *api) missingServices(ctx context.Context) (missing []string, err error) {
	defer func() {
		if err != nil {
			missing, err = nil, fmt.Errorf("reading the cluster's own services: %w", err)
		}
	}()
	absent := func(err error) error {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	ready := false
	iss, err := a.dynamic.Resource(clusterIssuers).Get(ctx, issuer, metav1.GetOptions{})
	if absent(err) != nil {
		return nil, err
	}
	if err == nil {
		ready = conditionIs(iss, "Ready", "True")
	}
	if !ready {
		missing = append(missing, "the issuer "+issuer)
	}

	webhook, err := a.client.AppsV1().Deployments("cert-manager").Get(ctx, "cert-manager-webhook", metav1.GetOptions{})
	if absent(err) != nil {
		return nil, err
	}
	if err != nil || webhook.Status.AvailableReplicas < 1 {
		missing = append(missing, "cert-manager's webhook")
	}

	traefik, err := a.client.AppsV1().DaemonSets("kube-system").Get(ctx, "traefik", metav1.GetOptions{})
	if absent(err) != nil {
		return nil, err
	}
	if err != nil || traefik.Status.NumberAvailable < 1 {
		missing = append(missing, "Traefik")
	}

	ports, err := a.client.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{
		LabelSelector: "svccontroller.k3s.cattle.io/svcname=traefik",
		FieldSelector: "status.phase=" + string(corev1.PodRunning),
		Limit:         1,
	})
	if err != nil {
		return nil, err
	}
	if len(ports.Items) == 0 {
		missing = append(missing, "Traefik's ports")
	}
	return missing, nil
}

// conditionIs reports whether o's condition of the type has the status.
func conditionIs(o *unstructured.Unstructured, conditionType, status string) bool {
	conditions, _, _ := unstructured.NestedSlice(o.Object, "status", "conditions")
	for _, c := range conditions {
		if m, ok := c.(map[string]any); ok && m["type"] == conditionType && m["status"] == status {
			return true
		}
	}
	return false
}

// waitServices waits until the cluster's own services are ready for
// apps, and fails naming those that aren't when the time is up.
func (a *api) waitServices(ctx context.Context) error {
	var missing []string
	err := poll(ctx, servicesTimeout, 2*time.Second, func(ctx context.Context) (bool, error) {
		now, err := a.missingServices(ctx)
		if err != nil {
			return false, err
		}
		missing = now
		return len(missing) == 0, nil
	})
	if errors.Is(err, errNotInTime) {
		return fmt.Errorf("the cluster isn't ready for apps: still waiting for %s", strings.Join(missing, ", "))
	}
	return err
}

// traefikConfig is the HelmChartConfig that has k3s's Traefik redirect
// HTTP to HTTPS, but for ACME's HTTP-01 challenges, route Ingresses to
// ExternalName Services, and run on every node behind a Service that
// sends a node's traffic to that node's Traefik, which keeps the
// visitor's address.
const traefikConfig = `apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: traefik
  namespace: kube-system
spec:
  valuesContent: |-
    ports:
      web:
        allowACMEByPass: true
        http:
          redirections:
            entryPoint:
              to: websecure
              scheme: https
              permanent: true
    providers:
      kubernetesIngress:
        allowExternalNameServices: true
    deployment:
      kind: DaemonSet
    service:
      spec:
        externalTrafficPolicy: Local
`

// cert-manager, which issues the certificates of apps' ingresses.
const certManagerVersion = "v1.21.3"

// certManager is the HelmChart that installs cert-manager.
func certManager() string {
	return fmt.Sprintf(`apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: cert-manager
  namespace: kube-system
spec:
  repo: https://charts.jetstack.io
  chart: cert-manager
  version: %s
  targetNamespace: cert-manager
  createNamespace: true
  valuesContent: |-
    crds:
      enabled: true
`, certManagerVersion)
}

// issuerManifest is the ClusterIssuer: Let's Encrypt's, over HTTP-01
// through Traefik, for email, or a self-signed one without.
func issuerManifest(email string) string {
	if email == "" {
		return fmt.Sprintf(`apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: %s
spec:
  selfSigned: {}
`, issuer)
	}
	return fmt.Sprintf(`apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: %s
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: %q
    privateKeySecretRef:
      name: moostack-acme-account
    solvers:
      - http01:
          ingress:
            ingressClassName: traefik
`, issuer, email)
}

// autoDeployFile is the path of the manifest called name in the control
// plane's directory of manifests that k3s applies itself, and applies
// again until it takes.
func autoDeployFile(name string) string {
	return "/var/lib/rancher/k3s/server/manifests/moostack-" + name + ".yaml"
}

// The console, which runs on the control plane's node.
const (
	// consoleNamespace is the namespace of the console's objects, each
	// named consoleName. No app can have its name.
	consoleNamespace = "kube-system"
	consoleName      = "moostack-console"
	// consoleContainer is the console's container.
	consoleContainer = "console"
	// consoleAppliedTimeout is how long k3s gets to apply the console's
	// objects.
	consoleAppliedTimeout = 2 * time.Minute
	// consoleReadyTimeout is how long the console gets to be ready.
	consoleReadyTimeout = 5 * time.Minute
)

// consoleObjects is the console's objects: its account, which may only
// list pods, one replica of image on the control plane's node, replaced
// and never rolled, and its route on addresses, the node's, for
// /console and what is under it.
func consoleObjects(image string, addresses []netip.Addr) string {
	var hosts []string
	for _, a := range addresses {
		hosts = append(hosts, "Host(`"+a.String()+"`)")
	}
	return fmt.Sprintf(`apiVersion: v1
kind: ServiceAccount
metadata:
  name: %[4]s
  namespace: %[1]s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: %[4]s
rules:
  - apiGroups: [""]
    resources: [pods]
    verbs: [list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: %[4]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: %[4]s
subjects:
  - kind: ServiceAccount
    name: %[4]s
    namespace: %[1]s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[4]s
  namespace: %[1]s
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: %[4]s
  template:
    metadata:
      labels:
        app: %[4]s
    spec:
      serviceAccountName: %[4]s
      nodeSelector:
        node-role.kubernetes.io/control-plane: "true"
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
      containers:
        - name: %[5]s
          image: %[2]s
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities:
              drop: [ALL]
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            httpGet:
              path: /console/login
              port: http
          volumeMounts:
            - name: run
              mountPath: /run/console
      volumes:
        - name: run
          emptyDir:
            medium: Memory
---
apiVersion: v1
kind: Service
metadata:
  name: %[4]s
  namespace: %[1]s
spec:
  selector:
    app: %[4]s
  ports:
    - name: http
      port: 80
      targetPort: http
---
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: %[4]s
  namespace: %[1]s
spec:
  entryPoints: [websecure]
  routes:
    - match: (%[3]s) && (Path(`+"`/console`"+`) || PathPrefix(`+"`/console/`"+`))
      kind: Rule
      services:
        - name: %[4]s
          port: http
  tls: {}
`, consoleNamespace, image, strings.Join(hosts, " || "), consoleName, consoleContainer)
}

// RunConsole writes the console's objects, with image, to the control
// plane's directory of manifests that k3s applies itself. It waits
// consoleAppliedTimeout for the console's Deployment to ask for image,
// and then consoleReadyTimeout for it to run as applied; when that time
// is up, its error lists the pods and last events of the console's
// namespace.
func (b *Backend) RunConsole(ctx context.Context, image string, addresses []netip.Addr) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return causeIfEnded(ctx, b.runConsole(ctx, image, addresses))
}

func (b *Backend) runConsole(ctx context.Context, image string, addresses []netip.Addr) error {
	host, _ := b.controlPlane()
	if host == nil {
		return errNoControlPlane
	}
	if err := write(ctx, host, autoDeployFile("console"), consoleObjects(image, addresses)); err != nil {
		return fmt.Errorf("writing the console's objects: %w", err)
	}
	a, err := b.connect(ctx)
	if err != nil {
		return err
	}
	deployments := a.client.AppsV1().Deployments(consoleNamespace)
	var why error
	err = poll(ctx, consoleAppliedTimeout, 2*time.Second, func(ctx context.Context) (bool, error) {
		d, err := deployments.Get(ctx, consoleName, metav1.GetOptions{})
		if err != nil {
			why = err
			return false, nil
		}
		runs := ""
		if containers := d.Spec.Template.Spec.Containers; len(containers) > 0 {
			runs = containers[0].Image
		}
		why = fmt.Errorf("the console runs %s", runs)
		return runs == image, nil
	})
	if errors.Is(err, errNotInTime) {
		return fmt.Errorf("the console's objects weren't applied: %w", why)
	}
	if err != nil {
		return err
	}
	waiting := "its deployment wasn't read in time"
	err = poll(ctx, consoleReadyTimeout, time.Second, func(ctx context.Context) (bool, error) {
		d, err := deployments.Get(ctx, consoleName, metav1.GetOptions{})
		if err != nil {
			if ctx.Err() == nil {
				waiting = "its deployment can't be read: " + err.Error()
			}
			return false, nil
		}
		w, err := deploymentRollingOut(*d)
		if err != nil {
			return false, err
		}
		waiting = w
		return w == "", nil
	})
	if errors.Is(err, errNotInTime) {
		return fmt.Errorf("the console isn't ready: %s\n%s", waiting, a.diagnostics(ctx, consoleNamespace))
	}
	if err != nil {
		return fmt.Errorf("the console isn't ready: %w", err)
	}
	return nil
}

// ConsoleToken runs console token in the console's container, in one of
// its pods that runs, and returns what it prints. The command's own
// error output is in the error of one that fails.
func (b *Backend) ConsoleToken(ctx context.Context) (string, error) {
	if ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	token, err := b.consoleToken(ctx)
	return token, causeIfEnded(ctx, err)
}

func (b *Backend) consoleToken(ctx context.Context) (string, error) {
	a, err := b.connect(ctx)
	if err != nil {
		return "", err
	}
	pods, err := a.client.CoreV1().Pods(consoleNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + consoleName,
		FieldSelector: "status.phase=" + string(corev1.PodRunning),
	})
	if err != nil {
		return "", fmt.Errorf("listing the console's pods: %w", err)
	}
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil {
			out, err := a.exec(ctx, consoleNamespace, p.Name, consoleContainer, "console", "token")
			return strings.TrimSpace(out), err
		}
	}
	return "", errors.New("the console isn't running")
}

// execProtocol is the protocol of a command run in a container over a
// WebSocket: each message starts with a byte that says which stream the
// rest is of.
const (
	execProtocol = "v4.channel.k8s.io"
	execStdout   = 1
	execStderr   = 2
	execStatus   = 3
)

// execOutput is what a command run in a container has sent so far: what
// it printed, and how it ended once it has.
type execOutput struct {
	stdout, stderr strings.Builder
	status         *metav1.Status
}

// take adds a message of the command's connection: the output, the
// error output or the status its first byte says it is of. A message of
// any other stream is left out.
func (o *execOutput) take(message []byte) error {
	if len(message) == 0 {
		return nil
	}
	switch message[0] {
	case execStdout:
		o.stdout.Write(message[1:])
	case execStderr:
		o.stderr.Write(message[1:])
	case execStatus:
		status := &metav1.Status{}
		if err := json.Unmarshal(message[1:], status); err != nil {
			return fmt.Errorf("its status can't be read: %w", err)
		}
		o.status = status
	}
	return nil
}

// exec runs command in the container of the pod and returns what it
// prints. A command that fails is an error with what it printed as its
// error output. It closes its connection when ctx ends, and then fails
// with ctx's cause.
func (a *api) exec(ctx context.Context, namespace, pod, container string, command ...string) (string, error) {
	u := a.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").Timeout(0).
		VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, scheme.ParameterCodec).URL()
	u.Scheme = "wss"
	tlsConfig, err := rest.TLSConfigFor(a.config)
	if err != nil {
		return "", err
	}
	header := http.Header{}
	switch {
	case a.config.BearerToken != "":
		header.Set("Authorization", "Bearer "+a.config.BearerToken)
	case a.config.Username != "":
		r := &http.Request{Header: header}
		r.SetBasicAuth(a.config.Username, a.config.Password)
	}
	dial := a.config.Dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	stop := func() bool { return true }
	defer func() { stop() }()
	dialer := websocket.Dialer{
		TLSClientConfig: tlsConfig,
		Subprotocols:    []string{execProtocol},
		NetDialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			conn, err := dial(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			stop = context.AfterFunc(ctx, func() { conn.Close() })
			return noDeadlines{conn}, nil
		},
	}
	conn, res, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		if ctx.Err() != nil {
			return "", context.Cause(ctx)
		}
		if res != nil {
			said, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
			return "", fmt.Errorf("running %s in %s: %s: %s", strings.Join(command, " "), pod, res.Status, strings.TrimSpace(string(said)))
		}
		return "", fmt.Errorf("running %s in %s: %w", strings.Join(command, " "), pod, err)
	}
	defer conn.Close()

	var out execOutput
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if err := out.take(message); err != nil {
			return "", fmt.Errorf("running %s in %s: %w", strings.Join(command, " "), pod, err)
		}
	}
	stdout, stderr, status := &out.stdout, &out.stderr, out.status
	if status == nil && ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	if status == nil {
		return "", fmt.Errorf("running %s in %s: the connection ended before the command did", strings.Join(command, " "), pod)
	}
	if status.Status != metav1.StatusSuccess {
		said := strings.TrimSpace(stderr.String())
		if said == "" {
			said = status.Message
		}
		return "", fmt.Errorf("running %s in %s: %s", strings.Join(command, " "), pod, said)
	}
	return stdout.String(), nil
}

// noDeadlines is a connection that ignores the deadlines it is given.
type noDeadlines struct{ net.Conn }

func (noDeadlines) SetDeadline(time.Time) error      { return nil }
func (noDeadlines) SetReadDeadline(time.Time) error  { return nil }
func (noDeadlines) SetWriteDeadline(time.Time) error { return nil }
