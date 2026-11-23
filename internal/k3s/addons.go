package k3s

import (
	"fmt"
	"net/netip"
	"strings"
)

// TraefikConfig is the HelmChartConfig that has k3s's Traefik redirect
// HTTP to HTTPS, but for ACME's HTTP-01 challenges, route Ingresses to
// ExternalName Services, and run on every node behind a Service that
// sends a node's traffic to that node's Traefik, which keeps the
// visitor's address.
const TraefikConfig = `apiVersion: helm.cattle.io/v1
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

// Issuer is the ClusterIssuer that apps' ingresses get their certificates
// from.
const Issuer = "moostack"

// CertManager is the HelmChart that installs cert-manager.
func CertManager() string {
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

// IssuerManifest is the ClusterIssuer: Let's Encrypt's, over HTTP-01
// through Traefik, for email, or a self-signed one without.
func IssuerManifest(email string) string {
	if email == "" {
		return fmt.Sprintf(`apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: %s
spec:
  selfSigned: {}
`, Issuer)
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
`, Issuer, email)
}

// The console, which runs on the control plane's node.
const (
	// ConsoleNamespace is the namespace of the console's objects, each
	// named ConsoleName. No app can have its name.
	ConsoleNamespace = "kube-system"
	ConsoleName      = "moostack-console"
	// ConsoleRepository is the console's repository in the cluster's
	// registry, under a name no app can have.
	ConsoleRepository = ConsoleNamespace + "/console"
	// ConsoleImageScript prints the image the console's Deployment runs,
	// and fails while there is none.
	ConsoleImageScript = "k3s kubectl get deployment " + ConsoleName + " -n " + ConsoleNamespace + " -o jsonpath='{.spec.template.spec.containers[0].image}' --request-timeout=20s"
	// ConsoleTokenScript prints a new login token of the console.
	ConsoleTokenScript = "k3s kubectl exec deployment/" + ConsoleName + " -n " + ConsoleNamespace + " -- console token"
	// ConsoleReadyScript waits up to 5 minutes for the console to be
	// ready.
	ConsoleReadyScript = "k3s kubectl rollout status deployment/" + ConsoleName + " -n " + ConsoleNamespace + " --timeout=300s"
)

// ConsoleURL is the URL of the console's login page at address, one of
// the control plane node's.
func ConsoleURL(address netip.Addr) string {
	host := address.String()
	if address.Is6() {
		host = "[" + host + "]"
	}
	return "https://" + host + "/console/login"
}

// Console is the console's objects: its account, which may only list
// pods, one replica of image on the control plane's node, replaced and
// never rolled, and its route on addresses, the node's, for /console and
// what is under it.
func Console(image string, addresses []netip.Addr) string {
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
        - name: console
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
`, ConsoleNamespace, image, strings.Join(hosts, " || "), ConsoleName)
}

// AutoDeployFile is the path of the manifest called name in the control
// plane's directory of manifests that k3s applies itself, and applies
// again until it takes.
func AutoDeployFile(name string) string {
	return "/var/lib/rancher/k3s/server/manifests/moostack-" + name + ".yaml"
}
