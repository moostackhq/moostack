package k3s

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
