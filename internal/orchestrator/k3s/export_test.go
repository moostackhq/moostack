package k3s

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/moostackhq/moostack/internal/model"
)

// Objects is objectsOf: the app's namespace and HelmChart, and the
// objects of its namespace.
func Objects(app model.App) (cluster, own string, err error) {
	o, err := objectsOf(app)
	return o.cluster, o.own, err
}

// RollingOut is deploymentRollingOut, statefulSetRollingOut or
// daemonSetRollingOut of workload.
func RollingOut(workload any) (string, error) {
	switch w := workload.(type) {
	case appsv1.Deployment:
		return deploymentRollingOut(w)
	case appsv1.StatefulSet:
		return statefulSetRollingOut(w)
	case appsv1.DaemonSet:
		return daemonSetRollingOut(w)
	}
	panic("not a workload")
}

// EventTime is eventTime.
func EventTime(e corev1.Event) time.Time { return eventTime(e) }
