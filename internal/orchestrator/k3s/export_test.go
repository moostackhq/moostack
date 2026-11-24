package k3s

import (
	"net/netip"
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

func EventTime(e corev1.Event) time.Time { return eventTime(e) }

// NodeFiles is nodeFiles: each file's content, by its path.
func NodeFiles(n model.Node, c Cluster, controlPlane netip.Addr, token string) map[string]string {
	files := map[string]string{}
	for _, f := range nodeFiles(n, c, controlPlane, token) {
		files[f.path] = f.content
	}
	return files
}

// Scripts are the scripts the backend runs on a host of a cluster of
// k3s version, whose control plane is at controlPlane, by what each is
// for.
func Scripts(version string, controlPlane netip.Addr) (map[string]string, error) {
	scripts := map[string]string{
		"facts":                   factsScript,
		"downloaded":              downloadedScript,
		"start":                   startScript,
		"reach":                   reachScript(controlPlane),
		"uninstall-control-plane": uninstallScript(controlPlaneRole),
		"uninstall-node":          uninstallScript(agentRole),
	}
	for name, r := range map[string]role{"install-control-plane": controlPlaneRole, "install-node": agentRole} {
		script, err := installScript(version, r)
		if err != nil {
			return nil, err
		}
		scripts[name] = script
	}
	return scripts, nil
}

func ConsoleObjects(image string, addresses []netip.Addr) string {
	return consoleObjects(image, addresses)
}
