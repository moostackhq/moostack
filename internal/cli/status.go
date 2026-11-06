package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/urfave/cli/v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/gitrepo"
	"github.com/moostackhq/moostack/internal/k3s"
	"github.com/moostackhq/moostack/internal/objects"
)

// kubeNode is what status reads of a Kubernetes node.
type kubeNode struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Unschedulable bool `json:"unschedulable"`
	} `json:"spec"`
	Status struct {
		Addresses []struct {
			Type    string `json:"type"`
			Address string `json:"address"`
		} `json:"addresses"`
		Capacity   map[string]string `json:"capacity"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		NodeInfo struct {
			KubeletVersion string `json:"kubeletVersion"`
			Architecture   string `json:"architecture"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

// kubeObject is what status reads of a Deployment, a StatefulSet, a
// DaemonSet or an Ingress.
type kubeObject struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int `json:"replicas"`
		Template struct {
			Spec struct {
				Containers []struct {
					Image string `json:"image"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
		Rules []struct {
			Host string `json:"host"`
		} `json:"rules"`
	} `json:"spec"`
	Status struct {
		ReadyReplicas          int `json:"readyReplicas"`
		NumberReady            int `json:"numberReady"`
		DesiredNumberScheduled int `json:"desiredNumberScheduled"`
	} `json:"status"`
}

type kubeList[T any] struct {
	Items []T `json:"items"`
}

// clusterStatus is mooctl cluster status.
func clusterStatus(ctx context.Context, env Env, cmd *cli.Command) error {
	d, err := openCluster(env, cmd)
	if err != nil {
		return err
	}
	nodes, err := d.Nodes()
	if err != nil {
		return err
	}
	fmt.Fprintf(env.Stdout, "Cluster %s, k3s %s\n", d.Cluster.Ref, d.Cluster.K3s)
	cp, ok, err := d.ControlPlane()
	if err != nil {
		return err
	}
	if !ok {
		if len(nodes) > 0 {
			w := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
			printDirNodes(env, w, nodes)
			return errors.New("the cluster directory has nodes but no control plane")
		}
		fmt.Fprintln(env.Stdout, "\nNo nodes.")
		return nil
	}
	w := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
	cpClient, err := dialNode(ctx, env, cp)
	if err != nil {
		printDirNodes(env, w, nodes)
		return fmt.Errorf("the control plane can't be reached, so this is the cluster directory alone: %w", err)
	}
	defer cpClient.Close()

	var kubeNodes kubeList[kubeNode]
	if err := kubectlJSON(ctx, cpClient, "get nodes", &kubeNodes); err != nil {
		printDirNodes(env, w, nodes)
		return err
	}
	var namespaces kubeList[struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}]
	if err := kubectlJSON(ctx, cpClient, "get namespaces -l "+objects.AppLabel, &namespaces); err != nil {
		return err
	}
	var objs kubeList[kubeObject]
	if err := kubectlJSON(ctx, cpClient, "get deployments,statefulsets,daemonsets,ingresses -A", &objs); err != nil {
		return err
	}
	var drift []string
	if applied, err := (remoteHost{cpClient}).appliedCommit(ctx); err != nil {
		drift = append(drift, err.Error())
	} else if applied != "" && !commitID.MatchString(applied) {
		drift = append(drift, fmt.Sprintf("the control plane's %s doesn't hold a commit: %q; operations are refused until it does", k3s.AppliedCommitFile, applied))
	} else if applied != "" {
		if repo, err := gitrepo.Open(ctx, d.Path); err == nil {
			if ok, err := repo.IsAncestor(ctx, applied); err == nil && !ok {
				drift = append(drift, fmt.Sprintf("the cluster applied commit %.12s, which the cluster directory doesn't have: fetch it", applied))
			}
		}
	}

	// Nodes.
	fmt.Fprintln(env.Stdout)
	fmt.Fprintln(w, "NODE\tROLE\tREADY\tK3S\tARCH\tMEMORY\tPRIVATE ADDRESS")
	byName := map[string]kubeNode{}
	for _, k := range kubeNodes.Items {
		byName[k.Metadata.Name] = k
	}
	for _, n := range nodes {
		role := k3s.RoleOf(n.ControlPlane)
		k, on := byName[n.Name]
		if !on {
			fmt.Fprintf(w, "%s\t%s\t%s\t\t%s\t%s\t%s\n", n.Name, role, "not on the cluster", n.Architecture, mib(n.MemoryMiB), n.PrivateAddress)
			drift = append(drift, n.Name+" is in the cluster directory but not on the cluster")
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", n.Name, role, ready(k), k.Status.NodeInfo.KubeletVersion, k.Status.NodeInfo.Architecture, capacity(k), n.PrivateAddress)
		if v := k.Status.NodeInfo.KubeletVersion; v != d.Cluster.K3s {
			drift = append(drift, fmt.Sprintf("%s runs k3s %s, the cluster directory pins %s", n.Name, v, d.Cluster.K3s))
		}
		if n.ControlPlane != (k.Metadata.Labels["node-role.kubernetes.io/control-plane"] == "true") {
			drift = append(drift, n.Name+" is a control plane on one side and not on the other")
		}
		if a := internalIP(k); a != n.PrivateAddress.String() {
			drift = append(drift, fmt.Sprintf("%s is at %s on the cluster, and at %s in the cluster directory", n.Name, a, n.PrivateAddress))
		}
		if a := k.Status.NodeInfo.Architecture; a != n.Architecture {
			drift = append(drift, fmt.Sprintf("%s is %s on the cluster, and %s in the cluster directory", n.Name, a, n.Architecture))
		}
	}
	for _, k := range kubeNodes.Items {
		if !slices.ContainsFunc(nodes, func(n clusterdir.Node) bool { return n.Name == k.Metadata.Name }) {
			fmt.Fprintf(w, "%s\t\t%s\t%s\t%s\t%s\t%s\n", k.Metadata.Name, ready(k), k.Status.NodeInfo.KubeletVersion, k.Status.NodeInfo.Architecture, capacity(k), internalIP(k))
			drift = append(drift, k.Metadata.Name+" is on the cluster but not in the cluster directory; remove it with mooctl cluster node-remove "+k.Metadata.Name)
		}
	}
	w.Flush()

	// Apps.
	fmt.Fprintln(env.Stdout)
	if len(namespaces.Items) == 0 {
		fmt.Fprintln(env.Stdout, "No apps.")
	} else {
		fmt.Fprintln(w, "APP\tREADY\tIMAGE\tHOSTS")
		for _, ns := range namespaces.Items {
			app := ns.Metadata.Name
			readyN, wantN := 0, 0
			var images, hosts []string
			for _, o := range objs.Items {
				if o.Metadata.Namespace != app {
					continue
				}
				switch o.Kind {
				case "Deployment", "StatefulSet":
					want := 1
					if o.Spec.Replicas != nil {
						want = *o.Spec.Replicas
					}
					wantN += want
					readyN += o.Status.ReadyReplicas
				case "DaemonSet":
					wantN += o.Status.DesiredNumberScheduled
					readyN += o.Status.NumberReady
				case "Ingress":
					for _, r := range o.Spec.Rules {
						if r.Host != "" {
							hosts = append(hosts, r.Host)
						}
					}
				}
				for _, c := range o.Spec.Template.Spec.Containers {
					if !slices.Contains(images, c.Image) {
						images = append(images, c.Image)
					}
				}
			}
			fmt.Fprintf(w, "%s\t%d/%d\t%s\t%s\n", app, readyN, wantN, strings.Join(images, ", "), strings.Join(hosts, ", "))
		}
		w.Flush()
	}

	if len(drift) > 0 {
		fmt.Fprintln(env.Stdout, "\nDrift:")
		for _, s := range drift {
			fmt.Fprintln(env.Stdout, "- "+s)
		}
	}
	return nil
}

// printDirNodes prints the nodes of the cluster directory alone.
func printDirNodes(env Env, w *tabwriter.Writer, nodes []clusterdir.Node) {
	fmt.Fprintln(env.Stdout)
	fmt.Fprintln(w, "NODE\tROLE\tARCH\tMEMORY\tPRIVATE ADDRESS")
	for _, n := range nodes {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", n.Name, k3s.RoleOf(n.ControlPlane), n.Architecture, mib(n.MemoryMiB), n.PrivateAddress)
	}
	w.Flush()
}

// internalIP is a Kubernetes node's internal IP address.
func internalIP(k kubeNode) string {
	for _, a := range k.Status.Addresses {
		if a.Type == "InternalIP" {
			return a.Address
		}
	}
	return ""
}

// kubectlJSON runs k3s kubectl args -o json on c into v.
func kubectlJSON(ctx context.Context, c remote, args string, v any) error {
	out, err := run(ctx, c, "k3s kubectl "+args+" -o json --request-timeout=60s", "")
	if err != nil {
		return fmt.Errorf("k3s kubectl %s: %w", args, err)
	}
	return json.Unmarshal([]byte(out), v)
}

// ready is whether a Kubernetes node is Ready, and cordoned.
func ready(k kubeNode) string {
	state := "?"
	for _, c := range k.Status.Conditions {
		if c.Type == "Ready" {
			state = "no"
			if c.Status == "True" {
				state = "yes"
			}
		}
	}
	if k.Spec.Unschedulable {
		state += ", cordoned"
	}
	return state
}

// capacity is a node's memory as Kubernetes sees it.
func capacity(k kubeNode) string {
	s := k.Status.Capacity["memory"]
	if kib, ok := strings.CutSuffix(s, "Ki"); ok {
		if n, err := strconv.Atoi(kib); err == nil {
			return mib(n / 1024)
		}
	}
	return s
}

func mib(n int) string { return fmt.Sprintf("%d MiB", n) }
