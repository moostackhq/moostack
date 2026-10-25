package cli_test

import (
	"encoding/json"
	"errors"
	"testing"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

func TestClusterStatusSaysWhetherAnUpdateIsNeededInProgressOrStoppedPartway(t *testing.T) {
	for _, c := range []struct {
		name string
		// set sets up the update cluster, pinned to 1.1.0.
		set        func(o *operator)
		line, json string
	}{
		{"not started", func(o *operator) {}, "Update: not started; run mooctl cluster update\n", "not_started"},
		{"stopped partway", func(o *operator) {
			o.nodes["node1"].runs("1.1.0", "agent", "updater").runs("2.1.0", "containerd")
		}, "Update: stopped partway; run mooctl cluster update again to finish it\n", "stopped_partway"},
		{"stopped before the last service", func(o *operator) {
			updatedTo110(o)
			o.nodes["node3"].runsServices("1.0.0", "proxy")
		}, "Update: stopped partway; run mooctl cluster update again to finish it\n", "stopped_partway"},
		{"up to date", updatedTo110, "Update: none needed\n", "up_to_date"},
		{"at the target but for a service that isn't healthy", func(o *operator) {
			updatedTo110(o)
			n := o.nodes["node3"]
			n.mu.Lock()
			n.setService(&nodev1.Component{Name: "proxy", Version: "1.1.0", Health: nodev1.Health_HEALTH_UNHEALTHY})
			n.mu.Unlock()
		}, "Update: stopped partway; run mooctl cluster update again to finish it\n", "stopped_partway"},
		{"up to date but for a node status can't reach", func(o *operator) {
			updatedTo110(o)
			o.nodes["node2"].err = errors.New("no route to host")
		}, "Update: stopped partway; run mooctl cluster update again to finish it\n", "stopped_partway"},
		{"in progress", func(o *operator) {
			o.nodes["node1"].runs("1.1.0", "agent")
			for _, n := range o.nodes {
				l := bobsLock
				l.Operation = "cluster update"
				n.locked(l)
			}
		}, "Update: in progress\n", "in_progress"},
		{"in progress, with node1 on a release that doesn't record the operation", func(o *operator) {
			o.nodes["node2"].runs("1.1.0", "agent")
			for name, n := range o.nodes {
				l := bobsLock
				if name != "node1" {
					l.Operation = "cluster update"
				}
				n.locked(l)
			}
		}, "Update: in progress\n", "in_progress"},
		{"refused, as a service would skip a release", func(o *operator) {
			updatedTo110(o)
			o.nodes["node3"].runsServices("0.9.0", "dns")
		}, "Update: refused, so cluster update won't run:\nnode3: its dns service runs release 0.9.0, and release 1.1.0 updates nodes on 1.1.0 or 1.0.0", "refused"},
		{"refused, as mooctl doesn't operate a node's release", func(o *operator) {
			o.nodes["node2"].runs("0.9.0", "agent")
		}, "Update: refused, so cluster update won't run:\nnode2: it runs release 0.9.0, which this mooctl, of release 1.1.0, doesn't operate", "refused"},
		{"development builds", func(o *operator) {
			o.nodes["node1"].runs(devVersion, "agent")
		}, "Update: development builds run; mooctl cluster update replaces them with the pinned release\n", "development_builds"},
		{"up to date but for a development build of a service", func(o *operator) {
			updatedTo110(o)
			o.nodes["node2"].runsServices(devVersion, "dns")
		}, "Update: development builds run; mooctl cluster update replaces them with the pinned release\n", "development_builds"},
		{"in progress, from a source", func(o *operator) {
			updatedTo110(o)
			o.nodes["node2"].runs(devVersion, "updater")
			for _, n := range o.nodes {
				l := bobsLock
				l.Operation = "cluster update"
				n.locked(l)
			}
		}, "Update: in progress\n", "in_progress"},
		{"refused, with a development build", func(o *operator) {
			o.nodes["node1"].runs(devVersion, "agent")
			o.nodes["node2"].runsServices("0.9.0", "dns")
		}, "Update: refused, so cluster update won't run:\nnode2: its dns service runs release 0.9.0", "refused"},
		{"another operation", func(o *operator) {
			for _, n := range o.nodes {
				n.locked(bobsLock)
			}
		}, "Update: not started; run mooctl cluster update\n", "not_started"},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOperator(t, "op")
			source := updateCluster(t, o)
			c.set(o)
			r := o.mustRun("--release-source", source, "cluster", "status")
			wantContains(t, "cluster status", r.stdout, "\n"+c.line)
			r = o.mustRun("--release-source", source, "cluster", "status", "--json")
			var view struct{ Update string }
			if err := json.Unmarshal([]byte(r.stdout), &view); err != nil {
				t.Fatal(err)
			}
			if view.Update != c.json {
				t.Errorf("cluster status --json says update %q, want %q", view.Update, c.json)
			}
		})
	}
}

func TestClusterStatusShowsTheNodesWhenItCantOpenThePinnedRelease(t *testing.T) {
	o := newOperator(t, "op")
	updateCluster(t, o)
	// A release source that is down.
	s := serveSource(t, t.TempDir())
	o.client = s.Client()
	s.Close()
	r := o.mustRun("--release-source", s.URL, "cluster", "status")
	wantContains(t, "cluster status", r.stdout, "Cluster prod on release 1.1.0\n", "Update: unknown, as mooctl can't open the pinned release\n", "\nnode3  direct\n  agent  1.0.0    -       healthy\n")
	wantContains(t, "cluster status's warnings", r.stderr, "can't open release 1.1.0", "showing what the nodes run without it")
	r = o.mustRun("--release-source", s.URL, "cluster", "status", "--json")
	var view struct{ Update string }
	if err := json.Unmarshal([]byte(r.stdout), &view); err != nil {
		t.Fatal(err)
	}
	if view.Update != "unknown" {
		t.Errorf("cluster status --json says update %q, want unknown", view.Update)
	}
}
