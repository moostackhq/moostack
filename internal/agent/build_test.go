//go:build linux

package agent_test

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/buildid"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/nodetest"
	"github.com/moostackhq/moostack/internal/ociimage"
)

// Status says which build of each component and service runs, so that
// two builds of one version are told apart: a component's by the sums of
// its files, a service's by its image's digest.
func TestStatusSaysWhichBuildOfEachComponentAndServiceRuns(t *testing.T) {
	n := nodetest.Start(t)
	n.StartServices(t)
	status := n.EventuallyHealthy(t)
	sumOf := func(file string) string {
		t.Helper()
		out, err := n.Output("root", "sha256sum", file)
		if err != nil {
			t.Fatalf("sha256sum %s: %v %s", file, err, out)
		}
		return strings.Fields(out)[0]
	}
	want := map[string]string{
		"agent":   buildid.FromSums(map[string]string{"agent": sumOf(hostlayout.Agent)}),
		"runc":    buildid.FromSums(map[string]string{"runc": sumOf(hostlayout.Runc)}),
		"updater": buildid.FromSums(map[string]string{"updater": sumOf(hostlayout.Updater)}),
	}
	containerd := map[string]string{}
	for _, f := range hostlayout.ContainerdFiles {
		containerd[f] = sumOf(path.Join(hostlayout.BinDir, f))
	}
	want["containerd"] = buildid.FromSums(containerd)
	// Status doesn't wait for the files to be hashed: the builds show
	// once they are.
	nodetest.Eventually(t, time.Minute, func() error {
		status, err := n.Status(t)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		updater, err := n.Updater.Status(ctx, &nodev1.UpdaterStatusRequest{})
		if err != nil {
			return err
		}
		for _, c := range append(status.Components, updater.Updater) {
			if c.Build != want[c.Name] {
				return fmt.Errorf("status says %s's build is %q, want %q", c.Name, c.Build, want[c.Name])
			}
		}
		return nil
	})
	for _, s := range status.Services {
		digest, err := ociimage.Digest(nodetest.ImageArchive(t, s.Name))
		if err != nil {
			t.Fatal(err)
		}
		if s.Build != digest {
			t.Errorf("status says %s's build is %q, want its image's digest %s", s.Name, s.Build, digest)
		}
	}
}
