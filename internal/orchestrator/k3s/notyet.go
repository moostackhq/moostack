package k3s

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/moostackhq/moostack/internal/model"
	"github.com/moostackhq/moostack/internal/orchestrator"
)

// errNotYet is the error of the methods in this file.
var errNotYet = errors.New("the k3s backend can't do this yet")

func notYet(method string) error { return fmt.Errorf("%s: %w", method, errNotYet) }

func (b *Backend) Preflight(context.Context, orchestrator.Host, orchestrator.NodeRequest) (model.Node, error) {
	return model.Node{}, notYet("Preflight")
}

func (b *Backend) AddNode(context.Context, orchestrator.Host, model.Node) error {
	return notYet("AddNode")
}

func (b *Backend) RemoveNode(context.Context, string, orchestrator.Host, orchestrator.RemoveOptions) error {
	return notYet("RemoveNode")
}

func (b *Backend) Uninstall(context.Context, orchestrator.Host, model.Node) error {
	return notYet("Uninstall")
}

func (b *Backend) RunConsole(context.Context, string, []netip.Addr) error {
	return notYet("RunConsole")
}

func (b *Backend) ConsoleToken(context.Context) (string, error) { return "", notYet("ConsoleToken") }
