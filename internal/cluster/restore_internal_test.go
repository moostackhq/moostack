package cluster

import (
	"errors"
	"strings"
	"testing"
)

func TestANodeFileThatCantBePutBackKeepsItsIdentityListed(t *testing.T) {
	restored := false
	err := restoreNodes([]string{"web-1", "web-2"}, func(name string) error {
		if name == "web-2" {
			return errors.New("disk full")
		}
		return nil
	}, func() { restored = true })
	if err == nil || !strings.Contains(err.Error(), "web-2") {
		t.Errorf("restoreNodes: %v, want an error naming web-2", err)
	}
	if restored {
		t.Error("the list was put back, so web-2 is gone and its identity unlisted")
	}
}

func TestNodeFilesPutBackRestoreTheList(t *testing.T) {
	var made []string
	restored := false
	err := restoreNodes([]string{"web-1", "web-2"}, func(name string) error {
		made = append(made, name)
		return nil
	}, func() { restored = true })
	if err != nil || !restored || len(made) != 2 {
		t.Errorf("restoreNodes: %v, restored %v, made %v", err, restored, made)
	}
}
