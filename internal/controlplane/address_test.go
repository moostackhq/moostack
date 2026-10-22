package controlplane_test

import (
	"encoding/base64"
	"net/netip"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	controlplanev1 "github.com/moostackhq/moostack/api/controlplane/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
)

// A token of a later release, with a field this one doesn't know, is
// read, as ParseToken says.
func TestATokenWithAFieldOfALaterReleaseIsRead(t *testing.T) {
	tok := &controlplanev1.JoinToken{Address: "cp.example.com:7444", CaFingerprint: "sha256:" + strings.Repeat("ab", 32), Secret: "s"}
	token := encodeToken(t, tok)
	data, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "moostack-join:"))
	data = protowire.AppendTag(data, 9, protowire.BytesType)
	data = protowire.AppendString(data, "a newer release's field")
	got, err := controlplanev1.ParseToken("moostack-join:" + base64.RawURLEncoding.EncodeToString(data))
	if err != nil || got.Address != tok.Address || got.Secret != tok.Secret {
		t.Errorf("ParseToken: %v, %v, want the token's fields", got, err)
	}
}

// The node file and the token judge a control plane's address alike.
func TestTheNodeFileAndTheTokenAgreeOnAnAddress(t *testing.T) {
	for _, addr := range []string{"cp.example.com:7444", "[2001:db8::1]:443", "cp.example.com:0", "cp.example.com:http", "cp.example.com:99999", ":7444", "cp.example.com"} {
		_, nodeErr := hostlayout.ParseNodeConfig([]byte("cluster: c\nnode: n\nmode: control_plane\ncontrol_plane: \"" + addr + "\"\nprivate_address: 10.0.0.1\n"))
		tokenErr := func() error {
			_, err := controlplanev1.ParseToken(encodeToken(t, &controlplanev1.JoinToken{Address: addr, CaFingerprint: "sha256:" + strings.Repeat("ab", 32), Secret: "s"}))
			return err
		}()
		if (nodeErr == nil) != (tokenErr == nil) {
			t.Errorf("%s: node file %v, token %v", addr, nodeErr, tokenErr)
		}
	}
}

// Any address the token takes, the node file takes and reads back the
// same, and the reverse.
func FuzzTheTokenAndTheNodeFileAgreeOnAnAddress(f *testing.F) {
	for _, a := range []string{"cp.example.com:7444", "[2001:db8::1]:443", "\n:1", "cp.example.com:0", ":7444", "a b:1", "cp:http"} {
		f.Add(a)
	}
	f.Fuzz(func(t *testing.T, addr string) {
		// A token that can't be written takes the address no more than one
		// that can't be read.
		token, tokenErr := controlplanev1.EncodeToken(&controlplanev1.JoinToken{Address: addr, CaFingerprint: "sha256:" + strings.Repeat("ab", 32), Secret: "s"})
		if tokenErr == nil {
			_, tokenErr = controlplanev1.ParseToken(token)
		}
		// A node file that can't be written takes the address no more than
		// one that can't be read.
		var node hostlayout.NodeConfig
		data, nodeErr := hostlayout.NodeConfig{Cluster: "c", Node: "n", Mode: hostlayout.ModeControlPlane, ControlPlane: addr, PrivateAddress: netip.MustParseAddr("10.0.0.1")}.Marshal()
		if nodeErr == nil {
			node, nodeErr = hostlayout.ParseNodeConfig(data)
		}
		if (tokenErr == nil) != (nodeErr == nil) {
			t.Fatalf("%q: the token says %v, the node file %v", addr, tokenErr, nodeErr)
		}
		if nodeErr == nil && node.ControlPlane != addr {
			t.Errorf("%q reads back from the node file as %q", addr, node.ControlPlane)
		}
	})
}

// encodeToken is tok encoded, failing t if it can't be.
func encodeToken(t testing.TB, tok *controlplanev1.JoinToken) string {
	t.Helper()
	token, err := controlplanev1.EncodeToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
