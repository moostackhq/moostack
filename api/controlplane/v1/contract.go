package controlplanev1

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
)

// The protocol names, by TLS application-layer protocol negotiation
// (ALPN), of a node's connections to the control plane, over TLS 1.3.
// Under ProtocolJoin, the node calls ControlPlane over the connection.
// Under ProtocolAgent and ProtocolUpdater, the node's agent or updater
// proves the node identity, and serves its node API over the connection,
// as an HTTP/2 server of gRPC: the control plane is the client. The
// control plane must start HTTP/2 within 10 s of accepting the
// connection, as a client that connects at once does, rather than on its
// first call, or the node closes the connection and dials again. It may
// ping as often as every 5 s, and must answer the node's pings, which
// come after 20 s idle, within 10 s.
const (
	ProtocolJoin    = "moostack-join"
	ProtocolAgent   = "moostack-agent"
	ProtocolUpdater = "moostack-updater"
)

// tokenPrefix starts every join token.
const tokenPrefix = "moostack-join:"

// EncodeToken is tok as an operator passes it: "moostack-join:" and the
// protobuf of tok in base64url, unpadded. It fails for a field that
// isn't UTF-8, which protobuf can't hold.
func EncodeToken(tok *JoinToken) (string, error) {
	data, err := proto.Marshal(tok)
	if err != nil {
		return "", fmt.Errorf("encoding the join token: %w", err)
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

// fingerprintPattern is a CA fingerprint's shape.
var fingerprintPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// CheckAddress checks that address is a control plane's: host:port, with
// a host of UTF-8 without spaces or control characters, and a port from 1 to 65535
// by number.
func CheckAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if host == "" {
		return errors.New("no host")
	}
	if !utf8.ValidString(host) {
		return errors.New("the host isn't UTF-8")
	}
	if strings.IndexFunc(host, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("the host %q has a space or a control character", host)
	}
	if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
		return fmt.Errorf("bad port %q", port)
	}
	return nil
}

// ParseToken reads a join token that EncodeToken wrote, and refuses one
// without an address of a host and a port, a fingerprint of its shape,
// or a secret. Fields it doesn't know, of a later release's token, are
// ignored.
func ParseToken(token string) (*JoinToken, error) {
	encoded, ok := strings.CutPrefix(token, tokenPrefix)
	if !ok {
		return nil, fmt.Errorf("a join token starts with %s", tokenPrefix)
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("the join token isn't base64url, unpadded: %w", err)
	}
	tok := &JoinToken{}
	if err := proto.Unmarshal(data, tok); err != nil {
		return nil, fmt.Errorf("the join token: %w", err)
	}
	if err := CheckAddress(tok.Address); err != nil {
		return nil, fmt.Errorf("the join token's address %q: %w", tok.Address, err)
	}
	if !fingerprintPattern.MatchString(tok.CaFingerprint) {
		return nil, fmt.Errorf("the join token's CA fingerprint %q isn't sha256: and 64 lower-case hex digits", tok.CaFingerprint)
	}
	if tok.Secret == "" {
		return nil, errors.New("the join token has no secret")
	}
	return tok, nil
}
