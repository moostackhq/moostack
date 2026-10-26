package nodev1

import (
	"math"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/distribution/reference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// containerName is what a container may be called: its name names its
// containerd container, snapshot, socket directory and log, and
// containerd takes no hyphen at its end or next to another. Its length
// is checked apart.
var containerName = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// apiName is the form of an API's name, as gRPC names a service.
var apiName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)

// The bounds of a container's image and API names.
const (
	maxImage = 1024
	maxAPI   = 255
)

// LinuxCapabilities are the capabilities Linux has, by the names runc
// takes, which drops a name it doesn't know.
var LinuxCapabilities = []string{
	"CAP_AUDIT_CONTROL", "CAP_AUDIT_READ", "CAP_AUDIT_WRITE", "CAP_BLOCK_SUSPEND", "CAP_BPF",
	"CAP_CHECKPOINT_RESTORE", "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_DAC_READ_SEARCH", "CAP_FOWNER",
	"CAP_FSETID", "CAP_IPC_LOCK", "CAP_IPC_OWNER", "CAP_KILL", "CAP_LEASE",
	"CAP_LINUX_IMMUTABLE", "CAP_MAC_ADMIN", "CAP_MAC_OVERRIDE", "CAP_MKNOD", "CAP_NET_ADMIN",
	"CAP_NET_BIND_SERVICE", "CAP_NET_BROADCAST", "CAP_NET_RAW", "CAP_PERFMON", "CAP_SETFCAP",
	"CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYSLOG", "CAP_SYS_ADMIN",
	"CAP_SYS_BOOT", "CAP_SYS_CHROOT", "CAP_SYS_MODULE", "CAP_SYS_NICE", "CAP_SYS_PACCT",
	"CAP_SYS_PTRACE", "CAP_SYS_RAWIO", "CAP_SYS_RESOURCE", "CAP_SYS_TIME", "CAP_SYS_TTY_CONFIG",
	"CAP_WAKE_ALARM",
}

// CheckName fails with INVALID_ARGUMENT unless name may name a
// container: at most 63 bytes, of lowercase letters, digits and single
// hyphens between them, starting with a letter.
func CheckName(name string) error {
	if len(name) > 63 || !containerName.MatchString(name) {
		return status.Errorf(codes.InvalidArgument, "a container's name is of lowercase letters, digits and single hyphens between them, starting with a letter, at most 63, not %q", name)
	}
	return nil
}

// CheckContainer fails with INVALID_ARGUMENT unless Run runs c: a name
// CheckName takes, an image's name, a uid and gid if any, capabilities
// Linux has, each once, and an API as gRPC names one if any, each within
// its bounds. The agent refuses what it fails before it changes
// anything, and a caller can check a container before it removes the
// one the container replaces.
func CheckContainer(c *Container) error {
	if err := CheckName(c.Name); err != nil {
		return err
	}
	switch {
	case c.Image == "" || len(c.Image) > maxImage || !utf8.ValidString(c.Image) || strings.ContainsFunc(c.Image, func(r rune) bool { return !unicode.IsPrint(r) || unicode.IsSpace(r) }):
		return status.Errorf(codes.InvalidArgument, "the container %s needs the name of an image, at most %d bytes, not %q", c.Name, maxImage, c.Image)
	case c.Api != "" && (len(c.Api) > maxAPI || !apiName.MatchString(c.Api)):
		return status.Errorf(codes.InvalidArgument, "the container %s's API is a gRPC service name, at most %d bytes, not %q", c.Name, maxAPI, c.Api)
	}
	if c.User != nil {
		user, group, hasGroup := strings.Cut(*c.User, ":")
		if !idPart(user) || (hasGroup && !idPart(group)) {
			return status.Errorf(codes.InvalidArgument, "the container %s's user is a uid, with a gid after a colon, each at most 2147483647, not %q", c.Name, *c.User)
		}
	}
	if c.Capabilities != nil {
		names := c.Capabilities.Names
		for i, name := range names {
			if !slices.Contains(LinuxCapabilities, name) || slices.Contains(names[:i], name) {
				return status.Errorf(codes.InvalidArgument, "the container %s's capabilities are Linux's, such as CAP_NET_BIND_SERVICE, each once, not %q", c.Name, names)
			}
		}
	}
	return nil
}

// idPart is whether part of a user is a uid or a gid runc takes: a
// number of at most 2^31-1. A name would be looked up in the image only
// once the container it replaces is gone, so Run takes none.
func idPart(part string) bool {
	if part == "" || len(part) > 10 || strings.Trim(part, "0123456789") != "" {
		return false
	}
	n, err := strconv.ParseUint(part, 10, 31)
	return err == nil && n <= math.MaxInt32
}

// NormalizeReference is ref, an image's reference, normalized as Docker
// normalizes one: with its registry, its repository's path and a tag,
// latest if it has neither a tag nor a digest, so that nginx:1.27 is
// docker.io/library/nginx:1.27. A reference with both a tag and a digest
// keeps the digest alone. It fails with INVALID_ARGUMENT for a reference
// that doesn't parse, whose registry isn't a host as a URL names one,
// which containerd's resolver refuses, or that is longer than an image's
// name may be once normalized.
func NormalizeReference(ref string) (string, error) {
	named, err := reference.ParseDockerRef(ref)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "%q isn't an image's reference: %v", ref, err)
	}
	if _, err := url.Parse("https://" + reference.Domain(named)); err != nil {
		return "", status.Errorf(codes.InvalidArgument, "%q isn't an image's reference: its registry %q isn't a host: %v", ref, reference.Domain(named), err)
	}
	normalized := named.String()
	if len(normalized) > maxImage {
		return "", status.Errorf(codes.InvalidArgument, "the image's reference %q is longer than %d bytes once normalized", ref, maxImage)
	}
	return normalized, nil
}

// CheckReference fails with INVALID_ARGUMENT unless ref is a reference
// as NormalizeReference makes it, so that the name a node stores and
// reports is always the full one.
func CheckReference(ref string) error {
	normalized, err := NormalizeReference(ref)
	if err != nil {
		return err
	}
	if normalized != ref {
		return status.Errorf(codes.InvalidArgument, "the image's reference %q isn't normalized: it is %q", ref, normalized)
	}
	return nil
}
