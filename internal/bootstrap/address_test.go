package bootstrap_test

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/bootstrap"
)

func TestPrivateAddress(t *testing.T) {
	for name, c := range map[string]struct {
		addrs []string
		// hosts are the address typed and the one SSH resolved.
		hosts []string
		want  string
		// fails is in the error, when there is no address to choose.
		fails string
	}{
		"the SSH host, among several":      {[]string{"10.0.2.15", "10.0.1.2"}, []string{"10.0.1.2"}, "10.0.1.2", ""},
		"the SSH host, mapped to IPv6":     {[]string{"10.0.2.15", "10.0.1.2"}, []string{"::ffff:10.0.1.2"}, "10.0.1.2", ""},
		"the SSH host, over IPv6":          {[]string{"10.0.1.2", "fd00::2"}, []string{"fd00::2"}, "fd00::2", ""},
		"the only one, behind NAT":         {[]string{"10.0.1.2"}, []string{"203.0.113.7"}, "10.0.1.2", ""},
		"the only one, by DNS name":        {[]string{"10.0.1.2"}, []string{"web1.example.com"}, "10.0.1.2", ""},
		"several, none the SSH host":       {[]string{"10.0.2.15", "10.0.1.2"}, []string{"127.0.0.1"}, "", "the addresses 10.0.2.15, 10.0.1.2, so choose"},
		"several, the SSH host by name":    {[]string{"10.0.2.15", "10.0.1.2"}, []string{"web1"}, "", "--private-address"},
		"typed, though SSH goes elsewhere": {[]string{"10.0.2.15", "10.0.1.2"}, []string{"10.0.1.2", "127.0.0.1"}, "10.0.1.2", ""},
		"resolved, from an alias":          {[]string{"10.0.2.15", "10.0.1.2"}, []string{"web1", "10.0.1.2"}, "10.0.1.2", ""},
		"typed, before the resolved one":   {[]string{"10.0.2.15", "10.0.1.2"}, []string{"10.0.1.2", "10.0.2.15"}, "10.0.1.2", ""},
		"neither among several":            {[]string{"10.0.2.15", "10.0.1.2"}, []string{"web1", "127.0.0.1"}, "", "--private-address"},
		"none":                             {nil, []string{"10.0.1.2"}, "", "no address"},
	} {
		t.Run(name, func(t *testing.T) {
			var addrs []netip.Addr
			for _, a := range c.addrs {
				addrs = append(addrs, netip.MustParseAddr(a))
			}
			got, err := bootstrap.PrivateAddress(addrs, c.hosts...)
			if c.fails != "" {
				if err == nil || !strings.Contains(err.Error(), c.fails) {
					t.Errorf("PrivateAddress: %v, %v, want an error with %q", got, err, c.fails)
				}
				return
			}
			if err != nil || got.String() != c.want {
				t.Errorf("PrivateAddress: %v, %v, want %s", got, err, c.want)
			}
		})
	}
}
