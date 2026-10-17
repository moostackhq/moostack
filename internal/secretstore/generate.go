package secretstore

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
)

// defaultLength is the length a generator uses when none is set: the
// characters of a password, or the random bytes of hex and base64.
const defaultLength = 32

// defaultCharset is what a password is made of when no charset is set.
const defaultCharset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// generator is a type of generated value.
type generator struct {
	name string
	// length says whether the type takes a length, and charset whether
	// it takes a charset.
	length, charset bool
	// generate generates a value of length n, the default if none is
	// set, with the settings g.
	generate func(n int, g Generate) (string, error)
}

// generators are the types of generated value.
var generators = []generator{
	{name: "password", length: true, charset: true, generate: func(n int, g Generate) (string, error) {
		chars := charset(defaultCharset)
		if g.Charset != "" {
			chars = charset(g.Charset)
		}
		return password(n, chars)
	}},
	{name: "hex", length: true, generate: func(n int, _ Generate) (string, error) {
		return hex.EncodeToString(randomBytes(n)), nil
	}},
	{name: "base64", length: true, generate: func(n int, _ Generate) (string, error) {
		return base64.StdEncoding.EncodeToString(randomBytes(n)), nil
	}},
	{name: "uuid", generate: func(int, Generate) (string, error) {
		b := randomBytes(16)
		b[6] = b[6]&0x0f | 0x40
		b[8] = b[8]&0x3f | 0x80
		return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
	}},
}

// GeneratorTypes are the types a value can be generated as, for a
// message: "password, hex, base64 or uuid".
func GeneratorTypes() string {
	var names []string
	for _, g := range generators {
		names = append(names, g.name)
	}
	return strings.Join(names[:len(names)-1], ", ") + " or " + names[len(names)-1]
}

// Check checks that the settings can generate a value. The types are
// password, which takes a length and a charset, hex and base64, which
// take a length in random bytes, and uuid, which takes neither.
func (g Generate) Check() error {
	_, err := g.generator()
	return err
}

// generator is the type the settings generate, once they are checked.
func (g Generate) generator() (generator, error) {
	if g.Type == "" {
		return generator{}, errors.New("generate needs a type: " + GeneratorTypes())
	}
	i := slices.IndexFunc(generators, func(gen generator) bool { return gen.name == g.Type })
	if i < 0 {
		return generator{}, fmt.Errorf("unknown generator type %q; use %s", g.Type, GeneratorTypes())
	}
	gen := generators[i]
	switch {
	case g.Length < 0:
		return generator{}, errors.New("generate: length can't be negative")
	case g.Length != 0 && !gen.length:
		return generator{}, fmt.Errorf("generate: a %s takes no length", g.Type)
	case g.Charset != "" && !gen.charset:
		return generator{}, fmt.Errorf("generate: charset works only with type password, not %s", g.Type)
	case g.Charset != "" && len(charset(g.Charset)) < 2:
		return generator{}, errors.New("generate: charset needs at least 2 different characters")
	}
	return gen, nil
}

// NewValue generates a value with the settings.
func (g Generate) NewValue() (string, error) {
	gen, err := g.generator()
	if err != nil {
		return "", err
	}
	n := g.Length
	if n == 0 {
		n = defaultLength
	}
	return gen.generate(n, g)
}

// password is n characters chosen at random from chars.
func password(n int, chars []rune) (string, error) {
	count := big.NewInt(int64(len(chars)))
	p := make([]rune, n)
	for i := range p {
		j, err := rand.Int(rand.Reader, count)
		if err != nil {
			return "", err
		}
		p[i] = chars[j.Int64()]
	}
	return string(p), nil
}

// charset is the different characters in s, in order.
func charset(s string) []rune {
	var chars []rune
	for _, r := range s {
		if !slices.Contains(chars, r) {
			chars = append(chars, r)
		}
	}
	return chars
}

// randomBytes is n random bytes. crypto/rand.Read never fails.
func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
