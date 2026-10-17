package secretedit

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/secretstore"
)

// header starts the document that Edit opens.
var header = `# Secrets, by name. For example:
#
# db_password:
#   value: ""                                 # generated on save if empty and generate is set
#   generate: {type: password, length: 32}   # ` + secretstore.GeneratorTypes() + `
#                                             # a password also takes a charset: "abc123!@#"
#   rotate: true                              # marked for rotation
#
`

// namedSecret is a secret and its name.
type namedSecret struct {
	name   string
	secret secretstore.Secret
}

// storedSecret is a secret in the store. err is why its value can't be
// decrypted, if it can't; secret then has only its settings.
type storedSecret struct {
	namedSecret
	err error
}

// readSecrets reads every secret in s. For one that can't be decrypted,
// it keeps the error and the settings the file has.
func readSecrets(s *secretstore.Store) ([]storedSecret, error) {
	var secrets []storedSecret
	for _, name := range s.Names() {
		secret, err := s.Get(name)
		if err != nil {
			generate, rotate, serr := s.Settings(name)
			if serr != nil {
				return nil, serr
			}
			secret = secretstore.Secret{Generate: generate, Rotate: rotate}
		}
		secrets = append(secrets, storedSecret{namedSecret{name, secret}, err})
	}
	return secrets, nil
}

// render writes secrets as a YAML document. A secret that can't be
// decrypted is shown with its settings and an empty value, under a
// comment that says why and what to do about it.
func render(secrets []storedSecret) ([]byte, error) {
	if len(secrets) == 0 {
		return nil, nil
	}
	doc := &yaml.Node{Kind: yaml.MappingNode}
	for _, s := range secrets {
		key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s.name}
		value := &yaml.Node{}
		if err := value.Encode(s.secret); err != nil {
			return nil, err
		}
		for _, n := range value.Content {
			if n.Kind == yaml.MappingNode {
				n.Style = yaml.FlowStyle
			}
		}
		if s.err != nil {
			key.HeadComment = fmt.Sprintf("%v.\nDelete it to remove it, or give it a value to set it again. Left as it is, it is kept.", s.err)
		}
		doc.Content = append(doc.Content, key, value)
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// parse reads a document that render wrote and the operator edited. It
// refuses a second YAML document with anything in it, whose secrets
// would otherwise be ignored and so deleted; an empty one, as a trailing
// --- makes, is fine. It also refuses names that aren't plain strings,
// unknown fields, duplicate names and generate settings that can't
// generate a value.
func parse(data []byte) ([]namedSecret, error) {
	var docs []*yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var n yaml.Node
		err := dec.Decode(&n)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		docs = append(docs, &n)
	}
	for _, d := range docs[min(1, len(docs)):] {
		if !empty(d) {
			return nil, fmt.Errorf("line %d: the secrets must be one YAML document; remove the --- that starts another", d.Line)
		}
	}
	if len(docs) == 0 || empty(docs[0]) {
		return nil, nil
	}
	doc := docs[0].Content[0]
	if doc.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("line %d: the secrets must be a mapping from each name to its fields", doc.Line)
	}
	for i := 0; i < len(doc.Content); i += 2 {
		key := doc.Content[i]
		if key.Kind != yaml.ScalarNode {
			return nil, fmt.Errorf("line %d: a secret's name must be a plain name, not a list or mapping", key.Line)
		}
		if key.Tag != "!!str" {
			return nil, fmt.Errorf("line %d: %q isn't a plain name; put it in quotes", key.Line, key.Value)
		}
		if key.Value == "" {
			return nil, fmt.Errorf("line %d: a secret has no name", key.Line)
		}
	}
	// Decoding into a map checks fields and duplicates; the node keeps
	// the order.
	var byName map[string]*secretstore.Secret
	strict := yaml.NewDecoder(bytes.NewReader(data))
	strict.KnownFields(true)
	if err := strict.Decode(&byName); err != nil {
		return nil, err
	}
	var secrets []namedSecret
	for i := 0; i < len(doc.Content); i += 2 {
		key := doc.Content[i]
		s := byName[key.Value]
		if s == nil {
			s = &secretstore.Secret{}
		}
		if s.Generate != nil {
			if err := s.Generate.Check(); err != nil {
				return nil, fmt.Errorf("line %d: secret %q: %w", key.Line, key.Value, err)
			}
		}
		secrets = append(secrets, namedSecret{key.Value, *s})
	}
	return secrets, nil
}

// empty reports whether the YAML document d holds nothing.
func empty(d *yaml.Node) bool {
	if len(d.Content) == 0 {
		return true
	}
	n := d.Content[0]
	return n.Kind == yaml.ScalarNode && n.Tag == "!!null"
}

// change is a secret to put, with its value generated first if generate
// is set.
type change struct {
	namedSecret
	generate bool
}

// plan is what an edit changes in the store.
type plan struct {
	deletes []string
	puts    []change
}

// planEdit works out what turning stored into edited changes.
//
// A secret with a value keeps it, whatever its generate settings say. A
// secret whose value is empty gets one generated from its generate
// settings, and without them is refused.
//
// A secret that couldn't be decrypted is shown with an empty value. Left
// as it was shown, it is kept. With a value, it is set again with the
// settings given. Its settings can't change without a value, and it is
// never generated.
//
// An edit that leaves no secret that can be read is refused, so that an
// emptied document can't wipe the store, nor a deleted cluster CA key go
// unnoticed among broken secrets. If none could be read before, removing
// them all is allowed: nothing readable is lost, and it is how the last
// broken secret is removed.
func planEdit(stored []storedSecret, edited []namedSecret) (plan, error) {
	var p plan
	// left counts the secrets that can be read after the edit.
	left := 0
	for _, s := range stored {
		if !slices.ContainsFunc(edited, func(e namedSecret) bool { return e.name == s.name }) {
			p.deletes = append(p.deletes, s.name)
		}
	}
	for _, e := range edited {
		i := slices.IndexFunc(stored, func(s storedSecret) bool { return s.name == e.name })
		secret := e.secret
		if i >= 0 {
			old := stored[i]
			if old.err != nil && secret.Value == "" {
				if !old.secret.SameSettings(secret) {
					return plan{}, fmt.Errorf("secret %q can't be decrypted, so its settings can't change without a value; give it one, or put its settings back", e.name)
				}
				continue
			}
			if old.err == nil && old.secret.Equal(secret) {
				left++
				continue
			}
		}
		left++
		if secret.Value == "" && secret.Generate == nil {
			return plan{}, fmt.Errorf("secret %q needs a value or generate settings", e.name)
		}
		p.puts = append(p.puts, change{namedSecret{e.name, secret}, secret.Value == ""})
	}
	readable := slices.ContainsFunc(stored, func(s storedSecret) bool { return s.err == nil })
	if readable && left == 0 {
		return plan{}, errors.New("the edit leaves no secret that can be read, which is refused; keep at least one")
	}
	return p, nil
}

// empty reports whether the plan changes nothing.
func (p plan) empty() bool {
	return len(p.deletes) == 0 && len(p.puts) == 0
}

// apply changes s as planned, generating values first, and returns the
// names of the secrets it generated.
func (p plan) apply(s *secretstore.Store) ([]string, error) {
	var generated []string
	for i, c := range p.puts {
		if !c.generate {
			continue
		}
		value, err := c.secret.Generate.NewValue()
		if err != nil {
			return nil, fmt.Errorf("secret %q: %w", c.name, err)
		}
		p.puts[i].secret.Value = value
		generated = append(generated, c.name)
	}
	for _, name := range p.deletes {
		if err := s.Delete(name); err != nil {
			return nil, err
		}
	}
	for _, c := range p.puts {
		if err := s.Put(c.name, c.secret); err != nil {
			return nil, err
		}
	}
	return generated, nil
}
