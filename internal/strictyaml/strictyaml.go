// Package strictyaml decodes YAML strictly: one document, with no field
// that the value decoded into doesn't have.
package strictyaml

import (
	"bytes"
	"errors"
	"io"

	"gopkg.in/yaml.v3"
)

// Unmarshal decodes data, which must be a single YAML document, into v,
// refusing fields that v doesn't have. Empty data is refused too.
func Unmarshal(data []byte, v any) error { return unmarshal(data, v, true) }

// UnmarshalNewer is Unmarshal, but ignores fields that v doesn't have, as
// a newer writer may add.
func UnmarshalNewer(data []byte, v any) error { return unmarshal(data, v, false) }

// unmarshal is Unmarshal, refusing unknown fields only if known.
func unmarshal(data []byte, v any, known bool) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(known)
	if err := dec.Decode(v); errors.Is(err, io.EOF) {
		return errors.New("the file is empty")
	} else if err != nil {
		return err
	}
	var more yaml.Node
	if err := dec.Decode(&more); !errors.Is(err, io.EOF) {
		return errors.New("more than one YAML document")
	}
	return nil
}
