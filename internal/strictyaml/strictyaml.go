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
func Unmarshal(data []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
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
