package secretstore

import (
	"errors"
	"fmt"
	"slices"
)

// ErrNoGenerate is the error for rotating a secret without generate
// settings.
var ErrNoGenerate = errors.New("it has no generate settings, so it can't be rotated; change it by hand and remove its rotate mark")

// Rotate generates new values for the secrets called names, or with no
// names for every secret marked rotate, and clears their mark. With no
// names, it also returns the secrets marked rotate that have no generate
// settings, which must be changed by hand. A named secret without
// generate settings is refused, and nothing is rotated.
//
// Only the secrets it rotates must be readable, so a value it can't
// decrypt elsewhere doesn't stop it.
func (s *Store) Rotate(names ...string) (rotated, byHand []string, err error) {
	if len(names) == 0 {
		for _, e := range s.secrets {
			switch {
			case !e.Rotate:
			case e.Generate == nil:
				byHand = append(byHand, e.Name)
			default:
				names = append(names, e.Name)
			}
		}
	}
	type target struct {
		name   string
		secret Secret
	}
	var targets []target
	for _, name := range names {
		if slices.ContainsFunc(targets, func(t target) bool { return t.name == name }) {
			continue
		}
		// Get checks the settings, which a new value is generated with.
		secret, err := s.Get(name)
		if err != nil {
			return nil, nil, err
		}
		if secret.Generate == nil {
			return nil, nil, fmt.Errorf("secret %q: %w", name, ErrNoGenerate)
		}
		targets = append(targets, target{name, secret})
	}
	// Every value is generated before any is put, so that a failure
	// leaves the store as it was.
	for i := range targets {
		value, err := targets[i].secret.Generate.NewValue()
		if err != nil {
			return nil, nil, fmt.Errorf("secret %q: %w", targets[i].name, err)
		}
		targets[i].secret.Value, targets[i].secret.Rotate = value, false
	}
	for _, t := range targets {
		if err := s.Put(t.name, t.secret); err != nil {
			return nil, nil, err
		}
		rotated = append(rotated, t.name)
	}
	return rotated, byHand, nil
}

// MarkedForRotation are the names of the secrets marked rotate, as the
// file has them.
func (s *Store) MarkedForRotation() []string {
	var marked []string
	for _, e := range s.secrets {
		if e.Rotate {
			marked = append(marked, e.Name)
		}
	}
	return marked
}
