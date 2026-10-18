package strictyaml_test

import (
	"strings"
	"testing"

	"github.com/moostackhq/moostack/internal/strictyaml"
)

type file struct {
	Name string `yaml:"name"`
}

func TestUnmarshalReadsOneDocument(t *testing.T) {
	var f file
	if err := strictyaml.Unmarshal([]byte("name: a\n"), &f); err != nil || f.Name != "a" {
		t.Errorf("Unmarshal: %+v, %v", f, err)
	}
}

func TestUnmarshalRefusesUnknownFields(t *testing.T) {
	var f file
	if err := strictyaml.Unmarshal([]byte("name: a\nextra: b\n"), &f); err == nil || !strings.Contains(err.Error(), "extra") {
		t.Errorf("Unmarshal of an unknown field: %v", err)
	}
}

func TestUnmarshalRefusesASecondDocument(t *testing.T) {
	for _, data := range []string{"name: a\n---\nname: b\n", "name: a\n---\n---\nname: b\n"} {
		var f file
		if err := strictyaml.Unmarshal([]byte(data), &f); err == nil {
			t.Errorf("Unmarshal accepted\n%s", data)
		}
	}
}

func TestUnmarshalRefusesEmptyData(t *testing.T) {
	for _, data := range []string{"", "\n", "# only a comment\n"} {
		var f file
		if err := strictyaml.Unmarshal([]byte(data), &f); err == nil || err.Error() != "the file is empty" {
			t.Errorf("Unmarshal of %q: %v, want the file is empty", data, err)
		}
	}
}
