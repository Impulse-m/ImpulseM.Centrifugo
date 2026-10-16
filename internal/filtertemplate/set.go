package filtertemplate

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

const (
	variableOpen          = "{{"
	variableClose         = "}}"
	userVariable          = "user"
	clientVariable        = "client"
	claimPrefix           = "claims."
	withoutNamespaceLabel = "channel.without_namespace"
)

// Set is the compiled templates of all namespaces of a config. It is immutable and safe for
// concurrent use.
type Set struct {
	claims    []string
	templates map[string]*Template
}

// CompileSet compiles the templates of the namespaces. The key of sources is the namespace name,
// "" for channels without namespace. A source without a filter has no template.
func CompileSet(sources map[string]Source) (*Set, error) {
	set := &Set{templates: make(map[string]*Template)}
	for _, name := range slices.Sorted(maps.Keys(sources)) {
		t, err := compileSource(name, sources[name])
		if err != nil {
			return nil, err
		}
		if t != nil {
			set.templates[name] = t
		}
	}
	var paths []string
	for _, t := range set.templates {
		t.root.variables(func(name string) {
			if path, ok := strings.CutPrefix(name, claimPrefix); ok {
				paths = append(paths, path)
			}
		})
	}
	slices.Sort(paths)
	set.claims = slices.Compact(paths)
	return set, nil
}

// Claims returns the sorted union of the claim paths of all templates of the set. The slice is
// shared and must not be modified.
func (s *Set) Claims() []string {
	return s.claims
}

// Template returns the template of a namespace, or nil when the namespace has none.
func (s *Set) Template(name string) *Template {
	return s.templates[name]
}

// Label names the channels of a namespace in an error message: the namespace, or channel.without_namespace for "".
// The config errors of the templates use it too, so one spelling reaches the operator.
func Label(name string) string {
	if name == "" {
		return withoutNamespaceLabel
	}
	return fmt.Sprintf("namespace %q", name)
}
