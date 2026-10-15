package filtertemplate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"
)

const (
	variableOpen          = "{{"
	variableClose         = "}}"
	userVariable          = "user"
	clientVariable        = "client"
	claimPrefix           = "claims."
	withoutNamespaceLabel = "without_namespace"
	valueSizeHint         = 64
)

type variableKind uint8

const (
	variableUser variableKind = iota
	variableClient
	variableClaim
)

// variable is a reference to a value of the connection. index is the position of a claim in the
// union of claim paths of the set.
type variable struct {
	name  string
	kind  variableKind
	path  string
	index int
}

// part is a piece of a templated field: literal text, then the variable which follows it, if any.
type part struct {
	text string
	v    *variable
}

// field is a string of a node. A field without variables keeps its text in text. A templated
// field keeps its pieces in parts, and size is a hint for the size of its rendered value.
type field struct {
	text      string
	parts     []part
	templated bool
	size      int
}

// node is a compiled filter node. Op, key and cmp are literal.
type node struct {
	op    string
	key   string
	cmp   string
	val   field
	vals  []field
	nodes []*node
}

// spec is the filter tree as decoded from the configured JSON. Unknown fields are refused.
type spec struct {
	Op    string   `json:"op"`
	Key   string   `json:"key"`
	Cmp   string   `json:"cmp"`
	Val   string   `json:"val"`
	Vals  []string `json:"vals"`
	Nodes []spec   `json:"nodes"`
}

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
		t.root.variables(func(v *variable) {
			if v.kind == variableClaim {
				paths = append(paths, v.path)
			}
		})
	}
	slices.Sort(paths)
	set.claims = slices.Compact(paths)
	for _, t := range set.templates {
		t.set = set
		t.root.variables(func(v *variable) {
			if v.kind == variableClaim {
				v.index, _ = slices.BinarySearch(set.claims, v.path)
			}
		})
	}
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

func compileSource(name string, src Source) (*Template, error) {
	label := sourceLabel(name)
	if len(src.Filter) == 0 {
		if src.OnMissingValue != "" {
			return nil, fmt.Errorf("%s: on_missing_value requires filter", label)
		}
		return nil, nil
	}
	onMissing, err := parseOnMissing(src.OnMissingValue)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	data, err := json.Marshal(src.Filter)
	if err != nil {
		return nil, fmt.Errorf("%s: filter: %w", label, err)
	}
	var sp spec
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sp); err != nil {
		return nil, fmt.Errorf("%s: filter: %w", label, err)
	}
	root, err := compileNode(sp)
	if err != nil {
		return nil, fmt.Errorf("%s: filter: %w", label, err)
	}
	t := &Template{root: root, onMissing: onMissing}
	t.countParts()
	return t, nil
}

func sourceLabel(name string) string {
	if name == "" {
		return withoutNamespaceLabel
	}
	return fmt.Sprintf("namespace %q", name)
}

func parseOnMissing(value string) (OnMissing, error) {
	switch value {
	case "", "reject":
		return OnMissingReject, nil
	case "skip":
		return OnMissingSkip, nil
	}
	return 0, fmt.Errorf("unknown on_missing_value %q", value)
}

func compileNode(sp spec) (*node, error) {
	for _, literal := range []string{sp.Op, sp.Key, sp.Cmp} {
		if strings.Contains(literal, variableOpen) {
			return nil, fmt.Errorf("variables are allowed only in val and vals, got %q", literal)
		}
	}
	n := &node{op: sp.Op, key: sp.Key, cmp: sp.Cmp}
	val, err := parseField(sp.Val)
	if err != nil {
		return nil, err
	}
	n.val = val
	if sp.Vals != nil {
		n.vals = make([]field, len(sp.Vals))
		for i, s := range sp.Vals {
			f, err := parseField(s)
			if err != nil {
				return nil, err
			}
			n.vals[i] = f
		}
	}
	if len(sp.Nodes) > 0 {
		n.nodes = make([]*node, len(sp.Nodes))
		for i, child := range sp.Nodes {
			c, err := compileNode(child)
			if err != nil {
				return nil, err
			}
			n.nodes[i] = c
		}
	}
	return n, nil
}

// parseField splits a val or vals string into literal text and variables.
func parseField(s string) (field, error) {
	if !strings.Contains(s, variableOpen) {
		return field{text: s}, nil
	}
	f := field{templated: true}
	rest := s
	for {
		start := strings.Index(rest, variableOpen)
		if start < 0 {
			f.parts = append(f.parts, part{text: rest})
			f.size += len(rest)
			return f, nil
		}
		end := strings.Index(rest[start+len(variableOpen):], variableClose)
		if end < 0 {
			return field{}, fmt.Errorf("%q without %q in %q", variableOpen, variableClose, s)
		}
		nameEnd := start + len(variableOpen) + end
		v, err := parseVariable(strings.TrimSpace(rest[start+len(variableOpen) : nameEnd]))
		if err != nil {
			return field{}, err
		}
		f.parts = append(f.parts, part{text: rest[:start], v: v})
		f.size += start + valueSizeHint
		rest = rest[nameEnd+len(variableClose):]
	}
}

func parseVariable(name string) (*variable, error) {
	switch {
	case name == userVariable:
		return &variable{name: name, kind: variableUser}, nil
	case name == clientVariable:
		return &variable{name: name, kind: variableClient}, nil
	case strings.HasPrefix(name, claimPrefix):
		path := strings.TrimPrefix(name, claimPrefix)
		if path == "" || strings.ContainsFunc(path, unicode.IsSpace) {
			return nil, fmt.Errorf("invalid claim path in variable %q", name)
		}
		return &variable{name: name, kind: variableClaim, path: path}, nil
	}
	return nil, fmt.Errorf("unknown variable %q", name)
}

// variables calls fn for every variable of the tree.
func (n *node) variables(fn func(v *variable)) {
	n.val.variables(fn)
	for i := range n.vals {
		n.vals[i].variables(fn)
	}
	for _, child := range n.nodes {
		child.variables(fn)
	}
}

func (f *field) variables(fn func(v *variable)) {
	for _, p := range f.parts {
		if p.v != nil {
			fn(p.v)
		}
	}
}

// countParts sets the sizes of the parts of the tree which a render allocates in one piece each.
func (t *Template) countParts() {
	t.root.count(t)
}

func (n *node) count(t *Template) {
	t.nodeCount++
	t.pointerCount += len(n.nodes)
	t.valueCount += len(n.vals)
	for _, child := range n.nodes {
		child.count(t)
	}
}
