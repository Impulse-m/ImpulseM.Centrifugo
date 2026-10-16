package filtertemplate

import (
	"errors"
	"io"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasttemplate"
	"google.golang.org/protobuf/proto"
)

func compileOne(t *testing.T, name string, src Source) *Template {
	t.Helper()
	set, err := CompileSet(map[string]Source{name: src})
	require.NoError(t, err)
	tmpl := set.Template(name)
	require.NotNil(t, tmpl)
	return tmpl
}

func sessionFilter() map[string]any {
	return map[string]any{"key": "session", "cmp": "neq", "val": "{{claims.sid}}"}
}

func tenantFilter() map[string]any {
	return map[string]any{
		"op": "and",
		"nodes": []any{
			map[string]any{"key": "tenant", "cmp": "eq", "val": "{{claims.tenant}}"},
			map[string]any{"key": "audience", "cmp": "in", "vals": []any{"all", "user:{{user}}"}},
		},
	}
}

func TestCompileForms(t *testing.T) {
	tests := []struct {
		name   string
		filter map[string]any
		onMiss string
	}{
		{name: "leaf", filter: sessionFilter()},
		{name: "nested and", filter: tenantFilter()},
		{name: "or and not", filter: map[string]any{
			"op": "or",
			"nodes": []any{
				map[string]any{"op": "not", "nodes": []any{map[string]any{"key": "a", "cmp": "eq", "val": "x"}}},
				map[string]any{"key": "b", "cmp": "eq", "val": "{{client}}"},
			},
		}},
		{name: "literal plus variable", filter: map[string]any{"key": "u", "cmp": "eq", "val": "user:{{user}}"}},
		{name: "whitespace inside braces", filter: map[string]any{"key": "u", "cmp": "eq", "val": "{{ claims.sid }}"}},
		{name: "vals with variable per entry", filter: map[string]any{"key": "u", "cmp": "in", "vals": []any{"{{user}}", "x-{{claims.sid}}"}}},
		{name: "on missing empty", filter: sessionFilter(), onMiss: ""},
		{name: "on missing reject", filter: sessionFilter(), onMiss: "reject"},
		{name: "on missing skip", filter: sessionFilter(), onMiss: "skip"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := compileOne(t, "chat", Source{Filter: tt.filter, OnMissingValue: tt.onMiss})
			require.NotNil(t, tmpl)
		})
	}
}

func TestOnMissingPolicy(t *testing.T) {
	require.Equal(t, OnMissingReject, compileOne(t, "a", Source{Filter: sessionFilter()}).OnMissing())
	require.Equal(t, OnMissingReject, compileOne(t, "a", Source{Filter: sessionFilter(), OnMissingValue: "reject"}).OnMissing())
	require.Equal(t, OnMissingSkip, compileOne(t, "a", Source{Filter: sessionFilter(), OnMissingValue: "skip"}).OnMissing())
}

func TestCompileErrors(t *testing.T) {
	tests := []struct {
		name   string
		source Source
	}{
		{name: "unknown variable", source: Source{Filter: map[string]any{"key": "k", "cmp": "eq", "val": "{{session}}"}}},
		{name: "braces without close", source: Source{Filter: map[string]any{"key": "k", "cmp": "eq", "val": "{{claims.sid"}}},
		{name: "variable in key", source: Source{Filter: map[string]any{"key": "{{user}}", "cmp": "eq", "val": "x"}}},
		{name: "variable in op", source: Source{Filter: map[string]any{"op": "{{user}}", "nodes": []any{}}}},
		{name: "variable in cmp", source: Source{Filter: map[string]any{"key": "k", "cmp": "{{user}}", "val": "x"}}},
		{name: "unknown field", source: Source{Filter: map[string]any{"key": "k", "cmpp": "eq", "val": "x"}}},
		{name: "on missing without filter", source: Source{OnMissingValue: "skip"}},
		{name: "unknown on missing", source: Source{Filter: sessionFilter(), OnMissingValue: "ignore"}},
		{name: "empty claim path", source: Source{Filter: map[string]any{"key": "k", "cmp": "eq", "val": "{{claims.}}"}}},
		{name: "claim path with space", source: Source{Filter: map[string]any{"key": "k", "cmp": "eq", "val": "{{claims.a b}}"}}},
		{name: "vals entry unknown variable", source: Source{Filter: map[string]any{"key": "k", "cmp": "in", "vals": []any{"{{nope}}"}}}},
		{name: "null node", source: Source{Filter: map[string]any{"op": "and", "nodes": []any{nil}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompileSet(map[string]Source{"chat": tt.source})
			require.Error(t, err)
			require.Contains(t, err.Error(), `namespace "chat"`)
		})
	}
}

// TestCompileErrorNamesField checks a compile error names the namespace, the node and the field of the variable.
func TestCompileErrorNamesField(t *testing.T) {
	_, err := CompileSet(map[string]Source{"chat": {Filter: map[string]any{
		"op": "and",
		"nodes": []any{
			map[string]any{"key": "k", "cmp": "eq", "val": "x"},
			map[string]any{"key": "k", "cmp": "in", "vals": []any{"a", "{{claims}}"}},
		},
	}}})
	require.EqualError(t, err, `namespace "chat": filter: nodes[1]: vals[1]: unknown variable "claims"`)
}

func TestEmptyFilterHasNoTemplate(t *testing.T) {
	set, err := CompileSet(map[string]Source{"chat": {}, "other": {Filter: map[string]any{}}})
	require.NoError(t, err)
	require.Nil(t, set.Template("chat"))
	require.Nil(t, set.Template("other"))
	require.Empty(t, set.Claims())
}

func TestSetClaimsUnionAndNamespaces(t *testing.T) {
	set, err := CompileSet(map[string]Source{
		"chat":  {Filter: sessionFilter()},
		"":      {Filter: tenantFilter()},
		"plain": {},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"sid", "tenant"}, set.Claims())
	require.NotNil(t, set.Template(""))
	require.NotNil(t, set.Template("chat"))
	require.Nil(t, set.Template("plain"))
	require.Nil(t, set.Template("missing"))
}

func TestSetClaimsDeduplicatedAcrossTemplates(t *testing.T) {
	set, err := CompileSet(map[string]Source{
		"a": {Filter: map[string]any{"op": "and", "nodes": []any{
			map[string]any{"key": "x", "cmp": "eq", "val": "{{claims.org.tenant}}"},
			map[string]any{"key": "y", "cmp": "eq", "val": "{{claims.sid}}"},
		}}},
		"b": {Filter: map[string]any{"key": "x", "cmp": "eq", "val": "{{claims.sid}}"}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"org.tenant", "sid"}, set.Claims())
}

func TestRenderSubstitutesValues(t *testing.T) {
	tmpl := compileOne(t, "tenant", Source{Filter: tenantFilter(), OnMissingValue: "skip"})
	claims := Claims{Paths: []string{"tenant"}, Values: []string{"acme"}}
	got, err := tmpl.Render(Values{Claims: claims, User: "u1", Client: "c1"})
	require.NoError(t, err)
	want := &protocol.FilterNode{
		Op: "and",
		Nodes: []*protocol.FilterNode{
			{Key: "tenant", Cmp: "eq", Val: "acme"},
			{Key: "audience", Cmp: "in", Vals: []string{"all", "user:u1"}},
		},
	}
	require.True(t, proto.Equal(want, got), "got %v", got)
}

func TestRenderLeavesTemplateUnchanged(t *testing.T) {
	tmpl := compileOne(t, "tenant", Source{Filter: tenantFilter()})
	claims := Claims{Paths: []string{"tenant"}, Values: []string{"acme"}}
	first, err := tmpl.Render(Values{Claims: claims, User: "u1"})
	require.NoError(t, err)
	first.Nodes[0].Val = "mutated"
	first.Nodes[1].Vals[0] = "mutated"
	second, err := tmpl.Render(Values{Claims: claims, User: "u2"})
	require.NoError(t, err)
	require.Equal(t, "acme", second.Nodes[0].Val)
	require.Equal(t, []string{"all", "user:u2"}, second.Nodes[1].Vals)
}

func TestRenderUserAndClient(t *testing.T) {
	tmpl := compileOne(t, "a", Source{Filter: map[string]any{"key": "owner", "cmp": "eq", "val": "{{client}}/{{user}}"}})
	got, err := tmpl.Render(Values{User: "bob", Client: "c-9"})
	require.NoError(t, err)
	require.Equal(t, "c-9/bob", got.Val)
}

func TestPlaceholderRendersEveryVariableAsX(t *testing.T) {
	tmpl := compileOne(t, "tenant", Source{Filter: tenantFilter()})
	want := &protocol.FilterNode{
		Op: "and",
		Nodes: []*protocol.FilterNode{
			{Key: "tenant", Cmp: "eq", Val: "x"},
			{Key: "audience", Cmp: "in", Vals: []string{"all", "user:x"}},
		},
	}
	require.True(t, proto.Equal(want, tmpl.Placeholder()))
	// The placeholder does not change the template: a render still reports its missing values.
	_, err := tmpl.Render(Values{})
	var missing MissingValueError
	require.ErrorAs(t, err, &missing)
	require.Equal(t, "claims.tenant", missing.Variable)
}

func TestRenderMissingValues(t *testing.T) {
	tmpl := compileOne(t, "chat", Source{Filter: sessionFilter()})
	tests := []struct {
		name     string
		values   Values
		variable string
	}{
		{name: "claim absent from values", values: Values{Claims: Claims{Paths: []string{"sid"}}, User: "u"}, variable: "claims.sid"},
		{name: "claim empty", values: Values{Claims: Claims{Paths: []string{"sid"}, Values: []string{""}}, User: "u"}, variable: "claims.sid"},
		{name: "no claims at all", values: Values{User: "u"}, variable: "claims.sid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tmpl.Render(tt.values)
			require.Nil(t, got)
			var missing MissingValueError
			require.True(t, errors.As(err, &missing))
			require.Equal(t, tt.variable, missing.Variable)
			require.EqualError(t, err, `missing value of "claims.sid"`)
		})
	}
}

func TestRenderMissingUser(t *testing.T) {
	tmpl := compileOne(t, "tenant", Source{Filter: tenantFilter()})
	claims := Claims{Paths: []string{"tenant"}, Values: []string{"acme"}}
	_, err := tmpl.Render(Values{Claims: claims})
	var missing MissingValueError
	require.True(t, errors.As(err, &missing))
	require.Equal(t, "user", missing.Variable)
}

func TestRenderMissingClientIsMissing(t *testing.T) {
	tmpl := compileOne(t, "a", Source{Filter: map[string]any{"key": "k", "cmp": "eq", "val": "{{client}}"}})
	_, err := tmpl.Render(Values{User: "u"})
	var missing MissingValueError
	require.True(t, errors.As(err, &missing))
	require.Equal(t, "client", missing.Variable)
}

func TestRenderStalePathsResolveByName(t *testing.T) {
	tmpl := compileOne(t, "chat", Source{Filter: map[string]any{
		"op": "and",
		"nodes": []any{
			map[string]any{"key": "a", "cmp": "eq", "val": "{{claims.org}}"},
			map[string]any{"key": "b", "cmp": "eq", "val": "{{claims.sid}}"},
		},
	}})
	// A config reload changed the union of paths: the connection keeps its old slice, in another order.
	stale := Claims{Paths: []string{"sid", "org"}, Values: []string{"s-1", "acme"}}
	got, err := tmpl.Render(Values{Claims: stale, User: "u"})
	require.NoError(t, err)
	require.Equal(t, "acme", got.Nodes[0].Val)
	require.Equal(t, "s-1", got.Nodes[1].Val)
}

func TestRenderPathAbsentFromStalePathsIsMissing(t *testing.T) {
	tmpl := compileOne(t, "chat", Source{Filter: sessionFilter()})
	// The reload added the sid path: the connection was verified before, so its claims have no sid.
	stale := Claims{Paths: []string{"org"}, Values: []string{"acme"}}
	got, err := tmpl.Render(Values{Claims: stale, User: "u"})
	require.Nil(t, got)
	var missing MissingValueError
	require.True(t, errors.As(err, &missing))
	require.Equal(t, "claims.sid", missing.Variable)
}

func TestAnd(t *testing.T) {
	leaf := &protocol.FilterNode{Key: "k", Cmp: "eq", Val: "v"}
	other := &protocol.FilterNode{Key: "o", Cmp: "eq", Val: "w"}

	require.Nil(t, And(nil, nil))
	require.Same(t, leaf, And(leaf, nil))
	require.Same(t, other, And(nil, other))

	both := And(leaf, other)
	require.Equal(t, "and", both.Op)
	require.Len(t, both.Nodes, 2)
	require.Same(t, leaf, both.Nodes[0])
	require.Same(t, other, both.Nodes[1])
}

func TestRenderAllocationBound(t *testing.T) {
	tmpl := compileOne(t, "tenant", Source{Filter: tenantFilter()})
	claims := Claims{Paths: []string{"tenant"}, Values: []string{"acme"}}
	values := Values{Claims: claims, User: "u1"}
	// The arena of a render is three slices (nodes, child pointers, vals), and one string per templated field: the
	// claim and "user:{{user}}". BenchmarkTemplateRender measures 5 allocations for this template. The race detector
	// adds one, so the bound allows 6 under -race.
	const arenaSlices = 3
	const templatedFields = 2
	const raceAllowance = 1
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := tmpl.Render(values); err != nil {
			panic(err)
		}
	})
	require.LessOrEqual(t, allocs, float64(arenaSlices+templatedFields+raceAllowance))
}

func BenchmarkTemplateRender(b *testing.B) {
	set, err := CompileSet(map[string]Source{"tenant": {Filter: tenantFilter()}})
	if err != nil {
		b.Fatal(err)
	}
	tmpl := set.Template("tenant")
	claims := Claims{Paths: []string{"tenant"}, Values: []string{"acme"}}
	values := Values{Claims: claims, User: "u1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tmpl.Render(values); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFasttemplateBaseline renders the two templated strings of BenchmarkTemplateRender with fasttemplate alone, the
// floor of the render without the filter tree.
func BenchmarkFasttemplateBaseline(b *testing.B) {
	tenant, err := fasttemplate.NewTemplate("{{claims.tenant}}", "{{", "}}")
	if err != nil {
		b.Fatal(err)
	}
	audience, err := fasttemplate.NewTemplate("user:{{user}}", "{{", "}}")
	if err != nil {
		b.Fatal(err)
	}
	tag := func(_ io.Writer, name string) (int, error) {
		if name == "user" || name == "claims.tenant" {
			return 0, nil
		}
		return 0, errors.New("unknown tag " + name)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tenant.ExecuteFuncStringWithErr(tag); err != nil {
			b.Fatal(err)
		}
		if _, err := audience.ExecuteFuncStringWithErr(tag); err != nil {
			b.Fatal(err)
		}
	}
}

func TestClaimsEqualByName(t *testing.T) {
	t.Parallel()
	shared := Claims{Paths: []string{"sid", "org.tenant"}, Values: []string{"s1", "t1"}}
	// A reload reorders the paths: the same values by name are equal.
	reordered := Claims{Paths: []string{"org.tenant", "sid"}, Values: []string{"t1", "s1"}}
	require.True(t, shared.Equal(reordered))
	require.True(t, reordered.Equal(shared))
	require.Equal(t, "t1", reordered.Value("org.tenant"))
	require.Equal(t, "", reordered.Value("missing"))
	// A missing claim and an empty one are the same value.
	require.True(t, Claims{Paths: []string{"sid"}, Values: []string{""}}.Equal(Claims{}))
	require.False(t, shared.Equal(Claims{Paths: []string{"sid"}, Values: []string{"s2"}}))
	require.False(t, Claims{}.Equal(shared))
}
