package app

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/configtypes"

	"github.com/stretchr/testify/require"
)

// markProxies sets a probe name in the emulated_headers of every configtypes.Proxy under v, found by walking its struct
// fields, and records the probes. path is the field path of v, which keeps the probe names apart.
func markProxies(v reflect.Value, path string, probes *[]string) {
	if v.Type() == reflect.TypeOf(configtypes.Proxy{}) {
		name := strings.ToLower("probe" + path)
		v.FieldByName("EmulatedHeaders").Set(reflect.ValueOf([]string{name}))
		*probes = append(*probes, name)
		return
	}
	if v.Kind() != reflect.Struct {
		return
	}
	for i := range v.NumField() {
		field := v.Type().Field(i)
		if field.IsExported() {
			markProxies(v.Field(i), path+"."+field.Name, probes)
		}
	}
}

// emulatedHeaderNames must list the emulated_headers of every proxy of the config, so that no proxy field added by an
// upstream sync drops its connect headers. The test marks every proxy field, and every named proxy, with a probe name.
func TestEmulatedHeaderNamesListEveryProxy(t *testing.T) {
	var cfg config.Config
	var probes []string
	markProxies(reflect.ValueOf(&cfg).Elem(), "", &probes)

	cfg.Proxies = configtypes.NamedProxies{{Name: "first"}, {Name: "second"}}
	for i := range cfg.Proxies {
		markProxies(reflect.ValueOf(&cfg.Proxies[i].Proxy).Elem(), fmt.Sprintf(".proxies[%d]", i), &probes)
	}

	require.Greater(t, len(probes), 10, "the walk finds the proxies of the config")
	names := emulatedHeaderNames(cfg)
	for _, probe := range probes {
		require.Contains(t, names, probe, "the proxy with the probe is read by emulatedHeaderNames")
	}
}
