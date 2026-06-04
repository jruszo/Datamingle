package monitoring

import (
	"reflect"
	"testing"
)

func TestNodeExporterArgsUsesSelectedCollectors(t *testing.T) {
	args := nodeExporterArgs(nodeExporterConfig{
		ListenAddress:        "127.0.0.1:9100",
		CollectorsConfigured: true,
		Collectors:           []string{"cpu", "meminfo", "filesystem"},
	})

	expected := []string{
		"--web.listen-address=127.0.0.1:9100",
		"--collector.disable-defaults",
		"--collector.cpu",
		"--collector.meminfo",
		"--collector.filesystem",
	}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("unexpected node_exporter args: %#v", args)
	}
}

func TestNodeExporterArgsKeepsDefaultsWhenCollectorsOmitted(t *testing.T) {
	args := nodeExporterArgs(nodeExporterConfig{ListenAddress: "127.0.0.1:9100"})

	expected := []string{"--web.listen-address=127.0.0.1:9100"}
	if !reflect.DeepEqual(args, expected) {
		t.Fatalf("unexpected node_exporter args: %#v", args)
	}
}
