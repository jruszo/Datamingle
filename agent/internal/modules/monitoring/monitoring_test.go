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

func TestParseConfigUsesScrapeProfiles(t *testing.T) {
	cfg, err := parseConfig(map[string]any{
		"scrape_interval_seconds": 30,
		"scrape_profiles": []any{
			map[string]any{
				"name":             "high",
				"interval_seconds": 5,
				"collectors":       []any{"cpu", "meminfo"},
			},
			map[string]any{
				"name":             "normal",
				"interval_seconds": 30,
				"collectors":       []any{"hwmon"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.ScrapeProfiles) != 2 {
		t.Fatalf("expected profiles, got %#v", cfg.ScrapeProfiles)
	}
	if cfg.ScrapeProfiles[0].Name != "high" {
		t.Fatalf("unexpected first profile: %#v", cfg.ScrapeProfiles[0])
	}
	if cfg.ScrapeProfiles[0].Interval.String() != "5s" {
		t.Fatalf("unexpected high profile interval: %s", cfg.ScrapeProfiles[0].Interval)
	}
	if !reflect.DeepEqual(cfg.ScrapeProfiles[0].Collectors, []string{"cpu", "meminfo"}) {
		t.Fatalf("unexpected high profile collectors: %#v", cfg.ScrapeProfiles[0].Collectors)
	}
}

func TestMetricsURLWithCollectors(t *testing.T) {
	metricsURL, err := metricsURLWithCollectors("http://127.0.0.1:9100/metrics?existing=1", []string{"cpu", "meminfo"})
	if err != nil {
		t.Fatal(err)
	}
	expected := "http://127.0.0.1:9100/metrics?collect%5B%5D=cpu&collect%5B%5D=meminfo&existing=1"
	if metricsURL != expected {
		t.Fatalf("unexpected metrics URL: %s", metricsURL)
	}
}
