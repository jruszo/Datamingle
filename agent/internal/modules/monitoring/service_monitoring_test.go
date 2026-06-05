package monitoring

import (
	"strings"
	"testing"
	"time"
)

func TestServiceExporterCommandUsesEnvironmentForCredentials(t *testing.T) {
	service := monitoredService{
		DBType:   "mysql",
		Host:     "mysql_demo",
		Port:     3306,
		Username: "metrics_user",
		Password: "secret",
		Collectors: []string{
			"global_status",
			"slave_status",
		},
		Exporter: serviceExporterConfig{
			ListenAddress: "127.0.0.1:9200",
		},
	}

	cmd, err := serviceExporterCommand("/tmp/mysqld_exporter", service)
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range cmd.Args {
		if strings.Contains(arg, "secret") {
			t.Fatalf("password leaked into argv: %#v", cmd.Args)
		}
	}
	if !contains(cmd.Env, "MYSQLD_EXPORTER_PASSWORD=secret") {
		t.Fatalf("expected mysql password in env")
	}
	if !contains(cmd.Args, "--mysqld.address=mysql_demo:3306") {
		t.Fatalf("expected mysql address flag in %#v", cmd.Args)
	}
	if !contains(cmd.Args, "--collect.global_status") {
		t.Fatalf("expected selected mysql collector in %#v", cmd.Args)
	}
	if !contains(cmd.Args, "--no-collect.global_variables") {
		t.Fatalf("expected deselected mysql default collector in %#v", cmd.Args)
	}
}

func TestPostgresExporterCommandUsesSplitDataSourceEnvironment(t *testing.T) {
	service := monitoredService{
		DBType:   "pgsql",
		Host:     "postgres_demo",
		Port:     5432,
		Username: "metrics_user",
		Password: "secret",
		Database: "postgres",
		Collectors: []string{
			"database",
			"wal",
		},
		Exporter: serviceExporterConfig{
			ListenAddress: "127.0.0.1:9201",
		},
	}

	cmd, err := serviceExporterCommand("/tmp/postgres_exporter", service)
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range cmd.Args {
		if strings.Contains(arg, "secret") {
			t.Fatalf("password leaked into argv: %#v", cmd.Args)
		}
	}
	if !contains(cmd.Env, "DATA_SOURCE_USER=metrics_user") {
		t.Fatalf("expected postgres user in env")
	}
	if !contains(cmd.Env, "DATA_SOURCE_PASS=secret") {
		t.Fatalf("expected postgres password in env")
	}
	if !contains(cmd.Env, "DATA_SOURCE_URI=postgres_demo:5432/postgres?sslmode=disable") {
		t.Fatalf("expected postgres uri in env")
	}
	if !contains(cmd.Args, "--collector.database") {
		t.Fatalf("expected selected postgres collector in %#v", cmd.Args)
	}
	if !contains(cmd.Args, "--no-collector.locks") {
		t.Fatalf("expected deselected postgres default collector in %#v", cmd.Args)
	}
}

func TestParseServiceMonitoringConfigUsesServiceScrapeProfiles(t *testing.T) {
	cfg, err := parseServiceMonitoringConfig(map[string]any{
		"scrape_interval_seconds": 30,
		"scrape_profiles": []any{
			map[string]any{
				"name":             "high",
				"interval_seconds": 5,
			},
			map[string]any{
				"name":             "low",
				"interval_seconds": 60,
			},
		},
		"services": []any{
			map[string]any{
				"assignment_id": 1,
				"db_type":       "mysql",
				"collectors":    []any{"global_status", "global_variables"},
				"scrape_profiles": []any{
					map[string]any{
						"name":             "high",
						"interval_seconds": 5,
						"collectors":       []any{"global_status"},
					},
					map[string]any{
						"name":             "low",
						"interval_seconds": 60,
						"collectors":       []any{"global_variables"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	profiles := cfg.effectiveScrapeProfiles()
	if len(profiles) != 2 {
		t.Fatalf("expected two effective profiles, got %#v", profiles)
	}
	if profiles[0].Name != "high" || profiles[0].Interval != 5*time.Second {
		t.Fatalf("unexpected high profile: %#v", profiles[0])
	}
	collectors, ok := cfg.Services[0].collectorsForProfile("low")
	if !ok || !contains(collectors, "global_variables") {
		t.Fatalf("expected low profile collectors, got %#v", collectors)
	}
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
