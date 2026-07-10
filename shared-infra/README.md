# Shared Infrastructure

This stack runs local shared observability infrastructure for development and manual testing.

The Docker Compose project name is pinned to `datamingle-shared-infra`, so Docker groups these containers as a dedicated shared infrastructure stack instead of under a generic compose project.

## Services

- VictoriaMetrics on http://localhost:8428 for metrics storage and local infrastructure scraping in the `local-dev` tenant installation.
- Quickwit on http://localhost:7280 with two local nodes, PostgreSQL metastore, MinIO object storage, OTLP ingest, and Jaeger storage enabled.
- Grafana on http://localhost:3000 with anonymous admin access and provisioned VictoriaMetrics, Quickwit, and Jaeger datasources.
- Jaeger UI on http://localhost:16686 backed by Quickwit gRPC storage.
- MinIO console on http://localhost:9001 with `minioadmin` / `minioadmin` for inspecting local object storage.

## Start

```bash
docker-compose -f shared-infra/docker-compose.yml up -d
```

## Stop

```bash
docker-compose -f shared-infra/docker-compose.yml down
```

Use `down -v` only when you want to delete local VictoriaMetrics, Quickwit, Grafana, PostgreSQL, and MinIO data.

## Test Metrics

VictoriaMetrics scrapes its own, Quickwit, and Grafana infrastructure metrics. Query the local-dev VictoriaMetrics instance directly with:

```bash
curl "http://localhost:8428/prometheus/api/v1/query?query=up"
```

In Grafana, use the `VictoriaMetrics local-dev` datasource for stored and scraped metrics.

## Per-Tenant Metrics

Datamingle routes metrics by organization id to a tenant-specific VictoriaMetrics base URL. For local development, all unknown tenants fall back to `http://victoriametrics-local-dev:8428`.

Configure backend reads and Django agent ingest proxying with:

```bash
DATAMINGLE_METRICS_BACKEND_URL=http://victoriametrics-local-dev:8428
DATAMINGLE_METRICS_TENANT_URLS={"org_123":"http://victoriametrics-org-123:8428"}
```

Agents remote-write to Datamingle's Django endpoint at `/api/v1/prometheus/write`.
The Django backend authenticates the Datamingle agent API key and proxies the
ingest body to the tenant's VictoriaMetrics installation.

## Test Traces

Quickwit emits its own traces to its OTLP endpoint, and Jaeger Query reads them back through Quickwit gRPC storage. Open http://localhost:16686 and search for the `quickwit` service after the stack has been running for a short time.

Application services can send OTLP traces to:

- gRPC: `http://localhost:7281`
- Inside Docker network: `http://quickwit:7281`

## Test Logs

The Grafana `Quickwit Logs` datasource is provisioned against the default OpenTelemetry logs index `otel-logs-v0_7`. Application log shippers can later be pointed at Quickwit HTTP ingest or OTLP log ingest depending on the collector pipeline we add.
