# Shared Infrastructure

This stack runs local shared observability infrastructure for development and manual testing. In production, these services are expected to be shared across tenants rather than owned by an individual tenant app stack.

The Docker Compose project name is pinned to `datamingle-shared-infra`, so Docker groups these containers as a dedicated shared infrastructure stack instead of under a generic compose project.

## Services

- Cortex on http://localhost:9009 for Prometheus remote-write metrics under the `local-dev` tenant.
- Quickwit on http://localhost:7280 with two local nodes, PostgreSQL metastore, MinIO object storage, OTLP ingest, and Jaeger storage enabled.
- Grafana on http://localhost:3000 with anonymous admin access and provisioned Cortex, Prometheus, Quickwit, and Jaeger datasources.
- Jaeger UI on http://localhost:16686 backed by Quickwit gRPC storage.
- Prometheus on http://localhost:9090 as a local metrics producer that remote-writes to Cortex.
- MinIO console on http://localhost:9001 with `minioadmin` / `minioadmin` for inspecting local object storage.

## Start

```bash
docker-compose -f shared-infra/docker-compose.yml up -d
```

## Stop

```bash
docker-compose -f shared-infra/docker-compose.yml down
```

Use `down -v` only when you want to delete local Cortex, Quickwit, Grafana, Prometheus, PostgreSQL, and MinIO data.

## Test Metrics

Prometheus remote-writes scraped metrics into Cortex with the `X-Scope-OrgID: local-dev` header. Query Cortex directly with:

```bash
curl -H "X-Scope-OrgID: local-dev" "http://localhost:9009/prometheus/api/v1/query?query=up"
```

In Grafana, use the `Cortex` datasource for shared metrics and the `Prometheus Local` datasource to inspect the local scrape source.

## Test Traces

Quickwit emits its own traces to its OTLP endpoint, and Jaeger Query reads them back through Quickwit gRPC storage. Open http://localhost:16686 and search for the `quickwit` service after the stack has been running for a short time.

Application services can send OTLP traces to:

- gRPC: `http://localhost:7281`
- Inside Docker network: `http://quickwit:7281`

## Test Logs

The Grafana `Quickwit Logs` datasource is provisioned against the default OpenTelemetry logs index `otel-logs-v0_7`. Application log shippers can later be pointed at Quickwit HTTP ingest or OTLP log ingest depending on the collector pipeline we add.
