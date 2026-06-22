# Datamingle Agent Implementation Plan

## Summary

Build a Go-based Datamingle Agent in `agent/` plus a new backend `api_agents` Django app and frontend `agents` feature. The agent will behave like a Datadog-style host process: it connects to Datamingle with only `DATAMINGLE_URL` and a Datamingle-managed agent API key, keeps a websocket open for configuration and command notifications, periodically fetches full configuration, manages modules/exporters for one or more assigned database servers, and executes only workflow-approved commands.

This document is intentionally a living plan. It should be refined before implementation starts.

## Goals

- Create a comprehensive Datamingle Agent similar in spirit to Datadog Agent.
- Keep the agent in the repository `agent/` directory.
- Write the agent in Go.
- Make installation easy: the operator should need only the Datamingle host and an organization-scoped API key.
- Manage agents from the Datamingle site in a new `Agents` menu.
- Allow users to assign one or more Datamingle database servers/instances to an agent.
- Let the agent receive configuration updates and command requests from Datamingle over websocket.
- Let the agent fetch full configuration on startup and periodically.
- Reconnect automatically if websocket connectivity is lost.
- Make the agent modular from the beginning so future modules can be added cleanly.
- Support optional gh-ost and pt-online-schema-change tooling only when the online schema-change module is enabled.
- Start with strong test coverage across backend, frontend, and agent.

## Non-Goals For V1

- Do not build a logs pipeline yet. Provide the module interface and disabled placeholder only.
- Do not allow arbitrary direct command execution from the UI. Commands must be workflow-gated.
- Do not store raw time-series metrics in Django. Metrics should flow to the configured Datamingle telemetry backend, such as OTel/VictoriaMetrics.
- Do not support every operating system at launch. Start with Linux packages.

## Current Repo Context

- `agent/` currently contains only `README.md`; there is no existing Go module.
- Backend is Django/DRF with local allauth JWT for browser APIs.
- Backend currently has no agent API, no websocket endpoint, and no Channels dependency.
- Existing database/server records live in `sql.models.Instance`.
- Existing credentials on `Instance` are encrypted fields and can be decrypted by the backend when authorized.
- Frontend uses feature modules and navigation manifests under `frontend/src/features/*`.
- Local dev stack already includes Redis, which can be used for Django Channels.
- Current backend serving is WSGI-oriented Gunicorn behind nginx, so websocket support requires ASGI serving or an ASGI sidecar.
- WSGI-to-ASGI migration checklist:
  - Choose either an ASGI sidecar beside existing Gunicorn or a full backend migration to ASGI under uvicorn/daphne.
  - Audit request handling, middleware compatibility, auth/session middleware, and third-party WSGI-only dependencies.
  - Update local/staging process managers and nginx websocket proxying before production rollout.
  - Load-test HTTP and websocket traffic against baseline Gunicorn latency, error rate, worker memory, and reconnect behavior.
  - Promote only after automated tests, staging metrics, and a canary are clean; rollback by disabling the ASGI sidecar or setting `DATAMINGLE_SERVE_ASGI=0`.

## Key Architecture

- Use Datamingle-managed per-agent API keys for agent authentication.
- Datamingle creates per-agent API keys, stores only hashes and display metadata, and validates inbound keys server-side.
- Add backend models for agents, assignments, config revisions, commands, command events, tool artifacts, and module status.
- Add Django Channels plus `channels-redis`.
- Use websocket for notifications and lightweight control messages.
- Use authenticated REST endpoints for full config, command payloads, command state transitions, and command results.
- Enforce one active command-capable agent assignment per database instance in V1.
- Allow one agent to monitor many database instances.
- Include `organization_id` on agent records now, even though the current app is single-organization.

## Backend Data Model

Add models in a new Django app, preferably `api_agents` or a backend domain module paired with `api_agents` views.

### Agent

Represents one installed Datamingle Agent.

Fields:

- `id`
- `organization_id`
- `name`
- `display_name`
- `status`: `pending`, `online`, `offline`, `disabled`, `revoked`
- `api_key_prefix`
- `hostname`
- `platform`
- `architecture`
- `agent_version`
- `install_id`
- `last_seen_at`
- `last_connected_at`
- `last_disconnected_at`
- `last_config_revision`
- `desired_config_revision`
- `enabled`
- `metadata`
- `create_time`
- `update_time`

Rules:

- Never store the raw API key; store only hashed local keys and derived visible prefixes/fingerprints.
- API key value is shown only once during creation or rotation. The UI presents `Create/Rotate key`, displays the new raw key and install/update command once, and records an audit event.
- Rotation revokes the old key immediately by default. A configurable grace period may be added later, but the backend must then accept both fingerprints only until the grace window expires.
- Agents receive replacement keys through operator-driven reinstall/update commands or a future authenticated provisioning REST/websocket flow. Raw keys are never written to durable config.
- Disabled or revoked agents cannot connect over REST or websocket.
- `install_id` is generated by the agent on first registration and then bound to the backend agent record.

### AgentInstanceAssignment

Assigns existing Datamingle `Instance` records to an agent.

Fields:

- `id`
- `agent`
- `instance`
- `enabled`
- `modules`
- `capabilities`
- `command_enabled`
- `metrics_enabled`
- `online_schema_enabled`
- `logs_enabled`
- `create_time`
- `update_time`

Rules:

- One active command-capable assignment per `Instance` in V1.
- Multiple agents may eventually monitor the same instance for metrics-only use, but V1 should keep this conservative unless explicitly expanded.
- Assignment save increments the agent desired config revision.

### AgentConfigRevision

Stores config revision metadata for auditability and reconciliation.

Fields:

- `id`
- `agent`
- `revision`
- `config_hash`
- `summary`
- `created_by`
- `create_time`

Rules:

- Revision increments on assignment changes, module changes, artifact changes, and credential-impacting instance changes.
- Agent reports the last applied revision in heartbeat and websocket messages.

### AgentCommand

Represents a backend-created workflow-gated command.

Fields:

- `id`
- `agent`
- `instance`
- `workflow_type`
- `workflow_id`
- `command_type`: `query.execute`, `schema.change`, `connection.test`
- `status`: `queued`, `dispatched`, `accepted`, `running`, `succeeded`, `failed`, `cancelled`, `expired`
- `idempotency_key`
- `payload`
- `result`
- `error`
- `lease_owner`
- `lease_expires_at`
- `cancel_requested_at`
- `queued_at`
- `dispatched_at`
- `accepted_at`
- `started_at`
- `finished_at`
- `create_time`
- `update_time`

Rules:

- Commands are created only after existing Datamingle workflow authorization permits execution.
- Commands are scoped to an assigned instance.
- Backend rejects command creation if the target instance has no online command-capable agent.
- Commands are immutable except for status, lease, result, and audit/event updates.
- The backend service creates `idempotency_key` for internal workflow dispatch; clients may supply one only for explicitly retryable user-driven APIs.
- Use a canonical format such as `sql_workflow:<workflow_id>` for workflow-backed commands or UUIDv4 for external retries. Validate non-empty keys at 128 characters or less.
- Keep idempotency keys with command records for the command retention period, then remove them with command cleanup. Do not use workflow ID alone outside a namespaced composite key.
- Initial lease duration is 300 seconds. Agents renew at half the lease duration. A watchdog treats commands as stale after one extra lease interval.
- Lease acquire/renew uses atomic compare-and-set updates on `lease_owner` and `lease_expires_at`; expired commands are failed or requeued according to retry policy.

### AgentCommandEvent

Append-only event stream for command progress.

Fields:

- `id`
- `command`
- `event_type`
- `message`
- `payload`
- `create_time`

Rules:

- Store redacted logs only.
- Use this for UI progress and debugging.

### AgentToolArtifact

Pinned external tool manifest for gh-ost and pt-online-schema-change.

Fields:

- `id`
- `tool_name`: `gh-ost`, `pt-online-schema-change`
- `version`
- `platform`
- `architecture`
- `download_url`
- `sha256`
- `size_bytes`
- `enabled`
- `notes`
- `create_time`
- `update_time`

Rules:

- Backend rejects enabled artifacts without SHA256.
- Agent downloads artifacts only when the matching module is enabled.
- Agent verifies checksum before marking the module healthy.

## Backend APIs

### Browser/Admin APIs

- `GET /api/v1/agents/`
  - List agents with health, status, assignment count, version, hostname, and last seen.

- `POST /api/v1/agents/`
  - Create pending agent record.
  - Create a Datamingle agent API key.
  - Return raw key once plus install command.

- `GET /api/v1/agents/{id}/`
  - Return detail, assignments, modules, health, and latest command summary.

- `PATCH /api/v1/agents/{id}/`
  - Rename, enable, disable, or update metadata.

- `DELETE /api/v1/agents/{id}/`
  - Revoke the old Datamingle agent API key if present.
  - Disable agent and disconnect websocket.

- `GET /api/v1/agents/{id}/assignments/`
  - List assigned instances and module configuration.

- `PUT /api/v1/agents/{id}/assignments/`
  - Replace assignments and module settings.
  - Increment config revision.
  - Broadcast `config.changed`.

- `GET /api/v1/agents/{id}/commands/`
  - Show command history and event summaries.

- `GET /api/v1/agents/{id}/commands/{command_id}/`
  - Show command detail, events, result, and error.

- `POST /api/v1/agents/{id}/commands/{command_id}/cancel/`
  - Request cancellation for a running command.
  - Broadcast `command.cancel`.

- `GET /api/v1/agents/tool-artifacts/`
  - Staff-only list of pinned external tools.

- `POST /api/v1/agents/tool-artifacts/`
  - Staff-only create artifact.

- `PATCH /api/v1/agents/tool-artifacts/{id}/`
  - Staff-only update artifact.

- `DELETE /api/v1/agents/tool-artifacts/{id}/`
  - Staff-only disable or delete artifact.

### Agent REST APIs

- `POST /api/v1/agent/register/`
  - Authenticate API key.
  - Bind install ID, hostname, version, platform, and architecture.
  - Return agent ID and current desired config revision.

- `GET /api/v1/agent/me/config/`
  - Return full config revision.
  - Include assigned instances, enabled modules, credentials, telemetry endpoints, and tool artifact manifests.
  - Include credentials only for active assignments on the authenticated agent.

- `POST /api/v1/agent/me/heartbeat/`
  - Agent sends status, module health, running versions, current config revision, and command summary.

- `GET /api/v1/agent/commands/{id}/`
  - Fetch full command payload after websocket notification.
  - Validate command belongs to authenticated agent.

- `POST /api/v1/agent/commands/{id}/ack/`
  - Move `dispatched` or `queued` to `accepted`.

- `POST /api/v1/agent/commands/{id}/start/`
  - Move to `running` and acquire/refresh lease.

- `POST /api/v1/agent/commands/{id}/progress/`
  - Append event and refresh lease.

- `POST /api/v1/agent/commands/{id}/finish/`
  - Store redacted result and move to `succeeded`.

- `POST /api/v1/agent/commands/{id}/fail/`
  - Store redacted error and move to `failed`.

- `POST /api/v1/agent/commands/{id}/cancel/`
  - Confirm cancellation and store final event.

## Websocket Protocol

Endpoint:

```text
wss://<datamingle-host>/api/ws/agent/
```

Authentication:

- Use `Authorization: Bearer <api_key>`.
- Validate through the same backend agent API key authentication class.
- Reject disabled, revoked, or wrong-organization agents.

Server messages:

```json
{"type":"config.changed","revision":12,"reason":"assignment.updated"}
{"type":"command.available","command_id":123,"command_type":"schema.change"}
{"type":"command.cancel","command_id":123}
{"type":"ping","sent_at":"2026-05-12T20:00:00Z"}
```

Agent messages:

```json
{"type":"hello","agent_version":"0.1.0","config_revision":11}
{"type":"config.applied","revision":12,"config_hash":"..."}
{"type":"module.status","module":"online_schema","status":"healthy"}
{"type":"command.progress","command_id":123,"message":"copy phase started"}
{"type":"pong","sent_at":"2026-05-12T20:00:01Z"}
```

Rules:

- Websocket messages are notifications, not the source of truth.
- Agent fetches full config after `config.changed`, after reconnect, on startup, and on periodic refresh.
- Backend sends `command.available`; agent fetches command payload over REST.
- If websocket disconnects during a command, the command continues and reports over REST when connectivity returns.
- Server sends `ping` every `heartbeat_interval` seconds, default 30. The hello/ack payload advertises `heartbeat_interval`, `pong_timeout`, and optional `agent_heartbeat_interval`.
- Agent must answer each `ping` with `pong` carrying the original `sent_at`. If `pong_timeout` elapses, default 90 seconds, the server marks the connection dead and the agent reconnects.
- Agent may send its own heartbeat over REST at `agent_heartbeat_interval`, default 30 seconds, independent of websocket pings.
- Reconnect uses exponential backoff with an initial delay of 1 second, multiplier 2x, uniform jitter between 50% and 100% of the current delay, a 60 second cap, and infinite retries while the process is running.

## Go Agent Layout

Initialize `agent/` as a Go module.

Suggested package layout:

```text
agent/
  cmd/datamingle-agent/
  internal/client/
  internal/config/
  internal/ws/
  internal/modules/
  internal/modules/core/
  internal/modules/mysql/
  internal/modules/metrics/
  internal/modules/online_schema/
  internal/modules/logs/
  internal/process/
  internal/tools/
  internal/secrets/
  internal/commands/
  internal/redact/
  internal/version/
  packaging/systemd/
  packaging/docker/
  docs/
```

Inter-module communication:

- `internal/config` owns immutable config snapshots passed into modules.
- `internal/secrets` owns credential material behind a thread-safe store using `sync.RWMutex` or channels.
- `internal/commands` exposes a typed `CommandBus` for routing commands/events to `internal/modules/*` and `internal/modules/core`.
- Modules run as goroutines managed by `internal/process` unless a module explicitly needs a subprocess. `Stop` must cancel goroutines/subprocesses and wait for cleanup.
- Modules implement a `HealthReporter` contract aggregated by `internal/process` or `internal/ws` for telemetry. Shared state must not be mutated without the owning store.

CLI commands:

- `datamingle-agent run`
- `datamingle-agent status`
- `datamingle-agent config check`
- `datamingle-agent doctor`
- `datamingle-agent version`

Minimal config:

```yaml
datamingle_url: https://datamingle.example.com
api_key_env: DATAMINGLE_AGENT_API_KEY
agent_name: prod-db-agent-01
data_dir: /var/lib/datamingle-agent
log_dir: /var/log/datamingle-agent
runtime_dir: /run/datamingle-agent
```

Default Linux paths:

- Config: `/etc/datamingle-agent/agent.yaml`
- Data: `/var/lib/datamingle-agent`
- Logs: `/var/log/datamingle-agent`
- Runtime: `/run/datamingle-agent`

## Agent Runtime Behavior

Startup sequence:

1. Load local config.
2. Validate Datamingle URL and API key source.
3. Create or load local install ID.
4. Register with backend.
5. Fetch full config.
6. Reconcile modules and tools.
7. Connect websocket.
8. Start heartbeat loop.
9. Start periodic full-config refresh loop.

Reconciliation sequence:

1. Compare local applied revision with backend desired revision.
2. Stop modules removed from config.
3. Start modules added to config.
4. Update modules with changed assignments.
5. Download/verify optional tools required by enabled modules.
6. Report `config.applied` or degraded module status.

Failure behavior:

- If full config fetch fails, keep the last applied in-memory config and retry.
- If websocket disconnects, keep modules running and reconnect.
- If credentials are removed from an assignment, stop affected module processes and remove temporary credential files.
- If module startup fails, mark module degraded and continue running other modules.

Shutdown behavior:

- Handle SIGTERM and SIGINT as graceful shutdown signals. Cancel the root context, stop accepting new commands, and allow in-flight commands to finish until `shutdown_timeout`, default 30 seconds.
- When the timeout elapses, cancel running command contexts and terminate child processes. A second signal exits immediately.
- Cleanup order: stop heartbeat and full-config refresh loops, close websocket, stop modules, remove temporary credential files, flush/report final status, and unregister active websocket metadata when applicable.
- systemd should set a matching `TimeoutStopSec` greater than `shutdown_timeout`.

## Module System

Each module implements:

```go
type Module interface {
    Name() string
    Capabilities() []string
    ApplyConfig(ctx context.Context, cfg ModuleConfig) error
    Health(ctx context.Context) ModuleHealth
    Stop(ctx context.Context) error
}
```

V1 modules:

- `core`
  - Registration, config sync, websocket, heartbeat, and local status.

- `mysql`
  - Database connectivity checks.
  - Shared connection helpers for query and schema-change commands.

- `metrics`
  - Manage node exporter and mysqld exporter.
  - Send metrics to Datamingle-configured OTel/VictoriaMetrics endpoint.
  - Use temporary credentials for mysqld exporter.

- `online_schema`
  - Download and verify gh-ost / pt-online-schema-change when enabled.
  - Execute approved schema-change commands.
  - Report progress and final status.

- `logs`
  - Disabled placeholder only.
  - Define interface so the module can be enabled later without reshaping the agent.

## Credentials

- Backend sends DB credentials only to the authenticated agent for active assignments.
- Agent holds database passwords in memory only.
- Use a systemd `RuntimeDirectory` or `/run/datamingle-agent` tmpfs runtime dir for credential files.
- If a subprocess needs a credential file, write it under runtime dir with `0600` permissions and an agent-managed prefix/suffix such as `dmagent-*-credentials.cnf`.
- Run `cleanupStaleCredentialFiles` at startup and periodically. It scans only agent-managed credential files and removes entries older than a safe TTL, default 1 hour.
- Remove subprocess credential files on module stop, assignment removal, or shutdown.
- Never write database passwords to durable agent config.
- Redact passwords, DSNs, API keys, tokens, and credential file paths from logs and command events.
- Centralize redaction in `internal/redact` with `SensitivePatterns`, `redactSensitive`, `redactDSN`, and `applyRedactionToLogEntry`.
- Structured logs are redacted field-by-field for password/API key/token names and DSN/query credentials. Plain text logs use regex replacement for secrets and credential paths. All loggers and command-event emitters use the same storage-time sanitizer.

## Command Execution

Commands are workflow-gated only.

Supported V1 command types:

- `query.execute`
  - Execute bounded SQL.
  - Enforce timeout, row limit, and result truncation.
  - Validate assigned instance and allowed database.

- `schema.change`
  - Execute approved MySQL DDL.
  - Executor is `gh-ost` or `pt-online-schema-change`.
  - Requires `online_schema` module enabled.

- `connection.test`
  - Test agent-side network path and credentials for assigned instance.

State machine:

```text
queued -> dispatched -> accepted -> running -> succeeded
                                           -> failed
                                           -> cancelled
                                           -> expired
```

Rules:

- Use command leases so stale running commands can be detected.
- Use idempotency keys so retries do not duplicate execution.
- Use per-instance execution locks for schema changes.
- Continue running a command if websocket disconnects.
- Report command progress over REST and optionally websocket.
- Cancellation is backend-driven and best-effort:
  - mark cancel requested;
  - send websocket `command.cancel`;
  - terminate the child process gracefully;
  - wait `CANCEL_FORCE_KILL_TIMEOUT_MS`, default 5000, or `command.forceKillTimeoutMs` when supplied;
  - call `forceKill(pid)` after timeout;
  - if force kill fails or the process remains, log PID/reason and report terminal `cancel_failed` diagnostics;
  - otherwise report `cancelled`.

## gh-ost And pt-online-schema-change

Tooling strategy:

- Do not bundle gh-ost or pt-osc in the base agent.
- Download only when `online_schema` is enabled.
- Use backend-managed pinned artifact manifests.
- Verify SHA256 before use.
- Cache verified artifacts locally by tool/version/platform.

Cache layout:

```text
/var/lib/datamingle-agent/tools/
  gh-ost/
    1.1.6/
      linux-amd64/gh-ost
  pt-online-schema-change/
    3.6.0/
      linux-amd64/pt-online-schema-change
```

Failure behavior:

- Download failure marks `online_schema` degraded.
- Checksum mismatch deletes the artifact and marks `online_schema` unhealthy.
- Schema-change command fails before execution if required tooling is unavailable.

## Frontend Management

Add a new primary navigation item:

- Label: `Agents`
- Suggested route prefix: `/agents`
- Suggested permission: `sql.menu_agent` or a new explicit agent permission set.

Pages:

- Agents list
  - Online/offline status.
  - Agent version.
  - Hostname.
  - Assigned instances.
  - Module health.
  - Last seen.
  - Current/desired config revision.

- Agent create/install
  - Use a modal or dedicated page.
  - Create backend agent and Datamingle agent API key.
  - Show API key exactly once.
  - Show install command requiring only Datamingle host and API key.

- Agent detail
  - Health summary.
  - Config revision.
  - Assignment management.
  - Module status.
  - Recent command history.
  - Install metadata.
  - Revoke/disable actions.

- Assignment management
  - Select one or more existing `Instance` records.
  - Enable modules per assignment.
  - Save assignments.
  - Show config revision update.

- Tool artifacts
  - Staff-only.
  - Manage gh-ost and pt-online-schema-change versions, platform, URL, checksum, and enabled status.

UI behavior:

- Keep pages dense and operational.
- Do not create a marketing-style landing page.
- Keep create/install separate from list browsing.
- Detail opens from selected list item.

## Backend Integration With Existing Workflows

Schema-change execution:

- Existing workflow approval remains the source of authorization.
- When a workflow reaches execution, backend determines if the target instance is assigned to an online command-capable agent.
- If yes, create `AgentCommand` with type `schema.change`.
- Dispatch via websocket.
- Show agent command progress in workflow execution UI.

Query execution:

- Existing query privilege checks remain in backend.
- Backend creates agent command only after privilege checks pass.
- Agent executes query and returns bounded result.
- Backend logs query in existing query log flow with indication that execution happened through agent.

Connection testing:

- Add option to test connection through assigned agent.
- Useful when Datamingle backend cannot directly reach the database network.

## Security And Audit

Datamingle agent API key validation:

- Validate API key hash against Datamingle agent records.
- Verify organization ID from the matched agent record.
- Verify the agent is allowed to connect.
- Cache successful validation briefly by key hash.
- Reject disabled/revoked Datamingle agent records.

Required key permissions:

- `datamingle-agent:connect`
- `datamingle-agent:read-config`
- `datamingle-agent:execute-command`

Audit events:

- Agent created.
- API key created.
- API key revoked.
- Agent disabled/enabled.
- Assignment changed.
- Config revision created.
- Config dispatched.
- Command dispatched.
- Command accepted.
- Command started.
- Command finished.
- Command failed.
- Command cancelled.

Redaction:

- Redact database passwords.
- Redact Datamingle API keys.
- Redact DSNs.
- Redact temporary credential file paths.
- Redact command-line args known to include passwords.

## Deployment

Backend:

- Add `channels` and `channels-redis` to requirements.
- Add `ASGI_APPLICATION`.
- Configure Redis channel layer.
- Update `archery/asgi.py` to route HTTP and websocket protocols.
- Update local dev startup from WSGI-only Gunicorn to ASGI-capable serving.
- Update nginx config to proxy websocket upgrades.

Agent:

- Build Linux amd64 and arm64 binaries.
- Provide systemd unit.
- Provide install script that accepts Datamingle URL and API key.
- Provide Docker image later if useful, but systemd Linux install is V1 priority.

Example install command shown by UI:

```bash
curl -fsSLO https://<datamingle-host>/api/v1/agents/install.sh
curl -fsSLO https://<datamingle-host>/api/v1/agents/install.sh.sha256
sha256sum -c install.sh.sha256
less install.sh
sudo DATAMINGLE_URL="https://<datamingle-host>" DATAMINGLE_AGENT_API_KEY="<one-time-key>" bash install.sh
```

Installer verification:

- Publish `install.sh`, `install.sh.sha256`, and an optional detached GPG signature.
- Document the trusted public key fingerprint in release notes and the UI before showing the command.
- Manual install is the preferred path: download, verify checksum or GPG signature, inspect, then run with sudo.
- If a one-liner is needed, use a verification-aware mode such as `bash -s -- --verify` after the script has checked its embedded checksum/signature metadata.

## Testing Plan

Backend tests:

- Datamingle agent API key validation success/failure.
- Agent registration binds install ID and host metadata.
- Disabled/revoked agents cannot authenticate.
- Config payload includes only assigned instances.
- Config payload includes decrypted credentials only for the authenticated agent.
- Assignment updates increment config revision.
- Assignment updates broadcast `config.changed`.
- Duplicate command-capable assignment is rejected.
- Command state transitions enforce the expected state machine.
- Command progress appends events.
- Cancellation broadcasts `command.cancel`.
- Tool artifacts require SHA256 when enabled.
- Tool artifact changes increment affected config revisions.

Agent tests:

- Config file parsing and validation.
- Registration payload formation.
- REST retry behavior.
- Websocket reconnect/backoff.
- Full-config fetch on startup.
- Full-config fetch after reconnect.
- Full-config fetch after `config.changed`.
- Module start/stop/update reconciliation.
- Secret redaction.
- Command state machine.
- Idempotency handling.
- Process cancellation.
- Tool artifact download.
- SHA256 verification.
- Checksum mismatch deletion.
- Module degraded health reporting.

Integration tests:

- Fake Datamingle REST + websocket server.
- Fake MySQL target.
- Agent receives assignment, applies config, and reports healthy.
- Agent loses websocket and reconnects.
- Agent receives command notification, fetches command over REST, executes fake command, and reports success.
- Agent continues command during websocket outage.
- Online schema module downloads a fake artifact and verifies checksum.

Frontend tests:

- Agents nav visibility.
- Agents list rendering.
- Agent detail rendering.
- Agent creation shows API key once.
- Assignment save calls the correct API and updates displayed revision.
- Tool artifact validation errors display correctly.

Verification commands:

```bash
go test ./... -race -coverprofile=coverage.out
docker exec -w /opt/datamingle/backend datamingle-app python manage.py test api_agents
docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql --check
black --check backend
cd frontend && npm run build
```

## Acceptance Criteria

- A new agent can be created from the Datamingle UI.
- UI shows an install command containing only Datamingle host and one-time API key.
- Installed agent registers with Datamingle.
- Agent appears online in the UI.
- User can assign one or more database instances to the agent.
- Assignment save causes websocket `config.changed`.
- Agent fetches full config and applies assignment.
- Agent heartbeat reports module status and config revision.
- Websocket reconnect works after backend or network interruption.
- Periodic full config refresh works even without websocket notifications.
- Backend sends database credentials only for assigned instances.
- Workflow-approved command can be dispatched to the agent.
- Agent reports command progress and final result.
- gh-ost/pt-osc are downloaded and verified only when `online_schema` is enabled.
- Tests exist for the major backend, frontend, and Go agent flows from the beginning.

## Assumptions

- V1 supports Linux amd64 and arm64 packages first.
- Current Datamingle remains single-organization, but agent models include `organization_id` for future multi-org support.
- Datamingle-managed agent API keys are the chosen auth strategy.
- Commands are workflow-gated only.
- gh-ost and pt-online-schema-change are downloaded on enable with checksum verification.
- Metrics are sent to a configured Datamingle/OTel/VictoriaMetrics endpoint from backend config.
- Go is not installed in the current local workspace, so implementation must add Go setup guidance and CI tooling.

## Open Questions For Later Iterations

- Should metrics-only duplicate assignments be allowed in V1, or should all assignments remain unique per instance?
- Which exact Datamingle agent API key metadata fields should the UI expose?
- Should Datamingle proxy tool downloads, or should agents download directly from vendor/GitHub URLs?
- Which schema-change executor should be preferred by default for MySQL: gh-ost or pt-online-schema-change?
- Should agent command results be stored fully in Django or offloaded to object storage for large outputs?
- Should the agent expose a local localhost-only status endpoint for debugging?
