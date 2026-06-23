# Instance Queryable Flag — Design Spec

**Date:** 2026-06-23
**Status:** Approved
**Approach:** 1 — Instance-level `queryable` flag

## Summary

Add a `queryable` BooleanField to the `Instance` model, expose it and the existing
`monitoring_enabled` field in the instance editor form, and gate query execution /
instance listing on the `queryable` flag. Fix any breakage in the agent command
dispatch pipeline so that queries execute successfully when the flag is set.

## Database Model Changes

- `sql.Instance` gains `queryable = models.BooleanField("Queryable", default=False)`.
  New field sits next to `monitoring_enabled` (currently line 665).
- Generated via `makemigrations sql`; no hand-editing of migration files.

No changes to `InfrastructureNode`, `ServiceRecommendation`, or any other model.

## Backend Changes

### Serializers

- `api_instances.serializers.InstanceCreateSerializer`:
  Add `"monitoring_enabled"` and `"queryable"` to the `Meta.fields` tuple.
- Instance editor retrieval serializer (the one backing `GET /v1/instance/<pk>/`):
  Ensure it includes `monitoring_enabled`, `queryable`, and `node_name`.

### Query API (`api_queries/views.py`)

- `QueryInstanceList.get`: After the existing `filter_agent_runnable_instances()`
  call, add `.filter(queryable=True)` so only explicitly queryable instances
  appear in the query UI dropdown.
- `QueryExecute.post`: After fetching the instance by name, add a guard:
  `if not instance.queryable: raise ValidationError("This instance is not queryable.")`
- `QueryDescribe.post`: Same guard added after instance fetch.

### Agent Dispatch

- Investigate and fix the agent command dispatch pipeline if broken.
  The flow: `run_agent_command_sync()` → `create_agent_command_for_instance()` →
  `command_capable_assignment_for_instance()` → `dispatch_agent_command()` →
  `notify_command_available()` (websocket) → `wait_for_agent_command()` (poll).
- No structural changes to this pipeline; only bug fixes if needed.

## Frontend Changes

### Types (`lib/api.ts`)

- `InstanceCreatePayload` gains `queryable: boolean` and `monitoring_enabled: boolean`.

### Instance Editor (`InventoryEditorPage.vue`)

- Add two checkboxes in the right column (alongside SSL settings, Teams, Tags):
  - "Enable monitoring" (`form.monitoring_enabled`)
  - "Enable SQL queries" (`form.queryable`)
- Wire them into `buildInstancePayload()`, `applyInstance()`, and `resetForm()`.

### Query Page

- No changes needed. The backend filter already controls which instances appear.

## Verification

- `docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql --check`
  must report no drift.
- `black --check backend` must pass.
- `npm run build` from `frontend/` must pass.
- Targeted Django tests for query execute/describe with queryable=True/False.
