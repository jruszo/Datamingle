# Instance Queryable Flag — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `queryable` BooleanField to the `Instance` model, expose it and `monitoring_enabled` in the instance editor form, and gate query execution on the flag. Fix any breakage in the agent dispatch pipeline.

**Architecture:** A new model field with default `False` ensures no existing instances become queryable unintentionally. Three serializers gain the field. The query API adds a queryset filter and per-request guards. The frontend adds checkboxes in the editor form.

**Tech Stack:** Django 6.0 (migrations, DRF serializers), Vue 3 + TypeScript (shadcn-vue form components).

## Global Constraints

- Do not hand-edit migration files; generate via `makemigrations`.
- Follow the repo's Black formatting (`black --check backend` must pass).
- Frontend must pass `npm run build` from `frontend/`.
- Follow existing code patterns in the files being modified.

---

### Task 1: Add `queryable` field to the Instance model

**Files:**
- Modify: `backend/sql/models.py:665`

**Interfaces:**
- Produces: `Instance.queryable` — `BooleanField("Queryable", default=False)`

- [ ] **Step 1: Add the field**

In `backend/sql/models.py`, after line 665 (`monitoring_enabled = models.BooleanField("Monitoring Enabled", default=True)`), add:

```python
    queryable = models.BooleanField("Queryable", default=False)
```

- [ ] **Step 2: Generate the migration**

Copy the updated models.py into the container (if not bind-mounted), then run:

```bash
docker cp backend/sql/models.py datamingle-app:/opt/datamingle/backend/sql/models.py && docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql
```

If bind-mounted, run directly:

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql
```

- [ ] **Step 3: Copy migration file back to host**

If the migration was generated inside the container, find and copy it:

```bash
ls backend/sql/migrations/ | grep queryable
```

If absent on host, copy: `docker cp datamingle-app:/opt/datamingle/backend/sql/migrations/<file> backend/sql/migrations/`

- [ ] **Step 4: Verify migration applies cleanly**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py migrate sql --check
```

Expected: no output (dry-run succeeds), or run without `--check` to apply.

- [ ] **Step 5: Commit**

```bash
git add backend/sql/models.py backend/sql/migrations/*queryable*
git commit -m "feat: add queryable field to Instance model"
```

---

### Task 2: Add `monitoring_enabled` and `queryable` to serializers

**Files:**
- Modify: `backend/api_instances/serializers.py:335-357` (InstanceEditorSerializer)
- Modify: `backend/api_instances/serializers.py:638-659` (InstanceDetailSerializer)
- Modify: `backend/api_instances/serializers.py:425-446` (InstanceCreateSerializer)

**Interfaces:**
- Consumes: `Instance.queryable` (Task 1), `Instance.monitoring_enabled` (existing)
- Produces: All three serializers include `"monitoring_enabled"` and `"queryable"` in their `Meta.fields`

- [ ] **Step 1: Add to InstanceEditorSerializer**

In `backend/api_instances/serializers.py`, at `InstanceEditorSerializer.Meta.fields` (line 337), add `"monitoring_enabled"` and `"queryable"` after `"db_type"`:

```python
class InstanceEditorSerializer(serializers.ModelSerializer):
    team_ids = serializers.SerializerMethodField()
    instance_tag_ids = serializers.SerializerMethodField()
    node_name = serializers.CharField(source="node.name", read_only=True)

    def get_team_ids(self, obj):
        return list(
            obj.resource_group.values_list("team_id", flat=True).order_by("team_id")
        )

    def get_instance_tag_ids(self, obj):
        return list(obj.instance_tag.values_list("id", flat=True).order_by("id"))

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "type",
            "db_type",
            "monitoring_enabled",
            "queryable",
            "host",
            "port",
            "user",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "service_name",
            "sid",
            "team_ids",
            "instance_tag_ids",
            "node",
            "node_name",
        )
```

- [ ] **Step 2: Add to InstanceDetailSerializer**

In `backend/api_instances/serializers.py`, at `InstanceDetailSerializer.Meta.fields` (line 640), add `"monitoring_enabled"` and `"queryable"` after `"db_type"`:

```python
    class Meta:
        model = Instance
        fields = (
            "instance_name",
            "type",
            "db_type",
            "monitoring_enabled",
            "queryable",
            "host",
            "port",
            "user",
            "password",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "service_name",
            "sid",
            "node",
            "team_ids",
            "instance_tag_ids",
        )
```

- [ ] **Step 3: Add to InstanceCreateSerializer**

In `backend/api_instances/serializers.py`, at `InstanceCreateSerializer.Meta.fields` (line 427), add `"monitoring_enabled"` and `"queryable"` after `"db_type"`:

```python
    class Meta:
        model = Instance
        fields = (
            "instance_name",
            "type",
            "db_type",
            "monitoring_enabled",
            "queryable",
            "host",
            "port",
            "user",
            "password",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "service_name",
            "sid",
            "node",
            "team_ids",
            "instance_tag_ids",
        )
        extra_kwargs = {"password": {"write_only": True, "required": False}}
```

- [ ] **Step 4: Commit**

```bash
git add backend/api_instances/serializers.py
git commit -m "feat: expose monitoring_enabled and queryable in instance serializers"
```

---

### Task 3: Add queryable guards to query API

**Files:**
- Modify: `backend/api_queries/views.py:249,411,452`

**Interfaces:**
- Consumes: `Instance.queryable` (Task 1)
- Produces: `QueryInstanceList` filters by `queryable=True`; `QueryExecute` and `QueryDescribe` reject non-queryable instances

- [ ] **Step 1: Filter QueryInstanceList by queryable**

In `backend/api_queries/views.py`, in `QueryInstanceList.get` (line 447), add `.filter(queryable=True)` after `filter_agent_runnable_instances(queryset)`:

```python
    def get(self, request):
        _require_query_page_access(request)

        instance_type = request.query_params.get("type")
        db_type = request.query_params.getlist("db_type")
        if not db_type:
            db_type = request.query_params.getlist("db_type[]")

        queryset = user_instances(
            request.user,
            type=instance_type,
            db_type=db_type or None,
            tag_codes=["can_read"],
        )
        queryset = filter_agent_runnable_instances(queryset).filter(queryable=True).order_by("instance_name")
        serializer = QueryInstanceSerializer(queryset, many=True)
        return success_response(data=serializer.data)
```

- [ ] **Step 2: Add guard to QueryExecute**

In `backend/api_queries/views.py`, in `QueryExecute.post` (after line 267, the `instance = user_instances(user).get(...)` block), add:

```python
        try:
            instance = user_instances(user).get(instance_name=instance_name)
        except Instance.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "Your group is not associated with this instance."}
            )

        if not instance.queryable:
            raise serializers.ValidationError(
                {"errors": "This instance is not queryable."}
            )
```

- [ ] **Step 3: Add guard to QueryDescribe**

In `backend/api_queries/views.py`, in `QueryDescribe.post` (after line 472, the `instance = user_instances(...).get(...)` block), add:

```python
        try:
            instance = user_instances(request.user, tag_codes=["can_read"]).get(
                pk=data["instance_id"]
            )
        except Instance.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": "The instance is not associated with your group."}
            )

        if not instance.queryable:
            raise serializers.ValidationError(
                {"errors": "This instance is not queryable."}
            )
```

- [ ] **Step 4: Commit**

```bash
git add backend/api_queries/views.py
git commit -m "feat: gate query API on instance.queryable flag"
```

---

### Task 4: Add `monitoring_enabled` and `queryable` to frontend types

**Files:**
- Modify: `frontend/src/lib/api.ts:399-417`

**Interfaces:**
- Produces: `InstanceCreatePayload` now includes `monitoring_enabled: boolean` and `queryable: boolean`

- [ ] **Step 1: Update InstanceCreatePayload**

In `frontend/src/lib/api.ts`, at line 399, add the two fields after `db_type`:

```typescript
export type InstanceCreatePayload = {
  instance_name: string
  type: string
  db_type: string
  monitoring_enabled: boolean
  queryable: boolean
  host: string
  port: number
  user: string
  password: string
  is_ssl: boolean
  verify_ssl: boolean
  db_name: string
  show_db_name_regex: string
  denied_db_name_regex: string
  charset: string
  service_name: string
  sid: string
  team_ids: number[]
  instance_tag_ids: number[]
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/lib/api.ts
git commit -m "feat: add monitoring_enabled and queryable to InstanceCreatePayload type"
```

---

### Task 5: Add checkboxes to instance editor form

**Files:**
- Modify: `frontend/src/features/inventory/pages/InventoryEditorPage.vue:54-92` (reactive form)
- Modify: `frontend/src/features/inventory/pages/InventoryEditorPage.vue:74-112` (resetForm / applyInstance)
- Modify: `frontend/src/features/inventory/pages/InventoryEditorPage.vue:225-261` (buildInstancePayload)
- Modify: `frontend/src/features/inventory/pages/InventoryEditorPage.vue:513-528` (template — SSL checkboxes area)

**Interfaces:**
- Consumes: `InstanceCreatePayload.monitoring_enabled`, `InstanceCreatePayload.queryable` (Task 4)
- Produces: Two checkboxes in the editor form, wired through reactive form, payload builder, and reset/apply helpers

- [ ] **Step 1: Add fields to reactive form**

In `frontend/src/features/inventory/pages/InventoryEditorPage.vue`, in the `form` reactive object (line 54), add after `db_type`:

```typescript
const form = reactive({
  instance_name: '',
  type: 'master',
  db_type: 'mysql',
  monitoring_enabled: true,
  queryable: false,
  host: '',
  port: 3306,
  user: '',
  password: '',
  is_ssl: false,
  verify_ssl: true,
  db_name: '',
  show_db_name_regex: '',
  denied_db_name_regex: '',
  charset: '',
  service_name: '',
  sid: '',
  team_ids: [] as number[],
  instance_tag_ids: [] as number[],
})
```

- [ ] **Step 2: Add fields to resetForm**

In `resetForm()` (line 74), add after `form.db_type`:

```typescript
function resetForm() {
  form.instance_name = ''
  form.type = 'master'
  form.db_type = 'mysql'
  form.monitoring_enabled = true
  form.queryable = false
  form.host = ''
  form.port = 3306
  form.user = ''
  form.password = ''
  form.is_ssl = false
  form.verify_ssl = true
  form.db_name = ''
  form.show_db_name_regex = ''
  form.denied_db_name_regex = ''
  form.charset = ''
  form.service_name = ''
  form.sid = ''
  form.team_ids = []
  form.instance_tag_ids = []
}
```

- [ ] **Step 3: Add fields to applyInstance**

In `applyInstance(instance: InstanceEditorRecord)` (line 94), add after `form.db_type`:

```typescript
function applyInstance(instance: InstanceEditorRecord) {
  form.instance_name = instance.instance_name
  form.type = instance.type
  form.db_type = instance.db_type
  form.monitoring_enabled = instance.monitoring_enabled
  form.queryable = instance.queryable
  form.host = instance.host
  form.port = instance.port
  form.user = instance.user
  form.password = ''
  form.is_ssl = instance.is_ssl
  form.verify_ssl = instance.verify_ssl
  form.db_name = instance.db_name
  form.show_db_name_regex = instance.show_db_name_regex
  form.denied_db_name_regex = instance.denied_db_name_regex
  form.charset = instance.charset
  form.service_name = instance.service_name ?? ''
  form.sid = instance.sid ?? ''
  form.team_ids = [...instance.team_ids]
  form.instance_tag_ids = [...instance.instance_tag_ids]
}
```

- [ ] **Step 4: Add fields to buildInstancePayload**

In `buildInstancePayload()` (line 243), add after `db_type`:

```typescript
function buildInstancePayload(): InstanceCreatePayload | null {
  const instanceName = form.instance_name.trim()
  const host = form.host.trim()

  if (!instanceName) {
    formError.value = 'Instance name cannot be blank.'
    return null
  }
  if (!host) {
    formError.value = 'Host cannot be blank.'
    return null
  }
  if (!Number.isFinite(form.port) || form.port <= 0) {
    formError.value = 'Port must be a positive integer.'
    return null
  }

  formError.value = ''
  return {
    instance_name: instanceName,
    type: form.type,
    db_type: form.db_type,
    monitoring_enabled: form.monitoring_enabled,
    queryable: form.queryable,
    host,
    port: form.port,
    user: form.user.trim(),
    password: form.password,
    is_ssl: form.is_ssl,
    verify_ssl: form.verify_ssl,
    db_name: form.db_name.trim(),
    show_db_name_regex: form.show_db_name_regex.trim(),
    denied_db_name_regex: form.denied_db_name_regex.trim(),
    charset: form.charset.trim(),
    service_name: form.service_name.trim(),
    sid: form.sid.trim(),
    team_ids: [...form.team_ids],
    instance_tag_ids: [...form.instance_tag_ids],
  }
}
```

- [ ] **Step 5: Add checkboxes to template**

In the template, after the `</div>` that closes the left column (line 529) and before the right-column `<div class="space-y-6">` (line 531), the two checkboxes go at the top of the right column. Add them inside the right column's `<div class="space-y-6">`, before the Teams section:

```html
          <div class="space-y-6">
            <div class="grid gap-4 md:grid-cols-2">
              <label class="flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
                <input v-model="form.monitoring_enabled" class="rounded border-slate-300" type="checkbox">
                <span>Enable monitoring</span>
              </label>

              <label class="flex items-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
                <input v-model="form.queryable" class="rounded border-slate-300" type="checkbox">
                <span>Enable SQL queries</span>
              </label>
            </div>

            <div class="grid min-w-0 gap-2">
              <span class="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Teams</span>
```

This puts monitoring and queryable checkboxes at the top of the right column, above the Teams selector.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/features/inventory/pages/InventoryEditorPage.vue
git commit -m "feat: add monitoring and queryable checkboxes to instance editor"
```

---

### Task 6: Investigate and fix agent dispatch pipeline

**Files:**
- Read (no changes expected unless bugs found):
  `backend/api_agents/services.py:794-909`,
  `backend/api_agents/dispatch.py:34-80`,
  `backend/api_agents/consumers.py`,
  `agent/internal/ws/client.go`

**Interfaces:**
- The pipeline: `run_agent_command_sync()` → `create_agent_command_for_instance()` → `command_capable_assignment_for_instance()` → `dispatch_agent_command()` → `notify_command_available()` (websocket) → `wait_for_agent_command()` (poll)

- [ ] **Step 1: Check that Django Channels is configured and Redis is reachable**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py shell -c "
from channels.layers import get_channel_layer
layer = get_channel_layer()
print('Channel layer:', type(layer).__name__)
# Test connection
import asyncio
asyncio.get_event_loop().run_until_complete(layer.send('__test_channel__', {'type': 'test'}))
print('Channel layer test: OK')
"
```

Expected: prints "Channel layer: RedisChannelLayer" and "Channel layer test: OK".

If the layer is None or unreachable, check `CHANNEL_LAYER_URL` / `CACHE_URL` env vars and ensure Redis is running.

- [ ] **Step 2: Check that agents register their websocket channel correctly**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py shell -c "
from api_agents.models import Agent
from api_agents.services import agent_active_websocket_channel, has_active_agent_websocket
for a in Agent.objects.filter(status='online', enabled=True):
    ch = agent_active_websocket_channel(a)
    print(f'Agent {a.name} (#{a.id}): channel={ch!r}, has_ws={has_active_agent_websocket(a)}')
print('Done')
"
```

Expected: online agents show a non-empty channel name and `has_ws=True`. If agents show empty channels, there may be a disconnect/connect issue in the consumer.

- [ ] **Step 3: Check that command-capable assignments exist**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py shell -c "
from api_agents.services import command_capable_assignment_for_instance
from sql.models import Instance
for inst in Instance.objects.filter(db_type='mysql')[:5]:
    a = command_capable_assignment_for_instance(inst.id)
    print(f'{inst.instance_name}: assignment={a}')
"
```

Expected: each MySQL instance with a configured agent shows an assignment object. If none show, check agent assignment configuration.

- [ ] **Step 4: Review `send_agent_message` for channel send issues**

Read `backend/api_agents/dispatch.py:55-80`. Verify `active_agent_channel_name()` resolves correctly. Check `backend/api_agents/consumers.py` for the `connect` method — confirm it stores the channel name in agent metadata under the key `"active_websocket"` → `"channel_name"`.

- [ ] **Step 5: Verify the Go agent websocket client**

Read `agent/internal/ws/client.go`. Confirm the agent:
- Connects with `Authorization: Bearer <api_key>` header
- Handles `command.available` messages by fetching and executing the command
- Reports results back via REST

If the agent source has been modified, rebuild the agent binary and redeploy.

- [ ] **Step 6: Test a command dispatch manually**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py shell -c "
from api_agents.services import run_agent_command_sync
from api_agents.models import AgentCommandType
from sql.models import Instance
inst = Instance.objects.filter(db_type='mysql').first()
if inst:
    try:
        cmd = run_agent_command_sync(
            instance=inst,
            command_type=AgentCommandType.QUERY_EXECUTE,
            workflow_type='query.test',
            workflow_id='test:debug123',
            payload={'db_name': inst.db_name or '', 'sql': 'SELECT 1', 'limit': 1, 'max_execution_time_ms': 10000, 'submitted_by': 'test'},
            timeout_seconds=30,
        )
        print('Command succeeded:', cmd.result)
    except Exception as exc:
        print('Command failed:', exc)
else:
    print('No MySQL instances found')
"
```

If this succeeds, the pipeline is working. If it fails, note the exact error message and trace back to the failing step.

- [ ] **Step 7: Commit any fixes found**

If no changes needed, skip this step. Otherwise:

```bash
git add <fixed-files>
git commit -m "fix: resolve agent command dispatch issue"
```

---

### Task 7: Verification

**Files:**
- None modified (verification only)

- [ ] **Step 1: Run Black formatting check**

```bash
black --check backend
```

Expected: "All done!" or "would reformat <files>" — if the latter, run `black backend` to auto-format.

- [ ] **Step 2: Run migration drift check**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql --check
```

Expected: "No changes detected".

- [ ] **Step 3: Run frontend build**

```bash
npm run build
```

Run from `frontend/`. Expected: exit code 0 with no TypeScript errors.

- [ ] **Step 4: Run relevant Django tests**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py test api_instances.tests api_queries.tests --verbosity=2
```

Expected: all tests pass.

- [ ] **Step 5: Commit verification results (if any CI config changes needed)**

If no changes, final verification is complete.

```bash
git status
git log --oneline -5
```
