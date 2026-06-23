# Remove Instance Tags — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the `InstanceTag` model, `instance_tag` M2M field, and all `can_read`/`can_write` tag-based access control, replacing with team-based access and the `queryable` field.

**Architecture:** Remove tag model + M2M field from the database, strip tag parameters from all utility functions (team.py, query_privileges.py), remove tag fields from all serializers, remove tag selectors from frontend forms, and clean up tests.

**Tech Stack:** Django 6.0 (migrations, DRF serializers), Vue 3 + TypeScript, MySQL.

## Global Constraints

- Do not hand-edit existing migration files.
- Generate all migrations via `makemigrations`.
- `black --check backend` must pass.
- `npm run build` from `frontend/` must pass.
- Django test suite must pass.
- Each task commits independently with a descriptive message.

---

### Task 1: Remove tag-based logic from team.py

**Files:**
- Modify: `backend/sql/utils/team.py:242-374`

**Interfaces:**
- Removes: `_grant_levels_for_tags()` function, `tag_codes` parameter from `user_has_group_instance_access()`, `user_instances()`
- Produces: `user_instances(user, type=None, db_type=None)` — no more `tag_codes` parameter

- [ ] **Step 1: Remove `_grant_levels_for_tags()`**

Delete the entire function (lines 242-250):

```python
def _grant_levels_for_tags(tag_codes):
    if not tag_codes:
        return None
    normalized = set(tag_codes)
    if "can_write" in normalized:
        return WRITE_ACCESS_LEVELS
    if "can_read" in normalized:
        return READ_ACCESS_LEVELS
    return set()
```

- [ ] **Step 2: Simplify `user_has_group_instance_access()`**

Replace the function (lines 267-283) — remove `tag_codes` parameter and tag filtering:

```python
def user_has_group_instance_access(user, instance):
    if user.is_superuser:
        return True
    if user.has_perm("sql.query_all_instances"):
        return True
    return Instance.objects.filter(
        pk=instance.pk, resource_group__in=user_groups(user)
    ).exists()
```

- [ ] **Step 3: Simplify `user_has_instance_query_access()`**

Replace line 287 — remove `tag_codes=["can_read"]`:

```python
def user_has_instance_query_access(user, instance):
    if not instance.queryable:
        return False
    if user_has_group_instance_access(user, instance):
        return True
    return temp_instance_access_level(user, instance) in READ_ACCESS_LEVELS
```

- [ ] **Step 4: Simplify `user_instances()`**

Replace the function (lines 340-375) — remove `tag_codes` parameter and the `if tag_codes:` block:

```python
def user_instances(user, type=None, db_type=None):
    temp_grant_instance_ids = list(
        active_instance_grants(user).values_list("instance_id", flat=True)
    )

    if user.has_perm("sql.query_all_instances"):
        instances = Instance.objects.all()
    else:
        instances = Instance.objects.filter(
            Q(resource_group__in=user_groups(user)) | Q(id__in=temp_grant_instance_ids)
        )
    if type:
        instances = instances.filter(type=type)
    if db_type:
        instances = instances.filter(db_type__in=db_type)
    return instances.distinct()
```

- [ ] **Step 5: Commit**

```bash
git add backend/sql/utils/team.py
git commit -m "refactor: remove tag-based access from team utilities"
```

---

### Task 2: Remove tag-based logic from query_privileges.py

**Files:**
- Modify: `backend/sql/query_privileges.py:59,240`

- [ ] **Step 1: Remove can_read check granting admin-level access**

In `query_priv_check()`, delete lines 58-64 (the `can_read` tag check block):

```python
    # Before (remove this entire block):
    if user_instances(user, tag_codes=["can_read"]).filter(pk=instance.pk).exists():
        priv_limit = int(SysConfig().get("admin_query_limit", 5000))
        result["data"]["limit_num"] = (
            min(priv_limit, limit_num) if limit_num else priv_limit
        )
        return result
```

The surrounding code should become:

```python
    if user.has_perm("sql.query_all_instances"):
        priv_limit = int(SysConfig().get("admin_query_limit", 5000))
        result["data"]["limit_num"] = (
            min(priv_limit, limit_num) if limit_num else priv_limit
        )
        return result

    # Only MySQL performs table-level permission checks.
    if instance.db_type == "mysql":
```

- [ ] **Step 2: Remove can_read from legacy instance lookup**

At line 240, change:

```python
    try:
        user_instances(request.user, tag_codes=["can_read"]).get(
            instance_name=instance_name
        )
    except Instance.DoesNotExist:
```

To:

```python
    try:
        user_instances(request.user).get(
            instance_name=instance_name
        )
    except Instance.DoesNotExist:
```

- [ ] **Step 3: Commit**

```bash
git add backend/sql/query_privileges.py
git commit -m "refactor: remove can_read tag from query privilege checks"
```

---

### Task 3: Remove tag-based logic from api_queries/views.py

**Files:**
- Modify: `backend/api_queries/views.py:277,450,478,757`

- [ ] **Step 1: QueryExecute (line 277) — remove can_read permission check**

Change lines 268-277. Remove the `can_read` check block:

```python
        if not (
            user.is_superuser
            or user.has_perm("sql.query_submit")
            or temp_instance_access_level(user, instance) in READ_ACCESS_LEVELS
        ):
```

(Remove the `or user_instances(user, tag_codes=["can_read"]).filter(...)` block)

- [ ] **Step 2: QueryInstanceList (line 450) — remove tag_codes**

Change line 450 from:

```python
            tag_codes=["can_read"],
```

To:

```python
```

(Remove the entire `tag_codes=["can_read"],` line)

- [ ] **Step 3: QueryDescribe (line 478) — remove tag_codes**

Change from:

```python
            instance = user_instances(request.user, tag_codes=["can_read"]).get(
                pk=data["instance_id"]
            )
```

To:

```python
            instance = user_instances(request.user).get(
                pk=data["instance_id"]
            )
```

- [ ] **Step 4: QueryPrivilegesApplyListCreate (line 757) — remove tag_codes**

Change from:

```python
            instance = user_instances(user, tag_codes=["can_read"]).get(
                instance_name=data["instance_name"]
            )
```

To:

```python
            instance = user_instances(user).get(
                instance_name=data["instance_name"]
            )
```

- [ ] **Step 5: Commit**

```bash
git add backend/api_queries/views.py
git commit -m "refactor: remove can_read tag from query API views"
```

---

### Task 4: Remove tag-based logic from api_workflows

**Files:**
- Modify: `backend/api_workflows/views.py:696,728`
- Modify: `backend/api_workflows/serializers.py:258-260`

- [ ] **Step 1: _export_submission_scope — remove tag_codes**

In `backend/api_workflows/views.py`, change line 696 from:

```python
    instances = (
        filter_agent_runnable_instances(user_instances(user, tag_codes=["can_read"]))
```

To:

```python
    instances = (
        filter_agent_runnable_instances(user_instances(user))
```

- [ ] **Step 2: _export_submission_scope — remove can_read tag check**

Remove lines 726-730 (the `can_read` tag existence check and direct_groups block):

```python
        if (
            _can_submit_export_workflow(user)
            and instance.instance_tag.filter(tag_code="can_read", active=True).exists()
        ):
            direct_groups = {
                team_id: team_name
                for team_id, team_name in instance.resource_group.filter(
                    is_deleted=0, team_id__in=direct_group_ids
                ).values_list("team_id", "team_name")
            }
            allowed_groups.update(direct_groups)
```

Replace with a simplified version that checks team membership instead:

```python
        if _can_submit_export_workflow(user):
            direct_groups = {
                team_id: team_name
                for team_id, team_name in instance.resource_group.filter(
                    is_deleted=0, team_id__in=direct_group_ids
                ).values_list("team_id", "team_name")
            }
            allowed_groups.update(direct_groups)
```

- [ ] **Step 3: Export workflow serializer — remove can_read tag check**

In `backend/api_workflows/serializers.py`, change lines 254-269. Replace the `can_read` tag check:

```python
            if not (
                actor.is_superuser
                or (
                    has_group_request_access
                    and instance.instance_tag.filter(
                        tag_code="can_read", active=True
                    ).exists()
                )
                or has_temporary_read_access
            ):
```

To:

```python
            if not (
                actor.is_superuser
                or has_group_request_access
                or has_temporary_read_access
            ):
```

- [ ] **Step 4: Commit**

```bash
git add backend/api_workflows/views.py backend/api_workflows/serializers.py
git commit -m "refactor: remove can_read tag from workflow views and serializers"
```

---

### Task 5: Remove tags from instance serializers, views, and URLs

**Files:**
- Modify: `backend/api_instances/serializers.py` (multiple classes)
- Modify: `backend/api_instances/views.py:709-718` (InstanceMetadata)
- Modify: `backend/api_instances/urls.py` (tag routes)

- [ ] **Step 1: InstanceEditorSerializer — remove instance_tag_ids**

Remove `get_instance_tag_ids` method and the field from `Meta.fields`:

```python
class InstanceEditorSerializer(serializers.ModelSerializer):
    team_ids = serializers.SerializerMethodField()
    node_name = serializers.CharField(source="node.name", read_only=True)

    def get_team_ids(self, obj):
        return list(
            obj.resource_group.values_list("team_id", flat=True).order_by("team_id")
        )

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "type",
            "db_type",
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
            "node",
            "node_name",
        )
```

- [ ] **Step 2: InstanceListSerializer — remove instance_tag_ids**

Remove `instance_tag_ids` field, `get_instance_tag_ids` method, and the field from `Meta.fields`.

- [ ] **Step 3: InstanceCreateSerializer — remove instance_tag_ids**

Remove `instance_tag_ids` PrimaryKeyRelatedField, and from `Meta.fields`. In `create()`, remove `instance_tags = validated_data.pop("instance_tag", [])` and `instance.instance_tag.set(instance_tags)`:

```python
    def create(self, validated_data):
        teams = validated_data.pop("resource_group", [])
        with transaction.atomic():
            instance = Instance.objects.create(**validated_data)
            instance.resource_group.set(teams)
        return instance

    class Meta:
        model = Instance
        fields = (
            "instance_name",
            "type",
            "db_type",
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
        )
```

- [ ] **Step 4: InstanceDetailSerializer — remove instance_tag_ids**

Remove `instance_tag_ids` PrimaryKeyRelatedField, from `Meta.fields`. In `update()`, remove `instance_tags = validated_data.pop("instance_tag", None)` and `instance.instance_tag.set(instance_tags)`.

- [ ] **Step 5: Delete tag serializers**

Delete the following classes entirely from `api_instances/serializers.py`:
- `InstanceTagLookupSerializer`
- `InstanceTagManagementSerializer`
- `InstanceTagCreateSerializer`
- `InstanceTagUpdateSerializer`

- [ ] **Step 6: InstanceMetadata view — remove tags**

In `backend/api_instances/views.py`, in `InstanceMetadata.get()` (line 709-718), remove `"tags"` from the payload dict. Change:

```python
        payload = {
            "instance_types": instance_types,
            "db_types": db_types,
            "nodes": InfrastructureNode.objects.filter(enabled=True).order_by(
                "name", "id"
            ),
            "tags": InstanceTag.objects.filter(active=True).order_by("tag_name", "id"),
            "teams": Team.objects.filter(is_deleted=0).order_by("team_name", "team_id"),
        }
```

To:

```python
        payload = {
            "instance_types": instance_types,
            "db_types": db_types,
            "nodes": InfrastructureNode.objects.filter(enabled=True).order_by(
                "name", "id"
            ),
            "teams": Team.objects.filter(is_deleted=0).order_by("team_name", "team_id"),
        }
```

Also remove `InstanceTag` from the imports at the top of `views.py` if it's no longer used.

- [ ] **Step 7: Remove tag URLs**

In `backend/api_instances/urls.py`, remove any URL patterns that route to tag-related views. Search for `tag` in the file and remove matching patterns.

- [ ] **Step 8: Commit**

```bash
git add backend/api_instances/serializers.py backend/api_instances/views.py backend/api_instances/urls.py
git commit -m "refactor: remove instance tags from serializers, views, and URLs"
```

---

### Task 6: Remove tags from infrastructure serializer

**Files:**
- Modify: `backend/api_infrastructure/serializers.py:398-402,537`

- [ ] **Step 1: Remove service_tag_ids from DatabaseServiceWriteSerializer**

Remove the `service_tag_ids` field definition (lines 398-402):

```python
    service_tag_ids = serializers.PrimaryKeyRelatedField(
        source="instance_tag",
        queryset=InstanceTag.objects.filter(active=True),
        many=True,
        required=False,
    )
```

Remove `"service_tag_ids"` from `Meta.fields`.

In `create()` (line 484), remove `instance_tags = validated_data.pop("instance_tag", [])` and `instance.instance_tag.set(instance_tags)`.

In `update()` (line 497), remove `instance_tags = validated_data.pop("instance_tag", None)` and the related set logic.

- [ ] **Step 2: Commit**

```bash
git add backend/api_infrastructure/serializers.py
git commit -m "refactor: remove instance tags from infrastructure serializer"
```

---

### Task 7: Remove InstanceTag model and M2M field

**Files:**
- Modify: `backend/sql/models.py`

- [ ] **Step 1: Remove `instance_tag` M2M field from Instance model**

Delete lines 697-699:

```python
    instance_tag = models.ManyToManyField(
        InstanceTag, verbose_name="Instance Tag", blank=True
    )
```

- [ ] **Step 2: Remove InstanceTag model class**

Find and delete the entire `InstanceTag` model class from `sql/models.py`.

- [ ] **Step 3: Generate migration**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql
```

- [ ] **Step 4: Apply migration**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py migrate sql
```

- [ ] **Step 5: Commit**

```bash
git add backend/sql/models.py backend/sql/migrations/*tag*
git commit -m "refactor: remove InstanceTag model and instance_tag M2M"
```

---

### Task 8: Frontend tag cleanup

**Files:**
- Modify: `frontend/src/lib/api.ts` (InstanceCreatePayload, InstanceInventoryRecord, InstanceEditorRecord, InstanceInventoryMetadata)
- Modify: `frontend/src/features/infrastructure/api.ts` (DatabaseServicePayload, DatabaseServiceRecord)
- Modify: `frontend/src/features/inventory/pages/InventoryEditorPage.vue`
- Modify: `frontend/src/features/infrastructure/pages/InfrastructurePage.vue`

- [ ] **Step 1: Remove from lib/api.ts types**

From `InstanceCreatePayload`: remove `instance_tag_ids: number[]`.

From `InstanceInventoryRecord`: remove `instance_tag_ids: number[]`.

From `InstanceInventoryMetadata`: remove the `tags` field (if present).

From `InstanceEditorRecord` (extends `InstanceCreatePayload`): automatically updated.

- [ ] **Step 2: Remove from infrastructure/api.ts types**

From `DatabaseServicePayload`: remove `service_tag_ids: number[]`.

From `DatabaseServiceRecord`: remove `service_tag_ids` (if present).

- [ ] **Step 3: Remove from InventoryEditorPage.vue**

Remove from the reactive `form`: `instance_tag_ids: [] as number[]`.

Remove from `resetForm()`: `form.instance_tag_ids = []`.

Remove from `applyInstance()`: `form.instance_tag_ids = [...instance.instance_tag_ids]`.

Remove from `buildInstancePayload()`: `instance_tag_ids: [...form.instance_tag_ids],`.

Remove the tag selector from the template (the entire `Instance Tags` dropdown section, lines 548-577 in the original unmodified file).

- [ ] **Step 4: Remove from InfrastructurePage.vue**

Remove from `serviceForm` reactive: `service_tag_ids: []`.

Remove from `resetServiceForm()`: `serviceForm.service_tag_ids = []`.

Remove from `openServiceDialog()` populate: `serviceForm.service_tag_ids = [...service.service_tag_ids]`.

Remove the tag selector from the service dialog template.

Also check the node form for any tag fields and remove them.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/lib/api.ts frontend/src/features/infrastructure/api.ts frontend/src/features/inventory/pages/InventoryEditorPage.vue frontend/src/features/infrastructure/pages/InfrastructurePage.vue
git commit -m "refactor: remove instance tags from frontend types and forms"
```

---

### Task 9: Test and demo data cleanup

**Files:**
- Modify: `backend/sql/utils/tests.py`
- Modify: `backend/sql/test_local_demo.py`
- Modify: `backend/sql/local_demo.py`
- Modify: `backend/api_core/legacy_tests.py`

These files reference `can_read` tags and `InstanceTag` in test setup. Search for `can_read`, `InstanceTag`, `tag_code`, `instance_tag`, `tag_codes` and remove or update all references.

- [ ] **Step 1: Clean up sql/utils/tests.py**

Search for `can_read` and `InstanceTag` references. Remove tag creation and tag-based assertions. Update `user_instances()` calls to not pass `tag_codes`.

- [ ] **Step 2: Clean up sql/test_local_demo.py**

Remove `can_read` tag assertion from tests.

- [ ] **Step 3: Clean up sql/local_demo.py**

Remove `"tags": ["can_read", "can_write"]` from demo data fixtures. Remove the `can_read` tag creation.

- [ ] **Step 4: Clean up api_core/legacy_tests.py**

Remove all `InstanceTag` creation and `can_read` references. Update assertions accordingly. This file has many references (30+ occurrences) — systematically remove all tag-related test code.

- [ ] **Step 5: Run tests to identify remaining references**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py test --verbosity=2 2>&1 | grep -i "tag\|InstanceTag"
```

Any remaining failures due to tag references must be fixed.

- [ ] **Step 6: Commit**

```bash
git add backend/sql/utils/tests.py backend/sql/test_local_demo.py backend/sql/local_demo.py backend/api_core/legacy_tests.py
git commit -m "test: remove InstanceTag and can_read references from tests"
```

---

### Task 10: Verification

- [ ] **Step 1: Black formatting check**

```bash
docker exec -w /opt/datamingle/backend datamingle-app black --check .
```

Expected: "All done!" or "310 files would be left unchanged."

- [ ] **Step 2: Migration drift check**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py makemigrations sql --check
```

Expected: "No changes detected in app 'sql'".

- [ ] **Step 3: Frontend build**

```bash
cd frontend && npm run build
```

Expected: No TypeScript errors. Build succeeds.

- [ ] **Step 4: Django tests**

```bash
docker exec -w /opt/datamingle/backend datamingle-app python manage.py test --verbosity=2
```

Expected: All tests pass, 0 failures.

- [ ] **Step 5: Final review**

```bash
git log --oneline -10
git status
```
