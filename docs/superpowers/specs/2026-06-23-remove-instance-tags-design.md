# Remove Instance Tags & can_read Access Control — Design Spec

**Date:** 2026-06-23
**Status:** Approved

## Summary

Remove the `InstanceTag` model, the `instance_tag` M2M field on `Instance`, and all
tag-based access control (`can_read`/`can_write`) inherited from Archery. Replace
with team-based access (`resource_group`) for general instance access and the new
`queryable` BooleanField for query access gating.

## Database Changes

- Remove `instance_tag = models.ManyToManyField(InstanceTag, ...)` from `Instance` model.
- Remove the `InstanceTag` model class from `sql/models.py`.
- Generated via `makemigrations sql` (drops join table + tag table).

## Backend Changes

### `sql/utils/team.py`

- Remove `_grant_levels_for_tags()` function entirely.
- Remove `tag_codes` parameter from `user_has_group_instance_access()`. Simplify to team membership check only.
- `user_has_instance_query_access()`: replace `can_read` tag check with `instance.queryable` + team membership.
- Remove `tag_codes` parameter from `user_instances()`. Remove the tag-filtering UNION block.

### `sql/query_privileges.py`

- Line 59: Remove `can_read` tag check granting admin-level access. Keep `query_all_instances` permission + table-level privilege checks.
- Line 240: Remove `tag_codes=["can_read"]` from the legacy view's instance lookup.

### `api_queries/views.py`

- Line 277 (`QueryExecute`): Remove `tag_codes=["can_read"]` from permission check.
- Line 450 (`QueryInstanceList`): Remove `tag_codes=["can_read"]` from `user_instances()`. Keep `queryable=True` filter.
- Line 478 (`QueryDescribe`): Remove `tag_codes=["can_read"]` from instance lookup.
- Line 757 (`QueryPrivilegesApplyListCreate`): Remove `tag_codes=["can_read"]` from instance lookup.

### `api_workflows/views.py`

- Line 696 (`_export_submission_scope`): Remove `tag_codes=["can_read"]` from `user_instances()`.
- Line 728: Remove `instance_tag.filter(tag_code="can_read")` check. Replace with team membership.

### `api_workflows/serializers.py`

- Line 258-260: Remove `can_read` tag existence check from export workflow validation.

### `api_instances/serializers.py`

- Remove `instance_tag_ids` field, `get_instance_tag_ids()` method from:
  `InstanceEditorSerializer`, `InstanceListSerializer`, `InstanceCreateSerializer`, `InstanceDetailSerializer`.
- Remove `instance_tag.set()` calls from `create()`/`update()` in `InstanceCreateSerializer` and `InstanceDetailSerializer`.
- Delete: `InstanceTagLookupSerializer`, `InstanceTagManagementSerializer`, `InstanceTagCreateSerializer`, `InstanceTagUpdateSerializer`.

### `api_infrastructure/serializers.py`

- Remove `service_tag_ids` (source=`instance_tag`) from `DatabaseServiceWriteSerializer` fields and create/update.

### `api_instances/views.py`

- `InstanceMetadata.get`: Remove `"tags"` from the metadata payload.

### `api_instances/urls.py`

- Remove tag-related URL patterns.

## Frontend Changes

- Remove `instance_tag_ids` from `InstanceCreatePayload` type (`lib/api.ts`).
- Remove `service_tag_ids` from `DatabaseServicePayload` type (`features/infrastructure/api.ts`).
- Remove tag selector from `InventoryEditorPage.vue` (form reactive, resetForm, applyInstance, buildInstancePayload, template).
- Remove tag selector from `InfrastructurePage.vue` (serviceForm, resetServiceForm, openServiceDialog, template).
- Remove tag-related columns from inventory list if any.

## Test Changes

- Update `sql/utils/tests.py`, `sql/test_local_demo.py`, `sql/local_demo.py`,
  `api_core/legacy_tests.py` to remove `can_read` tag references.
- Update any test that creates/uses InstanceTag or `instance_tag_ids`.

## Verification

- `makemigrations sql --check` reports no drift.
- `black --check backend` passes.
- `npm run build` passes.
- Django test suite passes.
