# SPA Bootstrap Parity Checklist

This file tracks the migration work needed to remove the legacy Bootstrap/Django-template frontend while keeping the SPA feature-complete for the product surfaces we still need.

## Status Legend

- `[ ]` Not started
- `[~]` In progress
- `[x]` Done
- `[!]` Blocked or needs decision

## Groundwork

- [x] Inventory current legacy routes/templates/endpoints and mark each as migrate, redirect, or delete.
- [x] Create and work on dedicated branch `feature/spa-bootstrap-parity`.

## Data Dictionary Under Inventory

- [x] Implement Data Dictionary DRF API for visible instances, databases/tables, table details, and export.
- [x] Add Data Dictionary SPA route under Inventory with browse/detail/export UI.
- [x] Add Data Dictionary API client methods and types in frontend shared API.
- [x] Add Data Dictionary backend tests for permissions, browsing, details, and export.
- [x] Add Data Dictionary frontend/e2e coverage for browse/detail/export states.

## Audit Visibility

- [x] Implement Audit DRF APIs for general audit, SQL workflow audit, query audit, and workflow logs.
- [x] Add Audit SPA feature module, routes, navigation, filters, tables, and detail links.
- [x] Add Audit API client methods and types in frontend shared API.
- [x] Add Audit backend tests for filters, permissions, and result shapes.
- [x] Add Audit frontend/e2e coverage for filtering and navigation links.

## Instance Operations

- [x] Implement Instance Operations DRF API for database management list/create/edit.
- [x] Implement Instance Operations DRF API for account list/create/edit/grant/reset/lock/delete.
- [x] Implement Instance Operations DRF API for parameter list/history/edit.
- [x] Implement Instance Operations DRF API for diagnostics process/tablespace/trx/locks/kill flow.
- [x] Add Instance Operations SPA feature module and primary navigation.
- [x] Add Database Management SPA page with list/create/edit flows.
- [x] Add Instance Account SPA page with list/create/edit/grant/reset/lock/delete flows.
- [x] Add Parameter Settings SPA page with editable parameters and history.
- [x] Add Session Diagnostics SPA page with tabs and kill confirmation flow.
- [x] Add Instance Operations API client methods and shared types.
- [x] Add Instance Operations backend tests for permissions and mutating actions.
- [x] Add Instance Operations frontend/e2e coverage for key actions.

## Remove Instead Of Migrate

- [x] Remove SQL optimization routes/endpoints/templates/navigation and related seeded permissions.
- [x] Remove Binlog/My2SQL routes/endpoints/templates/navigation and related seeded permissions.
- [x] Remove SchemaSync routes/endpoints/templates/navigation and related seeded permissions.
- [x] Remove legacy DBA principles route/template/navigation and seeded permission.
- [x] Update legacy navigation or redirects for migrated/removed URLs.

## Verification

- [x] Run backend targeted tests in `datamingle-app` container.
- [x] Run migration drift check with `docker exec datamingle-app python manage.py makemigrations sql --check`.
- [x] Run frontend `npm run build` from `frontend/`.
- [x] Run relevant Playwright specs or add/run focused specs for new SPA areas.
- [x] Review git diff for unintended changes and summarize remaining cleanup risks.

## Post-Verification Cleanup

- [x] Remove retired SQL optimization/Binlog/SchemaSync plugin wrappers and detached backend modules.
- [x] Remove My2SQL-only notification adapter and tests.
- [x] Remove retired binary downloads, mounts, and documentation references from Docker, Helm, README, and requirements.
- [x] Generate migration for removed retired permission metadata.
- [x] Convert migrated legacy page URLs to SPA redirects and delete their Bootstrap templates.

## Scope Decisions

- Keep: server inventory, data dictionary, instance operations, audit visibility, workflows, archives, queries, permissions, settings, and mailbox.
- Remove: SQL optimization, Binlog/My2SQL, SchemaSync, and legacy DBA principles.
- Data Dictionary belongs under Inventory as metadata browsing, separate from the online query console.
- Instance Operations must have separate navigation and permissions from basic Inventory management.
- Audit visibility is required before the Bootstrap views can be fully removed.

## Legacy Surface Inventory

### Migrate To SPA

- Data Dictionary: `/data_dictionary/`, `/data_dictionary/table_list/`, `/data_dictionary/table_info/`, `/data_dictionary/export/`.
- Instance database management: `/database/`, `/instance/database/list/`, `/instance/database/create/`, `/instance/database/edit/`.
- Instance account management: `/instanceaccount/`, `/instance/user/list`, `/instance/user/create/`, `/instance/user/edit/`, `/instance/user/grant/`, `/instance/user/reset_pwd/`, `/instance/user/lock/`, `/instance/user/delete/`.
- Parameter settings: `/instanceparam/`, `/param/list/`, `/param/history/`, `/param/edit/`.
- Session diagnostics: `/dbdiagnostic/`, `/db_diagnostic/process/`, `/db_diagnostic/create_kill_session/`, `/db_diagnostic/kill_session/`, `/db_diagnostic/tablespace/`, `/db_diagnostic/trxandlocks/`, `/db_diagnostic/innodb_trx/`.
- Audit: `/audit/`, `/audit/log/`, `/audit_sqlquery/`, `/query/querylog_audit/`, `/audit_sqlworkflow/`, `/sqlworkflow_list_audit/`, `/workflow/log/`.

### Already Covered Or Mostly Covered In SPA

- Dashboard: `/dashboard/` is covered by `/`.
- SQL workflows and export workflows: `/sqlworkflow/`, `/sqlexportworkflow/`, submit/detail/review/execute/download flows are covered by `/workflows`.
- Online Query: `/sqlquery/`, `/query/`, `/query/querylog/`, `/query/favorite/`, `/query/generate_sql/`, and table describe are covered by `/queries`.
- Query privilege management: `/queryapplylist/`, `/queryapplydetail/<id>/`, `/queryuserprivileges/`, and query privilege actions are covered by `/permission-management`.
- Archives: `/archive/`, `/archive/<id>/`, and archive actions are covered by `/archives`.
- Inventory list/edit/test connection: `/instance/`, `/instance/list/`, `/check/instance/` are covered by `/inventory`.
- System settings, user, permission group, resource group, profile, mailbox, login, and WorkOS flows are covered by existing SPA modules.

### Remove Instead Of Migrate

- SQL optimization: `/sqladvisor/`, `/slowquery/`, `/slowquery_advisor/`, `/slowquery/*`, `/query/explain/`.
- SQL analysis: `/sqlanalyze/`, `/sql_analyze/generate/`, `/sql_analyze/analyze/`.
- Binlog/My2SQL: `/my2sql/`, `/binlog/list/`, `/binlog/my2sql/`, `/binlog/del_log/`.
- SchemaSync: `/schemasync/`, `/instance/schemasync/`.
- DBA principles/docs: `/dbaprinciples/`.

Retired permission codenames are documented in `RETIRED_SQL_PERMISSION_CODENAMES`
and removed by `remove_retired_sql_permissions` in
`sql/migrations/0015_alter_permission_options.py`. Legacy Archery `src/init_sql`
bootstrap files were removed; schema and permission changes should go through
Django migrations.
