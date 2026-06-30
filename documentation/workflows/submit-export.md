# Submit Data Export

Use export workflows for governed extraction of query results.

## Steps

1. Open `Workflows`.
2. Choose `New Export Request`.
3. Select team, instance, database, and schema when applicable.
4. Choose an export format.
5. Enter a `SELECT` or `WITH` query.
6. Run export validation.
7. Review row-count and threshold checks.
8. Submit the export workflow.

## Formats

Supported export formats include CSV, TSV, SQL, and XLSX.

The export file is available only after successful workflow completion and only to users with download permission.

Export workflows require a queryable service and an online command-capable
agent. If the service has a workflow policy, Datamingle uses it for approval.
Policy-free export services can use the team-level audit setting.
