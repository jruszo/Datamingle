# Submit DDL Or DML

## Steps

1. Open `Workflows`.
2. Choose `New DDL Request` or `New DML Request`.
3. Select resource group, instance, and database.
4. Enter a clear workflow name.
5. Add an optional demand URL or ticket link.
6. Paste the SQL.
7. Run SQL check.
8. Review syntax type, warnings, errors, and affected-row estimates.
9. Submit the request.

## Fresh Check Requirement

Run SQL check again after changing:

- SQL content,
- instance,
- database,
- schema,
- target details.

The request cannot be submitted if the checked syntax type does not match the request page.

## MySQL Cluster Targets

For MySQL clusters, Datamingle only allows DDL and DML requests against the
detected master. Replicas are filtered from the target list or blocked with a
topology reason if submitted directly.

If the cluster master is not registered in Datamingle, or if discovery finds
multiple possible masters, DDL and DML are blocked until topology discovery can
identify one writable master. Add missing cluster services to Datamingle and
wait for the next inventory refresh, or trigger a refresh from the
infrastructure view.
