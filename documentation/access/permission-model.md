# Permission Model

## Permission Groups

Permission groups grant UI and action permissions, such as query submission, workflow review, archive management, audit visibility, or system settings.

## Resource Groups

Resource groups are shown as teams in the current SPA. They connect users to
database instances. A user must normally share a team with an instance to see or
request work against that instance.

Service-level query and workflow enablement still applies after team visibility.
A visible service will not appear in online query, export, or DDL/DML selectors
unless the corresponding service capability is enabled and an eligible agent is
online.

## Instance Access Levels

Temporary instance access levels include:

- Query only,
- Query + DML,
- Query + DML + DDL.

These access levels control what a user can submit for the selected instance.
