# Agents

Agents are Datamingle workers installed near database services. Datamingle sends command work to agents over websocket.

## Pages

- [Create And Install Agents](create-install.md)
- [Agent Assignments](assignments.md)
- [Agent Commands](commands.md)

## Statuses

| Status | Meaning |
| --- | --- |
| pending | Created but not yet connected. |
| online | Connected and available. |
| offline | Previously connected but not currently available. |
| disabled | Disabled by an administrator. |
| revoked | No longer trusted. |

