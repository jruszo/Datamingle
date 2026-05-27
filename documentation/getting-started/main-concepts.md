# Main Concepts

| Concept | What it means |
| --- | --- |
| Instance | A database service registered in Datamingle. |
| Resource group | A group connecting users to database instances. |
| Permission group | A role-like group granting Datamingle UI and action permissions. |
| Instance tag | A label used to classify instances for access and filtering. |
| Agent | A Datamingle worker installed near services to run commands. |
| Workflow | A governed request with check, review, execution, and audit history. |
| Mailbox | A personal queue for approvals, execution tasks, and completion notices. |
| Grant | Temporary or permanent access created through approval. |

## Typical Flow

1. Administrators register instances and assign them to resource groups.
2. Administrators install agents and assign services to agents.
3. Users request access or use existing resource-group access.
4. Users submit queries or governed workflows.
5. Reviewers approve or reject requests.
6. Agents execute approved work.
7. Datamingle records audit history.

