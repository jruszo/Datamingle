# AGENTS.md

## Workflow

- Create feature work on a dedicated branch. Prefer descriptive names such as `feature/<scope>` or `fix/<scope>`.
- Keep commits focused and non-interactive.
- Push the branch and open a PR when the user asks for it instead of stopping at local changes.

## Docker And Python

- Prefer running Django and other Python app commands inside the Archery container, not on the host.
- The local development app container is `datamingle-archery`.
- The local compose file in use is `src/docker-compose/docker-compose.local-arm.yml`.
- Rebuild the app container with:
  - `docker-compose -f src/docker-compose/docker-compose.local-arm.yml up -d --build archery`
- Run Django commands with:
  - `docker exec datamingle-archery python manage.py <command>`
- If container code is not bind-mounted for a file you changed, copy the file into the container with `docker cp` before running containerized commands.

## Migrations

- Do not hand-write Django migrations for normal model changes.
- Generate migrations by running `makemigrations` in the Archery container after syncing changed files if needed.
- Copy generated migration files back out to the host repo and commit them.
- Before finishing, verify migration drift with:
  - `docker exec datamingle-archery python manage.py makemigrations sql --check`

## Linting And Verification

- For Python formatting, use Black.
- The repo CI lint behavior matches:
  - `black --check .`
- If Black reports drift, format the affected files locally with `black <paths>` and rerun `black --check .`.
- For frontend verification, run:
  - `npm run build` from `frontend/`
- For backend verification, prefer targeted Django tests in the container over host execution.

## Frontend Preferences

- Do not add hero topbars or oversized hero headers for internal product pages unless explicitly requested.
- Prefer separate pages, drawers, or modals over cramming create/edit/detail workflows into one dashboard panel.
- Request creation should usually be a modal or a dedicated page, not an always-visible form at the top of a management screen.
- Detail and approval flows should open on selection, not occupy permanent space when no item is selected.
- Favor dense, task-oriented layouts over marketing-style presentation.

## UI Behavior Preferences

- When a list item has a review or approval workflow, clicking the item should open the detail flow.
- For management screens, separate list browsing from create flows and from review/detail flows.
- Default toward practical enterprise UI patterns over decorative presentation.
