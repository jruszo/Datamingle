# Frontend

Vue 3 SPA using:

- Vite
- TypeScript
- Vue Router
- Pinia
- Tailwind CSS
- shadcn-vue

## Run with Datamingle Docker Backend

1. Make sure backend is running on `http://localhost:9123`.
2. Copy env file:

```sh
cp .env.example .env
```

3. Install dependencies and start dev server:

```sh
npm install
npm run dev
```

By default, frontend API calls go to `VITE_API_BASE_URL=/api` and Vite proxies `/api/*` to `VITE_BACKEND_PROXY_TARGET=http://localhost:9123`.

## Scripts

```sh
npm run dev
npm run build
npm run lint
npm run type-check
npm run e2e
```

## Playwright E2E

The Playwright smoke suite assumes the local Docker demo environment and resets it from zero before running:

```sh
npm run e2e
```

This command:

- tears down the local ARM Docker stack
- recreates the app and demo databases from scratch
- runs `smoke_local_demo`
- starts Vite on a fixed local port
- runs the Playwright Chromium smoke tests

For quick frontend iterations with an already running local Vite dev server on `127.0.0.1:5173`, rerun only the browser suite:

```sh
npm run e2e:test
```
