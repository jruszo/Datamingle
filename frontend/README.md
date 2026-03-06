# Frontend

Vue 3 SPA using:

- Vite
- TypeScript
- Vue Router
- Pinia
- Tailwind CSS
- shadcn-vue

## Run with Archery Docker Backend

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
```
