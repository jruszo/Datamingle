import { fileURLToPath, URL } from 'node:url'

import { defineConfig, loadEnv } from 'vite'
import tailwindcss from '@tailwindcss/vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')

  return {
    plugins: [tailwindcss(), vue()],
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url)),
        '@enterprise-feature-modules': fileURLToPath(
          new URL('./src/extensions/enterprise-feature-modules.ts', import.meta.url),
        ),
      },
    },
    server: {
      proxy: {
        '/api': {
          target: env.VITE_BACKEND_PROXY_TARGET || 'http://localhost:9123',
          changeOrigin: false,
          ws: true,
        },
      },
    },
  }
})
