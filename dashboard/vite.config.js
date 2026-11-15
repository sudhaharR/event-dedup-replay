import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The dashboard calls the API as /api/... and Vite forwards it to the Spring Boot service.
// Same origin for the browser, so no CORS issues. In Docker (Stage 11) nginx does the same.
const target = process.env.API_TARGET || 'http://localhost:8080'
const proxy = {
  '/api': {
    target,
    changeOrigin: true,
    rewrite: (path) => path.replace(/^\/api/, ''),
    // Proxied calls are same-origin for the browser, so CORS does not apply. Drop the browser's
    // Origin header, otherwise the service would check it against its CORS list and answer 403
    // whenever the dashboard runs on a port that is not in that list.
    configure: (proxyServer) => {
      proxyServer.on('proxyReq', (proxyReq) => proxyReq.removeHeader('origin'))
    },
  },
}

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy },
  preview: { port: 4173, proxy },
})
