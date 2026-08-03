import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// The SPA is served by nginx as a separate container. In dev, proxy
// /v1 and /metrics to the engine so the browser talks to one origin and SSE works
// without CORS. VITE_API_BASE lets a built bundle target a remote engine if needed;
// at runtime nginx does the same proxying (see nginx.conf).
const API_TARGET = process.env.VITE_API_TARGET ?? "http://localhost:7300";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: {
      "/v1": { target: API_TARGET, changeOrigin: true },
      "/metrics": { target: API_TARGET, changeOrigin: true },
    },
  },
});
