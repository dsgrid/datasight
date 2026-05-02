import path from "node:path";
import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [svelte(), tailwindcss()],
  base: "/static/",
  resolve: {
    alias: {
      $lib: path.resolve(__dirname, "src/lib"),
    },
  },
  server: {
    proxy: {
      "/api": "http://localhost:8084",
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Plotly's prebuilt min bundle is ~4.6 MB and is already loaded
    // dynamically by PlotlyChart.svelte, so the chunk-size warning is noise.
    chunkSizeWarningLimit: 5000,
  },
});
