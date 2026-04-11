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
  },
});
