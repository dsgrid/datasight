import path from "node:path";
import { defineConfig } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  plugins: [svelte({ hot: false })],
  resolve: {
    alias: {
      $lib: path.resolve(__dirname, "src/lib"),
    },
  },
  test: {
    include: ["tests/**/*.test.ts"],
    environment: "jsdom",
  },
});
