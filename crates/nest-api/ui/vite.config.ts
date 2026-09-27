import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { viteSingleFile } from "vite-plugin-singlefile";

// One self-contained index.html (scripts, styles and shaders inlined): the
// daemon embeds it with include_str!, and the UI must work on a LAN with no
// internet, so nothing is fetched from a CDN.
export default defineConfig({
  plugins: [svelte(), viteSingleFile()],
  build: {
    outDir: "../web",
    emptyOutDir: false,
    target: "es2022",
    assetsInlineLimit: 100_000_000,
    reportCompressedSize: false,
  },
  server: {
    // `npm run dev` against a running node: SPARKNEST_API=http://host:7411
    proxy: { "/v1": process.env.SPARKNEST_API ?? "http://127.0.0.1:7411" },
  },
});
