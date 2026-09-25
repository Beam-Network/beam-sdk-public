import { defineConfig } from "tsup";

const entry = {
  index: "src/index.ts",
  "web-agent/index": "src/web-agent/index.ts",
  "rooms/index": "src/rooms/index.ts",
  "broadcast/index": "src/broadcast/index.ts",
  "transfers/index": "src/transfers/index.ts",
};

export default defineConfig([
  // Package builds: ESM + CJS with declarations, per subpath entry.
  {
    entry,
    format: ["esm", "cjs"],
    target: "es2022",
    platform: "browser",
    dts: true,
    sourcemap: true,
    clean: true,
    splitting: true,
    treeshake: true,
    outExtension: ({ format }) => ({ js: format === "cjs" ? ".cjs" : ".js" }),
  },
  // Script-tag build. This is what the console's embed snippet loads, so the
  // global name and file path are part of the public contract: changing either
  // breaks every page that pasted the snippet.
  {
    entry: { beam: "src/global.ts" },
    format: ["iife"],
    globalName: "Beam",
    target: "es2020",
    platform: "browser",
    dts: false,
    sourcemap: true,
    minify: true,
    clean: false,
    outExtension: () => ({ js: ".min.js" }),
    footer: { js: "" },
  },
]);
