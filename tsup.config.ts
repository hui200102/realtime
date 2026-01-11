import { defineConfig } from "tsup"

export default defineConfig({
  entry: ["src/client/index.ts", "src/server/index.ts", "src/shared/index.ts"],
  format: ["cjs", "esm"],
  splitting: false,
  clean: true,
  bundle: true,
  dts: true,
  treeshake: true,
  minify: false,
  sourcemap: false,
})
