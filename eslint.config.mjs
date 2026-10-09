import js from "@eslint/js";
import tseslint from "typescript-eslint";

// Scoped to the web packages and their example. The rest of this monorepo
// (sdks/*, packages/cli, packages/vscode) has never been linted and lints its
// own sources with `tsc --noEmit`; pointing these rules at it would fail on code
// that was not written against them.
const LINTED = ["packages/web-sdk/**/*.ts", "packages/web-sdk-server/**/*.ts"];

export default tseslint.config(
  {
    ignores: [
      "**/dist/**",
      "**/node_modules/**",
      "**/coverage/**",
      "docs/api/**",
      "sdks/**",
      "packages/cli/**",
      "packages/vscode/**",
      "packages/zapier/**",
      // Untracked local Python trees; they carry vendored browser JS that has
      // nothing to do with this monorepo's sources.
      "python/**",
      "**/.venv/**",
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: LINTED,
    rules: {
      "@typescript-eslint/consistent-type-imports": ["error", { prefer: "type-imports" }],
      "@typescript-eslint/no-non-null-assertion": "error",
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }],
      // The SDK crosses a network boundary, so `unknown` from JSON is normal and
      // narrowing happens at the parse site rather than through the type system.
      "@typescript-eslint/no-explicit-any": "error",
      eqeqeq: ["error", "always", { null: "ignore" }],
      "no-console": "error",
    },
  },
  {
    // Tests reach into fakes and assert on loose shapes; the example is plain JS
    // that legitimately logs.
    files: ["**/test/**/*.ts", "examples/**/*.js"],
    rules: {
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-explicit-any": "off",
      "no-console": "off",
    },
  },
  {
    // The example is untyped JS, so its globals are declared here rather than
    // inferred from a tsconfig lib.
    files: ["examples/**/*.js"],
    languageOptions: {
      globals: {
        console: "readonly",
        document: "readonly",
        process: "readonly",
        URL: "readonly",
        fetch: "readonly",
        localStorage: "readonly",
        navigator: "readonly",
        window: "readonly",
      },
    },
  },
);
