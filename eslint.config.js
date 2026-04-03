export default [
  {
    files: ["src/datasight/web/static/**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "script",
      globals: {
        // Browser globals
        window: "readonly",
        document: "readonly",
        localStorage: "readonly",
        fetch: "readonly",
        console: "readonly",
        navigator: "readonly",
        crypto: "readonly",
        requestAnimationFrame: "readonly",
        setTimeout: "readonly",
        URL: "readonly",
        Blob: "readonly",
        TextDecoder: "readonly",
        // Libraries loaded via CDN
        marked: "readonly",
        hljs: "readonly",
        DOMPurify: "readonly",
      },
    },
    rules: {
      "no-undef": "error",
      "no-unused-vars": ["warn", { vars: "local", args: "none" }],
      "no-redeclare": "error",
      "eqeqeq": ["warn", "smart"],
      "no-debugger": "error",
      "no-duplicate-case": "error",
      "no-empty": ["warn", { allowEmptyCatch: true }],
      "no-constant-condition": "warn",
    },
  },
];
