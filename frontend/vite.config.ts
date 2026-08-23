import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { readFileSync } from "node:fs";

const extensionPackage = JSON.parse(
  readFileSync(new URL("../extension/package.json", import.meta.url), "utf8"),
) as { version: string };

export default defineConfig({
  plugins: [
    react(),
    {
      name: "inject-extension-version",
      transformIndexHtml(html) {
        return html.replaceAll("__EXTENSION_VERSION__", extensionPackage.version);
      },
    },
  ],
  server: {
    // A bind-mounted source tree on a macOS or Windows host does not deliver
    // filesystem events into a Linux container, so the dev server in
    // docker-compose.dev.yml kept serving the modules it had read at boot: you
    // edit a file, reload, and see the old page, with nothing in the log to say
    // why. Polling is the only watcher that works across that boundary.
    //
    // Opt-in because it is a permanent syscall loop over every watched file,
    // which is a real cost to pay on a host that does not need it: compose sets
    // the variable, a native `npm run dev` keeps the free native watcher. The
    // interval is a floor on how stale HMR can be, not a delay added to it.
    watch: process.env.VITE_USE_POLLING === "true" ? { usePolling: true, interval: 300 } : undefined,
  },
  build: {
    modulePreload: { polyfill: false },
  },
});
