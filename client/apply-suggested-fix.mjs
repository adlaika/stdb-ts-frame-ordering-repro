// Patch the INSTALLED spacetimedb package with the suggested ordering fix, so
// the fix can be verified against the published code rather than a replica.
//
//   node apply-suggested-fix.mjs            apply
//   node apply-suggested-fix.mjs --revert   restore
import { readFileSync, writeFileSync, existsSync, copyFileSync } from "node:fs";

const TARGET = "./node_modules/spacetimedb/dist/index.mjs";
const BACKUP = "./node_modules/spacetimedb/dist/index.mjs.orig";

const STOCK = `  set onmessage(handler) {
    this.#ws.onmessage = async (msg) => {
      let data;
      try {
        data = await this.#decompress(new Uint8Array(msg.data));
      } catch (e) {
        console.error(
          "[SpacetimeDB] WebSocket decompress failed, closing socket:",
          e
        );
        this.#ws.close();
        return;
      }
      handler({ data });
    };
  }`;

const FIXED = `  set onmessage(handler) {
    let tail = Promise.resolve();
    this.#ws.onmessage = (msg) => {
      const raw = new Uint8Array(msg.data);
      tail = tail
        .then(async () => handler({ data: await this.#decompress(raw) }))
        .catch((e) => {
          console.error(
            "[SpacetimeDB] WebSocket decompress failed, closing socket:",
            e
          );
          this.#ws.close();
        });
    };
  }`;

if (process.argv.includes("--revert")) {
  if (!existsSync(BACKUP)) {
    console.error("No backup found — nothing to revert.");
    process.exit(1);
  }
  copyFileSync(BACKUP, TARGET);
  console.log("Reverted to the stock SDK.");
  process.exit(0);
}

const src = readFileSync(TARGET, "utf8");
if (src.includes("let tail = Promise.resolve();")) {
  console.log("Already patched.");
  process.exit(0);
}
const hits = src.split(STOCK).length - 1;
if (hits !== 1) {
  console.error(
    `Expected exactly 1 occurrence of the stock handler, found ${hits}. ` +
      `The SDK version probably differs from 2.8.1 — patch it by hand.`,
  );
  process.exit(1);
}
if (!existsSync(BACKUP)) copyFileSync(TARGET, BACKUP);
writeFileSync(TARGET, src.replace(STOCK, FIXED));
console.log("Patched. Re-run `npx tsx repro.ts` — expect 0 stale.");
