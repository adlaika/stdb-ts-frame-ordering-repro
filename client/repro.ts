// Repro: the TypeScript SDK delivers WebSocket frames out of order under
// compression, leaving the client cache holding a stale row indefinitely.
//
// Each iteration issues a BURST of concurrent writes to the SAME primary key:
//
//   write_cell(id, 1..BURST-1, <2MB of compressible padding>)  <- tiny on the wire, SLOW to inflate
//   write_cell(id, BURST,      "")                             <- trivial to inflate
//
// All are in flight together. The server commits them in order, so the row
// always ends at BURST. On the client the final cheap frame finishes
// decompressing before a heavy earlier one, `handler({data})` is called for it
// first, and the row's delete+insert deltas are applied out of order -- leaving
// the client on BURST-1. Nothing repairs it: the row is not written again, so
// it stays wrong for the life of the subscription.
//
// The run uses the STOCK SDK -- no withWSFn, no instrumentation -- so the
// result cannot be an artefact of the harness. After a miss it opens a FRESH
// subscription, which returns 2, proving the server was correct all along and
// only the delta stream lost it.
//
// Usage:
//   npx tsx repro.ts            # default: compression gzip  -> misses
//   npx tsx repro.ts none       # control:  compression none -> no misses
import { readFileSync } from "node:fs";
import { DbConnection } from "./module_bindings";

const URI = process.env.REPRO_STDB_URI ?? "ws://127.0.0.1:3000";
const DB = process.env.REPRO_STDB_DB ?? "ordering-repro";
const COMPRESSION = (process.argv[2] ?? "gzip") as "gzip" | "none";
const ITERATIONS = Number(process.env.ITERATIONS ?? 20);
const PADDING_BYTES = Number(process.env.PADDING_BYTES ?? 2_000_000);
const BUDGET_MS = 3_000;
const BURST = Number(process.env.BURST ?? 8);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Highly COMPRESSIBLE padding.
 *
 *  Decompression cost scales with OUTPUT size, not wire size, so this is what
 *  makes one frame slow to decode: a few KB on the wire inflating back to many
 *  MB. (Incompressible padding is the wrong choice here -- gzip stores it in
 *  raw blocks, so a large frame decompresses at nearly memcpy speed and the
 *  timing gap the race needs never opens.) */
function compressible(bytes: number): string {
  const unit = "spacetimedb-frame-ordering-repro-padding-0123456789-";
  return unit.repeat(Math.ceil(bytes / unit.length)).slice(0, bytes);
}

function connect(): Promise<DbConnection> {
  return new Promise((resolve, reject) => {
    DbConnection.builder()
      .withUri(URI)
      .withDatabaseName(DB)
      .withCompression(COMPRESSION)
      .onConnect((conn: DbConnection) => {
        conn
          .subscriptionBuilder()
          .onApplied(() => resolve(conn))
          .subscribe(["SELECT * FROM cell"]);
      })
      .onConnectError((_c: unknown, e: Error) => reject(e))
      .build();
  });
}

const read = (conn: any, id: string): number | null => {
  for (const row of conn.db.cell.iter()) if (row.id === id) return Number(row.value);
  return null;
};

/** Wait for the client cache to reach `want`, or give up. */
async function waitFor(conn: any, id: string, want: number): Promise<number> {
  const t0 = Date.now();
  while (Date.now() - t0 < BUDGET_MS) {
    if (read(conn, id) === want) return Date.now() - t0;
    await sleep(10);
  }
  return -1;
}

/** Open a second, independent subscription and read the row through it. If the
 *  server were wrong, this would also return the stale value. */
function readViaFreshSubscription(conn: any, id: string): Promise<number | null> {
  return new Promise((resolve) => {
    conn
      .subscriptionBuilder()
      .onApplied(() => resolve(read(conn, id)))
      .subscribe([`SELECT * FROM cell WHERE id = '${id}'`]);
    setTimeout(() => resolve(null), 4000);
  });
}

async function main() {
  const sdkVersion = JSON.parse(
    readFileSync("./node_modules/spacetimedb/package.json", "utf8"),
  ).version;
  console.log(
    `compression=${COMPRESSION}  iterations=${ITERATIONS}  padding=${PADDING_BYTES}B  sdk=${sdkVersion}`,
  );
  const big = compressible(PADDING_BYTES);
  const conn: any = await connect();

  let misses = 0;
  let anomalies = 0;
  for (let i = 0; i < ITERATIONS; i++) {
    const id = `cell-${i}-${Math.random().toString(36).slice(2, 8)}`;

    // A burst of concurrent writes to ONE key. All but the last carry heavy
    // padding; the last is empty. None are awaited, so their subscription
    // updates arrive back-to-back and race in the client's decompressor: the
    // final tiny frame can be decoded and applied before a heavy earlier one.
    for (let k = 1; k < BURST; k++)
      void conn.reducers.writeCell({ id, value: BigInt(k), padding: big });
    void conn.reducers.writeCell({ id, value: BigInt(BURST), padding: "" });

    const ms = await waitFor(conn, id, BURST);
    if (ms >= 0) {
      console.log(`iter ${i}: ok (${ms}ms)`);
    } else {
      const stale = read(conn, id);
      const viaFresh = await readViaFreshSubscription(conn, id);
      if (viaFresh === BURST) {
        misses++;
        console.log(
          `iter ${i}: MISS  client=${stale}  fresh-subscription=${viaFresh}` +
            `  <- server holds ${BURST}, the client's stream lost the update`,
        );
      } else {
        // The server itself never reached BURST, so this says nothing about
        // frame ordering. Counting it would let a server-side or harness
        // problem masquerade as the SDK bug being demonstrated.
        anomalies++;
        console.log(
          `iter ${i}: ANOMALY  client=${stale}  fresh-subscription=${viaFresh}` +
            `  <- server did not reach ${BURST}; NOT counted as a lost update`,
        );
      }
    }
    await sleep(150);
  }

  console.log(
    `\nTOTAL compression=${COMPRESSION}: ${misses}/${ITERATIONS} stale` +
      (anomalies ? `  (${anomalies} anomalies, excluded)` : ""),
  );
  conn.disconnect();
  // Exit code states what happened, so this is usable in CI:
  //   0 = the expected outcome (gzip reproduced, or none stayed clean)
  //   1 = the unexpected outcome (gzip did not reproduce, or none went stale)
  const expected = COMPRESSION === "gzip" ? misses > 0 : misses === 0;
  process.exit(expected ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
