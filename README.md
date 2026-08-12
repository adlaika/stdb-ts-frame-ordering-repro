# Repro: TypeScript SDK delivers WebSocket frames out of order under compression

`WebsocketDecompressAdapter` binds an `async` handler directly to `ws.onmessage` and
`await`s decompression inside it. Nothing sequences concurrent invocations, so when
several frames are in flight the one that finishes decompressing first is delivered
first, and `DbConnection` applies transaction updates **out of order**.

Row updates are delete+insert on a primary key and do **not** commute, so a reordered
pair leaves the client cache holding an **earlier** value. Nothing repairs it: the row
is not written again, so it stays wrong for the life of the subscription. No error, no
disconnect, no reconnect.

## What this measures

Each iteration writes to **one** primary key `BURST` times, concurrently and un-awaited:

- writes `1 … BURST-1` carry ~2 MB of highly compressible padding — tiny on the wire, **expensive to inflate**
- write `BURST` carries empty padding — **trivial to inflate**

The server commits them in order, so the row always ends at `BURST`. On the client the
final tiny frame frequently overtakes a heavy earlier one, and the cache settles on
`BURST-1` and stays there.

> Padding is deliberately *compressible*. Decompression cost scales with **output**
> size, not wire size. Incompressible padding produces a large frame that gzip stores
> in raw blocks and that inflates at nearly memcpy speed — the timing gap never opens
> and the bug does not reproduce.

The run uses the **stock SDK** — no `withWSFn`, no instrumentation — so the result
cannot be an artefact of the harness. On a miss it opens a **fresh subscription** for
the same row, which returns the correct value, proving the server was right the whole
time and only the delta stream lost it.

## Layout

```
module/    Cargo.toml, src/lib.rs   — one public table, one delete+insert reducer
client/    package.json, repro.ts   — the measurement
```

## Run it

```bash
# 1. module
cd module
spacetime build
spacetime publish ordering-repro --server local
spacetime generate --lang typescript --out-dir ../client/module_bindings --module-path .

# 2. client
cd ../client
npm install

# 3. reproduce (compression gzip — the DEFAULT)
npx tsx repro.ts

# 4. control (compression none — the same branch has no `await`, so it stays ordered)
npx tsx repro.ts none
```

Knobs: `BURST` (default 8), `PADDING_BYTES` (default 8000000; 2000000 is plenty),
`ITERATIONS` (default 20), `REPRO_STDB_URI`, `REPRO_STDB_DB`.

## Results

SpacetimeDB standalone 2.8.0 on localhost, Node 22.23.1, `spacetimedb` npm 2.8.1,
`BURST=8 PADDING_BYTES=2000000`:

| run | result |
|---|---|
| `npx tsx repro.ts` (gzip, default) | **12/20 stale** |
| `npx tsx repro.ts none` (control) | **0/10 stale** |
| gzip, with the suggested fix applied | **0/20 stale** |

A miss prints:

```
iter 0: MISS  client=7  fresh-subscription=8  <- server holds 8, the client's stream lost the update
```

`client=7` is the tell: the cache is sitting on the **second-to-last** write, because
the last frame — the cheap one — was applied before the heavy one that preceded it.

## Verify the suggested fix

```bash
cd client
node apply-suggested-fix.mjs      # patches node_modules/spacetimedb/dist/index.mjs
npx tsx repro.ts                  # 0/20
node apply-suggested-fix.mjs --revert
```

The fix chains each frame onto the previous one, keeping decompression async:

```ts
set onmessage(handler: (msg: { data: Uint8Array }) => void) {
  let tail: Promise<void> = Promise.resolve();
  this.#ws.onmessage = (msg: MessageEvent) => {
    const raw = new Uint8Array(msg.data);
    tail = tail
      .then(async () => handler({ data: await this.#decompress(raw) }))
      .catch((e) => {
        console.error('[SpacetimeDB] WebSocket decompress failed, closing socket:', e);
        this.#ws.close();
      });
  };
}
```

The `.catch` keeps one bad frame from poisoning the chain and stalling the stream,
preserving the close-on-failure behaviour added for #5667.

Throughput is unaffected in practice: JS is single-threaded, and every frame must be
decompressed before it can be applied, so this changes only the interleaving.

## Notes

- Reproduces identically on `spacetimedb` **2.0.3** and **2.8.1**.
- The `#inboundQueue` drain loop added to `DbConnectionImpl` in 2.8.x does **not**
  address this. It preserves the order frames arrive *at the handler*; the race is
  upstream of it, so frames reach it already reordered.
- The browser bundles (`dist/index.browser.mjs`, `dist/sdk/index.browser.mjs`) contain
  the identical handler and the same gzip default, so browsers run the same code path.
  All numbers here were measured under Node.
- Reproduction rate rises with anything that widens timing variance between frames —
  CPU load, larger payload spread, higher latency. On a quiet machine with a small
  burst it can sit at 0 for many iterations, which makes this easy to mistake for
  absent.
