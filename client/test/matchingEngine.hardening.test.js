import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";
import { MatchingEngine } from "../lib/matchingEngine.js";

const MARKET = "btc-usdt";

let engine;
let dir;

beforeEach(async () => {
  dir = await fs.mkdtemp(path.join(os.tmpdir(), "zync-harden-"));
  engine = new MatchingEngine(path.join(dir, "orders.json"));
});
afterEach(async () => {
  await fs.rm(dir, { recursive: true, force: true });
});

function now() {
  return Math.floor(Date.now() / 1000);
}

/**
 * The concurrency invariant we enforce (README "Matching-engine concurrency"):
 * after any serialized operation the order book is coherent —
 *   - every id in a book side maps to a live (open/partially_filled) limit order
 *     with positive remaining size;
 *   - every live resting limit order appears exactly once in its book side;
 *   - no order is left "open" with non-positive remaining size;
 *   - next_trade_id is ahead of every recorded trade id.
 */
function assertCoherent(eng) {
  const g = eng.inner;
  for (const [mid, book] of g.books) {
    for (const side of ["bid_ids", "ask_ids"]) {
      const ids = book[side];
      assert.equal(new Set(ids).size, ids.length, `${mid}.${side} has duplicate ids`);
      for (const id of ids) {
        const o = g.orders.get(id);
        assert.ok(o, `${side} id ${id} missing from orders`);
        assert.equal(o.order_type, "limit");
        assert.ok(
          o.status === "open" || o.status === "partially_filled",
          `resting order ${id} has status ${o.status}`,
        );
        assert.ok(o.size_remaining > 1e-12, `resting order ${id} has no size`);
        assert.equal(o.market_id, mid);
      }
    }
  }
  for (const o of g.orders.values()) {
    if ((o.status === "open" || o.status === "partially_filled")) {
      assert.ok(o.size_remaining > 1e-12, `order ${o.id} open but empty`);
      if (o.order_type === "limit") {
        const book = g.books.get(o.market_id);
        const inBook =
          !!book && (book.bid_ids.includes(o.id) || book.ask_ids.includes(o.id));
        assert.ok(inBook, `live resting order ${o.id} not in book`);
      }
    }
    if (o.status === "cancelled" || o.status === "filled") {
      assert.equal(o.size_remaining, 0, `terminal order ${o.id} has residual size`);
    }
  }
  for (const t of g.trades) {
    assert.ok(t.id < g.next_trade_id);
  }
}

describe("idempotent submission", () => {
  it("a repeated submit with the same key creates exactly one order and replays the result", async () => {
    const key = "client-key-abc";
    const first = await engine.submit(MARKET, "buy", "limit", 60000, 0.5, now(), {
      idempotencyKey: key,
    });
    const second = await engine.submit(MARKET, "buy", "limit", 60000, 0.5, now(), {
      idempotencyKey: key,
    });

    assert.equal(second.order.id, first.order.id, "same order id returned");
    assert.equal(second.idempotent_replay, true);

    const open = await engine.listOrders(MARKET, true);
    assert.equal(open.length, 1, "only one working order exists");
    assert.equal(open[0].id, first.order.id);
  });

  it("concurrent retries with the same key do not double-submit", async () => {
    const key = "race-key";
    const results = await Promise.all(
      Array.from({ length: 8 }, () =>
        engine.submit(MARKET, "sell", "limit", 70000, 1, now(), { idempotencyKey: key }),
      ),
    );
    const ids = new Set(results.map((r) => r.order.id));
    assert.equal(ids.size, 1, "all retries resolved to one order");
    const open = await engine.listOrders(MARKET, true);
    assert.equal(open.length, 1);
  });

  it("reusing a key with a different payload is a CONFLICT", async () => {
    const key = "reused";
    await engine.submit(MARKET, "buy", "limit", 60000, 0.5, now(), { idempotencyKey: key });
    await assert.rejects(
      () => engine.submit(MARKET, "buy", "limit", 61000, 0.5, now(), { idempotencyKey: key }),
      (e) => e.code === "CONFLICT",
    );
  });
});

describe("atomic cancel-all", () => {
  it("cancels every open / partially-filled order for a market and returns a correct summary", async () => {
    const a = await engine.submit(MARKET, "buy", "limit", 50000, 1, now());
    const b = await engine.submit(MARKET, "buy", "limit", 49000, 2, now());
    const c = await engine.submit(MARKET, "sell", "limit", 80000, 1, now());
    // an unrelated market must be untouched
    await engine.submit("eth-usdt", "buy", "limit", 2000, 1, now());

    const summary = await engine.cancelAll(MARKET);
    assert.equal(summary.cancelled_count, 3);
    assert.deepEqual(
      [...summary.cancelled_ids].sort(),
      [a.order.id, b.order.id, c.order.id].sort(),
    );

    assert.equal((await engine.listOrders(MARKET, true)).length, 0);
    assert.equal((await engine.listOrders("eth-usdt", true)).length, 1);
    assertCoherent(engine);
  });

  it("partially-filled orders are included", async () => {
    await engine.submit(MARKET, "sell", "limit", 60000, 1, now()); // resting ask
    const taker = await engine.submit(MARKET, "buy", "limit", 60000, 0.4, now()); // fills 0.4
    assert.equal(taker.order.status, "filled");

    const open = await engine.listOrders(MARKET, true);
    assert.equal(open.length, 1);
    assert.equal(open[0].status, "partially_filled");

    const summary = await engine.cancelAll(MARKET);
    assert.equal(summary.cancelled_count, 1);
    assert.equal((await engine.listOrders(MARKET, true)).length, 0);
    assertCoherent(engine);
  });
});

describe("serialization invariant under interleaving", () => {
  it("parallel submit + cancel + cancel-all always leave a coherent book", async () => {
    for (let iter = 0; iter < 15; iter++) {
      const d = await fs.mkdtemp(path.join(os.tmpdir(), "zync-race-"));
      const eng = new MatchingEngine(path.join(d, "o.json"));
      try {
        const seeded = [];
        for (let i = 0; i < 5; i++) {
          seeded.push(
            await eng.submit(MARKET, "sell", "limit", 60000 + i * 10, 1, now()),
          );
        }
        await Promise.all([
          eng.submit(MARKET, "buy", "limit", 60050, 3, now()), // crosses several asks
          eng.cancelAll(MARKET),
          eng.cancel(seeded[0].order.id).catch(() => {}), // may already be gone
          eng.submit(MARKET, "buy", "limit", 40000, 1, now()), // rests deep
        ]);
        assertCoherent(eng);
      } finally {
        await fs.rm(d, { recursive: true, force: true });
      }
    }
  });
});

describe("trading events", () => {
  it("emits accepted / fill / cancelled with per-market monotonic sequence", async () => {
    const seen = [];
    engine.bus.on("trading_event", (ev) => seen.push(ev));

    const resting = await engine.submit(MARKET, "sell", "limit", 60000, 1, now());
    const taker = await engine.submit(MARKET, "buy", "limit", 60000, 0.4, now());
    await engine.cancel(resting.order.id);

    const types = seen.map((e) => e.type);
    assert.ok(types.includes("order.accepted"));
    assert.ok(types.includes("order.filled")); // taker fully filled
    assert.ok(types.includes("order.partial_fill")); // maker partially filled
    assert.ok(types.includes("order.cancelled"));

    const seqs = seen.filter((e) => e.market_id === MARKET).map((e) => e.sequence);
    for (let i = 1; i < seqs.length; i++) {
      assert.equal(seqs[i], seqs[i - 1] + 1, "sequence increments by exactly 1");
    }

    void taker;
  });

  it("snapshot + getEventsSince support reconnect catch-up", async () => {
    const o1 = await engine.submit(MARKET, "buy", "limit", 50000, 1, now());
    const snap = await engine.getTradingSnapshot(MARKET);
    assert.equal(snap.open_orders.length, 1);
    assert.equal(snap.open_orders[0].id, o1.order.id);

    await engine.submit(MARKET, "buy", "limit", 49000, 1, now());
    const missed = await engine.getEventsSince(MARKET, snap.sequence);
    assert.ok(Array.isArray(missed));
    assert.ok(missed.every((e) => e.sequence > snap.sequence));

    // A sequence from the future forces a resync (null).
    assert.equal(await engine.getEventsSince(MARKET, snap.sequence + 999), null);
  });
});
