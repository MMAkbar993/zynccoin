import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import {
  TRADING_PROTOCOL_VERSION,
  TradingEventType,
  CancelReason,
} from "./tradingProtocol.js";
/** @typedef {'buy'|'sell'} SideStr */
/** @typedef {'limit'|'market'} OrderTypeStr */
/** @typedef {'open'|'partially_filled'|'filled'|'cancelled'} OrderStatusStr */

/**
 * @typedef {{
 *   id: string;
 *   market_id: string;
 *   side: SideStr;
 *   order_type: OrderTypeStr;
 *   price: number | null;
 *   size_original: number;
 *   size_remaining: number;
 *   status: OrderStatusStr;
 *   created_at: number;
 * }} OrderRec
 */

/**
 * @typedef {{
 *   id: number;
 *   market_id: string;
 *   price: number;
 *   size: number;
 *   maker_order_id: string;
 *   taker_order_id: string;
 *   taker_side: SideStr;
 *   ts: number;
 * }} StoredTrade
 */

/**
 * @typedef {{
 *   bid_ids: string[];
 *   ask_ids: string[];
 * }} MarketBook
 */

function innerEmpty() {
  return {
    /** @type {Map<string, OrderRec>} */
    orders: new Map(),
    /** @type {Map<string, MarketBook>} */
    books: new Map(),
    /** @type {StoredTrade[]} */
    trades: [],
    next_trade_id: 1,
  };
}

/**
 * @param {import('node:fs').PathLike} filePath
 */
function rebuildInner(p) {
  /** @type {Map<string, OrderRec>} */
  const orders = new Map();
  for (const o of p.orders) {
    orders.set(o.id, o);
  }

  /** @type {Map<string, MarketBook>} */
  const books = new Map();

  for (const o of orders.values()) {
    if (
      (o.status === "open" || o.status === "partially_filled") &&
      o.size_remaining > 1e-12 &&
      o.order_type === "limit"
    ) {
      insertResting(books, o.market_id, o);
    }
  }
  for (const b of books.values()) {
    sortIds(b, orders);
  }

  return {
    orders,
    books,
    trades: p.trades ?? [],
    next_trade_id: Math.max(1, p.next_trade_id ?? 1),
  };
}

/**
 * @param {Map<string, MarketBook>} books
 * @param {string} marketId
 * @param {OrderRec} o
 */
function insertResting(books, marketId, o) {
  const book = books.get(marketId) ?? { bid_ids: [], ask_ids: [] };
  if (o.side === "buy") {
    if (!book.bid_ids.includes(o.id)) book.bid_ids.push(o.id);
  } else {
    if (!book.ask_ids.includes(o.id)) book.ask_ids.push(o.id);
  }
  books.set(marketId, book);
}

/**
 * @param {MarketBook} book
 * @param {Map<string, OrderRec>} orders
 */
function sortIds(book, orders) {
  book.bid_ids.sort((a, b) => {
    const x = orders.get(a);
    const y = orders.get(b);
    if (!x || !y) return 0;
    const px = x.price ?? 0;
    const py = y.price ?? 0;
    const c = py - px;
    if (c !== 0) return c;
    return x.created_at - y.created_at;
  });
  book.ask_ids.sort((a, b) => {
    const x = orders.get(a);
    const y = orders.get(b);
    if (!x || !y) return 0;
    const px = x.price ?? 0;
    const py = y.price ?? 0;
    const c = px - py;
    if (c !== 0) return c;
    return x.created_at - y.created_at;
  });
}

/** How long a used idempotency key is remembered (in-memory only). */
const IDEMPOTENCY_TTL_MS = 10 * 60 * 1000;
/** Hard cap on retained idempotency keys to bound memory. */
const IDEMPOTENCY_MAX_ENTRIES = 1000;
/** Retained trading events per market for reconnect catch-up. */
const EVENT_LOG_MAX_PER_MARKET = 500;

/** Normalise an order record into the shape sent over the trading stream. */
function orderEventJson(o) {
  return {
    id: o.id,
    market_id: o.market_id,
    side: o.side,
    order_type: o.order_type,
    price: o.price == null ? null : o.price,
    size: o.size_original,
    size_remaining: o.size_remaining,
    status: o.status,
    created_at: o.created_at,
  };
}

/** Normalise a stored trade into the shape sent over the trading stream. */
function tradeEventJson(t) {
  return {
    id: t.id,
    market_id: t.market_id,
    price: t.price,
    size: t.size,
    maker_order_id: t.maker_order_id,
    taker_order_id: t.taker_order_id,
    taker_side: t.taker_side,
    ts: t.ts,
  };
}

/** Stable fingerprint of an order submission for idempotency-key reuse checks. */
function submissionFingerprint(marketId, side, orderType, price, size) {
  return JSON.stringify([marketId, side, orderType, price ?? null, size]);
}

/** Deep-ish clone of a submit result so stored idempotency results stay immutable. */
function cloneSubmitResult(r) {
  return {
    order: { ...r.order },
    trades: r.trades.map((t) => ({ ...t })),
    idempotent_replay: true,
  };
}

export class MatchingEngine {
  /**
   * @param {string} dataPath
   */
  constructor(dataPath) {
    this.path = path.isAbsolute(dataPath)
      ? dataPath
      : path.join(process.cwd(), dataPath);
    /** @type {ReturnType<typeof innerEmpty>} */
    this.inner = innerEmpty();

    // ── Concurrency: single serialized critical section ───────────────────────
    // Every state mutation (submit / cancel / cancelAll) and every coherent read
    // (snapshot / event catch-up) runs through this promise chain, so operations
    // never interleave. See README "Matching-engine concurrency".
    /** @type {Promise<unknown>} */
    this._chain = Promise.resolve();

    // ── Trading event stream ─────────────────────────────────────────────────
    /** @type {EventEmitter} */
    this.bus = new EventEmitter();
    this.bus.setMaxListeners(0);
    /** market_id -> last emitted sequence number */
    this._seq = new Map();
    /** market_id -> bounded array of recent events (ascending sequence) */
    this._eventLog = new Map();

    // ── Idempotency (in-memory, non-durable) ────────────────────────────────
    /** key -> { fingerprint, result, expiresAt } */
    this._idempotency = new Map();

    try {
      if (fsSync.existsSync(this.path)) {
        const s = fsSync.readFileSync(this.path, "utf8");
        const p = JSON.parse(s);
        this.inner = rebuildInner(p);
      }
    } catch (e) {
      console.warn(
        "matching persist corrupt, starting empty:",
        e?.message ?? e
      );
      this.inner = innerEmpty();
    }
  }

  async persist() {
    const g = this.inner;
    const file = {
      orders: [...g.orders.values()],
      trades: g.trades,
      next_trade_id: g.next_trade_id,
    };
    const json = JSON.stringify(file, null, 2);
    const dir = path.dirname(this.path);
    await fs.mkdir(dir, { recursive: true });
    await fs.writeFile(this.path, json, "utf8");
  }

  /** Persist without letting a disk failure abort the (already-applied) mutation. */
  async _persistSafe() {
    try {
      await this.persist();
    } catch (e) {
      console.error("matching persist failed:", e);
    }
  }

  // ── Serialization primitive ───────────────────────────────────────────────
  /**
   * Run `fn` as the sole active mutation/read against engine state. Calls are
   * FIFO-ordered; a rejection in one call does not stall the queue.
   * @template T
   * @param {() => Promise<T> | T} fn
   * @returns {Promise<T>}
   */
  _runExclusive(fn) {
    const result = this._chain.then(() => fn());
    // Keep the chain alive regardless of this call's outcome.
    this._chain = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  // ── Trading event emission ────────────────────────────────────────────────
  /**
   * Build the next event for a market, advancing its sequence counter.
   * MUST be called from inside the serialized critical section so sequence
   * order matches mutation order.
   */
  _makeEvent(marketId, type, data) {
    const seq = (this._seq.get(marketId) ?? 0) + 1;
    this._seq.set(marketId, seq);
    return {
      v: TRADING_PROTOCOL_VERSION,
      type,
      market_id: marketId,
      sequence: seq,
      ts: Date.now(),
      data,
    };
  }

  /** Append events to the per-market ring buffer and fan out to subscribers. */
  _publish(events) {
    for (const ev of events) {
      let log = this._eventLog.get(ev.market_id);
      if (!log) {
        log = [];
        this._eventLog.set(ev.market_id, log);
      }
      log.push(ev);
      if (log.length > EVENT_LOG_MAX_PER_MARKET) log.shift();
      this.bus.emit("trading_event", ev);
    }
  }

  /** Current per-market sequence (0 if the market has never had an event). */
  currentSequence(marketId) {
    return this._seq.get(marketId) ?? 0;
  }

  // ── Idempotency helpers ──────────────────────────────────────────────────
  _pruneIdempotency() {
    const now = Date.now();
    for (const [k, v] of this._idempotency) {
      if (v.expiresAt <= now) this._idempotency.delete(k);
    }
    // Bound memory: drop oldest insertions beyond the cap (Map preserves order).
    while (this._idempotency.size > IDEMPOTENCY_MAX_ENTRIES) {
      const oldest = this._idempotency.keys().next().value;
      if (oldest === undefined) break;
      this._idempotency.delete(oldest);
    }
  }

  // ── Public API (all serialized) ──────────────────────────────────────────
  /**
   * @param {string} marketId
   * @param {SideStr} side
   * @param {'limit'|'market'} orderType
   * @param {number | null} price
   * @param {number} size
   * @param {number} now
   * @param {{ idempotencyKey?: string | null }} [opts]
   */
  async submit(marketId, side, orderType, price, size, now, opts = {}) {
    return this._runExclusive(() =>
      this._submitLocked(marketId, side, orderType, price, size, now, opts),
    );
  }

  /** @param {string} id */
  async cancel(id) {
    return this._runExclusive(() => this._cancelLocked(id, CancelReason.USER));
  }

  /**
   * Cancel every open / partially-filled order for a market in one critical
   * section — atomic with respect to submit and single cancel.
   * @param {string} marketId
   * @returns {Promise<{ cancelled_count: number; cancelled_ids: string[] }>}
   */
  async cancelAll(marketId) {
    return this._runExclusive(() => this._cancelAllLocked(marketId));
  }

  /**
   * Coherent authoritative snapshot for a market, taken between mutations.
   * @param {string} marketId
   */
  async getTradingSnapshot(marketId) {
    return this._runExclusive(() => this._tradingSnapshotLocked(marketId));
  }

  /**
   * Events with sequence > `lastSequence` for reconnect catch-up, or `null` when
   * the gap is outside the retained window (caller should send a full snapshot).
   * @param {string} marketId
   * @param {number} lastSequence
   */
  async getEventsSince(marketId, lastSequence) {
    return this._runExclusive(() =>
      this._eventsSinceLocked(marketId, lastSequence),
    );
  }

  // ── Locked implementations ───────────────────────────────────────────────
  /**
   * @param {string} marketId
   * @param {SideStr} side
   * @param {'limit'|'market'} orderType
   * @param {number | null} price
   * @param {number} size
   * @param {number} now
   * @param {{ idempotencyKey?: string | null }} opts
   */
  async _submitLocked(marketId, side, orderType, price, size, now, opts) {
    const key =
      opts && opts.idempotencyKey != null && opts.idempotencyKey !== ""
        ? String(opts.idempotencyKey)
        : null;
    const fingerprint = submissionFingerprint(
      marketId,
      side,
      orderType,
      price,
      size,
    );

    if (key) {
      this._pruneIdempotency();
      const hit = this._idempotency.get(key);
      if (hit) {
        if (hit.fingerprint !== fingerprint) {
          throw new MatchError(
            "CONFLICT",
            "idempotency_key already used for a different order",
          );
        }
        // Replay the original result; no new order, no re-matching.
        return cloneSubmitResult(hit.result);
      }
    }

    if (size <= 0 || !Number.isFinite(size)) {
      throw new MatchError("BAD_REQUEST", "size must be positive");
    }
    if (orderType === "limit") {
      const p = price;
      if (p == null || p <= 0 || !Number.isFinite(p)) {
        throw new MatchError("BAD_REQUEST", "limit order requires price");
      }
    }

    const id = crypto.randomUUID();
    /** @type {OrderRec} */
    let taker = {
      id,
      market_id: marketId,
      side,
      order_type: orderType,
      price,
      size_original: size,
      size_remaining: size,
      status: "open",
      created_at: now,
    };

    /** @type {StoredTrade[]} */
    const newTrades = [];

    if (side === "buy" && orderType === "limit") {
      matchBuyLimit(this.inner, taker, /** @type {number} */ (price), newTrades, now);
    } else if (side === "buy" && orderType === "market") {
      matchBuyMarket(this.inner, taker, newTrades, now);
    } else if (side === "sell" && orderType === "limit") {
      matchSellLimit(this.inner, taker, /** @type {number} */ (price), newTrades, now);
    } else {
      matchSellMarket(this.inner, taker, newTrades, now);
    }

    if (orderType === "market") {
      const had = newTrades.length > 0;
      taker.size_remaining = 0;
      taker.status = had ? "filled" : "cancelled";
    } else if (taker.size_remaining <= 1e-12) {
      taker.size_remaining = 0;
      taker.status = "filled";
    } else {
      taker.status = newTrades.length === 0 ? "open" : "partially_filled";
    }

    this.inner.orders.set(taker.id, { ...taker });

    if (orderType === "limit" && taker.size_remaining > 1e-12) {
      const mid = taker.market_id;
      insertResting(this.inner.books, mid, taker);
      const ordersSnapshot = new Map(this.inner.orders);
      const book = this.inner.books.get(mid);
      if (book) sortIds(book, ordersSnapshot);
    }

    this.inner.trades.push(...newTrades);

    const outOrder = { ...this.inner.orders.get(taker.id) };
    const outTrades = [...newTrades];

    // ── Derive the trading-stream events for this submission ─────────────────
    const events = [];
    const takerJson = orderEventJson(outOrder);
    const takerTrades = outTrades.map(tradeEventJson);
    if (outOrder.status === "filled") {
      events.push(
        this._makeEvent(marketId, TradingEventType.ORDER_FILLED, {
          order: takerJson,
          trades: takerTrades,
        }),
      );
    } else if (outOrder.status === "cancelled") {
      // Unfilled market order.
      events.push(
        this._makeEvent(marketId, TradingEventType.ORDER_CANCELLED, {
          order: takerJson,
          reason: CancelReason.NO_LIQUIDITY,
        }),
      );
    } else {
      // open or partially_filled and now resting.
      events.push(
        this._makeEvent(marketId, TradingEventType.ORDER_ACCEPTED, {
          order: takerJson,
        }),
      );
      if (takerTrades.length > 0) {
        events.push(
          this._makeEvent(marketId, TradingEventType.ORDER_PARTIAL_FILL, {
            order: takerJson,
            trades: takerTrades,
          }),
        );
      }
    }
    // Maker-side updates (one event per distinct maker touched this submission).
    const makerIds = [...new Set(outTrades.map((t) => t.maker_order_id))];
    for (const mid of makerIds) {
      const mo = this.inner.orders.get(mid);
      if (!mo) continue;
      const mTrades = outTrades
        .filter((t) => t.maker_order_id === mid)
        .map(tradeEventJson);
      const type =
        mo.status === "filled"
          ? TradingEventType.ORDER_FILLED
          : TradingEventType.ORDER_PARTIAL_FILL;
      events.push(
        this._makeEvent(marketId, type, {
          order: orderEventJson(mo),
          trades: mTrades,
        }),
      );
    }

    const result = {
      order: /** @type {OrderRec} */ (outOrder),
      trades: outTrades,
    };

    if (key) {
      this._idempotency.set(key, {
        fingerprint,
        result: cloneSubmitResult(result),
        expiresAt: Date.now() + IDEMPOTENCY_TTL_MS,
      });
      this._pruneIdempotency();
    }

    await this._persistSafe();
    this._publish(events);

    return result;
  }

  /**
   * @param {string} id
   * @param {string} reason
   */
  async _cancelLocked(id, reason) {
    const g = this.inner;
    const o = g.orders.get(id);
    if (!o) throw new MatchError("NOT_FOUND", "not found");
    if (o.status === "filled" || o.status === "cancelled") {
      throw new MatchError("BAD_REQUEST", "order not cancellable");
    }
    if (o.size_remaining <= 1e-12) {
      throw new MatchError("BAD_REQUEST", "order not cancellable");
    }
    o.status = "cancelled";
    o.size_remaining = 0;
    const marketId = o.market_id;
    const book = g.books.get(marketId);
    if (book) {
      book.bid_ids = book.bid_ids.filter((x) => x !== id);
      book.ask_ids = book.ask_ids.filter((x) => x !== id);
    }
    const event = this._makeEvent(marketId, TradingEventType.ORDER_CANCELLED, {
      order: orderEventJson(o),
      reason,
    });
    await this._persistSafe();
    this._publish([event]);
  }

  /**
   * @param {string} marketId
   */
  async _cancelAllLocked(marketId) {
    const g = this.inner;
    const victims = [...g.orders.values()].filter(
      (o) =>
        o.market_id === marketId &&
        (o.status === "open" || o.status === "partially_filled") &&
        o.size_remaining > 1e-12,
    );

    const events = [];
    for (const o of victims) {
      o.status = "cancelled";
      o.size_remaining = 0;
      events.push(
        this._makeEvent(marketId, TradingEventType.ORDER_CANCELLED, {
          order: orderEventJson(o),
          reason: CancelReason.CANCEL_ALL,
        }),
      );
    }

    const book = g.books.get(marketId);
    if (book) {
      const ids = new Set(victims.map((v) => v.id));
      book.bid_ids = book.bid_ids.filter((x) => !ids.has(x));
      book.ask_ids = book.ask_ids.filter((x) => !ids.has(x));
    }

    if (victims.length > 0) {
      await this._persistSafe();
      this._publish(events);
    }

    return {
      cancelled_count: victims.length,
      cancelled_ids: victims.map((v) => v.id),
    };
  }

  /**
   * @param {string} marketId
   */
  _tradingSnapshotLocked(marketId) {
    const g = this.inner;
    const open_orders = [...g.orders.values()]
      .filter(
        (o) =>
          o.market_id === marketId &&
          (o.status === "open" || o.status === "partially_filled") &&
          o.size_remaining > 1e-12,
      )
      .sort((a, b) => a.created_at - b.created_at)
      .map(orderEventJson);

    const recent_fills = [];
    for (
      let i = g.trades.length - 1;
      i >= 0 && recent_fills.length < 50;
      i--
    ) {
      if (g.trades[i].market_id === marketId) {
        recent_fills.push(tradeEventJson(g.trades[i]));
      }
    }

    return {
      sequence: this._seq.get(marketId) ?? 0,
      open_orders,
      recent_fills,
    };
  }

  /**
   * @param {string} marketId
   * @param {number} lastSequence
   */
  _eventsSinceLocked(marketId, lastSequence) {
    const current = this._seq.get(marketId) ?? 0;
    if (!Number.isInteger(lastSequence) || lastSequence < 0) return null;
    if (lastSequence > current) return null; // client ahead of us → resync
    if (lastSequence === current) return [];
    const log = this._eventLog.get(marketId) ?? [];
    if (log.length === 0) return null;
    if (lastSequence < log[0].sequence - 1) return null; // gap beyond retention
    return log.filter((e) => e.sequence > lastSequence);
  }

  /**
   * @param {string} marketId
   */
  async restingDepth(marketId) {
    const g = this.inner;
    const book = g.books.get(marketId);
    if (!book) return { bids: [], asks: [] };

    const bidsAcc = new Map();
    for (const oid of book.bid_ids) {
      const o = g.orders.get(oid);
      if (o && o.size_remaining > 1e-12 && o.price != null) {
        const k = Math.floor(o.price * 1_000_000_000_000);
        bidsAcc.set(k, (bidsAcc.get(k) ?? 0) + o.size_remaining);
      }
    }
    let bids = [...bidsAcc.entries()].map(([k, sz]) => [k / 1_000_000_000_000, sz]);
    bids.sort((a, b) => b[0] - a[0]);

    const asksAcc = new Map();
    for (const oid of book.ask_ids) {
      const o = g.orders.get(oid);
      if (o && o.size_remaining > 1e-12 && o.price != null) {
        const k = Math.floor(o.price * 1_000_000_000_000);
        asksAcc.set(k, (asksAcc.get(k) ?? 0) + o.size_remaining);
      }
    }
    let asks = [...asksAcc.entries()].map(([k, sz]) => [k / 1_000_000_000_000, sz]);
    asks.sort((a, b) => a[0] - b[0]);

    return { bids, asks };
  }

  /**
   * @param {string} marketId
   * @param {number} limit
   */
  async recentTrades(marketId, limit) {
    const g = this.inner;
    const out = [];
    for (let i = g.trades.length - 1; i >= 0 && out.length < limit; i--) {
      const t = g.trades[i];
      if (t.market_id === marketId) out.push({ ...t });
    }
    return out;
  }

  /**
   * @param {string | undefined} marketId
   * @param {boolean} openOnly
   */
  async listOrders(marketId, openOnly) {
    const g = this.inner;
    let v = [...g.orders.values()].filter((o) =>
      marketId ? o.market_id === marketId : true
    );
    if (openOnly) {
      v = v.filter(
        (o) =>
          (o.status === "open" || o.status === "partially_filled") &&
          o.size_remaining > 1e-12
      );
    }
    v.sort((a, b) => a.created_at - b.created_at);
    return v;
  }
}

export class MatchError extends Error {
  /**
   * @param {'BAD_REQUEST'|'NOT_FOUND'|'CONFLICT'} code
   * @param {string} message
   */
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

/**
 * Duck-type MatchError so API routes work when Next.js bundles a separate
 * copy of this module from the custom server (instanceof would fail).
 * @param {unknown} e
 * @returns {e is MatchError}
 */
export function isMatchError(e) {
  if (e instanceof MatchError) return true;
  if (e == null || typeof e !== "object") return false;
  const code = /** @type {{ code?: unknown; message?: unknown }} */ (e).code;
  return code === "BAD_REQUEST" || code === "NOT_FOUND" || code === "CONFLICT";
}

/**
 * Map a MatchError code to an HTTP status. Kept next to isMatchError so API
 * routes stay consistent.
 * @param {string | undefined} code
 */
export function matchErrorStatus(code) {
  if (code === "NOT_FOUND") return 404;
  if (code === "CONFLICT") return 409;
  return 400;
}

/**
 * @param {ReturnType<typeof innerEmpty>} g
 * @param {StoredTrade[]} out
 * @param {string} marketId
 * @param {number} price
 * @param {number} size
 * @param {string} makerId
 * @param {string} takerId
 * @param {SideStr} takerSide
 * @param {number} now
 */
function recordTrade(g, out, marketId, price, size, makerId, takerId, takerSide, now) {
  const id = g.next_trade_id++;
  out.push({
    id,
    market_id: marketId,
    price,
    size,
    maker_order_id: makerId,
    taker_order_id: takerId,
    taker_side: takerSide,
    ts: now,
  });
}

/**
 * @param {ReturnType<typeof innerEmpty>} g
 * @param {OrderRec} taker
 * @param {number} limitPx
 * @param {StoredTrade[]} out
 * @param {number} now
 */
function matchBuyLimit(g, taker, limitPx, out, now) {
  const mid = taker.market_id;
  if (!g.books.has(mid)) g.books.set(mid, { bid_ids: [], ask_ids: [] });

  while (taker.size_remaining > 1e-12) {
    const book = g.books.get(mid);
    const oid = book?.ask_ids[0];
    if (!oid) break;

    const maker = g.orders.get(oid);
    const askPx =
      maker && maker.size_remaining > 1e-12 ? maker.price : null;
    if (askPx == null) {
      if (book && book.ask_ids[0] === oid) book.ask_ids.shift();
      continue;
    }
    if (askPx > limitPx) break;

    const tradeSz = Math.min(taker.size_remaining, maker.size_remaining);
    recordTrade(g, out, mid, askPx, tradeSz, oid, taker.id, "buy", now);

    maker.size_remaining -= tradeSz;
    taker.size_remaining -= tradeSz;
    let filled = false;
    if (maker.size_remaining <= 1e-12) {
      maker.size_remaining = 0;
      maker.status = "filled";
      filled = true;
    } else {
      maker.status = "partially_filled";
    }
    if (filled && book && book.ask_ids[0] === oid) book.ask_ids.shift();
  }
}

function matchBuyMarket(g, taker, out, now) {
  const mid = taker.market_id;
  if (!g.books.has(mid)) g.books.set(mid, { bid_ids: [], ask_ids: [] });

  while (taker.size_remaining > 1e-12) {
    const book = g.books.get(mid);
    const oid = book?.ask_ids[0];
    if (!oid) break;

    const maker = g.orders.get(oid);
    const askPx =
      maker && maker.size_remaining > 1e-12 ? maker.price : null;
    if (askPx == null) {
      if (book && book.ask_ids[0] === oid) book.ask_ids.shift();
      continue;
    }

    const tradeSz = Math.min(taker.size_remaining, maker.size_remaining);
    recordTrade(g, out, mid, askPx, tradeSz, oid, taker.id, "buy", now);

    maker.size_remaining -= tradeSz;
    taker.size_remaining -= tradeSz;
    let filled = false;
    if (maker.size_remaining <= 1e-12) {
      maker.size_remaining = 0;
      maker.status = "filled";
      filled = true;
    } else {
      maker.status = "partially_filled";
    }
    if (filled && book && book.ask_ids[0] === oid) book.ask_ids.shift();
  }
}

function matchSellLimit(g, taker, limitPx, out, now) {
  const mid = taker.market_id;
  if (!g.books.has(mid)) g.books.set(mid, { bid_ids: [], ask_ids: [] });

  while (taker.size_remaining > 1e-12) {
    const book = g.books.get(mid);
    const oid = book?.bid_ids[0];
    if (!oid) break;

    const maker = g.orders.get(oid);
    const bidPx =
      maker && maker.size_remaining > 1e-12 ? maker.price : null;
    if (bidPx == null) {
      if (book && book.bid_ids[0] === oid) book.bid_ids.shift();
      continue;
    }
    if (bidPx < limitPx) break;

    const tradeSz = Math.min(taker.size_remaining, maker.size_remaining);
    recordTrade(g, out, mid, bidPx, tradeSz, oid, taker.id, "sell", now);

    maker.size_remaining -= tradeSz;
    taker.size_remaining -= tradeSz;
    let filled = false;
    if (maker.size_remaining <= 1e-12) {
      maker.size_remaining = 0;
      maker.status = "filled";
      filled = true;
    } else {
      maker.status = "partially_filled";
    }
    if (filled && book && book.bid_ids[0] === oid) book.bid_ids.shift();
  }
}

function matchSellMarket(g, taker, out, now) {
  const mid = taker.market_id;
  if (!g.books.has(mid)) g.books.set(mid, { bid_ids: [], ask_ids: [] });

  while (taker.size_remaining > 1e-12) {
    const book = g.books.get(mid);
    const oid = book?.bid_ids[0];
    if (!oid) break;

    const maker = g.orders.get(oid);
    const bidPx =
      maker && maker.size_remaining > 1e-12 ? maker.price : null;
    if (bidPx == null) {
      if (book && book.bid_ids[0] === oid) book.bid_ids.shift();
      continue;
    }

    const tradeSz = Math.min(taker.size_remaining, maker.size_remaining);
    recordTrade(g, out, mid, bidPx, tradeSz, oid, taker.id, "sell", now);

    maker.size_remaining -= tradeSz;
    taker.size_remaining -= tradeSz;
    let filled = false;
    if (maker.size_remaining <= 1e-12) {
      maker.size_remaining = 0;
      maker.status = "filled";
      filled = true;
    } else {
      maker.status = "partially_filled";
    }
    if (filled && book && book.bid_ids[0] === oid) book.bid_ids.shift();
  }
}

/**
 * @param {string} dataPath
 */
export function createMatchingEngine(dataPath) {
  return new MatchingEngine(dataPath);
}
