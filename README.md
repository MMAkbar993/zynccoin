# ZyncSwap — Senior Frontend & Backend Developer Assessment

Welcome to the ZyncSwap **Senior Frontend & Backend Developer** assessment.

ZyncSwap is a trading dashboard for monitoring live crypto markets, placing paper orders, and reviewing order-book activity. This repository is a **Next.js full-stack app** — React UI, REST API routes, a custom WebSocket server, and server-side business logic in `client/lib/`.

**Your work is limited to the `client/` workspace.** No blockchain, wallet, or smart-contract work is required.

We are evaluating **senior-level judgment**: protocol design, consistency under concurrency, operational safety, and clear trade-off documentation — not CRUD alone. Submit what you have when time is up.

---

# Submission write-up (Requirements 1–3)

> All work is confined to `client/`. The starter dashboard / markets / trade flows are unchanged in behaviour; existing tests still pass.

## Architecture — what changed

| Area | File(s) | Change |
|------|---------|--------|
| Trading event stream | `client/lib/tradingProtocol.js` (new), `client/lib/tradingWss.js` (new), `client/server.js` | New `/ws/trading` WebSocket. Events are sourced from the matching-engine mutation path, not from REST. |
| Matching-engine events | `client/lib/matchingEngine.js` | Emits `order.accepted / partial_fill / filled / cancelled` on an `EventEmitter` (`engine.bus`) with per-market monotonic sequence numbers + a bounded per-market event ring buffer for reconnect catch-up. Adds `getTradingSnapshot()` / `getEventsSince()`. |
| Concurrency | `client/lib/matchingEngine.js` | One promise-queue mutex (`_runExclusive`). `submit`, `cancel`, `cancelAll`, and coherent reads all run in the same critical section. |
| Idempotency | `client/lib/matchingEngine.js`, `client/app/api/v1/orders/route.js` | `idempotency_key` (header or body) checked *inside* the write path. In-memory TTL map. |
| Atomic cancel-all | `client/lib/matchingEngine.js`, `client/app/api/v1/orders/cancel-all/route.js` (new) | `POST /api/v1/orders/cancel-all { market_id }`. |
| Feed safety | `client/lib/feedSafety.js` (new), `client/lib/marketEngine.js` | `MarketEngine.feedHealth()` (real socket state + last real tick time). `evaluateFeedSafety()` / `assertOrderAllowed()` policy. |
| Circuit breaker | `client/app/api/v1/orders/route.js`, `client/app/api/v1/feed/route.js` (new) | Market-order submits rejected with `503` when the feed is unsafe. |
| Risk surface | `client/lib/dashboardSummary.js`, `client/src/views/DashboardPage.tsx` | Feed health, breaker state, open-order count, notional exposure. |
| Client | `client/src/hooks/useTradingStream.ts` (new), `client/src/hooks/useFeedHealth.ts` (new), `client/src/views/TradePage.tsx`, `client/src/components/dex/TradeEntryPanel.tsx` | Trade page open orders + fills are now push-based; "Cancel all" with confirm; market order UI disabled when the feed is unsafe. |
| API robustness | `client/app/api/v1/orders/*`, `markets/route.js`, `dashboard/summary/route.js`, `feed/route.js` | Consistent `MatchError → HTTP` mapping, no stack traces, unexpected errors logged and converted to safe `500`s. `export const dynamic = "force-dynamic"` on routes that read runtime engine singletons (they threw during `next build` prerender otherwise). |

---

## Trading WebSocket protocol

**Path:** `ws(s)://<host>/ws/trading` &nbsp;•&nbsp; **Version:** `1` (`v` field on every event)

### Why a second WebSocket (not extending `/ws/markets`)

`/ws/markets` is a *broadcast* channel: every client gets the same full market `overview` object every 2 s, with no subscription state. Trading needs the opposite shape — **per-connection subscription by `market_id`**, **per-market sequence numbers**, and a **consistent snapshot on subscribe/reconnect**. Bolting that onto the broadcast socket would entangle two very different lifecycles and risk regressing the working markets feed. A dedicated socket keeps each concern isolated, lets the two reconnect independently, and left `/ws/markets` byte-for-byte unchanged.

### Subscription

```jsonc
// client → server
{ "type": "subscribe",   "market_id": "btc-usdt", "last_sequence": 42 }  // last_sequence optional
{ "type": "unsubscribe", "market_id": "btc-usdt" }
{ "type": "ping" }
```

- **No `last_sequence`** (or after a detected gap) → server replies with a fresh **snapshot**.
- **`last_sequence` present and within the retained window** → server replays the missed events (`resumed` ack, then the events).
- **`last_sequence` outside the retained window / ahead of the server** → server falls back to a fresh **snapshot**.
- A repeat `subscribe` for a market already subscribed is treated as a resync (old listener dropped — never more than one per market).

### Snapshot

```jsonc
{
  "v": 1,
  "type": "snapshot",
  "market_id": "btc-usdt",
  "sequence": 42,                       // current per-market sequence
  "data": {
    "open_orders":  [ { "id": "...", "market_id": "btc-usdt", "side": "buy",
                        "order_type": "limit", "price": 64000, "size": 0.5,
                        "size_remaining": 0.3, "status": "partially_filled",
                        "created_at": 1788... } ],
    "recent_fills": [ { "id": 7, "market_id": "btc-usdt", "price": 64000, "size": 0.2,
                        "maker_order_id": "...", "taker_order_id": "...",
                        "taker_side": "buy", "ts": 1788... } ]
  }
}
```

### Incremental events

```jsonc
{ "v": 1, "type": "order.accepted",     "market_id": "btc-usdt", "sequence": 43, "ts": 1788, "data": { "order": {…} } }
{ "v": 1, "type": "order.partial_fill", "market_id": "btc-usdt", "sequence": 44, "ts": 1788, "data": { "order": {…}, "trades": [ {…} ] } }
{ "v": 1, "type": "order.filled",       "market_id": "btc-usdt", "sequence": 45, "ts": 1788, "data": { "order": {…}, "trades": [ {…} ] } }
{ "v": 1, "type": "order.cancelled",    "market_id": "btc-usdt", "sequence": 46, "ts": 1788, "data": { "order": {…}, "reason": "user" | "cancel_all" | "no_liquidity" } }
```

- One `submit` produces: an `order.accepted` for a resting order (**or** `order.filled` / `order.cancelled` for a taker that fully filled / a market order with no liquidity), an `order.partial_fill` when the taker partially fills and rests, **plus** one `order.partial_fill` / `order.filled` per distinct maker order it touched.
- One `cancel` → one `order.cancelled` (`reason: "user"`).
- `cancel-all` → one `order.cancelled` (`reason: "cancel_all"`) per affected order, all emitted inside the same critical section.

### Sequence numbers & gap handling

- `sequence` is **per-market**, a monotonically increasing integer starting at `1`, assigned inside the serialized critical section so **sequence order == mutation order**.
- The client tracks the last applied sequence. On an incremental event it computes `expected = last + 1`:
  - `sequence < expected` → duplicate/replay → **ignored**.
  - `sequence == expected` → **applied**, `last` advances.
  - `sequence > expected` → **gap** → the client discards local trading state, re-subscribes **without** `last_sequence`, and rebuilds from the fresh snapshot the server sends. (Explicit strategy: *invalidate + reload authoritative snapshot*, never patch over a hole.)
- The server retains the **last 500 events per market** (`EVENT_LOG_MAX_PER_MARKET`) for reconnect replay. Beyond that → snapshot.

### Reconnect behaviour

- Client reconnects with a fixed 1.5 s backoff and shows `connecting → connected / reconnecting / disconnected` (visible on the trade page's Open orders tab, plus a `resyncing` sub-state during a gap recovery).
- On every reconnect the client re-subscribes with its `last_sequence`; the server resumes (replay) or re-snapshots. **Either way the open-orders view is rebuilt from server truth, so it cannot retain permanently stale rows or silently drop fills.**
- **No REST poll fallback for open orders.** The old 4 s `GET /api/v1/orders` poll on the trade page was removed. The snapshot-on-(re)subscribe path is the recovery mechanism; keeping a parallel poll would reintroduce the drift the requirement is about. (The 2 s `GET /api/v1/markets/:id` poll that drives the chart / order book / trade prints is unrelated and stays.)

---

## Matching-engine concurrency

- **Mechanism:** a promise-queue mutex — `MatchingEngine._runExclusive(fn)` chains each call onto `this._chain`. Calls run strictly FIFO; a rejection in one call does not stall the queue. No external dependency.
- **Critical section:** `submit`, `cancel`, `cancelAll`, `getTradingSnapshot`, and `getEventsSince` all go through `_runExclusive`. Within a locked op, all in-memory mutation is synchronous; the only `await` is the trailing `persist()`, by which point state is already coherent. Trading events are built (sequence assigned) and published from inside the lock.
- **Invariant enforced (and asserted by tests):** *after any serialized operation the order book is coherent* —
  1. every id in a book side maps to a live (`open` / `partially_filled`) **limit** order with positive remaining size;
  2. every live resting limit order appears exactly once in its book side;
  3. no order is left `open` with non-positive remaining size; terminal orders (`filled` / `cancelled`) have `size_remaining === 0`;
  4. `next_trade_id` is ahead of every recorded trade id.
- **Why cancel-all is atomic:** it is a *single* `_runExclusive` call that cancels every matching order, rewrites the book once, persists once, and emits all `order.cancelled` events — it is **not** a loop over the cancel API / `cancel()`. No `submit` or `cancel` can interleave between the individual cancellations, so a concurrent `submit` either lands entirely before cancel-all (and is then cancelled by it) or entirely after (and survives) — never half-matched against a book cancel-all is midway through clearing.

---

## Idempotency

- **Key location:** `Idempotency-Key` HTTP header (preferred) **or** `idempotency_key` in the JSON body. Header wins. This matches the existing "everything in the JSON body" convention while also supporting the RFC-style header.
- **Where it's checked:** inside `_submitLocked`, i.e. inside the serialized critical section — so concurrent retries with the same key are serialized and the second sees the stored result. It cannot be bypassed by racing.
- **Behaviour on retry:** same key + same order payload → the original `{ order, trades }` result is replayed (`idempotent_replay: true`); **no** new order, **no** re-matching. Same key + *different* payload → `409` plain text (`idempotency_key already used for a different order`).
- **Storage / retention:** in-process `Map` on the engine. TTL **10 minutes** (`IDEMPOTENCY_TTL_MS`); hard cap **1000 entries** (`IDEMPOTENCY_MAX_ENTRIES`), oldest-first eviction; expired entries pruned on each keyed write.
- **Restart behaviour:** the map is **not** persisted. After a process restart the keys are gone — a retry that arrives post-restart *can* create a duplicate order. This is an explicit, documented limitation (see "Known limitations"); durable idempotency would need the key map written alongside `matching_state.json` or moved to Redis.
- **Memory:** bounded by the 1000-entry cap × (small result object) ≈ tens of KB worst case.

---

## Circuit breaker

- **Feed truth** (`MarketEngine.feedHealth()`, derived only from real state):
  - `feed_connected` — upstream ticker socket `readyState === OPEN`.
  - `feed_mode` — `"live"` when connected, `"simulated"` otherwise (the app runs on frozen seed prices when Binance is geo-blocked / down).
  - `last_tick_age_ms` — `Date.now() − _lastTickAt`, where `_lastTickAt` is set **only** in `_applyTicker` when a real upstream tick is applied. `null` until the first tick. No standalone timer.
  - `stale_threshold_ms` — **10 000 ms**. Binance `@miniTicker` pushes ≈ 1/s per symbol, so a healthy live feed refreshes well inside 10 s; that leaves headroom for transient hiccups before we distrust the mark.
  - `breaker_active` — `!feed_connected || stale`.
- **What makes the feed unsafe for market orders:** disconnected **or** simulated **or** stale past the threshold **or** never ticked.
- **Market-order policy:** when unsafe, `POST /api/v1/orders` with `order_type: "market"` is rejected with **HTTP 503** + a plain-text explanation, *before* the engine is touched. **503 (not 409)** because the condition is a transient upstream-dependency failure that the client should retry later, not a conflict with the request itself. Server enforcement is authoritative; the UI disable (`useFeedHealth` → disabled button + banner) is only a hint and fails safe (if `/api/v1/feed` is unreachable the button is disabled).
- **Limit-order policy:** limit orders remain allowed when the feed is unsafe. Their execution price is chosen by the operator and they rest in the local book rather than sweeping an untrusted mark — the risk the breaker addresses does not apply. (Blocking all submits would also be defensible; this policy keeps the desk able to work resting orders during an outage.)
- **Recovery:** fully automatic. `feedHealth()` is recomputed on every request/read, so the moment real ticks resume `breaker_active` clears and market orders are accepted again. Nothing to reset.

---

## Exposure

**Formula:** `notional_exposure = Σ ( order.size_remaining × mark_price(order.market_id) )` over all `open` / `partially_filled` orders.

- **Price used:** the current **mark price** from `MarketEngine.overview()` (`m.mark_price`) for each order's market — the same live value the markets table shows.
- **Limitations:**
  - Gross notional, not margin-at-risk or delta — it does **not** net buys against sells and ignores each order's own limit price.
  - Uses the current mark even for far-from-market resting limits, so it over- or under-states the value at which those orders would actually fill.
  - When the feed is simulated the marks are frozen seed values, so the figure is only as trustworthy as the feed (the same panel shows feed state next to it).
  - Not currency-normalised beyond "quote is USDT-like" — every seed market quotes USDT so this is fine for the starter data set.

---

## Testing

```bash
npm install
npm run test          # node --test client/test/*.test.js
npm run client:build  # production build (also type-checks)
npm run dev           # http://localhost:3000  (HTTP + both WebSockets)
```

**Automated tests** (`client/test/`) — 26 total, all passing:

- `lib.test.js` — the original 6 (unchanged).
- `matchingEngine.hardening.test.js`
  - idempotent double-submit → exactly one working order, original result replayed;
  - 8 concurrent same-key retries → one order;
  - key reuse with a different payload → `CONFLICT`;
  - atomic cancel-all → correct `cancelled_count` / `cancelled_ids`, other markets untouched, partially-filled orders included, book coherent;
  - **interleaving:** 15 iterations of parallel `submit` + `cancel` + `cancelAll` + `submit`, asserting the coherence invariant every time;
  - trading events emitted with per-market monotonic sequence; snapshot + `getEventsSince` catch-up incl. future-sequence → resync.
- `feedSafety.test.js` — policy for every unsafe combination, limit-vs-market, unsafe→healthy recovery, **and** the real `MarketEngine` (never connected) reporting `simulated` / breaker-active and blocking market orders.
- `dashboardRisk.test.js` — exposure = Σ(size_remaining × mark); breaker state + reason surfaced; legacy `feed_connected` field preserved.

**Manual verification performed** (dev server, live Binance feed):

1. `/dashboard`, `/markets`, `/trade/btc-usdt` all load (200), no console/terminal errors.
2. `/ws/markets` still delivers `snapshot` + `tick` (unchanged).
3. `/ws/trading` delivers `snapshot` on subscribe.
4. Limit submit → `order.accepted` (seq 1) on the stream; row appears in Open orders.
5. Single cancel → `order.cancelled` (`reason: "user"`); row disappears.
6. `POST /orders/cancel-all` → one `order.cancelled` (`reason: "cancel_all"`) per order + `{ cancelled_count, cancelled_ids }`; rows disappear.
7. Duplicate `idempotency_key` → identical order id, `idempotent_replay: true`; different payload → `409`.
8. Reconnect with `last_sequence` at HEAD → `resumed`, no dupes. Reconnect with an absurd `last_sequence` → fresh `snapshot`.
9. cancel-all bad input → `400` (missing `market_id`) / `404` (unknown market), plain text.
10. Market order with a healthy feed → accepted.

**Manual step not automatable in this environment:** to see the breaker *reject* a live request, run the app with Binance unreachable (offline, or the feed geo-blocked — the terminal logs `running on simulated data`). Then `POST /api/v1/orders {order_type:"market"}` returns `503` and the trade page's market button is disabled with the reason shown. The policy itself is covered by `feedSafety.test.js` (including against the real engine).

---

## Known limitations

- **Idempotency is not durable.** Keys live only in the running process (see "Idempotency"). A retry after a restart can duplicate an order.
- **Sequence numbers are per-process.** After a restart `sequence` resets and the event ring buffer is empty; reconnecting clients get a fresh snapshot (their stale `last_sequence` is simply ahead of the server, which triggers the snapshot fallback). No fills are lost because the snapshot is authoritative, but a client cannot "replay across a restart".
- **Single-instance only.** `engine.bus` fan-out and the event log are in-memory. Running more than one server process would give each its own book / sequence space — there is no shared broadcast bus.
- **Persistence is best-effort.** `persist()` failures are logged, not surfaced; a crash between mutation and `writeFile` loses that mutation (pre-existing behaviour, unchanged).
- **`/ws/trading` is unauthenticated**, like `/ws/markets`. Any client can subscribe to any market's orders/fills. Acceptable for a single-desk paper-trading tool; not for multi-tenant.
- **The matching engine's market orders match only the local resting book** — they never reference `mark_price`. So in *this* engine an unsafe feed cannot actually mis-price a market fill; the breaker is enforced anyway because the requirement (and real venues) treat "fire a market order on an untrusted mark" as the hazard.
- **Exposure** is a gross estimate — see its "Limitations" above.

---

## What I would do with more production time

- **Durable event log** (append-only, e.g. SQLite or a segment file) so sequence numbers and replay survive restarts, and clients can catch up across deploys instead of full re-snapshot.
- **Durable / shared idempotency store** (persisted key map or Redis with native TTL) — removes the restart-duplicate window and makes it correct across instances.
- **Redis pub/sub (or NATS) for the trading bus** so `/ws/trading` can scale horizontally; the WS layer becomes stateless and any node can serve any subscription.
- **Distributed lock** (Redis Redlock / a single writer process) to keep the "one serialized critical section" invariant once there is more than one instance — the in-process promise queue only covers a single process.
- **AuthN/AuthZ on the trading stream** — token on the WS upgrade, per-account order scoping, and rate limiting on `subscribe`.
- **Observability** — counters for events emitted / gaps detected / resyncs / breaker trips / idempotency hits, and a histogram of `last_tick_age_ms`; structured logs instead of `console.*`.
- **Load / chaos testing** — many concurrent subscribers, forced socket drops mid-fill, and a fuzzer over interleaved submit/cancel/cancel-all asserting the coherence invariant (the unit test does 15 iterations; CI should do thousands).
- **Snapshot pagination / compaction** for markets with large open-order counts, and per-connection backpressure handling on slow WS clients.
- **Breaker refinement** — separate thresholds per market by liquidity, a short "cooldown" before re-enabling market orders after a flap, and an operator override with audit.

---

## Time Consideration

This assessment is scoped for **4–6 hours**. Prefer a correct, well-reasoned partial solution over a shallow full one. Use your README to record decisions, risks, and what you would finish next.

---

## Getting Started

You need **Node.js 18+** and **npm**.

```bash
# 1. Fork this repo and clone your fork
git clone https://github.com/YOUR_USERNAME/dashboard-assessment.git
cd dashboard-assessment
npm install

# 2. Set up environment variables (optional — defaults work for local dev)
cp .env.example .env

# 3. Start the app
npm run dev
# → http://localhost:3000
```

Verify the starter app loads:

- `/dashboard` — overview summary
- `/markets` — live market table + connection badge
- `/trade/btc-usdt` — chart, order book, server-backed open orders (HTTP polled today)

WebSocket feed today: `/ws/markets` (market overview ticks only).

---

## Project Structure

Your work lives under `client/`:

```
client/
├── app/
│   ├── (dex)/              # App pages (home, markets, trade, dashboard, …)
│   └── api/                # Next.js API routes
│       ├── health/
│       └── v1/
│           ├── config/
│           ├── dashboard/
│           ├── markets/
│           ├── orders/
│           └── …
├── lib/                    # Server-side logic
│   ├── marketEngine.js     # Live market prices (Binance feed + broadcast)
│   ├── matchingEngine.js   # In-memory order book & matching
│   ├── dashboardSummary.js # Dashboard aggregation helper
│   └── merge.js            # Price formatting helpers
├── server.js               # Custom server — HTTP + WebSocket at /ws/markets
├── test/                   # Existing unit tests (extend these)
└── src/
    ├── components/         # Reusable UI (charts, tables, panels)
    ├── context/            # React context (markets stream, favorites, paper trade)
    └── views/              # Page-level components
```

### Stack

| Layer | Technology |
|-------|------------|
| Frontend | Next.js 14 (App Router), React 18, TypeScript, Tailwind CSS |
| API | Next.js Route Handlers (`app/api/`) |
| Real-time | WebSocket (`/ws/markets`) via `server.js` + `lib/marketEngine.js` |
| Data | In-memory matching engine with JSON file persistence |
| Charts | klinecharts / lightweight-charts |

### Existing API endpoints (provided)

| Endpoint | Purpose |
|----------|---------|
| `GET /api/health` | Service health check |
| `GET /api/v1/config` | App name and runtime configuration |
| `GET /api/v1/dashboard/summary` | Aggregated dashboard metrics |
| `GET /api/v1/markets` | Full markets overview |
| `GET /api/v1/markets/:id` | Single market detail (order book, trades) |
| `GET /api/v1/orders` | List orders (`?market_id=`, `?open_only=true`) |
| `POST /api/v1/orders` | Submit a limit or market order |
| `DELETE /api/v1/orders/:id` | Cancel an open order |
| `WS /ws/markets` | Live market snapshots and price ticks |

Environment variables are read from the repo-root `.env` (see `.env.example`). Only `PORT` is commonly changed for local dev.

The starter already includes a dashboard, markets monitor enhancements, server-backed open orders (via HTTP polling), and `npm run test`. Treat that as production baseline — **regressions are scored harshly**. Your scored work is the three senior requirements below.

---

## Requirements

Implement Requirements **1–3**. Ambiguity is intentional: choose a design, document trade-offs, and defend them in your README.

### Requirement 1 — Real-time Trading Event Stream (40%)

**Problem:** Open orders and fills on the trade page are refreshed by HTTP polling. That does not scale for operators and drifts under load or reconnects.

**You must design and implement** a push-based trading event channel (extend `/ws/markets` or add a dedicated path such as `/ws/trading`). At minimum:

1. **Protocol** — Define a versioned message schema with:
   - An initial **snapshot** (open orders + recent fills for a subscribed `market_id`, or a documented multi-market alternative)
   - Incremental **events** for order accepted / partial fill / filled / cancelled
   - Monotonic sequence numbers (or equivalent) so clients can detect gaps
2. **Server** — Emit events from the matching engine path (submit / cancel / matches), not by scraping REST. Persist enough state that a reconnecting client can recover without silently dropping fills.
3. **Client** — Subscribe from the trade page (context or hook). On disconnect: show reconnecting state, then **re-hydrate** (snapshot and/or REST catch-up) so the UI does not show a stale book of orders. Prefer reducing or removing the open-orders poll once the stream is healthy; document the fallback if you keep a safety poll.
4. **README** — Document schema, subscription model, gap handling, and why you chose one WebSocket vs two.

**Acceptance criteria:**

- [ ] Submitting or cancelling an order updates the trade UI via the stream without relying on the old poll interval as the primary path
- [ ] Reconnect recovers to a consistent view (no permanent missing open orders / obvious stuck rows)
- [ ] Sequence gaps (or equivalent) are detected or mitigated; behavior is documented
- [ ] Connection / reconnect UX is visible on the trade surface
- [ ] Protocol and trade-offs are written up in the README

**Senior bar:** We care more about correctness under reconnect than pixel polish. A thin “websocket echo of the last REST response” without snapshot/gap thinking will not score well.

---

### Requirement 2 — Matching Engine Hardening (Idempotency, Atomic Cancel-All, Concurrency) (35%)

**Problem:** The matching engine is in-memory with async file persistence. Concurrent submit/cancel and naive client retries can double-submit or leave the book inconsistent.

**You must harden the write path:**

1. **Idempotent submit** — Support a client `idempotency_key` (header or body). Retries with the same key must not create a second live order; return the original result. Document TTL / storage limits of your key map.
2. **Atomic cancel-all** — `POST /api/v1/orders/cancel-all` (or equivalent) cancels every open / partially filled order for a `market_id` under a **single serialized critical section** shared with `submit` / `cancel` (mutex, async queue, or equivalent). Return a structured summary (`cancelled_count`, ids, etc.).
3. **API quality** — Plain-text `400` / `404` for bad input; JSON for success; never leak stack traces. Wire **Cancel all** on the trade Open orders tab with confirmation and stream/REST refresh.
4. **Tests** — Extend `npm run test` with non-trivial coverage, including at least:
   - Idempotent double-submit does not duplicate working orders
   - Cancel-all clears multiple opens
   - A concurrency / interleaving test (e.g. parallel submit + cancel-all) that asserts a coherent end state — document the invariant you enforce

**Acceptance criteria:**

- [ ] Idempotent retries are safe and covered by tests
- [ ] Cancel-all is atomic w.r.t. other mutations and covered by tests
- [ ] Concurrent interleaving test exists and matches the documented invariant
- [ ] UI cancel-all works end-to-end with clear errors
- [ ] README explains serialization strategy and idempotency lifetime

**Senior bar:** Looping `cancel()` without shared locking, or idempotency that only works in the happy single-tab case, will not score well.

---

### Requirement 3 — Feed Safety Circuit Breaker & Operator Risk Surface (25%)

**Problem:** When the upstream market feed is stale, disconnected, or simulated after geo-block, operators can still fire market orders against unreliable marks. Seniors own that risk.

**You must implement operational safety:**

1. **Server truth** — Expose feed health beyond a boolean if needed (e.g. `feed_connected`, `feed_mode: "live" | "simulated"`, `last_tick_age_ms` or equivalent) via dashboard summary and/or config. Derive staleness from the market engine — do not invent a fake timer disconnected from reality.
2. **Circuit breaker** — When the feed is unsafe (disconnected, simulated, or stale past a threshold you choose and document):
   - Block **market** order submits in the UI and reject them at the API with a clear plain-text `503` or `409` (pick one; document it)
   - Limit orders may remain allowed if you justify that choice; otherwise block all submits — either policy is acceptable if defended
3. **Dashboard risk surface** — Extend `/dashboard` with an operator panel showing feed health, breaker state, open-order count, and a simple **notional exposure** estimate for open orders (e.g. sum of `size_remaining * mark` or mid — document the formula and its limitations).
4. **Tests** — At least one test that unsafe feed ⇒ market submit rejected (engine helper, route-level, or pure policy module).

**Acceptance criteria:**

- [ ] Feed health / staleness is visible on the dashboard and grounded in engine state
- [ ] Unsafe feed blocks market orders in UI **and** API
- [ ] Breaker recovery when the feed becomes healthy again
- [ ] Exposure (or equivalent risk metric) is shown with a documented formula
- [ ] Policy and failure modes are explained in the README

**Senior bar:** A cosmetic “feed down” badge that still lets `POST /orders` market-buy will not score well.

---

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start dev server on port 3000 (HTTP + WebSocket) |
| `npm run client:build` | Production build |
| `npm run start` | Start production server |
| `npm run test` | Run Node test runner (`client/test/*.test.js`) — extend this |

---

## Evaluation Criteria

| Area | Weight |
|------|--------|
| Requirement 1 — Real-time Trading Event Stream | 40% |
| Requirement 2 — Matching Engine Hardening | 35% |
| Requirement 3 — Feed Safety Circuit Breaker & Risk Surface | 25% |

You are also evaluated on:

- Systems thinking (reconnect, concurrency, operational risk)
- API and protocol clarity
- Test quality for failure modes — not only happy paths
- Code clarity and consistency with the existing style
- README quality: decisions, invariants, known gaps
- No regressions in the starter dashboard / markets / trade flows

---

## Submission

- Do **not** open a PR to this repo — share your **fork URL**
- In your fork, update this `README.md` to explain:
  - How to install and run the app / tests
  - Your WebSocket trading protocol (schema, gaps, reconnect)
  - Idempotency + cancel-all concurrency model and invariants
  - Circuit-breaker policy, staleness threshold, exposure formula
  - What you would improve given more time (be specific)
- Verify on a clean checkout: `npm install` → `npm run test` → `npm run dev`, and walk Requirements 1–3 including failure paths (disconnect WS, retry submit, kill feed / simulated mode)
