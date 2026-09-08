import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { buildDashboardSummary } from "../lib/dashboardSummary.js";

function marketEngineStub(feedHealth) {
  return {
    overview() {
      return {
        volume_24h_total: 1000,
        server_ts: 1,
        markets: [
          { id: "btc-usdt", pair: "BTC-USDT", base: "BTC", quote: "USDT", mark_price: 60000, change_24h_pct: 1, volume_24h: 10 },
          { id: "eth-usdt", pair: "ETH-USDT", base: "ETH", quote: "USDT", mark_price: 3000, change_24h_pct: -1, volume_24h: 5 },
        ],
      };
    },
    isFeedConnected() {
      return feedHealth.feed_connected;
    },
    feedHealth() {
      return feedHealth;
    },
  };
}

describe("dashboard risk surface", () => {
  it("computes notional exposure as Σ(size_remaining × mark)", async () => {
    const matchingEngine = {
      async listOrders() {
        return [
          { id: "1", market_id: "btc-usdt", size_remaining: 0.5 }, // 30000
          { id: "2", market_id: "eth-usdt", size_remaining: 2 }, //   6000
          { id: "3", market_id: "unknown", size_remaining: 9 }, //       0
        ];
      },
    };
    const summary = await buildDashboardSummary(
      marketEngineStub({
        feed_connected: true,
        feed_mode: "live",
        last_tick_age_ms: 300,
        stale_threshold_ms: 10000,
        breaker_active: false,
      }),
      matchingEngine,
    );
    assert.equal(summary.notional_exposure, 36000);
    assert.equal(summary.open_orders_count, 3);
    assert.equal(summary.feed.market_orders_allowed, true);
    assert.equal(summary.feed.feed_mode, "live");
  });

  it("surfaces breaker state + reason when the feed is unsafe", async () => {
    const matchingEngine = { async listOrders() { return []; } };
    const summary = await buildDashboardSummary(
      marketEngineStub({
        feed_connected: false,
        feed_mode: "simulated",
        last_tick_age_ms: null,
        stale_threshold_ms: 10000,
        breaker_active: true,
      }),
      matchingEngine,
    );
    assert.equal(summary.feed.breaker_active, true);
    assert.equal(summary.feed.market_orders_allowed, false);
    assert.ok(summary.feed.breaker_reason);
    assert.equal(summary.feed_connected, false); // legacy field preserved
  });
});
