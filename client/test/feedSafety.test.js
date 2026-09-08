import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  evaluateFeedSafety,
  assertOrderAllowed,
  STALE_THRESHOLD_MS,
} from "../lib/feedSafety.js";
import { MarketEngine } from "../lib/marketEngine.js";

const healthy = {
  feed_connected: true,
  feed_mode: "live",
  last_tick_age_ms: 500,
  stale_threshold_ms: STALE_THRESHOLD_MS,
};

describe("evaluateFeedSafety", () => {
  it("allows a connected, live, fresh feed", () => {
    const r = evaluateFeedSafety(healthy);
    assert.equal(r.safe, true);
    assert.equal(r.reason, null);
  });

  it("blocks a disconnected feed", () => {
    const r = evaluateFeedSafety({ ...healthy, feed_connected: false, feed_mode: "simulated" });
    assert.equal(r.safe, false);
    assert.match(r.reason, /disconnected|simulated/i);
  });

  it("blocks a simulated feed even if the socket reports connected", () => {
    const r = evaluateFeedSafety({ ...healthy, feed_mode: "simulated" });
    assert.equal(r.safe, false);
  });

  it("blocks a stale feed past the threshold", () => {
    const r = evaluateFeedSafety({ ...healthy, last_tick_age_ms: STALE_THRESHOLD_MS + 1 });
    assert.equal(r.safe, false);
    assert.match(r.reason, /stale/i);
  });

  it("blocks a feed that has never ticked", () => {
    const r = evaluateFeedSafety({ ...healthy, last_tick_age_ms: null });
    assert.equal(r.safe, false);
  });

  it("treats missing health as unsafe", () => {
    assert.equal(evaluateFeedSafety(null).safe, false);
    assert.equal(evaluateFeedSafety(undefined).safe, false);
  });
});

describe("assertOrderAllowed (circuit breaker)", () => {
  it("rejects MARKET orders when the feed is unsafe", () => {
    const unsafe = { ...healthy, feed_connected: false, feed_mode: "simulated" };
    const r = assertOrderAllowed(unsafe, "market");
    assert.equal(r.allowed, false);
    assert.ok(r.reason);
  });

  it("still allows LIMIT orders when the feed is unsafe", () => {
    const unsafe = { ...healthy, feed_connected: false, feed_mode: "simulated" };
    assert.equal(assertOrderAllowed(unsafe, "limit").allowed, true);
  });

  it("allows MARKET orders once the feed recovers (unsafe -> healthy)", () => {
    const stale = { ...healthy, last_tick_age_ms: STALE_THRESHOLD_MS + 5000 };
    assert.equal(assertOrderAllowed(stale, "market").allowed, false);
    // feed catches up
    assert.equal(assertOrderAllowed({ ...healthy, last_tick_age_ms: 200 }, "market").allowed, true);
  });
});

describe("MarketEngine.feedHealth (real engine, never connected)", () => {
  it("reports simulated/disconnected and blocks market orders", () => {
    // Construct the real engine WITHOUT calling connectBinance() — this is the
    // exact state the app is in when the upstream feed is unavailable.
    const engine = new MarketEngine();
    const health = engine.feedHealth();
    assert.equal(health.feed_connected, false);
    assert.equal(health.feed_mode, "simulated");
    assert.equal(health.last_tick_age_ms, null);
    assert.equal(health.breaker_active, true);

    assert.equal(assertOrderAllowed(health, "market").allowed, false);
    assert.equal(assertOrderAllowed(health, "limit").allowed, true);

    // Simulate a real tick landing -> breaker clears on the next read.
    engine._lastTickAt = Date.now();
    Object.defineProperty(engine, "_binanceWs", { value: { readyState: 1 }, configurable: true });
    const recovered = engine.feedHealth();
    assert.equal(recovered.feed_connected, true);
    assert.equal(recovered.breaker_active, false);
    assert.equal(assertOrderAllowed(recovered, "market").allowed, true);
  });
});
