/**
 * Market-feed safety policy (circuit breaker) — see README "Circuit breaker".
 *
 * Pure module (no node built-ins, no engine imports) so it can be unit-tested in
 * isolation and reused by the market engine, API routes, and dashboard helper.
 *
 * The feed is considered UNSAFE for market orders when any of the following hold:
 *   - the upstream feed socket is disconnected (`feed_connected === false`)
 *   - the feed is running on simulated marks (`feed_mode === "simulated"`)
 *   - the feed is connected but no real tick has arrived within the stale window
 *
 * When unsafe, MARKET orders are rejected (API + UI). LIMIT orders remain allowed
 * because their execution price is chosen by the operator and they rest in the
 * local book rather than sweeping an untrusted mark (documented in README).
 *
 * The breaker is fully derived from live engine state on every call, so it
 * recovers automatically as soon as real ticks resume.
 */

/**
 * Staleness threshold. Binance `@miniTicker` streams push roughly once per second
 * per symbol, so a healthy live feed updates well inside this window. 10s gives
 * generous headroom for transient network hiccups before we distrust the mark.
 */
export const STALE_THRESHOLD_MS = 10_000;

/**
 * @typedef {{
 *   feed_connected: boolean;
 *   feed_mode: "live" | "simulated" | string;
 *   last_tick_age_ms: number | null;
 *   stale_threshold_ms?: number | null;
 *   breaker_active?: boolean;
 * }} FeedHealth
 */

/**
 * @param {FeedHealth | null | undefined} health
 * @returns {{ safe: boolean; reason: string | null }}
 */
export function evaluateFeedSafety(health) {
  if (!health || health.feed_connected === false) {
    return {
      safe: false,
      reason:
        "Market feed disconnected — marks are simulated and cannot be trusted. Market orders are disabled.",
    };
  }
  if (health.feed_mode === "simulated") {
    return {
      safe: false,
      reason:
        "Market feed is running in simulated mode. Market orders are disabled until the live feed returns.",
    };
  }
  const threshold = health.stale_threshold_ms ?? STALE_THRESHOLD_MS;
  const age = health.last_tick_age_ms;
  if (age == null || (typeof age === "number" && age > threshold)) {
    const secs = age == null ? "unknown" : Math.round(age / 1000);
    return {
      safe: false,
      reason: `Market feed is stale (${secs}s since last tick, threshold ${Math.round(
        threshold / 1000,
      )}s). Market orders are disabled.`,
    };
  }
  return { safe: true, reason: null };
}

/**
 * Enforcement helper shared by the orders route and tests.
 *
 * @param {FeedHealth | null | undefined} health
 * @param {"limit" | "market" | string} orderType
 * @returns {{ allowed: boolean; reason: string | null }}
 */
export function assertOrderAllowed(health, orderType) {
  if (orderType !== "market") return { allowed: true, reason: null };
  const { safe, reason } = evaluateFeedSafety(health);
  return { allowed: safe, reason };
}
