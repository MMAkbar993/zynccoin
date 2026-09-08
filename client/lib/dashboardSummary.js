import { evaluateFeedSafety } from "./feedSafety.js";

/**
 * Build aggregated dashboard metrics from the live market + matching engines.
 *
 * @param {{ overview: () => any; isFeedConnected?: () => boolean; feedHealth?: () => any }} marketEngine
 * @param {{ listOrders: (marketId: string | undefined, openOnly: boolean) => Promise<any[]> }} matchingEngine
 */
export async function buildDashboardSummary(marketEngine, matchingEngine) {
  const overview = marketEngine.overview();
  const markets = overview?.markets ?? [];

  const ranked = [...markets].sort((a, b) => b.change_24h_pct - a.change_24h_pct);
  const top_gainers = ranked.slice(0, 3).map(compactMarket);
  const top_losers = ranked
    .slice()
    .reverse()
    .slice(0, 3)
    .map(compactMarket);

  const openOrders = await matchingEngine.listOrders(undefined, true);

  // ── Operator risk surface ────────────────────────────────────────────────
  const feed =
    typeof marketEngine.feedHealth === "function"
      ? marketEngine.feedHealth()
      : {
          feed_connected: Boolean(marketEngine.isFeedConnected?.()),
          feed_mode: marketEngine.isFeedConnected?.() ? "live" : "simulated",
          last_tick_age_ms: null,
          stale_threshold_ms: null,
          breaker_active: !marketEngine.isFeedConnected?.(),
        };
  const { safe: market_orders_allowed, reason: breaker_reason } =
    evaluateFeedSafety(feed);

  // Notional exposure = Σ(size_remaining × mark_price) over open / partially
  // filled orders. Uses the current live mark for each market. See README
  // "Exposure" for the formula and its limitations.
  const markPrice = new Map(markets.map((m) => [m.id, Number(m.mark_price) || 0]));
  let notional_exposure = 0;
  for (const o of openOrders) {
    const remaining = Number(o.size_remaining) || 0;
    notional_exposure += remaining * (markPrice.get(o.market_id) ?? 0);
  }

  return {
    active_markets: markets.length,
    volume_24h_total: overview?.volume_24h_total ?? 0,
    top_gainers,
    top_losers,
    open_orders_count: openOrders.length,
    // Kept for backwards compatibility with existing consumers/tests.
    feed_connected: Boolean(feed.feed_connected),
    feed: {
      ...feed,
      market_orders_allowed,
      breaker_reason,
    },
    notional_exposure,
    server_ts: overview?.server_ts ?? Math.floor(Date.now() / 1000),
  };
}

function compactMarket(m) {
  return {
    id: m.id,
    pair: m.pair,
    base: m.base,
    quote: m.quote,
    mark_price: m.mark_price,
    change_24h_pct: m.change_24h_pct,
    volume_24h: m.volume_24h,
  };
}
