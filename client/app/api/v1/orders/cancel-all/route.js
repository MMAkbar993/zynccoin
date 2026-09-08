import { NextResponse } from "next/server";
import { getMarketEngine, getMatchingEngine } from "../../../../../lib/engines.js";

/**
 * POST /api/v1/orders/cancel-all  { "market_id": "btc-usdt" }
 *
 * Cancels every open / partially-filled order for the market in a single
 * serialized critical section (see MatchingEngine.cancelAll) — atomic with
 * respect to submit and single cancel. Cancellations are broadcast on
 * /ws/trading as `order.cancelled` events with reason `cancel_all`.
 */
export async function POST(req) {
  const body = await req.json().catch(() => ({}));
  const marketId = String(body.market_id ?? "").trim();
  if (!marketId) {
    return new NextResponse("market_id is required", { status: 400 });
  }
  if (!getMarketEngine().detail(marketId)) {
    return new NextResponse("unknown market_id", { status: 404 });
  }

  try {
    const summary = await getMatchingEngine().cancelAll(marketId);
    return NextResponse.json({
      market_id: marketId,
      cancelled_count: summary.cancelled_count,
      cancelled_ids: summary.cancelled_ids,
    });
  } catch (e) {
    console.error("cancel-all failed:", e?.message ?? e);
    return new NextResponse("internal error", { status: 500 });
  }
}
