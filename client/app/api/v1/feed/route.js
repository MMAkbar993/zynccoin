import { NextResponse } from "next/server";
import { getMarketEngine } from "../../../../lib/engines.js";
import { evaluateFeedSafety } from "../../../../lib/feedSafety.js";

// Depends on live engine state — must not be prerendered at build time.
export const dynamic = "force-dynamic";

/**
 * GET /api/v1/feed
 *
 * Live market-feed health + circuit-breaker verdict. Derived entirely from the
 * market engine (socket state + last real tick time) — no standalone timer.
 * Consumed by the trade page (to disable market orders) and pollable by
 * operators. The breaker recovers automatically once real ticks resume.
 */
export async function GET() {
  try {
    const health = getMarketEngine().feedHealth();
    const { safe, reason } = evaluateFeedSafety(health);
    return NextResponse.json({
      ...health,
      market_orders_allowed: safe,
      reason,
      server_ts: Math.floor(Date.now() / 1000),
    });
  } catch (e) {
    const message = String(e?.message ?? "");
    if (message.includes("not ready")) {
      return new NextResponse("service warming up", { status: 503 });
    }
    console.error("feed health failed:", message);
    return new NextResponse("failed to read feed health", { status: 500 });
  }
}
