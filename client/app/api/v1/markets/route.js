import { NextResponse } from "next/server";
import { getMarketEngine } from "../../../../lib/engines.js";

// Reads live engine state that only exists at runtime (server.js) — never
// prerender this at build time.
export const dynamic = "force-dynamic";

export function GET() {
  return NextResponse.json(getMarketEngine().overview());
}
