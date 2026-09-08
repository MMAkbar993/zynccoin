import { NextResponse } from "next/server";
import { getMatchingEngine } from "../../../../../lib/engines.js";
import { isMatchError, matchErrorStatus } from "../../../../../lib/matchingEngine.js";

export async function DELETE(_req, { params }) {
  let id = params.id?.trim();
  if (!id || !/^[0-9a-f-]{36}$/i.test(id)) {
    return new NextResponse("invalid order id", { status: 400 });
  }
  id = id.toLowerCase();
  try {
    await getMatchingEngine().cancel(id);
    return new NextResponse(null, { status: 204 });
  } catch (e) {
    if (isMatchError(e)) {
      return new NextResponse(e.message, { status: matchErrorStatus(e.code) });
    }
    console.error("order cancel failed:", e?.message ?? e);
    return new NextResponse("internal error", { status: 500 });
  }
}
