"use client";

import { useCallback, useEffect, useState } from "react";

export type FeedHealth = {
  feed_connected: boolean;
  feed_mode: "live" | "simulated" | string;
  last_tick_age_ms: number | null;
  stale_threshold_ms: number;
  breaker_active: boolean;
  market_orders_allowed: boolean;
  reason: string | null;
};

const POLL_MS = 4000;

/**
 * Polls `GET /api/v1/feed` for the circuit-breaker verdict. This only drives the
 * UI hint (disabling the market-order control). The API enforces the same policy
 * server-side and is authoritative — a stale/failed poll never enables a
 * market order that the server would reject.
 */
export function useFeedHealth() {
  const [health, setHealth] = useState<FeedHealth | null>(null);
  const [reachable, setReachable] = useState(true);

  const load = useCallback(async () => {
    try {
      const r = await fetch("/api/v1/feed");
      if (!r.ok) {
        setReachable(false);
        return;
      }
      setHealth((await r.json()) as FeedHealth);
      setReachable(true);
    } catch {
      setReachable(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const id = window.setInterval(load, POLL_MS);
    return () => window.clearInterval(id);
  }, [load]);

  // Fail safe: if we cannot read feed health, assume market orders are unsafe.
  const marketOrdersAllowed = reachable && health ? health.market_orders_allowed : false;
  const reason = !reachable
    ? "Cannot reach feed health endpoint — market orders disabled as a precaution."
    : (health?.reason ?? null);

  return { health, reachable, marketOrdersAllowed, reason };
}
