"use client";

import { useEffect, useRef, useState } from "react";
import {
  TRADING_WS_PATH,
  TradingEventType,
  ClientMessageType,
} from "../../lib/tradingProtocol.js";

export type TradingStreamStatus =
  | "connecting"
  | "connected"
  | "reconnecting"
  | "disconnected";

export type StreamOrder = {
  id: string;
  market_id: string;
  side: string;
  order_type: string;
  price: number | null;
  size: number;
  size_remaining: number;
  status: string;
  created_at: number;
};

export type StreamFill = {
  id: number;
  market_id: string;
  price: number;
  size: number;
  maker_order_id: string;
  taker_order_id: string;
  taker_side: string;
  ts: number;
};

const MAX_FILLS = 100;
const RECONNECT_MS = 1500;

function tradingWsUrl(): string {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}${TRADING_WS_PATH}`;
}

function sortOrders(list: StreamOrder[]): StreamOrder[] {
  return [...list].sort((a, b) => a.created_at - b.created_at);
}

/**
 * Push-based open-orders + fills for a single market. Subscribes to
 * `/ws/trading`, applies incremental events, detects sequence gaps and resyncs
 * from a fresh snapshot, and reconnects with a fixed backoff. There is no
 * REST poll fallback — every reconnect re-snapshots, so the view cannot retain
 * stale orders (see README "Client").
 */
export function useTradingStream(marketId: string) {
  const [status, setStatus] = useState<TradingStreamStatus>("connecting");
  const [resyncing, setResyncing] = useState(false);
  const [openOrders, setOpenOrders] = useState<StreamOrder[]>([]);
  const [fills, setFills] = useState<StreamFill[]>([]);

  const wsRef = useRef<WebSocket | null>(null);
  /** Last per-market sequence applied. -1 => awaiting snapshot. */
  const lastSeqRef = useRef<number>(-1);

  useEffect(() => {
    let alive = true;
    let ws: WebSocket | null = null;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let everConnected = false;

    // Fresh market => discard any prior trading state.
    lastSeqRef.current = -1;
    setOpenOrders([]);
    setFills([]);
    setResyncing(false);
    setStatus("connecting");

    const upsertOrder = (o: StreamOrder) => {
      if (o.status !== "open" && o.status !== "partially_filled") {
        setOpenOrders((prev) => prev.filter((x) => x.id !== o.id));
        return;
      }
      setOpenOrders((prev) => {
        const next = prev.some((x) => x.id === o.id)
          ? prev.map((x) => (x.id === o.id ? o : x))
          : [...prev, o];
        return sortOrders(next);
      });
    };
    const removeOrder = (id: string) =>
      setOpenOrders((prev) => prev.filter((x) => x.id !== id));
    const addFills = (incoming: StreamFill[]) => {
      if (!incoming.length) return;
      setFills((prev) => {
        const seen = new Set(prev.map((f) => f.id));
        const merged = [...incoming.filter((f) => !seen.has(f.id)), ...prev];
        return merged.slice(0, MAX_FILLS);
      });
    };

    const applyEvent = (ev: {
      type: string;
      data?: { order?: StreamOrder; trades?: StreamFill[] };
    }) => {
      const order = ev.data?.order;
      const trades = ev.data?.trades ?? [];
      switch (ev.type) {
        case TradingEventType.ORDER_ACCEPTED:
          if (order) upsertOrder(order);
          break;
        case TradingEventType.ORDER_PARTIAL_FILL:
          if (order) upsertOrder(order);
          addFills(trades);
          break;
        case TradingEventType.ORDER_FILLED:
          if (order) removeOrder(order.id);
          addFills(trades);
          break;
        case TradingEventType.ORDER_CANCELLED:
          if (order) removeOrder(order.id);
          break;
        default:
          break;
      }
    };

    const send = (obj: unknown) => {
      try {
        wsRef.current?.send(JSON.stringify(obj));
      } catch {
        /* socket not ready — reconnect path will re-subscribe */
      }
    };

    const requestResync = () => {
      setResyncing(true);
      lastSeqRef.current = -1;
      send({ type: ClientMessageType.SUBSCRIBE, market_id: marketId });
    };

    const handleMessage = (raw: string) => {
      let msg: any;
      try {
        msg = JSON.parse(raw);
      } catch {
        return;
      }
      if (!msg || typeof msg !== "object") return;

      if (
        msg.type === TradingEventType.PONG ||
        msg.type === TradingEventType.UNSUBSCRIBED ||
        msg.type === TradingEventType.RESUMED
      ) {
        return;
      }

      if (msg.type === TradingEventType.ERROR) {
        requestResync();
        return;
      }

      if (msg.type === TradingEventType.SNAPSHOT) {
        if (msg.market_id !== marketId) return;
        lastSeqRef.current = typeof msg.sequence === "number" ? msg.sequence : 0;
        setOpenOrders(sortOrders((msg.data?.open_orders ?? []) as StreamOrder[]));
        setFills(((msg.data?.recent_fills ?? []) as StreamFill[]).slice(0, MAX_FILLS));
        setResyncing(false);
        return;
      }

      // Incremental trading event.
      if (typeof msg.sequence !== "number" || msg.market_id !== marketId) return;
      if (lastSeqRef.current < 0) return; // no snapshot yet — wait for it

      const expected = lastSeqRef.current + 1;
      if (msg.sequence < expected) return; // duplicate / already applied
      if (msg.sequence > expected) {
        // Gap: an event was missed. Invalidate local state and reload snapshot.
        requestResync();
        return;
      }
      lastSeqRef.current = msg.sequence;
      applyEvent(msg);
    };

    const connect = () => {
      if (!alive) return;
      try {
        ws = new WebSocket(tradingWsUrl());
      } catch {
        retry = setTimeout(connect, 2000);
        return;
      }
      wsRef.current = ws;

      ws.onopen = () => {
        if (!alive) return;
        everConnected = true;
        setStatus("connected");
        const lastSeq = lastSeqRef.current;
        send({
          type: ClientMessageType.SUBSCRIBE,
          market_id: marketId,
          ...(lastSeq >= 0 ? { last_sequence: lastSeq } : {}),
        });
      };

      ws.onclose = () => {
        if (!alive) return;
        setStatus(everConnected ? "reconnecting" : "disconnected");
        retry = setTimeout(connect, RECONNECT_MS);
      };

      ws.onerror = () => {
        try {
          ws?.close();
        } catch {
          /* ignore */
        }
      };

      ws.onmessage = (e) => handleMessage(e.data as string);
    };

    connect();

    return () => {
      alive = false;
      if (retry) clearTimeout(retry);
      try {
        wsRef.current?.close();
      } catch {
        /* ignore */
      }
      wsRef.current = null;
    };
  }, [marketId]);

  return { status, resyncing, openOrders, fills, lastSequence: lastSeqRef.current };
}
