/**
 * WebSocket endpoint for the real-time trading event stream (`/ws/trading`).
 *
 * A dedicated socket (not `/ws/markets`) because trading needs per-connection
 * subscription state, per-market sequence numbers, and snapshot-on-subscribe —
 * see README "why one WebSocket vs two".
 *
 * Events are sourced from the matching engine's mutation path via
 * `matchingEngine.bus` — this module never scrapes the REST API.
 */
import { WebSocketServer } from "ws";
import { parse } from "node:url";
import {
  TRADING_PROTOCOL_VERSION,
  TradingEventType,
  ClientMessageType,
  TRADING_WS_PATH,
} from "./tradingProtocol.js";

const HEARTBEAT_MS = 30_000;

/**
 * @param {import('node:http').Server} httpServer
 * @param {import('./matchingEngine.js').MatchingEngine} matchingEngine
 */
export function registerTradingWss(httpServer, matchingEngine) {
  const wss = new WebSocketServer({ noServer: true });

  httpServer.on("upgrade", (req, socket, head) => {
    if (parse(req.url).pathname !== TRADING_WS_PATH) return; // let other handlers deal with it
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  });

  wss.on("connection", (ws) => {
    /** market_id -> subscription handle */
    const subs = new Map();

    const send = (obj) => {
      if (ws.readyState === ws.OPEN) {
        ws.send(typeof obj === "string" ? obj : JSON.stringify(obj));
      }
    };

    function teardown() {
      for (const sub of subs.values()) matchingEngine.bus.off("trading_event", sub.handler);
      subs.clear();
    }

    function subscribe(marketId, lastSequence) {
      // A repeat subscribe for a market we already track is treated as a resync
      // request: drop the old listener so there is never more than one per market.
      const existing = subs.get(marketId);
      if (existing) {
        matchingEngine.bus.off("trading_event", existing.handler);
        subs.delete(marketId);
      }

      const buffer = [];
      const sub = { ready: false, highWater: -Infinity, handler: null };

      // Attach the live listener FIRST so nothing emitted during snapshot
      // construction is lost; events are buffered until the snapshot is sent.
      sub.handler = (ev) => {
        if (ev.market_id !== marketId) return;
        if (!sub.ready) {
          buffer.push(ev);
          return;
        }
        if (ev.sequence <= sub.highWater) return; // already delivered
        sub.highWater = ev.sequence;
        send(ev);
      };
      matchingEngine.bus.on("trading_event", sub.handler);
      subs.set(marketId, sub);

      (async () => {
        try {
          let afterSeq;

          if (Number.isInteger(lastSequence)) {
            const missed = await matchingEngine.getEventsSince(marketId, lastSequence);
            if (missed) {
              send({
                v: TRADING_PROTOCOL_VERSION,
                type: TradingEventType.RESUMED,
                market_id: marketId,
                sequence: lastSequence,
              });
              for (const ev of missed) send(ev);
              afterSeq = missed.length ? missed[missed.length - 1].sequence : lastSequence;
            }
          }

          if (afterSeq === undefined) {
            const snap = await matchingEngine.getTradingSnapshot(marketId);
            send({
              v: TRADING_PROTOCOL_VERSION,
              type: TradingEventType.SNAPSHOT,
              market_id: marketId,
              sequence: snap.sequence,
              data: {
                open_orders: snap.open_orders,
                recent_fills: snap.recent_fills,
              },
            });
            afterSeq = snap.sequence;
          }

          // Flush anything buffered during the await, in order, once.
          sub.highWater = afterSeq;
          buffer.sort((a, b) => a.sequence - b.sequence);
          for (const ev of buffer) {
            if (ev.sequence > sub.highWater) {
              sub.highWater = ev.sequence;
              send(ev);
            }
          }
          buffer.length = 0;
          sub.ready = true;
        } catch {
          send({
            v: TRADING_PROTOCOL_VERSION,
            type: TradingEventType.ERROR,
            market_id: marketId,
            message: "subscription failed — retry subscribe",
          });
          matchingEngine.bus.off("trading_event", sub.handler);
          subs.delete(marketId);
        }
      })();
    }

    function unsubscribe(marketId) {
      const sub = subs.get(marketId);
      if (!sub) return;
      matchingEngine.bus.off("trading_event", sub.handler);
      subs.delete(marketId);
      send({
        v: TRADING_PROTOCOL_VERSION,
        type: TradingEventType.UNSUBSCRIBED,
        market_id: marketId,
      });
    }

    ws.on("message", (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return;
      }
      if (!msg || typeof msg !== "object") return;

      switch (msg.type) {
        case ClientMessageType.PING:
          send({ v: TRADING_PROTOCOL_VERSION, type: TradingEventType.PONG });
          break;
        case ClientMessageType.SUBSCRIBE:
          if (typeof msg.market_id === "string" && msg.market_id) {
            subscribe(
              msg.market_id,
              Number.isInteger(msg.last_sequence) ? msg.last_sequence : undefined,
            );
          }
          break;
        case ClientMessageType.UNSUBSCRIBE:
          if (typeof msg.market_id === "string") unsubscribe(msg.market_id);
          break;
        default:
          break;
      }
    });

    // Liveness: ws-level ping; terminate a socket that stops answering.
    ws.isAlive = true;
    ws.on("pong", () => {
      ws.isAlive = true;
    });
    const heartbeat = setInterval(() => {
      if (ws.isAlive === false) {
        ws.terminate();
        return;
      }
      ws.isAlive = false;
      try {
        ws.ping();
      } catch {
        /* ignore */
      }
    }, HEARTBEAT_MS);
    heartbeat.unref?.();

    ws.on("close", () => {
      clearInterval(heartbeat);
      teardown();
    });
    ws.on("error", () => ws.terminate());
  });

  return wss;
}
