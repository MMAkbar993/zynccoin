/**
 * Trading event stream protocol (see README "Trading WebSocket protocol").
 *
 * This module is intentionally dependency-free (no node built-ins) so it can be
 * imported by both the custom server and the browser client bundle.
 *
 * Transport: WebSocket at `/ws/trading`.
 * Every server->client trading event carries:
 *   - `v`         protocol version
 *   - `type`      one of TradingEventType
 *   - `market_id` the market the event belongs to
 *   - `sequence`  per-market monotonically increasing integer (starts at 1)
 *   - `ts`        server wall-clock ms (informational only)
 *   - `data`      type-specific payload
 *
 * Sequence numbers are per-market and per-process. A client detects a gap when
 * an incoming event's `sequence` is greater than `last_seen + 1` and reacts by
 * requesting a fresh snapshot (see README "sequence-gap handling").
 */

export const TRADING_PROTOCOL_VERSION = 1;

/** Server -> client message / event types. */
export const TradingEventType = {
  /** Authoritative state for a market: open orders + recent fills + sequence. */
  SNAPSHOT: "snapshot",
  /** Ack for a subscribe that was satisfied by replaying retained events. */
  RESUMED: "resumed",
  /** Ack for an unsubscribe. */
  UNSUBSCRIBED: "unsubscribed",
  /** A new order entered the book (resting, unfilled or partially filled+resting). */
  ORDER_ACCEPTED: "order.accepted",
  /** An order (taker or maker) was partially filled and still has size remaining. */
  ORDER_PARTIAL_FILL: "order.partial_fill",
  /** An order (taker or maker) was fully filled. */
  ORDER_FILLED: "order.filled",
  /** An order was cancelled (by the user, by cancel-all, or an unfilled market order). */
  ORDER_CANCELLED: "order.cancelled",
  /** Heartbeat reply. */
  PONG: "pong",
  /** Non-fatal error (e.g. snapshot build failed); client may retry subscribe. */
  ERROR: "error",
};

/** Reason attached to ORDER_CANCELLED events. */
export const CancelReason = {
  USER: "user",
  CANCEL_ALL: "cancel_all",
  NO_LIQUIDITY: "no_liquidity",
};

/** Client -> server message types. */
export const ClientMessageType = {
  SUBSCRIBE: "subscribe",
  UNSUBSCRIBE: "unsubscribe",
  PING: "ping",
};

export const TRADING_WS_PATH = "/ws/trading";
