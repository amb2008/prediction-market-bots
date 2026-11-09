"""
PARTIAL FILL HEDGING IMPLEMENTED, LOW VOLUME, IMPROVED TAKER FIRING SPEED
Kalshi Maker-Taker Arbitrage: FER vs CER

TAKER LATENCY IMPROVEMENTS
───────────────────────────
1. Taker fires BEFORE cancel (sequential, not concurrent).
   The cancel is cleanup — its latency doesn't affect P&L.
   Firing the taker first means the hedge lands as early as possible.

2. Dedicated aiohttp session for takers (separate from maker management).
   Avoids the taker HTTP request queuing behind a concurrent cancel call
   on a shared connector.

3. Taker payload + auth headers are pre-built synchronously the moment
   maker_filled is set, before any await. RSA signing (~1-3ms) happens
   off the hot path; by the time the first await yields, the request is
   ready to send immediately.

4. Pre-warmed persistent connection to Kalshi REST in the taker session
   (connector_owner=False, keepalive_timeout=30s). Eliminates TCP
   handshake latency on the hedge request.

FILL DETECTION
──────────────
Primary:   user_orders WS channel.
Secondary: 404-age inference on cancel calls.
Partial fills hedged immediately at exact incremental quantity.

MARKET FREEZE
─────────────
Big tick (≥ FREEZE_TICK_THRESHOLD) → cancel all resting makers,
wait CALM_DURATION seconds of calm before resuming.
"""

import asyncio
import aiohttp
import websockets
import json
import time
import csv
import os
import base64
import uuid
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
KALSHI_KEY_ID    = 'DUMMY_KEY_ID'
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_REST  = "https://external-api.kalshi.com"
WS_URL     = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
ORDER_PATH = "/trade-api/v2/portfolio/events/orders"

TICKERS = {
    "FER": "KXITFMATCH-26JUL02RODTSI-ROD",
    "CER": "KXITFMATCH-26JUL02RODTSI-TSI"
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}
GAME_DESCRIPTION = "Ferrer vs Cerundolo (ATP)"

TRADE_QUANTITY          = 1
BID_SPREAD              = 0.03
ASK_SPREAD              = 0.03
THRESHOLD               = 0.98
TAKER_BUFFER            = 0.10
UPDATE_TICK             = 0.01
MIN_ORDER_AGE_FOR_FILL  = 0.1
FREEZE_TICK_THRESHOLD   = 0.05
CALM_DURATION           = 2.0
LOG_FILE                = "maker_taker_log.csv"
# ═══════════════════════════════════════════════════════════

with open(PRIVATE_KEY_PATH, "rb") as _fh:
    PRIVATE_KEY = serialization.load_pem_private_key(_fh.read(), password=None)

# ── Price + orderbook state ─────────────────────────────────
curr_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None} for k in TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None} for k in TICKERS}
orderbooks  = {k: {"yes": {}, "no": {}} for k in TICKERS}
last_seq:    dict[str, int] = {}
resync_needed: set[str]     = set()

# ── Maker-order tracking ─────────────────────────────────────
bid_order_id:    str   | None = None
bid_price:       float | None = None
bid_placed_at:   float | None = None
bid_fill_hedged: float        = 0.0

ask_order_id:    str   | None = None
ask_price:       float | None = None
ask_placed_at:   float | None = None
ask_fill_hedged: float        = 0.0

cancelled_order_ids: set[str] = set()

# ── Safety flags ─────────────────────────────────────────────
maker_filled:    bool = False
updating_orders: bool = False
hedge_executed:  bool = False

# ── Freeze state ─────────────────────────────────────────────
market_frozen:             bool  = False
last_big_move_at:          float = 0.0
freeze_cancel_in_progress: bool  = False

tick_count = 0

# ── Two separate aiohttp sessions ────────────────────────────
# _maker_session: used for GTC order placement, cancels, WS resync
# _taker_session: dedicated to hedge IOC takers only — never shares
#                 connection pool with cancel calls
_maker_session: aiohttp.ClientSession | None = None
_taker_session: aiohttp.ClientSession | None = None

# ════════════════════════════════════════════════════════════
# AUTH
# ════════════════════════════════════════════════════════════
def auth(method: str, path: str) -> dict:
    """Generate Kalshi auth headers. Call synchronously before any await."""
    ts  = str(int(time.time() * 1000))
    sig = PRIVATE_KEY.sign(
        (ts + method + path).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type":            "application/json",
        "Accept":                  "application/json",
    }

# ════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════
def init_log():
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow(["timestamp_utc", "event", "details"])

def log(event: str, details: str = ""):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # with open(LOG_FILE, "a", newline="") as f:
    #     csv.writer(f).writerow([ts, event, details])
    print(f"  📝 [{ts}] {event}  {details}")

# ════════════════════════════════════════════════════════════
# SEQUENCE CHECKING
# ════════════════════════════════════════════════════════════
last_global_seq: int = -1

def check_sequence(seq: int, ticker: str, msg_type: str) -> bool:
    global last_global_seq
    if msg_type == "orderbook_snapshot":
        if seq > last_global_seq: last_global_seq = seq
        return True
    if msg_type == "orderbook_delta":
        if last_global_seq == -1:
            last_global_seq = seq; return True
        if seq <= last_global_seq: return True
        if seq == last_global_seq + 1:
            last_global_seq = seq; return True
        print(f"  🔴 SEQ GAP: {seq - last_global_seq - 1} missed — queuing resync")
        resync_needed.update(TICKERS.keys())
        last_global_seq = seq
        return False
    return True

async def request_resync(session):
    for ticker in list(resync_needed):
        leg = TICKERS_REVERSE.get(ticker, ticker)
        try:
            url  = f"{BASE_REST}/trade-api/v2/markets/{ticker}/orderbook?depth=100"
            async with session.get(url, headers=auth("GET", f"/trade-api/v2/markets/{ticker}/orderbook")) as resp:
                data = await resp.json()
                book = data.get("orderbook_fp") or data.get("orderbook", {})
                if book:
                    apply_snapshot(leg, {"yes_dollars_fp": book.get("yes_dollars", []),
                                         "no_dollars_fp":  book.get("no_dollars",  [])})
                    update_derived(leg)
                    resync_needed.discard(ticker)
                    print(f"  ✅ [{leg}] resync complete")
        except Exception as e:
            print(f"  ❌ [{leg}] resync failed: {e}")

# ════════════════════════════════════════════════════════════
# ORDERBOOK
# ════════════════════════════════════════════════════════════
def apply_snapshot(leg, msg):
    for side in ("yes", "no"):
        orderbooks[leg][side] = {
            int(round(float(x[0]) * 100)): float(x[1])
            for x in msg.get(f"{side}_dollars_fp", [])
        }

def apply_delta(leg, msg):
    side = msg.get("side"); price = msg.get("price_dollars"); delta = msg.get("delta_fp")
    if side not in ("yes", "no") or price is None or delta is None: return
    c  = int(round(float(price) * 100))
    bk = orderbooks[leg][side]
    nq = bk.get(c, 0.0) + float(delta)
    if nq <= 0.0001: bk.pop(c, None)
    else:            bk[c] = nq

def update_derived(leg):
    prev_prices[leg] = curr_prices[leg].copy()
    def best(side):
        b = orderbooks[leg][side]
        return max(b) / 100.0 if b else None
    yb = best("yes"); nb = best("no"); p = curr_prices[leg]
    p["yes_bid"] = round(yb,       4) if yb is not None else None
    p["no_ask"]  = round(1.0 - yb, 4) if yb is not None else None
    p["no_bid"]  = round(nb,       4) if nb is not None else None
    p["yes_ask"] = round(1.0 - nb, 4) if nb is not None else None

# ════════════════════════════════════════════════════════════
# MARKET SPEED / FREEZE
# ════════════════════════════════════════════════════════════
def check_market_speed() -> bool:
    global market_frozen, last_big_move_at
    max_move = 0.0
    for leg in TICKERS:
        for field in ("yes_ask", "no_ask", "yes_bid", "no_bid"):
            c = curr_prices[leg].get(field)
            p = prev_prices[leg].get(field)
            if c is not None and p is not None:
                max_move = max(max_move, abs(c - p))
    now = time.time()
    if max_move >= FREEZE_TICK_THRESHOLD:
        last_big_move_at = now
        if not market_frozen:
            market_frozen = True
            print(f"  🧊 MARKET FROZEN  (tick Δ={max_move:.3f} ≥ {FREEZE_TICK_THRESHOLD})")
            log("MARKET_FROZEN", f"tick_move={max_move:.3f}")
            return True
        return False
    if market_frozen and (now - last_big_move_at) >= CALM_DURATION:
        market_frozen = False
        print(f"  ✅ Market calm for {now - last_big_move_at:.1f}s — resuming makers")
        log("MARKET_UNFROZEN", f"calm_for={now - last_big_move_at:.1f}s")
    return False

# ════════════════════════════════════════════════════════════
# REST ORDER HELPERS
# ════════════════════════════════════════════════════════════
async def place_maker_order(session, team: str, v2_side: str, price: float) -> str | None:
    price = round(min(0.99, max(0.01, round(price, 2))), 2)
    payload = {
        "ticker":                     TICKERS[team],
        "client_order_id":            str(uuid.uuid4()),
        "side":                       v2_side,
        "count":                      f"{TRADE_QUANTITY}.00",
        "price":                      f"{price:.4f}",
        "time_in_force":              "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }
    try:
        async with session.post(f"{BASE_REST}{ORDER_PATH}", json=payload,
                                headers=auth("POST", ORDER_PATH)) as resp:
            res      = await resp.json()
            order_id = res.get("order_id")
            if resp.status in (200, 201) and order_id:
                print(f"  📌 MAKER {v2_side.upper()} on {team}_Y @ ${price:.2f} | id={order_id}")
                log("MAKER_PLACED", f"side={v2_side} team={team} price={price} id={order_id}")
                return order_id
            print(f"  ⚠️  Maker place failed ({v2_side}): {res}")
            log("MAKER_PLACE_FAIL", str(res))
            return None
    except Exception as e:
        print(f"  ❌ Maker order error: {e}")
        return None


async def cancel_order(session, order_id: str) -> str:
    cancel_path = f"{ORDER_PATH}/{order_id}"
    try:
        async with session.delete(f"{BASE_REST}{cancel_path}",
                                  headers=auth("DELETE", cancel_path)) as resp:
            body = await resp.text()
            if resp.status in (200, 204):
                cancelled_order_ids.add(order_id)
                print(f"  🗑  Cancelled {order_id[:12]}… ({resp.status})")
                log("ORDER_CANCELLED", order_id)
                return "confirmed"
            elif resp.status == 404:
                print(f"  ⚠️  Cancel 404 for {order_id[:12]}…")
                log("CANCEL_404_UNCONFIRMED", f"order={order_id}")
                return "not_found"
            else:
                print(f"  ⚠️  Cancel {order_id[:12]}… status={resp.status}: {body[:80]}")
                log("CANCEL_ERROR", f"order={order_id} status={resp.status}")
                return "error"
    except Exception as e:
        print(f"  ❌ Cancel error: {e}")
        return "error"


def _build_taker_payload(team: str, v2_side: str, price: float,
                          quantity: float) -> tuple[dict, dict]:
    """
    Build the taker payload + auth headers synchronously.
    Call this BEFORE any await so signing happens off the hot path.
    Returns (payload, headers).
    """
    price = round(min(0.99, max(0.01, price)), 4)
    payload = {
        "ticker":                     TICKERS[team],
        "client_order_id":            str(uuid.uuid4()),
        "side":                       v2_side,
        "count":                      f"{quantity:.2f}",
        "price":                      f"{price:.4f}",
        "time_in_force":              "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
    }
    headers = auth("POST", ORDER_PATH)   # RSA sign happens here, synchronously
    return payload, headers


async def fire_taker(payload: dict, headers: dict) -> tuple[bool, dict]:
    """
    Send a pre-built taker payload on the dedicated taker session.
    The session is separate from the maker session so it is never
    blocked behind a concurrent cancel call.
    """
    try:
        async with _taker_session.post(
            f"{BASE_REST}{ORDER_PATH}", json=payload, headers=headers
        ) as resp:
            res    = await resp.json()
            filled = resp.status == 201 and float(res.get("fill_count", "0")) > 0
            return filled, res
    except Exception as e:
        return False, {"error": str(e)}


async def cancel_all_makers(session):
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    for oid in filter(None, [bid_order_id, ask_order_id]):
        await cancel_order(session, oid)
    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0


async def cancel_all_makers_frozen(session):
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    global freeze_cancel_in_progress

    if freeze_cancel_in_progress:
        return
    freeze_cancel_in_progress = True

    try:
        legs = [
            ("bid", bid_order_id, bid_price, bid_placed_at, bid_fill_hedged),
            ("ask", ask_order_id, ask_price, ask_placed_at, ask_fill_hedged),
        ]
        for side, oid, px, placed_at, already_hedged in legs:
            if oid is None or maker_filled:
                continue
            result = await cancel_order(session, oid)
            if maker_filled:
                return
            if result == "confirmed":
                if side == "bid":
                    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
                else:
                    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0
            elif result == "not_found":
                age = time.time() - (placed_at or time.time())
                if age >= MIN_ORDER_AGE_FOR_FILL:
                    remaining_qty = max(0.0, TRADE_QUANTITY - already_hedged)
                    log("FILL_INFERRED_404_FROZEN", f"{side} id={oid} age={age:.3f}s qty={remaining_qty:.4f}")
                    await on_maker_filled(session, side, px or 0, oid, quantity=remaining_qty)
                    return
                else:
                    log("CANCEL_404_TOO_FRESH_FROZEN", f"{side} id={oid} age={age:.3f}s — leaving tracked")
    finally:
        freeze_cancel_in_progress = False

# ════════════════════════════════════════════════════════════
# PARTIAL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_partial_fill(maker_session, filled_side: str, fill_price: float, quantity: float):
    """
    Fires a taker for exactly `quantity` contracts. Maker stays resting.
    Taker payload is built synchronously before any await.
    """
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n  ⚡ [{ts}] PARTIAL FILL: {filled_side.upper()} @ ${fill_price:.4f}  qty={quantity:.4f}")
    log("PARTIAL_FILL", f"side={filled_side} price={fill_price} qty={quantity:.4f}")

    if filled_side == "bid":
        cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
        team, v2_side = "CER", "bid"
        print(f"  🎯 Partial Hedge: BUY {quantity:.4f}× CER_YES  mkt={cer_y:.4f}  limit=${taker_px:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY CER_YES qty={quantity:.4f} limit={taker_px}")
    else:
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
        team, v2_side = "CER", "ask"
        print(f"  🎯 Partial Hedge: BUY {quantity:.4f}× CER_NO  mkt={cer_n:.4f}  ask limit=${taker_px:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY CER_NO qty={quantity:.4f} ask@{taker_px}")

    # Build payload + sign synchronously, then fire
    payload, headers = _build_taker_payload(team, v2_side, taker_px, quantity)
    filled, res = await fire_taker(payload, headers)

    if filled:
        qty_filled = float(res.get("fill_count", str(quantity)))
        avg        = float(res.get("average_fill_price", taker_px))
        print(f"  ✅ Partial Hedge FILLED  qty={qty_filled:.4f} @ ${avg:.4f}")
        log("PARTIAL_HEDGE_FILLED", f"qty={qty_filled:.4f} avg={avg}")
    else:
        print(f"  🚨 PARTIAL HEDGE FAILED → {res}")
        log("PARTIAL_HEDGE_MISSED", f"side={filled_side} qty={quantity:.4f}: {res}")

# ════════════════════════════════════════════════════════════
# FULL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_maker_filled(maker_session, filled_side: str, fill_price: float, order_id: str,
                          quantity: float | None = None):
    """
    Full/final fill on a maker order.

    Latency optimisation order:
      1. Set maker_filled + claim other_id synchronously (no await).
      2. Build taker payload + sign auth headers synchronously (RSA off hot path).
      3. Await taker on dedicated _taker_session (no competition with cancels).
      4. Await cancel of other maker (cleanup, latency doesn't matter).
    """
    global maker_filled, hedge_executed
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged

    if maker_filled:
        return

    hedge_qty = TRADE_QUANTITY if quantity is None else quantity

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n{'='*70}")
    print(f"  🔥 [{ts}] MAKER FILLED: {filled_side.upper()} @ ${fill_price:.4f}  hedge_qty={hedge_qty:.4f}")
    print(f"  id={order_id}")
    log("MAKER_FILLED", f"side={filled_side} fill_price={fill_price} hedge_qty={hedge_qty:.4f} id={order_id}")

    # ── Step 1: lock + claim synchronously ──────────────────
    maker_filled = True
    hedge_executed = False

    other_id = ask_order_id if filled_side == "bid" else bid_order_id
    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0

    # ── Step 2: build taker payload + sign synchronously ────
    if hedge_qty <= 0.001:
        taker_payload = None
        taker_headers = None
        taker_px      = None
        team = v2_side_taker = None
    elif filled_side == "bid":
        cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
        team, v2_side_taker = "CER", "bid"
        print(f"  🎯 Hedge: BUY {hedge_qty:.4f}× CER_YES  mkt={cer_y:.4f}  limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY CER_YES qty={hedge_qty:.4f} limit={taker_px} mkt={cer_y}")
        taker_payload, taker_headers = _build_taker_payload(team, v2_side_taker, taker_px, hedge_qty)
    else:
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
        team, v2_side_taker = "CER", "ask"
        print(f"  🎯 Hedge: BUY {hedge_qty:.4f}× CER_NO  mkt={cer_n:.4f}  ask limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY CER_NO qty={hedge_qty:.4f} ask@{taker_px} mkt={cer_n}")
        taker_payload, taker_headers = _build_taker_payload(team, v2_side_taker, taker_px, hedge_qty)

    # ── Step 3: fire taker FIRST on dedicated session ───────
    if hedge_qty <= 0.001:
        print(f"  ✅ All quantity already hedged via prior partial fills.")
        log("HEDGE_SKIPPED", "all qty hedged via partial fills")
        filled = False; res = {}
    else:
        filled, res = await fire_taker(taker_payload, taker_headers)

    # ── Step 4: cancel other maker AFTER taker lands ────────
    if other_id:
        print(f"  🗑  Cancelling other maker {other_id[:12]}… (post-hedge)")
        await cancel_order(maker_session, other_id)

    # ── Result logging ───────────────────────────────────────
    if hedge_qty <= 0.001:
        pass   # already logged above
    elif filled_side == "bid":
        if filled:
            avg   = float(res.get("average_fill_price", taker_px))
            total = round(fill_price + avg, 4)
            print(f"  ✅ Hedge FILLED @ ${avg:.4f}  total=${total:.4f}")
            print(f"  💰 LOCKED PROFIT: $1.00 − ${total:.4f} = ${round(1-total,4):.4f}")
            log("HEDGE_FILLED", f"avg={avg} total={total} profit={round(1-total,4)}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY CER_YES @ {taker_px} → {res}")
            print(f"  🚨 HOLDING {hedge_qty:.4f}× FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_YES qty={hedge_qty:.4f} limit={taker_px}: {res}")
    else:
        if filled:
            yes_avg = float(res.get("average_fill_price", taker_px))
            no_cost = round(1.0 - yes_avg, 4)
            profit  = round(fill_price - no_cost, 4)
            print(f"  ✅ Hedge FILLED: CER_NO cost=${no_cost:.4f}")
            print(f"  💰 LOCKED PROFIT: ${fill_price:.4f} − ${no_cost:.4f} = ${profit:.4f}")
            log("HEDGE_FILLED", f"no_cost={no_cost} fer_sold={fill_price} profit={profit}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY CER_NO ask @ {taker_px} → {res}")
            print(f"  🚨 SHORT {hedge_qty:.4f}× FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_NO qty={hedge_qty:.4f} ask={taker_px}: {res}")

    hedge_executed = True
    print(f"\n  🔒 MAKER MODE LOCKED. Restart to resume.")
    print(f"{'='*70}\n")

# ════════════════════════════════════════════════════════════
# WS FILL DETECTION
# ════════════════════════════════════════════════════════════
async def handle_user_order_update(maker_session, msg: dict):
    global bid_fill_hedged, ask_fill_hedged

    if maker_filled:
        return

    order_id = msg.get("order_id", "")
    if order_id in cancelled_order_ids:
        print(f"  Cancelled order WS update ignored: {msg.get('status','')} {order_id[:12]}")
        cancelled_order_ids.discard(order_id)
        return

    if   order_id == bid_order_id: filled_side = "bid"
    elif order_id == ask_order_id: filled_side = "ask"
    else:                          return

    status     = msg.get("status", "")
    fill_count = float(msg.get("fill_count_fp",    "0"))
    remaining  = float(msg.get("remaining_count_fp", "1"))

    is_fully_filled = (status == "filled") or (fill_count >= TRADE_QUANTITY and remaining < 0.001)

    if fill_count <= 0:
        return

    already_hedged = bid_fill_hedged if filled_side == "bid" else ask_fill_hedged
    incremental    = round(fill_count - already_hedged, 6)

    if incremental < 0.001:
        if is_fully_filled:
            fill_price = float(msg.get("yes_price_dollars", "0"))
            await on_maker_filled(maker_session, filled_side, fill_price, order_id, quantity=0.0)
        return

    fill_price = float(msg.get("yes_price_dollars", "0"))
    if fill_price == 0:
        cost = float(msg.get("maker_fill_cost_dollars", "0"))
        fill_price = (cost / fill_count) if fill_count > 0 else (bid_price or ask_price or 0)

    if not is_fully_filled:
        log("WS_PARTIAL_FILL", f"side={filled_side} fill={fill_count:.4f} rem={remaining:.4f} incr={incremental:.4f}")
        await on_partial_fill(maker_session, filled_side, fill_price, incremental)
        if filled_side == "bid": bid_fill_hedged = fill_count
        else:                    ask_fill_hedged = fill_count
    else:
        log("WS_FILL_DETECTED", f"side={filled_side} status={status} price={fill_price} incr={incremental:.4f}")
        await on_maker_filled(maker_session, filled_side, fill_price, order_id, quantity=incremental)

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ════════════════════════════════════════════════════════════
async def update_maker_orders(session):
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    global updating_orders

    if maker_filled or updating_orders or market_frozen or freeze_cancel_in_progress:
        return

    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")
    if cer_y is None or cer_n is None:
        return

    target_bid = round(max(0.01, 1.0 - cer_y - BID_SPREAD), 2)
    target_ask = round(min(0.99, cer_n + ASK_SPREAD),        2)
    bid_valid  = (target_bid + cer_y) <= THRESHOLD
    ask_valid  = (target_ask - cer_n) >= (1.0 - THRESHOLD)

    updating_orders = True
    try:
        # ── BID ───────────────────────────────────────────
        if bid_order_id is not None:
            stale      = bid_price is None or abs(target_bid - bid_price) >= UPDATE_TICK
            too_costly = bid_price is not None and bid_price + cer_y > THRESHOLD
            if stale or too_costly or not bid_valid:
                oid_to_cancel = bid_order_id
                if maker_filled: return
                result = await cancel_order(session, oid_to_cancel)
                if maker_filled: return
                if result == "confirmed":
                    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
                elif result == "not_found":
                    age = time.time() - (bid_placed_at or time.time())
                    if age >= MIN_ORDER_AGE_FOR_FILL:
                        rqty = max(0.0, TRADE_QUANTITY - bid_fill_hedged)
                        log("FILL_INFERRED_404", f"bid id={oid_to_cancel} age={age:.3f}s qty={rqty:.4f}")
                        await on_maker_filled(session, "bid", bid_price or 0, oid_to_cancel, quantity=rqty)
                    else:
                        log("CANCEL_404_TOO_FRESH", f"bid id={oid_to_cancel} age={age:.3f}s")
                    return
                else:
                    return

        if bid_order_id is None and bid_valid and not maker_filled and not market_frozen:
            oid = await place_maker_order(session, "FER", "bid", target_bid)
            if maker_filled: return
            if oid:
                bid_order_id  = oid
                bid_price     = target_bid
                bid_placed_at = time.time()
                bid_fill_hedged = 0.0

        # ── ASK ───────────────────────────────────────────
        if ask_order_id is not None:
            stale      = ask_price is None or abs(target_ask - ask_price) >= UPDATE_TICK
            low_margin = ask_price is not None and ask_price - cer_n < (1.0 - THRESHOLD)
            if stale or low_margin or not ask_valid:
                oid_to_cancel = ask_order_id
                if maker_filled: return
                result = await cancel_order(session, oid_to_cancel)
                if maker_filled: return
                if result == "confirmed":
                    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0
                elif result == "not_found":
                    age = time.time() - (ask_placed_at or time.time())
                    if age >= MIN_ORDER_AGE_FOR_FILL:
                        rqty = max(0.0, TRADE_QUANTITY - ask_fill_hedged)
                        log("FILL_INFERRED_404", f"ask id={oid_to_cancel} age={age:.3f}s qty={rqty:.4f}")
                        await on_maker_filled(session, "ask", ask_price or 0, oid_to_cancel, quantity=rqty)
                    else:
                        log("CANCEL_404_TOO_FRESH", f"ask id={oid_to_cancel} age={age:.3f}s")
                    return
                else:
                    return

        if ask_order_id is None and ask_valid and not maker_filled and not market_frozen:
            oid = await place_maker_order(session, "FER", "ask", target_ask)
            if maker_filled: return
            if oid:
                ask_order_id  = oid
                ask_price     = target_ask
                ask_placed_at = time.time()
                ask_fill_hedged = 0.0

    finally:
        updating_orders = False

# ════════════════════════════════════════════════════════════
# LIVE PRINTOUT
# ════════════════════════════════════════════════════════════
last_printed: dict = {"FER": {}, "CER": {}}

def print_live_odds():
    changed = any(
        curr_prices[k].get(f) != last_printed.get(k, {}).get(f)
        for k in TICKERS for f in ["yes_ask", "no_ask"]
    )
    if not changed:
        return
    for k in TICKERS:
        last_printed[k] = {"yes_ask": curr_prices[k].get("yes_ask"),
                            "no_ask":  curr_prices[k].get("no_ask")}

    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    fer_y = curr_prices["FER"].get("yes_ask")
    fer_n = curr_prices["FER"].get("no_ask")
    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait.."
    status_flag = " 🔒LOCKED" if maker_filled else (" 🧊FROZEN" if market_frozen else "")
    bid_stat = f"BID@{bid_price:.2f}({bid_order_id[-6:]})" if bid_order_id else \
               f"→{fmt(round(max(0.01,1-(cer_y or 0)-BID_SPREAD),2))}"
    ask_stat = f"ASK@{ask_price:.2f}({ask_order_id[-6:]})" if ask_order_id else \
               f"→{fmt(round(min(0.99,(cer_n or 0)+ASK_SPREAD),2))}"

    print(
        f"{ts}{status_flag}  | "
        f"FER_Y: {fmt(fer_y):<6} FER_N: {fmt(fer_n):<6} | "
        f"CER_Y: {fmt(cer_y):<6} CER_N: {fmt(cer_n):<6} | "
        f"MAKERS: {bid_stat} / {ask_stat}"
    )

# ════════════════════════════════════════════════════════════
# WS MESSAGE HANDLER
# ════════════════════════════════════════════════════════════
async def process_ws_message(maker_session, msg_data: dict):
    global tick_count
    msg_type = msg_data.get("type")
    msg      = msg_data.get("msg", {})

    if msg_type == "subscribed":
        print(f"  ✅ Subscribed: {msg_data.get('msg',{}).get('channels','?')}")
        return
    if msg_type == "error":
        print(f"  ❌ WS error: {msg_data}")
        return
    if msg_type == "user_order":
        await handle_user_order_update(maker_session, msg)
        return

    ticker = msg.get("market_ticker")
    if ticker not in TICKERS_REVERSE:
        return
    leg = TICKERS_REVERSE[ticker]
    seq = msg_data.get("seq")

    if seq is not None:
        if not check_sequence(seq, ticker, msg_type):
            if resync_needed:
                await request_resync(maker_session)
            return

    if msg_type == "orderbook_snapshot":
        apply_snapshot(leg, msg); update_derived(leg); tick_count += 1
    elif msg_type == "orderbook_delta":
        apply_delta(leg, msg);    update_derived(leg); tick_count += 1
    else:
        return

    print_live_odds()

    newly_frozen = check_market_speed()
    if newly_frozen and (bid_order_id or ask_order_id):
        print("  🧊 Cancelling all resting makers due to freeze…")
        while updating_orders:
            await asyncio.sleep(0.01)
        await cancel_all_makers_frozen(maker_session)

    if not maker_filled and not market_frozen and not updating_orders:
        asyncio.create_task(update_maker_orders(maker_session))

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
async def monitor():
    global _maker_session, _taker_session

    init_log()
    print("=" * 90)
    print(f"  🎾 Maker-Taker Arb  —  {GAME_DESCRIPTION}")
    print(f"  FER_Y BID = 1 − CER_Y − {BID_SPREAD}   (hedge: take CER_YES)")
    print(f"  FER_Y ASK = CER_N + {ASK_SPREAD}         (hedge: take CER_NO)")
    print(f"  THRESHOLD={THRESHOLD}  UPDATE_TICK={UPDATE_TICK}  MIN_ORDER_AGE={MIN_ORDER_AGE_FOR_FILL}s")
    print(f"  FREEZE: tick Δ ≥ {FREEZE_TICK_THRESHOLD} → cancel all, wait {CALM_DURATION}s calm")
    print(f"  TAKER: dedicated session, payload pre-built sync, fires before cancel")
    print("=" * 90)

    ws_headers = auth("GET", "/trade-api/ws/v2")

    # Dedicated connector for taker session — keep-alive, no limit on connections
    taker_connector = aiohttp.TCPConnector(limit=0, keepalive_timeout=30)

    async with aiohttp.ClientSession() as maker_session, \
               aiohttp.ClientSession(connector=taker_connector) as taker_session:

        _maker_session = maker_session
        _taker_session = taker_session

        # Warm up the taker session's connection to BASE_REST so the first
        # hedge request doesn't pay TCP handshake cost.
        try:
            async with taker_session.get(f"{BASE_REST}/trade-api/v2/exchange/status",
                                         headers=auth("GET", "/trade-api/v2/exchange/status"),
                                         timeout=aiohttp.ClientTimeout(total=3)) as _r:
                pass
        except Exception:
            pass   # warm-up failure is non-fatal

        try:
            while True:
                if maker_filled:
                    print("\n🔒 MAKER MODE LOCKED. Monitoring only. Ctrl+C to exit.\n")
                    while True:
                        await asyncio.sleep(60)

                try:
                    async with websockets.connect(
                        WS_URL, additional_headers=ws_headers, ping_interval=15
                    ) as ws:
                        print(f"\n✅ Connected: {WS_URL}")
                        await cancel_all_makers(maker_session)

                        last_seq.clear(); resync_needed.clear()
                        for k in TICKERS:
                            orderbooks[k]  = {"yes": {}, "no": {}}
                            curr_prices[k] = {"yes_ask": None, "yes_bid": None,
                                              "no_ask":  None, "no_bid":  None}

                        await ws.send(json.dumps({
                            "id": 1, "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta", "user_orders"],
                                "market_tickers": list(TICKERS.values()),
                            }
                        }))

                        async for raw in ws:
                            await process_ws_message(maker_session, json.loads(raw))

                except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                    print(f"\n❌ WS error: {e}. Cancelling makers + retrying in 3s…")
                    await cancel_all_makers(maker_session)
                    await asyncio.sleep(3)

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n🔴 Shutting down — cancelling makers…")
            log("SHUTDOWN", "manual")
            await cancel_all_makers(maker_session)
            print("🔴 Done.")

if __name__ == "__main__":
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n❌ Fatal: {e}")