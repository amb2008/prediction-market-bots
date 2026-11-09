"""
Kalshi Maker-Taker Arbitrage: FER vs CER — MULTI-LEVEL SPREADS

MAKERS
──────
Places NUM_SPREAD_LEVELS resting makers on each side simultaneously,
stepping the spread out by SPREAD_STEP per level:

  Level 0: bid = 1 − CER_Y − BID_SPREAD                ask = CER_N + ASK_SPREAD
  Level 1: bid = 1 − CER_Y − BID_SPREAD − SPREAD_STEP  ask = CER_N + ASK_SPREAD + SPREAD_STEP
  Level 2: ...                                           ...

Each level is INDEPENDENT. A fill on one level fires its own hedge taker;
other levels are NOT cancelled as a result. An order is cancelled only when
its resting price + current taker cost would breach THRESHOLD.

FILL DETECTION
──────────────
Primary:   user_orders WS — fires on every state change.
Secondary: 404-age inference on cancel calls.
Partial fills hedged immediately at exact incremental quantity.

MARKET FREEZE
─────────────
Big tick (≥ FREEZE_TICK_THRESHOLD) → cancel all resting makers, all levels.
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
    "CER": "KXATPMATCH-26JUL01RINDAM-RIN",
    "FER": "KXATPMATCH-26JUL01RINDAM-DAM"
}
TICKERS_REVERSE  = {v: k for k, v in TICKERS.items()}
GAME_DESCRIPTION = "Ferrer vs Cerundolo (ATP)"

TRADE_QUANTITY      = 1
BID_SPREAD          = 0.02    # innermost bid:  price = 1 − CER_Y − BID_SPREAD
ASK_SPREAD          = 0.02    # innermost ask:  price = CER_N + ASK_SPREAD
SPREAD_STEP         = 0.01    # each extra level widens by this amount
NUM_SPREAD_LEVELS   = 2       # total levels (e.g. 3 → spreads at 0.03, 0.04, 0.05)
THRESHOLD           = 0.98    # cancel order if resting_price + taker_cost > THRESHOLD
TAKER_BUFFER        = 0.10    # aggression buffer on hedge taker limit price
UPDATE_TICK         = 0.01    # reprice if target moved by this much
MIN_ORDER_AGE_FOR_FILL = 0.1  # 404-cancel on order older than this → infer fill

FREEZE_TICK_THRESHOLD = 0.03
CALM_DURATION         = 2.0

LOG_FILE = "maker_taker_log.csv"
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

# ── Multi-level maker tracking ───────────────────────────────
# bid_orders[level] / ask_orders[level]:
#   {"order_id": str, "price": float, "placed_at": float, "fill_hedged": float}
bid_orders: dict[int, dict] = {}
ask_orders: dict[int, dict] = {}

cancelled_order_ids: set[str] = set()

# ── Concurrency guards ───────────────────────────────────────
updating_orders:           bool  = False
freeze_cancel_in_progress: bool  = False

# ── Market freeze ────────────────────────────────────────────
market_frozen:    bool  = False
last_big_move_at: float = 0.0

tick_count = 0
_session: aiohttp.ClientSession | None = None

# ════════════════════════════════════════════════════════════
# AUTH
# ════════════════════════════════════════════════════════════
def auth(method: str, path: str) -> dict:
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
                print(f"  ⚠️  Cancel 404 for {order_id[:12]}… — may have filled")
                log("CANCEL_404_UNCONFIRMED", f"order={order_id}")
                return "not_found"
            else:
                print(f"  ⚠️  Cancel {order_id[:12]}… status={resp.status}: {body[:80]}")
                log("CANCEL_ERROR", f"order={order_id} status={resp.status}")
                return "error"
    except Exception as e:
        print(f"  ❌ Cancel error: {e}")
        return "error"


async def place_taker_order(session, team: str, v2_side: str, price: float,
                            quantity: float | None = None) -> tuple[bool, dict]:
    qty   = TRADE_QUANTITY if quantity is None else quantity
    price = round(min(0.99, max(0.01, price)), 4)
    payload = {
        "ticker":                     TICKERS[team],
        "client_order_id":            str(uuid.uuid4()),
        "side":                       v2_side,
        "count":                      f"{qty:.2f}",
        "price":                      f"{price:.4f}",
        "time_in_force":              "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
    }
    try:
        async with session.post(f"{BASE_REST}{ORDER_PATH}", json=payload,
                                headers=auth("POST", ORDER_PATH)) as resp:
            res    = await resp.json()
            filled = resp.status == 201 and float(res.get("fill_count", "0")) > 0
            return filled, res
    except Exception as e:
        return False, {"error": str(e)}


async def cancel_all_makers(session):
    """Cancel every resting maker across all levels. Used at startup/shutdown/reconnect."""
    for orders_dict in (bid_orders, ask_orders):
        for order in list(orders_dict.values()):
            await cancel_order(session, order["order_id"])
    bid_orders.clear()
    ask_orders.clear()


async def cancel_all_makers_frozen(session):
    """
    Freeze-safe cancel: iterates all active orders across all levels.
    A 404 on an order old enough to have filled triggers an immediate hedge taker.
    Re-entrancy guard prevents a second freeze tick from double-cancelling.
    """
    global freeze_cancel_in_progress

    if freeze_cancel_in_progress:
        return
    freeze_cancel_in_progress = True

    try:
        # Snapshot all active orders before any awaits
        targets = (
            [("bid", lv, o) for lv, o in list(bid_orders.items())] +
            [("ask", lv, o) for lv, o in list(ask_orders.items())]
        )

        for side, level, order in targets:
            oid       = order["order_id"]
            px        = order["price"]
            placed_at = order["placed_at"]
            hedged    = order["fill_hedged"]

            orders_dict = bid_orders if side == "bid" else ask_orders
            # Skip if already removed by a concurrent fill handler
            if orders_dict.get(level, {}).get("order_id") != oid:
                continue

            result = await cancel_order(session, oid)

            if result == "confirmed":
                orders_dict.pop(level, None)

            elif result == "not_found":
                age = time.time() - (placed_at or time.time())
                if age >= MIN_ORDER_AGE_FOR_FILL:
                    remaining_qty = max(0.0, TRADE_QUANTITY - hedged)
                    orders_dict.pop(level, None)
                    log("FILL_INFERRED_404_FROZEN", f"{side} L{level} id={oid} age={age:.3f}s qty={remaining_qty:.4f}")
                    await on_maker_filled(session, side, px, oid, quantity=remaining_qty)
                else:
                    log("CANCEL_404_TOO_FRESH_FROZEN", f"{side} L{level} id={oid} age={age:.3f}s — leaving tracked")
            # "error" → leave tracked, conservative

    finally:
        freeze_cancel_in_progress = False

# ════════════════════════════════════════════════════════════
# FILL HANDLERS
# ════════════════════════════════════════════════════════════
def _find_order(order_id: str) -> tuple[str, int, dict] | tuple[None, None, None]:
    """Search bid_orders and ask_orders for a matching order_id. Returns (side, level, order)."""
    for lv, o in bid_orders.items():
        if o["order_id"] == order_id:
            return "bid", lv, o
    for lv, o in ask_orders.items():
        if o["order_id"] == order_id:
            return "ask", lv, o
    return None, None, None


async def on_partial_fill(session, filled_side: str, fill_price: float, quantity: float):
    """
    Fires a taker for `quantity` contracts immediately on a partial fill.
    Does NOT cancel other orders — the resting portion stays alive.
    """
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n  ⚡ [{ts}] PARTIAL FILL: {filled_side.upper()} @ ${fill_price:.4f}  qty={quantity:.4f}")
    log("PARTIAL_FILL", f"side={filled_side} price={fill_price} qty={quantity:.4f}")

    if filled_side == "bid":
        cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
        print(f"  🎯 Partial Hedge: BUY {quantity:.4f}× CER_YES  mkt={cer_y:.4f}  limit=${taker_px:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY CER_YES qty={quantity:.4f} limit={taker_px}")
        filled, res = await place_taker_order(session, "CER", "bid", taker_px, quantity)
    else:
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
        print(f"  🎯 Partial Hedge: BUY {quantity:.4f}× CER_NO  mkt={cer_n:.4f}  ask limit=${taker_px:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY CER_NO qty={quantity:.4f} ask@{taker_px}")
        filled, res = await place_taker_order(session, "CER", "ask", taker_px, quantity)

    if filled:
        qty_filled = float(res.get("fill_count", str(quantity)))
        avg        = float(res.get("average_fill_price", taker_px))
        print(f"  ✅ Partial Hedge FILLED  qty={qty_filled:.4f} @ ${avg:.4f}")
        log("PARTIAL_HEDGE_FILLED", f"qty={qty_filled:.4f} avg={avg}")
    else:
        print(f"  🚨 PARTIAL HEDGE FAILED → {res}")
        log("PARTIAL_HEDGE_MISSED", f"side={filled_side} qty={quantity:.4f}: {res}")


async def on_maker_filled(session, filled_side: str, fill_price: float, order_id: str,
                          quantity: float | None = None):
    """
    Called on the FULL (or final) fill of a maker order.
    Fires a hedge taker for `quantity` contracts.

    NO GLOBAL LOCK — other resting makers at other levels are not cancelled here.
    They remain alive and will be managed by the normal threshold/stale checks.
    """
    hedge_qty = TRADE_QUANTITY if quantity is None else quantity

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n{'='*70}")
    print(f"  🔥 [{ts}] MAKER FILLED: {filled_side.upper()} @ ${fill_price:.4f}  hedge_qty={hedge_qty:.4f}")
    print(f"  id={order_id}")
    log("MAKER_FILLED", f"side={filled_side} fill_price={fill_price} hedge_qty={hedge_qty:.4f} id={order_id}")

    # Prepare and fire taker — no other orders touched
    if hedge_qty <= 0.001:
        print(f"  ✅ All qty already hedged via prior partial fills.")
        log("HEDGE_SKIPPED", "all qty hedged via partial fills")
        print(f"{'='*70}\n")
        return

    if filled_side == "bid":
        cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
        print(f"  🎯 Hedge: BUY {hedge_qty:.4f}× CER_YES  mkt={cer_y:.4f}  limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY CER_YES qty={hedge_qty:.4f} limit={taker_px} mkt={cer_y}")
        filled, res = await place_taker_order(session, "CER", "bid", taker_px, hedge_qty)
        if filled:
            avg   = float(res.get("average_fill_price", taker_px))
            total = round(fill_price + avg, 4)
            print(f"  ✅ Hedge FILLED @ ${avg:.4f}  total=${total:.4f}")
            print(f"  💰 PROFIT: $1.00 − ${total:.4f} = ${round(1-total,4):.4f}")
            log("HEDGE_FILLED", f"avg={avg} total={total} profit={round(1-total,4)}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY CER_YES @ {taker_px} → {res}")
            print(f"  🚨 HOLDING {hedge_qty:.4f}× FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_YES qty={hedge_qty:.4f} limit={taker_px}: {res}")
    else:
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
        print(f"  🎯 Hedge: BUY {hedge_qty:.4f}× CER_NO  mkt={cer_n:.4f}  ask limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY CER_NO qty={hedge_qty:.4f} ask@{taker_px} mkt={cer_n}")
        filled, res = await place_taker_order(session, "CER", "ask", taker_px, hedge_qty)
        if filled:
            yes_avg = float(res.get("average_fill_price", taker_px))
            no_cost = round(1.0 - yes_avg, 4)
            profit  = round(fill_price - no_cost, 4)
            print(f"  ✅ Hedge FILLED: CER_NO cost=${no_cost:.4f}")
            print(f"  💰 PROFIT: ${fill_price:.4f} − ${no_cost:.4f} = ${profit:.4f}")
            log("HEDGE_FILLED", f"no_cost={no_cost} fer_sold={fill_price} profit={profit}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY CER_NO ask @ {taker_px} → {res}")
            print(f"  🚨 SHORT {hedge_qty:.4f}× FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_NO qty={hedge_qty:.4f} ask={taker_px}: {res}")

    print(f"{'='*70}\n")

# ════════════════════════════════════════════════════════════
# WS FILL DETECTION
# ════════════════════════════════════════════════════════════
async def handle_user_order_update(session, msg: dict):
    order_id = msg.get("order_id", "")

    if order_id in cancelled_order_ids:
        print(f"  Cancelled order WS update ignored: {msg.get('status','')} {order_id[:12]}")
        cancelled_order_ids.discard(order_id)
        return

    filled_side, level, order_info = _find_order(order_id)
    if order_info is None:
        return

    status     = msg.get("status", "")
    fill_count = float(msg.get("fill_count_fp",    "0"))
    remaining  = float(msg.get("remaining_count_fp", "1"))

    is_fully_filled = (status == "filled") or (fill_count >= TRADE_QUANTITY and remaining < 0.001)

    if fill_count <= 0:
        return

    already_hedged = order_info["fill_hedged"]
    incremental    = round(fill_count - already_hedged, 6)

    if incremental < 0.001:
        if is_fully_filled:
            # Nothing new to hedge; remove from tracking
            orders_dict = bid_orders if filled_side == "bid" else ask_orders
            orders_dict.pop(level, None)
            log("WS_FILL_ALREADY_HEDGED", f"side={filled_side} L{level} id={order_id}")
        return

    fill_price = float(msg.get("yes_price_dollars", "0"))
    if fill_price == 0:
        cost = float(msg.get("maker_fill_cost_dollars", "0"))
        fill_price = (cost / fill_count) if fill_count > 0 else (order_info["price"] or 0)

    if not is_fully_filled:
        # Partial fill: hedge increment, leave order resting
        log("WS_PARTIAL_FILL", f"side={filled_side} L{level} fill={fill_count:.4f} rem={remaining:.4f} incr={incremental:.4f}")
        await on_partial_fill(session, filled_side, fill_price, incremental)
        order_info["fill_hedged"] = fill_count   # update AFTER taker fires
    else:
        # Final fill: hedge remainder, remove from tracking
        orders_dict = bid_orders if filled_side == "bid" else ask_orders
        orders_dict.pop(level, None)
        log("WS_FILL_DETECTED", f"side={filled_side} L{level} price={fill_price} incr={incremental:.4f} id={order_id}")
        await on_maker_filled(session, filled_side, fill_price, order_id, quantity=incremental)

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT — multi-level
# ════════════════════════════════════════════════════════════
async def update_maker_orders(session):
    """
    For each spread level (0 … NUM_SPREAD_LEVELS−1):
      • bid target = 1 − CER_Y − BID_SPREAD − level × SPREAD_STEP
      • ask target = CER_N + ASK_SPREAD + level × SPREAD_STEP

    Per order, cancel if:
      • resting price + CER_Y > THRESHOLD  (bid too costly now)
      • resting price − CER_N < (1−THRESHOLD) (ask margin too thin now)
      • target price moved ≥ UPDATE_TICK (reprice)

    On 404: age check → infer fill if old enough, else leave tracked.
    No global lock — other levels continue running after any fill.
    """
    global updating_orders

    if updating_orders or market_frozen or freeze_cancel_in_progress:
        return

    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")
    if cer_y is None or cer_n is None:
        return

    updating_orders = True
    try:
        for level in range(NUM_SPREAD_LEVELS):
            spread_bid = BID_SPREAD + level * SPREAD_STEP
            spread_ask = ASK_SPREAD + level * SPREAD_STEP

            target_bid = round(max(0.01, 1.0 - cer_y - spread_bid), 2)
            target_ask = round(min(0.99, cer_n + spread_ask),        2)
            bid_valid  = (target_bid + cer_y) <= THRESHOLD
            ask_valid  = (target_ask - cer_n) >= (1.0 - THRESHOLD)

            # ── BID ───────────────────────────────────────
            existing_bid = bid_orders.get(level)
            if existing_bid:
                oid = existing_bid["order_id"]
                stale      = abs(target_bid - existing_bid["price"]) >= UPDATE_TICK
                too_costly = existing_bid["price"] + cer_y > THRESHOLD
                if stale or too_costly or not bid_valid:
                    if market_frozen: break
                    result = await cancel_order(session, oid)
                    # Re-check: fill handler may have removed this slot during the await
                    if bid_orders.get(level, {}).get("order_id") == oid:
                        if result == "confirmed":
                            bid_orders.pop(level, None)
                        elif result == "not_found":
                            age = time.time() - (existing_bid["placed_at"] or time.time())
                            if age >= MIN_ORDER_AGE_FOR_FILL:
                                remaining_qty = max(0.0, TRADE_QUANTITY - existing_bid["fill_hedged"])
                                bid_orders.pop(level, None)
                                log("FILL_INFERRED_404", f"bid L{level} id={oid} age={age:.3f}s qty={remaining_qty:.4f}")
                                await on_maker_filled(session, "bid", existing_bid["price"], oid, quantity=remaining_qty)
                            else:
                                log("CANCEL_404_TOO_FRESH", f"bid L{level} id={oid} age={age:.3f}s — waiting for WS")
                            continue  # move to ask for this level; bid slot handled
                        else:
                            continue  # error: leave tracked, still try ask below

            if level not in bid_orders and bid_valid and not market_frozen:
                oid = await place_maker_order(session, "FER", "bid", target_bid)
                if oid:
                    bid_orders[level] = {"order_id": oid, "price": target_bid,
                                         "placed_at": time.time(), "fill_hedged": 0.0}

            # ── ASK ───────────────────────────────────────
            existing_ask = ask_orders.get(level)
            if existing_ask:
                oid = existing_ask["order_id"]
                stale      = abs(target_ask - existing_ask["price"]) >= UPDATE_TICK
                low_margin = existing_ask["price"] - cer_n < (1.0 - THRESHOLD)
                if stale or low_margin or not ask_valid:
                    if market_frozen: break
                    result = await cancel_order(session, oid)
                    if ask_orders.get(level, {}).get("order_id") == oid:
                        if result == "confirmed":
                            ask_orders.pop(level, None)
                        elif result == "not_found":
                            age = time.time() - (existing_ask["placed_at"] or time.time())
                            if age >= MIN_ORDER_AGE_FOR_FILL:
                                remaining_qty = max(0.0, TRADE_QUANTITY - existing_ask["fill_hedged"])
                                ask_orders.pop(level, None)
                                log("FILL_INFERRED_404", f"ask L{level} id={oid} age={age:.3f}s qty={remaining_qty:.4f}")
                                await on_maker_filled(session, "ask", existing_ask["price"], oid, quantity=remaining_qty)
                            else:
                                log("CANCEL_404_TOO_FRESH", f"ask L{level} id={oid} age={age:.3f}s — waiting for WS")
                            continue
                        else:
                            continue

            if level not in ask_orders and ask_valid and not market_frozen:
                oid = await place_maker_order(session, "FER", "ask", target_ask)
                if oid:
                    ask_orders[level] = {"order_id": oid, "price": target_ask,
                                         "placed_at": time.time(), "fill_hedged": 0.0}

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

    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S")
    fer_y = curr_prices["FER"].get("yes_ask")
    fer_n = curr_prices["FER"].get("no_ask")
    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait.."
    status_flag = " 🧊FROZEN" if market_frozen else ""
    sum_y = f"{round(fer_y + cer_y, 2):.2f}" if (fer_y and cer_y) else " N/A"
    sum_n = f"{round(fer_n + cer_n, 2):.2f}" if (fer_n and cer_n) else " N/A"

    bids_str = "  ".join(
        f"B{lv}@{o['price']:.2f}({o['order_id'][-5:]})"
        for lv, o in sorted(bid_orders.items())
    ) or "BID→none"
    asks_str = "  ".join(
        f"A{lv}@{o['price']:.2f}({o['order_id'][-5:]})"
        for lv, o in sorted(ask_orders.items())
    ) or "ASK→none"

    print(
        f"{ts}{status_flag}  | "
        f"FER_Y: {fmt(fer_y):<6} FER_N: {fmt(fer_n):<6} | "
        f"CER_Y: {fmt(cer_y):<6} CER_N: {fmt(cer_n):<6} | "
        f"SUM_Y: {sum_y}  SUM_N: {sum_n}  | "
        f"{bids_str}  {asks_str}"
    )

# ════════════════════════════════════════════════════════════
# WS MESSAGE HANDLER
# ════════════════════════════════════════════════════════════
async def process_ws_message(session, msg_data: dict):
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
        await handle_user_order_update(session, msg)
        return

    ticker = msg.get("market_ticker")
    if ticker not in TICKERS_REVERSE:
        return
    leg = TICKERS_REVERSE[ticker]
    seq = msg_data.get("seq")

    if seq is not None:
        if not check_sequence(seq, ticker, msg_type):
            if resync_needed:
                await request_resync(session)
            return

    if msg_type == "orderbook_snapshot":
        apply_snapshot(leg, msg); update_derived(leg); tick_count += 1
    elif msg_type == "orderbook_delta":
        apply_delta(leg, msg);    update_derived(leg); tick_count += 1
    else:
        return

    print_live_odds()

    newly_frozen = check_market_speed()
    if newly_frozen and (bid_orders or ask_orders):
        print("  🧊 Cancelling all resting makers due to freeze…")
        while updating_orders:
            await asyncio.sleep(0.01)
        await cancel_all_makers_frozen(session)

    if not market_frozen and not updating_orders:
        asyncio.create_task(update_maker_orders(session))

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
async def monitor():
    global _session

    init_log()
    print("=" * 90)
    print(f"  🎾 Maker-Taker Arb (MULTI-LEVEL)  —  {GAME_DESCRIPTION}")
    print(f"  Levels: {NUM_SPREAD_LEVELS}  Step: {SPREAD_STEP}")
    for lv in range(NUM_SPREAD_LEVELS):
        b = BID_SPREAD + lv * SPREAD_STEP
        a = ASK_SPREAD + lv * SPREAD_STEP
        print(f"    L{lv}: bid=1−CER_Y−{b:.2f}  ask=CER_N+{a:.2f}")
    print(f"  THRESHOLD={THRESHOLD}  TAKER_BUFFER={TAKER_BUFFER}  UPDATE_TICK={UPDATE_TICK}")
    print(f"  FREEZE: tick Δ ≥ {FREEZE_TICK_THRESHOLD} → cancel all, wait {CALM_DURATION}s calm")
    print(f"  No global lock — each fill fires its own taker independently")
    print("=" * 90)

    ws_headers = auth("GET", "/trade-api/ws/v2")

    async with aiohttp.ClientSession() as session:
        _session = session

        try:
            while True:
                try:
                    async with websockets.connect(
                        WS_URL, additional_headers=ws_headers, ping_interval=15
                    ) as ws:
                        print(f"\n✅ Connected: {WS_URL}")
                        await cancel_all_makers(session)

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
                            await process_ws_message(session, json.loads(raw))

                except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                    print(f"\n❌ WS error: {e}. Cancelling makers + retrying in 3s…")
                    await cancel_all_makers(session)
                    await asyncio.sleep(3)

        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n🔴 Shutting down — cancelling makers…")
            log("SHUTDOWN", "manual")
            await cancel_all_makers(session)
            print("🔴 Done.")

if __name__ == "__main__":
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n❌ Fatal: {e}")