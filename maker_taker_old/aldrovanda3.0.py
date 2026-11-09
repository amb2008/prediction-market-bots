"""
Kalshi Maker-Taker Arbitrage: FER vs CER — DUAL-SIDED MAKERS (SEEMINGLY FILLED ORDERS ON BOTH SIDES SIMULTANEOUSLY ACCIDENTALLY)

Resting maker orders are now placed on BOTH markets simultaneously:
  FER_Y BID = 1 − CER_Y − BID_SPREAD     (hedge on fill: take CER_Y)
  FER_Y ASK = CER_N + ASK_SPREAD          (hedge on fill: take CER_N)
  CER_Y BID = 1 − FER_Y − BID_SPREAD     (hedge on fill: take FER_Y)
  CER_Y ASK = FER_N + ASK_SPREAD          (hedge on fill: take FER_N)

Whichever of the four resting orders fills first triggers on_maker_filled,
which cancels the OTHER THREE resting orders and fires exactly one hedge
taker on the opposite market, then locks permanently.

FILL DETECTION
─────────────────────────
Primary:  user_orders WS channel — fires on_maker_filled the moment an order executes.
Secondary: 404-age inference — cancel returning 404 on an order ≥ MIN_ORDER_AGE_FOR_FILL
           seconds old is treated as a confirmed fill; taker fires immediately.

MARKET FREEZE
─────────────
If any single WS tick moves any tracked price by ≥ FREEZE_TICK_THRESHOLD:
  • All resting maker orders (all four) are cancelled immediately.
  • No new maker orders are placed until CALM_DURATION seconds pass with no
    tick exceeding FREEZE_TICK_THRESHOLD.
  • The freeze state is shown in the live printout as 🧊FROZEN.
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
# CONFIG — edit this block for each match
# ═══════════════════════════════════════════════════════════
KALSHI_KEY_ID    = 'DUMMY_KEY_ID'
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_REST         = "https://external-api.kalshi.com"
WS_URL            = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
ORDER_PATH        = "/trade-api/v2/portfolio/events/orders"
CHECK_ORDER_PATH  = "/trade-api/v2/portfolio/orders"

TICKERS = {
    "FER": "KXATPMATCH-26JUN29KOKBUB-KOK",
    "CER": "KXATPMATCH-26JUN29KOKBUB-BUB"
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}
LEGS = list(TICKERS.keys())          # ["FER", "CER"]
GAME_DESCRIPTION = "Ferrer vs Cerundolo (ATP)"

TRADE_QUANTITY          = 1
BID_SPREAD              = 0.03   # this_Y bid = 1 - other_Y - BID_SPREAD
ASK_SPREAD              = 0.03   # this_Y ask = other_N + ASK_SPREAD
THRESHOLD               = 0.98
TAKER_BUFFER            = 0.10
UPDATE_TICK             = 0.01
MIN_ORDER_AGE_FOR_FILL  = 0.1    # seconds — 404 cancel on orders older than this → infer fill

FREEZE_TICK_THRESHOLD   = 0.05   # single-tick move ≥ this → freeze all makers
CALM_DURATION           = 2.0    # seconds without a big move → unfreeze and resume

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

# ── Maker-order tracking — now per-leg, per-side ─────────────
# makers[leg]["bid"] / makers[leg]["ask"] each hold:
#   {"order_id": str|None, "price": float|None, "placed_at": float|None}
makers: dict[str, dict[str, dict]] = {
    leg: {
        "bid": {"order_id": None, "price": None, "placed_at": None},
        "ask": {"order_id": None, "price": None, "placed_at": None},
    }
    for leg in LEGS
}

cancelled_order_ids: set[str] = set()

# ── Safety flags ─────────────────────────────────────────────
maker_filled:    bool  = False
updating_orders: bool  = False
hedge_executed:  bool  = False

# ── Market speed / freeze state ──────────────────────────────
market_frozen:             bool  = False
last_big_move_at:          float = 0.0
freeze_cancel_in_progress: bool  = False

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

def other_leg(leg: str) -> str:
    """Returns the other tracked leg given one leg name. Only valid for 2-leg setups."""
    return LEGS[1] if leg == LEGS[0] else LEGS[0]

# ════════════════════════════════════════════════════════════
# MARKET SPEED / FREEZE
# ════════════════════════════════════════════════════════════
def check_market_speed() -> bool:
    global market_frozen, last_big_move_at

    max_move = 0.0
    for leg in TICKERS:
        for field in ("yes_ask", "no_ask", "yes_bid", "no_bid"):
            curr = curr_prices[leg].get(field)
            prev = prev_prices[leg].get(field)
            if curr is not None and prev is not None:
                max_move = max(max_move, abs(curr - prev))

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
            print(f"  ⚠️  Maker place failed ({team} {v2_side}): {res}")
            log("MAKER_PLACE_FAIL", f"team={team} {res}")
            return None
    except Exception as e:
        print(f"  ❌ Maker order error ({team}): {e}")
        return None


async def cancel_order(session, order_id: str) -> str:
    """
    Returns "confirmed" (200/204), "not_found" (404), or "error".
    Only adds to cancelled_order_ids on confirmed cancel.
    """
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
                print(f"  ⚠️  Cancel 404 for {order_id[:12]}… — order not found, may have filled")
                log("CANCEL_404_UNCONFIRMED", f"order={order_id}")
                return "not_found"
            else:
                print(f"  ⚠️  Cancel {order_id[:12]}… status={resp.status}: {body[:80]}")
                log("CANCEL_ERROR", f"order={order_id} status={resp.status}")
                return "error"
    except Exception as e:
        print(f"  ❌ Cancel error: {e}")
        return "error"


async def place_taker_order(session, team: str, v2_side: str, price: float) -> tuple[bool, dict]:
    price = round(min(0.99, max(0.01, price)), 4)
    payload = {
        "ticker":                     TICKERS[team],
        "client_order_id":            str(uuid.uuid4()),
        "side":                       v2_side,
        "count":                      f"{TRADE_QUANTITY}.00",
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


def _clear_maker(leg: str, side: str):
    makers[leg][side]["order_id"]  = None
    makers[leg][side]["price"]     = None
    makers[leg][side]["placed_at"] = None


async def cancel_all_makers(session):
    """Cancels all four resting makers (both legs, both sides). Used at
    startup/reconnect/shutdown — not the freeze-safe path."""
    for leg in LEGS:
        for side in ("bid", "ask"):
            oid = makers[leg][side]["order_id"]
            if oid:
                await cancel_order(session, oid)
            _clear_maker(leg, side)


async def cancel_all_makers_frozen(session):
    """
    Used ONLY when a freeze is triggered (see process_ws_message).

    Cancels all four resting makers (both legs, both sides). A "not_found"
    (404) result on any cancel is checked against MIN_ORDER_AGE_FOR_FILL:

      • old enough  → treat as a confirmed fill, fire the hedge taker via
                       on_maker_filled immediately (locks maker_filled and
                       clears all four legs' tracking).
      • too fresh   → leave that order's tracking untouched so the WS
                       user_order channel can still resolve it later.

    Re-entrancy guard: freeze_cancel_in_progress ensures only one cancel
    pass runs at a time, so a second big tick arriving mid-cancel can't
    re-snapshot and double-cancel the same order_id.
    """
    global freeze_cancel_in_progress

    if freeze_cancel_in_progress:
        return
    freeze_cancel_in_progress = True

    try:
        # Snapshot all four (leg, side, order_id, price, placed_at) up front
        targets = []
        for leg in LEGS:
            for side in ("bid", "ask"):
                m = makers[leg][side]
                if m["order_id"]:
                    targets.append((leg, side, m["order_id"], m["price"], m["placed_at"]))

        for leg, side, oid, px, placed_at in targets:
            if maker_filled:
                return

            result = await cancel_order(session, oid)
            if maker_filled:
                return

            if result == "confirmed":
                _clear_maker(leg, side)

            elif result == "not_found":
                age = time.time() - (placed_at or time.time())
                if age >= MIN_ORDER_AGE_FOR_FILL:
                    log("FILL_INFERRED_404_FROZEN", f"{leg}_{side} id={oid} age={age:.3f}s — firing taker")
                    await on_maker_filled(session, leg, side, px or 0, oid)
                    return
                else:
                    log("CANCEL_404_TOO_FRESH_FROZEN", f"{leg}_{side} id={oid} age={age:.3f}s — leaving tracked")
                    # leave this maker's tracking untouched; WS will resolve it

            # "error" — also leave tracking untouched, conservative default.

    finally:
        freeze_cancel_in_progress = False

# ════════════════════════════════════════════════════════════
# FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_maker_filled(session, filled_leg: str, filled_side: str, fill_price: float, order_id: str):
    """
    filled_leg:  "FER" or "CER" — which market's resting order filled.
    filled_side: "bid" or "ask" — which side filled.

    On a BID fill (we bought {filled_leg}_YES):
      Hedge: BUY {other_leg}_YES → covers the other_leg-wins outcome.
    On an ASK fill (we sold {filled_leg}_YES, i.e. went short it):
      Hedge: BUY {other_leg}_NO  → covers the filled_leg-wins outcome.
    """
    global maker_filled, hedge_executed

    if maker_filled:
        return

    hedge_leg = other_leg(filled_leg)

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n{'='*70}")
    print(f"  🔥 [{ts}] MAKER FILLED: {filled_leg} {filled_side.upper()} @ ${fill_price:.4f}")
    print(f"  Detected via: {'WS' if order_id else 'REST poll'} | id={order_id}")
    log("MAKER_FILLED", f"leg={filled_leg} side={filled_side} fill_price={fill_price} id={order_id}")

    maker_filled   = True
    hedge_executed = False

    # Claim ALL THREE other resting orders for cancellation right now,
    # synchronously, with no await in between. update_maker_orders checks
    # maker_filled (already set above) before issuing its own cancel, so it
    # backs off instead of racing us. The hedge fires with zero added delay.
    other_orders = []
    for leg in LEGS:
        for side in ("bid", "ask"):
            if leg == filled_leg and side == filled_side:
                continue   # this is the order that just filled, not ours to cancel
            oid = makers[leg][side]["order_id"]
            if oid:
                other_orders.append(oid)
            _clear_maker(leg, side)
    _clear_maker(filled_leg, filled_side)   # also clear the filled order's own tracking

    # Prepare taker coroutine before any awaits
    if filled_side == "bid":
        hedge_y  = curr_prices[hedge_leg].get("yes_ask") or 0.99
        taker_px = round(min(0.99, hedge_y + TAKER_BUFFER), 4)
        print(f"  🎯 Hedge: BUY {hedge_leg}_YES  mkt={hedge_y:.4f}  limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY {hedge_leg}_YES limit={taker_px} mkt={hedge_y}")
        taker_coro = place_taker_order(session, hedge_leg, "bid", taker_px)
    else:
        hedge_n  = curr_prices[hedge_leg].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - hedge_n - TAKER_BUFFER), 4)
        print(f"  🎯 Hedge: BUY {hedge_leg}_NO  mkt={hedge_n:.4f}  ask limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY {hedge_leg}_NO ask@{taker_px} mkt={hedge_n}")
        taker_coro = place_taker_order(session, hedge_leg, "ask", taker_px)

    # Fire hedge and cancel the other resting makers concurrently
    cancel_coros = [cancel_order(session, oid) for oid in other_orders]
    if cancel_coros:
        print(f"  ⚡ Executing hedge and cancelling {len(other_orders)} other maker(s) concurrently")
        results = await asyncio.gather(taker_coro, *cancel_coros)
        filled, res = results[0]
    else:
        filled, res = await taker_coro

    if filled_side == "bid":
        if filled:
            avg   = float(res.get("average_fill_price", taker_px))
            total = round(fill_price + avg, 4)
            print(f"  ✅ Hedge FILLED @ ${avg:.4f}  total=${total:.4f}")
            print(f"  💰 LOCKED PROFIT: $1.00 − ${total:.4f} = ${round(1-total,4):.4f}")
            log("HEDGE_FILLED", f"avg={avg} total={total} profit={round(1-total,4)}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY {hedge_leg}_YES @ {taker_px} → {res}")
            print(f"  🚨 HOLDING {filled_leg}_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY {hedge_leg}_YES limit={taker_px}: {res}")
    else:
        if filled:
            yes_avg = float(res.get("average_fill_price", taker_px))
            no_cost = round(1.0 - yes_avg, 4)
            profit  = round(fill_price - no_cost, 4)
            print(f"  ✅ Hedge FILLED: {hedge_leg}_NO cost=${no_cost:.4f}")
            print(f"  💰 LOCKED PROFIT: ${fill_price:.4f} − ${no_cost:.4f} = ${profit:.4f}")
            log("HEDGE_FILLED", f"no_cost={no_cost} {filled_leg}_sold={fill_price} profit={profit}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY {hedge_leg}_NO ask @ {taker_px} → {res}")
            print(f"  🚨 SHORT {filled_leg}_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY {hedge_leg}_NO ask={taker_px}: {res}")

    hedge_executed = True
    print(f"\n  🔒 MAKER MODE LOCKED. Restart to resume.")
    print(f"{'='*70}\n")

# ════════════════════════════════════════════════════════════
# WS FILL DETECTION
# ════════════════════════════════════════════════════════════
async def handle_user_order_update(session, msg: dict):
    if maker_filled:
        return

    order_id = msg.get("order_id", "")

    if order_id in cancelled_order_ids:
        print("Canceled order update received; ignoring.", msg.get("status", ""), order_id)
        cancelled_order_ids.discard(order_id)
        return

    # Find which (leg, side) this order_id belongs to
    target = None
    for leg in LEGS:
        for side in ("bid", "ask"):
            if makers[leg][side]["order_id"] == order_id:
                target = (leg, side)
                break
        if target:
            break
    if target is None:
        return
    filled_leg, filled_side = target

    status     = msg.get("status", "")
    fill_count = float(msg.get("fill_count_fp",    "0"))
    remaining  = float(msg.get("remaining_count_fp", "1"))

    is_filled = (status == "filled") or (fill_count >= TRADE_QUANTITY and remaining < 0.001)
    if not is_filled:
        if fill_count > 0:
            print(f"  ⚡ Partial fill on {filled_leg}_{filled_side}: {fill_count} filled, {remaining} remain")
        return

    fill_price = float(msg.get("yes_price_dollars", "0"))
    if fill_price == 0:
        cost = float(msg.get("maker_fill_cost_dollars", "0"))
        fallback_px = makers[filled_leg][filled_side]["price"] or 0
        fill_price = (cost / fill_count) if fill_count > 0 else fallback_px

    log("WS_FILL_DETECTED", f"leg={filled_leg} side={filled_side} status={status} price={fill_price} id={order_id}")
    await on_maker_filled(session, filled_leg, filled_side, fill_price, order_id)

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ════════════════════════════════════════════════════════════
async def _update_one_side(session, leg: str, side: str, target_price: float, is_valid: bool):
    """
    Manages a single resting maker (one leg, one side).
    Mirrors the bid/ask logic from the original single-sided version,
    generalized to work for any (leg, side) pair.
    """
    m = makers[leg][side]

    if m["order_id"] is not None:
        stale = m["price"] is None or abs(target_price - m["price"]) >= UPDATE_TICK
        # Validity re-check uses the same threshold logic as is_valid, computed by caller
        if stale or not is_valid:
            oid_to_cancel = m["order_id"]
            # Re-check right before sending the cancel: on_maker_filled may have
            # claimed this exact order_id in the gap since our entry check.
            if maker_filled:
                return
            result = await cancel_order(session, oid_to_cancel)
            if maker_filled:
                return

            if result == "confirmed":
                _clear_maker(leg, side)
            elif result == "not_found":
                age = time.time() - (m["placed_at"] or time.time())
                if age >= MIN_ORDER_AGE_FOR_FILL:
                    log("FILL_INFERRED_404", f"{leg}_{side} id={oid_to_cancel} age={age:.3f}s — firing taker")
                    await on_maker_filled(session, leg, side, m["price"] or 0, oid_to_cancel)
                else:
                    log("CANCEL_404_TOO_FRESH", f"{leg}_{side} id={oid_to_cancel} age={age:.3f}s — waiting for WS")
                return
            else:
                return

    if m["order_id"] is None and is_valid and not maker_filled and not market_frozen:
        v2_side = side   # "bid" or "ask" map directly to Kalshi's v2_side
        oid = await place_maker_order(session, leg, v2_side, target_price)
        if maker_filled:
            return
        if oid:
            m["order_id"]  = oid
            m["price"]     = target_price
            m["placed_at"] = time.time()


async def update_maker_orders(session):
    """
    Maintains resting bid+ask makers on BOTH legs simultaneously.

    For leg L with other leg O:
      L_bid target = 1 − O_yes_ask − BID_SPREAD   (hedge: take O_YES)
      L_ask target = O_no_ask  + ASK_SPREAD        (hedge: take O_NO)

    bid_valid: L_bid + O_yes_ask ≤ THRESHOLD
    ask_valid: L_ask − O_no_ask  ≥ (1 − THRESHOLD)
    """
    global updating_orders

    if maker_filled or updating_orders or market_frozen or freeze_cancel_in_progress:
        return

    updating_orders = True
    try:
        for leg in LEGS:
            opp = other_leg(leg)
            opp_y = curr_prices[opp].get("yes_ask")
            opp_n = curr_prices[opp].get("no_ask")
            if opp_y is None or opp_n is None:
                continue

            target_bid = round(max(0.01, 1.0 - opp_y - BID_SPREAD), 2)
            target_ask = round(min(0.99, opp_n + ASK_SPREAD),        2)
            bid_valid  = (target_bid + opp_y) <= THRESHOLD
            ask_valid  = (target_ask - opp_n) >= (1.0 - THRESHOLD)

            await _update_one_side(session, leg, "bid", target_bid, bid_valid)
            if maker_filled:
                return
            await _update_one_side(session, leg, "ask", target_ask, ask_valid)
            if maker_filled:
                return

    finally:
        updating_orders = False

# ════════════════════════════════════════════════════════════
# LIVE PRINTOUT
# ════════════════════════════════════════════════════════════
last_printed: dict = {leg: {} for leg in LEGS}

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
    status_flag = " 🔒LOCKED" if maker_filled else (" 🧊FROZEN" if market_frozen else "")

    def mk_stat(leg, side):
        m = makers[leg][side]
        if m["order_id"]:
            return f"{leg[:1]}{side[0].upper()}@{m['price']:.2f}({m['order_id'][-6:]})"
        return f"{leg[:1]}{side[0].upper()}→.."

    makers_str = "  ".join(mk_stat(leg, side) for leg in LEGS for side in ("bid", "ask"))

    sum_y = f"{round(fer_y + cer_y, 2):.2f}" if (fer_y and cer_y) else " N/A"
    sum_n = f"{round(fer_n + cer_n, 2):.2f}" if (fer_n and cer_n) else " N/A"

    print(
        f"{ts}{status_flag}  | "
        f"FER_Y: {fmt(fer_y):<6} FER_N: {fmt(fer_n):<6} | "
        f"CER_Y: {fmt(cer_y):<6} CER_N: {fmt(cer_n):<6} | "
        f"SUM_Y: {sum_y}  SUM_N: {sum_n}  | "
        f"MAKERS: {makers_str}"
    )

# ════════════════════════════════════════════════════════════
# WS MESSAGE HANDLER
# ════════════════════════════════════════════════════════════
async def process_ws_message(session, msg_data: dict):
    global tick_count
    msg_type = msg_data.get("type")
    msg      = msg_data.get("msg", {})

    if msg_type == "subscribed":
        print(f"  ✅ Subscribed: sid={msg_data.get('sid')} channels={msg_data.get('msg',{}).get('channels','?')}")
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

    any_makers_resting = any(
        makers[l][s]["order_id"] for l in LEGS for s in ("bid", "ask")
    )

    newly_frozen = check_market_speed()
    if newly_frozen and any_makers_resting:
        print("  🧊 Cancelling all resting makers due to freeze…")
        while updating_orders:
            await asyncio.sleep(0.01)
        await cancel_all_makers_frozen(session)

    if not maker_filled and not market_frozen and not updating_orders:
        asyncio.create_task(update_maker_orders(session))

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
async def monitor():
    global _session

    init_log()
    print("=" * 90)
    print(f"  🎾 Maker-Taker Arb (DUAL-SIDED)  —  {GAME_DESCRIPTION}")
    print(f"  FER_Y BID = 1 − CER_Y − {BID_SPREAD}   CER_Y BID = 1 − FER_Y − {BID_SPREAD}")
    print(f"  FER_Y ASK = CER_N + {ASK_SPREAD}        CER_Y ASK = FER_N + {ASK_SPREAD}")
    print(f"  THRESHOLD={THRESHOLD}  UPDATE_TICK={UPDATE_TICK}  MIN_ORDER_AGE={MIN_ORDER_AGE_FOR_FILL}s")
    print(f"  FREEZE: tick Δ ≥ {FREEZE_TICK_THRESHOLD} → cancel all, wait {CALM_DURATION}s calm to resume")
    print("=" * 90)

    ws_headers = auth("GET", "/trade-api/ws/v2")

    async with aiohttp.ClientSession() as session:
        _session = session

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