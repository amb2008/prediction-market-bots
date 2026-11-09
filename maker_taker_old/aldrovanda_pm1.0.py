"""
Kalshi Maker / Polymarket Taker Arbitrage

STRATEGY
─────────
Resting GTC maker orders on Kalshi FER.
When one fills, the hedge taker fires on Polymarket (same game, opposite outcome).

PRICE FEEDS
───────────
• Kalshi WS  → orderbook_delta for FER + CER (bid/ask derivation, freeze detection)
• Polymarket WS → market_data_lite for PM_SLUG (real-time pm_yes_ask / pm_no_ask)

MAKER PRICING  (all based on live PM prices, not Kalshi CER proxy)
──────────────────────────────────────────────────────────────────
  bid target = 1 − pm_yes_ask − BID_SPREAD
    (if we get filled, we buy PM YES at pm_yes_ask + buffer → covered)
    valid when: bid + pm_yes_ask ≤ THRESHOLD

  ask target = pm_no_ask + ASK_SPREAD
    (if we get filled, we buy PM NO at pm_no_ask + buffer → covered)
    valid when: ask − pm_no_ask ≥ (1 − THRESHOLD)

HEDGE TAKER
───────────
  BID fill → BUY PM ENG_YES (OUTCOME_SIDE_YES)
  ASK fill → BUY PM ENG_NO  (OUTCOME_SIDE_NO)

FILL DETECTION
──────────────
Primary:   Kalshi user_orders WS.
Secondary: 404-age inference on cancel calls.
Partial fills hedged immediately at exact incremental quantity.

MARKET FREEZE
─────────────
Big Kalshi tick (≥ FREEZE_TICK_THRESHOLD) → cancel all makers, wait CALM_DURATION.
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
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as rsa_padding
from cryptography.hazmat.primitives.asymmetric import ed25519

# ═══════════════════════════════════════════════════════════
# CONFIG — edit this block for each match
# ═══════════════════════════════════════════════════════════
KALSHI_KEY_ID     = 'DUMMY_KEY_ID'
KALSHI_KEY_PATH   = r"C:\Users\aberm\OneDrive\Desktop\paper.txt"
KALSHI_REST       = "https://external-api.kalshi.com"
KALSHI_WS_URL     = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_ORDER_PATH = "/trade-api/v2/portfolio/events/orders"

KALSHI_TICKERS = {
    "FER": "KXWTAMATCH-26JUL02RAKSAK-SAK",
    "CER": "KXWTAMATCH-26JUL02RAKSAK-RAK"
}
KALSHI_TICKERS_REVERSE = {v: k for k, v in KALSHI_TICKERS.items()}

PM_KEY_ID       = 'DUMMY_KEY_ID'
PM_KEY_PATH     = r"C:\Users\aberm\OneDrive\Desktop\poly.txt"
PM_REST         = "https://api.polymarket.us"
PM_ORDER_PATH   = "/v1/orders"
PM_SLUG         = "aec-wta-kamrak-marsak-2026-07-01"
# YES = ENG/FER wins  |  NO = COD/CER wins

GAME_DESCRIPTION       = "ENG vs COD (World Cup 2026)"
TRADE_QUANTITY         = 1
BID_SPREAD             = 0.03    # Kalshi bid = 1 − pm_yes_ask − BID_SPREAD
ASK_SPREAD             = 0.03    # Kalshi ask = pm_no_ask   + ASK_SPREAD
THRESHOLD              = 0.97
TAKER_BUFFER           = 0.10    # added to PM ask when placing hedge taker
UPDATE_TICK            = 0.01
MIN_ORDER_AGE_FOR_FILL = 0.1
FREEZE_TICK_THRESHOLD  = 0.05
CALM_DURATION          = 2.0
LOG_FILE               = "kalshi_maker_pm_taker.csv"
# ═══════════════════════════════════════════════════════════

# ── Keys ────────────────────────────────────────────────────
with open(KALSHI_KEY_PATH, "rb") as _fh:
    _KALSHI_KEY = serialization.load_pem_private_key(_fh.read(), password=None)

with open(PM_KEY_PATH, "r") as _fh:
    _PM_KEY = ed25519.Ed25519PrivateKey.from_private_bytes(
        base64.b64decode(_fh.read().strip())[:32]
    )

# ── Kalshi price / orderbook state ──────────────────────────
curr_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None}
               for k in KALSHI_TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None}
               for k in KALSHI_TICKERS}
orderbooks  = {k: {"yes": {}, "no": {}} for k in KALSHI_TICKERS}
last_seq:    dict[str, int] = {}
resync_needed: set[str]     = set()

# ── Live PM prices (updated via PM WS) ──────────────────────
pm_yes_ask: float | None = None   # cost to buy ENG YES on PM (best ask)
pm_no_ask:  float | None = None   # cost to buy ENG NO  on PM (best ask)
# Note: PM WS gives bestBid/bestAsk for YES only.
# pm_yes_ask = bestAsk   (what we'd pay to buy YES)
# pm_no_ask  = 1 − bestBid  (implied NO ask: what we'd pay to buy NO)

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
_kalshi_session: aiohttp.ClientSession | None = None
_pm_session:     aiohttp.ClientSession | None = None

# ════════════════════════════════════════════════════════════
# AUTH
# ════════════════════════════════════════════════════════════
def kalshi_auth(method: str, path: str) -> dict:
    ts  = str(int(time.time() * 1000))
    sig = _KALSHI_KEY.sign(
        (ts + method + path).encode(),
        rsa_padding.PSS(mgf=rsa_padding.MGF1(hashes.SHA256()),
                        salt_length=rsa_padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type":            "application/json",
        "Accept":                  "application/json",
    }

def pm_auth(method: str, path: str) -> dict:
    ts  = str(int(time.time() * 1000))
    sig = _PM_KEY.sign((ts + method + path).encode())
    return {
        "Content-Type":    "application/json",
        "X-PM-Access-Key": PM_KEY_ID,
        "X-PM-Signature":  base64.b64encode(sig).decode(),
        "X-PM-Timestamp":  ts,
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
        resync_needed.update(KALSHI_TICKERS.keys())
        last_global_seq = seq
        return False
    return True

async def request_resync(session):
    for ticker in list(resync_needed):
        leg = KALSHI_TICKERS_REVERSE.get(ticker, ticker)
        try:
            url  = f"{KALSHI_REST}/trade-api/v2/markets/{ticker}/orderbook?depth=100"
            async with session.get(url, headers=kalshi_auth("GET", f"/trade-api/v2/markets/{ticker}/orderbook")) as resp:
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
# KALSHI ORDERBOOK
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
# MARKET SPEED / FREEZE (Kalshi ticks only)
# ════════════════════════════════════════════════════════════
def check_market_speed() -> bool:
    global market_frozen, last_big_move_at
    max_move = 0.0
    for leg in KALSHI_TICKERS:
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
            print(f"  🧊 MARKET FROZEN  (Kalshi tick Δ={max_move:.3f} ≥ {FREEZE_TICK_THRESHOLD})")
            log("MARKET_FROZEN", f"tick_move={max_move:.3f}")
            return True
        return False
    if market_frozen and (now - last_big_move_at) >= CALM_DURATION:
        market_frozen = False
        print(f"  ✅ Market calm for {now - last_big_move_at:.1f}s — resuming makers")
        log("MARKET_UNFROZEN", f"calm_for={now - last_big_move_at:.1f}s")
    return False

# ════════════════════════════════════════════════════════════
# KALSHI ORDER HELPERS
# ════════════════════════════════════════════════════════════
async def place_kalshi_maker(session, v2_side: str, price: float) -> str | None:
    price = round(min(0.99, max(0.01, round(price, 2))), 2)
    payload = {
        "ticker":                     KALSHI_TICKERS["FER"],
        "client_order_id":            str(uuid.uuid4()),
        "side":                       v2_side,
        "count":                      f"{TRADE_QUANTITY}.00",
        "price":                      f"{price:.4f}",
        "time_in_force":              "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
    }
    try:
        async with session.post(f"{KALSHI_REST}{KALSHI_ORDER_PATH}", json=payload,
                                headers=kalshi_auth("POST", KALSHI_ORDER_PATH)) as resp:
            res      = await resp.json()
            order_id = res.get("order_id")
            if resp.status in (200, 201) and order_id:
                print(f"  📌 KALSHI MAKER {v2_side.upper()} FER_Y @ ${price:.2f} | id={order_id}")
                log("MAKER_PLACED", f"side={v2_side} price={price} id={order_id}")
                return order_id
            print(f"  ⚠️  Kalshi maker place failed ({v2_side}): {res}")
            log("MAKER_PLACE_FAIL", str(res))
            return None
    except Exception as e:
        print(f"  ❌ Kalshi maker error: {e}")
        return None


async def cancel_kalshi_order(session, order_id: str) -> str:
    cancel_path = f"{KALSHI_ORDER_PATH}/{order_id}"
    try:
        async with session.delete(f"{KALSHI_REST}{cancel_path}",
                                  headers=kalshi_auth("DELETE", cancel_path)) as resp:
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


async def cancel_all_kalshi_makers(session):
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    for oid in filter(None, [bid_order_id, ask_order_id]):
        await cancel_kalshi_order(session, oid)
    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0


async def cancel_all_kalshi_makers_frozen(session):
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
            result = await cancel_kalshi_order(session, oid)
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
                    rqty = max(0.0, TRADE_QUANTITY - already_hedged)
                    log("FILL_INFERRED_404_FROZEN", f"{side} id={oid} age={age:.3f}s qty={rqty:.4f}")
                    await on_maker_filled(session, side, px or 0, oid, quantity=rqty)
                    return
                else:
                    log("CANCEL_404_TOO_FRESH_FROZEN", f"{side} id={oid} age={age:.3f}s — leaving tracked")
    finally:
        freeze_cancel_in_progress = False

# ════════════════════════════════════════════════════════════
# POLYMARKET TAKER
# ════════════════════════════════════════════════════════════
async def place_pm_taker(outcome_side: str, price: float,
                          quantity: float | None = None) -> tuple[bool, dict]:
    qty = TRADE_QUANTITY if quantity is None else quantity
    payload = {
        "marketSlug":  PM_SLUG,
        "type":        "ORDER_TYPE_LIMIT",
        "action":      "ORDER_ACTION_BUY",
        "outcomeSide": outcome_side,
        "quantity":    qty,
        "price":       {"value": f"{price:.4f}", "currency": "USD"},
        "tif":         "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
    }
    try:
        async with _pm_session.post(
            f"{PM_REST}{PM_ORDER_PATH}", json=payload,
            headers=pm_auth("POST", PM_ORDER_PATH)
        ) as resp:
            res    = await resp.json()
            filled = (resp.status == 200 and
                      (bool(res.get("executions")) or
                       res.get("state") == "ORDER_STATE_FILLED"))
            return filled, res
    except Exception as e:
        return False, {"error": str(e)}

# ════════════════════════════════════════════════════════════
# PARTIAL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_partial_fill(kalshi_session, filled_side: str, fill_price: float, quantity: float):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n  ⚡ [{ts}] PARTIAL FILL: {filled_side.upper()} @ ${fill_price:.4f}  qty={quantity:.4f}")
    log("PARTIAL_FILL", f"side={filled_side} price={fill_price} qty={quantity:.4f}")

    if filled_side == "bid":
        ref  = pm_yes_ask or 0.99
        tpx  = round(min(0.99, ref + TAKER_BUFFER), 4)
        side = "OUTCOME_SIDE_YES"
        print(f"  🎯 Partial Hedge: BUY {quantity:.4f}× PM ENG_YES  pm_ask={ref:.4f}  limit=${tpx:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY PM YES qty={quantity:.4f} limit={tpx} pm_ask={ref}")
    else:
        ref  = pm_no_ask or 0.99
        tpx  = round(min(0.99, ref + TAKER_BUFFER), 4)
        side = "OUTCOME_SIDE_NO"
        print(f"  🎯 Partial Hedge: BUY {quantity:.4f}× PM ENG_NO  pm_ask={ref:.4f}  limit=${tpx:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY PM NO qty={quantity:.4f} limit={tpx} pm_ask={ref}")

    filled, res = await place_pm_taker(side, tpx, quantity)
    if filled:
        execs = res.get("executions", [])
        avg   = (sum(float(e.get("price", tpx)) for e in execs) / len(execs)) if execs else tpx
        print(f"  ✅ Partial PM Hedge FILLED  qty={quantity:.4f} @ ~${avg:.4f}")
        log("PARTIAL_HEDGE_FILLED", f"qty={quantity:.4f} avg~{avg:.4f}")
    else:
        print(f"  🚨 PARTIAL PM HEDGE FAILED → {res}")
        log("PARTIAL_HEDGE_MISSED", f"side={filled_side} qty={quantity:.4f}: {res}")

# ════════════════════════════════════════════════════════════
# FULL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_maker_filled(kalshi_session, filled_side: str, fill_price: float, order_id: str,
                          quantity: float | None = None):
    global maker_filled, hedge_executed
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged

    if maker_filled:
        return

    hedge_qty = TRADE_QUANTITY if quantity is None else quantity

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n{'='*70}")
    print(f"  🔥 [{ts}] KALSHI MAKER FILLED: {filled_side.upper()} @ ${fill_price:.4f}  hedge_qty={hedge_qty:.4f}")
    print(f"  id={order_id}")
    log("MAKER_FILLED", f"side={filled_side} fill_price={fill_price} hedge_qty={hedge_qty:.4f} id={order_id}")

    maker_filled   = True
    hedge_executed = False

    other_id = ask_order_id if filled_side == "bid" else bid_order_id
    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0

    if hedge_qty <= 0.001:
        if other_id:
            await cancel_kalshi_order(kalshi_session, other_id)
        print(f"  ✅ All qty already hedged via partial fills.")
        log("HEDGE_SKIPPED", "all qty hedged via partial fills")
        hedge_executed = True
        print(f"  🔒 MAKER MODE LOCKED. Restart to resume.")
        print(f"{'='*70}\n")
        return

    if filled_side == "bid":
        ref  = pm_yes_ask or 0.99
        tpx  = round(min(0.99, ref + TAKER_BUFFER), 4)
        side = "OUTCOME_SIDE_YES"
        print(f"  🎯 Hedge: BUY {hedge_qty:.4f}× PM ENG_YES  pm_ask={ref:.4f}  limit=${tpx:.4f}")
        log("HEDGE_ATTEMPT", f"BUY PM YES qty={hedge_qty:.4f} limit={tpx} pm_ask={ref}")
    else:
        ref  = pm_no_ask or 0.99
        tpx  = round(min(0.99, ref + TAKER_BUFFER), 4)
        side = "OUTCOME_SIDE_NO"
        print(f"  🎯 Hedge: BUY {hedge_qty:.4f}× PM ENG_NO  pm_ask={ref:.4f}  limit=${tpx:.4f}")
        log("HEDGE_ATTEMPT", f"BUY PM NO qty={hedge_qty:.4f} limit={tpx} pm_ask={ref}")

    # Fire PM taker FIRST, then cancel other maker
    filled, res = await place_pm_taker(side, tpx, hedge_qty)

    if other_id:
        print(f"  🗑  Cancelling other Kalshi maker {other_id[:12]}…")
        await cancel_kalshi_order(kalshi_session, other_id)

    if filled:
        execs  = res.get("executions", [])
        avg    = (sum(float(e.get("price", tpx)) for e in execs) / len(execs)) if execs else tpx
        if filled_side == "bid":
            total  = round(fill_price + avg, 4)
            print(f"  ✅ PM Hedge FILLED @ ~${avg:.4f}  total=${total:.4f}")
            print(f"  💰 LOCKED PROFIT: $1.00 − ${total:.4f} = ${round(1-total,4):.4f}")
            log("HEDGE_FILLED", f"avg={avg:.4f} total={total} profit={round(1-total,4)}")
        else:
            no_cost = round(1.0 - avg, 4)
            profit  = round(fill_price - no_cost, 4)
            print(f"  ✅ PM Hedge FILLED @ ~${avg:.4f}  ENG_NO cost=${no_cost:.4f}")
            print(f"  💰 LOCKED PROFIT: ${fill_price:.4f} − ${no_cost:.4f} = ${profit:.4f}")
            log("HEDGE_FILLED", f"avg={avg:.4f} no_cost={no_cost} profit={profit}")
    else:
        if filled_side == "bid":
            print(f"  🚨 PM HEDGE FAILED: BUY ENG_YES @ {tpx} → {res}")
            print(f"  🚨 HOLDING {hedge_qty:.4f}× Kalshi FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY PM YES qty={hedge_qty:.4f} limit={tpx}: {res}")
        else:
            print(f"  🚨 PM HEDGE FAILED: BUY ENG_NO @ {tpx} → {res}")
            print(f"  🚨 SHORT {hedge_qty:.4f}× Kalshi FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY PM NO qty={hedge_qty:.4f} limit={tpx}: {res}")

    hedge_executed = True
    print(f"\n  🔒 MAKER MODE LOCKED. Restart to resume.")
    print(f"{'='*70}\n")

# ════════════════════════════════════════════════════════════
# WS FILL DETECTION
# ════════════════════════════════════════════════════════════
async def handle_user_order_update(kalshi_session, msg: dict):
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
            await on_maker_filled(kalshi_session, filled_side, fill_price, order_id, quantity=0.0)
        return

    fill_price = float(msg.get("yes_price_dollars", "0"))
    if fill_price == 0:
        cost = float(msg.get("maker_fill_cost_dollars", "0"))
        fill_price = (cost / fill_count) if fill_count > 0 else (bid_price or ask_price or 0)

    if not is_fully_filled:
        log("WS_PARTIAL_FILL", f"side={filled_side} fill={fill_count:.4f} rem={remaining:.4f} incr={incremental:.4f}")
        await on_partial_fill(kalshi_session, filled_side, fill_price, incremental)
        if filled_side == "bid": bid_fill_hedged = fill_count
        else:                    ask_fill_hedged = fill_count
    else:
        log("WS_FILL_DETECTED", f"side={filled_side} status={status} price={fill_price} incr={incremental:.4f}")
        await on_maker_filled(kalshi_session, filled_side, fill_price, order_id, quantity=incremental)

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT (uses live PM prices)
# ════════════════════════════════════════════════════════════
async def update_maker_orders(session):
    """
    Maker prices derived from live Polymarket asks:
      bid target = 1 − pm_yes_ask − BID_SPREAD
      ask target = pm_no_ask     + ASK_SPREAD

    Valid when:
      bid + pm_yes_ask ≤ THRESHOLD   (buying both sides costs ≤ threshold)
      ask − pm_no_ask  ≥ 1−THRESHOLD (selling FER YES + buying PM NO pays ≥ threshold)
    """
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    global updating_orders

    if maker_filled or updating_orders or market_frozen or freeze_cancel_in_progress:
        return

    if pm_yes_ask is None or pm_no_ask is None:
        return   # wait for PM WS to deliver prices

    target_bid = round(max(0.01, 1.0 - pm_yes_ask - BID_SPREAD), 2)
    target_ask = round(min(0.99, pm_no_ask + ASK_SPREAD),         2)
    bid_valid  = (target_bid + pm_yes_ask) <= THRESHOLD
    ask_valid  = (target_ask - pm_no_ask)  >= (1.0 - THRESHOLD)

    updating_orders = True
    try:
        # ── BID ───────────────────────────────────────────
        if bid_order_id is not None:
            stale      = bid_price is None or abs(target_bid - bid_price) >= UPDATE_TICK
            too_costly = bid_price is not None and bid_price + pm_yes_ask > THRESHOLD
            if stale or too_costly or not bid_valid:
                oid_to_cancel = bid_order_id
                if maker_filled: return
                result = await cancel_kalshi_order(session, oid_to_cancel)
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
            oid = await place_kalshi_maker(session, "bid", target_bid)
            if maker_filled: return
            if oid:
                bid_order_id  = oid
                bid_price     = target_bid
                bid_placed_at = time.time()
                bid_fill_hedged = 0.0

        # ── ASK ───────────────────────────────────────────
        if ask_order_id is not None:
            stale      = ask_price is None or abs(target_ask - ask_price) >= UPDATE_TICK
            low_margin = ask_price is not None and ask_price - pm_no_ask < (1.0 - THRESHOLD)
            if stale or low_margin or not ask_valid:
                oid_to_cancel = ask_order_id
                if maker_filled: return
                result = await cancel_kalshi_order(session, oid_to_cancel)
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
            oid = await place_kalshi_maker(session, "ask", target_ask)
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
last_printed: dict = {k: {} for k in KALSHI_TICKERS}

def print_live_odds():
    changed = any(
        curr_prices[k].get(f) != last_printed.get(k, {}).get(f)
        for k in KALSHI_TICKERS for f in ["yes_ask", "no_ask"]
    )
    if not changed:
        return
    for k in KALSHI_TICKERS:
        last_printed[k] = {"yes_ask": curr_prices[k].get("yes_ask"),
                            "no_ask":  curr_prices[k].get("no_ask")}

    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S")
    fer_y = curr_prices["FER"].get("yes_ask")
    fer_n = curr_prices["FER"].get("no_ask")
    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait.."
    def fmtp(v): return f"${v:.4f}" if v is not None else " ----"

    status_flag = " 🔒LOCKED" if maker_filled else (" 🧊FROZEN" if market_frozen else "")
    t_bid = round(max(0.01, 1.0 - (pm_yes_ask or 0) - BID_SPREAD), 2) if pm_yes_ask else None
    t_ask = round(min(0.99, (pm_no_ask or 0) + ASK_SPREAD),         2) if pm_no_ask else None
    bid_stat = f"BID@{bid_price:.2f}({bid_order_id[-6:]})" if bid_order_id else f"→{fmt(t_bid)}"
    ask_stat = f"ASK@{ask_price:.2f}({ask_order_id[-6:]})" if ask_order_id else f"→{fmt(t_ask)}"

    sum_y = f"{round(fer_y + cer_y, 2):.2f}" if (fer_y and cer_y) else " N/A"
    sum_n = f"{round(fer_n + cer_n, 2):.2f}" if (fer_n and cer_n) else " N/A"

    print(
        f"{ts}{status_flag}  | "
        f"K/FER_Y: {fmt(fer_y):<6} K/FER_N: {fmt(fer_n):<6} | "
        f"K/CER_Y: {fmt(cer_y):<6} K/CER_N: {fmt(cer_n):<6} | "
        f"SUM_Y: {sum_y}  SUM_N: {sum_n}  | "
        f"PM_YES_ask: {fmtp(pm_yes_ask)}  PM_NO_ask: {fmtp(pm_no_ask)}  | "
        f"MAKERS: {bid_stat} / {ask_stat}"
    )

# ════════════════════════════════════════════════════════════
# KALSHI WS MESSAGE HANDLER
# ════════════════════════════════════════════════════════════
async def process_kalshi_ws_message(session, msg_data: dict):
    global tick_count
    msg_type = msg_data.get("type")
    msg      = msg_data.get("msg", {})

    if msg_type == "subscribed":
        print(f"  ✅ Kalshi subscribed: {msg_data.get('msg',{}).get('channels','?')}")
        return
    if msg_type == "error":
        print(f"  ❌ Kalshi WS error: {msg_data}")
        return
    if msg_type == "user_order":
        await handle_user_order_update(session, msg)
        return

    ticker = msg.get("market_ticker")
    if ticker not in KALSHI_TICKERS_REVERSE:
        return
    leg = KALSHI_TICKERS_REVERSE[ticker]
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
    if newly_frozen and (bid_order_id or ask_order_id):
        print("  🧊 Cancelling all Kalshi makers due to freeze…")
        while updating_orders:
            await asyncio.sleep(0.01)
        await cancel_all_kalshi_makers_frozen(session)

    if not maker_filled and not market_frozen and not updating_orders:
        asyncio.create_task(update_maker_orders(session))

# ════════════════════════════════════════════════════════════
# POLYMARKET WS RUNNER (market_data_lite → pm_yes_ask / pm_no_ask)
# ════════════════════════════════════════════════════════════
async def run_pm_ws():
    """
    Subscribes to Polymarket market_data_lite for PM_SLUG.
    Updates pm_yes_ask and pm_no_ask in real time.

    bestAsk  → pm_yes_ask  (cost to buy ENG YES immediately)
    bestBid  → pm_no_ask = 1 − bestBid  (implied NO ask)
    """
    global pm_yes_ask, pm_no_ask

    from polymarket_us import PolymarketUS

    while True:
        ws_handle = None
        try:
            client    = PolymarketUS(key_id=PM_KEY_ID, secret_key=open(PM_KEY_PATH).read().strip())
            ws_handle = client.ws.markets()

            def _on_lite(d: dict):
                global pm_yes_ask, pm_no_ask
                lite = d.get("marketDataLite", {})
                bid  = lite.get("bestBid")
                ask  = lite.get("bestAsk")
                if isinstance(bid, dict): bid = bid.get("value")
                if isinstance(ask, dict): ask = ask.get("value")
                if ask is not None:
                    pm_yes_ask = round(float(ask), 4)
                if bid is not None:
                    pm_no_ask = round(1.0 - float(bid), 4)
                # Trigger a maker order update whenever PM prices change
                if not maker_filled and not market_frozen and not updating_orders:
                    asyncio.get_event_loop().call_soon_threadsafe(
                        lambda: asyncio.ensure_future(
                            update_maker_orders(_kalshi_session)
                        ) if _kalshi_session else None
                    )

            ws_handle.on("market_data_lite", _on_lite)
            ws_handle.on("error", lambda e: print(f"⚠️  PM WS: {e}"))

            await ws_handle.connect()
            await ws_handle.subscribe("prices", "SUBSCRIPTION_TYPE_MARKET_DATA_LITE", [PM_SLUG])
            print(f"✅ Polymarket WS → '{PM_SLUG}'")

            while True:
                await asyncio.sleep(30)

        except Exception as e:
            print(f"❌ PM WS error: {e}. Reconnecting in 5s…")
            pm_yes_ask = pm_no_ask = None
            try:
                if ws_handle:
                    await ws_handle.close()
            except Exception:
                pass
            await asyncio.sleep(5)

# ════════════════════════════════════════════════════════════
# KALSHI WS RUNNER
# ════════════════════════════════════════════════════════════
async def run_kalshi_ws(session):
    global _kalshi_session
    _kalshi_session = session

    ws_headers = kalshi_auth("GET", "/trade-api/ws/v2")

    while True:
        if maker_filled:
            print("\n🔒 LOCKED. Monitoring only. Ctrl+C to exit.\n")
            while True:
                await asyncio.sleep(60)

        try:
            async with websockets.connect(
                KALSHI_WS_URL, additional_headers=ws_headers, ping_interval=15
            ) as ws:
                print(f"\n✅ Kalshi WS connected: {KALSHI_WS_URL}")
                await cancel_all_kalshi_makers(session)

                last_seq.clear(); resync_needed.clear()
                for k in KALSHI_TICKERS:
                    orderbooks[k]  = {"yes": {}, "no": {}}
                    curr_prices[k] = {"yes_ask": None, "yes_bid": None,
                                      "no_ask":  None, "no_bid":  None}

                await ws.send(json.dumps({
                    "id": 1, "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta", "user_orders"],
                        "market_tickers": list(KALSHI_TICKERS.values()),
                    }
                }))

                async for raw in ws:
                    await process_kalshi_ws_message(session, json.loads(raw))

        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            print(f"\n❌ Kalshi WS error: {e}. Cancelling makers + retrying in 3s…")
            await cancel_all_kalshi_makers(session)
            await asyncio.sleep(3)

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
async def monitor():
    global _kalshi_session, _pm_session

    init_log()
    print("=" * 90)
    print(f"  Kalshi Maker / Polymarket Taker  —  {GAME_DESCRIPTION}")
    print(f"  Kalshi FER : {KALSHI_TICKERS['FER']}")
    print(f"  PM slug    : {PM_SLUG}")
    print(f"  BID = 1 − pm_yes_ask − {BID_SPREAD}   ASK = pm_no_ask + {ASK_SPREAD}")
    print(f"  THRESHOLD={THRESHOLD}  TAKER_BUFFER={TAKER_BUFFER}  FREEZE={FREEZE_TICK_THRESHOLD}")
    print(f"  PM WS feeds live pm_yes_ask / pm_no_ask → makers update on every PM tick")
    print("=" * 90)

    async with aiohttp.ClientSession() as kalshi_session, \
               aiohttp.ClientSession() as pm_session:
        _kalshi_session = kalshi_session
        _pm_session     = pm_session

        try:
            await asyncio.gather(
                run_kalshi_ws(kalshi_session),
                run_pm_ws(),
            )
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\n🔴 Shutting down — cancelling Kalshi makers…")
            log("SHUTDOWN", "manual")
            await cancel_all_kalshi_makers(kalshi_session)
            print("🔴 Done.")

if __name__ == "__main__":
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n❌ Fatal: {e}")