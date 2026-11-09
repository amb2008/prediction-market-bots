"""
Kalshi Maker-Taker Arbitrage: FER vs CER

MAKER ORDERS (resting GTC on FER_Y):
──────────────────────────────────────────────────────────────────
  FER_Y BID  at  (1 − CER_Y − BID_SPREAD)
    Fills when someone sells FER_YES at or below our bid.
    Hedge taker: BUY CER_YES  (covers CER winning)
    Total cost = bid_fill + CER_Y_taken  <  THRESHOLD  → guaranteed profit

  FER_Y ASK  at  (CER_N + ASK_SPREAD)
    Fills when someone buys FER_YES at or above our ask.
    Hedge taker: BUY CER_NO  (covers FER winning, offsetting our short FER_YES)
    Profit = ask_fill − CER_N_taken  >  (1 − THRESHOLD)  → guaranteed profit

PRICE MAP REMINDER:
  yes_ask = 1 − best_NO_bid  (cost to BUY YES as taker)
  yes_bid = best_YES_bid     (what you receive when selling YES)
  CER_Y   = CER yes_ask     (cost to buy CER YES)
  CER_N   = CER no_ask      (cost to buy CER NO)

ORDER LIFECYCLE:
  On every price update:
    - Recalculate target bid/ask based on current CER_Y and CER_N
    - Cancel any order whose price has moved ≥ UPDATE_TICK from target
    - Cancel any order where the taker leg would breach THRESHOLD
    - Re-place at updated prices if valid

SAFETY PROTOCOL:
  The moment any maker fills:
    1. Set maker_filled = True   (blocks ALL new maker placement immediately)
    2. Cancel the other resting maker
    3. Execute exactly ONE hedge taker
    4. Lock permanently — restart the script to resume makers
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

BASE_REST  = "https://external-api.kalshi.com"
WS_URL     = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
ORDER_PATH = "/trade-api/v2/portfolio/events/orders"
CHECK_ORDER_PATH = "/trade-api/v2/portfolio/orders"

TICKERS = {
    "CER": "KXCS2GAME-26JUN241100OGAPOGEE-OG", # Replace with actual
    "FER": "KXCS2GAME-26JUN241100OGAPOGEE-APOGEE", # Replace with actual
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}
GAME_DESCRIPTION = "Ferrer vs Cerundolo (ATP)"

TRADE_QUANTITY  = 1      # contracts per leg
BID_SPREAD      = 0.04   # FER_Y bid  = 1 - CER_Y - BID_SPREAD
ASK_SPREAD      = 0.04   # FER_Y ask  = CER_N + ASK_SPREAD
THRESHOLD       = 0.96   # cancel if taker cost would exceed this
TAKER_BUFFER    = 0.05   # aggressiveness added to hedge taker price (5 cents)
UPDATE_TICK     = 0.01   # min price movement (1 cent) that triggers order replace
POLL_INTERVAL   = 0.05    # seconds between fill-status polls
LOG_FILE        = "maker_taker_log.csv"
# ═══════════════════════════════════════════════════════════

# ── Load private key ────────────────────────────────────────
with open(PRIVATE_KEY_PATH, "rb") as _fh:
    PRIVATE_KEY = serialization.load_pem_private_key(_fh.read(), password=None)

# ── Price + orderbook state ─────────────────────────────────
curr_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None} for k in TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None} for k in TICKERS}
orderbooks  = {k: {"yes": {}, "no": {}} for k in TICKERS}

last_seq: dict[str, int] = {}
resync_needed: set[str]  = set()

# ── Maker-order tracking ────────────────────────────────────
bid_order_id: str | None   = None   # FER_Y resting bid
bid_price:    float | None = None
ask_order_id: str | None   = None   # FER_Y resting ask
ask_price:    float | None = None

# ── Safety flags ─────────────────────────────────────────────
maker_filled:    bool  = False   # True = permanently stop placing makers
updating_orders: bool  = False   # True = order update in flight (reentrancy guard)
hedge_executed:  bool  = False   # True = hedge taker has fired (prevent double hedge)

tick_count = 0
start_time = time.time()

# ── Global session (set in main, used for cleanup on exit) ──
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
    with open(LOG_FILE, "a", newline="") as f:
        csv.writer(f).writerow([ts, event, details])
    print(f"  📝 [{ts}] {event}  {details}")

# ════════════════════════════════════════════════════════════
# SEQUENCE CHECKING
# ════════════════════════════════════════════════════════════
last_global_seq: int = -1

def check_sequence(seq: int, ticker: str, msg_type: str) -> bool:
    global last_global_seq
    if msg_type == "orderbook_snapshot":
        if seq > last_global_seq:
            last_global_seq = seq
        return True
    if msg_type == "orderbook_delta":
        if last_global_seq == -1:
            last_global_seq = seq; return True
        if seq <= last_global_seq:
            return True
        if seq == last_global_seq + 1:
            last_global_seq = seq; return True
        gap = seq - last_global_seq - 1
        print(f"  🔴 SEQ GAP: {gap} msgs missed — queuing resync")
        resync_needed.update(TICKERS.keys())
        last_global_seq = seq
        return False
    return True

async def request_resync(session):
    for ticker in list(resync_needed):
        leg = TICKERS_REVERSE.get(ticker, ticker)
        try:
            url  = f"{BASE_REST}/trade-api/v2/markets/{ticker}/orderbook?depth=100"
            hdrs = auth("GET", f"/trade-api/v2/markets/{ticker}/orderbook")
            async with session.get(url, headers=hdrs) as resp:
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
    side  = msg.get("side")
    price = msg.get("price_dollars")
    delta = msg.get("delta_fp")
    if side not in ("yes", "no") or price is None or delta is None:
        return
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
# REST ORDER HELPERS
# ════════════════════════════════════════════════════════════
async def place_maker_order(session, team: str, v2_side: str, price: float) -> str | None:
    """
    Place a GTC maker order. Returns order_id or None.
    v2_side: "bid" (buy YES as maker) or "ask" (sell YES as maker)
    Price rounded to 1 cent (standard Kalshi maker granularity).
    """
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
        async with session.post(
            f"{BASE_REST}{ORDER_PATH}", json=payload, headers=auth("POST", ORDER_PATH)
        ) as resp:
            res      = await resp.json()
            order_id = res.get("order_id")
            if resp.status in (200, 201) and order_id:
                print(f"  📌 MAKER {v2_side.upper()} on {team}_Y @ ${price:.2f} | id={order_id}")
                log("MAKER_PLACED", f"side={v2_side} team={team} price={price} id={order_id}")
                return order_id
            else:
                print(f"  ⚠️  Maker place failed ({v2_side}): {res}")
                log("MAKER_PLACE_FAIL", str(res))
                return None
    except Exception as e:
        print(f"  ❌ Maker order error: {e}")
        return None


async def cancel_order(session, order_id: str) -> bool:
    """Cancel a GTC order. Returns True if gone (cancelled or already closed)."""
    cancel_path = f"{ORDER_PATH}/{order_id}"
    try:
        async with session.delete(
            f"{BASE_REST}{cancel_path}", headers=auth("DELETE", cancel_path)
        ) as resp:
            body = await resp.text()
            ok = resp.status in (200, 204, 404, 422)
            if ok:
                print(f"  🗑  Cancelled {order_id[:12]}…, {resp.status}, {await resp.text()}")
                log("ORDER_CANCELLED", order_id)
            else:
                print(f"  ⚠️  Cancel {order_id[:12]}… status={resp.status}: {body[:80]}")
            return ok
    except Exception as e:
        print(f"  ❌ Cancel error: {e}")
        return False


async def get_order_status(session, order_id: str) -> dict | None:
    """Fetch order details. Returns dict or None on failure."""
    path = f"{CHECK_ORDER_PATH}/{order_id}"
    try:
        async with session.get(
            f"{BASE_REST}{path}", headers=auth("GET", path)
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                reason = await resp.text()
                print(f"  ⚠️  Get order {order_id[:12]} failed: status={resp.status}, response: {reason[:80]}")
    except Exception as e:
        print(f"  ❌ Get order error: {e}")
        pass
    return None


async def place_taker_order(session, team: str, v2_side: str, price: float) -> tuple[bool, dict]:
    """
    Place an aggressive IOC taker order (hedge).
    v2_side:
      "bid" → buy YES  (e.g. buy CER_YES after a FER_Y bid fill)
      "ask" → sell YES (= buy NO, e.g. buy CER_NO after a FER_Y ask fill)
    Returns (filled, response_dict).
    """
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
        async with session.post(
            f"{BASE_REST}{ORDER_PATH}", json=payload, headers=auth("POST", ORDER_PATH)
        ) as resp:
            res    = await resp.json()
            filled = resp.status == 201 and float(res.get("fill_count", "0")) > 0
            return filled, res
    except Exception as e:
        return False, {"error": str(e)}


async def cancel_all_makers(session):
    """Cancel any resting maker orders. Called on reconnect and shutdown."""
    global bid_order_id, bid_price, ask_order_id, ask_price
    for oid in filter(None, [bid_order_id, ask_order_id]):
        await cancel_order(session, oid)
    bid_order_id = bid_price = ask_order_id = ask_price = None

# ════════════════════════════════════════════════════════════
# FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_maker_filled(session, filled_side: str, fill_price: float, order_id: str):
    """
    Called when a maker order fills. This is the ONE place that:
      1. Locks maker_filled forever
      2. Cancels the other open maker
      3. Fires exactly one hedge taker
    """
    global maker_filled, hedge_executed
    global bid_order_id, bid_price, ask_order_id, ask_price

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"\n{'='*70}")
    print(f"  🔥 [{ts}] MAKER FILLED: {filled_side.upper()} FER_Y @ ${fill_price:.4f}")
    print(f"  Order ID: {order_id}")
    log("MAKER_FILLED", f"side={filled_side} fill_price={fill_price} id={order_id}")

    # ── 1. SAFETY LOCK ──────────────────────────────────────
    maker_filled   = True   # blocks all future maker placement
    hedge_executed = False  # will be set True after hedge fires

    # ── 2. CANCEL THE OTHER MAKER ───────────────────────────
    other_id = ask_order_id if filled_side == "bid" else bid_order_id
    if other_id:
        print(f"  🗑  Cancelling other maker {other_id[:12]}…")
        await cancel_order(session, other_id)
    bid_order_id = bid_price = ask_order_id = ask_price = None

    # ── 3. EXECUTE HEDGE TAKER ──────────────────────────────
    if filled_side == "bid":
        # ── We BOUGHT FER_YES at fill_price ─────────────────
        # Exposure: receive $1 if FER wins
        # Hedge:    BUY CER_YES → receive $1 if CER wins → covered
        cer_y     = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px  = round(min(0.99, cer_y + TAKER_BUFFER), 4)

        print(f"  🎯 Hedge: BUY CER_YES  market ask={cer_y:.4f}  limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY CER_YES limit={taker_px} mkt_ask={cer_y}")

        filled, res = await place_taker_order(session, "CER", "bid", taker_px)
        if filled:
            cer_avg   = float(res.get("average_fill_price", taker_px))
            total     = round(fill_price + cer_avg, 4)
            profit    = round(1.0 - total, 4)
            print(f"  ✅ Hedge FILLED: CER_YES avg=${cer_avg:.4f}")
            print(f"  💰 LOCKED PROFIT: $1.00 − ${total:.4f} = ${profit:.4f}/contract")
            log("HEDGE_FILLED", f"cer_yes_avg={cer_avg} total={total} profit={profit}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY CER_YES @ {taker_px}  →  {res}")
            print(f"  🚨 HOLDING FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_YES limit={taker_px}: {res}")

    else:   # filled_side == "ask"
        # ── We SOLD FER_YES at fill_price ───────────────────
        # Exposure: owe $1 if FER wins
        # Hedge:    BUY CER_NO → receive $1 if FER wins → offset
        # To BUY CER_NO as IOC taker on Kalshi V2:
        #   place side="ask" (sell CER_YES) at a LOW limit → fills against YES bids
        #   fill price on YES sale ≈ 1 − CER_N  (= best YES bid)
        #   effective CER_NO cost = 1 − fill_yes_price ≈ CER_N  ✓
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        # ask limit = 1 − CER_N − TAKER_BUFFER  (below best YES bid → fills immediately)
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)

        print(f"  🎯 Hedge: BUY CER_NO  market ask={cer_n:.4f}  limit via ask=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"BUY CER_NO (ask@{taker_px}) mkt_no_ask={cer_n}")

        filled, res = await place_taker_order(session, "CER", "ask", taker_px)
        if filled:
            yes_sold_avg  = float(res.get("average_fill_price", taker_px))
            cer_no_cost   = round(1.0 - yes_sold_avg, 4)
            profit        = round(fill_price - cer_no_cost, 4)
            print(f"  ✅ Hedge FILLED: CER_NO cost=${cer_no_cost:.4f}  (YES sold @ {yes_sold_avg:.4f})")
            print(f"  💰 LOCKED PROFIT: ${fill_price:.4f} − ${cer_no_cost:.4f} = ${profit:.4f}/contract")
            log("HEDGE_FILLED", f"cer_no_cost={cer_no_cost} fer_yes_sold={fill_price} profit={profit}")
        else:
            print(f"  🚨 HEDGE FAILED: BUY CER_NO ask @ {taker_px}  →  {res}")
            print(f"  🚨 SHORT FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_NO ask limit={taker_px}: {res}")

    hedge_executed = True
    print(f"\n  🔒 MAKER MODE LOCKED. Restart script to resume.")
    print(f"{'='*70}\n")

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT — called after each price update
# ════════════════════════════════════════════════════════════
async def update_maker_orders(session):
    """
    Maintain resting bid and ask on FER_Y.

    BID: target = 1 − CER_Y − BID_SPREAD
         valid when target + CER_Y ≤ THRESHOLD  (i.e. hedge still fits under cap)
         cancel if: existing bid_price + CER_Y > THRESHOLD  OR price moved ≥ UPDATE_TICK

    ASK: target = CER_N + ASK_SPREAD
         valid when target − CER_N ≥ (1 − THRESHOLD)  (enough profit margin)
         cancel if: existing ask_price − CER_N < (1 − THRESHOLD)  OR price moved ≥ UPDATE_TICK
    """
    global bid_order_id, bid_price, ask_order_id, ask_price, updating_orders

    if maker_filled or updating_orders:
        return

    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")
    if cer_y is None or cer_n is None:
        return

    # ── Target prices (rounded to nearest cent) ───────────
    target_bid = round(max(0.01, 1.0 - cer_y - BID_SPREAD), 2)
    target_ask = round(min(0.99, cer_n + ASK_SPREAD),        2)

    # ── Validity: would the hedge still clear THRESHOLD? ──
    # BID valid: target_bid + cer_y ≤ THRESHOLD
    # ASK valid: target_ask − cer_n ≥ (1 − THRESHOLD)
    bid_valid = (target_bid + cer_y) <= THRESHOLD
    ask_valid = (target_ask - cer_n) >= (1.0 - THRESHOLD)

    updating_orders = True
    try:
        # ── Manage resting BID ────────────────────────────
        if bid_order_id is not None:
            stale     = (bid_price is None or abs(target_bid - bid_price) >= UPDATE_TICK)
            too_costly = (bid_price is not None and bid_price + cer_y > THRESHOLD)
            if stale or too_costly or not bid_valid:
                await cancel_order(session, bid_order_id)
                bid_order_id = bid_price = None

        if bid_order_id is None and bid_valid and not maker_filled:
            oid = await place_maker_order(session, "FER", "bid", target_bid)
            if oid:
                bid_order_id = oid
                bid_price    = target_bid

        # ── Manage resting ASK ────────────────────────────
        if ask_order_id is not None:
            stale       = (ask_price is None or abs(target_ask - ask_price) >= UPDATE_TICK)
            low_margin  = (ask_price is not None and ask_price - cer_n < (1.0 - THRESHOLD))
            if stale or low_margin or not ask_valid:
                await cancel_order(session, ask_order_id)
                ask_order_id = ask_price = None

        if ask_order_id is None and ask_valid and not maker_filled:
            oid = await place_maker_order(session, "FER", "ask", target_ask)
            if oid:
                ask_order_id = oid
                ask_price    = target_ask

    finally:
        updating_orders = False

# ════════════════════════════════════════════════════════════
# FILL POLLING — background task
# ════════════════════════════════════════════════════════════
async def poll_fills(session):
    """
    Checks order status every POLL_INTERVAL seconds.
    The moment a fill is detected, triggers on_maker_filled and exits.
    """
    global bid_order_id, bid_price, ask_order_id, ask_price

    while not maker_filled:
        await asyncio.sleep(POLL_INTERVAL)

        if maker_filled:
            break

        checks = [
            ("bid", bid_order_id, bid_price),
            ("ask", ask_order_id, ask_price),
        ]

        for side, oid, px in checks:
            if oid is None or maker_filled:
                continue

            status = await get_order_status(session, oid)
            if status is None:
                continue

            fill_count = float(status.get("fill_count", "0"))
            if fill_count > 0:
                avg_fill = float(status.get("average_fill_price") or px or 0)
                # Fire the fill handler — this also sets maker_filled = True
                await on_maker_filled(session, side, avg_fill, oid)
                bid_order_id = bid_price = ask_order_id = ask_price = None
                return   # exit poll loop; maker_filled is now True

    print("  🔒 Poll task exiting (maker_filled=True)")

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
        last_printed[k] = {
            "yes_ask": curr_prices[k].get("yes_ask"),
            "no_ask":  curr_prices[k].get("no_ask"),
        }

    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S")
    fer_y = curr_prices["FER"].get("yes_ask")
    fer_n = curr_prices["FER"].get("no_ask")
    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")

    # Target maker prices
    t_bid = round(max(0.01, 1.0 - (cer_y or 0) - BID_SPREAD), 2) if cer_y else None
    t_ask = round(min(0.99, (cer_n or 0) + ASK_SPREAD),        2) if cer_n else None

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait.."

    lock_flag = " 🔒LOCKED" if maker_filled else ""
    bid_stat  = f"BID@{bid_price:.2f}" if bid_order_id else f"→{fmt(t_bid)}"
    ask_stat  = f"ASK@{ask_price:.2f}" if ask_order_id else f"→{fmt(t_ask)}"

    sum_y = f"{round(fer_y + cer_y, 2):.2f}" if (fer_y and cer_y) else " N/A"
    sum_n = f"{round(fer_n + cer_n, 2):.2f}" if (fer_n and cer_n) else " N/A"

    print(
        f"{ts}{lock_flag}  | "
        f"FER_Y: {fmt(fer_y):<6} FER_N: {fmt(fer_n):<6} | "
        f"CER_Y: {fmt(cer_y):<6} CER_N: {fmt(cer_n):<6} | "
        f"SUM_Y: {sum_y}  SUM_N: {sum_n}  | "
        f"MAKERS: {bid_stat} / {ask_stat}"
    )

# ════════════════════════════════════════════════════════════
# WS MESSAGE HANDLER
# ════════════════════════════════════════════════════════════
async def process_ws_message(session, msg_data: dict):
    global tick_count
    msg_type = msg_data.get("type")
    msg      = msg_data.get("msg", {})
    ticker   = msg.get("market_ticker")
    seq      = msg_data.get("seq")

    if msg_type == "subscribed":
        print(f"  ✅ Subscribed: {msg_data}")
        return
    if msg_type == "error":
        print(f"  ❌ WS error: {msg_data}")
        return
    if ticker not in TICKERS_REVERSE:
        return

    leg = TICKERS_REVERSE[ticker]

    if seq is not None:
        valid = check_sequence(seq, ticker, msg_type)
        if not valid:
            if resync_needed:
                await request_resync(session)
            return

    if msg_type == "orderbook_snapshot":
        apply_snapshot(leg, msg)
        update_derived(leg)
        tick_count += 1
    elif msg_type == "orderbook_delta":
        apply_delta(leg, msg)
        update_derived(leg)
        tick_count += 1
    else:
        return

    print_live_odds()
    if not maker_filled:
        await update_maker_orders(session)

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
async def monitor():
    global _session

    init_log()
    print("=" * 90)
    print(f"  🎾 Maker-Taker Arb  —  {GAME_DESCRIPTION}")
    print(f"  FER_Y BID = 1 − CER_Y − {BID_SPREAD}   (hedge: take CER_Y)")
    print(f"  FER_Y ASK = CER_N + {ASK_SPREAD}         (hedge: take CER_NO)")
    print(f"  THRESHOLD = {THRESHOLD}  │  UPDATE_TICK = {UPDATE_TICK}  │  POLL = {POLL_INTERVAL}s")
    print(f"  SAFETY: first maker fill → cancel all others → one hedge → permanent lock")
    print("=" * 90)

    ws_headers = auth("GET", "/trade-api/ws/v2")

    async with aiohttp.ClientSession() as session:
        _session = session

        # Start fill-polling as a background task
        poll_task = asyncio.create_task(poll_fills(session))

        try:
            while True:
                # If locked out, just keep WS open for monitoring only
                if maker_filled:
                    if not poll_task.done():
                        await poll_task   # wait for hedge to complete
                    print("\n🔒 MAKER MODE LOCKED. Monitoring only. Press Ctrl+C to exit.\n")
                    while True:
                        await asyncio.sleep(60)

                try:
                    async with websockets.connect(
                        WS_URL, additional_headers=ws_headers, ping_interval=15
                    ) as ws:
                        print(f"\n✅ Connected: {WS_URL}")

                        # Cancel any stale makers from a previous connection
                        await cancel_all_makers(session)

                        # Reset books
                        last_seq.clear()
                        resync_needed.clear()
                        for k in TICKERS:
                            orderbooks[k]  = {"yes": {}, "no": {}}
                            curr_prices[k] = {"yes_ask": None, "yes_bid": None,
                                              "no_ask":  None, "no_bid":  None}

                        await ws.send(json.dumps({
                            "id": 1, "cmd": "subscribe",
                            "params": {
                                "channels":       ["orderbook_delta"],
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
            print("\n🔴 Shutting down — cancelling resting makers…")
            log("SHUTDOWN", "manual")
            await cancel_all_makers(session)
            if not poll_task.done():
                poll_task.cancel()
            print("🔴 Done.")

if __name__ == "__main__":
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"\n❌ Fatal: {e}")