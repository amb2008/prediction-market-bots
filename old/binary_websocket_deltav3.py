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

# === CONFIGURATION START ===
KALSHI_KEY_ID    = 'DUMMY_KEY_ID'
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_REST = "https://external-api.kalshi.com"
WS_URL    = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

# ✅ CHANGE 1: new V2 path (legacy /portfolio/orders is being deprecated)
PATH_V2_ORDER = "/trade-api/v2/portfolio/events/orders"

TICKERS = {
    "FER": "KXATPMATCH-26JUN24FERCER-FER",
    "CER": "KXATPMATCH-26JUN24FERCER-CER",
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE            = "sem_lee_prices.csv"
GAME_DESCRIPTION    = "NYY vs DET"
ARBITRAGE_THRESHOLD = 0.94
TRADE_QUANTITY      = 1
# === CONFIGURATION END ===

# ============================================================
# GLOBAL STATE7
# ============================================================
curr_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None,
                    "status": "active"} for k in TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None,
                    "status": "active"} for k in TICKERS}
orderbooks  = {k: {"yes": {}, "no": {}} for k in TICKERS}

last_seq: dict[str, int] = {}
resync_needed: set[str]  = set()

is_trading = False
executed   = False
tick_count = 0
start_time = time.time()

with open(PRIVATE_KEY_PATH, "rb") as f:
    PRIVATE_KEY = serialization.load_pem_private_key(f.read(), password=None)

# ============================================================
# AUTH
# ============================================================
def generate_auth_headers(method: str, path: str):
    timestamp_ms = str(int(time.time() * 1000))
    payload      = timestamp_ms + method + path
    signature    = PRIVATE_KEY.sign(
        payload.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256()
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type":            "application/json",
        "Accept":                  "application/json"
    }

# ============================================================
# LOGGING
# ============================================================
def init_log():
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            header = ["timestamp_utc", "elapsed_s"]
            for key in TICKERS.keys():
                header.extend([f"{key}_yes_ask", f"{key}_yes_bid",
                                f"{key}_no_ask",  f"{key}_no_bid"])
            header.append("signal")
            writer.writerow(header)

def log_row(ts_utc, elapsed, signal=""):
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        row_data = [ts_utc, f"{elapsed:.2f}"]
        for key in TICKERS.keys():
            row_data.extend([
                curr_prices[key]["yes_ask"], curr_prices[key]["yes_bid"],
                curr_prices[key]["no_ask"],  curr_prices[key]["no_bid"]
            ])
        row_data.append(signal)
        writer.writerow(row_data)

# ============================================================
# SEQUENCE CHECKING
# ============================================================
last_global_seq: int = -1

def check_sequence(seq: int, ticker: str, msg_type: str) -> bool:
    global last_global_seq

    if msg_type == "orderbook_snapshot":
        if seq > last_global_seq:
            last_global_seq = seq
        return True

    if msg_type == "orderbook_delta":
        if last_global_seq == -1:
            last_global_seq = seq
            return True
        if seq <= last_global_seq:
            return True
        if seq == last_global_seq + 1:
            last_global_seq = seq
            return True

        gap = seq - last_global_seq - 1
        print(f"  🔴 GLOBAL SEQ GAP: expected {last_global_seq+1}, got {seq} "
              f"(missed {gap}) — queuing REST resync for ALL markets")
        resync_needed.update(TICKERS.keys())
        last_global_seq = seq
        return False

    return True

async def request_resync_via_rest(session):
    if not resync_needed:
        return

    for ticker in list(resync_needed):
        leg_key = TICKERS_REVERSE.get(ticker, ticker)
        print(f"  🔄 REST resync for [{leg_key}]...")
        try:
            url     = f"{BASE_REST}/trade-api/v2/markets/{ticker}/orderbook?depth=100"
            headers = generate_auth_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook")
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
                book = data.get("orderbook_fp") or data.get("orderbook", {})
                if book:
                    apply_snapshot(leg_key, {
                        "yes_dollars_fp": book.get("yes_dollars", []),
                        "no_dollars_fp":  book.get("no_dollars",  []),
                    })
                    update_derived_prices(leg_key)
                    resync_needed.discard(ticker)
                    last_seq.pop(ticker, None)
                    print(f"  ✅ [{leg_key}] REST resync complete")
                else:
                    print(f"  ⚠️  [{leg_key}] REST resync returned empty book: {data}")
        except Exception as e:
            print(f"  ❌ [{leg_key}] REST resync failed: {e}")

# ============================================================
# ORDERBOOK STATE
# ============================================================
def apply_snapshot(leg_key, msg):
    orderbooks[leg_key]["yes"] = {
        int(round(float(lvl[0]) * 100)): float(lvl[1])
        for lvl in msg.get("yes_dollars_fp", [])
    }
    orderbooks[leg_key]["no"] = {
        int(round(float(lvl[0]) * 100)): float(lvl[1])
        for lvl in msg.get("no_dollars_fp", [])
    }

def apply_delta(leg_key, msg):
    side      = msg.get("side")
    price_str = msg.get("price_dollars")
    delta_str = msg.get("delta_fp")
    if side not in ("yes", "no") or price_str is None or delta_str is None:
        return
    price_cents = int(round(float(price_str) * 100))
    delta_qty   = float(delta_str)
    book        = orderbooks[leg_key][side]
    new_qty     = book.get(price_cents, 0.0) + delta_qty
    if new_qty <= 0.0001:
        book.pop(price_cents, None)
    else:
        book[price_cents] = new_qty

def best_bid(leg_key, side):
    book = orderbooks[leg_key][side]
    return max(book.keys()) if book else None

def update_derived_prices(leg_key):
    prev_prices[leg_key] = curr_prices[leg_key].copy()
    best_yes_cents = best_bid(leg_key, "yes")
    best_no_cents  = best_bid(leg_key, "no")

    if best_yes_cents is not None:
        d = best_yes_cents / 100.0
        curr_prices[leg_key]["yes_bid"] = round(d, 4)
        curr_prices[leg_key]["no_ask"]  = round(1.0 - d, 4)
    else:
        curr_prices[leg_key]["yes_bid"] = None
        curr_prices[leg_key]["no_ask"]  = None

    if best_no_cents is not None:
        d = best_no_cents / 100.0
        curr_prices[leg_key]["no_bid"]  = round(d, 4)
        curr_prices[leg_key]["yes_ask"] = round(1.0 - d, 4)
    else:
        curr_prices[leg_key]["no_bid"]  = None
        curr_prices[leg_key]["yes_ask"] = None

# ============================================================
# LEG ORDERING
# ============================================================
def get_leg_order_by_stability(side):
    changes = {}
    for key in TICKERS.keys():
        c = curr_prices[key].get(f"{side}_ask")
        p = prev_prices[key].get(f"{side}_ask")
        changes[key] = abs(c - p) if (c is not None and p is not None) else 0
    ordered   = sorted(TICKERS.keys(), key=lambda k: changes[k])
    order_str = " → ".join([f"{k}(Δ{changes[k]:.3f})" for k in ordered])
    print(f"  Leg order (stale→volatile): {order_str}")
    return ordered

# ============================================================
# ORDER EXECUTION  (V2 API)
# ============================================================
# ✅ CHANGE 2: V2 uses a unified YES-side price field and "bid"/"ask" sides.
#
#   Legacy shape:
#     { "action": "buy", "side": "yes", "type": "limit", "yes_price": 55,
#       "count": 1 (int) }
#
#   V2 shape:
#     { "side": "bid",   "price": "0.5500" (dollar string),
#       "count": "1.00"  (fp-string) }
#
#   Side mapping:
#     buy YES  →  side="bid",  price = yes_ask + buffer  (capped at 0.99)
#     buy NO   →  side="ask",  price = 1 − (no_ask + buffer)
#                 (selling YES at a lower price = buying NO more aggressively)
#
# ✅ CHANGE 3: V2 response shape (no nested "order" object):
#   Success  = HTTP 201  AND  fill_count > 0   (string, e.g. "1.00")
#   Order ID = res["order_id"]                 (not res["order"]["order_id"])
#   No "status" or "cancel_reason" fields.
# ============================================================
async def place_arb_orders(session, side):
    global is_trading
    global executed
    is_trading = True

    url  = f"{BASE_REST}{PATH_V2_ORDER}"
    legs = get_leg_order_by_stability(side)

    def make_payload(leg_key: str, ask_price: float, cents_buffer: int) -> dict:
        """Return a V2-compliant order payload."""
        if side == "yes":
            # Buy YES: bid at (yes_ask + buffer), capped at $0.99
            v2_side = "bid"
            price   = round(min(0.99, ask_price + cents_buffer / 100.0), 4)
        else:
            # Buy NO = Sell YES at (1 − no_price_with_buffer), floored at $0.01
            # Accepting a lower YES price = more aggressive NO buyer
            v2_side = "ask"
            price   = round(max(0.01, 1.0 - (ask_price + cents_buffer / 100.0)), 4)

        return {
            "ticker":                     TICKERS[leg_key],
            "client_order_id":            str(uuid.uuid4()),
            "side":                       v2_side,                  # "bid" or "ask"
            "count":                      f"{TRADE_QUANTITY}.00",   # fp string, not int
            "price":                      f"{price:.4f}",           # dollar string, not cents
            "time_in_force":              "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        }

    def is_filled(http_status: int, res: dict) -> bool:
        """V2: 201 + fill_count > 0 means the order executed."""
        return http_status == 201 and float(res.get("fill_count", "0")) > 0

    # ── Leg 1: Anchor ───────────────────────────────────────
    leg1       = legs[0]
    ask_price1 = curr_prices[leg1][f"{side}_ask"]
    payload1   = make_payload(leg1, ask_price1, cents_buffer=4)
    headers1   = generate_auth_headers("POST", PATH_V2_ORDER)

    try:
        async with session.post(url, json=payload1, headers=headers1) as resp:
            res    = await resp.json()
            filled = is_filled(resp.status, res)

            if not filled:
                print(f"    ⚠️ Leg 1 ({leg1}) missed. Safe abort. {res}")

                # Optimistic local deletion: remove the stale resting liquidity
                # we just failed to take, so we don't loop on the same signal.
                price_cents = int(round(float(payload1["price"]) * 100))
                if side == "yes":
                    # bid: we targeted NO bids at the complement price
                    orderbooks[leg1]["no"].pop(100 - price_cents, None)
                else:
                    # ask: we targeted YES bids; remove the best YES bid level
                    best_yes_c = best_bid(leg1, "yes")
                    if best_yes_c is not None:
                        orderbooks[leg1]["yes"].pop(best_yes_c, None)
                update_derived_prices(leg1)

                is_trading = False
                return False

            executed = True
            print(
                f"    ✅ Leg 1 ({leg1}) FILLED {res.get('fill_count')} contracts "
                f"@ avg ${res.get('average_fill_price', '?')} "
                f"| id={res.get('order_id')}"
            )
    except Exception as e:
        print(f"  ERROR Leg 1: {e}")
        is_trading = False
        return False

    # ── Leg 2: Aggressive Sweep ──────────────────────────────
    leg2       = legs[1]
    ask_price2 = curr_prices[leg2][f"{side}_ask"]
    payload2   = make_payload(leg2, ask_price2, cents_buffer=10)
    headers2   = generate_auth_headers("POST", PATH_V2_ORDER)

    try:
        async with session.post(url, json=payload2, headers=headers2) as resp:
            res    = await resp.json()
            filled = is_filled(resp.status, res)
            if filled:
                print(
                    f"    ✅ Leg 2 ({leg2}) FILLED {res.get('fill_count')} contracts "
                    f"@ avg ${res.get('average_fill_price', '?')} "
                    f"| id={res.get('order_id')}"
                )
                print("  🎉 Arbitrage fully executed!")
            else:
                print(f"    🚨 TOXIC LEG: Leg 2 ({leg2}) missed. {res}")
                print(f"    🚨 YOU HOLD {leg1} {side.upper()} — manual close may be needed")
    except Exception as e:
        print(f"  🚨 CRITICAL Leg 2 error: {e}")
        print(f"  🚨 YOU HOLD {leg1} {side.upper()} — check portfolio")
        filled = False

    await asyncio.sleep(0.5)
    is_trading = False
    return filled

# ============================================================
# ARB CHECK
# ============================================================
async def check_arbitrage(session):
    global is_trading
    global executed
    if is_trading:
        return
    if resync_needed:
        return
    if executed:
        return

    is_trading = True

    p1, p2  = list(TICKERS.keys())[0], list(TICKERS.keys())[1]
    now     = datetime.now(timezone.utc)
    ts_utc  = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elapsed = time.time() - start_time

    try:
        y1 = curr_prices[p1].get("yes_ask")
        y2 = curr_prices[p2].get("yes_ask")
        if y1 is not None and y2 is not None:
            sum_yes = y1 + y2
            if sum_yes < ARBITRAGE_THRESHOLD:
                signal = f"ARBITRAGE_YES_ASK: {sum_yes:.4f}"
                print(f"\n  🚨 [{ts_utc}] {signal}")
                print(f"  {p1} YES {y1:.2f} + {p2} YES {y2:.2f} = {sum_yes:.4f}")
                log_row(ts_utc, elapsed, signal)
                is_trading = False
                success = await place_arb_orders(session, "yes")
                if not success:
                    print("  Arb aborted")
                return

        n1 = curr_prices[p1].get("no_ask")
        n2 = curr_prices[p2].get("no_ask")
        if n1 is not None and n2 is not None:
            sum_no = n1 + n2
            if sum_no < ARBITRAGE_THRESHOLD:
                signal = f"ARBITRAGE_NO_ASK: {sum_no:.4f}"
                print(f"\n  🚨 [{ts_utc}] {signal}")
                print(f"  {p1} NO {n1:.2f} + {p2} NO {n2:.2f} = {sum_no:.4f}")
                log_row(ts_utc, elapsed, signal)
                is_trading = False
                success = await place_arb_orders(session, "no")
                if not success:
                    print("  Arb aborted")
                return
    finally:
        is_trading = False

# ============================================================
# LIVE PRINTOUT
# ============================================================
last_printed = {k: {"yes_ask": None, "no_ask": None} for k in TICKERS}

def print_live_odds():
    p1, p2 = list(TICKERS.keys())[0], list(TICKERS.keys())[1]

    changed = any(
        curr_prices[k].get(f) != last_printed[k].get(f)
        for k in TICKERS for f in ["yes_ask", "no_ask"]
    )
    if not changed:
        return
    for k in TICKERS:
        last_printed[k]["yes_ask"] = curr_prices[k].get("yes_ask")
        last_printed[k]["no_ask"]  = curr_prices[k].get("no_ask")

    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    p1_y = curr_prices[p1].get("yes_ask")
    p1_n = curr_prices[p1].get("no_ask")
    p2_y = curr_prices[p2].get("yes_ask")
    p2_n = curr_prices[p2].get("no_ask")

    resync_flag = " 🔄RESYNCING" if resync_needed else ""
    sum_y = f"{p1_y + p2_y:.2f}" if (p1_y and p2_y) else "N/A"
    sum_n = f"{p1_n + p2_n:.2f}" if (p1_n and p2_n) else "N/A"

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait..."

    print(f"{ts}{resync_flag}  | "
          f"{p1}_Y: {fmt(p1_y):<6} {p1}_N: {fmt(p1_n):<6} | "
          f"{p2}_Y: {fmt(p2_y):<6} {p2}_N: {fmt(p2_n):<6} | "
          f"SUM_Y: {sum_y:<5} SUM_N: {sum_n:<5}")

# ============================================================
# WS MESSAGE HANDLING
# ============================================================
async def process_ws_message(session, ws, msg_data):
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

    leg_key = TICKERS_REVERSE[ticker]

    if seq is not None:
        valid = check_sequence(seq, ticker, msg_type)
        if not valid:
            if ticker in resync_needed:
                await request_resync_via_rest(session)
            return

    if msg_type == "orderbook_snapshot":
        if tick_count < 2:
            print(f"\n🔍 RAW SNAPSHOT ({leg_key}) seq={seq}: {msg}\n")
        apply_snapshot(leg_key, msg)
        update_derived_prices(leg_key)
        tick_count += 1
        await check_arbitrage(session)
        print_live_odds()

    elif msg_type == "orderbook_delta":
        apply_delta(leg_key, msg)
        update_derived_prices(leg_key)
        tick_count += 1
        await check_arbitrage(session)
        print_live_odds()

# ============================================================
# MAIN
# ============================================================
async def monitor_arbitrage():
    init_log()

    print("=" * 100)
    print(f"  🚀 Kalshi Arb Monitor  (V2 orders: {PATH_V2_ORDER})")
    print(f"  {GAME_DESCRIPTION} | Threshold: < {ARBITRAGE_THRESHOLD:.2f}")
    print("=" * 100)

    ws_headers = generate_auth_headers("GET", "/trade-api/ws/v2")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(
                    WS_URL, additional_headers=ws_headers, ping_interval=15
                ) as ws:
                    print(f"✅ Connected: {WS_URL}\n")

                    last_seq.clear()
                    resync_needed.clear()
                    for k in TICKERS:
                        orderbooks[k]  = {"yes": {}, "no": {}}
                        curr_prices[k] = {"yes_ask": None, "yes_bid": None,
                                          "no_ask":  None, "no_bid":  None,
                                          "status": "active"}

                    await ws.send(json.dumps({
                        "id": 1, "cmd": "subscribe",
                        "params": {
                            "channels":       ["orderbook_delta"],
                            "market_tickers": list(TICKERS.values())
                        }
                    }))

                    async for msg in ws:
                        await process_ws_message(session, ws, json.loads(msg))

            except Exception as e:
                print(f"\n❌ WebSocket error: {e}. Retrying in 3s...")
                await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(monitor_arbitrage())
    except KeyboardInterrupt:
        print("\n🔴 Shut down manually.")
    except Exception as e:
        print(f"\n❌ Fatal: {e}")