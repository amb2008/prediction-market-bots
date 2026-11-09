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

BASE_REST     = "https://external-api.kalshi.com"
WS_URL        = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
PATH_V2_ORDER = "/trade-api/v2/portfolio/orders"

TICKERS = {
    "FRA": "KXWCGAME-26JUN22FRAIRQ-FRA",   # home team win
    "TIE": "KXWCGAME-26JUN22FRAIRQ-TIE",   # draw
    "IRQ": "KXWCGAME-26JUN22FRAIRQ-IRQ",   # away team win
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE              = "soccer_arb.csv"
GAME_DESCRIPTION      = "Soccer 3-Way Arb"
THRESHOLD_YES         = 0.96    # fire when sum of all 3 YES asks < this
THRESHOLD_NO          = 1.92    # fire when sum of all 3 NO asks < this
TRADE_QUANTITY        = 1
LEG2_BUFFER_CENTS     = 5       # extra cents on leg 2
LEG3_BUFFER_CENTS     = 10      # extra cents on leg 3
MIN_LEG_PRICE         = 0.03    # ignore sub-3¢ legs (noise)
# === CONFIGURATION END ===

# ============================================================
# GLOBAL STATE
# ============================================================
curr_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None,
                    "status": "active"} for k in TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None,
                    "status": "active"} for k in TICKERS}
orderbooks  = {k: {"yes": {}, "no": {}} for k in TICKERS}

last_global_seq: int = -1
resync_needed:   set[str] = set()
last_seq:        dict[str, int] = {}

is_trading = False
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
              f"(missed {gap}) — queuing REST resync")
        resync_needed.update(TICKERS.values())
        last_global_seq = seq
        return False

    return True

async def request_resync_via_rest(session):
    if not resync_needed:
        return
    for ticker in list(resync_needed):
        leg_key = TICKERS_REVERSE.get(ticker, ticker)
        print(f"  🔄 REST resync [{leg_key}]...")
        try:
            path    = f"/trade-api/v2/markets/{ticker}/orderbook"
            url     = f"{BASE_REST}{path}?depth=100"
            headers = generate_auth_headers("GET", path)
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
                    print(f"  ✅ [{leg_key}] REST resync complete")
                else:
                    print(f"  ⚠️  [{leg_key}] empty book: {data}")
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
# LEG ORDERING — stale first, across all 3 legs
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
# ORDER EXECUTION — 3 legs: anchor + sweep2 + sweep3
# ============================================================
async def place_arb_orders(session, side):
    global is_trading
    is_trading = True

    price_key = f"{side}_ask"
    url       = f"{BASE_REST}{PATH_V2_ORDER}"
    legs      = get_leg_order_by_stability(side)
    leg1, leg2, leg3 = legs[0], legs[1], legs[2]

    def make_payload(leg_key, price_cents):
        return {
            "ticker":                     TICKERS[leg_key],
            "client_order_id":            str(uuid.uuid4()),
            "action":                     "buy",
            "side":                       side,
            "type":                       "limit",
            "count":                      TRADE_QUANTITY,
            f"{side}_price":              price_cents,
            "time_in_force":              "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross"
        }

    # --- LEG 1: Anchor — exact ask, no buffer ---
    ask1         = curr_prices[leg1][price_key]
    price_cents1 = min(99, int(ask1 * 100))
    headers1     = generate_auth_headers("POST", PATH_V2_ORDER)
    try:
        async with session.post(url, json=make_payload(leg1, price_cents1),
                                headers=headers1) as resp:
            res = await resp.json()
            if resp.status not in [200, 201] or res.get("order", {}).get("status") != "executed":
                reason = res.get("order", {}).get("cancel_reason", "unknown")
                print(f"    ⚠️ Leg 1 ({leg1}) missed. Safe abort. ({reason})")
                # Optimistic local removal
                counter_side        = "no" if side == "yes" else "yes"
                counter_price_cents = 100 - price_cents1
                orderbooks[leg1][counter_side].pop(counter_price_cents, None)
                update_derived_prices(leg1)
                is_trading = False
                return False
            print(f"    ✅ Leg 1 ({leg1}) FILLED @ {price_cents1}¢ "
                  f"| id={res['order']['order_id']}")
    except Exception as e:
        print(f"  ERROR Leg 1: {e}")
        is_trading = False
        return False

    # --- LEG 2: Sweep with +LEG2_BUFFER_CENTS ---
    ask2         = curr_prices[leg2][price_key]
    price_cents2 = min(99, int(ask2 * 100) + LEG2_BUFFER_CENTS)
    headers2     = generate_auth_headers("POST", PATH_V2_ORDER)
    leg2_filled  = False
    try:
        async with session.post(url, json=make_payload(leg2, price_cents2),
                                headers=headers2) as resp:
            res = await resp.json()
            if resp.status in [200, 201] and res.get("order", {}).get("status") == "executed":
                print(f"    ✅ Leg 2 ({leg2}) FILLED @ {price_cents2}¢ "
                      f"| id={res['order']['order_id']}")
                leg2_filled = True
            else:
                reason = res.get("order", {}).get("cancel_reason", "unknown")
                print(f"    🚨 TOXIC: Leg 2 ({leg2}) missed @ {price_cents2}¢ ({reason})")
                print(f"    🚨 YOU HOLD {leg1} {side.upper()} — manual close needed")
                is_trading = False
                return False
    except Exception as e:
        print(f"  🚨 CRITICAL Leg 2 error: {e}")
        print(f"  🚨 YOU HOLD {leg1} {side.upper()} — check portfolio")
        is_trading = False
        return False

    # --- LEG 3: Aggressive sweep with +LEG3_BUFFER_CENTS ---
    ask3         = curr_prices[leg3][price_key]
    price_cents3 = min(99, int(ask3 * 100) + LEG3_BUFFER_CENTS)
    headers3     = generate_auth_headers("POST", PATH_V2_ORDER)
    try:
        async with session.post(url, json=make_payload(leg3, price_cents3),
                                headers=headers3) as resp:
            res     = await resp.json()
            success = (resp.status in [200, 201] and
                       res.get("order", {}).get("status") == "executed")
            if success:
                print(f"    ✅ Leg 3 ({leg3}) FILLED @ {price_cents3}¢ "
                      f"| id={res['order']['order_id']}")
                print(f"  🎉 All 3 legs filled! Arb complete.")
            else:
                reason = res.get("order", {}).get("cancel_reason", "unknown")
                print(f"    🚨 TOXIC: Leg 3 ({leg3}) missed @ {price_cents3}¢ ({reason})")
                print(f"    🚨 YOU HOLD {leg1} + {leg2} {side.upper()} — manual close needed")
    except Exception as e:
        print(f"  🚨 CRITICAL Leg 3 error: {e}")
        print(f"  🚨 YOU HOLD {leg1} + {leg2} {side.upper()} — check portfolio")
        success = False

    await asyncio.sleep(0.5)
    is_trading = False
    return success

# ============================================================
# ARB CHECK — 3-way YES and NO
# ============================================================
async def check_arbitrage(session):
    global is_trading
    if is_trading:
        return
    if resync_needed:
        return

    is_trading = True

    keys    = list(TICKERS.keys())
    now     = datetime.now(timezone.utc)
    ts_utc  = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elapsed = time.time() - start_time

    try:
        # --- YES arb: all 3 YES asks sum < THRESHOLD_YES ---
        yes_asks = {k: curr_prices[k].get("yes_ask") for k in keys}
        if all(v is not None for v in yes_asks.values()):
            if all(v >= MIN_LEG_PRICE for v in yes_asks.values()):
                sum_yes = sum(yes_asks.values())
                if sum_yes < THRESHOLD_YES:
                    signal = f"ARBITRAGE_YES: {sum_yes:.4f}"
                    prices_str = " + ".join([f"{k} {yes_asks[k]:.2f}" for k in keys])
                    print(f"\n  🚨 [{ts_utc}] {signal}")
                    print(f"  {prices_str} = {sum_yes:.4f}")
                    log_row(ts_utc, elapsed, signal)
                    is_trading = False
                    success = await place_arb_orders(session, "yes")
                    if not success:
                        print("  Arb aborted")
                    return

        # --- NO arb: all 3 NO asks sum < THRESHOLD_NO ---
        no_asks = {k: curr_prices[k].get("no_ask") for k in keys}
        if all(v is not None for v in no_asks.values()):
            if all(v >= MIN_LEG_PRICE for v in no_asks.values()):
                sum_no = sum(no_asks.values())
                if sum_no < THRESHOLD_NO:
                    signal = f"ARBITRAGE_NO: {sum_no:.4f}"
                    prices_str = " + ".join([f"{k} {no_asks[k]:.2f}" for k in keys])
                    print(f"\n  🚨 [{ts_utc}] {signal}")
                    print(f"  {prices_str} = {sum_no:.4f}")
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
    keys = list(TICKERS.keys())

    changed = any(
        curr_prices[k].get(f) != last_printed[k].get(f)
        for k in TICKERS for f in ["yes_ask", "no_ask"]
    )
    if not changed:
        return
    for k in TICKERS:
        last_printed[k]["yes_ask"] = curr_prices[k].get("yes_ask")
        last_printed[k]["no_ask"]  = curr_prices[k].get("no_ask")

    ts          = datetime.now(timezone.utc).strftime("%H:%M:%S")
    resync_flag = " 🔄" if resync_needed else ""

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait.."

    yes_asks = [curr_prices[k].get("yes_ask") for k in keys]
    no_asks  = [curr_prices[k].get("no_ask")  for k in keys]
    sum_y    = f"{sum(yes_asks):.2f}" if all(v is not None for v in yes_asks) else "N/A"
    sum_n    = f"{sum(no_asks):.2f}"  if all(v is not None for v in no_asks)  else "N/A"

    leg_strs = " | ".join(
        [f"{k}_Y:{fmt(curr_prices[k].get('yes_ask'))} "
         f"{k}_N:{fmt(curr_prices[k].get('no_ask'))}" for k in keys]
    )
    print(f"{ts}{resync_flag} | {leg_strs} | SUM_Y:{sum_y} SUM_N:{sum_n}")

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
            if resync_needed:
                await request_resync_via_rest(session)
            return

    if msg_type == "orderbook_snapshot":
        if tick_count < 3:
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

    print("=" * 110)
    print(f"  🚀 3-Way Soccer Arb Monitor | YES < {THRESHOLD_YES} | NO < {THRESHOLD_NO}")
    print(f"  {GAME_DESCRIPTION} | Legs: {' / '.join(TICKERS.keys())}")
    print("=" * 110)

    ws_headers = generate_auth_headers("GET", "/trade-api/ws/v2")
    if not ws_headers:
        print("❌ Could not generate WS auth headers.")
        return

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    additional_headers=ws_headers,
                    ping_interval=15
                ) as ws:
                    print(f"✅ Connected: {WS_URL}\n")

                    global last_global_seq
                    last_global_seq = -1
                    last_seq.clear()
                    resync_needed.clear()
                    for k in TICKERS:
                        orderbooks[k]  = {"yes": {}, "no": {}}
                        curr_prices[k] = {"yes_ask": None, "yes_bid": None,
                                           "no_ask":  None, "no_bid":  None,
                                           "status": "active"}

                    await ws.send(json.dumps({
                        "id":  1,
                        "cmd": "subscribe",
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