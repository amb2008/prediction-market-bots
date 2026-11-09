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
PATH_V2_ORDER = "/trade-api/v2/portfolio/events/orders"

TICKERS = {
    "ATL": "KXMLBGAME-26JUN232140ATLSD-ATL",
    "SD": "KXMLBGAME-26JUN232140ATLSD-SD",
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE            = "sem_lee_prices.csv"
GAME_DESCRIPTION    = "CHC vs TOR"
ARBITRAGE_THRESHOLD = 0.97
TRADE_QUANTITY      = "1.00"
executed = False
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

# Sequence tracking per ticker — key: ticker string, value: last seq seen
last_seq: dict[str, int] = {}

# Flag to signal the main loop that a re-snapshot is needed
resync_needed: set[str] = set()

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
# SEQUENCE CHECKING — per-ticker, gap triggers REST resync
# ============================================================
# Replace per-ticker tracking with a single global sequence
last_global_seq: int = -1

def check_sequence(seq: int, ticker: str, msg_type: str) -> bool:
    global last_global_seq

    if msg_type == "orderbook_snapshot":
        # Snapshots establish the baseline
        if seq > last_global_seq:
            last_global_seq = seq
        return True

    if msg_type == "orderbook_delta":
        if last_global_seq == -1:
            last_global_seq = seq
            return True

        if seq <= last_global_seq:
            return True   # duplicate/old, ignore silently

        if seq == last_global_seq + 1:
            last_global_seq = seq
            return True

        # Genuine gap
        gap = seq - last_global_seq - 1
        print(f"  🔴 GLOBAL SEQ GAP: expected {last_global_seq+1}, got {seq} "
              f"(missed {gap}) — queuing REST resync for ALL markets")
        
        # We missed messages on the shared channel; queue resync for everything
        resync_needed.update(TICKERS.keys())
        last_global_seq = seq
        return False

    return True

async def request_resync_via_rest(session):
    """
    Fetches a fresh orderbook snapshot via REST for any ticker needing resync.
    Much simpler than WS unsubscribe/resubscribe — no subscription ID needed.
    """
    if not resync_needed:
        return

    tickers_to_resync = list(resync_needed)
    for ticker in tickers_to_resync:
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
                    # Reset seq — next delta will establish new baseline
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
        int(round(float(lvl[0]) * 100)): float(lvl[1])  # <-- Changed to float
        for lvl in msg.get("yes_dollars_fp", [])
    }
    orderbooks[leg_key]["no"] = {
        int(round(float(lvl[0]) * 100)): float(lvl[1])  # <-- Changed to float
        for lvl in msg.get("no_dollars_fp", [])
    }

def apply_delta(leg_key, msg):
    side      = msg.get("side")
    price_str = msg.get("price_dollars")
    delta_str = msg.get("delta_fp")
    
    if side not in ("yes", "no") or price_str is None or delta_str is None:
        return
        
    price_cents = int(round(float(price_str) * 100))
    delta_qty   = float(delta_str)    # <-- Changed to float
    
    book        = orderbooks[leg_key][side]
    new_qty     = book.get(price_cents, 0.0) + delta_qty
    
    # Check against a tiny epsilon instead of pure 0 to avoid floating-point dust bugs
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
        # --- NEW: Explicitly clear stale prices ---
        curr_prices[leg_key]["yes_bid"] = None
        curr_prices[leg_key]["no_ask"]  = None
        
    if best_no_cents is not None:
        d = best_no_cents / 100.0
        curr_prices[leg_key]["no_bid"] = round(d, 4)
        curr_prices[leg_key]["yes_ask"] = round(1.0 - d, 4)
    else:
        # --- NEW: Explicitly clear stale prices ---
        curr_prices[leg_key]["no_bid"] = None
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
# ORDER EXECUTION
# ============================================================
async def place_arb_orders(session, side):
    global is_trading
    is_trading = True

    price_key = f"{side}_ask"
    url       = f"{BASE_REST}{PATH_V2_ORDER}"
    legs      = get_leg_order_by_stability(side)

    # Leg 1 — Anchor
    leg1         = legs[0]
    ask_price1   = curr_prices[leg1][price_key]
    price_cents1 = min(99, int(ask_price1 * 100)+2)

    if side == "yes":
        side1_v2 = "bid"
        price_dollars1 = price_cents1/100.0
    else:
        side1_v2 = "ask"
        price_dollars1 = 1.0 - (price_cents1/100.0)

    payload1 = {
        "ticker":                     TICKERS[leg1],
        "client_order_id":            str(uuid.uuid4()),
        "count":                      TRADE_QUANTITY,
        "side":                       side1_v2,
        "price":                      f"{price_dollars1:.2f}",
        "time_in_force":              "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
        "cancel_order_on_pause":      True,
        "reduce_only":                False,
    }

    headers1 = generate_auth_headers("POST", PATH_V2_ORDER)
    try:
        async with session.post(url, json=payload1, headers=headers1) as resp:
            res = await resp.json()
            if resp.status not in [200, 201] or res.get("order", {}).get("status") != "executed":
                reason = res.get("order", {}).get("cancel_reason", "unknown")
                print(f"    ⚠️ Leg 1 ({leg1}) missed. Safe abort. ({reason}), {res}")
                
                # --- OPTIMISTIC LOCAL DELETION ---
                # The order failed; the liquidity is gone. 
                # Instantly delete it locally so we don't loop while waiting for the WS delta.
                counter_side = "no" if side == "yes" else "yes"
                counter_price_cents = 100 - price_cents1
                
                orderbooks[leg1][counter_side].pop(counter_price_cents, None)
                update_derived_prices(leg1)
                # ---------------------------------
                
                is_trading = False
                return False
            print(f"    ✅ Leg 1 ({leg1}) FILLED @ {price_cents1}¢ "
                  f"| id={res['order']['order_id']}")
            executed = True
    except Exception as e:
        print(f"  ERROR Leg 1: {e}")
        is_trading = False
        return False

    # Leg 2 — Aggressive Sweep
    leg2         = legs[1]
    ask_price2   = curr_prices[leg2][price_key]
    price_cents2 = min(99, int(ask_price2 * 100) + 10)

    if side == "yes":
        side2_v2 = "bid"
        price_dollars2 = price_cents2/100.0
    else:
        side2_v2 = "ask"
        price_dollars2 = 1.0 - (price_cents2/100.0)


    payload2 = {
        "ticker":                     TICKERS[leg2],
        "client_order_id":            str(uuid.uuid4()),
        "side":                       side2_v2,
        "count":                      TRADE_QUANTITY,
        "price":                       f"{price_dollars2:.2f}",
        "time_in_force":              "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
        "cancel_order_on_pause":      True,
        "reduce_only":                False,
    }

    headers2 = generate_auth_headers("POST", PATH_V2_ORDER)
    try:
        async with session.post(url, json=payload2, headers=headers2) as resp:
            res     = await resp.json()
            success = (resp.status in [200, 201] and
                       res.get("order", {}).get("status") == "executed")
            if success:
                print(f"    ✅ Leg 2 ({leg2}) FILLED @ {price_cents2}¢ max "
                      f"| id={res['order']['order_id']}")
                print(f"  🎉 Arbitrage fully executed!")
            else:
                reason = res.get("order", {}).get("cancel_reason", "unknown")
                print(f"    🚨 TOXIC LEG: Leg 2 ({leg2}) missed @ {price_cents2}¢ ({reason})")
                print(f"    🚨 YOU HOLD {leg1} {side.upper()} — manual close may be needed")
    except Exception as e:
        print(f"  🚨 CRITICAL Leg 2 error: {e}")
        print(f"  🚨 YOU HOLD {leg1} {side.upper()} — check portfolio")
        success = False

    await asyncio.sleep(0.5)
    is_trading = False
    return success

# ============================================================
# ARB CHECK
# ============================================================
async def check_arbitrage(session):
    global is_trading
    if is_trading:
        return
    
    if executed:
        return

    # Don't trade while we know our book is stale
    if resync_needed:
        return

    is_trading = True   # lock before any await to prevent concurrent triggers

    p1, p2    = list(TICKERS.keys())[0], list(TICKERS.keys())[1]
    now       = datetime.now(timezone.utc)
    ts_utc    = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elapsed   = time.time() - start_time

    try:
        # YES arb
        y1 = curr_prices[p1].get("yes_ask")
        y2 = curr_prices[p2].get("yes_ask")
        if y1 is not None and y2 is not None:
            sum_yes = y1 + y2
            if sum_yes < ARBITRAGE_THRESHOLD:
                signal = f"ARBITRAGE_YES_ASK: {sum_yes:.4f}"
                print(f"\n  🚨 [{ts_utc}] {signal}")
                print(f"  {p1} YES {y1:.2f} + {p2} YES {y2:.2f} = {sum_yes:.4f}")
                log_row(ts_utc, elapsed, signal)
                is_trading = False   # release before place_arb_orders re-acquires
                success = await place_arb_orders(session, "yes")
                if not success:
                    print("  Arb aborted")
                return

        # NO arb
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

    ts  = datetime.now(timezone.utc).strftime("%H:%M:%S")
    p1_y, p1_n = curr_prices[p1].get("yes_ask"), curr_prices[p1].get("no_ask")
    p2_y, p2_n = curr_prices[p2].get("yes_ask"), curr_prices[p2].get("no_ask")

    # Fetch the direct bid prices from your data structure
    p1_y_bid = curr_prices[p1].get("yes_bid")
    p2_y_bid = curr_prices[p2].get("yes_bid")
    p1_n_bid = curr_prices[p1].get("no_bid")
    p2_n_bid = curr_prices[p2].get("no_bid")

    resync_flag = " 🔄RESYNCING" if resync_needed else ""
    
    # 1. Calculate raw numerical buy sums safely
    sum_y_val = (p1_y + p2_y) if (p1_y is not None and p2_y is not None) else None
    sum_n_val = (p1_n + p2_n) if (p1_n is not None and p2_n is not None) else None

    # 2. Format the Buy Sum strings
    sum_y_str = f"{sum_y_val:.2f}" if sum_y_val is not None else "N/A"
    sum_n_str = f"{sum_n_val:.2f}" if sum_n_val is not None else "N/A"

    # 3. Calculate and format the Sell Values using the Bids directly
    sell_y_val = (p1_y_bid + p2_y_bid) if (p1_y_bid is not None and p2_y_bid is not None) else None
    sell_n_val = (p1_n_bid + p2_n_bid) if (p1_n_bid is not None and p2_n_bid is not None) else None

    sell_both_y = f"${sell_y_val:.2f}" if sell_y_val is not None else "N/A"
    sell_both_n = f"${sell_n_val:.2f}" if sell_n_val is not None else "N/A"

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait..."

    # 4. Clean, aligned printout
    print(f"{ts}{resync_flag}  | "
          f"{p1}_Y: {fmt(p1_y):<6} {p1}_N: {fmt(p1_n):<6} | "
          f"{p2}_Y: {fmt(p2_y):<6} {p2}_N: {fmt(p2_n):<6} | "
          f"SUM_Y: {sum_y_str:<5} SUM_N: {sum_n_str:<5} ")

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
    p1, p2 = list(TICKERS.keys())[0], list(TICKERS.keys())[1]

    print("=" * 100)
    print(f"  🚀 WebSocket 2-Way Arbitrage Monitor (orderbook_delta + seq validation)")
    print(f"  {GAME_DESCRIPTION} | Threshold: < {ARBITRAGE_THRESHOLD:.2f}")
    print("=" * 100)

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

                    # Reset sequence state on reconnect
                    last_seq.clear()
                    resync_needed.clear()
                    for k in TICKERS:
                        orderbooks[k] = {"yes": {}, "no": {}}
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