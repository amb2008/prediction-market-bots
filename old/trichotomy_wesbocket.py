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
KALSHI_KEY_ID = 'DUMMY_KEY_ID'
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_REST = "https://external-api.kalshi.com"
WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
PATH_V2_ORDER = "/trade-api/v2/portfolio/orders"

# 3-Way Market Tickers
TICKERS = {
    "COL": "KXWCGAME-26JUN17UZBCOL-COL",
    "UZB": "KXWCGAME-26JUN17UZBCOL-UZB",
    "TIE": "KXWCGAME-26JUN17UZBCOL-TIE",
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE                = "soccer_prices_ws.csv"
GAME_DESCRIPTION        = "Soccer: Home vs Away vs Tie"
ARBITRAGE_THRESHOLD_YES = 0.98
ARBITRAGE_THRESHOLD_NO  = ARBITRAGE_THRESHOLD_YES * 2
TRADE_QUANTITY          = 1
# === CONFIGURATION END ===

# Global state
curr_prices = {k: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"} for k in TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"} for k in TICKERS}
is_trading       = False
tick_count       = 0
start_time       = time.time()
recorded_move_ts = []   # debounce by wall-clock seconds instead of poll index
last_traded_yes_sum = None
last_traded_no_sum  = None
# Load once, at module level — not per-call
with open(PRIVATE_KEY_PATH, "rb") as f:
    PRIVATE_KEY = serialization.load_pem_private_key(f.read(), password=None)

def generate_auth_headers(method: str, path: str):
    timestamp_ms = str(int(time.time() * 1000))
    payload = timestamp_ms + method + path
    signature = PRIVATE_KEY.sign(
        payload.encode("utf-8"),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256()
    )
    return {
        "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

def init_log():
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            header = ["timestamp_utc", "elapsed_s"]
            for key in TICKERS.keys():
                header.extend([f"{key}_yes_ask", f"{key}_yes_bid",
                               f"{key}_no_ask", f"{key}_no_bid"])
            header.append("signal")
            writer.writerow(header)
    print(f"Logging to: {LOG_FILE}")

def log_row(ts_utc, elapsed, signal=""):
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        row_data = [ts_utc, f"{elapsed:.2f}"]
        for key in TICKERS.keys():
            row_data.extend([
                curr_prices[key]["yes_ask"], curr_prices[key]["yes_bid"],
                curr_prices[key]["no_ask"], curr_prices[key]["no_bid"]
            ])
        row_data.append(signal)
        writer.writerow(row_data)

def is_within_debounce_window(now_ts, recorded_ts_list, window_s=10.0):
    return any(abs(now_ts - t) < window_s for t in recorded_ts_list)

def get_leg_order_by_stability(side):
    """
    Sorts legs so the one that has moved LEAST (the stale quote) goes first (Anchor).
    This attacks the lagging price before the market maker can update/cancel it.
    """
    changes = {}
    for key in TICKERS.keys():
        c_price = curr_prices[key].get(f"{side}_ask")
        p_price = prev_prices[key].get(f"{side}_ask")
        if c_price is not None and p_price is not None:
            changes[key] = abs(c_price - p_price)
        else:
            changes[key] = 0

    # Ascending: stale (least moved) first
    ordered = sorted(TICKERS.keys(), key=lambda k: changes[k])
    order_str = " → ".join([f"{k}(Δ{changes[k]:.3f})" for k in ordered])
    print(f"  Leg order (stale→volatile): {order_str}")
    return ordered

async def place_arb_orders(session, side):
    """
    Places Taker IOC orders across 3 legs.
    Leg 1 (Anchor): Targets the stale quote. If it fails, abort safely.
    Legs 2 & 3 (Completion): If Leg 1 fills, sweep aggressively up to 99¢ to force a hedge.
    """
    global is_trading
    is_trading = True

    leg_order = get_leg_order_by_stability(side)
    placed    = []
    price_key = f"{side}_ask"
    url       = f"{BASE_REST}{PATH_V2_ORDER}"
    is_first_leg = True

    for key in leg_order:
        if is_first_leg:
            ask_price   = curr_prices[key][price_key]
            price_cents = min(99, int(ask_price * 100) + 2)
        else:
            # Aggressive completion — sweep the book
            price_cents = 99

        payload = {
            "ticker": TICKERS[key],
            "client_order_id": str(uuid.uuid4()),
            "action": "buy",
            "side": side,
            "type": "limit",
            "count": TRADE_QUANTITY,
            f"{side}_price": price_cents,
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross"
        }

        headers = generate_auth_headers("POST", PATH_V2_ORDER)
        if not headers:
            is_trading = False
            return False

        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                res = await resp.json()
                http_status = resp.status

                if http_status in [200, 201]:
                    order_status = res.get("order", {}).get("status", "UNKNOWN")
                    order_id     = res.get("order", {}).get("order_id", "N/A")
                    actual_cost  = res.get("order", {}).get("taker_fill_cost_dollars", "UNKNOWN")

                    if order_status == "executed":
                        print(f"    ✅ {key} {side.upper()} FILLED @ {actual_cost} | id={order_id}")
                        placed.append(key)
                        is_first_leg = False   # anchor hit — switch to sweep mode
                    else:
                        if is_first_leg:
                            cancel_reason = res.get("order", {}).get("cancel_reason", "unknown")
                            print(f"    ⚠️ {key} anchor missed liquidity. Safe abort. (Reason: {cancel_reason})")
                            is_trading = False
                            return False
                        else:
                            print(f"    🚨 FATAL: {key} missed liquidity even at 99¢! Order book empty.")
                            print(f"    🚨 You are currently legged in on: {placed}")
                            is_trading = False
                            return False
                else:
                    print(f"    ❌ API Error on {key}: {res}")
                    if not is_first_leg:
                        print(f"    🚨 FATAL: API Error during sweep. Legged in on: {placed}")
                    is_trading = False
                    return False

        except Exception as e:
            print(f"  ERROR executing {key}: {e}")
            if not is_first_leg:
                print(f"    🚨 FATAL: Network crash during sweep. Legged in on: {placed}")
            is_trading = False
            return False

    print(f"  ✅ All 3 legs locked down.")
    await asyncio.sleep(0.5)  # cooldown
    is_trading = False
    return True

async def check_arbitrage(session):
    global last_traded_yes_sum, last_traded_no_sum

    if is_trading:
        return

    p1, p2, p3 = list(TICKERS.keys())
    now      = datetime.now(timezone.utc)
    now_ts   = time.time()
    ts_utc   = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elapsed  = now_ts - start_time

    # --- 3-Way YES arb check ---
    yes_prices = {k: curr_prices[k].get("yes_ask") for k in [p1, p2, p3]
                  if curr_prices[k].get("yes_ask") is not None}

    if len(yes_prices) == 3:
        sum_yes   = yes_prices[p1] + yes_prices[p2] + yes_prices[p3]
        sum_yes_q = int(sum_yes * 100)

        if sum_yes < ARBITRAGE_THRESHOLD_YES:
            if (last_traded_yes_sum is None or sum_yes_q != last_traded_yes_sum) and \
               not is_within_debounce_window(now_ts, recorded_move_ts):

                signal = f"ARBITRAGE_YES_ASK: {sum_yes:.4f}"
                recorded_move_ts.append(now_ts)
                print(f"\n  🚨 [{ts_utc}] {signal}")
                print(f"  {p1}({yes_prices[p1]:.2f}) + {p2}({yes_prices[p2]:.2f}) + {p3}({yes_prices[p3]:.2f}) = {sum_yes:.4f}")
                log_row(ts_utc, elapsed, signal)

                success = await place_arb_orders(session, "yes")
                if success:
                    last_traded_yes_sum = sum_yes_q
                else:
                    print("  Arb aborted — partial fill risk avoided")
                return
        else:
            last_traded_yes_sum = None
    else:
        last_traded_yes_sum = None

    # --- 3-Way NO arb check ---
    no_prices = {k: curr_prices[k].get("no_ask") for k in [p1, p2, p3]
                 if curr_prices[k].get("no_ask") is not None}

    if len(no_prices) == 3:
        sum_no   = no_prices[p1] + no_prices[p2] + no_prices[p3]
        sum_no_q = int(sum_no * 100)

        if sum_no < ARBITRAGE_THRESHOLD_NO:
            if (last_traded_no_sum is None or sum_no_q != last_traded_no_sum) and \
               not is_within_debounce_window(now_ts, recorded_move_ts):

                signal = f"ARBITRAGE_NO_ASK: {sum_no:.4f}"
                recorded_move_ts.append(now_ts)
                print(f"\n  🚨 [{ts_utc}] {signal}")
                print(f"  {p1}({no_prices[p1]:.2f}) + {p2}({no_prices[p2]:.2f}) + {p3}({no_prices[p3]:.2f}) = {sum_no:.4f}")
                log_row(ts_utc, elapsed, signal)

                success = await place_arb_orders(session, "no")
                if success:
                    last_traded_no_sum = sum_no_q
                else:
                    print("  Arb aborted — partial fill risk avoided")
                return
        else:
            last_traded_no_sum = None
    else:
        last_traded_no_sum = None

async def process_ws_message(session, msg_data):
    global tick_count
    msg_type = msg_data.get("type")

    if msg_type == "ticker":
        tick   = msg_data.get("msg", {})
        ticker = tick.get("market_ticker")

        if ticker in TICKERS_REVERSE:
            leg_key = TICKERS_REVERSE[ticker]

            if tick_count < 3:
                print(f"\n🔍 RAW TICK DATA ({leg_key}): {tick}\n")

            prev_prices[leg_key] = curr_prices[leg_key].copy()

            if "yes_ask_dollars" in tick:
                curr_prices[leg_key]["yes_ask"] = float(tick["yes_ask_dollars"])
                curr_prices[leg_key]["no_bid"]  = 1.0 - curr_prices[leg_key]["yes_ask"]

            if "yes_bid_dollars" in tick:
                curr_prices[leg_key]["yes_bid"] = float(tick["yes_bid_dollars"])
                curr_prices[leg_key]["no_ask"]  = 1.0 - curr_prices[leg_key]["yes_bid"]

            if "status" in tick:
                curr_prices[leg_key]["status"] = tick["status"]

            tick_count += 1

            await check_arbitrage(session)

            # LIVE 3-WAY ODDS PRINTOUT
            p1, p2, p3 = list(TICKERS.keys())
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")

            def fmt(k, field):
                v = curr_prices[k].get(field)
                return f"${v:.2f}" if v is not None else "Wait.."

            yes_vals = [curr_prices[k].get("yes_ask") for k in [p1, p2, p3]]
            no_vals  = [curr_prices[k].get("no_ask")  for k in [p1, p2, p3]]
            sum_y    = f"{sum(yes_vals):.2f}" if all(v is not None for v in yes_vals) else "N/A"
            sum_n    = f"{sum(no_vals):.2f}"  if all(v is not None for v in no_vals)  else "N/A"

            print(f"{ts} | {p1}_Y:{fmt(p1,'yes_ask'):<6} {p1}_N:{fmt(p1,'no_ask'):<6} | "
                  f"{p2}_Y:{fmt(p2,'yes_ask'):<6} {p2}_N:{fmt(p2,'no_ask'):<6} | "
                  f"{p3}_Y:{fmt(p3,'yes_ask'):<6} {p3}_N:{fmt(p3,'no_ask'):<6} | "
                  f"SUM_Y:{sum_y:<5} SUM_N:{sum_n:<5}")

async def monitor_arbitrage():
    init_log()

    p1, p2, p3 = list(TICKERS.keys())

    print("=" * 110)
    print(f"  🚀 WebSocket 3-Way Arbitrage Monitor Initialized")
    print(f"  {GAME_DESCRIPTION} | YES thresh: < {ARBITRAGE_THRESHOLD_YES:.2f} | NO thresh: < {ARBITRAGE_THRESHOLD_NO:.2f}")
    print("=" * 110)

    ws_headers = generate_auth_headers("GET", "/trade-api/ws/v2")
    if not ws_headers:
        print("❌ Could not generate WS headers. Check your private key.")
        return

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(WS_URL, additional_headers=ws_headers, ping_interval=15) as ws:
                    print(f"✅ Connected to Kalshi WebSocket: {WS_URL}\n")

                    sub_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["ticker"],
                            "market_tickers": list(TICKERS.values())
                        }
                    }
                    await ws.send(json.dumps(sub_msg))

                    async for msg in ws:
                        data = json.loads(msg)
                        await process_ws_message(session, data)

            except Exception as e:
                print(f"\n❌ WebSocket Connection Failed: {e}. Retrying in 3 seconds...")
                await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(monitor_arbitrage())
    except KeyboardInterrupt:
        print("\n🔴 Bot shut down manually by user.")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")