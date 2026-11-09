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

TICKERS = {
    "FAL": "KXCS2GAME-26JUN201300FALTS-FAL",
    "TS": "KXCS2GAME-26JUN201300FALTS-TS",
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE            = "sem_lee_prices.csv"
GAME_DESCRIPTION    = "OKL vs GEO"
ARBITRAGE_THRESHOLD = 0.98
TRADE_QUANTITY      = 1
# === CONFIGURATION END ===

# Global State Management
curr_prices = {k: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"} for k in TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"} for k in TICKERS}
is_trading = False 
tick_count = 0
start_time = time.time()

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

def get_leg_order_by_stability(side):
    """Sorts legs ascending: Stale Quote (Δ0.000) goes first (Anchor)."""
    changes = {}
    for key in TICKERS.keys():
        c_price = curr_prices[key].get(f"{side}_ask")
        p_price = prev_prices[key].get(f"{side}_ask")
        if c_price is not None and p_price is not None:
            changes[key] = abs(c_price - p_price)
        else:
            changes[key] = 0

    ordered = sorted(TICKERS.keys(), key=lambda k: changes[k], reverse=False)
    order_str = " → ".join([f"{k}(Δ{changes[k]:.3f})" for k in ordered])
    print(f"  Leg order (most→least moved): {order_str}")
    return ordered

async def place_arb_orders(session, side):
    """Executes legs sequentially with an aggressive buffer on the second leg to guarantee fills."""
    global is_trading
    is_trading = True
    
    price_key = f"{side}_ask"
    url = f"{BASE_REST}{PATH_V2_ORDER}"
    
    # We execute them in a fixed order (or you can use your old leg sorting)
    legs = get_leg_order_by_stability(side)
    
    # --- LEG 1: The Anchor ---
    leg1 = legs[0]
    ask_price1 = curr_prices[leg1][price_key]
    # Standard +2¢ overbid to cross the immediate spread
    price_cents1 = min(99, int(ask_price1 * 100)) 
    
    payload1 = {
        "ticker": TICKERS[leg1],
        "client_order_id": str(uuid.uuid4()),
        "action": "buy",
        "side": side,
        "type": "limit",
        "count": TRADE_QUANTITY,
        f"{side}_price": price_cents1,
        "time_in_force": "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross"
    }
    
    headers1 = generate_auth_headers("POST", PATH_V2_ORDER)
    if not headers1:
        is_trading = False
        return False

    try:
        async with session.post(url, json=payload1, headers=headers1) as resp:
            res = await resp.json()
            if resp.status not in [200, 201] or res.get("order", {}).get("status") != "executed":
                reason = res.get("order", {}).get("cancel_reason", "unknown")
                print(f"    ⚠️ Leg 1 ({leg1}) missed liquidity. Safe abort. (Reason: {reason})")
                is_trading = False
                return False
            
            order_id1 = res.get("order", {}).get("order_id", "N/A")
            print(f"    ✅ Leg 1 ({leg1}) FILLED @ {price_cents1}¢ | id={order_id1}")
    except Exception as e:
        print(f"  ERROR executing Leg 1: {e}")
        is_trading = False
        return False

    # --- LEG 2: The Aggressive Sweep ---
    # Leg 1 filled successfully. Now we fire Leg 2 with a massive buffer to force a fill.
    leg2 = legs[1]
    ask_price2 = curr_prices[leg2][price_key]
    # Heavy +10¢ buffer to absorb sudden spikes or depth exhaustion
    price_cents2 = min(99, int(ask_price2 * 100) + 10) 
    
    payload2 = {
        "ticker": TICKERS[leg2],
        "client_order_id": str(uuid.uuid4()),
        "action": "buy",
        "side": side,
        "type": "limit",
        "count": TRADE_QUANTITY,
        f"{side}_price": price_cents2,
        "time_in_force": "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross"
    }
    
    headers2 = generate_auth_headers("POST", PATH_V2_ORDER)
    if not headers2:
        print("    🚨 CRITICAL: Leg 1 filled but failed to generate headers for Leg 2!")
        is_trading = False
        return False

    try:
        async with session.post(url, json=payload2, headers=headers2) as resp:
            res = await resp.json()
            if resp.status in [200, 201] and res.get("order", {}).get("status") == "executed":
                order_id2 = res.get("order", {}).get("order_id", "N/A")
                print(f"    ✅ Leg 2 ({leg2}) FILLED using high-buffer @ {price_cents2}¢ max | id={order_id2}")
                print(f"  🎉 Arbitrage fully executed!")
                success = True
            else:
                reason = res.get("order", {}).get("cancel_reason", "unknown")
                print(f"    🚨 TOXIC LEGGED: Leg 2 ({leg2}) missed even with +10¢ buffer! (Reason: {reason})")
                success = False
    except Exception as e:
        print(f"  🚨 CRITICAL ERROR executing Leg 2: {e}")
        success = False

    await asyncio.sleep(0.5) # Cooldown
    is_trading = False
    return success

async def check_arbitrage(session):
    if is_trading:
        return

    p1, p2 = list(TICKERS.keys())[0], list(TICKERS.keys())[1]
    now = datetime.now(timezone.utc)
    ts_utc = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elapsed = time.time() - start_time
    signal = ""

    # YES Arb Check
    if curr_prices[p1].get("yes_ask") is not None and curr_prices[p2].get("yes_ask") is not None:
        sum_yes = curr_prices[p1]["yes_ask"] + curr_prices[p2]["yes_ask"]
        if sum_yes < ARBITRAGE_THRESHOLD:
            signal = f"ARBITRAGE_YES_ASK: {sum_yes:.4f}"
            print(f"\n  🚨 [{ts_utc}] {signal}")
            print(f"  {p1} YES {curr_prices[p1]['yes_ask']:.2f} + {p2} YES {curr_prices[p2]['yes_ask']:.2f} = {sum_yes:.4f}")
            log_row(ts_utc, elapsed, signal)
            
            success = await place_arb_orders(session, "yes")
            if not success:
                print("  Arb aborted — partial fill risk avoided")
            return

    # NO Arb Check
    if curr_prices[p1].get("no_ask") is not None and curr_prices[p2].get("no_ask") is not None:
        sum_no = curr_prices[p1]["no_ask"] + curr_prices[p2]["no_ask"]
        if sum_no < ARBITRAGE_THRESHOLD:
            signal = f"ARBITRAGE_NO_ASK: {sum_no:.4f}"
            print(f"\n  🚨 [{ts_utc}] {signal}")
            print(f"  {p1} NO {curr_prices[p1]['no_ask']:.2f} + {p2} NO {curr_prices[p2]['no_ask']:.2f} = {sum_no:.4f}")
            log_row(ts_utc, elapsed, signal)
            
            success = await place_arb_orders(session, "no")
            if not success:
                print("  Arb aborted — partial fill risk avoided")
            return

async def process_ws_message(session, msg_data):
    global tick_count
    msg_type = msg_data.get("type")
    
    if msg_type == "ticker":
        tick = msg_data.get("msg", {})
        ticker = tick.get("market_ticker")
        
        if ticker in TICKERS_REVERSE:
            leg_key = TICKERS_REVERSE[ticker]
            
            # 🔍 --- ADD THIS DEBUG PRINT ---
            if tick_count < 2:  # Print the raw payload for the first couple of ticks
                print(f"\n🔍 RAW TICK DATA ({leg_key}): {tick}\n")
            # -------------------------------
            
            prev_prices[leg_key] = curr_prices[leg_key].copy()
            
            # Kalshi WebSocket sends string dollar values (e.g. "0.2400")
            if "yes_ask_dollars" in tick: 
                curr_prices[leg_key]["yes_ask"] = float(tick["yes_ask_dollars"])
                # Derive NO bid: 1.00 - YES ask
                curr_prices[leg_key]["no_bid"] = 1.0 - curr_prices[leg_key]["yes_ask"]
                
            if "yes_bid_dollars" in tick: 
                curr_prices[leg_key]["yes_bid"] = float(tick["yes_bid_dollars"])
                # Derive NO ask: 1.00 - YES bid
                curr_prices[leg_key]["no_ask"] = 1.0 - curr_prices[leg_key]["yes_bid"]
                
            if "status" in tick:  
                curr_prices[leg_key]["status"] = tick["status"]
            
            tick_count += 1
            await check_arbitrage(session)

            # LIVE ODDS PRINTOUT
            p1, p2 = list(TICKERS.keys())[0], list(TICKERS.keys())[1]
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")

            p1_y = curr_prices[p1].get("yes_ask")
            p1_n = curr_prices[p1].get("no_ask")
            p2_y = curr_prices[p2].get("yes_ask")
            p2_n = curr_prices[p2].get("no_ask")

            p1_y_str = f"${p1_y:.2f}" if p1_y is not None else "Wait..."
            p1_n_str = f"${p1_n:.2f}" if p1_n is not None else "Wait..."
            p2_y_str = f"${p2_y:.2f}" if p2_y is not None else "Wait..."
            p2_n_str = f"${p2_n:.2f}" if p2_n is not None else "Wait..."

            sum_y_str = f"{(p1_y + p2_y):.2f}" if (p1_y is not None and p2_y is not None) else "N/A"
            sum_n_str = f"{(p1_n + p2_n):.2f}" if (p1_n is not None and p2_n is not None) else "N/A"

            print(f"{ts}  | {p1}_Y: {p1_y_str:<5} | {p1}_N: {p1_n_str:<5} | {p2}_Y: {p2_y_str:<5} | {p2}_N: {p2_n_str:<5} | SUM_Y: {sum_y_str:<4} | SUM_N: {sum_n_str:<4}")

async def monitor_arbitrage():
    init_log()

    p1, p2 = list(TICKERS.keys())[0], list(TICKERS.keys())[1]

    print("=" * 95)
    print(f"  🚀 WebSocket 2-Way Arbitrage Monitor Initialized")
    print(f"  {GAME_DESCRIPTION} | Threshold: < {ARBITRAGE_THRESHOLD:.2f}")
    print("-" * 95)
    print(f"{'TIMESTAMP':9} | {p1 + '_Y ASK':11} | {p1 + '_N ASK':11} | {p2 + '_Y ASK':11} | {p2 + '_N ASK':11} | {'SUM_Y':7} | {'SUM_N':7}")
    print("=" * 95)

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


# === STANDARD PYTHON EXECUTION BLOCK ===
if __name__ == "__main__":
    try:
        asyncio.run(monitor_arbitrage())
    except KeyboardInterrupt:
        print("\n🔴 Bot shut down manually by user.")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")