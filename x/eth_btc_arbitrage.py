import asyncio
import aiohttp
import websockets
import json
import time
import csv
import os
import base64
import uuid
from datetime import datetime
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

# === CONFIGURATION START ===
KALSHI_KEY_ID = 'DUMMY_KEY_ID'
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_REST = "https://external-api.kalshi.com"
WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
PATH_V2_ORDER = "/trade-api/v2/portfolio/events/orders"

def get_current_15m_tickers():
    """Dynamically calculates the current target Kalshi 15M tickers based on local time."""
    now_ts = time.time()
    current_block_start = (int(now_ts) // 900) * 900
    target_expiry_ts = current_block_start + 900  
    expiry_dt = datetime.fromtimestamp(target_expiry_ts)
    
    year_str = expiry_dt.strftime("%y")          
    month_str = expiry_dt.strftime("%b").upper() 
    day_str = expiry_dt.strftime("%d")            
    hhmm_str = expiry_dt.strftime("%H%M")        
    mm_str = expiry_dt.strftime("%M")            
    
    ticker_suffix = f"{year_str}{month_str}{day_str}{hhmm_str}-{mm_str}"
    return {
        "ETH": f"KXETH15M-{ticker_suffix}",
        "BTC": f"KXBTC15M-{ticker_suffix}",
    }

# Dynamically set initial tickers on startup
TICKERS = get_current_15m_tickers()
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE              = "sem_lee_prices.csv"
GAME_DESCRIPTION      = "LV vs PHX"
ARBITRAGE_THRESHOLD   = 0.85
LOWER_ARBITRAGE_THRESHOLD = 0.60  
TRADE_QUANTITY        = 1

# --- RUNTIME CONFIGURATIONS ---
PAPER_TRADING         = False   # Set to False to route orders live to Kalshi Exchange
START_MINUTE          = 2      # Allowed execution start minute within the 15-min bar (0-14)
END_MINUTE            = 13     # Allowed execution end minute within the 15-min bar (0-14)

MAX_ORDERS_PER_WINDOW = 1      # "Shots" allowed per 15-minute block
COOLDOWN_SECONDS      = 45     # Mandatory pause duration (in seconds) after a successful execution
# === CONFIGURATION END ===

# Global State Management
curr_prices = {k: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"} for k in ["ETH", "BTC"]}
prev_prices = {k: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"} for k in ["ETH", "BTC"]}
is_trading = False 
tick_count = 0
start_time = time.time()
last_report_minute = -1
paper_ledger = []  

# Simulated Portfolio Trackers
paper_holdings = {
    f"{asset}_{side}": {"qty": 0, "avg_cost": 0.0} 
    for asset in ["ETH", "BTC"] for side in ["YES", "NO"]
}

# Realized Profit & Settlement Trackers
pending_settlements = {} 
realized_profit = 0.0

# Shots and Cooldown Trackers
window_order_count = 0
last_order_time = 0
current_15m_block = None

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
            header = ["timestamp_local", "elapsed_s"]
            for key in ["ETH", "BTC"]:
                header.extend([f"{key}_yes_ask", f"{key}_yes_bid",
                               f"{key}_no_ask", f"{key}_no_bid"])
            header.append("signal")
            writer.writerow(header)

def log_row(ts_local, elapsed, signal=""):
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        row_data = [ts_local, f"{elapsed:.2f}"]
        for key in ["ETH", "BTC"]:
            row_data.extend([
                curr_prices[key]["yes_ask"], curr_prices[key]["yes_bid"],
                curr_prices[key]["no_ask"], curr_prices[key]["no_bid"]
            ])
        row_data.append(signal)
        writer.writerow(row_data)

def get_leg_order_by_stability(target_sides):
    changes = {}
    for key in ["ETH", "BTC"]:
        side = target_sides[key] 
        c_price = curr_prices[key].get(f"{side}_ask")
        p_price = prev_prices[key].get(f"{side}_ask")
        if c_price is not None and p_price is not None:
            changes[key] = abs(c_price - p_price)
        else:
            changes[key] = 0

    ordered = sorted(["ETH", "BTC"], key=lambda k: changes[k], reverse=False)
    order_str = " → ".join([f"{k}(Δ{changes[k]:.3f})" for k in ordered])
    print(f"   Leg order (most→least moved): {order_str}")
    return ordered

def is_within_time_window():
    now = datetime.now()
    minute_in_frame = now.minute % 15
    return START_MINUTE <= minute_in_frame <= END_MINUTE, minute_in_frame

def update_simulated_position(asset, side, qty, cost_cents):
    key = f"{asset}_{side.upper()}"
    cost_dollars = cost_cents / 100.0
    current = paper_holdings[key]
    
    if current["qty"] == 0:
        current["avg_cost"] = cost_dollars
    else:
        total_cost = (current["qty"] * current["avg_cost"]) + (qty * cost_dollars)
        current["avg_cost"] = total_cost / (current["qty"] + qty)
        
    current["qty"] += qty

async def check_pending_settlements(session):
    """Queries Kalshi REST API to resolve expired simulated positions and update payout."""
    global realized_profit, pending_settlements
    
    if not pending_settlements:
        return
        
    resolved_tickers = []
    
    for ticker, data in pending_settlements.items():
        url = f"{BASE_REST}/trade-api/v2/markets/{ticker}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    res = await resp.json()
                    market = res.get("market", {})
                    status = market.get("status")
                    
                    if status in ["determined", "finalized"]:
                        result = market.get("result", "") # "yes" or "no"
                        
                        payout = 0.0
                        if result == "yes":
                            payout = data["yes_qty"] * 1.00 # $1 per contract payout
                        elif result == "no":
                            payout = data["no_qty"] * 1.00
                            
                        profit = payout - data["cost_basis"]
                        realized_profit += profit
                        
                        print(f"\n💰 [SETTLEMENT] {ticker} resolved as {result.upper()}!")
                        print(f"   Payout: ${payout:.2f} | Cost: ${data['cost_basis']:.2f} | PnL: ${profit:+.2f}")
                        
                        resolved_tickers.append(ticker)
        except Exception as e:
            print(f"  [ERROR] Checking settlement for {ticker}: {e}")
            
    for t in resolved_tickers:
        del pending_settlements[t]

def print_portfolio_status():
    print("\n" + "="*85)
    print(f"📊 AUTOMATED PORTFOLIO SUMMARY REPORT — {datetime.now().strftime('%H:%M:%S')}")
    print("="*85)
    print(f"{'Position Contract':20} | {'Qty Held':8} | {'Avg Entry':10} | {'Current Mark':12} | {'Unrealized PnL':14}")
    print("-" * 85)
    
    total_unrealized = 0.0
    total_cost_basis = 0.0
    has_holdings = False
    
    for holding_key, data in paper_holdings.items():
        if data["qty"] > 0:
            has_holdings = True
            asset, side = holding_key.split("_")
            
            ask = curr_prices[asset].get(f"{side.lower()}_ask")
            bid = curr_prices[asset].get(f"{side.lower()}_bid")
            
            if ask is not None and bid is not None:
                current_mark = (ask + bid) / 2.0
            elif ask is not None:
                current_mark = ask
            else:
                current_mark = data["avg_cost"]
                
            cost_basis = data["qty"] * data["avg_cost"]
            current_value = data["qty"] * current_mark
            unrealized_pnl = current_value - cost_basis
            
            total_cost_basis += cost_basis
            total_unrealized += unrealized_pnl
            
            print(f"{holding_key:20} | {data['qty']:<8} | ${data['avg_cost']:<9.2f} | ${current_mark:<11.2f} | ${unrealized_pnl:+.2f}")
            
    if not has_holdings:
        print("   No open contract components in active portfolio.")
    print("-" * 85)
    
    if pending_settlements:
        print("\n⏳ PENDING SETTLEMENTS (Awaiting Kalshi Resolution)")
        print("-" * 85)
        for t, data in pending_settlements.items():
            print(f"   {t}: {data['yes_qty']} YES | {data['no_qty']} NO (Cost Basis: ${data['cost_basis']:.2f})")
        print("-" * 85)

    print(f"Total Active Cost Basis    : ${total_cost_basis:.2f}")
    print(f"Net Floating Session PnL   : ${total_unrealized:+.2f}")
    print(f"Total Realized Session PnL : ${realized_profit:+.2f}")
    print("="*85 + "\n")

async def place_arb_orders(session, target_sides):
    global is_trading, paper_ledger, window_order_count, last_order_time
    is_trading = True
    
    legs = get_leg_order_by_stability(target_sides)
    
    leg1 = legs[0]
    side1 = target_sides[leg1] # "yes" or "no"
    ask_price1 = curr_prices[leg1][f"{side1}_ask"]
    
    leg2 = legs[1]
    side2 = target_sides[leg2] # "yes" or "no"
    ask_price2 = curr_prices[leg2][f"{side2}_ask"]

    # Internal values kept as cents for ledger & position math
    price_cents1 = min(99, int(round(ask_price1 * 100)) + 2) 
    price_cents2 = 99

    if PAPER_TRADING:
        print(f"    🟢 [PAPER MODE] Simulating execution...")
        print(f"    ✅ Leg 1 Mock Filled: {leg1} {side1.upper()} @ {price_cents1}¢ max")
        print(f"    ✅ Leg 2 Mock Filled: {leg2} {side2.upper()} @ {price_cents2}¢ max (Taker sweep)")
        print(f"    🎉 Simulated Arbitrage fully processed!")
        
        paper_ledger.append({
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "leg1_info": f"{leg1} {side1.upper()}",
            "leg1_price": price_cents1,
            "leg2_info": f"{leg2} {side2.upper()}",
            "leg2_price": price_cents2,
            "total_cost": price_cents1 + price_cents2
        })
        
        update_simulated_position(leg1, side1, TRADE_QUANTITY, price_cents1)
        update_simulated_position(leg2, side2, TRADE_QUANTITY, price_cents2)
        
        window_order_count += 1
        last_order_time = time.time()
        
        await asyncio.sleep(0.5)
        is_trading = False
        return True

    # =========================================================================
    # V2 ENGINE TRANSLATION (Map "YES/NO" into "bid/ask" and fixed-point strings)
    # =========================================================================
    v2_count = f"{float(TRADE_QUANTITY):.2f}"

    # Leg 1 V2 Mapping (+2 cent taker buffer)
    if side1 == "yes":
        v2_side1 = "bid"
        v2_price1 = f"{min(0.99, ask_price1 + 0.02):.4f}"
    else:
        v2_side1 = "ask"
        v2_price1 = f"{max(0.01, (1.0 - ask_price1) - 0.02):.4f}"

    # Leg 2 V2 Mapping (+10 cent sweep buffer)
    if side2 == "yes":
        v2_side2 = "bid"
        v2_price2 = f"{min(0.99, ask_price2 + 0.10):.4f}"

    else:
        v2_side2 = "ask"
        v2_price2 = f"{max(0.01, (1.0 - ask_price2) - 0.10):.4f}"


    url = f"{BASE_REST}{PATH_V2_ORDER}"
    
    # ==========================================
    # EXECUTE LEG 1 (V2 Spec)
    # ==========================================
    payload1 = {
        "ticker": TICKERS[leg1],
        "client_order_id": str(uuid.uuid4()),
        "side": v2_side1,
        "price": v2_price1, 
        "count": v2_count,
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
            order_data = res.get("order", res) if isinstance(res, dict) else {}
            order_id1 = order_data.get("order_id", "N/A")
            fill_count1 = order_data.get("fill_count", "0.00")
            
            if resp.status not in [200, 201] or float(fill_count1) == 0:
                reason = order_data.get("cancel_reason", "No immediate liquidity / Canceled")
                print(f"    ⚠️ Leg 1 ({leg1} {side1.upper()}) missed liquidity. Safe abort. (Reason: {reason})")
                print(f"    🔍 [DEBUG] Raw Kalshi Response (Leg 1):\n{json.dumps(res, indent=4)}")
                is_trading = False
                return False
            
            print(f"    ✅ Leg 1 ({leg1} {side1.upper()}) FILLED {fill_count1} contracts | id={order_id1}")
            window_order_count += 1
    except Exception as e:
        print(f"  ERROR executing Leg 1: {e}")
        is_trading = False
        return False

    # ==========================================
    # EXECUTE LEG 2 (V2 Spec)
    # ==========================================
    payload2 = {
        "ticker": TICKERS[leg2],
        "client_order_id": str(uuid.uuid4()),
        "side": v2_side2,
        "price": v2_price2,
        "count": v2_count,
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
            order_data = res.get("order", res) if isinstance(res, dict) else {}
            order_id2 = order_data.get("order_id", "N/A")
            fill_count2 = order_data.get("fill_count", "0.00")

            if resp.status in [200, 201] and float(fill_count2) > 0:
                print(f"    ✅ Leg 2 ({leg2} {side2.upper()}) FILLED {fill_count2} contracts | id={order_id2}")
                print(f"  🎉 Arbitrage fully executed!")
                
                # Update ledger and local portfolio trackers for live execution
                paper_ledger.append({
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "leg1_info": f"{leg1} {side1.upper()}",
                    "leg1_price": price_cents1,
                    "leg2_info": f"{leg2} {side2.upper()}",
                    "leg2_price": price_cents2,
                    "total_cost": price_cents1 + price_cents2
                })
                
                update_simulated_position(leg1, side1, TRADE_QUANTITY, price_cents1)
                update_simulated_position(leg2, side2, TRADE_QUANTITY, price_cents2)
                
                last_order_time = time.time()
                success = True
            else:
                reason = order_data.get("cancel_reason", "No immediate liquidity / Canceled")
                print(f"    🚨 TOXIC LEGGED: Leg 2 ({leg2} {side2.upper()}) missed! (Reason: {reason})")
                print(f"    🔍 [DEBUG] Raw Kalshi Response (Leg 2):\n{json.dumps(res, indent=4)}")
                success = False
    except Exception as e:
        print(f"  🚨 CRITICAL ERROR executing Leg 2: {e}")
        success = False

    await asyncio.sleep(0.5) 
    is_trading = False
    return success

async def check_arbitrage(session, ts_local, elapsed):
    global window_order_count, last_order_time, current_15m_block
    
    if is_trading:
        return ""

    now = datetime.now()
    
    block = now.minute // 15
    if current_15m_block != block:
        current_15m_block = block
        window_order_count = 0
        
    if window_order_count >= MAX_ORDERS_PER_WINDOW:
        return ""
        
    if time.time() - last_order_time < COOLDOWN_SECONDS:
        return ""

    p1, p2 = "ETH", "BTC"

    p1_yes = curr_prices[p1].get("yes_ask")
    p2_no = curr_prices[p2].get("no_ask")
    
    if p1_yes is not None and p2_no is not None:
        sum_mix1 = p1_yes + p2_no
        if LOWER_ARBITRAGE_THRESHOLD <= sum_mix1 < ARBITRAGE_THRESHOLD:
            signal = f"ARBITRAGE_YES_NO: {sum_mix1:.4f}"
            print(f"\n  🚨 [{ts_local}] {signal}")
            print(f"  {p1} YES {p1_yes:.2f} + {p2} NO {p2_no:.2f} = {sum_mix1:.4f}")
            
            allowed, current_min = is_within_time_window()
            if not allowed:
                print(f"    ⏳ [WINDOW BLOCK] Signal rejected. Minute {current_min} of 15m frame is outside boundaries ({START_MINUTE}-{END_MINUTE}).")
                return signal

            success = await place_arb_orders(session, {p1: "yes", p2: "no"})
            if not success:
                print("  Arb aborted — partial fill risk avoided")
            return signal

    p1_no = curr_prices[p1].get("no_ask")
    p2_yes = curr_prices[p2].get("yes_ask")
    
    if p1_no is not None and p2_yes is not None:
        sum_mix2 = p1_no + p2_yes
        if LOWER_ARBITRAGE_THRESHOLD <= sum_mix2 < ARBITRAGE_THRESHOLD:
            signal = f"ARBITRAGE_NO_YES: {sum_mix2:.4f}"
            print(f"\n  🚨 [{ts_local}] {signal}")
            print(f"  {p1} NO {p1_no:.2f} + {p2} YES {p2_yes:.2f} = {sum_mix2:.4f}")
            
            allowed, current_min = is_within_time_window()
            if not allowed:
                print(f"    ⏳ [WINDOW BLOCK] Signal rejected. Minute {current_min} of 15m frame is outside boundaries ({START_MINUTE}-{END_MINUTE}).")
                return signal

            success = await place_arb_orders(session, {p1: "no", p2: "yes"})
            if not success:
                print("  Arb aborted — partial fill risk avoided")
            return signal

    return ""

async def process_ws_message(session, msg_data):
    global tick_count, last_report_minute
    msg_type = msg_data.get("type")
    
    if msg_type == "ticker":
        tick = msg_data.get("msg", {})
        ticker = tick.get("market_ticker")
        
        if ticker in TICKERS_REVERSE:
            leg_key = TICKERS_REVERSE[ticker]
            
            if tick_count < 2:  
                print(f"\n🔍 RAW TICK DATA ({leg_key}): {tick}\n")
            
            prev_prices[leg_key] = curr_prices[leg_key].copy()
            
            if "yes_ask_dollars" in tick: 
                curr_prices[leg_key]["yes_ask"] = float(tick["yes_ask_dollars"])
                curr_prices[leg_key]["no_bid"] = 1.0 - curr_prices[leg_key]["yes_ask"]
                
            if "yes_bid_dollars" in tick: 
                curr_prices[leg_key]["yes_bid"] = float(tick["yes_bid_dollars"])
                curr_prices[leg_key]["no_ask"] = 1.0 - curr_prices[leg_key]["yes_bid"]
                
            if "status" in tick:  
                curr_prices[leg_key]["status"] = tick["status"]
            
            tick_count += 1
            
            now = datetime.now()
            ts_local = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            elapsed = time.time() - start_time
            
            signal_string = await check_arbitrage(session, ts_local, elapsed)
            log_row(ts_local, elapsed, signal=signal_string)

            # Check if a minute has passed to resolve pending settlements and print status
            if now.minute != last_report_minute:
                if last_report_minute != -1:  
                    await check_pending_settlements(session)
                    print_portfolio_status()
                last_report_minute = now.minute

            # LIVE ODDS PRINTOUT
            p1, p2 = "ETH", "BTC"
            ts = now.strftime("%H:%M:%S")

            p1_y = curr_prices[p1].get("yes_ask")
            p1_n = curr_prices[p1].get("no_ask")
            p2_y = curr_prices[p2].get("yes_ask")
            p2_n = curr_prices[p2].get("no_ask")

            p1_y_str = f"${p1_y:.2f}" if p1_y is not None else "Wait..."
            p1_n_str = f"${p1_n:.2f}" if p1_n is not None else "Wait..."
            p2_y_str = f"${p2_y:.2f}" if p2_y is not None else "Wait..."
            p2_n_str = f"${p2_n:.2f}" if p2_n is not None else "Wait..."

            sum_y1_n2 = f"{(p1_y + p2_n):.2f}" if (p1_y is not None and p2_n is not None) else "N/A"
            sum_n1_y2 = f"{(p1_n + p2_y):.2f}" if (p1_n is not None and p2_y is not None) else "N/A"

            allowed, cur_m = is_within_time_window()
            time_since_order = time.time() - last_order_time

            if window_order_count >= MAX_ORDERS_PER_WINDOW:
                window_flag = "MAX_ORD"
            elif time_since_order < COOLDOWN_SECONDS:
                rem = int(COOLDOWN_SECONDS - time_since_order)
                window_flag = f"COOL:{rem:02d}"
            elif allowed:
                window_flag = "LIVE   "
            else:
                window_flag = "BLOCKED"

            print(f"{ts} (M:{cur_m:02d}|{window_flag:7}) | {p1}_Y: {p1_y_str:<5} | {p1}_N: {p1_n_str:<5} | {p2}_Y: {p2_y_str:<5} | {p2}_N: {p2_n_str:<5} | Y1_N2: {sum_y1_n2:<4} | N1_Y2: {sum_n1_y2:<4}")

async def monitor_arbitrage():
    global TICKERS, TICKERS_REVERSE, window_order_count, current_15m_block, paper_holdings, pending_settlements
    init_log()

    print("=" * 110)
    print(f"  🚀 WebSocket 2-Way Cross-Side Arbitrage Monitor Initialized (PAPER TRADING: {PAPER_TRADING})")
    print(f"  Time Window Filter: Allowed only between minutes {START_MINUTE} and {END_MINUTE} of the 15M bar structure.")
    print(f"  Execution Ceilings: Max {MAX_ORDERS_PER_WINDOW} arbs/window | {COOLDOWN_SECONDS}s reset cooldown.")
    print(f"  Arbitrage Corridor: Execution strictly limited between {LOWER_ARBITRAGE_THRESHOLD:.2f} and {ARBITRAGE_THRESHOLD:.2f}")
    print("-" * 110)
    print(f"{'TIMESTAMP':18} | {'ETH_Y ASK':11} | {'ETH_N ASK':11} | {'BTC_Y ASK':11} | {'BTC_N ASK':11} | {'Y1_N2':7} | {'N1_Y2':7}")
    print("=" * 110)

    async with aiohttp.ClientSession() as session:
        while True:
            ws_headers = generate_auth_headers("GET", "/trade-api/ws/v2")
            if not ws_headers:
                print("❌ Could not generate WS headers. Check your private key.")
                return
                
            try:
                async with websockets.connect(WS_URL, additional_headers=ws_headers, ping_interval=15) as ws:
                    print(f"✅ Connected to Kalshi WebSocket: {WS_URL}\n")
                    
                    TICKERS = get_current_15m_tickers()
                    TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}
                    current_15m_block = datetime.now().minute // 15
                    
                    print(f"📡 Initializing sub window targets:")
                    print(f"   ETH Target Asset: {TICKERS['ETH']}")
                    print(f"   BTC Target Asset: {TICKERS['BTC']}\n")

                    sub_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["ticker"],
                            "market_tickers": list(TICKERS.values())
                        }
                    }
                    await ws.send(json.dumps(sub_msg))
                    
                    async_iterator = ws.__aiter__()
                    while True:
                        now_tickers = get_current_15m_tickers()
                        
                        # --- MARKET ROLLOVER TRIGGER ---
                        if now_tickers["ETH"] != TICKERS["ETH"]:
                            print(f"\n🔄 [MARKET ROLLOVER] Pushing active contracts to pending settlement queue...")
                            
                            for asset in ["ETH", "BTC"]:
                                old_ticker = TICKERS[asset]
                                yes_qty = paper_holdings[f"{asset}_YES"]["qty"]
                                no_qty = paper_holdings[f"{asset}_NO"]["qty"]
                                
                                cost_basis = (paper_holdings[f"{asset}_YES"]["avg_cost"] * yes_qty) + \
                                             (paper_holdings[f"{asset}_NO"]["avg_cost"] * no_qty)
                                
                                # Move active positions into pending queue waiting for Kalshi to declare outcome
                                if yes_qty > 0 or no_qty > 0:
                                    pending_settlements[old_ticker] = {
                                        "asset": asset,
                                        "yes_qty": yes_qty,
                                        "no_qty": no_qty,
                                        "cost_basis": cost_basis
                                    }
                                
                                # Flush current window holdings back to zero
                                paper_holdings[f"{asset}_YES"] = {"qty": 0, "avg_cost": 0.0}
                                paper_holdings[f"{asset}_NO"]  = {"qty": 0, "avg_cost": 0.0}

                            TICKERS = now_tickers
                            TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}
                            
                            for k in ["ETH", "BTC"]:
                                curr_prices[k] = {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"}
                                prev_prices[k] = {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None, "status": "active"}
                            
                            window_order_count = 0
                            current_15m_block = datetime.now().minute // 15
                            
                            print(f"📡 Updating WS subscriptions for new 15M structure:")
                            print(f"   ETH: {TICKERS['ETH']}")
                            print(f"   BTC: {TICKERS['BTC']}\n")
                            
                            rot_sub_msg = {
                                "id": int(time.time()),
                                "cmd": "subscribe",
                                "params": {
                                    "channels": ["ticker"],
                                    "market_tickers": list(TICKERS.values())
                                }
                            }
                            await ws.send(json.dumps(rot_sub_msg))

                        try:
                            msg = await asyncio.wait_for(async_iterator.__anext__(), timeout=20)
                            data = json.loads(msg)
                            await process_ws_message(session, data)
                        except asyncio.TimeoutError:
                            ts_now = datetime.now().strftime("%H:%M:%S")
                            allowed, cur_m = is_within_time_window()
                            print(f"{ts_now} (M:{cur_m:02d}) | [IDLE: WebSocket connected. Awaiting market updates for {TICKERS['BTC']}...]")
            
            except Exception as e:
                print(f"\n❌ WebSocket Connection Failed: {e}. Retrying in 3 seconds...")
                await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(monitor_arbitrage())
    except KeyboardInterrupt:
        print("\n🔴 Bot shut down manually by user.")
        
        print("\n" + "="*75)
        print("📊 SESSION TRADING SUMMARY LEDGER")
        print("="*75)
        if not paper_ledger:
            print(" No trades were recorded in this session window.")
        else:
            total_capital = 0
            print(f"{'Timestamp':19} | {'Leg 1 Anchor':14} | {'Cost 1':6} | {'Leg 2 Sweep':14} | {'Cost 2':6} | {'Total Cost':10}")
            print("-" * 75)
            for trade in paper_ledger:
                print(f"{trade['timestamp']} | {trade['leg1_info']:14} | {trade['leg1_price']:5}¢ | {trade['leg2_info']:14} | {trade['leg2_price']:5}¢ | {trade['total_cost']:8}¢")
                total_capital += trade['total_cost']
            print("-" * 75)
            print(f"Total Arbitrage Executions           : {len(paper_ledger)}")
            print(f"Total Capital Committed              : ${total_capital / 100:.2f}")
            print(f"Total Realized Session Profit        : ${realized_profit:+.2f}")
        print("="*75 + "\n")
        
        # Print the final status mark before closing down entirely
        print_portfolio_status()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")