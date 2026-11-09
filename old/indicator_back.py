import csv
from datetime import datetime
import itertools
from collections import defaultdict

# === CONFIGURATION ===
LOG_FILE = "sem_lee_prices.csv"

# Grid Search Parameters
TEST_LOWER_THRESHOLDS = [0.30, 0.50, 0.60]
TEST_UPPER_THRESHOLDS = [0.85, 0.90]
TEST_MAX_ORDERS       = [1, 2]
TEST_COOLDOWNS        = [30, 45, 60]

# Window Customization Grid Settings (0-14 minute range within the 15-m frame)
TEST_START_MINUTES    = [0, 1, 2]
TEST_END_MINUTES      = [12, 13, 14]

# 💡 STOP LOSS THRESHOLDS (Now tracking the single contract value dipping below this level)
# e.g., 0.30 means if our single $0.70 contract drops below $0.30 on the bid side, it triggers an exit.
TEST_STOP_LOSSES      = [0.20, 0.30, 0.40]

# EXECUTION BUFFERS
BOTH_ENDS_BUFFER_CENTS = 2 
STOP_LOSS_FEE_CENTS    = 2  # Additional fee when executing a stop loss

def load_data(filepath):
    data = []
    fieldnames = ["timestamp_local", "elapsed_s"]
    for key in ["ETH", "BTC"]:
        fieldnames.extend([f"{key}_yes_ask", f"{key}_yes_bid",
                           f"{key}_no_ask", f"{key}_no_bid"])
    fieldnames.append("signal")

    try:
        with open(filepath, "r") as f:
            reader = csv.DictReader(f, fieldnames=fieldnames)
            for row in reader:
                if row['timestamp_local'] == 'timestamp_local':
                    continue
                    
                if row['ETH_yes_ask'] and row['BTC_no_ask'] and row['ETH_no_ask'] and row['BTC_yes_ask']:
                    data.append({
                        "ts_raw": row['timestamp_local'],
                        "ts_dt": datetime.strptime(row['timestamp_local'][:23], "%Y-%m-%d %H:%M:%S.%f"),
                        # Asks (for buying/entering positions)
                        "eth_y_ask": float(row['ETH_yes_ask']),
                        "eth_n_ask": float(row['ETH_no_ask']),
                        "btc_y_ask": float(row['BTC_yes_ask']),
                        "btc_n_ask": float(row['BTC_no_ask']),
                        # Bids (for selling/exiting positions via stop loss)
                        "eth_y_bid": float(row['ETH_yes_bid']) if row['ETH_yes_bid'] else 0.0,
                        "eth_n_bid": float(row['ETH_no_bid']) if row['ETH_no_bid'] else 0.0,
                        "btc_y_bid": float(row['BTC_yes_bid']) if row['BTC_yes_bid'] else 0.0,
                        "btc_n_bid": float(row['BTC_no_bid']) if row['BTC_no_bid'] else 0.0,
                    })
    except FileNotFoundError:
        print(f"Error: Could not find {filepath}. Make sure you have logged data first.")
    return data

def calculate_market_resolutions(data):
    windows = defaultdict(list)
    for row in data:
        dt = row["ts_dt"]
        window_key = (dt.date(), dt.hour, dt.minute // 15)
        windows[window_key].append(row)
        
    total_markets = 0
    same_resolution_markets = 0
    
    for window_key, window_rows in windows.items():
        if not window_rows:
            continue
        total_markets += 1
        
        last_row = window_rows[-1]
        eth_yes_wins = last_row["eth_y_ask"] > 0.50
        btc_yes_wins = last_row["btc_y_ask"] > 0.50
        
        if eth_yes_wins == btc_yes_wins:
            same_resolution_markets += 1
            
    return same_resolution_markets, total_markets

def run_backtest(data, lower_thresh, upper_thresh, max_orders, cooldown, start_min, end_min, stop_loss_level):
    total_trades = 0
    total_cost_cents = 0
    total_payout_cents = 0
    
    # Counters for tracking performance when asset resolutions diverge
    diff_res_zero_dollars = 0
    diff_res_hundred_dollars = 0  # Changed from $2.00 to $1.00 since we only buy single contracts now

    windows = defaultdict(list)
    for row in data:
        dt = row["ts_dt"]
        window_key = (dt.date(), dt.hour, dt.minute // 15)
        windows[window_key].append(row)

    for window_key, window_rows in windows.items():
        if not window_rows:
            continue

        last_row = window_rows[-1]
        
        eth_yes_settles = 100 if last_row["eth_y_ask"] > 0.50 else 0
        eth_no_settles = 100 if eth_yes_settles == 0 else 0
        
        btc_yes_settles = 100 if last_row["btc_y_ask"] > 0.50 else 0
        btc_no_settles = 100 if btc_yes_settles == 0 else 0

        window_order_count = 0
        last_order_time = None
        
        # Track open positions: list of dicts {"contract": "eth_y"|"btc_y"|"eth_n"|"btc_n", "cost": cents}
        active_trades = []

        for row in window_rows:
            dt = row["ts_dt"]
            minute_in_frame = dt.minute % 15
            
            # 1. Check stop loss on single contracts using the BID price
            remaining_trades = []
            for trade in active_trades:
                if trade["contract"] == "eth_y":
                    current_value = row["eth_y_bid"]
                elif trade["contract"] == "btc_y":
                    current_value = row["btc_y_bid"]
                elif trade["contract"] == "eth_n":
                    current_value = row["eth_n_bid"]
                else:
                    current_value = row["btc_n_bid"]
                
                # If single contract value dips below stop loss level, liquidate
                if current_value < stop_loss_level:
                    payout = max(0, int(round(current_value * 100)) - STOP_LOSS_FEE_CENTS)
                    total_payout_cents += payout
                    total_trades += 1
                else:
                    remaining_trades.append(trade)
            active_trades = remaining_trades
            
            # 2. Check window trade window boundary
            if not (start_min <= minute_in_frame <= end_min):
                continue
                
            if window_order_count >= max_orders:
                continue
                
            if last_order_time is not None:
                time_since = (dt - last_order_time).total_seconds()
                if time_since < cooldown:
                    continue

            # Directional Indicator Logic
            # SCENARIO 1: BOTH LOOK BULLISH (Yes probabilities are high)
            if row["eth_y_ask"] >= lower_thresh and row["btc_y_ask"] >= lower_thresh:
                # Pick the cheaper one within our boundaries to maximize upside
                if row["eth_y_ask"] <= row["btc_y_ask"] and row["eth_y_ask"] < upper_thresh:
                    cost = min(99, int(round(row["eth_y_ask"] * 100)) + BOTH_ENDS_BUFFER_CENTS)
                    total_cost_cents += cost
                    active_trades.append({"contract": "eth_y", "cost": cost})
                    window_order_count += 1
                    last_order_time = dt
                    continue
                elif row["btc_y_ask"] < row["eth_y_ask"] and row["btc_y_ask"] < upper_thresh:
                    cost = min(99, int(round(row["btc_y_ask"] * 100)) + BOTH_ENDS_BUFFER_CENTS)
                    total_cost_cents += cost
                    active_trades.append({"contract": "btc_y", "cost": cost})
                    window_order_count += 1
                    last_order_time = dt
                    continue

            # SCENARIO 2: BOTH LOOK BEARISH (No probabilities are high)
            if row["eth_n_ask"] >= lower_thresh and row["btc_n_ask"] >= lower_thresh:
                # Pick the cheaper No contract within our boundaries
                if row["eth_n_ask"] <= row["btc_n_ask"] and row["eth_n_ask"] < upper_thresh:
                    cost = min(99, int(round(row["eth_n_ask"] * 100)) + BOTH_ENDS_BUFFER_CENTS)
                    total_cost_cents += cost
                    active_trades.append({"contract": "eth_n", "cost": cost})
                    window_order_count += 1
                    last_order_time = dt
                    continue
                elif row["btc_n_ask"] < row["eth_n_ask"] and row["btc_n_ask"] < upper_thresh:
                    cost = min(99, int(round(row["btc_n_ask"] * 100)) + BOTH_ENDS_BUFFER_CENTS)
                    total_cost_cents += cost
                    active_trades.append({"contract": "btc_n", "cost": cost})
                    window_order_count += 1
                    last_order_time = dt
                    continue

        # 3. Expiration Settlement for positions that survived until the final row
        markets_diverged = (eth_yes_settles != btc_yes_settles)
        
        for trade in active_trades:
            if trade["contract"] == "eth_y":
                payout = eth_yes_settles
            elif trade["contract"] == "btc_y":
                payout = btc_yes_settles
            elif trade["contract"] == "eth_n":
                payout = eth_no_settles
            else:
                payout = btc_no_settles
                
            total_payout_cents += payout
            total_trades += 1
            
            # Track outcomes when directional correlation breaks
            if markets_diverged:
                if payout == 0:
                    diff_res_zero_dollars += 1
                elif payout == 100:
                    diff_res_hundred_dollars += 1

    net_profit = (total_payout_cents - total_cost_cents) / 100.0
    ret_per_shot = net_profit / total_trades if total_trades > 0 else 0.0

    return {
        "trades": total_trades,
        "cost": total_cost_cents / 100.0,
        "pnl": net_profit,
        "ret_per_shot": ret_per_shot,
        "diff_zero_count": diff_res_zero_dollars,
        "diff_two_count": diff_res_hundred_dollars
    }

def main():
    print("="*125)
    print("📊 KALSHI CROSS-ASSET DIRECTIONAL INDICATOR BACKTESTER")
    print("="*125)
    
    data = load_data(LOG_FILE)
    if not data:
        return
        
    print(f"Loaded {len(data)} valid market ticks. Running scenarios...\n")
    print(f"{'LOWER':<5} | {'UPPER':<5} | {'MAXORD':<6} | {'COOL':<4} | {'START':<5} | {'END':<4} | {'STOPL':<5} || {'TRADES':<6} | {'CAPITAL':<8} | {'NET PNL':<8} | {'RET/SHOT'}")
    print("-" * 125)
    
    results = []
    combinations = itertools.product(
        TEST_LOWER_THRESHOLDS, 
        TEST_UPPER_THRESHOLDS, 
        TEST_MAX_ORDERS, 
        TEST_COOLDOWNS,
        TEST_START_MINUTES,
        TEST_END_MINUTES,
        TEST_STOP_LOSSES
    )
    
    for lower, upper, max_ord, cd, start_m, end_m, stop_l in combinations:
        if lower >= upper:
            continue
        if start_m >= end_m:
            continue
            
        res = run_backtest(data, lower, upper, max_ord, cd, start_m, end_m, stop_l)
        
        if res["trades"] > 0:
            results.append({
                "params": (lower, upper, max_ord, cd, start_m, end_m, stop_l),
                "stats": res
            })
            print(f"{lower:<5.2f} | {upper:<5.2f} | {max_ord:<6} | {cd:<4} | {start_m:<5} | {end_m:<4} | {stop_l:<5.2f} || {res['trades']:<6} | ${res['cost']:<7.2f} | ${res['pnl']:<7.2f} | ${res['ret_per_shot']:+.4f}")
            
    print("-" * 125)
    
    if not results:
        print("No trades triggered under any of the tested parameters.")
    else:
        same_res, total_markets = calculate_market_resolutions(data)
        pct_same = (same_res / total_markets * 100) if total_markets > 0 else 0.0

        results.sort(key=lambda x: x["stats"]["pnl"], reverse=True)
        best_pnl = results[0]
        
        results.sort(key=lambda x: x["stats"]["ret_per_shot"], reverse=True)
        best_efficiency = results[0]
        
        # Display Results
        print(f"\n📈 GLOBAL DATA SUMMARY:")
        print(f"   Market Resolutions : {same_res}/{total_markets} windows resolved the SAME way ({pct_same:.2f}% directional alignment)")

        p1, s1 = best_pnl["params"], best_pnl["stats"]
        print(f"\n🏆 BEST PERFORMING CONFIGURATION (BY TOTAL PNL):")
        print(f"   Settings     : Lower={p1[0]}, Upper={p1[1]}, MaxOrd={p1[2]}, Cooldown={p1[3]}s, Window=[M{p1[4]} -> M{p1[5]}], StopLoss={p1[6]}")
        print(f"   Total Trades : {s1['trades']} | Net PnL: ${s1['pnl']:+.2f} | Per Shot: ${s1['ret_per_shot']:+.4f}")
        print(f"   Diverging Outcomes (Settled): {s1['diff_zero_count']} trades made $0.00 vs. {s1['diff_two_count']} trades made $1.00")
        
        p2, s2 = best_efficiency["params"], best_efficiency["stats"]
        print(f"\n🎯 BEST PERFORMING CONFIGURATION (BY RETURN PER SHOT):")
        print(f"   Settings     : Lower={p2[0]}, Upper={p2[1]}, MaxOrd={p2[2]}, Cooldown={p2[3]}s, Window=[M{p2[4]} -> M{p2[5]}], StopLoss={p2[6]}")
        print(f"   Total Trades : {s2['trades']} | Net PnL: ${s2['pnl']:+.2f} | Per Shot: ${s2['ret_per_shot']:+.4f}")
        print(f"   Diverging Outcomes (Settled): {s2['diff_zero_count']} trades made $0.00 vs. {s2['diff_two_count']} trades made $1.00")
        print("="*125 + "\n")

if __name__ == "__main__":
    main()