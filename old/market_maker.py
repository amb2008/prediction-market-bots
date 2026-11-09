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
PATH_V2_ORDER = "/trade-api/v2/portfolio/events/orders"

TICKERS = {
    "SD": "KXMLBGAME-26JUN222210ATLSD-SD",
    "ATL": "KXMLBGAME-26JUN222210ATLSD-ATL",
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE             = "overpriced_log.csv"
GAME_DESCRIPTION     = "PDA vs AMB"
OVERPRICED_THRESHOLD = 1.03   # fire when SUM_YES or SUM_NO exceeds this
TARGET_SUM           = 1.02   # undercut to bring sum to this level
TRADE_QUANTITY       = 1
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

last_global_seq: int  = -1
last_seq:        dict = {}
resync_needed:   set  = set()

is_trading     = False
trade_executed = False   # once True, never trade again
tick_count     = 0
start_time     = time.time()

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
        print(f"  🔴 SEQ GAP: expected {last_global_seq+1}, got {seq} "
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
                    last_seq.pop(ticker, None)
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
# PRICE CALCULATION
# ============================================================
def calculate_sell_prices(ask1, ask2):
    """
    Distribute the surplus above TARGET_SUM proportionally.
    Both legs get undercut so their combined sell price = TARGET_SUM.
    """
    current_sum = ask1 + ask2
    excess      = current_sum - TARGET_SUM
    sell1 = ask1 - excess * (ask1 / current_sum)
    sell2 = ask2 - excess * (ask2 / current_sum)
    sell1 = max(0.01, min(0.99, round(sell1, 4)))
    sell2 = max(0.01, min(0.99, round(sell2, 4)))
    return sell1, sell2

# ============================================================
# ORDER PLACEMENT — V2 spec
# ============================================================
async def place_sell_orders(session, side, sell_price1, sell_price2):
    """
    Places two GTC limit sell orders simultaneously using V2 endpoint.

    V2 quotes everything from the YES book:
      sell YES @ price  → side="ask", price=sell_price (dollar string)
      sell NO  @ price  → side="ask", price=1-sell_price (YES equivalent)
    """
    global is_trading
    is_trading = True

    url  = f"{BASE_REST}{PATH_V2_ORDER}"
    keys = list(TICKERS.keys())
    k1, k2 = keys[0], keys[1]

    def build_payload(key, price):
        # For YES sells: price is the YES ask we want to offer
        # For NO sells:  price must be converted to YES equivalent (1 - no_price)
        if side == "no":
            return {
                "ticker":                     TICKERS[key],
                "client_order_id":            str(uuid.uuid4()),
                "side":                       "ask",              # "ask" = sell YES (V2 spec)
                "price":                      f"{price:.2f}",
                "count":                      f"{float(TRADE_QUANTITY):.2f}",
                "time_in_force":              "good_till_canceled",
                "self_trade_prevention_type": "maker",
                "cancel_order_on_pause":      True,
                "reduce_only":                False,
            }
        else:
            return {
                "ticker":                     TICKERS[key],
                "client_order_id":            str(uuid.uuid4()),
                "side":                       "bid",              # "bid" = buy YES (V2 spec)
                "price":                      f"{price:.2f}",
                "count":                      f"{float(TRADE_QUANTITY):.2f}",
                "time_in_force":              "good_till_canceled",
                "self_trade_prevention_type": "maker",
                "cancel_order_on_pause":      True,
                "reduce_only":                False,
            }

    async def submit_one(key, price_dollars):
        headers = generate_auth_headers("POST", PATH_V2_ORDER)
        payload = build_payload(key, price_dollars)
        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                res      = await resp.json()
                # V2 response: top-level fields (not nested under "order")
                order_id = res.get("order_id")
                fill     = res.get("fill_count",      "0.00")
                remain   = res.get("remaining_count", "0.00")
                avg_px   = res.get("average_fill_price", "N/A")
                yes_px   = payload["price"]
                print(f"    [{key}] {side.upper()} @ {price_dollars:.4f} "
                      f"(YES book @ {yes_px}) | "
                      f"id={order_id} fill={fill} remain={remain} "
                      f"avg_px={avg_px} http={resp.status}")
                if resp.status == 201 and order_id:
                    return order_id
                print(f"    [{key}] ❌ Error: {res}")
                return None
        except Exception as e:
            print(f"    [{key}] submit error: {e}")
            return None

    # Fire both simultaneously
    id1, id2 = await asyncio.gather(
        submit_one(k1, sell_price1),
        submit_one(k2, sell_price2),
    )

    if id1 and id2:
        collected = sell_price1 + sell_price2
        print(f"\n  ✅ Both GTC sell orders resting in book")
        print(f"     {k1} SELL {side.upper()} @ {sell_price1:.4f}")
        print(f"     {k2} SELL {side.upper()} @ {sell_price2:.4f}")
        print(f"  ℹ️  When both fill: collect {sell_price1:.4f} + "
              f"{sell_price2:.4f} = {collected:.4f}")
        print(f"  ℹ️  Pay out $1.00 on winning leg → "
              f"net profit ≈ ${collected - 1.00:.4f} per pair")
        is_trading = False
        return id1, id2

    print(f"  ❌ One or both orders failed to submit")
    is_trading = False
    return None, None

# ============================================================
# OVERPRICED CHECK
# ============================================================
async def check_overpriced(session):
    global is_trading, trade_executed

    if is_trading:
        return
    if trade_executed:
        return
    if resync_needed:
        return

    is_trading = True

    p1, p2  = list(TICKERS.keys())[0], list(TICKERS.keys())[1]
    now     = datetime.now(timezone.utc)
    ts_utc  = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    elapsed = time.time() - start_time

    try:
        # --- YES overpriced ---
        y1 = curr_prices[p1].get("yes_ask")
        y2 = curr_prices[p2].get("yes_ask")
        if y1 is not None and y2 is not None:
            sum_yes = y1 + y2
            if sum_yes > OVERPRICED_THRESHOLD:
                sell1, sell2 = y1-0.01, y2-0.01
                signal = (f"YES_OVERPRICED sum={sum_yes:.4f} | "
                          f"selling {p1}@{sell1:.4f} {p2}@{sell2:.4f} "
                          f"target={TARGET_SUM:.2f}")
                print(f"\n  🎯 [{ts_utc}] {signal}")
                print(f"  {p1} YES ask={y1:.4f}  {p2} YES ask={y2:.4f}  "
                      f"SUM={sum_yes:.4f}  (threshold={OVERPRICED_THRESHOLD:.2f})")
                print(f"  Undercutting to: {p1}@{sell1:.4f} + "
                      f"{p2}@{sell2:.4f} = {sell1+sell2:.4f}")
                log_row(ts_utc, elapsed, signal)
                is_trading = False
                id1, id2 = await place_sell_orders(session, "no", sell1, sell2)
                if id1 and id2:
                    trade_executed = True
                    print(f"\n  🔒 TRADE PLACED — bot will not trade again this session")
                    print(f"  Order IDs: {id1}  |  {id2}")
                return

        # --- NO overpriced ---
        n1 = curr_prices[p1].get("no_ask")
        n2 = curr_prices[p2].get("no_ask")
        if n1 is not None and n2 is not None:
            sum_no = n1 + n2
            if sum_no > OVERPRICED_THRESHOLD:
                sell1, sell2 = 1-(n1-0.01), 1-(n2-0.01)
                signal = (f"NO_OVERPRICED sum={sum_no:.4f} | "
                          f"selling {p1}@{sell1:.4f} {p2}@{sell2:.4f} "
                          f"target={TARGET_SUM:.2f}")
                print(f"\n  🎯 [{ts_utc}] {signal}")
                print(f"  {p1} NO ask={n1:.4f}  {p2} NO ask={n2:.4f}  "
                      f"SUM={sum_no:.4f}  (threshold={OVERPRICED_THRESHOLD:.2f})")
                print(f"  Undercutting to: {p1}@{sell1:.4f} + "
                      f"{p2}@{sell2:.4f} = {sell1+sell2:.4f}")
                log_row(ts_utc, elapsed, signal)
                is_trading = False
                id1, id2 = await place_sell_orders(session, "yes", sell1, sell2)
                if id1 and id2:
                    trade_executed = True
                    print(f"\n  🔒 TRADE PLACED — bot will not trade again this session")
                    print(f"  Order IDs: {id1}  |  {id2}")
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

    sum_y = f"{p1_y+p2_y:.2f}" if (p1_y and p2_y) else "N/A"
    sum_n = f"{p1_n+p2_n:.2f}" if (p1_n and p2_n) else "N/A"

    flags = ""
    if resync_needed:   flags += " 🔄"
    if trade_executed:  flags += " 🔒DONE"

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait..."

    print(f"{ts}{flags}  | "
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
            if resync_needed:
                await request_resync_via_rest(session)
            return

    if msg_type == "orderbook_snapshot":
        if tick_count < 2:
            print(f"\n🔍 RAW SNAPSHOT ({leg_key}) seq={seq}: {msg}\n")
        apply_snapshot(leg_key, msg)
        update_derived_prices(leg_key)
        tick_count += 1
        await check_overpriced(session)
        print_live_odds()

    elif msg_type == "orderbook_delta":
        apply_delta(leg_key, msg)
        update_derived_prices(leg_key)
        tick_count += 1
        await check_overpriced(session)
        print_live_odds()

# ============================================================
# MAIN
# ============================================================
async def monitor():
    init_log()

    print("=" * 100)
    print(f"  🎯 Overpriced Market Monitor — GTC Limit Sell Strategy")
    print(f"  {GAME_DESCRIPTION}")
    print(f"  Fire when:   SUM_YES or SUM_NO > {OVERPRICED_THRESHOLD:.2f}")
    print(f"  Undercut to: target sum = {TARGET_SUM:.2f}")
    print(f"  Est. profit per pair: ${TARGET_SUM - 1.00:.2f} - fees")
    print(f"  ⚠️  Executes ONCE per session then monitors passively")
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
        asyncio.run(monitor())
    except KeyboardInterrupt:
        print("\n🔴 Shut down manually.")
    except Exception as e:
        print(f"\n❌ Fatal: {e}")