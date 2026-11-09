"""
binary_websocket_deltav4 — latency-optimized taker-taker arb.

STRATEGY (unchanged from v3):
Watch two complementary markets; when the sum of asks drops below
ARBITRAGE_THRESHOLD, buy BOTH sides. Legs are ordered stale→volatile and
executed SEQUENTIALLY: leg 1 (the stale anchor) first, and leg 2 fires
ONLY IF leg 1 filled — never an unhedged leg. On a leg-1 miss, abort
safely and optimistically delete the phantom liquidity locally.

WHY v3 NEVER TOOK LEG 1 — the fire path paid, in order:
  - two console prints + a SYNCHRONOUS CSV file write (open/write/close)
  - a leg-ordering computation with another print
  - a fresh RSA-PSS signature (~1-3 ms, blocking the event loop)
  - aiohttp JSON serialization
  - and a COLD TCP+TLS handshake (~100-300 ms), because the session was
    never warmed and idles out between rare signals
Total: ~150-350 ms of self-inflicted latency before the order even left,
against stale-quote windows that live for tens of milliseconds.

V4 LATENCY CHANGES (execution logic identical):
 1. PRE-SIGNED AUTH HEADERS: a background task keeps a fresh POST /orders
    signature cached (signed in a thread executor). Zero RSA at fire time.
 2. WARM CONNECTIONS: two pooled sockets are TLS-handshaken at startup and
    kept alive with a ping every KEEPALIVE_SECS, so leg 1 never pays a
    reconnect. (This is the big one for a rarely-firing bot.)
 3. NON-BLOCKING OUTPUT: all prints AND CSV rows go through a queue drained
    by a writer thread. Console/file I/O can no longer sit between signal
    detection and the order hitting the wire. Output content is unchanged.
 4. PRE-BUILT PAYLOADS: both legs' payloads are serialized to bytes before
    leg 1 fires. (Safe: the WS loop is blocked during the trade, so
    curr_prices cannot change between building leg 2 and sending it —
    identical values to v3, which read them after leg 1.) Leg 2 is still
    sent ONLY after leg 1 confirms filled.
 5. HOT-PATH HYGIENE: timestamp strings built only when a signal actually
    fires (was every tick), incremental best-bid tracking instead of
    max()-over-book per delta, orjson (optional) for WS parsing, WS
    compression disabled, uvloop/winloop (optional).
 6. TIMING INSTRUMENTATION: each leg logs detect→wire and round-trip ms
    (LEG_TIMING lines) so fire speed is measurable from the log.

Success criterion unchanged: HTTP 201 + fill_count > 0.
"""

import asyncio
import aiohttp
import websockets
import json
import time
import csv
import os
import sys
import base64
import uuid
import threading
import queue as _queue
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization

# ── Optional accelerators (no-ops if not installed) ──────────
try:
    import orjson
    def _jloads(b):  return orjson.loads(b)
    def _jdumps(o):  return orjson.dumps(o)            # returns bytes
except ImportError:
    def _jloads(b):  return json.loads(b)
    def _jdumps(o):  return json.dumps(o).encode()

# === CONFIGURATION START ===
KALSHI_KEY_ID    = 'DUMMY_KEY_ID'
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_REST = "https://external-api.kalshi.com"
WS_URL    = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

# V2 path (legacy /portfolio/orders is being deprecated)
PATH_V2_ORDER = "/trade-api/v2/portfolio/events/orders"
STATUS_PATH   = "/trade-api/v2/exchange/status"

TICKERS = {
    "FER": "KXWCADVANCE-26JUL07SUICOL-COL",
    "CER": "KXWCADVANCE-26JUL07SUICOL-SUI",
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}

LOG_FILE            = "sem_lee_prices.csv"
GAME_DESCRIPTION    = "NYY vs DET"
ARBITRAGE_THRESHOLD = 0.96
TRADE_QUANTITY      = 1

SIG_REFRESH_SECS = 2.0     # background re-sign cadence
SIG_MAX_AGE_SECS = 10.0    # cached sig older than this → sign fresh
KEEPALIVE_SECS   = 20.0    # connection keep-warm ping cadence
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
best_cents  = {k: {"yes": None, "no": None} for k in TICKERS}   # incremental best bid (¢)

last_seq: dict[str, int] = {}
resync_needed: set[str]  = set()

is_trading = False
executed   = False
tick_count = 0
start_time = time.time()

with open(PRIVATE_KEY_PATH, "rb") as f:
    PRIVATE_KEY = serialization.load_pem_private_key(f.read(), password=None)

# ============================================================
# NON-BLOCKING CONSOLE + CSV WRITER
# All prints and CSV rows are queued and written by a dedicated
# thread; slow console/file I/O can never sit on the fire path.
# ============================================================
_out_q: _queue.SimpleQueue = _queue.SimpleQueue()
_WRITER_SENTINEL = object()

def _writer_loop():
    write = sys.stdout.write
    flush = sys.stdout.flush
    while True:
        item = _out_q.get()
        if item is _WRITER_SENTINEL:
            flush(); return
        try:
            texts, rows = [], []
            def sort(it):
                (rows if isinstance(it, tuple) else texts).append(it)
            sort(item)
            while True:
                try:
                    nxt = _out_q.get_nowait()
                except _queue.Empty:
                    break
                if nxt is _WRITER_SENTINEL:
                    if texts: write("".join(texts)); flush()
                    _write_rows(rows)
                    return
                sort(nxt)
            if texts:
                write("".join(texts)); flush()
            _write_rows(rows)
        except Exception:
            pass

def _write_rows(rows):
    if not rows:
        return
    try:
        with open(LOG_FILE, "a", newline="") as f:
            w = csv.writer(f)
            for r in rows:
                w.writerow(r)
    except Exception:
        pass

_writer_thread = threading.Thread(target=_writer_loop, daemon=True, name="io-writer")
_writer_thread.start()

def qprint(text: str = ""):
    _out_q.put(text + "\n")

def queue_csv_row(row: tuple):
    _out_q.put(row)

def flush_output(timeout: float = 2.0):
    deadline = time.time() + timeout
    while not _out_q.empty() and time.time() < deadline:
        time.sleep(0.01)

# ============================================================
# AUTH  (RSA-PSS)  — with background pre-signing
# ============================================================
def _sign(method: str, path: str) -> dict:
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

def generate_auth_headers(method: str, path: str):
    """Synchronous signing — cold paths only (WS connect, resync, warmup)."""
    return _sign(method, path)

_post_hdr:    dict | None = None
_post_hdr_at: float       = 0.0

def post_order_headers() -> dict:
    """Ready-made POST /orders headers — zero RSA on the fire path.
    Both legs POST to the same path, so one cached signature serves both."""
    if _post_hdr is not None and (time.time() - _post_hdr_at) <= SIG_MAX_AGE_SECS:
        return _post_hdr
    return _sign("POST", PATH_V2_ORDER)     # cold fallback

async def signature_refresher():
    global _post_hdr, _post_hdr_at
    loop = asyncio.get_running_loop()
    while True:
        try:
            hdr = await loop.run_in_executor(None, _sign, "POST", PATH_V2_ORDER)
            _post_hdr, _post_hdr_at = hdr, time.time()
        except Exception:
            pass
        await asyncio.sleep(SIG_REFRESH_SECS)

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
    """Same CSV format as v3, but queued to the writer thread instead of
    doing synchronous file I/O on the fire path."""
    row_data = [ts_utc, f"{elapsed:.2f}"]
    for key in TICKERS.keys():
        row_data.extend([
            curr_prices[key]["yes_ask"], curr_prices[key]["yes_bid"],
            curr_prices[key]["no_ask"],  curr_prices[key]["no_bid"]
        ])
    row_data.append(signal)
    queue_csv_row(tuple(row_data))

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
        qprint(f"  🔴 GLOBAL SEQ GAP: expected {last_global_seq+1}, got {seq} "
               f"(missed {gap}) — queuing REST resync for ALL markets")
        resync_needed.update(TICKERS.values())   # real market tickers
        last_global_seq = seq
        return False

    return True

async def request_resync_via_rest(session):
    if not resync_needed:
        return

    for ticker in list(resync_needed):
        leg_key = TICKERS_REVERSE.get(ticker, ticker)
        qprint(f"  🔄 REST resync for [{leg_key}]...")
        try:
            url     = f"{BASE_REST}/trade-api/v2/markets/{ticker}/orderbook?depth=100"
            headers = generate_auth_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook")
            async with session.get(url, headers=headers) as resp:
                data = _jloads(await resp.read())
                book = data.get("orderbook_fp") or data.get("orderbook", {})
                if book:
                    apply_snapshot(leg_key, {
                        "yes_dollars_fp": book.get("yes_dollars", []),
                        "no_dollars_fp":  book.get("no_dollars",  []),
                    })
                    update_derived_prices(leg_key)
                    resync_needed.discard(ticker)
                    last_seq.pop(ticker, None)
                    qprint(f"  ✅ [{leg_key}] REST resync complete")
                else:
                    qprint(f"  ⚠️  [{leg_key}] REST resync returned empty book: {data}")
        except Exception as e:
            qprint(f"  ❌ [{leg_key}] REST resync failed: {e}")

# ============================================================
# ORDERBOOK STATE  (incremental best-bid tracking)
# ============================================================
def apply_snapshot(leg_key, msg):
    for side in ("yes", "no"):
        bk = {
            int(round(float(lvl[0]) * 100)): float(lvl[1])
            for lvl in msg.get(f"{side}_dollars_fp", [])
        }
        orderbooks[leg_key][side] = bk
        best_cents[leg_key][side] = max(bk) if bk else None

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
        if best_cents[leg_key][side] == price_cents:            # best removed → rescan
            best_cents[leg_key][side] = max(book) if book else None
    else:
        book[price_cents] = new_qty
        b = best_cents[leg_key][side]
        if b is None or price_cents > b:                        # new best → O(1)
            best_cents[leg_key][side] = price_cents

def best_bid(leg_key, side):
    return best_cents[leg_key][side]

def update_derived_prices(leg_key):
    prev_prices[leg_key] = curr_prices[leg_key].copy()
    best_yes_cents = best_cents[leg_key]["yes"]
    best_no_cents  = best_cents[leg_key]["no"]

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
# LEG ORDERING  (silent — caller prints AFTER leg 1 is in flight)
# ============================================================
def get_leg_order_by_stability(side):
    changes = {}
    for key in TICKERS.keys():
        c = curr_prices[key].get(f"{side}_ask")
        p = prev_prices[key].get(f"{side}_ask")
        changes[key] = abs(c - p) if (c is not None and p is not None) else 0
    ordered = sorted(TICKERS.keys(), key=lambda k: changes[k])
    return ordered, changes

# ============================================================
# ORDER EXECUTION  (V2 API)
#   Success  = HTTP 201 AND fill_count > 0
#   Order ID = res["order_id"]
# ============================================================
def make_payload(leg_key: str, side: str, ask_price: float, cents_buffer: int) -> dict:
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

async def place_arb_orders(session, side, signal_lines=None, detect_t=None):
    """FIRE PATH. Everything before the leg-1 POST is in-memory only:
    payloads pre-serialized, headers pre-signed, output deferred to the
    writer thread. Leg 2 is pre-built but sent ONLY if leg 1 fills —
    the sequential no-unhedged-leg guarantee is unchanged.

    (Pre-building leg 2 is safe: the WS read loop is blocked while this
    coroutine runs, so curr_prices cannot change between here and where
    v3 used to read them after leg 1.)"""
    global is_trading, executed
    is_trading = True
    if detect_t is None:
        detect_t = time.perf_counter()

    url = f"{BASE_REST}{PATH_V2_ORDER}"

    # ── Build EVERYTHING before touching the network ─────────
    legs, changes = get_leg_order_by_stability(side)
    leg1, leg2 = legs[0], legs[1]
    ask_price1 = curr_prices[leg1][f"{side}_ask"]
    ask_price2 = curr_prices[leg2][f"{side}_ask"]
    payload1   = make_payload(leg1, side, ask_price1, cents_buffer=4)
    payload2   = make_payload(leg2, side, ask_price2, cents_buffer=10)
    body1      = _jdumps(payload1)
    body2      = _jdumps(payload2)          # sent ONLY if leg 1 fills
    headers    = post_order_headers()       # cached — no RSA here

    # ── Leg 1: Anchor ────────────────────────────────────────
    t_send1 = time.perf_counter()
    try:
        async with session.post(url, data=body1, headers=headers) as resp:
            raw    = await resp.read()
            t_resp1 = time.perf_counter()
            res    = _jloads(raw)
            filled = is_filled(resp.status, res)
    except Exception as e:
        qprint(f"  ERROR Leg 1: {e}")
        is_trading = False
        return False

    # Now that leg 1's fate is decided, emit the deferred output
    # (identical content to v3, just written after the wire, not before).
    if signal_lines:
        for line in signal_lines:
            qprint(line)
    order_str = " → ".join([f"{k}(Δ{changes[k]:.3f})" for k in legs])
    qprint(f"  Leg order (stale→volatile): {order_str}")
    qprint(f"  ⏱ LEG_TIMING leg1 detect→wire={round((t_send1-detect_t)*1000,2)}ms "
           f"rtt={round((t_resp1-t_send1)*1000,2)}ms")

    if not filled:
        qprint(f"    ⚠️ Leg 1 ({leg1}) missed. Safe abort. {res}")

        # Optimistic local deletion: remove the stale resting liquidity
        # we just failed to take, so we don't loop on the same signal.
        price_cents = int(round(float(payload1["price"]) * 100))
        if side == "yes":
            # bid: we targeted NO bids at the complement price
            book = orderbooks[leg1]["no"]
            book.pop(100 - price_cents, None)
            if best_cents[leg1]["no"] == 100 - price_cents:
                best_cents[leg1]["no"] = max(book) if book else None
        else:
            # ask: we targeted YES bids; remove the best YES bid level
            best_yes_c = best_cents[leg1]["yes"]
            if best_yes_c is not None:
                orderbooks[leg1]["yes"].pop(best_yes_c, None)
                bk = orderbooks[leg1]["yes"]
                best_cents[leg1]["yes"] = max(bk) if bk else None
        update_derived_prices(leg1)

        is_trading = False
        return False

    executed = True
    qprint(
        f"    ✅ Leg 1 ({leg1}) FILLED {res.get('fill_count')} contracts "
        f"@ avg ${res.get('average_fill_price', '?')} "
        f"| id={res.get('order_id')}"
    )

    # ── Leg 2: Aggressive Sweep (ONLY runs because leg 1 filled) ──
    t_send2 = time.perf_counter()
    try:
        async with session.post(url, data=body2, headers=headers) as resp:
            raw    = await resp.read()
            t_resp2 = time.perf_counter()
            res    = _jloads(raw)
            filled = is_filled(resp.status, res)
            qprint(f"  ⏱ LEG_TIMING leg2 gap={round((t_send2-t_resp1)*1000,2)}ms "
                   f"rtt={round((t_resp2-t_send2)*1000,2)}ms")
            if filled:
                qprint(
                    f"    ✅ Leg 2 ({leg2}) FILLED {res.get('fill_count')} contracts "
                    f"@ avg ${res.get('average_fill_price', '?')} "
                    f"| id={res.get('order_id')}"
                )
                qprint("  🎉 Arbitrage fully executed!")
            else:
                qprint(f"    🚨 TOXIC LEG: Leg 2 ({leg2}) missed. {res}")
                qprint(f"    🚨 YOU HOLD {leg1} {side.upper()} — manual close may be needed")
    except Exception as e:
        qprint(f"  🚨 CRITICAL Leg 2 error: {e}")
        qprint(f"  🚨 YOU HOLD {leg1} {side.upper()} — check portfolio")
        filled = False

    await asyncio.sleep(0.5)
    is_trading = False
    return filled

# ============================================================
# ARB CHECK
# ============================================================
async def check_arbitrage(session, detect_t=None):
    global is_trading
    global executed
    if is_trading:
        return
    if resync_needed:
        return
    if executed:
        return

    is_trading = True

    p1, p2 = list(TICKERS.keys())[0], list(TICKERS.keys())[1]

    try:
        y1 = curr_prices[p1].get("yes_ask")
        y2 = curr_prices[p2].get("yes_ask")
        if y1 is not None and y2 is not None:
            sum_yes = y1 + y2
            if sum_yes < ARBITRAGE_THRESHOLD:
                # Timestamp strings + CSV row are built here (signal only,
                # not every tick) and the prints are DEFERRED into
                # place_arb_orders, which emits them after leg 1 is in flight.
                now     = datetime.now(timezone.utc)
                ts_utc  = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                elapsed = time.time() - start_time
                signal  = f"ARBITRAGE_YES_ASK: {sum_yes:.4f}"
                signal_lines = [
                    f"\n  🚨 [{ts_utc}] {signal}",
                    f"  {p1} YES {y1:.2f} + {p2} YES {y2:.2f} = {sum_yes:.4f}",
                ]
                log_row(ts_utc, elapsed, signal)
                is_trading = False
                success = await place_arb_orders(session, "yes",
                                                 signal_lines=signal_lines,
                                                 detect_t=detect_t)
                if not success:
                    qprint("  Arb aborted")
                return

        n1 = curr_prices[p1].get("no_ask")
        n2 = curr_prices[p2].get("no_ask")
        if n1 is not None and n2 is not None:
            sum_no = n1 + n2
            if sum_no < ARBITRAGE_THRESHOLD:
                now     = datetime.now(timezone.utc)
                ts_utc  = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                elapsed = time.time() - start_time
                signal  = f"ARBITRAGE_NO_ASK: {sum_no:.4f}"
                signal_lines = [
                    f"\n  🚨 [{ts_utc}] {signal}",
                    f"  {p1} NO {n1:.2f} + {p2} NO {n2:.2f} = {sum_no:.4f}",
                ]
                log_row(ts_utc, elapsed, signal)
                is_trading = False
                success = await place_arb_orders(session, "no",
                                                 signal_lines=signal_lines,
                                                 detect_t=detect_t)
                if not success:
                    qprint("  Arb aborted")
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

    qprint(f"{ts}{resync_flag}  | "
           f"{p1}_Y: {fmt(p1_y):<6} {p1}_N: {fmt(p1_n):<6} | "
           f"{p2}_Y: {fmt(p2_y):<6} {p2}_N: {fmt(p2_n):<6} | "
           f"SUM_Y: {sum_y:<5} SUM_N: {sum_n:<5}")

# ============================================================
# WS MESSAGE HANDLING
# ============================================================
async def process_ws_message(session, ws, msg_data):
    global tick_count
    detect_t = time.perf_counter()      # message-processing start (for LEG_TIMING)
    msg_type = msg_data.get("type")
    msg      = msg_data.get("msg", {})
    ticker   = msg.get("market_ticker")
    seq      = msg_data.get("seq")

    if msg_type == "subscribed":
        qprint(f"  ✅ Subscribed: {msg_data}")
        return
    if msg_type == "error":
        qprint(f"  ❌ WS error: {msg_data}")
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
            qprint(f"\n🔍 RAW SNAPSHOT ({leg_key}) seq={seq}: {msg}\n")
        apply_snapshot(leg_key, msg)
        update_derived_prices(leg_key)
        tick_count += 1
        await check_arbitrage(session, detect_t=detect_t)
        print_live_odds()

    elif msg_type == "orderbook_delta":
        apply_delta(leg_key, msg)
        update_derived_prices(leg_key)
        tick_count += 1
        await check_arbitrage(session, detect_t=detect_t)
        print_live_odds()

# ============================================================
# CONNECTION WARMUP / KEEPALIVE
# ============================================================
async def _warm(session):
    try:
        async with session.get(f"{BASE_REST}{STATUS_PATH}",
                               headers=generate_auth_headers("GET", STATUS_PATH),
                               timeout=aiohttp.ClientTimeout(total=3)) as r:
            await r.read()
    except Exception:
        pass

async def warm_connections(session):
    """TLS-handshake two pooled sockets before trading. Two, so a keepalive
    ping in flight can never leave the fire path without a warm socket."""
    await asyncio.gather(_warm(session), _warm(session))
    qprint("  🔥 Connections warmed (2 pooled sockets)")

async def connection_keepalive(session):
    """The single biggest v3 problem for a rarely-firing bot: the socket
    idles out between signals and leg 1 pays a fresh TCP+TLS handshake.
    Ping every KEEPALIVE_SECS so it never does."""
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(KEEPALIVE_SECS)
        try:
            hdr = await loop.run_in_executor(None, _sign, "GET", STATUS_PATH)
            try:
                async with session.get(f"{BASE_REST}{STATUS_PATH}", headers=hdr,
                                       timeout=aiohttp.ClientTimeout(total=3)) as r:
                    await r.read()
            except Exception:
                pass
        except Exception:
            pass

# ============================================================
# MAIN
# ============================================================
async def monitor_arbitrage():
    init_log()

    qprint("=" * 100)
    qprint(f"  🚀 Kalshi Arb Monitor v4  (V2 orders: {PATH_V2_ORDER})")
    qprint(f"  {GAME_DESCRIPTION} | Threshold: < {ARBITRAGE_THRESHOLD:.2f}")
    qprint(f"  Fire path: pre-signed headers, pre-built payloads, warm sockets, deferred I/O")
    qprint(f"  Leg 2 fires ONLY after leg 1 confirms filled (no unhedged leg)")
    qprint("=" * 100)

    ws_headers = generate_auth_headers("GET", "/trade-api/ws/v2")
    connector  = aiohttp.TCPConnector(limit=0, keepalive_timeout=60,
                                      ttl_dns_cache=300, enable_cleanup_closed=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        await warm_connections(session)
        refresher_task = asyncio.create_task(signature_refresher())
        keepalive_task = asyncio.create_task(connection_keepalive(session))

        try:
            while True:
                try:
                    async with websockets.connect(
                        WS_URL, additional_headers=ws_headers,
                        ping_interval=15, compression=None, max_queue=1024
                    ) as ws:
                        qprint(f"✅ Connected: {WS_URL}\n")

                        last_seq.clear()
                        resync_needed.clear()
                        for k in TICKERS:
                            orderbooks[k]  = {"yes": {}, "no": {}}
                            best_cents[k]  = {"yes": None, "no": None}
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
                            await process_ws_message(session, ws, _jloads(msg))

                except Exception as e:
                    qprint(f"\n❌ WebSocket error: {e}. Retrying in 3s...")
                    await asyncio.sleep(3)
        finally:
            refresher_task.cancel()
            keepalive_task.cancel()


if __name__ == "__main__":
    try:
        import uvloop                      # POSIX
        uvloop.install()
    except ImportError:
        try:
            import winloop                 # Windows
            winloop.install()
        except ImportError:
            pass
    try:
        asyncio.run(monitor_arbitrage())
    except KeyboardInterrupt:
        qprint("\n🔴 Shut down manually.")
    except Exception as e:
        qprint(f"\n❌ Fatal error: {e}")
    finally:
        flush_output()
