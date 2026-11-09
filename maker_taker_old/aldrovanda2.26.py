"""
ROUNDED TARGET BIDS
PARTIAL FILL HEDGING IMPLEMENTED, LOW VOLUME, IMPROVED TAKER FIRING SPEED, TRACKING TICKS AND PRICE DETLAS AT DIFFERENT TIMEFRAMES
WORKS WELL WITH 0.03 spreads and 0.97 threshold
Kalshi Maker-Taker Arbitrage: FER vs CER

TAKER LATENCY IMPROVEMENTS
───────────────────────────
1. Taker fires BEFORE cancel (taker request is written to the wire first;
   the cancel now runs CONCURRENTLY with the hedge round-trip instead of
   after it — the cancel is cleanup, its latency doesn't affect P&L).

2. Dedicated aiohttp session for takers (separate from maker management).
   Avoids the taker HTTP request queuing behind a concurrent cancel call
   on a shared connector.

3. PRE-SIGNED AUTH HEADERS. RSA-PSS signing (~1-3 ms, and it blocks the
   event loop) is now done in a background thread every SIG_REFRESH_SECS:
   - POST /orders headers (used by BOTH takers and maker placements)
   - DELETE headers for every currently-resting maker order
   At fill time the hedge grabs a ready-made signature: zero signing cost
   on the hot path. Falls back to synchronous signing if the cache is
   stale (> SIG_MAX_AGE_SECS old). NOTE: Kalshi rejects stale timestamps;
   keep SIG_MAX_AGE_SECS well inside the server tolerance.

4. Pre-warmed persistent connections on BOTH sessions (the maker session
   was previously cold — its first cancel paid a full TCP+TLS handshake,
   which is exactly the pick-off race we keep losing). A background
   keepalive ping every KEEPALIVE_SECS stops the sockets from idling out,
   so the hedge/cancel never pays a reconnect during a quiet stretch.

5. NON-BLOCKING CONSOLE OUTPUT. Every print/log line is queued and
   written by a dedicated thread. Windows console writes can block the
   event loop for milliseconds per line; the per-tick odds line was being
   paid on the same loop iteration that processes orderbook deltas.
   Output text/format is byte-identical to before (the log parser keeps
   working) — it just can't stall the trading loop anymore.

6. Taker payload is pre-serialized to bytes at build time; the request is
   sent with data= (no json.dumps inside aiohttp on the hot path).

7. Incremental best-price tracking: best bid per book is maintained O(1)
   per delta instead of max() over the whole price map on every message.

8. orjson (if installed) for WS parsing / payload serialization, and
   uvloop (POSIX) or winloop (Windows) event loop if installed. All
   optional — the script runs unchanged on stock CPython.

9. WS compression disabled (permessage-deflate costs CPU on every frame)
   and a cheap "action needed?" pre-check before spawning the maker
   re-quote task, so quiet ticks don't create pointless tasks.

FILL DETECTION
──────────────
Primary:   user_orders WS channel.
Secondary: 404-age inference on cancel calls.
Partial fills hedged immediately at exact incremental quantity.

MARKET FREEZE
─────────────
Big tick (≥ FREEZE_TICK_THRESHOLD) → cancel all resting makers,
wait CALM_DURATION seconds of calm before resuming.
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
from collections import deque
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

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
KALSHI_KEY_ID    = 'DUMMY_KEY_ID'
PRIVATE_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'

BASE_REST  = "https://external-api.kalshi.com"
WS_URL     = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
ORDER_PATH = "/trade-api/v2/portfolio/events/orders"
ORDER_URL  = f"{BASE_REST}{ORDER_PATH}"
STATUS_PATH = "/trade-api/v2/exchange/status"

TICKERS = {
    "FER": "KXITFMATCH-26JUL07TAINIK-TAI",
    "CER": "KXITFMATCH-26JUL07TAINIK-NIK"
}
TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}
GAME_DESCRIPTION = "Ferrer vs Cerundolo (ATP)"

TRADE_QUANTITY          = 1
BID_SPREAD              = 0.03
ASK_SPREAD              = 0.03
THRESHOLD               = 0.97
TAKER_BUFFER            = 0.10
UPDATE_TICK             = 0.01
MIN_ORDER_AGE_FOR_FILL  = 0.1
FREEZE_TICK_THRESHOLD   = 0.10
CALM_DURATION           = 2.0
LOG_FILE                = "maker_taker_log.csv"

SIG_REFRESH_SECS        = 2.0    # background re-sign cadence
SIG_MAX_AGE_SECS        = 10.0   # cached sig older than this → sign fresh
KEEPALIVE_SECS          = 20.0   # connection keep-warm ping cadence
# ═══════════════════════════════════════════════════════════

with open(PRIVATE_KEY_PATH, "rb") as _fh:
    PRIVATE_KEY = serialization.load_pem_private_key(_fh.read(), password=None)

# ── Precomputed round() targets (hot-path comparisons) ──────
_R_THRESHOLD   = round(THRESHOLD, 2)
_R_INV_THRESH  = round(1.0 - THRESHOLD, 2)
_R_UPDATE_TICK = round(UPDATE_TICK, 2)

# ── Price + orderbook state ─────────────────────────────────
curr_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None} for k in TICKERS}
prev_prices = {k: {"yes_ask": None, "yes_bid": None,
                    "no_ask":  None, "no_bid":  None} for k in TICKERS}
orderbooks  = {k: {"yes": {}, "no": {}} for k in TICKERS}
best_cents  = {k: {"yes": None, "no": None} for k in TICKERS}   # incremental best bid (¢)
last_seq:    dict[str, int] = {}
resync_needed: set[str]     = set()

# ── Maker-order tracking ─────────────────────────────────────
bid_order_id:    str   | None = None
bid_price:       float | None = None
bid_placed_at:   float | None = None
bid_fill_hedged: float        = 0.0

ask_order_id:    str   | None = None
ask_price:       float | None = None
ask_placed_at:   float | None = None
ask_fill_hedged: float        = 0.0

cancelled_order_ids: set[str] = set()

# ── Safety flags ─────────────────────────────────────────────
maker_filled:    bool = False
updating_orders: bool = False
hedge_executed:  bool = False

# ── Freeze state ─────────────────────────────────────────────
market_frozen:             bool  = False
last_big_move_at:          float = 0.0
freeze_cancel_in_progress: bool  = False

# ── Speed & Price Change Tracking ────────────────────────────
tick_timestamps: deque = deque()
price_history:   deque = deque()  # stores (timestamp, copy_of_curr_prices)

# ── Two separate aiohttp sessions ────────────────────────────
_maker_session: aiohttp.ClientSession | None = None
_taker_session: aiohttp.ClientSession | None = None

# ════════════════════════════════════════════════════════════
# NON-BLOCKING CONSOLE / LOG WRITER
# All prints go through a queue drained by a dedicated thread,
# so slow console I/O can never stall the event loop. Output
# text and ordering are identical to the direct-print version.
# ════════════════════════════════════════════════════════════
_out_q: _queue.SimpleQueue = _queue.SimpleQueue()
_WRITER_SENTINEL = object()

def _writer_loop():
    write = sys.stdout.write
    flush = sys.stdout.flush
    while True:
        item = _out_q.get()
        if item is _WRITER_SENTINEL:
            flush()
            return
        try:
            buf = [item]
            # batch whatever else is already queued into one write+flush
            while True:
                try:
                    nxt = _out_q.get_nowait()
                except _queue.Empty:
                    break
                if nxt is _WRITER_SENTINEL:
                    write("".join(buf)); flush()
                    return
                buf.append(nxt)
            write("".join(buf))
            flush()
        except Exception:
            pass

_writer_thread = threading.Thread(target=_writer_loop, daemon=True, name="console-writer")
_writer_thread.start()

def qprint(text: str = ""):
    _out_q.put(text + "\n")

def flush_output(timeout: float = 2.0):
    """Best-effort drain of the output queue (used at shutdown)."""
    deadline = time.time() + timeout
    while not _out_q.empty() and time.time() < deadline:
        time.sleep(0.01)

# ════════════════════════════════════════════════════════════
# AUTH  (RSA-PSS)  — with background pre-signing
# ════════════════════════════════════════════════════════════
def _sign(method: str, path: str) -> dict:
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

def auth(method: str, path: str) -> dict:
    """Synchronous signing — cold paths only (WS connect, resync, warmup)."""
    return _sign(method, path)

# Pre-signed header caches, refreshed off the event loop.
_post_hdr:       dict  | None = None
_post_hdr_at:    float        = 0.0
_del_hdr_cache:  dict[str, tuple[dict, float]] = {}

def post_order_headers() -> dict:
    """Ready-made POST /orders headers (takers AND maker placements)."""
    if _post_hdr is not None and (time.time() - _post_hdr_at) <= SIG_MAX_AGE_SECS:
        return _post_hdr
    return _sign("POST", ORDER_PATH)          # cold fallback

def delete_order_headers(order_id: str) -> dict:
    ent = _del_hdr_cache.get(order_id)
    if ent is not None and (time.time() - ent[1]) <= SIG_MAX_AGE_SECS:
        return ent[0]
    return _sign("DELETE", f"{ORDER_PATH}/{order_id}")   # cold fallback

def _presign_delete_sync(order_id: str):
    _del_hdr_cache[order_id] = (_sign("DELETE", f"{ORDER_PATH}/{order_id}"), time.time())

async def presign_delete(order_id: str):
    """Sign a DELETE header for a freshly-placed order without blocking the loop."""
    try:
        await asyncio.get_running_loop().run_in_executor(None, _presign_delete_sync, order_id)
    except Exception:
        pass

async def signature_refresher():
    """Keeps POST headers and DELETE headers for resting makers fresh."""
    global _post_hdr, _post_hdr_at
    loop = asyncio.get_running_loop()
    while True:
        try:
            hdr = await loop.run_in_executor(None, _sign, "POST", ORDER_PATH)
            _post_hdr, _post_hdr_at = hdr, time.time()
            for oid in (bid_order_id, ask_order_id):
                if oid:
                    await loop.run_in_executor(None, _presign_delete_sync, oid)
            live = {bid_order_id, ask_order_id}
            for k in list(_del_hdr_cache):
                if k not in live:
                    _del_hdr_cache.pop(k, None)
        except Exception:
            pass
        await asyncio.sleep(SIG_REFRESH_SECS)

# ════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════
def init_log():
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow(["timestamp_utc", "event", "details"])

def log(event: str, details: str = ""):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    qprint(f"  📝 [{ts}] {event}  {details}")

# ════════════════════════════════════════════════════════════
# SEQUENCE CHECKING
# ════════════════════════════════════════════════════════════
last_global_seq: int = -1

def check_sequence(seq: int, ticker: str, msg_type: str) -> bool:
    global last_global_seq
    if msg_type == "orderbook_snapshot":
        if seq > last_global_seq: last_global_seq = seq
        return True
    if msg_type == "orderbook_delta":
        if last_global_seq == -1:
            last_global_seq = seq; return True
        if seq <= last_global_seq: return True
        if seq == last_global_seq + 1:
            last_global_seq = seq; return True
        qprint(f"  🔴 SEQ GAP: {seq - last_global_seq - 1} missed — queuing resync")
        resync_needed.update(TICKERS.keys())
        last_global_seq = seq
        return False
    return True

async def request_resync(session):
    for ticker in list(resync_needed):
        leg = TICKERS_REVERSE.get(ticker, ticker)
        try:
            url  = f"{BASE_REST}/trade-api/v2/markets/{ticker}/orderbook?depth=100"
            async with session.get(url, headers=auth("GET", f"/trade-api/v2/markets/{ticker}/orderbook")) as resp:
                data = _jloads(await resp.read())
                book = data.get("orderbook_fp") or data.get("orderbook", {})
                if book:
                    apply_snapshot(leg, {"yes_dollars_fp": book.get("yes_dollars", []),
                                         "no_dollars_fp":  book.get("no_dollars",  [])})
                    update_derived(leg)
                    resync_needed.discard(ticker)
                    qprint(f"  ✅ [{leg}] resync complete")
        except Exception as e:
            qprint(f"  ❌ [{leg}] resync failed: {e}")

# ════════════════════════════════════════════════════════════
# ORDERBOOK  (incremental best-price tracking)
# ════════════════════════════════════════════════════════════
def apply_snapshot(leg, msg):
    for side in ("yes", "no"):
        bk = {
            int(round(float(x[0]) * 100)): float(x[1])
            for x in msg.get(f"{side}_dollars_fp", [])
        }
        orderbooks[leg][side] = bk
        best_cents[leg][side] = max(bk) if bk else None

def apply_delta(leg, msg):
    side = msg.get("side"); price = msg.get("price_dollars"); delta = msg.get("delta_fp")
    if side not in ("yes", "no") or price is None or delta is None: return
    c  = int(round(float(price) * 100))
    bk = orderbooks[leg][side]
    nq = bk.get(c, 0.0) + float(delta)
    if nq <= 0.0001:
        bk.pop(c, None)
        if best_cents[leg][side] == c:                 # best level removed → rescan
            best_cents[leg][side] = max(bk) if bk else None
    else:
        bk[c] = nq
        b = best_cents[leg][side]
        if b is None or c > b:                          # new best → O(1)
            best_cents[leg][side] = c

def update_derived(leg):
    prev_prices[leg] = curr_prices[leg].copy()
    yb_c = best_cents[leg]["yes"]; nb_c = best_cents[leg]["no"]
    yb = yb_c / 100.0 if yb_c is not None else None
    nb = nb_c / 100.0 if nb_c is not None else None
    p = curr_prices[leg]
    p["yes_bid"] = round(yb,       4) if yb is not None else None
    p["no_ask"]  = round(1.0 - yb, 4) if yb is not None else None
    p["no_bid"]  = round(nb,       4) if nb is not None else None
    p["yes_ask"] = round(1.0 - nb, 4) if nb is not None else None

# ════════════════════════════════════════════════════════════
# MARKET SPEED / FREEZE
# ════════════════════════════════════════════════════════════
def check_market_speed() -> bool:
    global market_frozen, last_big_move_at
    max_move = 0.0
    for leg in TICKERS:
        cp = curr_prices[leg]; pp = prev_prices[leg]
        for field in ("yes_ask", "no_ask", "yes_bid", "no_bid"):
            c = cp.get(field); p = pp.get(field)
            if c is not None and p is not None:
                d = c - p
                if d < 0: d = -d
                if d > max_move: max_move = d
    now = time.time()
    if max_move >= FREEZE_TICK_THRESHOLD:
        last_big_move_at = now
        if not market_frozen:
            market_frozen = True
            qprint(f"  🧊 MARKET FROZEN  (tick Δ={max_move:.3f} ≥ {FREEZE_TICK_THRESHOLD})")
            log("MARKET_FROZEN", f"tick_move={max_move:.3f}")
            return True
        return False
    if market_frozen and (now - last_big_move_at) >= CALM_DURATION:
        market_frozen = False
        qprint(f"  ✅ Market calm for {now - last_big_move_at:.1f}s — resuming makers")
        log("MARKET_UNFROZEN", f"calm_for={now - last_big_move_at:.1f}s")
    return False

# ════════════════════════════════════════════════════════════
# REST ORDER HELPERS
# ════════════════════════════════════════════════════════════
async def place_maker_order(session, team: str, v2_side: str, price: float) -> str | None:
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
        async with session.post(ORDER_URL, data=_jdumps(payload),
                                headers=post_order_headers()) as resp:
            res      = _jloads(await resp.read())
            order_id = res.get("order_id")
            if resp.status in (200, 201) and order_id:
                qprint(f"  📌 MAKER {v2_side.upper()} on {team}_Y @ ${price:.2f} | id={order_id}")
                log("MAKER_PLACED", f"side={v2_side} team={team} price={price} id={order_id}")
                # Pre-sign this order's DELETE header so a future cancel is instant.
                asyncio.create_task(presign_delete(order_id))
                return order_id
            qprint(f"  ⚠️  Maker place failed ({v2_side}): {res}")
            log("MAKER_PLACE_FAIL", str(res))
            return None
    except Exception as e:
        qprint(f"  ❌ Maker order error: {e}")
        return None

async def cancel_order(session, order_id: str) -> str:
    try:
        async with session.delete(f"{BASE_REST}{ORDER_PATH}/{order_id}",
                                  headers=delete_order_headers(order_id)) as resp:
            body = await resp.text()
            if resp.status in (200, 204):
                cancelled_order_ids.add(order_id)
                qprint(f"  🗑  Cancelled {order_id[:12]}… ({resp.status})")
                log("ORDER_CANCELLED", order_id)
                return "confirmed"
            elif resp.status == 404:
                qprint(f"  ⚠️  Cancel 404 for {order_id[:12]}…")
                log("CANCEL_404_UNCONFIRMED", f"order={order_id}")
                return "not_found"
            else:
                qprint(f"  ⚠️  Cancel {order_id[:12]}… status={resp.status}: {body[:80]}")
                log("CANCEL_ERROR", f"order={order_id} status={resp.status}")
                return "error"
    except Exception as e:
        qprint(f"  ❌ Cancel error: {e}")
        return "error"

def _build_taker_payload(team: str, v2_side: str, price: float,
                          quantity: float) -> tuple[bytes, dict]:
    """Payload is serialized to bytes here; headers come from the pre-signed
    cache — the hot path does no RSA signing and no json.dumps."""
    price = round(min(0.99, max(0.01, price)), 4)
    payload = {
        "ticker":                     TICKERS[team],
        "client_order_id":            str(uuid.uuid4()),
        "side":                       v2_side,
        "count":                      f"{quantity:.2f}",
        "price":                      f"{price:.4f}",
        "time_in_force":              "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
    }
    return _jdumps(payload), post_order_headers()

async def fire_taker(body: bytes, headers: dict) -> tuple[bool, dict]:
    try:
        async with _taker_session.post(ORDER_URL, data=body, headers=headers) as resp:
            res    = _jloads(await resp.read())
            filled = resp.status == 201 and float(res.get("fill_count", "0")) > 0
            return filled, res
    except Exception as e:
        return False, {"error": str(e)}

async def cancel_all_makers(session):
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    for oid in filter(None, [bid_order_id, ask_order_id]):
        await cancel_order(session, oid)
    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0

async def cancel_all_makers_frozen(session):
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    global freeze_cancel_in_progress

    if freeze_cancel_in_progress:
        return
    freeze_cancel_in_progress = True

    try:
        legs = [
            ("bid", bid_order_id, bid_price, bid_placed_at, bid_fill_hedged),
            ("ask", ask_order_id, ask_price, ask_placed_at, ask_fill_hedged),
        ]
        for side, oid, px, placed_at, already_hedged in legs:
            if oid is None or maker_filled:
                continue
            result = await cancel_order(session, oid)
            if maker_filled:
                return
            if result == "confirmed":
                if side == "bid":
                    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
                else:
                    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0
            elif result == "not_found":
                age = time.time() - (placed_at or time.time())
                if age >= MIN_ORDER_AGE_FOR_FILL:
                    remaining_qty = max(0.0, TRADE_QUANTITY - already_hedged)
                    log("FILL_INFERRED_404_FROZEN", f"{side} id={oid} age={age:.3f}s qty={remaining_qty:.4f}")
                    await on_maker_filled(session, side, px or 0, oid, quantity=remaining_qty)
                    return
                else:
                    log("CANCEL_404_TOO_FRESH_FROZEN", f"{side} id={oid} age={age:.3f}s — leaving tracked")
    finally:
        freeze_cancel_in_progress = False

# ════════════════════════════════════════════════════════════
# PARTIAL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_partial_fill(maker_session, filled_side: str, fill_price: float, quantity: float):
    # Build + fire the hedge FIRST; all console output happens after the
    # request is on the wire. Same lines, same order — just later.
    if filled_side == "bid":
        cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
        team, v2_side = "CER", "bid"
    else:
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
        team, v2_side = "CER", "ask"

    body, headers = _build_taker_payload(team, v2_side, taker_px, quantity)
    taker_task = asyncio.create_task(fire_taker(body, headers))
    await asyncio.sleep(0)   # let the taker task run to its first I/O await → request written

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    qprint(f"\n  ⚡ [{ts}] PARTIAL FILL: {filled_side.upper()} @ ${fill_price:.4f}  qty={quantity:.4f}")
    log("PARTIAL_FILL", f"side={filled_side} price={fill_price} qty={quantity:.4f}")
    if filled_side == "bid":
        qprint(f"  🎯 Partial Hedge: BUY {quantity:.4f}× CER_YES  mkt={cer_y:.4f}  limit=${taker_px:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY CER_YES qty={quantity:.4f} limit={taker_px}")
    else:
        qprint(f"  🎯 Partial Hedge: BUY {quantity:.4f}× CER_NO  mkt={cer_n:.4f}  ask limit=${taker_px:.4f}")
        log("PARTIAL_HEDGE_ATTEMPT", f"BUY CER_NO qty={quantity:.4f} ask@{taker_px}")

    filled, res = await taker_task

    if filled:
        qty_filled = float(res.get("fill_count", str(quantity)))
        avg        = float(res.get("average_fill_price", taker_px))
        qprint(f"  ✅ Partial Hedge FILLED  qty={qty_filled:.4f} @ ${avg:.4f}")
        log("PARTIAL_HEDGE_FILLED", f"qty={qty_filled:.4f} avg={avg}")
    else:
        qprint(f"  🚨 PARTIAL HEDGE FAILED → {res}")
        log("PARTIAL_HEDGE_MISSED", f"side={filled_side} qty={quantity:.4f}: {res}")

# ════════════════════════════════════════════════════════════
# FULL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_maker_filled(maker_session, filled_side: str, fill_price: float, order_id: str,
                          quantity: float | None = None):
    global maker_filled, hedge_executed
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged

    if maker_filled:
        return

    hedge_qty = TRADE_QUANTITY if quantity is None else quantity

    maker_filled = True
    hedge_executed = False

    other_id = ask_order_id if filled_side == "bid" else bid_order_id
    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0

    # ── Build + fire the hedge BEFORE any console output ──────
    taker_task = None
    taker_px   = None
    cer_y = cer_n = None
    if hedge_qty > 0.001:
        if filled_side == "bid":
            cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
            taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
            body, headers = _build_taker_payload("CER", "bid", taker_px, hedge_qty)
        else:
            cer_n    = curr_prices["CER"].get("no_ask") or 0.99
            taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
            body, headers = _build_taker_payload("CER", "ask", taker_px, hedge_qty)
        taker_task = asyncio.create_task(fire_taker(body, headers))
        await asyncio.sleep(0)   # taker request hits the wire before anything else runs

    # ── Cancel of the other maker overlaps the hedge round-trip ──
    # (taker was written first; the cancel is cleanup and runs concurrently
    # on the separate maker session instead of waiting for the hedge reply)
    cancel_task = None
    if other_id:
        cancel_task = asyncio.create_task(cancel_order(maker_session, other_id))

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    qprint(f"\n{'='*70}")
    qprint(f"  🔥 [{ts}] MAKER FILLED: {filled_side.upper()} @ ${fill_price:.4f}  hedge_qty={hedge_qty:.4f}")
    qprint(f"  id={order_id}")
    log("MAKER_FILLED", f"side={filled_side} fill_price={fill_price} hedge_qty={hedge_qty:.4f} id={order_id}")

    if hedge_qty <= 0.001:
        qprint(f"  ✅ All quantity already hedged via prior partial fills.")
        log("HEDGE_SKIPPED", "all qty hedged via partial fills")
        filled = False; res = {}
    else:
        if filled_side == "bid":
            qprint(f"  🎯 Hedge: BUY {hedge_qty:.4f}× CER_YES  mkt={cer_y:.4f}  limit=${taker_px:.4f}")
            log("HEDGE_ATTEMPT", f"BUY CER_YES qty={hedge_qty:.4f} limit={taker_px} mkt={cer_y}")
        else:
            qprint(f"  🎯 Hedge: BUY {hedge_qty:.4f}× CER_NO  mkt={cer_n:.4f}  ask limit=${taker_px:.4f}")
            log("HEDGE_ATTEMPT", f"BUY CER_NO qty={hedge_qty:.4f} ask@{taker_px} mkt={cer_n}")
        if other_id:
            qprint(f"  🗑  Cancelling other maker {other_id[:12]}… (post-hedge)")
        filled, res = await taker_task

    if cancel_task is not None:
        await cancel_task

    if hedge_qty <= 0.001:
        pass
    elif filled_side == "bid":
        if filled:
            avg   = float(res.get("average_fill_price", taker_px))
            total = round(fill_price + avg, 4)
            qprint(f"  ✅ Hedge FILLED @ ${avg:.4f}  total=${total:.4f}")
            qprint(f"  💰 LOCKED PROFIT: $1.00 − ${total:.4f} = ${round(1-total,4):.4f}")
            log("HEDGE_FILLED", f"avg={avg} total={total} profit={round(1-total,4)}")
        else:
            qprint(f"  🚨 HEDGE FAILED: BUY CER_YES @ {taker_px} → {res}")
            qprint(f"  🚨 HOLDING {hedge_qty:.4f}× FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_YES qty={hedge_qty:.4f} limit={taker_px}: {res}")
    else:
        if filled:
            yes_avg = float(res.get("average_fill_price", taker_px))
            no_cost = round(1.0 - yes_avg, 4)
            profit  = round(fill_price - no_cost, 4)
            qprint(f"  ✅ Hedge FILLED: CER_NO cost=${no_cost:.4f}")
            qprint(f"  💰 LOCKED PROFIT: ${fill_price:.4f} − ${no_cost:.4f} = ${profit:.4f}")
            log("HEDGE_FILLED", f"no_cost={no_cost} fer_sold={fill_price} profit={profit}")
        else:
            qprint(f"  🚨 HEDGE FAILED: BUY CER_NO ask @ {taker_px} → {res}")
            qprint(f"  🚨 SHORT {hedge_qty:.4f}× FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_NO qty={hedge_qty:.4f} ask={taker_px}: {res}")

    hedge_executed = True

    # Print the speed metrics requested at the very end of fill completion
    qprint(f"  📊 FILL SPEED STATISTICS: {get_speed_stats_str()}")
    qprint(f"\n  🔒 MAKER MODE LOCKED. Restart to resume.")
    qprint(f"{'='*70}\n")

# ════════════════════════════════════════════════════════════
# WS FILL DETECTION
# ════════════════════════════════════════════════════════════
async def handle_user_order_update(maker_session, msg: dict):
    global bid_fill_hedged, ask_fill_hedged

    if maker_filled:
        return

    order_id = msg.get("order_id", "")
    if order_id in cancelled_order_ids:
        qprint(f"  Cancelled order WS update ignored: {msg.get('status','')} {order_id[:12]}")
        cancelled_order_ids.discard(order_id)
        return

    if   order_id == bid_order_id: filled_side = "bid"
    elif order_id == ask_order_id: filled_side = "ask"
    else:                          return

    status     = msg.get("status", "")
    fill_count = float(msg.get("fill_count_fp",    "0"))
    remaining  = float(msg.get("remaining_count_fp", "1"))

    is_fully_filled = (status == "filled") or (fill_count >= TRADE_QUANTITY and remaining < 0.001)

    if fill_count <= 0:
        return

    already_hedged = bid_fill_hedged if filled_side == "bid" else ask_fill_hedged
    incremental    = round(fill_count - already_hedged, 6)

    if incremental < 0.001:
        if is_fully_filled:
            fill_price = float(msg.get("yes_price_dollars", "0"))
            await on_maker_filled(maker_session, filled_side, fill_price, order_id, quantity=0.0)
        return

    fill_price = float(msg.get("yes_price_dollars", "0"))
    if fill_price == 0:
        cost = float(msg.get("maker_fill_cost_dollars", "0"))
        fill_price = (cost / fill_count) if fill_count > 0 else (bid_price or ask_price or 0)

    if not is_fully_filled:
        log("WS_PARTIAL_FILL", f"side={filled_side} fill={fill_count:.4f} rem={remaining:.4f} incr={incremental:.4f}")
        await on_partial_fill(maker_session, filled_side, fill_price, incremental)
        if filled_side == "bid": bid_fill_hedged = fill_count
        else:                    ask_fill_hedged = fill_count
    else:
        log("WS_FILL_DETECTED", f"side={filled_side} status={status} price={fill_price} incr={incremental:.4f}")
        await on_maker_filled(maker_session, filled_side, fill_price, order_id, quantity=incremental)

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ════════════════════════════════════════════════════════════
def _maker_targets():
    cer_y = curr_prices["CER"]["yes_ask"]
    cer_n = curr_prices["CER"]["no_ask"]
    if cer_y is None or cer_n is None:
        return None
    target_bid = round(max(0.01, 1.0 - cer_y - BID_SPREAD), 2)
    target_ask = round(min(0.99, cer_n + ASK_SPREAD),        2)
    # Rounded to 2 decimal places to bypass floating point representation artifacts
    bid_valid  = round(target_bid + cer_y, 2) <= _R_THRESHOLD
    ask_valid  = round(target_ask - cer_n, 2) >= _R_INV_THRESH
    return cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid

def maker_action_needed() -> bool:
    """Cheap synchronous pre-check: only spawn the (task-creating, lock-taking)
    update_maker_orders when something would actually change. Mirrors the
    decision logic inside update_maker_orders exactly."""
    t = _maker_targets()
    if t is None:
        return False
    cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid = t
    if bid_order_id is None:
        if bid_valid: return True
    else:
        if bid_price is None or round(abs(target_bid - bid_price), 2) >= _R_UPDATE_TICK: return True
        if round(bid_price + cer_y, 2) > _R_THRESHOLD: return True
        if not bid_valid: return True
    if ask_order_id is None:
        if ask_valid: return True
    else:
        if ask_price is None or round(abs(target_ask - ask_price), 2) >= _R_UPDATE_TICK: return True
        if round(ask_price - cer_n, 2) < _R_INV_THRESH: return True
        if not ask_valid: return True
    return False

async def update_maker_orders(session):
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    global updating_orders

    if maker_filled or updating_orders or market_frozen or freeze_cancel_in_progress:
        return

    t = _maker_targets()
    if t is None:
        return
    cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid = t

    updating_orders = True
    try:
        # ── BID ───────────────────────────────────────────
        if bid_order_id is not None:
            stale      = bid_price is None or round(abs(target_bid - bid_price), 2) >= _R_UPDATE_TICK
            too_costly = bid_price is not None and round(bid_price + cer_y, 2) > _R_THRESHOLD
            if stale or too_costly or not bid_valid:
                oid_to_cancel = bid_order_id
                if maker_filled: return
                result = await cancel_order(session, oid_to_cancel)
                if maker_filled: return
                if result == "confirmed":
                    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
                elif result == "not_found":
                    age = time.time() - (bid_placed_at or time.time())
                    if age >= MIN_ORDER_AGE_FOR_FILL:
                        rqty = max(0.0, TRADE_QUANTITY - bid_fill_hedged)
                        log("FILL_INFERRED_404", f"bid id={oid_to_cancel} age={age:.3f}s qty={rqty:.4f}")
                        await on_maker_filled(session, "bid", bid_price or 0, oid_to_cancel, quantity=rqty)
                    else:
                        log("CANCEL_404_TOO_FRESH", f"bid id={oid_to_cancel} age={age:.3f}s")
                    return
                else:
                    return

        if bid_order_id is None and bid_valid and not maker_filled and not market_frozen:
            oid = await place_maker_order(session, "FER", "bid", target_bid)
            if maker_filled: return
            if oid:
                bid_order_id  = oid
                bid_price     = target_bid
                bid_placed_at = time.time()
                bid_fill_hedged = 0.0

        # ── ASK ───────────────────────────────────────────
        if ask_order_id is not None:
            stale      = ask_price is None or round(abs(target_ask - ask_price), 2) >= _R_UPDATE_TICK
            low_margin = ask_price is not None and round(ask_price - cer_n, 2) < _R_INV_THRESH
            if stale or low_margin or not ask_valid:
                oid_to_cancel = ask_order_id
                if maker_filled: return
                result = await cancel_order(session, oid_to_cancel)
                if maker_filled: return
                if result == "confirmed":
                    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0
                elif result == "not_found":
                    age = time.time() - (ask_placed_at or time.time())
                    if age >= MIN_ORDER_AGE_FOR_FILL:
                        rqty = max(0.0, TRADE_QUANTITY - ask_fill_hedged)
                        log("FILL_INFERRED_404", f"ask id={oid_to_cancel} age={age:.3f}s qty={rqty:.4f}")
                        await on_maker_filled(session, "ask", ask_price or 0, oid_to_cancel, quantity=rqty)
                    else:
                        log("CANCEL_404_TOO_FRESH", f"ask id={oid_to_cancel} age={age:.3f}s")
                    return
                else:
                    return

        if ask_order_id is None and ask_valid and not maker_filled and not market_frozen:
            oid = await place_maker_order(session, "FER", "ask", target_ask)
            if maker_filled: return
            if oid:
                ask_order_id  = oid
                ask_price     = target_ask
                ask_placed_at = time.time()
                ask_fill_hedged = 0.0

    finally:
        updating_orders = False

# ════════════════════════════════════════════════════════════
# SPEED METRIC GENERATOR
# ════════════════════════════════════════════════════════════
def get_speed_stats_str() -> str:
    """Calculates ticks and price delta statistics across sliding windows."""
    now = time.time()

    # 1. Calculate ticks frequencies (deque is time-ordered → scan newest-first, early exit)
    ticks_01s = 0; ticks_005s = 0
    cutoff_01 = now - 0.1; cutoff_005 = now - 0.05
    for t in reversed(tick_timestamps):
        if t < cutoff_01: break
        ticks_01s += 1
        if t >= cutoff_005: ticks_005s += 1

    # 2. Extract historical baseline snapshots (oldest-first, early exit once all found)
    p_1s, p_05s, p_01s = None, None, None
    c1, c05, c01 = now - 1.0, now - 0.5, now - 0.1
    for t, snap in price_history:
        if p_1s  is None and t >= c1:  p_1s  = snap
        if p_05s is None and t >= c05: p_05s = snap
        if t >= c01:
            if p_01s is None: p_01s = snap
            break

    def compute_deltas(old_snap):
        if not old_snap:
            return "N/A", 0.0
        max_side_name = "None"
        max_side_val = -1.0
        total_abs_delta = 0.0

        for k in TICKERS:
            cp = curr_prices[k]; op = old_snap[k]
            for side in ("yes_ask", "no_ask", "yes_bid", "no_bid"):
                c_val = cp.get(side)
                o_val = op.get(side)
                if c_val is not None and o_val is not None:
                    diff = abs(c_val - o_val)
                    total_abs_delta += diff
                    if diff > max_side_val:
                        max_side_val = diff
                        max_side_name = f"{k}_{'Y' if 'yes' in side else 'N'}{'A' if 'ask' in side else 'B'}"

        # Format max moving side as "NAME(+0.02)" or "None(0.00)"
        max_str = f"{max_side_name}({max_side_val:+.2f})" if max_side_val > 0.0001 else "None(0.00)"
        return max_str, total_abs_delta

    m_1s, tot_1s   = compute_deltas(p_1s)
    m_05s, tot_05s = compute_deltas(p_05s)
    m_01s, tot_01s = compute_deltas(p_01s)

    return (
        f"TICKS: {ticks_01s}/0.1s {ticks_005s}/0.05s | "
        f"Δ1s: {m_1s}/Σ{tot_1s:.2f} | "
        f"Δ0.5s: {m_05s}/Σ{tot_05s:.2f} | "
        f"Δ0.1s: {m_01s}/Σ{tot_01s:.2f}"
    )

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
        last_printed[k] = {"yes_ask": curr_prices[k].get("yes_ask"),
                            "no_ask":  curr_prices[k].get("no_ask")}

    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    fer_y = curr_prices["FER"].get("yes_ask")
    fer_n = curr_prices["FER"].get("no_ask")
    cer_y = curr_prices["CER"].get("yes_ask")
    cer_n = curr_prices["CER"].get("no_ask")

    def fmt(v): return f"${v:.2f}" if v is not None else "Wait.."
    status_flag = " 🔒LOCKED" if maker_filled else (" 🧊FROZEN" if market_frozen else "")
    bid_stat = f"BID@{bid_price:.2f}({bid_order_id[-6:]})" if bid_order_id else \
               f"→{fmt(round(max(0.01,1-(cer_y or 0)-BID_SPREAD),2))}"
    ask_stat = f"ASK@{ask_price:.2f}({ask_order_id[-6:]})" if ask_order_id else \
               f"→{fmt(round(min(0.99,(cer_n or 0)+ASK_SPREAD),2))}"

    # Get rolling speed and delta data string
    speed_metrics = get_speed_stats_str()

    qprint(
        f"{ts}{status_flag}  | "
        f"FER_Y: {fmt(fer_y):<6} FER_N: {fmt(fer_n):<6} | "
        f"CER_Y: {fmt(cer_y):<6} CER_N: {fmt(cer_n):<6} | "
        f"MAKERS: {bid_stat} / {ask_stat} | "
        f"{speed_metrics}"
    )

# ════════════════════════════════════════════════════════════
# WS MESSAGE HANDLER
# ════════════════════════════════════════════════════════════
async def process_ws_message(maker_session, msg_data: dict):
    msg_type = msg_data.get("type")
    msg      = msg_data.get("msg", {})

    if msg_type == "subscribed":
        qprint(f"  ✅ Subscribed: {msg_data.get('msg',{}).get('channels','?')}")
        return
    if msg_type == "error":
        qprint(f"  ❌ WS error: {msg_data}")
        return
    if msg_type == "user_order":
        await handle_user_order_update(maker_session, msg)
        return

    ticker = msg.get("market_ticker")
    if ticker not in TICKERS_REVERSE:
        return
    leg = TICKERS_REVERSE[ticker]
    seq = msg_data.get("seq")

    if seq is not None:
        if not check_sequence(seq, ticker, msg_type):
            if resync_needed:
                await request_resync(maker_session)
            return

    now = time.time()
    if msg_type == "orderbook_snapshot":
        apply_snapshot(leg, msg); update_derived(leg)
    elif msg_type == "orderbook_delta":
        apply_delta(leg, msg);    update_derived(leg)
    else:
        return

    # Track metrics history window
    tick_timestamps.append(now)
    # Perform a deep snapshot copy of the nested price dictionary
    price_snapshot = {k: curr_prices[k].copy() for k in curr_prices}
    price_history.append((now, price_snapshot))

    # Maintain max 1.05s elements bounds inside tracking lists to conserve memory
    cutoff = now - 1.05
    while tick_timestamps and tick_timestamps[0] < cutoff:
        tick_timestamps.popleft()
    while price_history and price_history[0][0] < cutoff:
        price_history.popleft()

    print_live_odds()

    newly_frozen = check_market_speed()
    if newly_frozen and (bid_order_id or ask_order_id):
        qprint("  🧊 Cancelling all resting makers due to freeze…")
        while updating_orders:
            await asyncio.sleep(0.01)
        await cancel_all_makers_frozen(maker_session)

    if (not maker_filled and not market_frozen and not updating_orders
            and maker_action_needed()):
        asyncio.create_task(update_maker_orders(maker_session))

# ════════════════════════════════════════════════════════════
# CONNECTION WARMUP / KEEPALIVE
# ════════════════════════════════════════════════════════════
async def _warm(session, label: str):
    try:
        async with session.get(f"{BASE_REST}{STATUS_PATH}",
                               headers=auth("GET", STATUS_PATH),
                               timeout=aiohttp.ClientTimeout(total=3)) as _r:
            await _r.read()
    except Exception:
        pass

async def warm_connections():
    """Open + TLS-handshake pooled connections on BOTH sessions before trading.
    Two concurrent warms on the taker session leave two hot sockets in the
    pool, so even if one is busy (e.g. mid-keepalive) the hedge never pays
    a fresh handshake."""
    await asyncio.gather(
        _warm(_taker_session, "taker"),
        _warm(_taker_session, "taker2"),
        _warm(_maker_session, "maker"),
    )

async def connection_keepalive():
    """Periodic pings so pooled sockets never idle past the server/connector
    keepalive window — the first request after a quiet stretch (which is
    exactly when a fill tends to happen) stays handshake-free."""
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(KEEPALIVE_SECS)
        if maker_filled:
            continue
        try:
            hdr = await loop.run_in_executor(None, _sign, "GET", STATUS_PATH)
            for sess in (_taker_session, _maker_session):
                if sess is None or sess.closed:
                    continue
                try:
                    async with sess.get(f"{BASE_REST}{STATUS_PATH}", headers=hdr,
                                        timeout=aiohttp.ClientTimeout(total=3)) as _r:
                        await _r.read()
                except Exception:
                    pass
        except Exception:
            pass

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
async def monitor():
    global _maker_session, _taker_session, tick_timestamps, price_history

    init_log()
    qprint("=" * 90)
    qprint(f"  🎾 Maker-Taker Arb  —  {GAME_DESCRIPTION}")
    qprint(f"  FER_Y BID = 1 − CER_Y − {BID_SPREAD}   (hedge: take CER_YES)")
    qprint(f"  FER_Y ASK = CER_N + {ASK_SPREAD}         (hedge: take CER_NO)")
    qprint(f"  THRESHOLD={THRESHOLD}  UPDATE_TICK={UPDATE_TICK}  MIN_ORDER_AGE={MIN_ORDER_AGE_FOR_FILL}s")
    qprint(f"  FREEZE: tick Δ ≥ {FREEZE_TICK_THRESHOLD} → cancel all, wait {CALM_DURATION}s calm")
    qprint(f"  TAKER: dedicated session, pre-signed headers, fires before cancel (cancel overlaps)")
    qprint("=" * 90)

    ws_headers = auth("GET", "/trade-api/ws/v2")
    taker_connector = aiohttp.TCPConnector(limit=0, keepalive_timeout=60,
                                           ttl_dns_cache=300, enable_cleanup_closed=True)
    maker_connector = aiohttp.TCPConnector(limit=0, keepalive_timeout=60,
                                           ttl_dns_cache=300, enable_cleanup_closed=True)

    async with aiohttp.ClientSession(connector=maker_connector) as maker_session, \
               aiohttp.ClientSession(connector=taker_connector) as taker_session:

        _maker_session = maker_session
        _taker_session = taker_session

        await warm_connections()

        refresher_task = asyncio.create_task(signature_refresher())
        keepalive_task = asyncio.create_task(connection_keepalive())

        try:
            while True:
                if maker_filled:
                    qprint("\n🔒 MAKER MODE LOCKED. Monitoring only. Ctrl+C to exit.\n")
                    while True:
                        await asyncio.sleep(60)

                try:
                    async with websockets.connect(
                        WS_URL, additional_headers=ws_headers,
                        ping_interval=15, compression=None, max_queue=1024
                    ) as ws:
                        qprint(f"\n✅ Connected: {WS_URL}")
                        await cancel_all_makers(maker_session)

                        last_seq.clear(); resync_needed.clear()
                        tick_timestamps.clear(); price_history.clear()
                        for k in TICKERS:
                            orderbooks[k]  = {"yes": {}, "no": {}}
                            best_cents[k]  = {"yes": None, "no": None}
                            curr_prices[k] = {"yes_ask": None, "yes_bid": None,
                                              "no_ask":  None, "no_bid":  None}

                        await ws.send(json.dumps({
                            "id": 1, "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta", "user_orders"],
                                "market_tickers": list(TICKERS.values()),
                            }
                        }))

                        async for raw in ws:
                            await process_ws_message(maker_session, _jloads(raw))

                except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                    qprint(f"\n❌ WS error: {e}. Cancelling makers + retrying in 3s…")
                    await cancel_all_makers(maker_session)
                    await asyncio.sleep(3)

        except (KeyboardInterrupt, asyncio.CancelledError):
            qprint("\n🔴 Shutting down — cancelling makers…")
            log("SHUTDOWN", "manual")
            await cancel_all_makers(maker_session)
            qprint("🔴 Done.")
        finally:
            refresher_task.cancel()
            keepalive_task.cancel()

if __name__ == "__main__":
    # Optional faster event loops — harmless no-ops if not installed.
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
        asyncio.run(monitor())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        qprint(f"\n❌ Fatal: {e}")
    finally:
        flush_output()
