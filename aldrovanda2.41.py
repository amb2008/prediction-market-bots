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

RECONNECT RECONCILIATION (new in _28)
─────────────────────────────────────
On any WS reconnect / error / shutdown, tracked makers are no longer
blindly cleared. Each cancel result is checked: a 404 on a sufficiently
old order means it FILLED while we were blind → the orderbooks are first
refreshed via REST (so the hedge prices are current, not pre-disconnect
stale), then the fill is hedged through the normal handler. No more
silent unhedged positions after a disconnect.

MID-LOOP PRICE REFRESH (new in _28)
───────────────────────────────────
update_maker_orders no longer snapshots CER prices once at the top.
Targets are recomputed from live curr_prices at EVERY decision point —
before each staleness check and again after each cancel returns, right
before placing the replacement. The WS handler keeps updating prices
during those awaits; quotes are now computed from the newest tick
instead of prices up to ~0.5s old.

FULL-RESOLUTION DATA RECORDER (new in _30)
──────────────────────────────────────────
Everything needed for offline pattern analysis is written to ONE
machine-readable CSV per session: botdata/data_<UTC timestamp>.csv.
A dedicated writer thread with a persistent file handle drains a queue
(batched, flushed every RECORD_FLUSH_SECS) so recording adds ~µs to the
hot path.

Row types (ev column):
  SESSION  one per start — JSON of full config in `info`
  S        orderbook snapshot — full raw book JSON in `info`
  D        EVERY orderbook delta (price change or not): leg, side,
           price (cents), delta qty, qty at level after
  UO       every user_orders WS message, raw JSON in `info`
  H        heartbeat every HEARTBEAT_SECS (proves feed alive when quiet)
  WS_*     connect/disconnect markers
  + every 📝 log event (MAKER_PLACED, ORDER_CANCELLED, WS_FILL_DETECTED,
    HEDGE_SENT, HEDGE_FILLED, TRADE_SUMMARY, DEPTH_GATE_*, MARKET_FROZEN,
    FILL_INFERRED_*, ...) with its details string in `info`

Every row also carries the live state at that instant: best bids of all
four books (cents), size resting at each best, our resting maker prices,
gate flags, and the WS seq. Raw S+D rows allow full book reconstruction
at any timestamp offline.

To hand data over for analysis: send the whole data_*.csv (zip it if
large). No trimming or labeling needed — trade outcomes are in the
TRADE_SUMMARY rows.

ORPHAN-ORDER RACE FIX (new in _40)
──────────────────────────────────
Race: a re-quote task passes its maker_filled check, awaits the placement
POST, and a fill arrives during that await. Previously the code then hit
`if maker_filled: return` AFTER the exchange accepted the order — returning
without recording the order id: a live, untracked, uncancellable maker
resting in locked mode (observed live: bid @ 0.15 placed 7ms after a fill,
later filled unhedged). Fixed with three layers:
 1. place_maker_order re-checks maker_filled / frozen immediately before
    sending (shrinks the race to pure network flight time).
 2. If a placement completes and the world changed while it was in flight,
    the order is RETIRED, not abandoned: cancelled immediately.
 3. If that cancel 404s (it filled inside the gap), the fill is hedged —
    through the normal handler when not yet locked, or via an EMERGENCY
    hedge that fires even in locked mode. Loud logging either way
    (ORPHAN_ORDER_RETIRE / EMERGENCY_HEDGE_*).

ADV PULL TRIGGER (new in _31)
─────────────────────────────
Side-matched adverse hedge drift — the indicator validated on 8h of
full-resolution data (fee-adj PnL +0.70 → +1.19; 16-20/29 losers avoided,
2/81 winners lost; active ~2% of the time):
  For the BID maker: the CER_YES effective ask (= 1 − CER no-book best
  bid) minus its minimum over the trailing ADV_PULL_WINDOW_S seconds.
  For the ASK maker: same on the CER_NO ask (via the yes-book best bid).
  Drift ≥ ADV_PULL_THRESHOLD → pull that side's maker.
  Re-quote when drift decays to ≤ ADV_PULL_CLEAR (hysteresis).
Evaluated O(1) on EVERY CER book update via monotonic rolling-max deques
(not the printed tick lines). Every crossing prints + logs
(ADV_PULL_BLOCK / ADV_PULL_CLEAR) with the drift value.
ADV_PULL_ENFORCE=True  → pulls/withholds the affected maker through the
                          normal re-quote path.
ADV_PULL_ENFORCE=False → shadow mode: prints when it WOULD have pulled,
                          zero effect on quoting.

DEPTH GATE (new in _29)
───────────────────────
Don't rest a maker whose hedge-side CER book is too thin to hedge into.
Evaluated IN CODE on every book update (O(1)) — not from printed lines —
so quiet-price depth drains (like the 16:07 trade, where depth collapsed
1500→5 during a 4.5s price-silent stretch and never printed) are caught
with their full real lead time. Rules:
  - BID maker requires CER_YES-ask depth  ≥ DEPTH_GATE_BLOCK_BELOW
  - ASK maker requires CER_NO-ask depth   ≥ DEPTH_GATE_BLOCK_BELOW
  - hysteresis: a blocked side clears at DEPTH_GATE_CLEAR_ABOVE
Every crossing prints + logs (DEPTH_GATE_BLOCK / DEPTH_GATE_CLEAR) with
the depth value and timestamp, so lead times are measurable from the log.
Blocked sides also flag the live tick line ("🛑GATE:...").
DEPTH_GATE_ENFORCE=True makes the gate cancel/withhold the affected maker
(via the normal re-quote path). Set False for shadow mode: full printing,
zero effect on quoting.

DIAGNOSTIC INSTRUMENTATION (new in _28)
───────────────────────────────────────
For good/bad trade analysis, every printed tick line is extended with:
  DEPTH CYA:b/c CNA:b/c FYA:b FNA:b   best-ask size (b) and cumulative
                                       size within 3c of best (c) —
                                       CER sizes are the hedge-side
                                       liquidity the taker will eat
  ADV Y:+x.xx N:+x.xx                  adverse drift: current CER ask
                                       minus its minimum over the
                                       trailing 1s window, per side
And every completed trade emits a machine-readable TRADE_SUMMARY log:
fill/hedge prices, slippage, edge-at-fill, profit, detect→send and
hedge round-trip latencies (ms), plus the full market state at the
fill instant (Σ1s, tick rates, ADV, hedge-side depth). HEDGE_SENT is
logged the moment the taker request is on the wire.

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
    "FER": "KXITFMATCH-26JUL29TAKSAM-TAK",
    "CER": "KXITFMATCH-26JUL29TAKSAM-SAM"
}

# TICKERS = {
#     "FER": "KXITFWMATCH-26JUL14SCHSOU-SCH",
#     "CER": "KXITFWMATCH-26JUL14SCHSOU-SOU"
# }

TICKERS_REVERSE = {v: k for k, v in TICKERS.items()}
GAME_DESCRIPTION = "Ferrer vs Cerundolo (ATP)"

TRADE_QUANTITY          = 1
BID_SPREAD              = 0.03
ASK_SPREAD              = 0.03
THRESHOLD               = 0.97
TAKER_BUFFER            = 0.15
UPDATE_TICK             = 0.01
MIN_ORDER_AGE_FOR_FILL  = 0.1
FREEZE_TICK_THRESHOLD   = 0.10
CALM_DURATION           = 2.0
LOG_FILE                = "maker_taker_log.csv"

DEPTH_GATE_ENFORCE      = False  # False = shadow mode (print/log only, no quoting effect)
DEPTH_GATE_BLOCK_BELOW  = 100.0  # block a side when hedge-side depth at best ask < this
DEPTH_GATE_CLEAR_ABOVE  = 120.0  # unblock when depth recovers above this (hysteresis)

BOT_VERSION             = "2.40"

ADV_PULL_ENFORCE        = True   # False = shadow mode (print when it would pull, no quoting effect)
ADV_PULL_THRESHOLD      = 0.02   # pull side when hedge-leg drift ≥ this ($)
ADV_PULL_CLEAR          = 0.01   # re-quote when drift decays to ≤ this ($)
ADV_PULL_WINDOW_S       = 1.5    # trailing window for the drift minimum
RECORD_ENABLED          = False   # full-resolution data recorder
RECORD_DIR              = "botdata"
RECORD_FLUSH_SECS       = 1.0    # recorder file flush cadence
HEARTBEAT_SECS          = 5.0    # heartbeat row cadence when market is quiet

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
# FULL-RESOLUTION DATA RECORDER
# One CSV per session; every book delta, order event, and trade
# outcome as a timestamped row. Dedicated thread + persistent
# handle + batched writes → ~µs cost on the hot path.
# ════════════════════════════════════════════════════════════
_rec_q: _queue.SimpleQueue = _queue.SimpleQueue()
_REC_SENTINEL = object()
_REC_HEADER = ["ts_ms", "ev", "leg", "side", "pc", "d", "q",
               "fyb", "fnb", "cyb", "cnb",
               "qfy", "qfn", "qcy", "qcn",
               "mb", "ma", "gate", "seq", "info"]
rec_path: str | None = None

def _recorder_loop():
    global rec_path
    try:
        os.makedirs(RECORD_DIR, exist_ok=True)
        rec_path = os.path.join(
            RECORD_DIR,
            f"data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv")
        f = open(rec_path, "w", newline="", encoding="utf-8")
        w = csv.writer(f)
        w.writerow(_REC_HEADER)
        f.flush()
    except Exception:
        return
    last_flush = time.time()
    while True:
        try:
            item = _rec_q.get(timeout=RECORD_FLUSH_SECS)
        except _queue.Empty:
            item = None
        try:
            if item is _REC_SENTINEL:
                f.flush(); f.close(); return
            if item is not None:
                rows = [item]
                while True:
                    try:
                        nxt = _rec_q.get_nowait()
                    except _queue.Empty:
                        break
                    if nxt is _REC_SENTINEL:
                        w.writerows(rows); f.flush(); f.close(); return
                    rows.append(nxt)
                w.writerows(rows)
            if time.time() - last_flush >= RECORD_FLUSH_SECS:
                f.flush(); last_flush = time.time()
        except Exception:
            pass

_recorder_thread = threading.Thread(target=_recorder_loop, daemon=True, name="data-recorder")
if RECORD_ENABLED:
    _recorder_thread.start()

def _market_state_row() -> tuple:
    """Live state attached to every recorded row: best bids (cents) of all
    four books, size at each best, our resting maker prices, gate flags."""
    fy = best_cents["FER"]["yes"]; fn = best_cents["FER"]["no"]
    cy = best_cents["CER"]["yes"]; cn = best_cents["CER"]["no"]
    def q(leg, s, b):
        return orderbooks[leg][s].get(b, 0.0) if b is not None else 0.0
    gate = (("B" if depth_gate_blocked["bid"] else "") +
            ("A" if depth_gate_blocked["ask"] else ""))
    return (fy if fy is not None else "", fn if fn is not None else "",
            cy if cy is not None else "", cn if cn is not None else "",
            q("FER", "yes", fy), q("FER", "no", fn),
            q("CER", "yes", cy), q("CER", "no", cn),
            bid_price if bid_order_id else "", ask_price if ask_order_id else "",
            gate)

def rec_delta(leg: str, side: str, pc: int, d: float, q_after: float, seq):
    if not RECORD_ENABLED:
        return
    _rec_q.put((int(time.time() * 1000), "D", leg, side, pc, d, q_after,
                *_market_state_row(), seq if seq is not None else "", ""))

def rec_event(ev: str, info: str = "", leg: str = "", seq=""):
    if not RECORD_ENABLED:
        return
    _rec_q.put((int(time.time() * 1000), ev, leg, "", "", "", "",
                *_market_state_row(), seq, info))

def stop_recorder(timeout: float = 3.0):
    if not RECORD_ENABLED:
        return
    _rec_q.put(_REC_SENTINEL)
    _recorder_thread.join(timeout=timeout)

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
    rec_event(event, details)

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
        resync_needed.update(TICKERS.values())   # real market tickers (request_resync expects these)
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
    """Apply one delta; returns (side, price_cents, delta_qty, qty_after)
    for the data recorder, or None if the message was malformed."""
    side = msg.get("side"); price = msg.get("price_dollars"); delta = msg.get("delta_fp")
    if side not in ("yes", "no") or price is None or delta is None: return None
    c  = int(round(float(price) * 100))
    dq = float(delta)
    bk = orderbooks[leg][side]
    nq = bk.get(c, 0.0) + dq
    if nq <= 0.0001:
        bk.pop(c, None)
        if best_cents[leg][side] == c:                 # best level removed → rescan
            best_cents[leg][side] = max(bk) if bk else None
        nq = 0.0
    else:
        bk[c] = nq
        b = best_cents[leg][side]
        if b is None or c > b:                          # new best → O(1)
            best_cents[leg][side] = c
    return (side, c, dq, nq)

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
# DIAGNOSTIC HELPERS  (hedge-side depth + adverse drift)
# ════════════════════════════════════════════════════════════
def ask_depth(leg: str, yes_side: bool) -> tuple[float, float]:
    """Size resting at the current best ask of (leg, YES or NO), and the
    cumulative size within 3c of that best ask. The YES ask is formed by
    NO-side bids and vice versa, so we read the opposite bid book."""
    book_side = "no" if yes_side else "yes"
    bk = orderbooks[leg][book_side]
    b  = best_cents[leg][book_side]
    if b is None or not bk:
        return 0.0, 0.0
    best_qty = bk.get(b, 0.0)
    lo = b - 3
    cum = 0.0
    for c, q in bk.items():
        if lo <= c <= b:
            cum += q
    return best_qty, cum

def adverse_drift(window: float = 1.0) -> tuple[float, float]:
    """How far each CER ask sits ABOVE its minimum over the trailing window.
    This is the 'hedge leg already ran away' pre-fill signal: a positive
    value means buying that CER side now costs more than it did moments ago."""
    cy = curr_prices["CER"]["yes_ask"]
    cn = curr_prices["CER"]["no_ask"]
    now = time.time()
    cutoff = now - window
    min_y = cy; min_n = cn
    for t, snap in price_history:
        if t < cutoff:
            continue
        s = snap["CER"]
        v = s.get("yes_ask")
        if v is not None and (min_y is None or v < min_y): min_y = v
        v = s.get("no_ask")
        if v is not None and (min_n is None or v < min_n): min_n = v
    advy = round(cy - min_y, 4) if (cy is not None and min_y is not None) else 0.0
    advn = round(cn - min_n, 4) if (cn is not None and min_n is not None) else 0.0
    return advy, advn

def market_state_str() -> str:
    """DEPTH + ADV segment appended to every printed tick line."""
    cy_b, cy_c = ask_depth("CER", True)
    cn_b, cn_c = ask_depth("CER", False)
    fy_b, _    = ask_depth("FER", True)
    fn_b, _    = ask_depth("FER", False)
    advy, advn = adverse_drift()
    base = (f"DEPTH CYA:{cy_b:.1f}/{cy_c:.1f} CNA:{cn_b:.1f}/{cn_c:.1f} "
            f"FYA:{fy_b:.1f} FNA:{fn_b:.1f} | "
            f"ADV Y:{advy:+.2f} N:{advn:+.2f}")
    blocked = [s.upper() for s in ("bid", "ask") if depth_gate_blocked[s]]
    if blocked:
        base += f" | 🛑GATE:{'+'.join(blocked)}"
    pulled = [s.upper() for s in ("bid", "ask") if adv_pull_blocked[s]]
    if pulled:
        base += f" | ⚡ADV:{'+'.join(pulled)}"
    return base

# ════════════════════════════════════════════════════════════
# DEPTH GATE
# Don't rest a maker whose hedge-side book can't absorb the hedge.
# Evaluated in code on EVERY book update — including price-quiet
# depth drains that never print a tick line.
# ════════════════════════════════════════════════════════════
depth_gate_blocked = {"bid": False, "ask": False}

def hedge_depth_best(side: str) -> float | None:
    """O(1) size resting at the hedge-side CER best ask for a maker side.
    bid maker → hedge buys CER_YES (ask formed by CER no-book best bid);
    ask maker → hedge buys CER_NO  (ask formed by CER yes-book best bid)."""
    book_side = "no" if side == "bid" else "yes"
    b = best_cents["CER"][book_side]
    if b is None:
        return None
    return orderbooks["CER"][book_side].get(b, 0.0)

def update_depth_gate():
    """Run on every book update. Prints + logs every threshold crossing so
    the log carries the gate's true lead time; quoting is only affected
    when DEPTH_GATE_ENFORCE is True (via _live_targets validity)."""
    for side in ("bid", "ask"):
        depth = hedge_depth_best(side)
        if depth is None:            # no book yet — leave state, stay quiet
            continue
        hedge_name = "CER_YES" if side == "bid" else "CER_NO"
        if not depth_gate_blocked[side] and depth < DEPTH_GATE_BLOCK_BELOW:
            depth_gate_blocked[side] = True
            mode = "blocking" if DEPTH_GATE_ENFORCE else "SHADOW (would block)"
            qprint(f"  🛑 DEPTH GATE: {hedge_name} best-ask depth {depth:.1f} < "
                   f"{DEPTH_GATE_BLOCK_BELOW:.0f} → {mode} {side.upper()} maker")
            log("DEPTH_GATE_BLOCK", f"side={side} hedge={hedge_name} depth={depth:.1f} "
                                    f"threshold={DEPTH_GATE_BLOCK_BELOW:.0f} enforce={DEPTH_GATE_ENFORCE}")
        elif depth_gate_blocked[side] and depth >= DEPTH_GATE_CLEAR_ABOVE:
            depth_gate_blocked[side] = False
            qprint(f"  ✅ DEPTH GATE: {hedge_name} best-ask depth {depth:.1f} ≥ "
                   f"{DEPTH_GATE_CLEAR_ABOVE:.0f} → {side.upper()} maker clear")
            log("DEPTH_GATE_CLEAR", f"side={side} hedge={hedge_name} depth={depth:.1f} "
                                    f"threshold={DEPTH_GATE_CLEAR_ABOVE:.0f}")

# ════════════════════════════════════════════════════════════
# ADV PULL TRIGGER
# Side-matched adverse hedge drift ≥ ADV_PULL_THRESHOLD → pull
# that side's maker. O(1) per book update via monotonic
# rolling-max deques over the opposite-side best bid (a rising
# hedge ask is exactly a falling opposite bid's rolling max
# minus its current value).
# ════════════════════════════════════════════════════════════
adv_pull_blocked = {"bid": False, "ask": False}

class _RollingMax:
    """Monotonic deque rolling maximum over a trailing time window."""
    __slots__ = ("win_ms", "dq")
    def __init__(self, win_ms: float):
        self.win_ms = win_ms
        self.dq = deque()           # (ts_ms, value), values non-increasing
    def update(self, ts_ms: float, v: int) -> int:
        dq = self.dq
        while dq and dq[-1][1] <= v:
            dq.pop()
        dq.append((ts_ms, v))
        cut = ts_ms - self.win_ms
        while dq and dq[0][0] < cut:
            dq.popleft()
        return dq[0][1]
    def reset(self):
        self.dq.clear()

_adv_max_cnb = _RollingMax(ADV_PULL_WINDOW_S * 1000)   # bid side: CER_YES ask = 1 − cnb
_adv_max_cyb = _RollingMax(ADV_PULL_WINDOW_S * 1000)   # ask side: CER_NO  ask = 1 − cyb

def reset_adv_pull():
    _adv_max_cnb.reset(); _adv_max_cyb.reset()
    adv_pull_blocked["bid"] = False
    adv_pull_blocked["ask"] = False

def update_adv_pull():
    """Run on every CER book update. Drift for a side = rolling max of the
    opposite best bid (cents) minus its current value. Prints + logs every
    crossing; quoting is only affected when ADV_PULL_ENFORCE is True."""
    ts_ms = time.time() * 1000
    for side, tracker, key in (("bid", _adv_max_cnb, "no"), ("ask", _adv_max_cyb, "yes")):
        cur = best_cents["CER"][key]
        if cur is None:
            continue
        mx = tracker.update(ts_ms, cur)
        drift = (mx - cur) / 100.0
        hedge_name = "CER_YES" if side == "bid" else "CER_NO"
        if not adv_pull_blocked[side] and drift >= ADV_PULL_THRESHOLD:
            adv_pull_blocked[side] = True
            mode = "pulling" if ADV_PULL_ENFORCE else "SHADOW (would pull)"
            qprint(f"  ⚡ ADV PULL: {hedge_name} ask +{drift:.2f} above its "
                   f"{ADV_PULL_WINDOW_S:.1f}s low → {mode} {side.upper()} maker")
            log("ADV_PULL_BLOCK", f"side={side} hedge={hedge_name} drift={drift:.4f} "
                                  f"threshold={ADV_PULL_THRESHOLD} enforce={ADV_PULL_ENFORCE}")
        elif adv_pull_blocked[side] and drift <= ADV_PULL_CLEAR:
            adv_pull_blocked[side] = False
            qprint(f"  ✅ ADV PULL: {hedge_name} drift decayed to +{drift:.2f} → "
                   f"{side.upper()} maker clear")
            log("ADV_PULL_CLEAR", f"side={side} hedge={hedge_name} drift={drift:.4f} "
                                  f"threshold={ADV_PULL_CLEAR}")

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
    # Last-instant gate: the caller's checks may be stale if a fill or freeze
    # landed while this task was queued. Re-check right before the wire.
    if maker_filled or market_frozen or freeze_cancel_in_progress:
        log("MAKER_PLACE_SUPPRESSED",
            f"side={v2_side} price={price} filled={maker_filled} frozen={market_frozen}")
        return None
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

async def refresh_books_rest(session):
    """Pull fresh orderbook snapshots via REST for both legs. Used before
    reconciliation so any inferred-fill hedge prices are current rather
    than pre-disconnect stale."""
    resync_needed.update(TICKERS.values())
    await request_resync(session)
    resync_needed.clear()

async def cancel_all_makers(session, reconcile: bool = True):
    """Cancel every tracked maker — WITHOUT going blind. A confirmed cancel
    clears the slot. A 404 on an order old enough to have been live means it
    FILLED while we weren't looking (e.g. during a WS disconnect): refresh
    the books via REST, then hedge it through the normal fill handler.
    Pass reconcile=False only when a truly unconditional wipe is wanted."""
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged

    tracked = [
        ("bid", bid_order_id, bid_price, bid_placed_at, bid_fill_hedged),
        ("ask", ask_order_id, ask_price, ask_placed_at, ask_fill_hedged),
    ]
    if not any(oid for _, oid, *_ in tracked):
        return

    books_refreshed = False
    for side, oid, px, placed_at, already_hedged in tracked:
        if oid is None:
            continue
        result = await cancel_order(session, oid)
        if result == "confirmed" or not reconcile:
            pass  # clean — clear below
        elif result == "not_found":
            age = time.time() - (placed_at or time.time())
            if age >= MIN_ORDER_AGE_FOR_FILL and not maker_filled:
                remaining_qty = max(0.0, TRADE_QUANTITY - already_hedged)
                log("FILL_INFERRED_404_RECONCILE",
                    f"{side} id={oid} age={age:.3f}s qty={remaining_qty:.4f} — filled while blind")
                if remaining_qty > 0.001:
                    if not books_refreshed:
                        qprint("  🔄 Refreshing orderbooks via REST before hedging inferred fill…")
                        await refresh_books_rest(session)
                        books_refreshed = True
                    await on_maker_filled(session, side, px or 0, oid, quantity=remaining_qty)
            else:
                log("CANCEL_404_RECONCILE_SKIPPED", f"{side} id={oid} age={age:.3f}s")
        else:
            # cancel errored — retry once before giving up loudly
            retry = await cancel_order(session, oid)
            if retry not in ("confirmed", "not_found"):
                qprint(f"  🚨 Could not cancel {oid[:12]}… after retry — VERIFY MANUALLY")
                log("CANCEL_UNRESOLVED", f"{side} id={oid}")
        if side == "bid":
            bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
        else:
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
# ORPHAN-ORDER RETIREMENT + EMERGENCY HEDGE
# For orders whose placement completed AFTER a fill/freeze made
# them unwanted: cancel immediately; if the cancel 404s the order
# filled inside the gap and MUST be hedged — even in locked mode.
# ════════════════════════════════════════════════════════════
async def emergency_hedge(filled_side: str, fill_price: float, order_id: str,
                          quantity: float):
    """Hedge a fill that occurred outside the normal flow (orphan-order race).
    Deliberately ignores the maker_filled lock — an unhedged leg is the one
    thing that must never survive."""
    if filled_side == "bid":
        cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
        body, headers = _build_taker_payload("CER", "bid", taker_px, quantity)
        desc = f"BUY CER_YES qty={quantity:.4f} limit={taker_px} mkt={cer_y}"
    else:
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
        body, headers = _build_taker_payload("CER", "ask", taker_px, quantity)
        desc = f"BUY CER_NO qty={quantity:.4f} ask@{taker_px} mkt={cer_n}"
    taker_task = asyncio.create_task(fire_taker(body, headers))
    await asyncio.sleep(0)
    qprint(f"  🚑 EMERGENCY HEDGE for orphan fill {order_id[:12]}…: {desc}")
    log("EMERGENCY_HEDGE_ATTEMPT", f"side={filled_side} order={order_id} {desc}")
    filled, res = await taker_task
    if filled:
        avg = float(res.get("average_fill_price", taker_px))
        qprint(f"  ✅ EMERGENCY HEDGE FILLED @ ${avg:.4f}")
        log("EMERGENCY_HEDGE_FILLED", f"side={filled_side} order={order_id} avg={avg}")
    else:
        qprint(f"  🚨 EMERGENCY HEDGE FAILED → {res}")
        qprint(f"  🚨 UNHEDGED {filled_side.upper()} POSITION from {order_id} — CLOSE MANUALLY")
        log("EMERGENCY_HEDGE_MISSED", f"side={filled_side} order={order_id}: {res}")

async def retire_orphan_order(session, side: str, price: float, order_id: str):
    """An order whose placement completed after a fill/freeze: cancel it now.
    Cancel 404 here means it executed inside the gap (its id came from a
    successful create moments ago) — confirm with one delayed retry, then
    hedge the fill through the appropriate path."""
    qprint(f"  🚨 ORPHAN ORDER: {side.upper()} {order_id[:12]}… placed during "
           f"fill/freeze race — retiring")
    log("ORPHAN_ORDER_RETIRE", f"side={side} price={price} id={order_id} "
                               f"filled={maker_filled} frozen={market_frozen}")
    result = await cancel_order(session, order_id)
    if result == "confirmed":
        qprint(f"  ✅ Orphan {order_id[:12]}… cancelled cleanly")
        return
    if result == "error":
        result = await cancel_order(session, order_id)          # one retry
        if result == "confirmed":
            return
    if result == "not_found":
        await asyncio.sleep(0.1)                                 # settle, then confirm
        result2 = await cancel_order(session, order_id)
        if result2 == "confirmed":
            qprint(f"  ✅ Orphan {order_id[:12]}… cancelled on retry")
            return
        # Double 404 on an id we just created ⇒ it filled in the gap. Hedge it.
        log("ORPHAN_FILL_INFERRED", f"side={side} price={price} id={order_id}")
        if not maker_filled:
            await on_maker_filled(session, side, price, order_id)
        else:
            await emergency_hedge(side, price, order_id, TRADE_QUANTITY)
        return
    qprint(f"  🚨 Orphan {order_id[:12]}… could not be cancelled — VERIFY MANUALLY")
    log("ORPHAN_UNRESOLVED", f"side={side} id={order_id}")

# ════════════════════════════════════════════════════════════
# PARTIAL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_partial_fill(maker_session, filled_side: str, fill_price: float, quantity: float,
                          detect_t: float | None = None):
    # Build + fire the hedge FIRST; all console output happens after the
    # request is on the wire. Same lines, same order — just later.
    advy_f, advn_f = adverse_drift()
    stats_f = get_speed_stats()
    if filled_side == "bid":
        dep_best, dep_3c = ask_depth("CER", True)
        cer_y    = curr_prices["CER"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, cer_y + TAKER_BUFFER), 4)
        team, v2_side = "CER", "bid"
    else:
        dep_best, dep_3c = ask_depth("CER", False)
        cer_n    = curr_prices["CER"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - cer_n - TAKER_BUFFER), 4)
        team, v2_side = "CER", "ask"

    body, headers = _build_taker_payload(team, v2_side, taker_px, quantity)
    taker_task = asyncio.create_task(fire_taker(body, headers))
    await asyncio.sleep(0)   # let the taker task run to its first I/O await → request written
    t_sent = time.perf_counter()
    log("HEDGE_SENT", f"side={filled_side} qty={quantity:.4f} limit={taker_px} partial=1")

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
    t_resp = time.perf_counter()

    if filled:
        qty_filled = float(res.get("fill_count", str(quantity)))
        avg        = float(res.get("average_fill_price", taker_px))
        qprint(f"  ✅ Partial Hedge FILLED  qty={qty_filled:.4f} @ ${avg:.4f}")
        log("PARTIAL_HEDGE_FILLED", f"qty={qty_filled:.4f} avg={avg}")
    else:
        qprint(f"  🚨 PARTIAL HEDGE FAILED → {res}")
        log("PARTIAL_HEDGE_MISSED", f"side={filled_side} qty={quantity:.4f}: {res}")

    lat_detect_send = round((t_sent - detect_t) * 1000, 2) if detect_t else -1
    lat_hedge_rtt   = round((t_resp - t_sent) * 1000, 2)
    sv = stats_f[1]
    if filled_side == "bid":
        mkt = cer_y; havg = float(res.get("average_fill_price", taker_px)) if filled else None
        slip = round(havg - cer_y, 4) if havg is not None else None
    else:
        mkt = cer_n
        havg = round(1.0 - float(res.get("average_fill_price", taker_px)), 4) if filled else None
        slip = round(havg - cer_n, 4) if havg is not None else None
    log("TRADE_SUMMARY",
        f"side={filled_side} fill={fill_price:.4f} qty={quantity:.4f} partial=1 "
        f"hedge_mkt={mkt} hedge_avg={havg} slip={slip} edge_at_fill=NA profit=NA "
        f"lat_detect_to_send_ms={lat_detect_send} lat_hedge_rtt_ms={lat_hedge_rtt} "
        f"s1={sv['s1']:.2f} s05={sv['s05']:.2f} s01={sv['s01']:.2f} "
        f"ticks01={sv['ticks01']} ticks005={sv['ticks005']} "
        f"advY={advy_f:+.4f} advN={advn_f:+.4f} "
        f"depth_hedge_best={dep_best:.1f} depth_hedge_3c={dep_3c:.1f}")

# ════════════════════════════════════════════════════════════
# FULL FILL HANDLER
# ════════════════════════════════════════════════════════════
async def on_maker_filled(maker_session, filled_side: str, fill_price: float, order_id: str,
                          quantity: float | None = None, detect_t: float | None = None):
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

    # ── Capture the market state at the fill instant (for TRADE_SUMMARY) ──
    advy_f, advn_f = adverse_drift()
    stats_f = get_speed_stats()          # (string, values dict)
    if filled_side == "bid":
        dep_best, dep_3c = ask_depth("CER", True)
    else:
        dep_best, dep_3c = ask_depth("CER", False)

    # ── Build + fire the hedge BEFORE any console output ──────
    taker_task = None
    taker_px   = None
    cer_y = cer_n = None
    t_sent = None
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
        t_sent = time.perf_counter()
        log("HEDGE_SENT", f"side={filled_side} qty={hedge_qty:.4f} limit={taker_px}")

    # ── Cancel of the other maker overlaps the hedge round-trip ──
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
    t_resp = time.perf_counter()

    if cancel_task is not None:
        await cancel_task

    profit = None
    hedge_avg = None
    slippage = None
    edge_at_fill = None
    if hedge_qty <= 0.001:
        pass
    elif filled_side == "bid":
        edge_at_fill = round(1.0 - (fill_price + cer_y), 4)
        if filled:
            avg   = float(res.get("average_fill_price", taker_px))
            total = round(fill_price + avg, 4)
            hedge_avg = avg
            slippage  = round(avg - cer_y, 4)
            profit    = round(1 - total, 4)
            qprint(f"  ✅ Hedge FILLED @ ${avg:.4f}  total=${total:.4f}")
            qprint(f"  💰 LOCKED PROFIT: $1.00 − ${total:.4f} = ${round(1-total,4):.4f}")
            log("HEDGE_FILLED", f"avg={avg} total={total} profit={round(1-total,4)}")
        else:
            qprint(f"  🚨 HEDGE FAILED: BUY CER_YES @ {taker_px} → {res}")
            qprint(f"  🚨 HOLDING {hedge_qty:.4f}× FER_YES unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"BUY CER_YES qty={hedge_qty:.4f} limit={taker_px}: {res}")
    else:
        edge_at_fill = round(fill_price - cer_n, 4)
        if filled:
            yes_avg = float(res.get("average_fill_price", taker_px))
            no_cost = round(1.0 - yes_avg, 4)
            hedge_avg = no_cost
            slippage  = round(no_cost - cer_n, 4)
            profit    = round(fill_price - no_cost, 4)
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

    # ── Machine-readable trade summary for good/bad analysis ──
    lat_detect_send = round((t_sent - detect_t) * 1000, 2) if (t_sent and detect_t) else -1
    lat_hedge_rtt   = round((t_resp - t_sent) * 1000, 2)   if t_sent else -1
    sv = stats_f[1]
    log("TRADE_SUMMARY",
        f"side={filled_side} fill={fill_price:.4f} qty={hedge_qty:.4f} "
        f"hedge_mkt={(cer_y if filled_side=='bid' else cer_n)} hedge_avg={hedge_avg} "
        f"slip={slippage} edge_at_fill={edge_at_fill} profit={profit} "
        f"lat_detect_to_send_ms={lat_detect_send} lat_hedge_rtt_ms={lat_hedge_rtt} "
        f"s1={sv['s1']:.2f} s05={sv['s05']:.2f} s01={sv['s01']:.2f} "
        f"ticks01={sv['ticks01']} ticks005={sv['ticks005']} "
        f"advY={advy_f:+.4f} advN={advn_f:+.4f} "
        f"depth_hedge_best={dep_best:.1f} depth_hedge_3c={dep_3c:.1f}")

    qprint(f"\n  🔒 MAKER MODE LOCKED. Restart to resume.")
    qprint(f"{'='*70}\n")

# ════════════════════════════════════════════════════════════
# WS FILL DETECTION
# ════════════════════════════════════════════════════════════
async def handle_user_order_update(maker_session, msg: dict):
    global bid_fill_hedged, ask_fill_hedged
    detect_t = time.perf_counter()      # fill detection instant (for latency stats)
    rec_event("UO", info=json.dumps(msg))

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
            await on_maker_filled(maker_session, filled_side, fill_price, order_id, quantity=0.0, detect_t=detect_t)
        return

    fill_price = float(msg.get("yes_price_dollars", "0"))
    if fill_price == 0:
        cost = float(msg.get("maker_fill_cost_dollars", "0"))
        fill_price = (cost / fill_count) if fill_count > 0 else (bid_price or ask_price or 0)

    if not is_fully_filled:
        log("WS_PARTIAL_FILL", f"side={filled_side} fill={fill_count:.4f} rem={remaining:.4f} incr={incremental:.4f}")
        await on_partial_fill(maker_session, filled_side, fill_price, incremental, detect_t=detect_t)
        if filled_side == "bid": bid_fill_hedged = fill_count
        else:                    ask_fill_hedged = fill_count
    else:
        log("WS_FILL_DETECTED", f"side={filled_side} status={status} price={fill_price} incr={incremental:.4f}")
        await on_maker_filled(maker_session, filled_side, fill_price, order_id, quantity=incremental, detect_t=detect_t)

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ════════════════════════════════════════════════════════════
def _live_targets():
    """Read CER prices FRESH from the shared state the WS handler maintains.
    Called at every decision point inside update_maker_orders — including
    again after each cancel round-trip — so quotes are computed from the
    newest tick rather than prices captured before the awaits (#3)."""
    cer_y = curr_prices["CER"]["yes_ask"]
    cer_n = curr_prices["CER"]["no_ask"]
    if cer_y is None or cer_n is None:
        return None
    target_bid = round(max(0.01, 1.0 - cer_y - BID_SPREAD), 2)
    target_ask = round(min(0.99, cer_n + ASK_SPREAD),        2)
    # Rounded to 2 decimal places to bypass floating point representation artifacts
    bid_valid  = round(target_bid + cer_y, 2) <= _R_THRESHOLD
    ask_valid  = round(target_ask - cer_n, 2) >= _R_INV_THRESH
    if DEPTH_GATE_ENFORCE:
        # A gated side is treated as invalid: the normal re-quote path
        # cancels any resting maker on that side and withholds placement
        # until the hedge-side depth recovers (DEPTH_GATE_CLEAR_ABOVE).
        if depth_gate_blocked["bid"]: bid_valid = False
        if depth_gate_blocked["ask"]: ask_valid = False
    if ADV_PULL_ENFORCE:
        # Same mechanism for the ADV pull trigger: a blocked side loses
        # validity, so the resting maker is cancelled and not re-placed
        # until the drift decays (ADV_PULL_CLEAR).
        if adv_pull_blocked["bid"]: bid_valid = False
        if adv_pull_blocked["ask"]: ask_valid = False
    return cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid

def maker_action_needed() -> bool:
    """Cheap synchronous pre-check: only spawn the (task-creating, lock-taking)
    update_maker_orders when something would actually change. Mirrors the
    decision logic inside update_maker_orders exactly."""
    t = _live_targets()
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

    t = _live_targets()
    if t is None:
        return

    updating_orders = True
    try:
        # ── BID ───────────────────────────────────────────
        # Fresh read for the bid decision:
        t = _live_targets()
        if t is None: return
        cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid = t
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

        # Re-read AFTER the cancel round-trip — the WS handler has been
        # updating prices during that await; place at the newest target (#3).
        t = _live_targets()
        if t is None: return
        cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid = t
        if bid_order_id is None and bid_valid and not maker_filled and not market_frozen:
            oid = await place_maker_order(session, "FER", "bid", target_bid)
            if oid and (maker_filled or market_frozen or freeze_cancel_in_progress):
                # World changed while the placement was in flight: never
                # abandon a live order id — retire it (cancel; hedge on 404).
                await retire_orphan_order(session, "bid", target_bid, oid)
                return
            if maker_filled: return
            if oid:
                bid_order_id  = oid
                bid_price     = target_bid
                bid_placed_at = time.time()
                bid_fill_hedged = 0.0

        # ── ASK ───────────────────────────────────────────
        # Fresh read for the ask decision (bid work above took 1-2 RTTs):
        t = _live_targets()
        if t is None: return
        cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid = t
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

        # Re-read after the ask cancel round-trip before placing (#3).
        t = _live_targets()
        if t is None: return
        cer_y, cer_n, target_bid, target_ask, bid_valid, ask_valid = t
        if ask_order_id is None and ask_valid and not maker_filled and not market_frozen:
            oid = await place_maker_order(session, "FER", "ask", target_ask)
            if oid and (maker_filled or market_frozen or freeze_cancel_in_progress):
                await retire_orphan_order(session, "ask", target_ask, oid)
                return
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
def get_speed_stats() -> tuple[str, dict]:
    """Calculates ticks and price delta statistics across sliding windows.
    Returns (formatted string, numeric values dict) — values feed TRADE_SUMMARY."""
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

    text = (
        f"TICKS: {ticks_01s}/0.1s {ticks_005s}/0.05s | "
        f"Δ1s: {m_1s}/Σ{tot_1s:.2f} | "
        f"Δ0.5s: {m_05s}/Σ{tot_05s:.2f} | "
        f"Δ0.1s: {m_01s}/Σ{tot_01s:.2f}"
    )
    values = {"ticks01": ticks_01s, "ticks005": ticks_005s,
              "s1": tot_1s, "s05": tot_05s, "s01": tot_01s}
    return text, values

def get_speed_stats_str() -> str:
    return get_speed_stats()[0]

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
        f"{speed_metrics} | "
        f"{market_state_str()}"
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
    dres = None
    if msg_type == "orderbook_snapshot":
        apply_snapshot(leg, msg); update_derived(leg)
    elif msg_type == "orderbook_delta":
        dres = apply_delta(leg, msg); update_derived(leg)
    else:
        return

    if leg == "CER":
        update_depth_gate()   # every CER book update — price change or not
        update_adv_pull()

    # Record at full resolution (post-update state on every row)
    if msg_type == "orderbook_snapshot":
        rec_event("S", info=json.dumps({s: orderbooks[leg][s] for s in ("yes", "no")}),
                  leg=leg, seq=seq if seq is not None else "")
    elif dres is not None:
        rec_delta(leg, dres[0], dres[1], dres[2], dres[3], seq)

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

async def heartbeat():
    """Periodic state row even when the market is silent — distinguishes
    'quiet market' from 'dead feed' in the recorded data."""
    while True:
        await asyncio.sleep(HEARTBEAT_SECS)
        rec_event("H")

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
    adv_mode = "ENFORCING" if ADV_PULL_ENFORCE else "SHADOW (print only)"
    qprint(f"  ⚡ ADV PULL [{adv_mode}]: pull side when hedge-leg drift ≥ {ADV_PULL_THRESHOLD:.2f} "
           f"over trailing {ADV_PULL_WINDOW_S:.1f}s, re-quote at ≤ {ADV_PULL_CLEAR:.2f}")
    gate_mode = "ENFORCING" if DEPTH_GATE_ENFORCE else "SHADOW (print only)"
    qprint(f"  DEPTH GATE [{gate_mode}]: block side if hedge-side depth < {DEPTH_GATE_BLOCK_BELOW:.0f}, "
           f"clear at ≥ {DEPTH_GATE_CLEAR_ABOVE:.0f}")
    if RECORD_ENABLED:
        qprint(f"  📼 DATA RECORDER: full-resolution stream → {RECORD_DIR}/data_<ts>.csv "
               f"(send this file back for analysis)")
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

        rec_event("SESSION", info=json.dumps({
            "version": BOT_VERSION, "tickers": TICKERS, "game": GAME_DESCRIPTION,
            "bid_spread": BID_SPREAD, "ask_spread": ASK_SPREAD,
            "threshold": THRESHOLD, "taker_buffer": TAKER_BUFFER,
            "update_tick": UPDATE_TICK, "trade_qty": TRADE_QUANTITY,
            "freeze_threshold": FREEZE_TICK_THRESHOLD, "calm_s": CALM_DURATION,
            "adv_enforce": ADV_PULL_ENFORCE, "adv_threshold": ADV_PULL_THRESHOLD,
            "adv_clear": ADV_PULL_CLEAR, "adv_window_s": ADV_PULL_WINDOW_S,
            "record_enabled": RECORD_ENABLED,
            "gate_enforce": DEPTH_GATE_ENFORCE,
            "gate_block_below": DEPTH_GATE_BLOCK_BELOW,
            "gate_clear_above": DEPTH_GATE_CLEAR_ABOVE,
        }))

        refresher_task = asyncio.create_task(signature_refresher())
        keepalive_task = asyncio.create_task(connection_keepalive())
        heartbeat_task = asyncio.create_task(heartbeat())

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
                        rec_event("WS_CONNECT")
                        await cancel_all_makers(maker_session)
                        if maker_filled:
                            continue   # reconcile inferred a fill during the blind window → locked

                        last_seq.clear(); resync_needed.clear()
                        tick_timestamps.clear(); price_history.clear()
                        reset_adv_pull()
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
                    rec_event("WS_DISCONNECT", info=str(e))
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
            heartbeat_task.cancel()

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
        stop_recorder()
        flush_output()
