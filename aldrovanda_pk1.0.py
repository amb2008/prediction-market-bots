"""
ERROR WITH POLY API
POLY-KALSHI CROSS-MARKET MAKER-TAKER ARB  —  x1.0
Makers rest on POLYMARKET US (low/no maker fees); hedges TAKE on KALSHI.
Logic base: aldrovanda 2.30 (single 3c spread per side, lock after fill,
depth gate in shadow, full data recorder, reconnect reconciliation,
mid-loop price refresh, all latency machinery) + the 2.40 orphan-order
race fix backported. NO adverse-drift canceling, NO ladder — per spec.

MARKET STRUCTURE
────────────────
Team A's market lives on Polymarket, team B's on Kalshi, complementary:
A_YES + B_YES = $1. Polymarket does not allow selling YES you don't own,
so BOTH maker sides are implemented as BUYS on Poly:
  "bid" slot: BUY A_YES @ (1 − kalshi_B_yes_ask − SPREAD)
              → on fill, hedge: TAKE B_YES on Kalshi
  "ask" slot: BUY A_NO  @ (1 − kalshi_B_no_ask − SPREAD)
              → on fill, hedge: TAKE B_NO on Kalshi
  (A_NO + B_NO also always pays exactly $1 for complementary teams.)
Either way: total cost ≈ 1 − SPREAD, payout $1.

MAKER-NOT-TAKER GUARANTEE (Poly taker fees are the whole reason for this
architecture): every Poly placement price is clamped strictly BELOW the
live Poly best ask for its outcome (from the market-data-lite stream).
Clamping down only improves the locked edge. If the book is unknown or
the clamp floors out, the placement is skipped.

FILL DETECTION ON POLY
──────────────────────
The provided SDK sample exposes only market data, so fills are detected by
POLLING order status every POLY_POLL_MS for each resting maker (defensive
parsing of status / filled quantity / executions), plus cancel-response
inference during re-quotes (cancel says already-filled → hedge it).
This adds up to ~POLY_POLL_MS of detection latency vs Kalshi's WS push —
the Kalshi hedge itself stays on the pre-signed fast path (~35ms).

⚠ VERIFY AGAINST POLYMARKET US DOCS (marked ⚠VERIFY in code):
  1. GET  /v1/orders/{id}   — order status endpoint + field names
  2. DELETE /v1/orders/{id} — cancel endpoint + "already filled" semantics
  3. Whether the order payload supports a post-only flag (if yes, set it
     in make_poly_order_payload as belt-and-suspenders with the clamp)
Only POST /v1/orders and the signing scheme are taken from your working
script verbatim.

FEES: Kalshi ≈1c/contract taker; Poly maker assumed ~0. SPREAD=0.03 →
~2c net per completed arb. Adjust SPREAD if Poly maker fees are nonzero.
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
from cryptography.hazmat.primitives.asymmetric import padding, ed25519
from cryptography.hazmat.primitives import serialization

try:
    import orjson
    def _jloads(b):  return orjson.loads(b)
    def _jdumps(o):  return orjson.dumps(o)
except ImportError:
    def _jloads(b):  return json.loads(b)
    def _jdumps(o):  return json.dumps(o).encode()

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════
# ── Kalshi (hedge / taker side) ──
KALSHI_KEY_ID    = 'DUMMY_KEY_ID'
KALSHI_KEY_PATH  = r"C:\Users\aberm\OneDrive\Desktop\paper.txt"
KALSHI_REST      = "https://external-api.kalshi.com"
KALSHI_WS        = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
KALSHI_ORDER_PATH = "/trade-api/v2/portfolio/events/orders"
KALSHI_ORDER_URL  = f"{KALSHI_REST}{KALSHI_ORDER_PATH}"
KALSHI_STATUS_PATH = "/trade-api/v2/exchange/status"
KALSHI_TICKER    = "KXATPMATCH-26JUL19WAWBUR-BUR"     # team B on Kalshi

# ── Polymarket US (maker side) ──
POLY_KEY_ID      = 'DUMMY_KEY_ID'
POLY_KEY_PATH    = r"C:\Users\aberm\OneDrive\Desktop\poly.txt"
POLY_REST        = "https://api.polymarket.us"
POLY_ORDER_PATH  = "/v1/orders"
POLY_CANCEL_PATH = "/v1/order"  
POLY_EVENT_SLUG  = "atp-stawaw-rombur-2026-07-20"           # parent event (listing aid)
POLY_MARKET_SLUG = "aec-atp-stawaw-rombur-2026-07-20"   # team A market on Poly

GAME_DESCRIPTION = "Ferrer (Poly) vs Cerundolo (Kalshi)"

TRADE_QUANTITY          = 1
SPREAD                  = 0.01      # both maker sides
THRESHOLD               = 0.99
TAKER_BUFFER            = 0.10      # Kalshi hedge IOC limit buffer
UPDATE_TICK             = 0.01
MIN_ORDER_AGE_FOR_FILL  = 0.5       # Poly detection is poll-based → be lenient
FREEZE_TICK_THRESHOLD   = 0.10
CALM_DURATION           = 2.0
POLY_POLL_MS            = 400       # order-status poll cadence per resting maker
LOG_FILE                = "poly_kalshi_log.csv"

BOT_VERSION             = "x1.0"
RECORD_ENABLED          = True
RECORD_DIR              = "botdata_x"
RECORD_FLUSH_SECS       = 1.0
HEARTBEAT_SECS          = 5.0

DEPTH_GATE_ENFORCE      = False     # shadow only (as in 2.30)
DEPTH_GATE_BLOCK_BELOW  = 100.0
DEPTH_GATE_CLEAR_ABOVE  = 120.0

SIG_REFRESH_SECS        = 2.0
SIG_MAX_AGE_SECS        = 10.0
KEEPALIVE_SECS          = 20.0
# ═══════════════════════════════════════════════════════════

with open(KALSHI_KEY_PATH, "rb") as _fh:
    KALSHI_PRIVATE_KEY = serialization.load_pem_private_key(_fh.read(), password=None)

def _load_poly_key(path: str):
    with open(path, "r") as fh:
        secret_key_str = fh.read().strip()
    private_bytes = base64.b64decode(secret_key_str)[:32]
    return ed25519.Ed25519PrivateKey.from_private_bytes(private_bytes)

POLY_PRIVATE_KEY = _load_poly_key(POLY_KEY_PATH)

_R_THRESHOLD   = round(THRESHOLD, 2)
_R_UPDATE_TICK = round(UPDATE_TICK, 2)

# ── Price state ──────────────────────────────────────────────
# POLY: from market_data_lite (best bid/ask of A_YES; A_NO derived)
# KAL : full orderbook via Kalshi WS (best bids; asks derived)
curr_prices = {
    "POLY": {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None},
    "KAL":  {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None},
}
prev_prices = {k: dict(v) for k, v in curr_prices.items()}
kal_orderbook = {"yes": {}, "no": {}}
kal_best      = {"yes": None, "no": None}      # cents, incremental
last_global_seq: int = -1
resync_needed: set[str] = set()

# ── Maker tracking (single bid + ask, 2.30 style) ───────────
# side "bid" = resting BUY A_YES ; side "ask" = resting BUY A_NO
bid_order_id:    str   | None = None
bid_price:       float | None = None
bid_placed_at:   float | None = None
bid_fill_hedged: float        = 0.0

ask_order_id:    str   | None = None
ask_price:       float | None = None
ask_placed_at:   float | None = None
ask_fill_hedged: float        = 0.0

cancelled_order_ids: set[str] = set()

maker_filled:    bool = False
updating_orders: bool = False

market_frozen:             bool  = False
last_big_move_at:          float = 0.0
freeze_cancel_in_progress: bool  = False

tick_timestamps: deque = deque()
price_history:   deque = deque()

_poly_session:  aiohttp.ClientSession | None = None
_kal_session:   aiohttp.ClientSession | None = None

# ════════════════════════════════════════════════════════════
# NON-BLOCKING CONSOLE + CSV WRITER  (from 2.30)
# ════════════════════════════════════════════════════════════
_out_q: _queue.SimpleQueue = _queue.SimpleQueue()
_WRITER_SENTINEL = object()

def _writer_loop():
    write = sys.stdout.write; flush = sys.stdout.flush
    while True:
        item = _out_q.get()
        if item is _WRITER_SENTINEL:
            flush(); return
        try:
            buf = [item]
            while True:
                try: nxt = _out_q.get_nowait()
                except _queue.Empty: break
                if nxt is _WRITER_SENTINEL:
                    write("".join(buf)); flush(); return
                buf.append(nxt)
            write("".join(buf)); flush()
        except Exception:
            pass

_writer_thread = threading.Thread(target=_writer_loop, daemon=True, name="console-writer")
_writer_thread.start()

def qprint(text: str = ""):
    _out_q.put(text + "\n")

def flush_output(timeout: float = 2.0):
    deadline = time.time() + timeout
    while not _out_q.empty() and time.time() < deadline:
        time.sleep(0.01)

# ════════════════════════════════════════════════════════════
# AUTH — Kalshi RSA-PSS (pre-signed hot path) + Poly Ed25519
# Ed25519 signing is ~50µs, no cache needed for the maker side;
# the Kalshi hedge keeps the pre-signed POST header cache.
# ════════════════════════════════════════════════════════════
def _kal_sign(method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    sig = KALSHI_PRIVATE_KEY.sign(
        (ts + method + path).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": KALSHI_KEY_ID, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "Content-Type": "application/json", "Accept": "application/json"}

_post_hdr: dict | None = None
_post_hdr_at: float = 0.0

def kal_post_headers() -> dict:
    if _post_hdr is not None and (time.time() - _post_hdr_at) <= SIG_MAX_AGE_SECS:
        return _post_hdr
    return _kal_sign("POST", KALSHI_ORDER_PATH)

async def signature_refresher():
    global _post_hdr, _post_hdr_at
    loop = asyncio.get_running_loop()
    while True:
        try:
            hdr = await loop.run_in_executor(None, _kal_sign, "POST", KALSHI_ORDER_PATH)
            _post_hdr, _post_hdr_at = hdr, time.time()
        except Exception:
            pass
        await asyncio.sleep(SIG_REFRESH_SECS)

def poly_headers(method: str, path: str) -> dict:
    """Ed25519 signature — exactly your working scheme from poly_order.py."""
    ts = str(int(time.time() * 1000))
    sig = POLY_PRIVATE_KEY.sign((ts + method + path).encode("utf-8"))
    return {"Content-Type": "application/json",
            "X-PM-Access-Key": POLY_KEY_ID,
            "X-PM-Signature": base64.b64encode(sig).decode("utf-8"),
            "X-PM-Timestamp": ts}

def poly_get_headers(method: str, path: str) -> dict:
    """Ed25519 signature — exactly your working scheme from poly_order.py."""
    ts = str(int(time.time() * 1000))
    sig = POLY_PRIVATE_KEY.sign((ts + method + path).encode("utf-8"))
    return {"X-PM-Access-Key": POLY_KEY_ID,
            "X-PM-Signature": base64.b64encode(sig).decode("utf-8"),
            "X-PM-Timestamp": ts}

# ════════════════════════════════════════════════════════════
# LOGGING + DATA RECORDER  (2.30 format; POLY lite rows added)
# ════════════════════════════════════════════════════════════
def init_log():
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow(["timestamp_utc", "event", "details"])

_rec_q: _queue.SimpleQueue = _queue.SimpleQueue()
_REC_SENTINEL = object()
_REC_HEADER = ["ts_ms", "ev", "leg", "side", "pc", "d", "q",
               "pyb", "pya", "kyb", "knb", "qky", "qkn",
               "mb", "ma", "gate", "seq", "info"]
rec_path: str | None = None

def _recorder_loop():
    global rec_path
    try:
        os.makedirs(RECORD_DIR, exist_ok=True)
        rec_path = os.path.join(RECORD_DIR,
            f"data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv")
        f = open(rec_path, "w", newline="", encoding="utf-8")
        w = csv.writer(f); w.writerow(_REC_HEADER); f.flush()
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
                    try: nxt = _rec_q.get_nowait()
                    except _queue.Empty: break
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

def _pcents(v):
    return int(round(v * 100)) if v is not None else ""

def _market_state_row() -> tuple:
    ky = kal_best["yes"]; kn = kal_best["no"]
    def q(s, b): return kal_orderbook[s].get(b, 0.0) if b is not None else 0.0
    gate = (("B" if depth_gate_blocked["bid"] else "") +
            ("A" if depth_gate_blocked["ask"] else ""))
    return (_pcents(curr_prices["POLY"]["yes_bid"]), _pcents(curr_prices["POLY"]["yes_ask"]),
            ky if ky is not None else "", kn if kn is not None else "",
            q("yes", ky), q("no", kn),
            bid_price if bid_order_id else "", ask_price if ask_order_id else "",
            gate)

def rec_delta(leg, side, pc, d, q_after, seq):
    if not RECORD_ENABLED: return
    _rec_q.put((int(time.time()*1000), "D", leg, side, pc, d, q_after,
                *_market_state_row(), seq if seq is not None else "", ""))

def rec_event(ev, info="", leg="", seq=""):
    if not RECORD_ENABLED: return
    _rec_q.put((int(time.time()*1000), ev, leg, "", "", "", "",
                *_market_state_row(), seq, info))

def stop_recorder(timeout: float = 3.0):
    if not RECORD_ENABLED: return
    _rec_q.put(_REC_SENTINEL)
    _recorder_thread.join(timeout=timeout)

def log(event: str, details: str = ""):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    qprint(f"  📝 [{ts}] {event}  {details}")
    rec_event(event, details)

# ════════════════════════════════════════════════════════════
# KALSHI ORDERBOOK  (single ticker; incremental best tracking)
# ════════════════════════════════════════════════════════════
def check_sequence(seq: int, msg_type: str) -> bool:
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
        resync_needed.add(KALSHI_TICKER)
        last_global_seq = seq
        return False
    return True

def kal_apply_snapshot(msg):
    for side in ("yes", "no"):
        bk = {int(round(float(x[0]) * 100)): float(x[1])
              for x in msg.get(f"{side}_dollars_fp", [])}
        kal_orderbook[side] = bk
        kal_best[side] = max(bk) if bk else None

def kal_apply_delta(msg):
    side = msg.get("side"); price = msg.get("price_dollars"); delta = msg.get("delta_fp")
    if side not in ("yes", "no") or price is None or delta is None: return None
    c = int(round(float(price) * 100)); dq = float(delta)
    bk = kal_orderbook[side]
    nq = bk.get(c, 0.0) + dq
    if nq <= 0.0001:
        bk.pop(c, None)
        if kal_best[side] == c:
            kal_best[side] = max(bk) if bk else None
        nq = 0.0
    else:
        bk[c] = nq
        b = kal_best[side]
        if b is None or c > b:
            kal_best[side] = c
    return (side, c, dq, nq)

def kal_update_derived():
    prev_prices["KAL"] = curr_prices["KAL"].copy()
    yb = kal_best["yes"]; nb = kal_best["no"]
    p = curr_prices["KAL"]
    p["yes_bid"] = round(yb/100, 4) if yb is not None else None
    p["no_ask"]  = round(1 - yb/100, 4) if yb is not None else None
    p["no_bid"]  = round(nb/100, 4) if nb is not None else None
    p["yes_ask"] = round(1 - nb/100, 4) if nb is not None else None

async def kal_resync(session):
    if not resync_needed: return
    try:
        url = f"{KALSHI_REST}/trade-api/v2/markets/{KALSHI_TICKER}/orderbook?depth=100"
        async with session.get(url, headers=_kal_sign("GET",
                f"/trade-api/v2/markets/{KALSHI_TICKER}/orderbook")) as resp:
            data = _jloads(await resp.read())
            book = data.get("orderbook_fp") or data.get("orderbook", {})
            if book:
                kal_apply_snapshot({"yes_dollars_fp": book.get("yes_dollars", []),
                                    "no_dollars_fp":  book.get("no_dollars",  [])})
                kal_update_derived()
                resync_needed.discard(KALSHI_TICKER)
                qprint("  ✅ [KAL] resync complete")
    except Exception as e:
        qprint(f"  ❌ [KAL] resync failed: {e}")

def poly_update_lite(best_bid: float | None, best_ask: float | None):
    """market_data_lite gives A_YES best bid/ask; A_NO book is its mirror."""
    prev_prices["POLY"] = curr_prices["POLY"].copy()
    p = curr_prices["POLY"]
    p["yes_bid"] = round(best_bid, 4) if best_bid is not None else None
    p["yes_ask"] = round(best_ask, 4) if best_ask is not None else None
    p["no_bid"]  = round(1 - best_ask, 4) if best_ask is not None else None
    p["no_ask"]  = round(1 - best_bid, 4) if best_bid is not None else None

# ════════════════════════════════════════════════════════════
# DIAGNOSTICS  (hedge-side = Kalshi; depth gate shadow)
# ════════════════════════════════════════════════════════════
def ask_depth_kal(yes_side: bool) -> tuple[float, float]:
    book_side = "no" if yes_side else "yes"
    bk = kal_orderbook[book_side]; b = kal_best[book_side]
    if b is None or not bk: return 0.0, 0.0
    lo = b - 3; cum = 0.0
    for c, qv in bk.items():
        if lo <= c <= b: cum += qv
    return bk.get(b, 0.0), cum

def adverse_drift(window: float = 1.0) -> tuple[float, float]:
    ky = curr_prices["KAL"]["yes_ask"]; kn = curr_prices["KAL"]["no_ask"]
    now = time.time(); cutoff = now - window
    min_y = ky; min_n = kn
    for t, snap in price_history:
        if t < cutoff: continue
        s = snap["KAL"]
        v = s.get("yes_ask")
        if v is not None and (min_y is None or v < min_y): min_y = v
        v = s.get("no_ask")
        if v is not None and (min_n is None or v < min_n): min_n = v
    advy = round(ky - min_y, 4) if (ky is not None and min_y is not None) else 0.0
    advn = round(kn - min_n, 4) if (kn is not None and min_n is not None) else 0.0
    return advy, advn

depth_gate_blocked = {"bid": False, "ask": False}

def hedge_depth_best(side: str) -> float | None:
    book_side = "no" if side == "bid" else "yes"
    b = kal_best[book_side]
    if b is None: return None
    return kal_orderbook[book_side].get(b, 0.0)

def update_depth_gate():
    for side in ("bid", "ask"):
        depth = hedge_depth_best(side)
        if depth is None: continue
        hedge_name = "KAL_B_YES" if side == "bid" else "KAL_B_NO"
        if not depth_gate_blocked[side] and depth < DEPTH_GATE_BLOCK_BELOW:
            depth_gate_blocked[side] = True
            mode = "blocking" if DEPTH_GATE_ENFORCE else "SHADOW (would block)"
            qprint(f"  🛑 DEPTH GATE: {hedge_name} best-ask depth {depth:.1f} < "
                   f"{DEPTH_GATE_BLOCK_BELOW:.0f} → {mode} {side.upper()} maker")
            log("DEPTH_GATE_BLOCK", f"side={side} depth={depth:.1f} enforce={DEPTH_GATE_ENFORCE}")
        elif depth_gate_blocked[side] and depth >= DEPTH_GATE_CLEAR_ABOVE:
            depth_gate_blocked[side] = False
            qprint(f"  ✅ DEPTH GATE: {hedge_name} depth {depth:.1f} → {side.upper()} clear")
            log("DEPTH_GATE_CLEAR", f"side={side} depth={depth:.1f}")

def market_state_str() -> str:
    ky_b, ky_c = ask_depth_kal(True)
    kn_b, kn_c = ask_depth_kal(False)
    advy, advn = adverse_drift()
    base = (f"KDEPTH KYA:{ky_b:.1f}/{ky_c:.1f} KNA:{kn_b:.1f}/{kn_c:.1f} | "
            f"ADV Y:{advy:+.2f} N:{advn:+.2f}")
    blocked = [s.upper() for s in ("bid", "ask") if depth_gate_blocked[s]]
    if blocked:
        base += f" | 🛑GATE:{'+'.join(blocked)}"
    return base

def check_market_speed() -> bool:
    global market_frozen, last_big_move_at
    max_move = 0.0
    for leg in ("POLY", "KAL"):
        cp = curr_prices[leg]; pp = prev_prices[leg]
        for field in ("yes_ask", "no_ask", "yes_bid", "no_bid"):
            c = cp.get(field); p = pp.get(field)
            if c is not None and p is not None:
                d = abs(c - p)
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
# POLYMARKET ORDER OPS  (maker side — Ed25519 signed REST)
# ════════════════════════════════════════════════════════════
def make_poly_order_payload(side: str, price: float, quantity: int) -> dict:
    """side 'bid' → BUY A_YES ; side 'ask' → BUY A_NO. Payload schema taken
    verbatim from your working poly_order.py.
    ⚠VERIFY: if the API supports a post-only flag, add it here."""
    return {
        "marketSlug":  POLY_MARKET_SLUG,
        "type":        "ORDER_TYPE_LIMIT",
        "action":      "ORDER_ACTION_BUY",
        "outcomeSide": "OUTCOME_SIDE_YES" if side == "bid" else "OUTCOME_SIDE_NO",
        "quantity":    quantity,
        "price":       {"value": f"{price:.2f}", "currency": "USD"},
        "tif":         "TIME_IN_FORCE_GOOD_TILL_CANCEL",
    }

async def poly_place_maker(side: str, price: float) -> str | None:
    if maker_filled or market_frozen or freeze_cancel_in_progress:
        log("MAKER_PLACE_SUPPRESSED", f"side={side} price={price} "
            f"filled={maker_filled} frozen={market_frozen}")
        return None
    payload = make_poly_order_payload(side, price, TRADE_QUANTITY)
    try:
        async with _poly_session.post(f"{POLY_REST}{POLY_ORDER_PATH}",
                                      data=_jdumps(payload),
                                      headers=poly_headers("POST", POLY_ORDER_PATH)) as resp:
            res = _jloads(await resp.read())
            order_id = res.get("id") or res.get("order", {}).get("id")
            if resp.status == 200 and order_id:
                outcome = "A_YES" if side == "bid" else "A_NO"
                qprint(f"  📌 POLY MAKER BUY {outcome} @ ${price:.2f} | id={order_id}")
                log("MAKER_PLACED", f"side={side} outcome={outcome} price={price} id={order_id}")
                # If the venue matched us instantly despite the clamp, the
                # executions field exposes it — treat as an immediate fill.
                if res.get("executions"):
                    log("MAKER_INSTANT_EXECUTION", f"side={side} id={order_id} — clamp breached, treating as fill")
                return order_id
            qprint(f"  ⚠️  Poly place failed ({side}): {resp.status} {res}")
            log("MAKER_PLACE_FAIL", f"status={resp.status} {res}")
            return None
    except Exception as e:
        qprint(f"  ❌ Poly order error: {e}")
        return None

async def poly_cancel(order_id: str) -> str:
    """⚠VERIFY: DELETE /v1/orders/{id} assumed. Returns 'confirmed',
    'already_filled', or 'error'. Defensive parsing on the response."""
    path = f"{POLY_CANCEL_PATH}/{order_id}/cancel"
    try:
        async with _poly_session.post(f"{POLY_REST}{path}", json={"marketSlug": POLY_MARKET_SLUG},
                                        headers=poly_headers("POST", path)) as resp:
            body = await resp.text()
            if resp.status in (200, 204):
                cancelled_order_ids.add(order_id)
                qprint(f"  🗑  Poly cancelled {order_id[:12]}… ({resp.status})")
                log("ORDER_CANCELLED", order_id)
                return "confirmed"
            low = body.lower()
            if resp.status in (404, 409, 410) or "filled" in low or "executed" in low:
                qprint(f"  ⚠️  Poly cancel says gone/filled for {order_id[:12]}… ({resp.status})")
                log("CANCEL_GONE_UNCONFIRMED", f"order={order_id} status={resp.status}")
                return "already_filled"
            qprint(f"  ⚠️  Poly cancel {order_id[:12]}… status={resp.status}: {body[:100]}")
            log("CANCEL_ERROR", f"order={order_id} status={resp.status}")
            return "error"
    except Exception as e:
        qprint(f"  ❌ Poly cancel error: {e}")
        return "error"

def _parse_poly_fill(res: dict) -> tuple[float, float | None, str]:
    """Defensively extract (filled_qty, avg_price, status) from an order-status
    response. ⚠VERIFY field names against docs; several conventions handled."""
    o = res.get("order", res) if isinstance(res, dict) else {}
    status = str(o.get("status", "")).lower()
    filled = 0.0
    for k in ("filledQuantity", "filled_quantity", "filledQty", "executedQuantity"):
        if k in o:
            try: filled = float(o[k]); break
            except (TypeError, ValueError): pass
    avg = None
    for k in ("avgFillPrice", "averageFillPrice", "avg_fill_price"):
        v = o.get(k)
        if isinstance(v, dict): v = v.get("value")
        if v is not None:
            try: avg = float(v); break
            except (TypeError, ValueError): pass
    exes = o.get("executions") or res.get("executions") or []
    if exes and filled == 0.0:
        tq = 0.0; tv = 0.0
        for e in exes:
            q = float(e.get("quantity", 0))
            pv = e.get("price"); pv = float(pv.get("value")) if isinstance(pv, dict) else float(pv or 0)
            tq += q; tv += q * pv
        filled = tq
        if tq > 0 and avg is None: avg = tv / tq
    return filled, avg, status

async def poly_order_status(order_id: str) -> dict | None:
    """⚠VERIFY: GET /v1/orders/{id} assumed."""
    path = f"{POLY_CANCEL_PATH}/{order_id}"
    try:
        async with _poly_session.get(f"{POLY_REST}{path}",
                                     headers=poly_get_headers("GET", path)) as resp:
            print(f"  🔍 Poly order status {order_id[:12]}… ({resp.status})")
            if resp.status == 200:
                return _jloads(await resp.read())
            return None
    except Exception:
        return None

# ════════════════════════════════════════════════════════════
# KALSHI TAKER (hedge)  — pre-signed fast path from 2.30
# ════════════════════════════════════════════════════════════
def _build_kal_taker(v2_side: str, price: float, quantity: float) -> tuple[bytes, dict]:
    price = round(min(0.99, max(0.01, price)), 4)
    payload = {
        "ticker": KALSHI_TICKER,
        "client_order_id": str(uuid.uuid4()),
        "side": v2_side,
        "count": f"{quantity:.2f}",
        "price": f"{price:.4f}",
        "time_in_force": "immediate_or_cancel",
        "self_trade_prevention_type": "taker_at_cross",
    }
    return _jdumps(payload), kal_post_headers()

async def fire_kal_taker(body: bytes, headers: dict) -> tuple[bool, dict]:
    try:
        async with _kal_session.post(KALSHI_ORDER_URL, data=body, headers=headers) as resp:
            res = _jloads(await resp.read())
            return resp.status == 201 and float(res.get("fill_count", "0")) > 0, res
    except Exception as e:
        return False, {"error": str(e)}

# ════════════════════════════════════════════════════════════
# FILL HANDLER  (lock after fill, 2.30 semantics)
# hedge legs: bid fill (long A_YES) → TAKE KAL B_YES
#             ask fill (long A_NO)  → TAKE KAL B_NO
# ════════════════════════════════════════════════════════════
async def on_maker_filled(filled_side: str, fill_price: float, order_id: str,
                          quantity: float | None = None, detect_t: float | None = None):
    global maker_filled
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged

    if maker_filled:
        return
    hedge_qty = TRADE_QUANTITY if quantity is None else quantity
    maker_filled = True

    other_id = ask_order_id if filled_side == "bid" else bid_order_id
    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0

    advy_f, advn_f = adverse_drift()
    stats_f = get_speed_stats()
    dep_best, dep_3c = ask_depth_kal(filled_side == "bid")

    taker_task = None; taker_px = None; mkt = None; t_sent = None
    if hedge_qty > 0.001:
        if filled_side == "bid":
            mkt = curr_prices["KAL"].get("yes_ask") or 0.99
            taker_px = round(min(0.99, mkt + TAKER_BUFFER), 4)
            body, headers = _build_kal_taker("bid", taker_px, hedge_qty)
        else:
            mkt = curr_prices["KAL"].get("no_ask") or 0.99
            taker_px = round(max(0.01, 1.0 - mkt - TAKER_BUFFER), 4)
            body, headers = _build_kal_taker("ask", taker_px, hedge_qty)
        taker_task = asyncio.create_task(fire_kal_taker(body, headers))
        await asyncio.sleep(0)
        t_sent = time.perf_counter()
        log("HEDGE_SENT", f"side={filled_side} qty={hedge_qty:.4f} limit={taker_px}")

    cancel_task = None
    if other_id:
        cancel_task = asyncio.create_task(poly_cancel(other_id))

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    what = "A_YES" if filled_side == "bid" else "A_NO"
    hedge_leg = "KAL B_YES" if filled_side == "bid" else "KAL B_NO"
    qprint(f"\n{'='*70}")
    qprint(f"  🔥 [{ts}] POLY MAKER FILLED: BUY {what} @ ${fill_price:.4f}  hedge_qty={hedge_qty:.4f}")
    qprint(f"  id={order_id}")
    log("MAKER_FILLED", f"side={filled_side} fill_price={fill_price} hedge_qty={hedge_qty:.4f} id={order_id}")

    if hedge_qty <= 0.001:
        qprint(f"  ✅ All quantity already hedged via prior partial fills.")
        log("HEDGE_SKIPPED", "all qty hedged via partial fills")
        filled = False; res = {}
    else:
        qprint(f"  🎯 Hedge: TAKE {hedge_leg}  mkt={mkt:.4f}  limit=${taker_px:.4f}")
        log("HEDGE_ATTEMPT", f"TAKE {hedge_leg} qty={hedge_qty:.4f} limit={taker_px} mkt={mkt}")
        if other_id:
            qprint(f"  🗑  Cancelling other Poly maker {other_id[:12]}… (post-hedge)")
        filled, res = await taker_task
    t_resp = time.perf_counter()

    if cancel_task is not None:
        await cancel_task

    profit = None; hedge_avg = None; slippage = None; edge_at_fill = None
    if hedge_qty > 0.001:
        edge_at_fill = round(1.0 - (fill_price + mkt), 4)
        if filled:
            if filled_side == "bid":
                avg = float(res.get("average_fill_price", taker_px))
            else:
                avg = round(1.0 - float(res.get("average_fill_price", taker_px)), 4)
            total = round(fill_price + avg, 4)
            hedge_avg = avg; slippage = round(avg - mkt, 4)
            profit = round(1 - total, 4)
            qprint(f"  ✅ Hedge FILLED: {hedge_leg} cost=${avg:.4f}  total=${total:.4f}")
            qprint(f"  💰 LOCKED PROFIT: $1.00 − ${total:.4f} = ${profit:.4f}")
            log("HEDGE_FILLED", f"avg={avg} total={total} profit={profit}")
        else:
            qprint(f"  🚨 HEDGE FAILED: TAKE {hedge_leg} @ {taker_px} → {res}")
            qprint(f"  🚨 HOLDING {hedge_qty:.4f}× POLY {what} unhedged — CLOSE MANUALLY")
            log("HEDGE_MISSED", f"TAKE {hedge_leg} qty={hedge_qty:.4f} limit={taker_px}: {res}")

    qprint(f"  📊 FILL SPEED STATISTICS: {stats_f[0]}")
    lat_detect_send = round((t_sent - detect_t) * 1000, 2) if (t_sent and detect_t) else -1
    lat_hedge_rtt   = round((t_resp - t_sent) * 1000, 2) if t_sent else -1
    sv = stats_f[1]
    log("TRADE_SUMMARY",
        f"side={filled_side} fill={fill_price:.4f} qty={hedge_qty:.4f} "
        f"hedge_mkt={mkt} hedge_avg={hedge_avg} slip={slippage} "
        f"edge_at_fill={edge_at_fill} profit={profit} "
        f"lat_detect_to_send_ms={lat_detect_send} lat_hedge_rtt_ms={lat_hedge_rtt} "
        f"s1={sv['s1']:.2f} s05={sv['s05']:.2f} s01={sv['s01']:.2f} "
        f"ticks01={sv['ticks01']} ticks005={sv['ticks005']} "
        f"advY={advy_f:+.4f} advN={advn_f:+.4f} "
        f"depth_hedge_best={dep_best:.1f} depth_hedge_3c={dep_3c:.1f}")
    qprint(f"\n  🔒 MAKER MODE LOCKED. Restart to resume.")
    qprint(f"{'='*70}\n")

async def on_partial_fill(filled_side: str, fill_price: float, quantity: float,
                          detect_t: float | None = None):
    if filled_side == "bid":
        mkt = curr_prices["KAL"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, mkt + TAKER_BUFFER), 4)
        body, headers = _build_kal_taker("bid", taker_px, quantity)
        hedge_leg = "KAL B_YES"
    else:
        mkt = curr_prices["KAL"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - mkt - TAKER_BUFFER), 4)
        body, headers = _build_kal_taker("ask", taker_px, quantity)
        hedge_leg = "KAL B_NO"
    taker_task = asyncio.create_task(fire_kal_taker(body, headers))
    await asyncio.sleep(0)
    log("HEDGE_SENT", f"side={filled_side} qty={quantity:.4f} limit={taker_px} partial=1")
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    qprint(f"\n  ⚡ [{ts}] POLY PARTIAL FILL: {filled_side.upper()} @ ${fill_price:.4f}  qty={quantity:.4f}")
    log("PARTIAL_FILL", f"side={filled_side} price={fill_price} qty={quantity:.4f}")
    qprint(f"  🎯 Partial Hedge: TAKE {hedge_leg}  mkt={mkt:.4f}  limit=${taker_px:.4f}")
    log("PARTIAL_HEDGE_ATTEMPT", f"TAKE {hedge_leg} qty={quantity:.4f} limit={taker_px}")
    filled, res = await taker_task
    if filled:
        qprint(f"  ✅ Partial Hedge FILLED")
        log("PARTIAL_HEDGE_FILLED", f"{res.get('fill_count')} @ {res.get('average_fill_price')}")
    else:
        qprint(f"  🚨 PARTIAL HEDGE FAILED → {res}")
        log("PARTIAL_HEDGE_MISSED", f"side={filled_side} qty={quantity:.4f}: {res}")

# ════════════════════════════════════════════════════════════
# POLY FILL POLLER  — primary fill detection (no user WS shown
# in the SDK sample, so we poll each resting maker's status)
# ════════════════════════════════════════════════════════════
async def poly_fill_poller():
    global bid_fill_hedged, ask_fill_hedged
    while True:
        await asyncio.sleep(POLY_POLL_MS / 1000.0)
        if maker_filled:
            continue
        for side in ("bid", "ask"):
            oid = bid_order_id if side == "bid" else ask_order_id
            if oid is None or maker_filled:
                continue
            res = await poly_order_status(oid)
            if res is None:
                continue
            detect_t = time.perf_counter()
            filled_qty, avg, status = _parse_poly_fill(res)
            already = bid_fill_hedged if side == "bid" else ask_fill_hedged
            incremental = round(filled_qty - already, 6)
            fully = status in ("filled", "order_status_filled") or filled_qty >= TRADE_QUANTITY
            px = avg if avg is not None else (bid_price if side == "bid" else ask_price) or 0
            if incremental >= 0.001:
                if fully:
                    log("POLL_FILL_DETECTED", f"side={side} status={status} qty={filled_qty}")
                    await on_maker_filled(side, px, oid, quantity=incremental, detect_t=detect_t)
                else:
                    log("POLL_PARTIAL_FILL", f"side={side} filled={filled_qty} incr={incremental}")
                    await on_partial_fill(side, px, incremental, detect_t=detect_t)
                    if side == "bid": bid_fill_hedged = filled_qty
                    else:             ask_fill_hedged = filled_qty
            elif fully and incremental < 0.001 and filled_qty > 0:
                await on_maker_filled(side, px, oid, quantity=0.0, detect_t=detect_t)

# ════════════════════════════════════════════════════════════
# ORDER MANAGEMENT  (single bid/ask, mid-loop refresh, orphan fix,
# maker-not-taker clamp against the live Poly book)
# ════════════════════════════════════════════════════════════
def _live_targets():
    """Targets priced off KALSHI (the hedge venue), clamped to be MAKERS on
    the Poly book. Fresh reads at every decision point (mid-loop refresh)."""
    ky = curr_prices["KAL"]["yes_ask"]
    kn = curr_prices["KAL"]["no_ask"]
    if ky is None or kn is None:
        return None
    target_bid = round(max(0.01, 1.0 - ky - SPREAD), 2)     # BUY A_YES
    target_ask = round(max(0.01, 1.0 - kn - SPREAD), 2)     # BUY A_NO
    # ── maker clamp: never cross the Poly book (taker fees!) ──
    pya = curr_prices["POLY"]["yes_ask"]                     # A_YES best ask
    pna = curr_prices["POLY"]["no_ask"]                      # A_NO  best ask
    if pya is not None and target_bid >= round(pya, 2):
        target_bid = round(pya - 0.01, 2)
    if pna is not None and target_ask >= round(pna, 2):
        target_ask = round(pna - 0.01, 2)
    # validity: total cost must stay ≤ THRESHOLD, and price must be placeable
    bid_valid = target_bid >= 0.01 and round(target_bid + ky, 2) <= _R_THRESHOLD
    ask_valid = target_ask >= 0.01 and round(target_ask + kn, 2) <= _R_THRESHOLD
    if DEPTH_GATE_ENFORCE:
        if depth_gate_blocked["bid"]: bid_valid = False
        if depth_gate_blocked["ask"]: ask_valid = False
    return ky, kn, target_bid, target_ask, bid_valid, ask_valid

def maker_action_needed() -> bool:
    t = _live_targets()
    if t is None: return False
    ky, kn, tb, ta, bv, av = t
    if bid_order_id is None:
        if bv: return True
    else:
        if bid_price is None or round(abs(tb - bid_price), 2) >= _R_UPDATE_TICK: return True
        if round(bid_price + ky, 2) > _R_THRESHOLD: return True
        if not bv: return True
    if ask_order_id is None:
        if av: return True
    else:
        if ask_price is None or round(abs(ta - ask_price), 2) >= _R_UPDATE_TICK: return True
        if round(ask_price + kn, 2) > _R_THRESHOLD: return True
        if not av: return True
    return False

async def handle_gone_order(side: str, oid: str, placed_at, hedged: float):
    """A Poly cancel reported already-filled/gone: confirm via status, hedge."""
    res = await poly_order_status(oid)
    detect_t = time.perf_counter()
    filled_qty, avg, status = _parse_poly_fill(res or {})
    age = time.time() - (placed_at or time.time())
    if filled_qty > 0.001 or (res is None and age >= MIN_ORDER_AGE_FOR_FILL):
        rqty = max(0.0, (filled_qty or TRADE_QUANTITY) - hedged)
        px = avg if avg is not None else (bid_price if side == "bid" else ask_price) or 0
        log("FILL_INFERRED_CANCEL", f"{side} id={oid} qty={rqty:.4f} status={status}")
        if rqty > 0.001:
            await on_maker_filled(side, px, oid, quantity=rqty, detect_t=detect_t)
    else:
        log("CANCEL_GONE_NO_FILL", f"{side} id={oid} status={status} — treating as cancelled")

async def update_maker_orders():
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    global updating_orders

    if maker_filled or updating_orders or market_frozen or freeze_cancel_in_progress:
        return
    if _live_targets() is None:
        return

    updating_orders = True
    try:
        # ── BID (BUY A_YES) ────────────────────────────────
        t = _live_targets()
        if t is None: return
        ky, kn, target_bid, target_ask, bid_valid, ask_valid = t
        if bid_order_id is not None:
            stale = bid_price is None or round(abs(target_bid - bid_price), 2) >= _R_UPDATE_TICK
            costly = bid_price is not None and round(bid_price + ky, 2) > _R_THRESHOLD
            if stale or costly or not bid_valid:
                oid = bid_order_id
                if maker_filled: return
                result = await poly_cancel(oid)
                if maker_filled: return
                if result == "confirmed":
                    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
                elif result == "already_filled":
                    pa, h = bid_placed_at, bid_fill_hedged
                    bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
                    await handle_gone_order("bid", oid, pa, h)
                    return
                else:
                    return

        t = _live_targets()          # fresh after the cancel round-trip
        if t is None: return
        ky, kn, target_bid, target_ask, bid_valid, ask_valid = t
        if bid_order_id is None and bid_valid and not maker_filled and not market_frozen:
            oid = await poly_place_maker("bid", target_bid)
            if oid and (maker_filled or market_frozen or freeze_cancel_in_progress):
                await retire_orphan_order("bid", target_bid, oid)
                return
            if maker_filled: return
            if oid:
                bid_order_id = oid; bid_price = target_bid
                bid_placed_at = time.time(); bid_fill_hedged = 0.0

        # ── ASK (BUY A_NO) ─────────────────────────────────
        t = _live_targets()
        if t is None: return
        ky, kn, target_bid, target_ask, bid_valid, ask_valid = t
        if ask_order_id is not None:
            stale = ask_price is None or round(abs(target_ask - ask_price), 2) >= _R_UPDATE_TICK
            costly = ask_price is not None and round(ask_price + kn, 2) > _R_THRESHOLD
            if stale or costly or not ask_valid:
                oid = ask_order_id
                if maker_filled: return
                result = await poly_cancel(oid)
                if maker_filled: return
                if result == "confirmed":
                    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0
                elif result == "already_filled":
                    pa, h = ask_placed_at, ask_fill_hedged
                    ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0
                    await handle_gone_order("ask", oid, pa, h)
                    return
                else:
                    return

        t = _live_targets()
        if t is None: return
        ky, kn, target_bid, target_ask, bid_valid, ask_valid = t
        if ask_order_id is None and ask_valid and not maker_filled and not market_frozen:
            oid = await poly_place_maker("ask", target_ask)
            if oid and (maker_filled or market_frozen or freeze_cancel_in_progress):
                await retire_orphan_order("ask", target_ask, oid)
                return
            if maker_filled: return
            if oid:
                ask_order_id = oid; ask_price = target_ask
                ask_placed_at = time.time(); ask_fill_hedged = 0.0
    finally:
        updating_orders = False

async def retire_orphan_order(side: str, price: float, order_id: str):
    """2.40 backport: a placement that completed after a fill/freeze is never
    abandoned — cancel it; if it's already gone it filled → hedge (emergency
    path fires even in locked mode)."""
    qprint(f"  🚨 ORPHAN ORDER: {side.upper()} {order_id[:12]}… placed during "
           f"fill/freeze race — retiring")
    log("ORPHAN_ORDER_RETIRE", f"side={side} price={price} id={order_id} "
                               f"filled={maker_filled} frozen={market_frozen}")
    result = await poly_cancel(order_id)
    if result == "confirmed":
        qprint(f"  ✅ Orphan {order_id[:12]}… cancelled cleanly")
        return
    if result == "error":
        result = await poly_cancel(order_id)
        if result == "confirmed":
            return
    if result == "already_filled":
        await asyncio.sleep(0.1)
        res = await poly_order_status(order_id)
        filled_qty, avg, status = _parse_poly_fill(res or {})
        qty = filled_qty if filled_qty > 0.001 else TRADE_QUANTITY
        px = avg if avg is not None else price
        log("ORPHAN_FILL_INFERRED", f"side={side} price={px} qty={qty} id={order_id}")
        if not maker_filled:
            await on_maker_filled(side, px, order_id, quantity=qty)
        else:
            await emergency_hedge(side, px, order_id, qty)
        return
    qprint(f"  🚨 Orphan {order_id[:12]}… could not be cancelled — VERIFY MANUALLY")
    log("ORPHAN_UNRESOLVED", f"side={side} id={order_id}")

async def emergency_hedge(filled_side: str, fill_price: float, order_id: str, quantity: float):
    if filled_side == "bid":
        mkt = curr_prices["KAL"].get("yes_ask") or 0.99
        taker_px = round(min(0.99, mkt + TAKER_BUFFER), 4)
        body, headers = _build_kal_taker("bid", taker_px, quantity)
        leg = "KAL B_YES"
    else:
        mkt = curr_prices["KAL"].get("no_ask") or 0.99
        taker_px = round(max(0.01, 1.0 - mkt - TAKER_BUFFER), 4)
        body, headers = _build_kal_taker("ask", taker_px, quantity)
        leg = "KAL B_NO"
    taker_task = asyncio.create_task(fire_kal_taker(body, headers))
    await asyncio.sleep(0)
    qprint(f"  🚑 EMERGENCY HEDGE for orphan fill {order_id[:12]}…: TAKE {leg}")
    log("EMERGENCY_HEDGE_ATTEMPT", f"side={filled_side} order={order_id} TAKE {leg} limit={taker_px}")
    filled, res = await taker_task
    if filled:
        qprint(f"  ✅ EMERGENCY HEDGE FILLED")
        log("EMERGENCY_HEDGE_FILLED", f"side={filled_side} order={order_id} {res.get('average_fill_price')}")
    else:
        qprint(f"  🚨 EMERGENCY HEDGE FAILED → {res}")
        qprint(f"  🚨 UNHEDGED POLY POSITION from {order_id} — CLOSE MANUALLY")
        log("EMERGENCY_HEDGE_MISSED", f"side={filled_side} order={order_id}: {res}")

async def cancel_all_makers(reconcile: bool = True):
    """Reconnect/shutdown reconciliation — cancel results checked; gone
    orders confirmed via status and hedged if they filled while blind."""
    global bid_order_id, bid_price, bid_placed_at, bid_fill_hedged
    global ask_order_id, ask_price, ask_placed_at, ask_fill_hedged
    for side in ("bid", "ask"):
        oid = bid_order_id if side == "bid" else ask_order_id
        if oid is None:
            continue
        pa = bid_placed_at if side == "bid" else ask_placed_at
        h  = bid_fill_hedged if side == "bid" else ask_fill_hedged
        result = await poly_cancel(oid)
        if side == "bid":
            bid_order_id = bid_price = bid_placed_at = None; bid_fill_hedged = 0.0
        else:
            ask_order_id = ask_price = ask_placed_at = None; ask_fill_hedged = 0.0
        if result == "already_filled" and reconcile and not maker_filled:
            await handle_gone_order(side, oid, pa, h)

# ════════════════════════════════════════════════════════════
# SPEED METRICS + LIVE PRINTOUT
# ════════════════════════════════════════════════════════════
def get_speed_stats() -> tuple[str, dict]:
    now = time.time()
    ticks_01s = 0; ticks_005s = 0
    c01 = now - 0.1; c005 = now - 0.05
    for t in reversed(tick_timestamps):
        if t < c01: break
        ticks_01s += 1
        if t >= c005: ticks_005s += 1
    p_1s = p_05s = p_01s = None
    b1, b05, b01 = now - 1.0, now - 0.5, now - 0.1
    for t, snap in price_history:
        if p_1s  is None and t >= b1:  p_1s  = snap
        if p_05s is None and t >= b05: p_05s = snap
        if t >= b01:
            if p_01s is None: p_01s = snap
            break
    def cd(old):
        if not old: return "N/A", 0.0
        mn = "None"; mv = -1.0; tot = 0.0
        for k in ("POLY", "KAL"):
            for f in ("yes_ask", "no_ask", "yes_bid", "no_bid"):
                c = curr_prices[k].get(f); o = old[k].get(f)
                if c is not None and o is not None:
                    d = abs(c - o); tot += d
                    if d > mv:
                        mv = d
                        mn = f"{k}_{'Y' if 'yes' in f else 'N'}{'A' if 'ask' in f else 'B'}"
        return (f"{mn}({mv:+.2f})" if mv > 0.0001 else "None(0.00)"), tot
    m1, t1 = cd(p_1s); m05, t05 = cd(p_05s); m01, t01 = cd(p_01s)
    text = (f"TICKS: {ticks_01s}/0.1s {ticks_005s}/0.05s | "
            f"Δ1s: {m1}/Σ{t1:.2f} | Δ0.5s: {m05}/Σ{t05:.2f} | Δ0.1s: {m01}/Σ{t01:.2f}")
    return text, {"ticks01": ticks_01s, "ticks005": ticks_005s, "s1": t1, "s05": t05, "s01": t01}

last_printed = {"POLY": {}, "KAL": {}}

def print_live_odds():
    changed = any(curr_prices[k].get(f) != last_printed.get(k, {}).get(f)
                  for k in ("POLY", "KAL") for f in ("yes_ask", "no_ask"))
    if not changed:
        return
    for k in ("POLY", "KAL"):
        last_printed[k] = {"yes_ask": curr_prices[k].get("yes_ask"),
                           "no_ask":  curr_prices[k].get("no_ask")}
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    def fmt(v): return f"${v:.2f}" if v is not None else "Wait.."
    p = curr_prices["POLY"]; kk = curr_prices["KAL"]
    status_flag = " 🔒LOCKED" if maker_filled else (" 🧊FROZEN" if market_frozen else "")
    bid_stat = f"YES-BID@{bid_price:.2f}({bid_order_id[-6:]})" if bid_order_id else "→wait"
    ask_stat = f"NO-BID@{ask_price:.2f}({ask_order_id[-6:]})" if ask_order_id else "→wait"
    qprint(f"{ts}{status_flag}  | "
           f"POLY_Y: {fmt(p['yes_bid'])}/{fmt(p['yes_ask'])} | "
           f"KAL_Y: {fmt(kk['yes_ask']):<6} KAL_N: {fmt(kk['no_ask']):<6} | "
           f"MAKERS: {bid_stat} / {ask_stat} | "
           f"{get_speed_stats()[0]} | {market_state_str()}")

def _track_tick(now):
    tick_timestamps.append(now)
    price_history.append((now, {k: curr_prices[k].copy() for k in curr_prices}))
    cutoff = now - 1.05
    while tick_timestamps and tick_timestamps[0] < cutoff:
        tick_timestamps.popleft()
    while price_history and price_history[0][0] < cutoff:
        price_history.popleft()

def _after_market_update(kal_session_ref):
    print_live_odds()
    newly_frozen = check_market_speed()
    if newly_frozen and (bid_order_id or ask_order_id):
        qprint("  🧊 Cancelling all resting Poly makers due to freeze…")
        asyncio.create_task(freeze_cancel())
    if (not maker_filled and not market_frozen and not updating_orders
            and maker_action_needed()):
        asyncio.create_task(update_maker_orders())

async def freeze_cancel():
    global freeze_cancel_in_progress
    if freeze_cancel_in_progress:
        return
    freeze_cancel_in_progress = True
    try:
        while updating_orders:
            await asyncio.sleep(0.01)
        await cancel_all_makers(reconcile=True)
    finally:
        freeze_cancel_in_progress = False

# ════════════════════════════════════════════════════════════
# MARKET DATA LOOPS  (Kalshi WS  +  Polymarket SDK lite stream)
# ════════════════════════════════════════════════════════════
async def kalshi_ws_loop():
    ws_headers = _kal_sign("GET", "/trade-api/ws/v2")
    while True:
        if maker_filled:
            await asyncio.sleep(5); continue
        try:
            async with websockets.connect(KALSHI_WS, additional_headers=ws_headers,
                                          ping_interval=15, compression=None,
                                          max_queue=1024) as ws:
                qprint(f"✅ Kalshi WS connected")
                rec_event("WS_CONNECT", leg="KAL")
                global last_global_seq
                last_global_seq = -1
                kal_orderbook["yes"] = {}; kal_orderbook["no"] = {}
                kal_best["yes"] = kal_best["no"] = None
                await ws.send(json.dumps({"id": 1, "cmd": "subscribe",
                    "params": {"channels": ["orderbook_delta"],
                               "market_tickers": [KALSHI_TICKER]}}))
                async for raw in ws:
                    md = _jloads(raw)
                    msg_type = md.get("type"); msg = md.get("msg", {}); seq = md.get("seq")
                    if msg_type == "error":
                        qprint(f"  ❌ Kalshi WS error: {md}"); continue
                    if msg.get("market_ticker") != KALSHI_TICKER:
                        continue
                    if seq is not None and not check_sequence(seq, msg_type):
                        await kal_resync(_kal_session); continue
                    now = time.time()
                    dres = None
                    if msg_type == "orderbook_snapshot":
                        kal_apply_snapshot(msg); kal_update_derived()
                        rec_event("S", info=json.dumps(kal_orderbook), leg="KAL",
                                  seq=seq if seq is not None else "")
                    elif msg_type == "orderbook_delta":
                        dres = kal_apply_delta(msg); kal_update_derived()
                        if dres:
                            rec_delta("KAL", dres[0], dres[1], dres[2], dres[3], seq)
                    else:
                        continue
                    update_depth_gate()
                    _track_tick(now)
                    _after_market_update(_kal_session)
        except Exception as e:
            rec_event("WS_DISCONNECT", info=str(e), leg="KAL")
            qprint(f"\n❌ Kalshi WS error: {e}. Cancelling makers + retrying in 3s…")
            await cancel_all_makers()
            await asyncio.sleep(3)

async def poly_lite_loop():
    """Polymarket market-data-lite via the official SDK (as in your poly.py).
    Falls back to a loud error if the SDK is missing."""
    try:
        from polymarket_us import PolymarketUS
    except ImportError:
        qprint("❌ polymarket_us SDK not installed — pip install polymarket-us")
        return
    with open(POLY_KEY_PATH, "r") as fh:
        secret = fh.read().strip()
    while True:
        if maker_filled:
            await asyncio.sleep(5); continue
        try:
            client = PolymarketUS(key_id=POLY_KEY_ID, secret_key=secret)
            ws = client.ws.markets()

            def on_lite(d):
                try:
                    md = d.get("marketDataLite", d)
                    bb = md.get("bestBid", {}); ba = md.get("bestAsk", {})
                    bbv = float(bb.get("value")) if bb and bb.get("value") is not None else None
                    bav = float(ba.get("value")) if ba and ba.get("value") is not None else None
                    poly_update_lite(bbv, bav)
                    rec_event("PL", info=f"{bbv},{bav}", leg="POLY")
                    now = time.time()
                    _track_tick(now)
                    _after_market_update(_kal_session)
                except Exception as e:
                    qprint(f"  ⚠️ poly lite parse: {e}")

            ws.on("market_data_lite", on_lite)
            qprint(f"✅ Polymarket lite stream connecting: {POLY_MARKET_SLUG}")
            await ws.connect()
            await ws.subscribe("prices", "SUBSCRIPTION_TYPE_MARKET_DATA_LITE",
                               [POLY_MARKET_SLUG])
            rec_event("WS_CONNECT", leg="POLY")
            while True:              # SDK delivers via callbacks; keep alive
                await asyncio.sleep(3600)
        except Exception as e:
            rec_event("WS_DISCONNECT", info=str(e), leg="POLY")
            qprint(f"\n❌ Poly stream error: {e}. Retrying in 3s…")
            await asyncio.sleep(3)

# ════════════════════════════════════════════════════════════
# WARMUP / KEEPALIVE / HEARTBEAT / MAIN
# ════════════════════════════════════════════════════════════
async def _warm(session, base, path, headers):
    try:
        async with session.get(f"{base}{path}", headers=headers,
                               timeout=aiohttp.ClientTimeout(total=3)) as r:
            await r.read()
    except Exception:
        pass

async def warm_connections():
    await asyncio.gather(
        _warm(_kal_session, KALSHI_REST, KALSHI_STATUS_PATH, _kal_sign("GET", KALSHI_STATUS_PATH)),
        _warm(_kal_session, KALSHI_REST, KALSHI_STATUS_PATH, _kal_sign("GET", KALSHI_STATUS_PATH)),
        _warm(_poly_session, POLY_REST, POLY_ORDER_PATH, poly_headers("GET", POLY_ORDER_PATH)),
    )
    qprint("  🔥 Connections warmed (Kalshi ×2, Poly ×1)")

async def connection_keepalive():
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(KEEPALIVE_SECS)
        try:
            hdr = await loop.run_in_executor(None, _kal_sign, "GET", KALSHI_STATUS_PATH)
            await _warm(_kal_session, KALSHI_REST, KALSHI_STATUS_PATH, hdr)
            await _warm(_poly_session, POLY_REST, POLY_ORDER_PATH,
                        poly_headers("GET", POLY_ORDER_PATH))
        except Exception:
            pass

async def heartbeat():
    while True:
        await asyncio.sleep(HEARTBEAT_SECS)
        rec_event("H")

async def monitor():
    global _poly_session, _kal_session
    init_log()
    qprint("=" * 92)
    qprint(f"  🌐 POLY↔KALSHI CROSS-MARKET ARB {BOT_VERSION}  —  {GAME_DESCRIPTION}")
    qprint(f"  MAKERS on Polymarket: BUY A_YES @ 1−KAL_Y−{SPREAD} / BUY A_NO @ 1−KAL_N−{SPREAD}")
    qprint(f"  HEDGE takes Kalshi: B_YES / B_NO  (pre-signed fast path)")
    qprint(f"  MAKER CLAMP: Poly prices held strictly below Poly best ask (no taker fees)")
    qprint(f"  FILLS: polled every {POLY_POLL_MS}ms  |  THRESHOLD={THRESHOLD}  LOCK after fill")
    qprint(f"  ⚠ Verify Poly GET/DELETE order endpoints against docs before live use")
    qprint("=" * 92)

    kal_conn  = aiohttp.TCPConnector(limit=0, keepalive_timeout=60, ttl_dns_cache=300,
                                     enable_cleanup_closed=True)
    poly_conn = aiohttp.TCPConnector(limit=0, keepalive_timeout=60, ttl_dns_cache=300,
                                     enable_cleanup_closed=True)
    async with aiohttp.ClientSession(connector=poly_conn) as ps, \
               aiohttp.ClientSession(connector=kal_conn) as ks:
        _poly_session = ps
        _kal_session = ks
        await warm_connections()
        rec_event("SESSION", info=json.dumps({
            "version": BOT_VERSION, "poly_market": POLY_MARKET_SLUG,
            "kalshi_ticker": KALSHI_TICKER, "spread": SPREAD,
            "threshold": THRESHOLD, "taker_buffer": TAKER_BUFFER,
            "poll_ms": POLY_POLL_MS, "qty": TRADE_QUANTITY,
            "record_enabled": RECORD_ENABLED,
            "gate_enforce": DEPTH_GATE_ENFORCE}))
        tasks = [asyncio.create_task(t()) for t in
                 (signature_refresher, connection_keepalive, heartbeat,
                  poly_fill_poller, kalshi_ws_loop, poly_lite_loop)]
        try:
            while True:
                if maker_filled:
                    qprint("\n🔒 MAKER MODE LOCKED. Monitoring only. Ctrl+C to exit.\n")
                    while True:
                        await asyncio.sleep(60)
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            qprint("\n🔴 Shutting down — cancelling Poly makers…")
            log("SHUTDOWN", "manual")
            await cancel_all_makers()
            qprint("🔴 Done.")
        finally:
            for t in tasks:
                t.cancel()

if __name__ == "__main__":
    try:
        import uvloop; uvloop.install()
    except ImportError:
        try:
            import winloop; winloop.install()
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
