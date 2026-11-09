"""
Cross-platform arbitrage scanner: Polymarket US ↔ Kalshi — READ-ONLY.

PRICE MAP
══════════
Polymarket: one slug covers the whole game. Team-A is the FIRST team in the slug.
  pm["ask"]  = cost to BUY  Team-A YES as a taker
  pm["bid"]  = best buyer price for Team-A YES (what you receive when selling)

Kalshi: separate YES ticker per team, prices derived from the orderbook:
  yes_ask  = 1 − best_NO_bid   (cost to BUY that team YES as a taker)
  yes_bid  = best_YES_bid      (what you receive when selling that team YES)
  no_ask   = 1 − best_YES_bid
  no_bid   = best_NO_bid       (what you receive when selling that team NO)

ARB SIGNALS
═══════════
BUY_COVER
  Buy Team-A on PM  (pay pm_ask)  +  Buy Team-B on Kalshi  (pay K_B_yes_ask)
  → covers both outcomes → guaranteed $1 payout
  → signal when  pm_ask + K_B_yes_ask  < ARB_BUY_THRESHOLD

SELL_COVER
  Sell Team-A YES on PM  (receive pm_bid, owe $1 if A wins)
  Sell Team-A NO  on Kalshi  (receive K_A_no_bid, owe $1 if A loses)
  → exactly one side pays $1 → guaranteed $1 cost, but collected > $1
  → signal when  pm_bid + K_A_no_bid  > ARB_SELL_THRESHOLD

SPREAD_A  (same-direction, risk-free price discrepancy on Team-A)
  Sell Team-A YES on PM (receive pm_bid), Buy Team-A YES on Kalshi (pay K_A_yes_ask)
  → zero net exposure; pocket the difference
  → signal when  pm_bid − K_A_yes_ask  > ARB_SPREAD_MIN

These three signals correspond to the user's framing:
  pm_ask pairs with  K_HOU_no_ask / K_TOR_yes_ask  (BUY_COVER)
  pm_bid pairs with  K_HOU_yes_ask                 (SPREAD_A)
  pm_bid + K_HOU_no_bid  >  threshold              (SELL_COVER)
"""

import asyncio
import json
import time
import base64
import os
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ╔══════════════════════════════════════════════════════════════╗
# ║   GAME CONFIG — only block you need to change per game      ║
# ╚══════════════════════════════════════════════════════════════╝
GAME_DESCRIPTION   = "HOU vs TOR  (MLB 2026-06-23)"

# Polymarket: slug from the event URL.
# TEAM_A MUST be the first team in the slug — PM bid/ask refer to TEAM_A winning.
PM_SLUG  = "aec-mlb-nyy-det-2026-06-23"
TEAM_A   = "HOU"
TEAM_B   = "TOR"

# Kalshi: YES on each ticker means that team wins the game.
KALSHI_TICKERS = {
    TEAM_A: "KXMLBGAME-26JUN231840NYYDET-NYY",
    TEAM_B: "KXMLBGAME-26JUN231840NYYDET-DET",
}

ARB_BUY_THRESHOLD   = 0.96   # BUY_COVER  triggers when sum-of-asks < this
ARB_SELL_THRESHOLD  = 1.04   # SELL_COVER triggers when sum-of-bids > this
ARB_SPREAD_MIN      = 0.02   # SPREAD_A   triggers when spread > this
# ══════════════════════════════════════════════════════════════

# ── Kalshi credentials ──────────────────────────────────────
KALSHI_KEY_ID   = 'DUMMY_KEY_ID'
KALSHI_KEY_PATH = r"path_to_private_key"
KALSHI_WS_URL   = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

# ── Polymarket credentials ──────────────────────────────────
PM_KEY_ID     = 'DUMMY_KEY_ID'
PM_SECRET_KEY_PATH = 'DUMMY_PRIVATE_KEY_PATH'
# "979z35LWJA2MrolauIiD5fdhN/4viKVY1ORYTpYWzbJVwRgIdKfi+c0wV9gESQ3P8egXoTWxUar91SvDagyRpQ=="

# ── Load Kalshi private key once ────────────────────────────
with open(KALSHI_KEY_PATH, "rb") as _fh:
    _KALSHI_KEY = serialization.load_pem_private_key(_fh.read(), password=None)
with open(PM_SECRET_KEY_PATH, "r") as _fh:
    PM_SECRET_KEY = _fh.read().strip()

# ════════════════════════════════════════════════════════════
# Shared price state
# ════════════════════════════════════════════════════════════
pm: dict = {"ask": None, "bid": None}      # Polymarket: Team-A only

kx: dict = {                                # Kalshi: one entry per team
    TEAM_A: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None},
    TEAM_B: {"yes_ask": None, "yes_bid": None, "no_ask": None, "no_bid": None},
}

_books: dict      = {TEAM_A: {"yes": {}, "no": {}}, TEAM_B: {"yes": {}, "no": {}}}
_ticker_map: dict = {v: k for k, v in KALSHI_TICKERS.items()}   # raw ticker → team label

# ════════════════════════════════════════════════════════════
# Kalshi auth
# ════════════════════════════════════════════════════════════
def kalshi_auth(method: str, path: str) -> dict:
    ts  = str(int(time.time() * 1000))
    sig = _KALSHI_KEY.sign(
        (ts + method + path).encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }

# ════════════════════════════════════════════════════════════
# Kalshi orderbook helpers
# ════════════════════════════════════════════════════════════
def _snap(team: str, msg: dict):
    """Replace the local orderbook from a snapshot message."""
    for side in ("yes", "no"):
        _books[team][side] = {
            int(round(float(r[0]) * 100)): float(r[1])
            for r in msg.get(f"{side}_dollars_fp", [])
        }

def _delta(team: str, msg: dict):
    """Apply an incremental delta to the local orderbook."""
    side  = msg.get("side")
    price = msg.get("price_dollars")
    qty   = msg.get("delta_fp")
    if side not in ("yes", "no") or price is None or qty is None:
        return
    c  = int(round(float(price) * 100))
    bk = _books[team][side]
    nq = bk.get(c, 0.0) + float(qty)
    if nq <= 0.0001:
        bk.pop(c, None)
    else:
        bk[c] = nq

def _derive(team: str):
    """
    Derive ask/bid prices from the raw Kalshi orderbook for one team.

    yes_bid = best price a buyer offers for YES shares  (what you receive selling)
    no_ask  = 1 - yes_bid                               (cost to BUY NO as a taker)
    no_bid  = best price a buyer offers for NO shares
    yes_ask = 1 - no_bid                                (cost to BUY YES as a taker)
    """
    def best(side) -> float | None:
        b = _books[team][side]
        return max(b) / 100.0 if b else None

    yb = best("yes")
    nb = best("no")
    p  = kx[team]

    p["yes_bid"] = round(yb,       4) if yb is not None else None
    p["no_ask"]  = round(1.0 - yb, 4) if yb is not None else None
    p["no_bid"]  = round(nb,       4) if nb is not None else None
    p["yes_ask"] = round(1.0 - nb, 4) if nb is not None else None

# ════════════════════════════════════════════════════════════
# Display & arb engine (called after every price update)
# ════════════════════════════════════════════════════════════
_prev = [None]

def _f(v) -> str:
    return f"{v:.4f}" if v is not None else "  ----"

def on_update():
    """Re-evaluate arb signals and print a status line whenever prices change."""
    state = (
        pm["ask"], pm["bid"],
        kx[TEAM_A]["yes_ask"], kx[TEAM_A]["no_bid"],
        kx[TEAM_B]["yes_ask"], kx[TEAM_B]["yes_bid"],
    )
    if state == _prev[0]:
        return
    _prev[0] = state

    # Shorthand
    pa    = pm["ask"]                    # PM: cost to buy  Team-A
    pb    = pm["bid"]                    # PM: receive for selling Team-A
    ka_ya = kx[TEAM_A]["yes_ask"]        # Kalshi: cost to buy  Team-A
    ka_yb = kx[TEAM_A]["yes_bid"]        # Kalshi: receive selling Team-A
    ka_nb = kx[TEAM_A]["no_bid"]         # Kalshi: receive selling Team-A NO (≈ Team-B price)
    kb_ya = kx[TEAM_B]["yes_ask"]        # Kalshi: cost to buy  Team-B
    kb_yb = kx[TEAM_B]["yes_bid"]        # Kalshi: receive selling Team-B

    # ── Cross-platform coverage sums ──────────────────────
    #
    # BUY_COVER:  pay pm_ask (buy A on PM) + pay kb_ya (buy B on Kalshi)
    #             → both outcomes covered, guaranteed $1 back
    buy_sum  = round(pa + kb_ya, 4) if (pa is not None and kb_ya is not None) else None

    # SELL_COVER: receive pm_bid (sell A on PM) + receive ka_nb (sell A-NO on Kalshi)
    #             → guaranteed $1 owed (one side will lose), but collected > $1
    sell_sum = round(pb + ka_nb, 4) if (pb is not None and ka_nb is not None) else None

    # ── Arb detection ─────────────────────────────────────
    arbs = []

    if buy_sum is not None and buy_sum < ARB_BUY_THRESHOLD:
        edge = round(1.0 - buy_sum, 4)
        arbs.append(
            f"⚡ BUY_COVER   "
            f"PM {TEAM_A} ask {_f(pa)} + Kalshi {TEAM_B} ask {_f(kb_ya)} = {buy_sum:.4f}  "
            f"→ edge ${edge:.4f}"
        )

    if sell_sum is not None and sell_sum > ARB_SELL_THRESHOLD:
        edge = round(sell_sum - 1.0, 4)
        arbs.append(
            f"⚡ SELL_COVER  "
            f"PM {TEAM_A} bid {_f(pb)} + Kalshi {TEAM_A} NO bid {_f(ka_nb)} = {sell_sum:.4f}  "
            f"→ edge ${edge:.4f}"
        )

    if pb is not None and ka_ya is not None:
        spread = round(pb - ka_ya, 4)
        if spread > ARB_SPREAD_MIN:
            arbs.append(
                f"⚡ SPREAD_{TEAM_A}  "
                f"sell PM bid {_f(pb)} / buy Kalshi ask {_f(ka_ya)}  "
                f"→ spread ${spread:.4f}"
            )

    # ── One-line status printout ───────────────────────────
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")

    buy_tag  = " ⚡" if (buy_sum  is not None and buy_sum  < ARB_BUY_THRESHOLD)  else "  "
    sell_tag = " ⚡" if (sell_sum is not None and sell_sum > ARB_SELL_THRESHOLD) else "  "

    print(
        f"[{ts}] "
        f"PM {TEAM_A}: ask={_f(pa)} bid={_f(pb)}  │  "
        f"Kalshi {TEAM_A}: ask={_f(ka_ya)} bid={_f(ka_yb)}  │  "
        f"Kalshi {TEAM_B}: ask={_f(kb_ya)} bid={_f(kb_yb)}  │  "
        f"BUY={_f(buy_sum)}{buy_tag} "
        f"SELL={_f(sell_sum)}{sell_tag}"
    )

    for arb in arbs:
        print(f"  🚨 {arb}")

# ════════════════════════════════════════════════════════════
# Kalshi WebSocket runner
# ════════════════════════════════════════════════════════════
async def run_kalshi():
    import websockets

    while True:
        try:
            headers = kalshi_auth("GET", "/trade-api/ws/v2")
            async with websockets.connect(
                KALSHI_WS_URL, additional_headers=headers, ping_interval=15
            ) as ws:
                print("✅ Kalshi WS connected")

                # Reset state on reconnect
                for team in (TEAM_A, TEAM_B):
                    _books[team] = {"yes": {}, "no": {}}
                    for k in kx[team]:
                        kx[team][k] = None

                await ws.send(json.dumps({
                    "id":  1,
                    "cmd": "subscribe",
                    "params": {
                        "channels":       ["orderbook_delta"],
                        "market_tickers": list(KALSHI_TICKERS.values()),
                    },
                }))

                async for raw in ws:
                    d   = json.loads(raw)
                    typ = d.get("type")
                    msg = d.get("msg", {})
                    tkr = msg.get("market_ticker")
                    if tkr not in _ticker_map:
                        continue
                    team = _ticker_map[tkr]
                    if   typ == "orderbook_snapshot": _snap(team, msg)
                    elif typ == "orderbook_delta":    _delta(team, msg)
                    else:                             continue
                    _derive(team)
                    on_update()

        except Exception as e:
            print(f"❌ Kalshi error: {e}. Reconnecting in 3s…")
            await asyncio.sleep(3)

# ════════════════════════════════════════════════════════════
# Polymarket WebSocket runner
# ════════════════════════════════════════════════════════════
async def run_polymarket():
    from polymarket_us import PolymarketUS

    while True:
        ws_handle = None
        try:
            client    = PolymarketUS(key_id=PM_KEY_ID, secret_key=PM_SECRET_KEY)
            ws_handle = client.ws.markets()

            def _on_lite(d: dict):
                """Handle a market_data_lite event from Polymarket."""
                lite = d.get("marketDataLite", {})
                bid  = lite.get("bestBid")
                ask  = lite.get("bestAsk")
                # SDK may return plain float or {"value": float}
                if isinstance(bid, dict): bid = bid.get("value")
                if isinstance(ask, dict): ask = ask.get("value")
                if bid is not None: pm["bid"] = round(float(bid), 4)
                if ask is not None: pm["ask"] = round(float(ask), 4)
                on_update()

            ws_handle.on("market_data_lite", _on_lite)
            ws_handle.on("error", lambda e: print(f"⚠️  PM WS: {e}"))

            await ws_handle.connect()
            await ws_handle.subscribe("prices", "SUBSCRIPTION_TYPE_MARKET_DATA_LITE", [PM_SLUG])
            print(f"✅ Polymarket WS → '{PM_SLUG}'  ({TEAM_A} = bestBid/bestAsk)")

            # SDK manages heartbeats internally; we just keep the task alive
            while True:
                await asyncio.sleep(30)

        except Exception as e:
            print(f"❌ Polymarket error: {e}. Reconnecting in 5s…")
            try:
                if ws_handle:
                    await ws_handle.close()
            except Exception:
                pass
            await asyncio.sleep(5)

# ════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════
async def main():
    print("═" * 100)
    print(f"  Cross-Platform Arb Scanner — READ ONLY")
    print(f"  {GAME_DESCRIPTION}")
    print(f"  PM slug    : '{PM_SLUG}'  ({TEAM_A} = bestBid/bestAsk)")
    print(f"  Kalshi {TEAM_A} : {KALSHI_TICKERS[TEAM_A]}")
    print(f"  Kalshi {TEAM_B} : {KALSHI_TICKERS[TEAM_B]}")
    print(f"  BUY_COVER  threshold: sum-of-asks  < {ARB_BUY_THRESHOLD}")
    print(f"  SELL_COVER threshold: sum-of-bids  > {ARB_SELL_THRESHOLD}")
    print(f"  SPREAD_{TEAM_A}  threshold: pm_bid - kalshi_ask > {ARB_SPREAD_MIN}")
    print("═" * 100)
    print(
        f"\n  {'Time':<10}"
        f"{'PM ' + TEAM_A + ' ask':>14}{'PM ' + TEAM_A + ' bid':>14}"
        f"{'K/' + TEAM_A + ' ask':>14}{'K/' + TEAM_A + ' bid':>14}"
        f"{'K/' + TEAM_B + ' ask':>14}{'K/' + TEAM_B + ' bid':>14}"
        f"{'BUY_SUM':>14}{'SELL_SUM':>14}"
    )
    print(f"  {'─' * 112}")

    await asyncio.gather(run_kalshi(), run_polymarket())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🔴 Stopped.")