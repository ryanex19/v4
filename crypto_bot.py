"""
crypto_bot_v4.py — Fast Execution Upgrade (WebSocket + lower latency)

Upgrades over v3:
  - Polymarket CLOB WebSocket for real-time mid prices
  - Local PM price cache updated by WebSocket (much faster reaction)
  - Active loop polls every 0.4s instead of 2s
  - Reduced REST calls during the critical window
  - Same strategy stack:
      1. Strategy 6 Max-Edge
      2. Dump-and-Hedge
      3. Core directional

Usage:
    python crypto_bot_v4.py
    python crypto_bot_v4.py --amount 3
    python crypto_bot_v4.py --balance 5000 --amount 3
"""

import time
import json
import argparse
import requests
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import signal
from typing import Optional, Dict, List
from collections import defaultdict, deque

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import websocket
    HAS_WS = True
except ImportError:
    HAS_WS = False
    print("WARNING: websocket-client not installed. Run: pip install websocket-client")

# ─── CONFIG ────────────────────────────────────────────────────────────────────
GAMMA_API   = "https://gamma-api.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
CLOB_WS     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_API = "https://api.binance.com"
BINANCE_WS  = "wss://stream.binance.com:9443/ws"

ENTRY_SECONDS_MAX = 50
ENTRY_SECONDS_MIN = 10
PRICE_MIN = {
    "BTC": 0.93,
    "ETH": 0.92,
    "SOL": 0.92,
}
PRICE_MAX = 0.995

WAKE_BEFORE        = 90
POLL_INTERVAL      = 0.4          # was 2.0 — much faster reaction
POLL_INTERVAL_IDLE = 1.5
WS_ACTIVATE_SEC    = 90

DELTA_SKIP   = 0.0003
DELTA_HARD   = 0.00025
DELTA_WEAK   = 0.001
DELTA_STRONG = 0.002

MIN_CONFIDENCE        = 0.22
MIN_CONFIDENCE_CORE   = 0.56
MIN_CONFIDENCE_STRICT = 0.56

ATR_PERIODS    = 5
ATR_MULTIPLIER = 1.5

DEFAULT_PAPER_BALANCE = 3000.0
BASE_AMOUNT           = 10.0
MAX_AMOUNT_MULT       = 2.5
MIN_AMOUNT_MULT       = 0.6

# Strategy 6
S6_DOWN_MULT    = 2.0
S6_VALUE_MULT   = 2.5
S6_LATE_MULT    = 1.5
S6_DANGEROUS_PX = 0.99

# Dump-and-Hedge
DUMP_THRESHOLD    = 0.18
HEDGE_SUM_TARGET  = 0.96
DUMP_LOOKBACK_SEC = 25
DUMP_SIZE_MULT    = 0.80
HEDGE_SIZE_MULT   = 0.60

BINANCE_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}

MARKETS = {
    "btc-updown-5m": "BTC",
    "eth-updown-5m": "ETH",
    "sol-updown-5m": "SOL",
}


def ts_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"[{ts_str()}] {msg}", flush=True)


def now_unix():
    return int(time.time())


def next_close_ts():
    return ((now_unix() // 300) + 1) * 300


# ─── Binance Price Cache (WebSocket) ───────────────────────────────────────────
class BinancePriceCache:
    def __init__(self):
        self._lock = threading.Lock()
        self._prices: Dict[str, float] = {}
        self._ws = None
        self._running = False

    def update(self, symbol: str, price: float):
        with self._lock:
            self._prices[symbol] = price

    def get(self, symbol: str) -> float:
        with self._lock:
            return self._prices.get(symbol, 0.0)

    def start(self):
        if not HAS_WS or self._running:
            return
        streams = "/".join(f"{s.lower()}@trade" for s in BINANCE_SYMBOLS.values())
        url = f"{BINANCE_WS}/{streams}"

        def on_message(ws, message):
            try:
                data = json.loads(message)
                if "data" in data:
                    data = data["data"]
                sym = data.get("s")
                price = float(data.get("p", 0))
                if sym and price > 0:
                    self.update(sym, price)
            except Exception:
                pass

        def on_error(ws, error):
            log(f"[BINANCE-WS] error: {error}")

        def on_close(ws, *args):
            self._running = False
            log("[BINANCE-WS] closed")

        def run():
            self._running = True
            self._ws = websocket.WebSocketApp(
                url, on_message=on_message, on_error=on_error, on_close=on_close
            )
            self._ws.run_forever(ping_interval=20, ping_timeout=10)

        threading.Thread(target=run, daemon=True).start()
        log("[BINANCE-WS] started")

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass


binance_cache = BinancePriceCache()


# ─── Polymarket CLOB Price Cache (WebSocket) ───────────────────────────────────
class PolymarketPriceCache:
    """
    Maintains real-time mid prices for token IDs via CLOB WebSocket.
    Falls back to REST if WS is unavailable.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._mids: Dict[str, float] = {}
        self._subscribed: set = set()
        self._ws = None
        self._running = False
        self._pending_subs: List[str] = []

    def update(self, token_id: str, mid: float):
        if mid <= 0:
            return
        with self._lock:
            self._mids[token_id] = mid

    def get(self, token_id: str) -> float:
        with self._lock:
            return self._mids.get(token_id, 0.0)

    def subscribe(self, token_ids: List[str]):
        new = [t for t in token_ids if t and t not in self._subscribed]
        if not new:
            return
        with self._lock:
            self._pending_subs.extend(new)
            self._subscribed.update(new)
        if self._ws and self._running:
            self._send_subscribe(new)

    def _send_subscribe(self, token_ids: List[str]):
        if not self._ws:
            return
        try:
            msg = {
                "type": "market",
                "assets_ids": token_ids,
            }
            self._ws.send(json.dumps(msg))
            log(f"[PM-WS] subscribed {len(token_ids)} tokens")
        except Exception as e:
            log(f"[PM-WS] subscribe error: {e}")

    def start(self):
        if not HAS_WS or self._running:
            return

        def on_message(ws, message):
            try:
                data = json.loads(message)
                if isinstance(data, list):
                    for item in data:
                        self._handle_event(item)
                else:
                    self._handle_event(data)
            except Exception:
                pass

        def on_error(ws, error):
            log(f"[PM-WS] error: {error}")

        def on_close(ws, *args):
            self._running = False
            log("[PM-WS] closed — will fall back to REST")

        def on_open(ws):
            log("[PM-WS] connected")
            with self._lock:
                pending = list(self._pending_subs)
                self._pending_subs.clear()
            if pending:
                self._send_subscribe(pending)

        def run():
            self._running = True
            self._ws = websocket.WebSocketApp(
                CLOB_WS,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            self._ws.run_forever(ping_interval=20, ping_timeout=10)

        threading.Thread(target=run, daemon=True).start()
        log("[PM-WS] starting...")

    def _handle_event(self, event: dict):
        if not isinstance(event, dict):
            return
        asset_id = event.get("asset_id") or event.get("token_id") or event.get("asset")
        if asset_id:
            price = event.get("price") or event.get("mid") or event.get("last_trade_price")
            if price is not None:
                try:
                    self.update(str(asset_id), float(price))
                except (TypeError, ValueError):
                    pass

        if "bids" in event or "asks" in event:
            asset_id = event.get("asset_id") or event.get("market")
            if asset_id:
                try:
                    best_bid = float(event["bids"][0]["price"]) if event.get("bids") else 0
                    best_ask = float(event["asks"][0]["price"]) if event.get("asks") else 0
                    if best_bid > 0 and best_ask > 0:
                        mid = (best_bid + best_ask) / 2
                        self.update(str(asset_id), mid)
                    elif best_bid > 0:
                        self.update(str(asset_id), best_bid)
                    elif best_ask > 0:
                        self.update(str(asset_id), best_ask)
                except Exception:
                    pass

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass


pm_cache = PolymarketPriceCache()


# ─── PM price history for dump detection ───────────────────────────────────────
pm_price_history: Dict[str, deque] = {
    "BTC": deque(maxlen=50),
    "ETH": deque(maxlen=50),
    "SOL": deque(maxlen=50),
}
_pm_hist_lock = threading.Lock()


def update_pm_history(asset: str, up_px: float, down_px: float):
    with _pm_hist_lock:
        pm_price_history[asset].append((now_unix(), up_px, down_px))


def get_pm_history(asset: str) -> list:
    with _pm_hist_lock:
        return list(pm_price_history.get(asset, []))


# ─── Binance helpers ───────────────────────────────────────────────────────────
def get_binance_candles(symbol: str, interval: str = "1m", limit: int = 6) -> list:
    try:
        r = requests.get(
            f"{BINANCE_API}/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=4,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log(f"[BINANCE] candles {symbol}: {e}")
        return []


def get_binance_price_rest(symbol: str) -> float:
    try:
        r = requests.get(
            f"{BINANCE_API}/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=3,
        )
        r.raise_for_status()
        price = float(r.json()["price"])
        binance_cache.update(symbol, price)
        return price
    except Exception as e:
        log(f"[BINANCE] price {symbol}: {e}")
        return 0.0


def get_binance_price(symbol: str) -> float:
    p = binance_cache.get(symbol)
    if p > 0:
        return p
    return get_binance_price_rest(symbol)


def get_window_open_price(symbol: str, window_ts: int) -> float:
    try:
        r = requests.get(
            f"{BINANCE_API}/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "5m",
                "startTime": window_ts * 1000,
                "limit": 1,
            },
            timeout=4,
        )
        r.raise_for_status()
        candles = r.json()
        if candles:
            return float(candles[0][1])
        return 0.0
    except Exception as e:
        log(f"[BINANCE] open {symbol}: {e}")
        return 0.0


def get_window_close_price(symbol: str, window_ts: int) -> float:
    try:
        r = requests.get(
            f"{BINANCE_API}/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "5m",
                "startTime": window_ts * 1000,
                "limit": 1,
            },
            timeout=4,
        )
        r.raise_for_status()
        candles = r.json()
        if candles:
            return float(candles[0][4])
        return 0.0
    except Exception as e:
        log(f"[BINANCE] close {symbol}: {e}")
        return 0.0


def get_atr(symbol: str, window_ts: int, periods: int = 5) -> float:
    try:
        r = requests.get(
            f"{BINANCE_API}/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "5m",
                "endTime": window_ts * 1000,
                "limit": periods,
            },
            timeout=4,
        )
        r.raise_for_status()
        candles = r.json()
        if not candles:
            return 0.0
        ranges = [float(c[2]) - float(c[3]) for c in candles]
        return sum(ranges) / len(ranges)
    except Exception:
        return 0.0


def analyze(symbol: str, window_ts: int) -> dict:
    current_price = get_binance_price(symbol)
    if current_price <= 0:
        return {"confidence": 0, "direction": None, "reason": "no price"}

    window_open = get_window_open_price(symbol, window_ts)
    if window_open <= 0:
        candles = get_binance_candles(symbol, "1m", 6)
        if candles:
            for c in candles:
                if int(c[0]) >= window_ts * 1000:
                    window_open = float(c[1])
                    break
            if window_open <= 0:
                window_open = float(candles[0][1])
        else:
            return {"confidence": 0, "direction": None, "reason": "no open"}

    delta = (current_price - window_open) / window_open
    delta_pct = abs(delta) * 100
    delta_dir = "Up" if delta > 0 else "Down"

    atr = get_atr(symbol, window_ts, ATR_PERIODS)
    if atr > 0:
        candles_5m = get_binance_candles(symbol, "5m", 1)
        if candles_5m:
            current_range = float(candles_5m[0][2]) - float(candles_5m[0][3])
            if current_range > atr * ATR_MULTIPLIER:
                return {
                    "confidence": 0,
                    "direction": None,
                    "window_open": window_open,
                    "current_price": current_price,
                    "delta_pct": delta_pct,
                    "reason": f"ATR skip range ${current_range:.2f} > {ATR_MULTIPLIER}x ATR",
                }

    if abs(delta) < DELTA_HARD:
        return {
            "confidence": 0,
            "direction": None,
            "window_open": window_open,
            "current_price": current_price,
            "delta_pct": delta_pct,
            "reason": f"HARD SKIP delta {delta_pct:.4f}% < {DELTA_HARD*100:.3f}%",
        }

    if abs(delta) < DELTA_SKIP:
        return {
            "confidence": 0,
            "direction": None,
            "window_open": window_open,
            "current_price": current_price,
            "delta_pct": delta_pct,
            "reason": f"delta {delta_pct:.4f}% < {DELTA_SKIP*100:.3f}%",
        }

    if abs(delta) >= DELTA_STRONG * 5:
        delta_weight = 7
    elif abs(delta) >= DELTA_STRONG:
        delta_weight = 5
    elif abs(delta) >= DELTA_WEAK:
        delta_weight = 3
    else:
        delta_weight = 1

    score = delta_weight if delta > 0 else -delta_weight

    candles = get_binance_candles(symbol, "1m", 3)
    momentum_str = "no data"
    if len(candles) >= 2:
        prev_close = float(candles[-2][4])
        last_close = float(candles[-1][4])
        momentum_up = last_close > prev_close
        if (delta > 0 and momentum_up) or (delta < 0 and not momentum_up):
            score += 2
            momentum_str = f"{'↑' if momentum_up else '↓'} confirms"
        else:
            momentum_str = f"{'↑' if momentum_up else '↓'} contradicts"

    confidence = min(abs(score) / 9.0, 1.0)
    direction = "Up" if score > 0 else "Down"

    return {
        "score": score,
        "confidence": confidence,
        "direction": direction,
        "window_open": window_open,
        "current_price": current_price,
        "delta_pct": delta_pct,
        "delta_weight": delta_weight,
        "momentum": momentum_str,
        "atr": atr,
        "reason": f"delta={delta_pct:.4f}% ({delta_dir}, w={delta_weight}) {momentum_str}",
    }


def get_market_for_close(slug_prefix: str, close_ts: int) -> Optional[dict]:
    start_ts = close_ts - 300
    slug = f"{slug_prefix}-{start_ts}"
    try:
        r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=5)
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        event = data[0]
    except Exception as e:
        log(f"[PM] market {slug}: {e}")
        return None

    if not event.get("active") or event.get("closed"):
        return None

    markets = event.get("markets", [])
    if not markets:
        return None

    market = markets[0]
    try:
        outcome_prices = json.loads(market.get("outcomePrices", "[]"))
        outcomes       = json.loads(market.get("outcomes", "[]"))
        clob_token_ids = json.loads(market.get("clobTokenIds", "[]"))
    except (json.JSONDecodeError, TypeError):
        return None

    if len(outcome_prices) < 2 or len(clob_token_ids) < 2 or len(outcomes) < 2:
        return None

    prices = [float(p) for p in outcome_prices]
    up_idx = 0 if outcomes[0].lower() in ("up", "yes") else 1
    down_idx = 1 - up_idx
    winner_idx = 0 if prices[0] >= prices[1] else 1

    return {
        "slug": slug,
        "slug_prefix": slug_prefix,
        "crypto": MARKETS[slug_prefix],
        "title": event.get("title", ""),
        "close_ts": close_ts,
        "start_ts": start_ts,
        "winner_side": outcomes[winner_idx],
        "winner_price": prices[winner_idx],
        "winner_token": clob_token_ids[winner_idx],
        "loser_price": prices[1 - winner_idx],
        "up_price": prices[up_idx],
        "down_price": prices[down_idx],
        "up_token": clob_token_ids[up_idx],
        "down_token": clob_token_ids[down_idx],
        "condition_id": market.get("conditionId", ""),
        "liquidity": float(event.get("liquidity", 0) or 0),
        "outcomes": outcomes,
        "token_ids": clob_token_ids,
    }


def get_clob_price(token_id: str) -> float:
    """Prefer WebSocket cache, fall back to REST."""
    if not token_id:
        return 0.0
    cached = pm_cache.get(token_id)
    if cached > 0:
        return cached
    try:
        r = requests.get(f"{CLOB_API}/midpoint", params={"token_id": token_id}, timeout=2.5)
        r.raise_for_status()
        mid = float(r.json().get("mid", 0) or 0)
        if mid > 0:
            pm_cache.update(token_id, mid)
        return mid
    except Exception:
        return 0.0


def calc_size(base: float, confidence: float, price: float) -> float:
    conf_mult = MIN_AMOUNT_MULT + (confidence - MIN_CONFIDENCE) / max(1e-9, (1.0 - MIN_CONFIDENCE)) * (MAX_AMOUNT_MULT - MIN_AMOUNT_MULT)
    conf_mult = max(MIN_AMOUNT_MULT, min(MAX_AMOUNT_MULT, conf_mult))
    edge = max(0.0, 1.0 - price)
    edge_mult = 1.0 + min(0.4, edge * 2)
    size = base * conf_mult * edge_mult
    return round(max(base * MIN_AMOUNT_MULT, min(base * MAX_AMOUNT_MULT, size)), 2)


# ─── Strategy 6: Max Edge ──────────────────────────────────────────────────────
def strategy_6_max_edge(
    confidence: float,
    direction: str,
    px: float,
    seconds_left: float,
    base_size: float,
) -> Optional[dict]:
    if confidence < MIN_CONFIDENCE_STRICT:
        return None
    if direction == "Up" and px >= S6_DANGEROUS_PX:
        return None

    size_mult = 1.0
    reason_parts = ["S6-MaxEdge"]

    if px <= 0.97:
        size_mult = S6_VALUE_MULT
        reason_parts.append("Value")
    elif direction == "Down":
        size_mult = S6_DOWN_MULT
        reason_parts.append("DownBoost")
    elif seconds_left <= 35:
        size_mult = S6_LATE_MULT
        reason_parts.append("Late")

    size = round(base_size * size_mult, 2)
    return {
        "action": "ENTER",
        "direction": direction,
        "size": size,
        "size_mult": size_mult,
        "strategy": "Strategy6-MaxEdge",
        "reason": "+".join(reason_parts),
        "confidence": confidence,
        "px": px,
    }


# ─── Dump-and-Hedge ────────────────────────────────────────────────────────────
def check_dump_and_hedge(
    asset: str,
    up_px: float,
    down_px: float,
    base_size: float,
) -> Optional[dict]:
    history = get_pm_history(asset)
    if len(history) < 3:
        return None

    now = now_unix()
    recent = [p for p in history if now - p[0] <= DUMP_LOOKBACK_SEC]
    if len(recent) < 2:
        return None

    max_up = max(p[1] for p in recent)
    max_down = max(p[2] for p in recent)

    up_dump = (max_up - up_px) / max_up >= DUMP_THRESHOLD if max_up > 0.01 else False
    down_dump = (max_down - down_px) / max_down >= DUMP_THRESHOLD if max_down > 0.01 else False

    if not (up_dump or down_dump):
        return None

    if up_dump and not down_dump:
        dumped_side, dumped_px = "Up", up_px
        hedge_side, hedge_px = "Down", down_px
    elif down_dump and not up_dump:
        dumped_side, dumped_px = "Down", down_px
        hedge_side, hedge_px = "Up", up_px
    else:
        return None

    combined = dumped_px + hedge_px

    if combined <= HEDGE_SUM_TARGET:
        size_each = round(base_size * HEDGE_SIZE_MULT, 2)
        return {
            "action": "ENTER_BOTH",
            "legs": [
                {"direction": dumped_side, "size": size_each, "px": dumped_px},
                {"direction": hedge_side, "size": size_each, "px": hedge_px},
            ],
            "strategy": "DumpAndHedge",
            "reason": f"Dump+Hedge locked ({dumped_side} dumped, sum={combined:.3f})",
            "expected_profit_pct": (1.0 - combined) * 100,
            "combined": combined,
        }

    size = round(base_size * DUMP_SIZE_MULT, 2)
    return {
        "action": "ENTER",
        "direction": dumped_side,
        "size": size,
        "size_mult": DUMP_SIZE_MULT,
        "strategy": "DumpAndHedge",
        "reason": f"Dump on {dumped_side} (sum={combined:.3f} still high)",
        "pending_hedge": hedge_side,
        "combined": combined,
    }


# ─── Main Bot ──────────────────────────────────────────────────────────────────
class CryptoBot:
    def __init__(self, paper: bool, dry_run: bool, amount: float, paper_balance: float):
        self.paper = paper
        self.dry_run = dry_run
        self.base_amount = amount
        self.paper_balance = paper_balance
        self.starting_balance = paper_balance
        self.traded_slugs = set()
        self.trades: List[dict] = []
        self.open_positions: List[dict] = []
        self.daily_pnl: Dict[str, float] = defaultdict(float)
        self._shutdown = False
        self.stats = {
            "wins": 0,
            "losses": 0,
            "by_asset": defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0}),
            "by_strategy": defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0}),
        }

        mode = "DRY RUN" if dry_run else ("PAPER" if paper else "LIVE")
        log("=" * 70)
        log(f"Crypto Up/Down Bot v4 (FAST) | {mode} | base ${amount}/trade | bal ${paper_balance:.2f}")
        log(f"Assets: {', '.join(MARKETS.values())}")
        log(f"Entry window: {ENTRY_SECONDS_MIN}-{ENTRY_SECONDS_MAX}s | Poll: {POLL_INTERVAL}s")
        log(f"Price min BTC={PRICE_MIN['BTC']} ETH/SOL={PRICE_MIN['ETH']} max={PRICE_MAX}")
        log(f"Min conf core/S6={MIN_CONFIDENCE_CORE*100:.0f}% | WS={'ON' if HAS_WS else 'OFF'}")
        log("─" * 70)
        log("Strategies: Strategy6-MaxEdge → DumpAndHedge → Core")
        log("=" * 70)

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        for sym in BINANCE_SYMBOLS.values():
            get_binance_price_rest(sym)

        binance_cache.start()
        pm_cache.start()

    def _handle_signal(self, signum, frame):
        log(f"Signal {signum} — shutting down...")
        self._shutdown = True
        binance_cache.stop()
        pm_cache.stop()

    def run(self):
        while not self._shutdown:
            try:
                self._cycle()
            except KeyboardInterrupt:
                break
            except Exception as e:
                log(f"Cycle error: {e}")
                time.sleep(3)
        self._print_summary()
        log("Bot exited.")

    def _resolve_open_positions(self):
        if not self.open_positions:
            return
        still_open = []
        for pos in self.open_positions:
            if now_unix() < pos["close_ts"] + 12:
                still_open.append(pos)
                continue

            symbol = BINANCE_SYMBOLS[pos["crypto"]]
            open_p = pos.get("window_open") or get_window_open_price(symbol, pos["start_ts"])
            close_p = get_window_close_price(symbol, pos["start_ts"])

            if open_p <= 0 or close_p <= 0:
                still_open.append(pos)
                continue

            actual = "Up" if close_p >= open_p else "Down"
            won = actual == pos["side"]
            shares = pos["amount"] / pos["price_entry"]

            if won:
                payout = shares * 1.0
                pnl = payout - pos["amount"]
                self.paper_balance += payout
                result = "WIN"
                self.stats["wins"] += 1
            else:
                payout = 0.0
                pnl = -pos["amount"]
                result = "LOSS"
                self.stats["losses"] += 1

            day = pos["timestamp"][:10]
            self.daily_pnl[day] += pnl
            self.stats["by_asset"][pos["crypto"]]["pnl"] += pnl
            strat = pos.get("strategy", "Core")
            self.stats["by_strategy"][strat]["pnl"] += pnl

            if won:
                self.stats["by_asset"][pos["crypto"]]["wins"] += 1
                self.stats["by_strategy"][strat]["wins"] += 1
            else:
                self.stats["by_asset"][pos["crypto"]]["losses"] += 1
                self.stats["by_strategy"][strat]["losses"] += 1

            pos.update({
                "resolved": True,
                "actual_dir": actual,
                "pnl": pnl,
                "payout": payout,
                "result": result,
                "close_price": close_p,
                "open_price": open_p,
            })
            self.trades.append(pos)
            log(f"📊 RESOLVED [{pos['crypto']}] {pos['side']} → {result} | "
                f"open={open_p:.4f} close={close_p:.4f} | PnL={pnl:+.2f} | "
                f"strat={strat} | bal=${self.paper_balance:.2f}")

        self.open_positions = still_open

    def _cycle(self):
        self._resolve_open_positions()

        close_ts = next_close_ts()
        sleep_secs = close_ts - now_unix() - WAKE_BEFORE

        if sleep_secs > 0:
            log(f"💤 Sleep {sleep_secs:.0f}s → close "
                f"{datetime.fromtimestamp(close_ts, tz=timezone.utc).strftime('%H:%M:%S')} UTC "
                f"| bal=${self.paper_balance:.2f} | open={len(self.open_positions)} "
                f"| W/L={self.stats['wins']}/{self.stats['losses']}")
            end = time.time() + sleep_secs
            while time.time() < end and not self._shutdown:
                time.sleep(min(3, end - time.time()))
            if self._shutdown:
                return

        if now_unix() >= close_ts + 5:
            log("⚠️  Too late for this window, skipping")
            for prefix in MARKETS:
                self.traded_slugs.add(f"{prefix}-{close_ts - 300}")
            return

        log(f"⚡ Active — close {datetime.fromtimestamp(close_ts, tz=timezone.utc).strftime('%H:%M:%S')} UTC")

        entered_slugs = set()

        # Pre-fetch markets and subscribe tokens to PM WebSocket
        market_cache = {}
        for prefix in MARKETS:
            m = get_market_for_close(prefix, close_ts)
            if m:
                market_cache[prefix] = m
                tokens = [m.get("up_token"), m.get("down_token"), m.get("winner_token")]
                pm_cache.subscribe([t for t in tokens if t])

        while not self._shutdown:
            seconds_left = close_ts - now_unix()
            if seconds_left <= 0:
                log("⏰ Closed")
                for prefix in MARKETS:
                    self.traded_slugs.add(f"{prefix}-{close_ts - 300}")
                break

            pending = [
                p for p in MARKETS
                if f"{p}-{close_ts - 300}" not in self.traded_slugs
                and f"{p}-{close_ts - 300}" not in entered_slugs
            ]
            if not pending:
                time.sleep(POLL_INTERVAL)
                continue

            def fetch_one(prefix):
                market = market_cache.get(prefix) or get_market_for_close(prefix, close_ts)
                if not market:
                    return prefix, None, None

                # Prefer live WS prices
                up_clob = get_clob_price(market.get("up_token", ""))
                down_clob = get_clob_price(market.get("down_token", ""))
                win_clob = get_clob_price(market.get("winner_token", ""))

                if up_clob > 0:
                    market["up_price"] = up_clob
                if down_clob > 0:
                    market["down_price"] = down_clob
                if win_clob > 0:
                    market["winner_price"] = win_clob

                crypto = MARKETS[prefix]
                ta = analyze(BINANCE_SYMBOLS[crypto], close_ts - 300)
                return prefix, market, ta

            with ThreadPoolExecutor(max_workers=len(pending)) as ex:
                futures = {ex.submit(fetch_one, p): p for p in pending}
                results = []
                for f in as_completed(futures):
                    try:
                        results.append(f.result())
                    except Exception as e:
                        log(f"Fetch err: {e}")

            seconds_left = close_ts - now_unix()

            for prefix, market, ta in results:
                if not market or not ta:
                    continue
                slug = market["slug"]
                crypto = market["crypto"]
                if slug in self.traded_slugs or slug in entered_slugs:
                    continue

                up_px = market.get("up_price", market.get("winner_price", 0.5))
                down_px = market.get("down_price", market.get("loser_price", 0.5))
                update_pm_history(crypto, up_px, down_px)

                if seconds_left > ENTRY_SECONDS_MAX + 5:
                    log(f"   [{crypto}] {seconds_left:.0f}s | "
                        f"PM:{market['winner_side']}@{market['winner_price']:.3f} | "
                        f"δ:{ta.get('delta_pct',0):.4f}% conf:{ta.get('confidence',0):.0%}")
                    continue

                log(f"🎯 [{crypto}] {seconds_left:.1f}s | "
                    f"PM:{market['winner_side']}@{market['winner_price']:.3f} | "
                    f"δ:{ta.get('delta_pct',0):.4f}% conf:{ta.get('confidence',0):.0%} | "
                    f"{ta.get('reason','')[:55]}")

                if ENTRY_SECONDS_MIN <= seconds_left <= ENTRY_SECONDS_MAX:
                    self._evaluate_entry(market, ta, seconds_left, entered_slugs)

            time.sleep(POLL_INTERVAL)

    def _evaluate_entry(self, market, ta, seconds_left, entered_slugs):
        slug = market["slug"]
        crypto = market["crypto"]
        price_min = PRICE_MIN.get(crypto, 0.92)
        price = market["winner_price"]
        confidence = ta.get("confidence", 0)
        ta_dir = ta.get("direction")
        delta_pct = ta.get("delta_pct", 0)

        if price < price_min:
            log(f"   [{crypto}] SKIP price {price:.3f} < {price_min}")
            return
        if price > PRICE_MAX:
            log(f"   [{crypto}] SKIP price {price:.3f} > {PRICE_MAX}")
            return
        if confidence < MIN_CONFIDENCE:
            log(f"   [{crypto}] SKIP conf {confidence:.0%} < {MIN_CONFIDENCE:.0%}")
            return
        if ta_dir and ta_dir != market["winner_side"]:
            log(f"   [{crypto}] SKIP dir mismatch Binance={ta_dir} PM={market['winner_side']}")
            return
        if delta_pct < DELTA_SKIP * 100:
            log(f"   [{crypto}] SKIP delta {delta_pct:.4f}% too small")
            return
        if delta_pct < DELTA_HARD * 100:
            log(f"   [{crypto}] SKIP HARD delta {delta_pct:.4f}%")
            return

        direction = market["winner_side"]
        up_px = market.get("up_price", price if direction == "Up" else market.get("loser_price", 0.5))
        down_px = market.get("down_price", price if direction == "Down" else market.get("loser_price", 0.5))

        # Priority 1: Strategy 6
        s6 = strategy_6_max_edge(
            confidence=confidence,
            direction=direction,
            px=price,
            seconds_left=seconds_left,
            base_size=self.base_amount,
        )
        if s6:
            if self.paper_balance < s6["size"]:
                log(f"   [{crypto}] SKIP S6 insufficient balance")
                return
            log(f"   🔥 Strategy6 hit | {s6['reason']} | mult={s6['size_mult']:.1f}x")
            self._enter(market, ta, seconds_left, s6["size"], strategy=s6["strategy"], reason=s6["reason"])
            entered_slugs.add(slug)
            self.traded_slugs.add(slug)
            return

        # Priority 2: Dump-and-Hedge
        dah = check_dump_and_hedge(crypto, up_px, down_px, self.base_amount)
        if dah:
            if dah["action"] == "ENTER_BOTH":
                total_needed = sum(leg["size"] for leg in dah["legs"])
                if self.paper_balance < total_needed:
                    log(f"   [{crypto}] SKIP DAH both-legs insufficient balance")
                    return
                log(f"   💥 Dump-and-Hedge LOCKED | {dah['reason']} | "
                    f"exp≈{dah.get('expected_profit_pct',0):.1f}%")
                for leg in dah["legs"]:
                    leg_market = dict(market)
                    leg_market["winner_side"] = leg["direction"]
                    leg_market["winner_price"] = leg["px"]
                    self._enter(
                        leg_market, ta, seconds_left, leg["size"],
                        strategy="DumpAndHedge",
                        reason=dah["reason"],
                    )
                entered_slugs.add(slug)
                self.traded_slugs.add(slug)
                return
            else:
                if self.paper_balance < dah["size"]:
                    log(f"   [{crypto}] SKIP DAH insufficient balance")
                    return
                log(f"   💥 Dump detected | {dah['reason']}")
                dump_market = dict(market)
                dump_market["winner_side"] = dah["direction"]
                dump_market["winner_price"] = up_px if dah["direction"] == "Up" else down_px
                self._enter(
                    dump_market, ta, seconds_left, dah["size"],
                    strategy="DumpAndHedge",
                    reason=dah["reason"],
                )
                entered_slugs.add(slug)
                self.traded_slugs.add(slug)
                return

        # Priority 3: Core
        if confidence < MIN_CONFIDENCE_CORE:
            log(f"   [{crypto}] SKIP core conf {confidence:.0%} < {MIN_CONFIDENCE_CORE:.0%}")
            return

        size = calc_size(self.base_amount, confidence, price)
        if direction == "Up" and price >= S6_DANGEROUS_PX:
            size = round(size * 0.35, 2)
            log(f"   [{crypto}] Core reduced size on Up@high-px (×0.35)")

        if self.paper_balance < size:
            log(f"   [{crypto}] SKIP insufficient balance ${self.paper_balance:.2f} < ${size:.2f}")
            return

        self._enter(market, ta, seconds_left, size, strategy="Core", reason="Core-conf≥56")
        entered_slugs.add(slug)
        self.traded_slugs.add(slug)

    def _enter(self, market, ta, seconds_left, size, strategy: str = "Core", reason: str = ""):
        price = market["winner_price"]
        expected_pnl = (size / price) - size if price > 0 else 0
        crypto = market["crypto"]

        log(f"🟢 ENTER [{crypto} {market['winner_side']}] {market['title'][:40]}")
        log(f"   px={price:.3f} | left={seconds_left:.1f}s | size=${size:.2f} "
            f"(base ${self.base_amount}) | exp PnL=+${expected_pnl:.2f}")
        log(f"   spot={ta.get('current_price',0):.4f} δ={ta.get('delta_pct',0):.4f}% "
            f"conf={ta.get('confidence',0):.0%} | strat={strategy} | {reason}")

        if self.paper or self.dry_run:
            log(f"   📄 PAPER — virtual fill")
            self.paper_balance -= size
            self.open_positions.append({
                "crypto": crypto,
                "title": market["title"],
                "side": market["winner_side"],
                "price_entry": price,
                "amount": size,
                "seconds_left": seconds_left,
                "pnl_expected": expected_pnl,
                "delta_pct": ta.get("delta_pct", 0),
                "confidence": ta.get("confidence", 0),
                "timestamp": ts_str(),
                "slug": market["slug"],
                "start_ts": market["start_ts"],
                "close_ts": market["close_ts"],
                "window_open": ta.get("window_open"),
                "resolved": False,
                "strategy": strategy,
                "reason": reason,
            })
            log(f"   ✅ Recorded | bal=${self.paper_balance:.2f}")
        else:
            log("   ⚠️  Live path not implemented in this paper-focused build")

    def _print_summary(self):
        self._resolve_open_positions()
        log("─" * 70)
        log(f"SUMMARY  trades={len(self.trades)}  open={len(self.open_positions)}")
        log(f"Wins {self.stats['wins']}  |  Losses {self.stats['losses']}")
        if self.trades:
            total_pnl = sum(t.get("pnl", 0) for t in self.trades)
            log(f"Realized PnL: {total_pnl:+.2f}")
        log(f"Start bal: ${self.starting_balance:.2f}  →  Current: ${self.paper_balance:.2f}  "
            f"({self.paper_balance - self.starting_balance:+.2f})")

        log("By asset:")
        for asset, s in self.stats["by_asset"].items():
            log(f"  {asset}: W{s['wins']}/L{s['losses']}  PnL={s['pnl']:+.2f}")

        log("By strategy:")
        for strat, s in self.stats["by_strategy"].items():
            log(f"  {strat}: W{s['wins']}/L{s['losses']}  PnL={s['pnl']:+.2f}")

        if self.daily_pnl:
            log("Daily PnL:")
            for d, pnl in sorted(self.daily_pnl.items()):
                log(f"  {d}: {pnl:+.2f}")
        log("─" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fast 5m crypto paper bot v4 (BTC/ETH/SOL)")
    parser.add_argument("--paper", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--amount", type=float, default=None)
    parser.add_argument("--balance", type=float, default=None)
    args = parser.parse_args()

    env_mode = os.getenv("BOT_MODE", "").lower()
    env_amount = os.getenv("TRADE_AMOUNT")
    env_balance = os.getenv("PAPER_BALANCE")

    amount = args.amount if args.amount is not None else float(env_amount) if env_amount else BASE_AMOUNT
    paper_balance = args.balance if args.balance is not None else float(env_balance) if env_balance else DEFAULT_PAPER_BALANCE

    if args.live or env_mode == "live":
        paper, dry_run = False, False
    elif args.dry_run or env_mode == "dry-run":
        paper, dry_run = False, True
    else:
        paper, dry_run = True, False

    bot = CryptoBot(paper=paper, dry_run=dry_run, amount=amount, paper_balance=paper_balance)
    bot.run()