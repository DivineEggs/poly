#!/usr/bin/env python3
"""
Polymarket Crypto Arb Bot v2
================================
Clean rewrite. Single loop. Hard dedup. Kill switch.

Usage:
    python3 bot_v2.py --paper    # paper mode — logs orders, no real money
    python3 bot_v2.py --live     # live trading

To stop safely:  touch ~/polymarket-bot/STOP   (or Ctrl+C)
"""

import asyncio
import argparse
import json
import logging
import math
import os
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Set, Tuple

try:
    import numpy as np
    from scipy import stats
    import websockets
    import aiohttp
    from dotenv import load_dotenv
except ImportError as e:
    print(f"\n❌ Missing package: {e}")
    print("Run: pip3 install websockets aiohttp numpy scipy python-dotenv")
    sys.exit(1)

load_dotenv()


# ═══════════════════════════════════════════════════════════
# CONFIG — all settings here, no separate config.py
# ═══════════════════════════════════════════════════════════

MAX_PER_TRADE_USD       = 3.0   # max USDC per single order
MAX_TOTAL_EXPOSURE_USD  = 6.0   # max total open exposure at any time
MAX_DAILY_LOSS_USD      = 10.0  # halt for the day if we lose this much
MAX_ORDERS_PER_ROUND    = 1     # hard cap: 1 order per round, ever
MIN_SHARE_PRICE         = 0.05  # skip if computed order price < 5¢
MAX_SHARE_PRICE         = 0.40  # skip if computed order price > 40¢
MIN_SHARES              = 5     # Polymarket minimum order size
MIN_EDGE_CENTS          = 2.0   # minimum edge (cents) required to trade
NO_ENTRY_BUFFER_S       = 45    # don't enter within this many seconds of round end
MEAN_REVERSION          = 0.18  # mean-reversion coefficient in pricing model
VOL_WINDOW_S            = 60    # seconds of price history for vol estimate
DEFAULT_VOL             = 0.80  # annualised vol fallback when history is thin
DISCOVERY_CACHE_S       = 15.0  # seconds to cache a discovered round
ORDER_TTL_S             = 30    # cancel open orders older than this

POLYMARKET_HOST = "https://clob.polymarket.com"
GAMMA_API       = "https://gamma-api.polymarket.com"
CHAIN_ID        = 137

# Slug prefixes for 5-min round discovery.
# Full slug sent to Gamma API: prefix + "-" + round_start_unix_timestamp
SLUG_PREFIXES: Dict[str, str] = {
    "btcusdt": "btc-updown-5m",
    "ethusdt": "eth-up-or-down-5m",
}

STOP_FILE = os.path.expanduser("~/polymarket-bot/STOP")
LOG_FILE  = "bot_v2.log"


# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    fmt = "%(asctime)s │ %(levelname)-5s │ %(message)s"
    logging.basicConfig(
        level=logging.DEBUG,
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, mode="a"),
        ],
    )
    for noisy in ("websockets", "aiohttp", "httpx", "hpack", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger("bot_v2")


log = logging.getLogger("bot_v2")


# ═══════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════

@dataclass
class Round:
    condition_id: str
    token_yes:    str
    token_no:     str
    symbol:       str    # "btcusdt" / "ethusdt"
    start_time:   float
    end_time:     float
    start_price:  float  # Chainlink price-to-beat (0.0 if not available)


# ═══════════════════════════════════════════════════════════
# RISK GATE — every order must pass through here
# ═══════════════════════════════════════════════════════════

class RiskGate:
    """
    Independent risk gate. Tracks actual cost = price × shares,
    not budget estimates.
    """

    def __init__(self) -> None:
        self.total_exposure: float = 0.0   # sum of actual costs of open orders
        self.daily_loss:     float = 0.0   # realised loss today
        self._reset_at:      float = time.time()

    def _daily_reset(self) -> None:
        if time.time() - self._reset_at >= 86400:
            log.info(f"📅 Daily reset — yesterday's loss: ${self.daily_loss:.2f}")
            self.daily_loss = 0.0
            self._reset_at = time.time()

    def check(self, actual_cost: float) -> Tuple[bool, str]:
        """
        Returns (allowed, reason).
        actual_cost = price × shares — the real USDC that will leave the wallet.
        """
        self._daily_reset()
        if self.daily_loss >= MAX_DAILY_LOSS_USD:
            return False, f"daily loss limit (${self.daily_loss:.2f} lost today)"
        if self.total_exposure + actual_cost > MAX_TOTAL_EXPOSURE_USD:
            return False, (
                f"exposure limit (${self.total_exposure:.2f} open "
                f"+ ${actual_cost:.2f} new > ${MAX_TOTAL_EXPOSURE_USD:.2f})"
            )
        return True, "ok"

    def open(self, actual_cost: float) -> None:
        """Call immediately after an order is confirmed placed."""
        self.total_exposure = round(self.total_exposure + actual_cost, 4)

    def close(self, actual_cost: float, pnl: float) -> None:
        """Call when an order is filled (pnl = realised) or cancelled (pnl = 0)."""
        self.total_exposure = max(0.0, round(self.total_exposure - actual_cost, 4))
        if pnl < 0:
            self.daily_loss = round(self.daily_loss + abs(pnl), 4)

    def status(self) -> str:
        return (
            f"exposure=${self.total_exposure:.2f}/{MAX_TOTAL_EXPOSURE_USD:.0f} "
            f"loss=${self.daily_loss:.2f}/{MAX_DAILY_LOSS_USD:.0f}"
        )


# ═══════════════════════════════════════════════════════════
# BINANCE PRICE FEED
# ═══════════════════════════════════════════════════════════

class BinanceFeed:
    """Public WebSocket aggTrade stream — no API key needed."""

    def __init__(self, symbols: list) -> None:
        self.symbols   = symbols
        self._prices:  Dict[str, float] = {}
        self._ticks:   Dict[str, Deque] = {s: deque(maxlen=3000) for s in symbols}
        self._connected = False

    async def start(self) -> None:
        streams = "/".join(f"{s}@aggTrade" for s in self.symbols)
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        while True:
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10
                ) as ws:
                    self._connected = True
                    log.info(f"✅ Binance connected — {self.symbols}")
                    async for msg in ws:
                        self._parse(msg)
            except Exception as exc:
                self._connected = False
                log.warning(f"Binance disconnected: {exc} — retry in 2s")
                await asyncio.sleep(2)

    def _parse(self, raw: str) -> None:
        try:
            data  = json.loads(raw).get("data", {})
            sym   = data.get("s", "").lower()
            if sym not in self._ticks:
                return
            price = float(data["p"])
            ts    = data["T"] / 1000.0
            self._prices[sym] = price
            self._ticks[sym].append((ts, price))
        except (KeyError, ValueError, json.JSONDecodeError):
            pass

    def price(self, symbol: str) -> float:
        return self._prices.get(symbol, 0.0)

    def vol(self, symbol: str) -> float:
        """Annualised volatility from recent 1-s returns."""
        ticks = self._ticks.get(symbol, deque())
        if len(ticks) < 30:
            return DEFAULT_VOL
        now    = ticks[-1][0]
        cutoff = now - VOL_WINDOW_S
        prices = [p for t, p in ticks if t >= cutoff]
        if len(prices) < 10:
            return DEFAULT_VOL
        sampled = prices[::max(1, len(prices) // VOL_WINDOW_S)]
        if len(sampled) < 5:
            return DEFAULT_VOL
        lr = np.diff(np.log(sampled))
        v  = float(np.std(lr) * np.sqrt(365.25 * 24 * 3600))
        return max(0.20, min(3.0, v))

    @property
    def is_connected(self) -> bool:
        return self._connected


# ═══════════════════════════════════════════════════════════
# FAIR VALUE PRICING
# ═══════════════════════════════════════════════════════════

def fair_value_up(
    start_px:     float,
    current_px:   float,
    t_remaining_s: float,
    duration_s:   float,
    vol_annual:   float,
) -> float:
    """
    P(price_at_end > start_price) — normal CDF with mean reversion.

    Requirement 14: P(up) = Φ(adjusted_move / sigma_remaining)
      adjusted_move = raw_move × (1 - MEAN_REVERSION × frac_elapsed)
      sigma_remaining = vol_annual × sqrt(t_remaining_years)
    """
    if t_remaining_s <= 0:
        return 1.0 if current_px > start_px else 0.0
    if start_px <= 0:
        return 0.5

    move         = (current_px - start_px) / start_px
    frac_elapsed = max(0.0, 1.0 - (t_remaining_s / duration_s))
    adj_move     = move * (1.0 - MEAN_REVERSION * frac_elapsed)

    t_years          = t_remaining_s / (365.25 * 24.0 * 3600.0)
    sigma_remaining  = vol_annual * math.sqrt(t_years)

    if sigma_remaining < 1e-10:
        return 1.0 if adj_move > 0 else 0.0

    z = adj_move / sigma_remaining
    return float(stats.norm.cdf(z))


# ═══════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════

async def fetch_round(
    session:     aiohttp.ClientSession,
    symbol:      str,
    slug_prefix: str,
) -> Optional[Round]:
    """
    Fetch the current active 5-min round for one symbol.
    Tries current 5-min boundary, then next, then previous — returns the
    first one that is active and accepting orders.
    """
    from datetime import datetime

    now  = time.time()
    base = int(now // 300) * 300

    for t in [base, base + 300, base - 300]:
        slug = f"{slug_prefix}-{t}"
        try:
            async with session.get(
                f"{GAMMA_API}/events",
                params={"slug": slug},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    continue
                events = await resp.json()
        except Exception:
            continue

        if not events:
            continue

        event   = events[0]
        markets = event.get("markets", [])
        if not markets:
            continue
        market = markets[0]

        if not market.get("acceptingOrders", False):
            continue

        end_iso   = market.get("endDate")   or event.get("endDate", "")
        start_iso = event.get("startTime")  or market.get("startDate", "")

        try:
            end_t   = datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
            start_t = datetime.fromisoformat(start_iso.replace("Z", "+00:00")).timestamp()
        except Exception:
            end_t, start_t = float(t + 300), float(t)

        if end_t <= now:
            continue

        try:
            token_ids = json.loads(market.get("clobTokenIds", "[]"))
        except (json.JSONDecodeError, TypeError):
            continue
        if len(token_ids) < 2:
            continue

        start_price = 0.0
        meta = event.get("eventMetadata") or {}
        try:
            start_price = float(meta.get("priceToBeat", 0.0))
        except (ValueError, TypeError):
            pass

        condition_id = market.get("conditionId", "")
        log.info(
            f"📡 {symbol.upper()} │ {event.get('title', slug)[:50]} "
            f"│ ends={end_iso[11:19]} UTC │ cid={condition_id[:10]}…"
        )
        return Round(
            condition_id=condition_id,
            token_yes=token_ids[0],
            token_no=token_ids[1],
            symbol=symbol,
            start_time=start_t,
            end_time=end_t,
            start_price=start_price,
        )

    return None


# ═══════════════════════════════════════════════════════════
# BOT
# ═══════════════════════════════════════════════════════════

class Bot:

    def __init__(self, paper: bool) -> None:
        self.paper = paper

        self.feed    = BinanceFeed(list(SLUG_PREFIXES.keys()))
        self.risk    = RiskGate()

        self._session: Optional[aiohttp.ClientSession] = None
        self._client  = None   # py_clob_client.ClobClient, set in _connect_clob

        # Hard dedup: condition_ids we have already traded this session.
        # Once added, never removed. One order per round, period.
        self.rounds_traded: Set[str] = set()

        # Open orders: order_id → {condition_id, cost, placed_at}
        self._open: Dict[str, dict] = {}

        # Start-prices per condition_id (set on first discovery of that round)
        self._start_px: Dict[str, float] = {}

        # Discovery cache: symbol → (expires_at, Round|None)
        self._round_cache: Dict[str, Tuple[float, Optional[Round]]] = {}

    # ── Startup / shutdown ────────────────────────────────

    async def start(self) -> None:
        setup_logging()
        self._session = aiohttp.ClientSession()

        log.info("=" * 60)
        log.info("  POLYMARKET CRYPTO ARB BOT v2")
        log.info(f"  Mode : {'📝 PAPER' if self.paper else '💰 LIVE'}")
        log.info(f"  Limits: per_trade=${MAX_PER_TRADE_USD}  "
                 f"exposure=${MAX_TOTAL_EXPOSURE_USD}  "
                 f"daily_loss=${MAX_DAILY_LOSS_USD}")
        log.info(f"  Price range: [{MIN_SHARE_PRICE:.2f}, {MAX_SHARE_PRICE:.2f}]  "
                 f"min_shares={MIN_SHARES}  edge≥{MIN_EDGE_CENTS}¢")
        log.info(f"  STOP file: {STOP_FILE}")
        log.info("=" * 60)

        if not self.paper:
            if not await self._connect_clob():
                log.error("CLOB connection failed — aborting")
                return

        try:
            await asyncio.gather(
                self.feed.start(),      # Binance WebSocket (reconnects automatically)
                self._trade_loop(),     # THE single trade loop — only one, always
                self._status_loop(),    # Periodic status log
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def _connect_clob(self) -> bool:
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            key         = os.environ.get("POLY_API_KEY", "")
            secret      = os.environ.get("POLY_API_SECRET", "")
            passphrase  = os.environ.get("POLY_PASSPHRASE", "")
            private_key = os.environ.get("POLY_PRIVATE_KEY", "")
            funder      = os.environ.get("POLY_FUNDER", "")

            missing = [
                name for name, val in [
                    ("POLY_API_KEY",     key),
                    ("POLY_API_SECRET",  secret),
                    ("POLY_PASSPHRASE",  passphrase),
                    ("POLY_PRIVATE_KEY", private_key),
                    ("POLY_FUNDER",      funder),
                ] if not val
            ]
            if missing:
                log.error(f"Missing .env vars: {', '.join(missing)}")
                return False

            if not private_key.startswith("0x"):
                private_key = "0x" + private_key

            self._client = ClobClient(
                host=POLYMARKET_HOST,
                chain_id=CHAIN_ID,
                key=private_key,
                creds=ApiCreds(
                    api_key=key,
                    api_secret=secret,
                    api_passphrase=passphrase,
                ),
                signature_type=1,   # Magic/proxy wallet: EOA signs, funder holds USDC
                funder=funder,
            )
            log.info(f"✅ CLOB connected (funder={funder[:10]}…  sig_type=1)")
            return True

        except Exception as exc:
            log.error(f"CLOB connect error: {exc}")
            return False

    async def _shutdown(self) -> None:
        log.info("Shutting down — cancelling open orders…")
        for oid, info in list(self._open.items()):
            await self._cancel(oid, info["cost"])
        if self._session:
            await self._session.close()
        log.info(
            f"Shutdown complete. rounds_traded={len(self.rounds_traded)} "
            f"{self.risk.status()}"
        )

    # ── THE single trade loop ─────────────────────────────

    async def _trade_loop(self) -> None:
        """
        Single trading coroutine. There is exactly one instance of this
        in asyncio.gather — impossible to duplicate by construction.

        Checks the STOP file, discovers rounds, evaluates edge, and places
        at most one order per round for the lifetime of the process.
        """
        # Wait for Binance to connect before doing anything
        while not self.feed.is_connected:
            await asyncio.sleep(0.5)

        log.info("🚀 Trade loop started")

        while True:
            try:
                await self._tick()
            except Exception as exc:
                log.error(f"Tick error: {exc}", exc_info=True)
            await asyncio.sleep(1.0)

    async def _tick(self) -> None:
        """One iteration: kill-switch → stale-order cleanup → discover → evaluate."""

        # ── Kill switch ──────────────────────────────────────────────
        if os.path.exists(STOP_FILE):
            log.warning(f"🛑 STOP file detected ({STOP_FILE}) — halting bot now")
            os.kill(os.getpid(), signal.SIGTERM)
            return

        # ── Cancel orders that have been open too long ────────────────
        now = time.time()
        for oid, info in list(self._open.items()):
            if now - info["placed_at"] > ORDER_TTL_S:
                log.debug(f"Cancelling stale order {oid[:14]}… (>{ORDER_TTL_S}s)")
                await self._cancel(oid, info["cost"])

        # ── Discover active rounds (cached) ──────────────────────────
        for symbol, prefix in SLUG_PREFIXES.items():
            rnd = await self._get_round(symbol, prefix)
            if rnd:
                await self._evaluate(rnd)

    # ── Round discovery with cache ────────────────────────

    async def _get_round(self, symbol: str, prefix: str) -> Optional[Round]:
        """Return the active round for symbol, refreshing cache every 15s or on expiry."""
        now             = time.time()
        exp, cached_rnd = self._round_cache.get(symbol, (0.0, None))

        # Use cache if still valid and round hasn't ended
        if cached_rnd and cached_rnd.end_time > now and now < exp:
            return cached_rnd

        # Fetch fresh
        rnd = await fetch_round(self._session, symbol, prefix)
        self._round_cache[symbol] = (now + DISCOVERY_CACHE_S, rnd)
        return rnd

    # ── Evaluate one round ────────────────────────────────

    async def _evaluate(self, rnd: Round) -> None:
        """
        Evaluate a round for a trading opportunity.
        If edge exists and all checks pass, place exactly one order.
        """

        # ── Hard dedup (req 3, 9): one order per round, period ───────
        if rnd.condition_id in self.rounds_traded:
            return

        # ── Time check ───────────────────────────────────────────────
        now         = time.time()
        t_remaining = rnd.end_time - now
        if t_remaining < NO_ENTRY_BUFFER_S:
            return

        # ── Binance price ─────────────────────────────────────────────
        current_px = self.feed.price(rnd.symbol)
        if current_px <= 0:
            return

        # ── Lock in start price on first sight of this round ─────────
        if rnd.condition_id not in self._start_px:
            start_px = rnd.start_price if rnd.start_price > 0 else current_px
            self._start_px[rnd.condition_id] = start_px
        start_px = self._start_px[rnd.condition_id]

        # ── Fair value (req 14) ───────────────────────────────────────
        vol   = self.feed.vol(rnd.symbol)
        p_up  = fair_value_up(
            start_px=start_px,
            current_px=current_px,
            t_remaining_s=t_remaining,
            duration_s=300.0,
            vol_annual=vol,
        )
        fair_yes = p_up
        fair_no  = 1.0 - fair_yes

        # ── Fetch order books ─────────────────────────────────────────
        yes_book = await self._get_book(rnd.token_yes)
        no_book  = await self._get_book(rnd.token_no)
        if not yes_book or not no_book:
            return

        yes_ask = self._best_ask(yes_book)
        no_ask  = self._best_ask(no_book)
        yes_bid = self._best_bid(yes_book)
        no_bid  = self._best_bid(no_book)

        if yes_ask is None or no_ask is None:
            return

        market_yes = (yes_bid + yes_ask) / 2.0 if yes_bid else yes_ask
        market_no  = (no_bid  + no_ask)  / 2.0 if no_bid  else no_ask

        # ── Edge calculation ─────────────────────────────────────────
        # edge = how much market price exceeds our fair value
        edge_yes_overpriced = market_yes - fair_yes   # > 0 means YES overpriced
        edge_no_overpriced  = market_no  - fair_no    # > 0 means NO  overpriced

        min_edge = MIN_EDGE_CENTS / 100.0

        # ── Select trade direction (req 6): BUY complement only ──────
        if edge_yes_overpriced >= min_edge and edge_yes_overpriced >= edge_no_overpriced:
            # YES overpriced → BUY NO (req 7: 1¢ below NO ask, maker only)
            order_token = rnd.token_no
            raw_price   = no_ask - 0.01
            edge_used   = edge_yes_overpriced
            direction   = f"YES overpriced ({market_yes:.2f} > fair {fair_yes:.2f}) → BUY NO"
        elif edge_no_overpriced >= min_edge:
            # NO overpriced → BUY YES (req 7: 1¢ below YES ask, maker only)
            order_token = rnd.token_yes
            raw_price   = yes_ask - 0.01
            edge_used   = edge_no_overpriced
            direction   = f"NO overpriced ({market_no:.2f} > fair {fair_no:.2f}) → BUY YES"
        else:
            return   # no edge

        order_price = round(raw_price, 2)

        # ── Maker safety: must stay below the ask ────────────────────
        ref_ask = no_ask if order_token == rnd.token_no else yes_ask
        if order_price <= 0 or order_price >= ref_ask:
            log.debug(f"Maker guard: {order_price:.2f} >= ask {ref_ask:.2f} — skip")
            return

        # ── Price range filter (req 10) ──────────────────────────────
        if order_price < MIN_SHARE_PRICE or order_price > MAX_SHARE_PRICE:
            log.debug(
                f"Price range skip: {order_price:.2f} not in "
                f"[{MIN_SHARE_PRICE:.2f}, {MAX_SHARE_PRICE:.2f}]"
            )
            return

        # ── Minimum-shares affordability check (req 11) ──────────────
        min_cost = MIN_SHARES * order_price
        if min_cost > MAX_PER_TRADE_USD:
            log.debug(
                f"Min-size skip: {MIN_SHARES} × {order_price:.2f} = "
                f"${min_cost:.2f} > ${MAX_PER_TRADE_USD:.2f}"
            )
            return

        # ── Compute shares and actual cost (req 4) ───────────────────
        shares      = max(MIN_SHARES, int(MAX_PER_TRADE_USD / order_price))
        actual_cost = round(shares * order_price, 4)   # real USDC leaving wallet

        # ── RiskGate check (req 2) ────────────────────────────────────
        allowed, reason = self.risk.check(actual_cost)
        if not allowed:
            log.debug(f"RiskGate blocked: {reason}")
            return

        # ── Log intention ─────────────────────────────────────────────
        log.info(
            f"{'📝' if self.paper else '🟢'} {rnd.symbol.upper()} │ {direction} │ "
            f"{shares} shares @ {order_price:.2f} │ cost=${actual_cost:.2f} │ "
            f"edge={edge_used*100:.1f}¢ │ fair_yes={fair_yes:.2f} │ "
            f"mkt_yes={market_yes:.2f} │ t={t_remaining:.0f}s left"
        )

        # ── Place order ───────────────────────────────────────────────
        order_id = await self._place_order(
            token_id=order_token,
            price=order_price,
            size=float(shares),
        )

        if order_id:
            # Hard dedup: this round is permanently locked (req 3, 9)
            self.rounds_traded.add(rnd.condition_id)
            self._open[order_id] = {
                "condition_id": rnd.condition_id,
                "cost":         actual_cost,
                "placed_at":    time.time(),
            }
            self.risk.open(actual_cost)   # actual cost, not estimate
            log.info(f"   ↳ Order confirmed: {order_id[:20]}…")

    # ── Order execution ───────────────────────────────────

    async def _place_order(
        self, token_id: str, price: float, size: float
    ) -> Optional[str]:
        """Place a BUY limit order. Returns order_id or None on failure."""

        if self.paper:
            fake_id = f"paper_{int(time.time() * 1000)}"
            log.info(f"   [PAPER] order_id={fake_id}")
            return fake_id

        if not self._client:
            log.error("No CLOB client — cannot place order")
            return None

        try:
            from py_clob_client.clob_types import OrderArgs
            args   = OrderArgs(token_id=token_id, price=price, size=size, side="BUY")
            result = self._client.create_and_post_order(args)

            if result and result.get("orderID"):
                return result["orderID"]

            log.warning(f"Unexpected order response: {result}")
            return None

        except Exception as exc:
            log.error(f"Order error: {exc}")
            return None

    async def _cancel(self, order_id: str, cost: float) -> None:
        """Cancel an open order and release its risk exposure."""
        self._open.pop(order_id, None)
        self.risk.close(cost, 0.0)   # no PnL on cancel

        if self.paper:
            log.debug(f"[PAPER] Cancelled {order_id[:16]}")
            return

        if not self._client:
            return
        try:
            self._client.cancel(order_id=order_id)
            log.debug(f"Cancelled {order_id[:16]}…")
        except Exception as exc:
            log.warning(f"Cancel failed ({order_id[:14]}…): {exc}")

    # ── Order book helpers ────────────────────────────────

    async def _get_book(self, token_id: str) -> Optional[Dict]:
        try:
            async with self._session.get(
                f"{POLYMARKET_HOST}/book",
                params={"token_id": token_id},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception:
            return None

    @staticmethod
    def _best_ask(book: Dict) -> Optional[float]:
        try:
            return float(book["asks"][0]["price"])
        except (KeyError, IndexError, ValueError, TypeError):
            return None

    @staticmethod
    def _best_bid(book: Dict) -> Optional[float]:
        try:
            return float(book["bids"][0]["price"])
        except (KeyError, IndexError, ValueError, TypeError):
            return None

    # ── Periodic status ───────────────────────────────────

    async def _status_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            prices = "  ".join(
                f"{s.upper()}=${self.feed.price(s):,.2f}"
                for s in SLUG_PREFIXES
                if self.feed.price(s) > 0
            )
            log.info(
                f"📊 {prices} │ {self.risk.status()} │ "
                f"rounds_traded={len(self.rounds_traded)} "
                f"open_orders={len(self._open)}"
            )


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Polymarket Crypto Arb Bot v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 bot_v2.py --paper    # safe test, no real orders\n"
            "  python3 bot_v2.py --live     # real money\n"
            "\n"
            "Stop safely:  touch ~/polymarket-bot/STOP\n"
            "Also add POLY_FUNDER=0x... to your .env file.\n"
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--paper", action="store_true", help="Paper mode — no real orders")
    group.add_argument("--live",  action="store_true", help="Live trading — real money")
    args = parser.parse_args()

    bot  = Bot(paper=args.paper)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_signal(sig, frame):
        log.info(f"Signal {sig} — shutting down…")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        loop.run_until_complete(bot.start())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
