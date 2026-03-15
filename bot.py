#!/usr/bin/env python3
"""
Polymarket Crypto Round Maker Bot
==================================
Watches BTC/ETH on Binance. When prices move, posts maker orders
on Polymarket's crypto round markets at the correct price.

Usage:
    python3 bot.py --paper      # Fake trades, real prices (test mode)
    python3 bot.py --live       # Real money trading

Press Ctrl+C to stop.
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
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

# ── Third-party imports ──
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

# ── Load .env file (API keys) ──
load_dotenv()

# ── Our config ──
import config as cfg


# ═══════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════

def setup_logging():
    fmt = "%(asctime)s │ %(levelname)-5s │ %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(
        level=getattr(logging, cfg.LOG_LEVEL),
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(cfg.LOG_FILE, mode="a"),
        ],
    )
    # Quiet down noisy libraries
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

log = logging.getLogger("bot")


# ═══════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════

@dataclass
class PriceTick:
    price: float
    timestamp: float
    symbol: str

@dataclass
class RoundMarket:
    """A Polymarket crypto round market."""
    condition_id: str       # Polymarket market ID
    token_id_yes: str       # YES token ID
    token_id_no: str        # NO token ID
    symbol: str             # e.g., "btcusdt"
    direction: str          # "up" or "down"
    duration: int           # seconds (300 or 900)
    start_time: float       # unix timestamp
    end_time: float         # unix timestamp
    start_price: float      # price at round start

@dataclass
class TradeRecord:
    """Log of a completed trade."""
    timestamp: float
    market_id: str
    side: str
    price: float
    size: float
    pnl: float = 0.0
    status: str = "open"


# ═══════════════════════════════════════════════════════════
# AGENT 1: BINANCE PRICE FEED
# ═══════════════════════════════════════════════════════════

class BinanceFeed:
    """
    Connects to Binance WebSocket for real-time BTC/ETH prices.
    No API key needed — this is a public data stream.
    """

    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self._prices: Dict[str, float] = {}
        self._ticks: Dict[str, Deque[PriceTick]] = {
            s: deque(maxlen=3000) for s in symbols
        }
        self._running = False
        self._connected = False

    async def start(self):
        """Connect to Binance and stream prices."""
        self._running = True
        streams = "/".join(f"{s}@aggTrade" for s in self.symbols)
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"

        while self._running:
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10
                ) as ws:
                    self._connected = True
                    log.info(f"✅ Connected to Binance — streaming {self.symbols}")

                    async for msg in ws:
                        if not self._running:
                            break
                        self._parse(msg)

            except Exception as e:
                self._connected = False
                if self._running:
                    log.warning(f"Binance disconnected: {e} — reconnecting in 2s")
                    await asyncio.sleep(2)

    def stop(self):
        self._running = False

    def _parse(self, raw: str):
        """Parse a Binance aggTrade message."""
        try:
            data = json.loads(raw).get("data", {})
            symbol = data.get("s", "").lower()
            if symbol not in self._ticks:  # _ticks is initialized with known symbols
                return

            tick = PriceTick(
                price=float(data["p"]),
                timestamp=data["T"] / 1000.0,
                symbol=symbol,
            )
            self._prices[symbol] = tick.price
            self._ticks[symbol].append(tick)
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

    def get_price(self, symbol: str) -> float:
        """Current price for a symbol."""
        return self._prices.get(symbol, 0.0)

    def get_volatility(self, symbol: str) -> float:
        """
        Compute annualized volatility from recent 1-second returns.
        This is the key input for our fair value model.
        """
        ticks = self._ticks.get(symbol)
        if not ticks or len(ticks) < 30:
            return 0.8  # default assumption for BTC

        # Sample prices at 1-second intervals over last 60s
        now = ticks[-1].timestamp
        prices = []
        target_t = now - cfg.VOL_WINDOW_SECONDS

        for tick in ticks:
            if tick.timestamp >= target_t:
                prices.append(tick.price)

        if len(prices) < 10:
            return 0.8

        # Subsample to ~1 per second
        sampled = prices[::max(1, len(prices) // cfg.VOL_WINDOW_SECONDS)]
        if len(sampled) < 5:
            return 0.8

        log_returns = np.diff(np.log(sampled))
        vol = float(np.std(log_returns) * np.sqrt(365.25 * 24 * 3600))

        # Clamp to reasonable range
        return max(0.20, min(3.0, vol))

    @property
    def is_connected(self) -> bool:
        return self._connected


# ═══════════════════════════════════════════════════════════
# AGENT 2: PRICING ENGINE
# ═══════════════════════════════════════════════════════════

class PricingEngine:
    """
    Converts exchange price moves into fair values for Polymarket rounds.

    The model:
    - A "5-min BTC up" round resolves YES if BTC ends higher than start.
    - Given current price move and remaining time + volatility,
      we compute P(YES) using the normal CDF.
    - If our fair value differs from the market price → edge → trade.
    """

    def __init__(self):
        self.mean_reversion = cfg.MEAN_REVERSION_FACTOR
        self.min_edge = cfg.MIN_EDGE_CENTS / 100.0

    def fair_value_yes(
        self,
        start_price: float,
        current_price: float,
        time_remaining_s: float,
        duration_s: float,
        direction: str,
        vol: float,
    ) -> float:
        """
        Compute fair price of YES token.

        For "up" round: P(YES) = P(price_at_end > start_price)
        For "down" round: P(YES) = P(price_at_end < start_price)
        """
        if time_remaining_s <= 0:
            move = (current_price - start_price) / start_price
            went_up = move > 0
            if direction == "up":
                return 1.0 if went_up else 0.0
            else:
                return 0.0 if went_up else 1.0

        # Current move as fraction
        move = (current_price - start_price) / start_price

        # Time remaining in years (for annualized vol)
        t_years = time_remaining_s / (365.25 * 24 * 3600)

        # Expected remaining volatility
        sigma = vol * math.sqrt(t_years)
        if sigma < 1e-10:
            sigma = 1e-10

        # Mean reversion adjustment
        frac_elapsed = 1.0 - (time_remaining_s / duration_s)
        adj_move = move * (1.0 - self.mean_reversion * frac_elapsed)

        # Z-score → probability via normal CDF
        d = adj_move / sigma
        prob_up = float(stats.norm.cdf(d))

        if direction == "up":
            return round(prob_up, 4)
        else:
            return round(1.0 - prob_up, 4)

    def find_edge(
        self,
        fair_yes: float,
        market_yes: float,
    ) -> Tuple[Optional[str], float]:
        """
        Determine if there's a tradeable edge, net of fees.

        Fees: 10% maker + 10% taker = ~20% round-trip on these markets.
        We compute net edge AFTER subtracting maker fee from our sale price.

        Returns (side, net_edge_in_decimal):
            side = "sell_yes" if market overprices YES
            side = "sell_no" if market overprices NO
            side = None if no profitable edge after fees
        """
        fair_no = 1.0 - fair_yes
        market_no = 1.0 - market_yes

        # Net edge = gross edge minus maker fee on the sale
        # When we sell YES at market_yes, we net: market_yes * (1 - MAKER_FEE)
        net_sell_yes = market_yes * (1.0 - cfg.MAKER_FEE) - fair_yes
        net_sell_no  = market_no  * (1.0 - cfg.MAKER_FEE) - fair_no

        best_side = None
        best_edge = 0.0

        if net_sell_yes > self.min_edge and net_sell_yes >= net_sell_no:
            best_side = "sell_yes"
            best_edge = net_sell_yes
        elif net_sell_no > self.min_edge:
            best_side = "sell_no"
            best_edge = net_sell_no

        return best_side, best_edge

    def maker_price(self, fair_yes: float, side: str, edge: float) -> float:
        """
        Compute the limit order price for our maker order.
        We capture a portion of the edge (not all — need to be attractive).
        """
        capture_ratio = 0.55  # take 55% of edge, leave 45% for taker

        if side == "sell_yes":
            price = fair_yes + edge * capture_ratio
        else:
            fair_no = 1.0 - fair_yes
            price = fair_no + edge * capture_ratio

        # Round to cent (Polymarket tick size) and clamp
        price = round(max(0.01, min(0.99, price)) * 100) / 100
        return price


# ═══════════════════════════════════════════════════════════
# AGENT 3 + 5: RISK MANAGER (combined for simplicity)
# ═══════════════════════════════════════════════════════════

class RiskManager:
    """
    Controls position sizes and stops trading when limits are hit.
    This is your safety net.
    """

    def __init__(self):
        self.max_per_trade = cfg.MAX_PER_TRADE_USD
        self.max_total = cfg.MAX_TOTAL_EXPOSURE_USD
        self.max_daily_loss = cfg.MAX_DAILY_LOSS_USD
        self.no_entry_buffer = cfg.NO_ENTRY_BUFFER_SECONDS

        # State
        self.open_exposure: float = 0.0
        self.daily_pnl: float = 0.0
        self.daily_reset_time: float = time.time()
        self.trades_today: int = 0
        self.open_positions: Dict[str, float] = {}  # market_id → $ amount

    def can_trade(
        self,
        proposed_usd: float,
        time_remaining: float,
        confidence: float,
    ) -> Tuple[bool, float, str]:
        """
        Check if a proposed trade is allowed.
        Returns (allowed, adjusted_size_usd, reason).
        """
        self._check_daily_reset()

        # Time buffer
        if time_remaining < self.no_entry_buffer:
            return False, 0, f"Too close to round end ({time_remaining:.0f}s left)"

        # Daily loss limit
        if self.daily_pnl <= -self.max_daily_loss:
            return False, 0, f"Daily loss limit hit (${self.daily_pnl:.2f})"

        # Confidence check
        if confidence < cfg.MIN_CONFIDENCE:
            return False, 0, f"Low confidence ({confidence:.2f})"

        # Total exposure
        remaining_capacity = self.max_total - self.open_exposure
        if remaining_capacity <= 0:
            return False, 0, f"Max exposure reached (${self.open_exposure:.2f})"

        # Size: min of proposed, per-trade limit, and remaining capacity
        size = min(proposed_usd, self.max_per_trade, remaining_capacity)

        # Scale down if we're getting close to daily loss limit
        loss_headroom = self.max_daily_loss + self.daily_pnl
        if loss_headroom < self.max_daily_loss * 0.3:
            size *= 0.5  # half size when close to limit

        if size < 1.0:
            return False, 0, "Position too small (<$1)"

        return True, round(size, 2), "OK"

    def open_position(self, market_id: str, usd_amount: float):
        self.open_positions[market_id] = usd_amount
        self.open_exposure = sum(self.open_positions.values())
        self.trades_today += 1

    def close_position(self, market_id: str, pnl: float):
        self.open_positions.pop(market_id, None)
        self.open_exposure = sum(self.open_positions.values())
        self.daily_pnl += pnl

    def _check_daily_reset(self):
        if time.time() - self.daily_reset_time > 86400:
            log.info(f"📊 Daily reset — yesterday's PnL: ${self.daily_pnl:.2f}")
            self.daily_pnl = 0.0
            self.trades_today = 0
            self.daily_reset_time = time.time()

    def status(self) -> str:
        return (
            f"PnL: ${self.daily_pnl:+.2f} │ "
            f"Open: ${self.open_exposure:.2f}/{self.max_total:.0f} │ "
            f"Trades: {self.trades_today}"
        )


# ═══════════════════════════════════════════════════════════
# AGENT 4: POLYMARKET EXECUTION
# ═══════════════════════════════════════════════════════════

class PolymarketExecutor:
    """
    Handles order placement on Polymarket's CLOB.

    In paper mode: logs orders but doesn't send them.
    In live mode: uses py-clob-client to place real orders.
    """

    # Proxy wallet (Gnosis Safe / Magic email wallet) — this is where USDC lives
    # EOA (signing key): 0x8a6e9a250fAdDbE137Dde4a1468D7E439fC6380E  ← signs txns
    # Proxy (funder):    0x4962C6d3b430456558b77844321D2d604969dE4E  ← holds USDC
    FUNDER = "0x4962C6d3b430456558b77844321D2d604969dE4E"

    def __init__(self, paper_mode: bool = True):
        self.paper = paper_mode
        self._client = None
        self._http: Optional[aiohttp.ClientSession] = None
        self.orders: List[Dict] = []

    async def connect(self):
        """Initialize Polymarket client and shared HTTP session."""
        self._http = aiohttp.ClientSession()

        if self.paper:
            log.info("📝 Running in PAPER mode — real books, no real orders")
            return True

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            key        = os.environ.get("POLY_API_KEY", "")
            secret     = os.environ.get("POLY_API_SECRET", "")
            passphrase = os.environ.get("POLY_PASSPHRASE", "")
            private_key = os.environ.get("POLY_PRIVATE_KEY", "")

            if not all([key, secret, passphrase, private_key]):
                log.error(
                    "❌ Missing Polymarket credentials in .env!\n"
                    "   POLY_API_KEY, POLY_API_SECRET, POLY_PASSPHRASE, POLY_PRIVATE_KEY"
                )
                return False

            creds = ApiCreds(
                api_key=key,
                api_secret=secret,
                api_passphrase=passphrase,
            )

            # signature_type=1 = Magic/proxy wallet (EOA signs, but funder is the proxy Gnosis Safe)
            # EOA (signer):  0x8a6e9a250fAdDbE137Dde4a1468D7E439fC6380E
            # Proxy (funder): 0x4962C6d3b430456558b77844321D2d604969dE4E  ← holds USDC
            self._client = ClobClient(
                host=cfg.POLYMARKET_API_URL,
                chain_id=cfg.CHAIN_ID,
                key=private_key,
                creds=creds,
                signature_type=1,
                funder=self.FUNDER,
            )

            log.info(f"✅ Connected to Polymarket CLOB API (proxy={self.FUNDER[:10]}…)")
            return True

        except ImportError:
            log.error("❌ py-clob-client not installed. Run: pip3 install py-clob-client")
            return False
        except Exception as e:
            log.error(f"❌ Polymarket connection failed: {e}")
            return False

    async def place_maker_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        market_id: str,
    ) -> Optional[str]:
        """
        Place a maker limit order.

        For sell_yes: we SELL YES tokens at `price`
        For sell_no: we SELL NO tokens at `price`
        """
        order_record = {
            "time": time.strftime("%H:%M:%S"),
            "market": market_id[:12],
            "side": side,
            "price": price,
            "size": size,
            "usd": round(price * size, 2),
            "status": "placed",
        }

        if self.paper:
            order_id = f"paper_{int(time.time()*1000)}"
            order_record["order_id"] = order_id
            self.orders.append(order_record)
            log.info(
                f"📝 [PAPER] {side.upper()} │ "
                f"{size:.0f} shares @ {price*100:.0f}¢ │ "
                f"${price*size:.2f}"
            )
            return order_id

        try:
            from py_clob_client.clob_types import OrderArgs
            # side is passed in as "BUY" or "SELL" from the trading loop
            clob_side = side.upper() if side.upper() in ("BUY", "SELL") else "BUY"
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=clob_side,
            )
            log.debug(
                f"[ORDER] {clob_side} token={token_id[:12]}… "
                f"price={price} size={size} "
                f"maker={self.FUNDER[:10]}… sig_type=1"
            )
            result = self._client.create_and_post_order(order_args)

            if result and result.get("orderID"):
                order_id = result["orderID"]
                order_record["order_id"] = order_id
                self.orders.append(order_record)
                log.info(
                    f"🟢 ORDER PLACED │ {side.upper()} │ "
                    f"{size:.0f} shares @ {price*100:.0f}¢ │ "
                    f"${price*size:.2f} │ ID: {order_id[:8]}"
                )
                return order_id
            else:
                log.warning(f"Order failed: {result}")
                return None

        except Exception as e:
            log.error(f"Order error: {e}")
            return None

    async def close(self):
        if self._http:
            await self._http.close()

    async def cancel_order(self, order_id: str) -> bool:
        if self.paper:
            log.debug(f"[PAPER] Cancel: {order_id}")
            return True
        try:
            self._client.cancel(order_id=order_id)
            return True
        except Exception as e:
            log.warning(f"Cancel failed: {e}")
            return False

    async def get_order_book(self, token_id: str) -> Optional[Dict]:
        """
        Fetch the real CLOB order book for a token.
        In paper mode with synthetic token IDs, returns a synthetic book
        so the trading logic can be tested end-to-end.
        """
        # Synthetic token IDs (paper mode with synthetic rounds)
        if token_id.startswith(("yes_", "no_", "synthetic_")):
            return self._synthetic_book()

        try:
            url = f"{cfg.POLYMARKET_API_URL}/book?token_id={token_id}"
            async with self._http.get(url) as resp:
                if resp.status != 200:
                    log.warning(f"CLOB book HTTP {resp.status} for {token_id[:12]}…")
                    return None
                return await resp.json()
        except Exception as e:
            log.warning(f"Book fetch error: {e}")
            return None

    def _synthetic_book(self) -> Dict:
        """Randomised fake order book for paper trading logic tests."""
        import random
        mid = round(random.uniform(0.35, 0.65), 2)
        spread = 0.01
        return {
            "bids": [
                {"price": str(round(mid - spread*(i+1), 2)),
                 "size": str(random.randint(50, 500))}
                for i in range(5)
            ],
            "asks": [
                {"price": str(round(mid + spread*(i+1), 2)),
                 "size": str(random.randint(50, 500))}
                for i in range(5)
            ],
        }


# ═══════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════

class MarketDiscovery:
    """
    Finds active crypto round markets on Polymarket.
    Always queries the real Gamma API by series slug.
    Tracks round lifecycle: detects endings and auto-discovers next round.
    """

    GAMMA_API = cfg.GAMMA_API_URL

    def __init__(self, paper_mode: bool = True):
        self.paper = paper_mode  # kept for reference; discovery is always real
        self._session: Optional[aiohttp.ClientSession] = None
        # Per-symbol cache: condition_id -> RoundMarket
        self._active: Dict[str, RoundMarket] = {}
        self._cache_until: float = 0.0

    async def start(self):
        self._session = aiohttp.ClientSession()

    async def stop(self):
        if self._session:
            await self._session.close()

    async def find_active_rounds(self) -> List[RoundMarket]:
        """
        Return currently active rounds for all configured symbols.
        Paper mode uses synthetic rounds (for logic testing).
        Live mode uses real Gamma API, cached 15s, refreshed on round end.
        """
        if self.paper:
            return self._synthetic_rounds()

        now = time.time()

        # Check if any cached rounds have expired — force refresh if so
        for cid, rnd in list(self._active.items()):
            if rnd.end_time <= now:
                log.info(
                    f"🔄 Round ended: {rnd.symbol.upper()} "
                    f"(cid={cid[:10]}…) — fetching next round"
                )
                self._active.pop(cid, None)
                self._cache_until = 0  # force immediate refresh

        if now >= self._cache_until:
            await self._refresh_all()
            self._cache_until = now + 15.0

        return list(self._active.values())

    async def _refresh_all(self):
        """Fetch current round for each symbol using computed event slugs."""
        for symbol, series_prefix in cfg.SERIES_SLUGS.items():
            existing = [r for r in self._active.values() if r.symbol == symbol]
            if existing and existing[0].end_time > time.time():
                continue
            try:
                rnd = await self._fetch_current_round(symbol, series_prefix)
                if rnd:
                    self._active[rnd.condition_id] = rnd
            except Exception as e:
                log.warning(f"Discovery error for {symbol}: {e}")

    async def _fetch_current_round(
        self, symbol: str, series_prefix: str
    ) -> Optional[RoundMarket]:
        """
        Compute the current 5-minute round slug from wall clock time and fetch it.
        Slug format: {btc|eth}-updown-5m-{round_start_unix_timestamp}
        Rounds are aligned to 5-minute boundaries (epoch % 300 == 0).
        Tries current boundary, then next, then previous as fallback.
        """
        from datetime import datetime
        now = time.time()

        # Convert series slug (btc-up-or-down-5m) to event slug prefix (btc-updown-5m)
        # btc-up-or-down-5m -> btc-updown-5m
        # eth-up-or-down-5m -> eth-updown-5m
        ticker = series_prefix.replace("-up-or-down-", "-updown-")

        # Try current boundary, next, and previous
        base = int(now // 300) * 300
        for t in [base, base + 300, base - 300]:
            slug = f"{ticker}-{t}"
            rnd = await self._fetch_event_by_slug(symbol, slug, t, t + 300)
            if rnd:
                return rnd

        log.warning(f"No active round found for {symbol} (tried slugs around t={base})")
        return None

    async def _fetch_event_by_slug(
        self, symbol: str, slug: str, fallback_start: float, fallback_end: float
    ) -> Optional[RoundMarket]:
        """Fetch a single event by exact slug and parse into RoundMarket."""
        from datetime import datetime
        url = f"{self.GAMMA_API}/events"

        async with self._session.get(url, params={"slug": slug}) as resp:
            if resp.status != 200:
                return None
            events = await resp.json()

        if not events:
            return None

        event  = events[0]
        markets = event.get("markets", [])
        if not markets:
            return None
        market = markets[0]

        if not market.get("acceptingOrders", False):
            return None

        end_iso   = market.get("endDate") or event.get("endDate", "")
        start_iso = event.get("startTime") or market.get("startDate", "")

        try:
            end_time   = datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
            start_time = datetime.fromisoformat(start_iso.replace("Z", "+00:00")).timestamp()
        except Exception:
            end_time, start_time = fallback_end, fallback_start

        if end_time <= time.time():
            return None

        token_ids_raw = market.get("clobTokenIds", "[]")
        try:
            token_ids = json.loads(token_ids_raw)
        except (json.JSONDecodeError, TypeError):
            return None

        if len(token_ids) < 2:
            return None

        start_price = 0.0
        meta = event.get("eventMetadata") or {}
        if meta and "priceToBeat" in meta:
            start_price = float(meta["priceToBeat"])

        condition_id = market.get("conditionId", "")

        log.info(
            f"📡 Discovered: {event.get('title', symbol)} | "
            f"price_to_beat=${start_price:,.2f} | "
            f"ends={end_iso} | cid={condition_id[:10]}…"
        )

        return RoundMarket(
            condition_id=condition_id,
            token_id_yes=token_ids[0],
            token_id_no=token_ids[1],
            symbol=symbol,
            direction="up",
            duration=300,
            start_time=start_time,
            end_time=end_time,
            start_price=start_price,
        )

    def _synthetic_rounds(self) -> List[RoundMarket]:
        """Generate fake rounds aligned to 5-min boundaries (paper mode only)."""
        now = time.time()
        round_start = now - (now % 300)
        round_end = round_start + 300
        rounds = []
        for symbol in cfg.SYMBOLS:
            for direction in ["up", "down"]:
                rounds.append(RoundMarket(
                    condition_id=f"synthetic_{symbol}_{direction}_{int(round_start)}",
                    token_id_yes=f"yes_{symbol}_{direction}",
                    token_id_no=f"no_{symbol}_{direction}",
                    symbol=symbol,
                    direction=direction,
                    duration=300,
                    start_time=round_start,
                    end_time=round_end,
                    start_price=0.0,
                ))
        return rounds


# ═══════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════

class Bot:
    """
    Main bot — ties everything together.

    Loop:
    1. Feed streams prices from Binance
    2. Every second, check for active rounds
    3. For each round, compute fair value vs market price
    4. If edge exists and risk allows → place maker order
    5. Manage open orders (cancel stale ones)
    """

    def __init__(self, paper_mode: bool = True):
        self.paper = paper_mode
        self.feed = BinanceFeed(cfg.SYMBOLS)
        self.pricer = PricingEngine()
        self.risk = RiskManager()
        self.executor = PolymarketExecutor(paper_mode)
        self.discovery = MarketDiscovery(paper_mode)

        self._running = False
        self._round_start_prices: Dict[str, float] = {}  # track round start prices
        self._active_orders: Dict[str, Dict] = {}         # order_id → info
        self._stats = {"signals": 0, "orders": 0, "fills": 0}
        # Dedup guard: condition_ids currently locked for order placement.
        # Prevents concurrent _check_opportunities calls from doubling up on
        # the same market within one round.
        self._order_lock: asyncio.Lock = asyncio.Lock()
        self._locked_markets: set = set()  # condition_ids with an in-flight order

    async def start(self):
        """Boot up everything and start trading."""
        setup_logging()
        self._running = True

        log.info("=" * 55)
        log.info("  POLYMARKET CRYPTO ARB BOT")
        log.info(f"  Mode: {'📝 PAPER' if self.paper else '💰 LIVE'}")
        log.info(f"  Symbols: {cfg.SYMBOLS}")
        log.info(f"  Max per trade: ${cfg.MAX_PER_TRADE_USD}")
        log.info(f"  Max exposure: ${cfg.MAX_TOTAL_EXPOSURE_USD}")
        log.info(f"  Max daily loss: ${cfg.MAX_DAILY_LOSS_USD}")
        log.info("=" * 55)

        # Connect to Polymarket
        connected = await self.executor.connect()
        if not connected and not self.paper:
            log.error("Cannot start — Polymarket connection failed")
            return

        await self.discovery.start()

        # Run all tasks
        try:
            await asyncio.gather(
                self.feed.start(),          # Binance price stream
                self._trading_loop(),       # Main decision loop
                self._status_loop(),        # Periodic status updates
                self._order_cleanup_loop(), # Cancel stale orders
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self):
        log.info("Shutting down...")
        self._running = False
        self.feed.stop()
        await self.discovery.stop()
        for oid in list(self._active_orders.keys()):
            await self.executor.cancel_order(oid)
        await self.executor.close()
        log.info(f"Final stats: {self._stats}")
        log.info(f"Risk: {self.risk.status()}")

    async def _trading_loop(self):
        """
        Core loop — runs every second.
        Checks all active rounds for trading opportunities.
        """
        # Wait for price feed to connect
        while self._running and not self.feed.is_connected:
            await asyncio.sleep(0.5)

        log.info("🚀 Trading loop started")

        while self._running:
            try:
                await self._check_opportunities()
            except Exception as e:
                log.error(f"Trading loop error: {e}")

            await asyncio.sleep(1.0)  # check every second

    async def _check_opportunities(self):
        """Scan all active rounds for edge."""
        rounds = await self.discovery.find_active_rounds()

        for rnd in rounds:
            current_price = self.feed.get_price(rnd.symbol)
            if current_price <= 0:
                continue

            # Set start price if this is a new round
            if rnd.condition_id not in self._round_start_prices:
                self._round_start_prices[rnd.condition_id] = current_price
                rnd.start_price = current_price
            else:
                rnd.start_price = self._round_start_prices[rnd.condition_id]

            time_remaining = rnd.end_time - time.time()
            if time_remaining <= 0:
                # Round ended — clean up
                self._round_start_prices.pop(rnd.condition_id, None)
                self._locked_markets.discard(rnd.condition_id)
                continue

            # Get volatility
            vol = self.feed.get_volatility(rnd.symbol)

            # Compute fair value
            fair_yes = self.pricer.fair_value_yes(
                start_price=rnd.start_price,
                current_price=current_price,
                time_remaining_s=time_remaining,
                duration_s=rnd.duration,
                direction=rnd.direction,
                vol=vol,
            )

            # Get market price (from order book)
            book = await self.executor.get_order_book(rnd.token_id_yes)
            if not book or not book.get("asks") or not book.get("bids"):
                continue

            best_ask = float(book["asks"][0]["price"])
            best_bid = float(book["bids"][0]["price"])
            market_yes = (best_bid + best_ask) / 2

            # Find edge
            side, edge = self.pricer.find_edge(fair_yes, market_yes)

            if not side:
                continue

            self._stats["signals"] += 1

            # Compute our maker order price
            our_price = self.pricer.maker_price(fair_yes, side, edge)

            # How many shares?
            shares = cfg.MAX_PER_TRADE_USD / our_price
            usd_amount = our_price * shares

            # Confidence metric
            move = abs(current_price - rnd.start_price) / rnd.start_price
            confidence = min(1.0, edge * 20 + move * 50)

            # Risk check
            allowed, adjusted_usd, reason = self.risk.can_trade(
                usd_amount, time_remaining, confidence
            )

            if not allowed:
                log.debug(f"Risk block: {reason}")
                continue

            # --- Dedup guard ---
            # Only one in-flight order per market per round. Prevents concurrent
            # _check_opportunities calls (or loop re-entries) from doubling up.
            if rnd.condition_id in self._locked_markets:
                log.debug(f"Dedup block: order already in-flight for {rnd.condition_id[:10]}…")
                continue

            # --- Position strategy ---
            # We never hold conditional tokens, only USDC.
            # "sell_yes" = YES overpriced → BUY NO at complement price (economically identical)
            # "sell_no"  = NO overpriced  → BUY YES at complement price
            # BUY orders only require USDC collateral — no token holdings needed.
            if side == "sell_yes":
                order_token_id = rnd.token_id_no
                order_side = "BUY"
                order_price = round(1.0 - our_price, 2)  # complement
            else:  # sell_no
                order_token_id = rnd.token_id_yes
                order_side = "BUY"
                order_price = round(1.0 - our_price, 2)  # complement

            order_price = max(0.01, min(0.99, order_price))
            adjusted_shares = max(5.0, adjusted_usd / order_price)  # min 5 shares (CLOB minimum)

            # Lock this market before awaiting the network call
            async with self._order_lock:
                if rnd.condition_id in self._locked_markets:
                    # Another coroutine snuck in between the check above and the lock
                    log.debug(f"Dedup block (lock): {rnd.condition_id[:10]}…")
                    continue
                self._locked_markets.add(rnd.condition_id)

            # Place the order!
            try:
                order_id = await self.executor.place_maker_order(
                    token_id=order_token_id,
                    price=order_price,
                    size=round(adjusted_shares),
                    side=order_side,
                    market_id=rnd.condition_id,
                )
            finally:
                self._locked_markets.discard(rnd.condition_id)

            if order_id:
                self._stats["orders"] += 1
                self._active_orders[order_id] = {
                    "market_id": rnd.condition_id,
                    "side": side,
                    "price": our_price,
                    "size": adjusted_shares,
                    "placed_at": time.time(),
                }
                self.risk.open_position(rnd.condition_id, adjusted_usd)

                log.info(
                    f"{'📝' if self.paper else '🟢'} "
                    f"{rnd.symbol.upper()} {rnd.direction.upper()} │ "
                    f"{side}→{order_side} {order_token_id[:8]}… @ {order_price*100:.0f}¢ │ "
                    f"edge={edge*100:.1f}¢ │ fair={fair_yes*100:.0f}¢ │ "
                    f"mkt={market_yes*100:.0f}¢ │ "
                    f"${adjusted_usd:.2f}"
                )

    async def _order_cleanup_loop(self):
        """Cancel orders that are too old."""
        while self._running:
            now = time.time()
            stale = [
                oid for oid, info in self._active_orders.items()
                if now - info["placed_at"] > cfg.ORDER_TIMEOUT_SECONDS
            ]
            for oid in stale:
                await self.executor.cancel_order(oid)
                info = self._active_orders.pop(oid, {})
                self.risk.close_position(info.get("market_id", ""), 0)
                log.debug(f"Cancelled stale order: {oid[:12]}")

            await asyncio.sleep(5)

    async def _status_loop(self):
        """Print status every 30 seconds."""
        while self._running:
            await asyncio.sleep(30)
            prices = " │ ".join(
                f"{s.upper()}: ${self.feed.get_price(s):,.2f}"
                for s in cfg.SYMBOLS
                if self.feed.get_price(s) > 0
            )
            log.info(f"📊 {prices} │ {self.risk.status()} │ Orders: {self._stats['orders']}")


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Polymarket Crypto Round Maker Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 bot.py --paper     Test with fake trades (recommended first)
  python3 bot.py --live      Real money trading

Setup:
  See SETUP_GUIDE.txt for step-by-step instructions.
        """,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--paper", action="store_true", help="Paper trading (no real money)")
    mode.add_argument("--live", action="store_true", help="Live trading (real money!)")
    args = parser.parse_args()

    paper_mode = args.paper
    bot = Bot(paper_mode=paper_mode)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Handle Ctrl+C gracefully
    def handle_shutdown(sig, frame):
        log.info("\n⏹  Stopping bot (Ctrl+C)...")
        loop.create_task(bot.shutdown())
        # Give it 3 seconds to clean up, then force exit
        loop.call_later(3, sys.exit, 0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        loop.run_until_complete(bot.start())
    except KeyboardInterrupt:
        loop.run_until_complete(bot.shutdown())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
