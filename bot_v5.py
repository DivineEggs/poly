#!/usr/bin/env python3
"""
Polymarket Bot v5 — Maker Strategy (No PriceToBeat needed)
============================================================
Instead of computing fair value from a start price, we:
1. Track BTC/ETH price movement on Binance over last 30-60 seconds
2. Compare direction/momentum to Polymarket's current book prices
3. If book is stale (hasn't caught up to Binance move) → trade the mispricing
4. Buy underpriced side, immediately post sell at +2¢, manage exit

No async. Synchronous. Cannot duplicate.
"""

import json
import math
import os
import sys
import time
import requests
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

MAX_TRADE_USD = 3.0
MAX_DAILY_LOSS = 10.0
TAKE_PROFIT = 0.02
STOP_LOSS = 0.03
EMERGENCY_EXIT_SECONDS = 60
MIN_PRICE = 0.05
MAX_PRICE = 0.45
MIN_SHARES = 5
MIN_EDGE = 0.02
NO_ENTRY_BUFFER = 120

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

SLUG_PREFIXES = {
    "btcusdt": "btc-updown-5m",
    "ethusdt": "eth-updown-5m",
}

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

def log(msg):
    line = f"{time.strftime('%H:%M:%S')} │ {msg}"
    print(line)
    with open("bot_v5.log", "a") as f:
        f.write(line + "\n")


# ═══════════════════════════════════════════════════════════
# BINANCE
# ═══════════════════════════════════════════════════════════

def get_binance_price(symbol):
    try:
        r = requests.get(BINANCE_API, params={"symbol": symbol.upper()}, timeout=3)
        return float(r.json()["price"])
    except Exception:
        return 0.0


def get_binance_momentum(symbol):
    """
    Get price change over last 1-minute and 5-minute candles.
    Returns (change_1m_pct, change_5m_pct, current_price)
    """
    try:
        # Last 5 one-minute candles
        r = requests.get(BINANCE_KLINES, params={
            "symbol": symbol.upper(), "interval": "1m", "limit": 5
        }, timeout=5)
        candles = r.json()
        if len(candles) < 5:
            return 0, 0, 0

        current = float(candles[-1][4])  # latest close
        price_1m_ago = float(candles[-2][4])  # 1 min ago close
        price_5m_ago = float(candles[0][1])   # 5 min ago open

        change_1m = (current - price_1m_ago) / price_1m_ago
        change_5m = (current - price_5m_ago) / price_5m_ago

        return change_1m, change_5m, current
    except Exception:
        return 0, 0, 0


# ═══════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════

def get_active_round(symbol):
    prefix = SLUG_PREFIXES.get(symbol)
    if not prefix:
        return None

    now = time.time()
    round_start = int(now - (now % 300))

    for ts in [round_start, round_start - 300]:
        slug = f"{prefix}-{ts}"
        try:
            r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=5)
            if r.status_code != 200:
                continue
            data = r.json()
            if not data:
                continue

            event = data[0]
            markets = event.get("markets", [])
            if not markets:
                continue

            market = markets[0]
            tokens_raw = market.get("clobTokenIds", "[]"); tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            if len(tokens) < 2:
                continue

            # Get current book prices from API
            outcome_prices_raw = market.get("outcomePrices", "[]"); outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
            up_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.5
            down_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.5

            end_time = ts + 300
            try:
                from datetime import datetime
                end_time = datetime.fromisoformat(
                    market["endDate"].replace("Z", "+00:00")
                ).timestamp()
            except Exception:
                pass

            if time.time() > end_time:
                continue

            return {
                "condition_id": market.get("conditionId", ""),
                "token_up": tokens[0],
                "token_down": tokens[1],
                "up_price": up_price,
                "down_price": down_price,
                "best_bid": float(market.get("bestBid", 0)),
                "best_ask": float(market.get("bestAsk", 0)),
                "end_time": end_time,
                "slug": slug,
                "symbol": symbol,
            }
        except Exception:
            continue
    return None


# ═══════════════════════════════════════════════════════════
# ORDER BOOK
# ═══════════════════════════════════════════════════════════

def get_book(client, token_id):
    try:
        book = client.get_order_book(token_id)
        bids = book.bids if hasattr(book, "bids") else []
        asks = book.asks if hasattr(book, "asks") else []
        if not bids or not asks:
            return None
        return {
            "bid": float(bids[0].price),
            "ask": float(asks[0].price),
        }
    except Exception:
        return None


def get_midpoint(client, token_id):
    try:
        book = get_book(client, token_id)
        if book:
            return (book["bid"] + book["ask"]) / 2
        return 0
    except Exception:
        return 0


# ═══════════════════════════════════════════════════════════
# EDGE DETECTION — THE KEY LOGIC
# ═══════════════════════════════════════════════════════════

def find_edge(change_1m, change_5m, up_book, down_book):
    """
    Compare Binance momentum to Polymarket book.

    If BTC is dropping on Binance:
      - DOWN should be more expensive (higher probability)
      - If DOWN ask is still cheap → buy DOWN (market is stale)

    If BTC is rising on Binance:
      - UP should be more expensive
      - If UP ask is still cheap → buy UP (market is stale)

    Returns: {side, token_key, price, edge} or None
    """
    if not up_book or not down_book:
        return None

    # Need a meaningful move (at least 0.05% in 1 min)
    if abs(change_1m) < 0.0002:
        return None

    # BTC is dropping → DOWN should be worth more
    if change_1m < -0.0002:
        # DOWN is underpriced if its ask is below what momentum suggests
        # Simple model: a 0.1% drop in 1 min should move DOWN price by ~5-10¢
        expected_down_premium = abs(change_1m) * 50  # scale factor
        fair_down = 0.50 + expected_down_premium
        edge = fair_down - down_book["ask"]

        if edge > MIN_EDGE and down_book["ask"] >= MIN_PRICE and down_book["ask"] <= MAX_PRICE:
            maker_price = round(down_book["ask"] - 0.01, 2)
            maker_price = max(MIN_PRICE, maker_price)
            return {
                "side": "BUY DOWN",
                "token_key": "token_down",
                "price": maker_price,
                "edge": edge,
                "fair": fair_down,
                "market_ask": down_book["ask"],
            }

    # BTC is rising → UP should be worth more
    if change_1m > 0.0002:
        expected_up_premium = abs(change_1m) * 50
        fair_up = 0.50 + expected_up_premium
        edge = fair_up - up_book["ask"]

        if edge > MIN_EDGE and up_book["ask"] >= MIN_PRICE and up_book["ask"] <= MAX_PRICE:
            maker_price = round(up_book["ask"] - 0.01, 2)
            maker_price = max(MIN_PRICE, maker_price)
            return {
                "side": "BUY UP",
                "token_key": "token_up",
                "price": maker_price,
                "edge": edge,
                "fair": fair_up,
                "market_ask": up_book["ask"],
            }

    return None


# ═══════════════════════════════════════════════════════════
# ORDER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def place_buy(client, token_id, price, size):
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        order = OrderArgs(token_id=token_id, price=price, size=size, side=BUY)
        signed = client.create_order(order)
        result = client.post_order(signed, OrderType.GTC)
        if result and result.get("orderID"):
            return result["orderID"]
        return None
    except Exception as e:
        log(f"   BUY error: {e}")
        return None


def place_sell(client, token_id, price, size):
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL
        order = OrderArgs(token_id=token_id, price=price, size=size, side=SELL)
        signed = client.create_order(order)
        result = client.post_order(signed, OrderType.GTC)
        if result and result.get("orderID"):
            return result["orderID"]
        return None
    except Exception as e:
        log(f"   SELL error: {e}")
        return None


def cancel_order(client, order_id):
    try:
        client.cancel(order_id=order_id)
    except Exception:
        pass


def check_filled(client, order_id):
    try:
        order = client.get_order(order_id)
        if order:
            matched = float(order.get("size_matched", 0))
            total = float(order.get("original_size", order.get("size", 0)))
            if total > 0 and matched >= total * 0.9:
                return True
        return False
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════
# TRADE LIFECYCLE
# ═══════════════════════════════════════════════════════════

def execute_trade(client, token_id, entry_price, shares, round_end_time, paper=False):
    actual_cost = entry_price * shares

    # STEP 1: BUY
    log(f"   BUYING {shares} @ {entry_price*100:.0f}¢ = ${actual_cost:.2f}")

    if paper:
        log(f"   [PAPER] Buy filled")
    else:
        buy_id = place_buy(client, token_id, entry_price, shares)
        if not buy_id:
            log(f"   ❌ Buy failed")
            return 0

        filled = False
        for _ in range(15):
            if check_filled(client, buy_id):
                filled = True
                break
            time.sleep(1)

        if not filled:
            log(f"   Buy not filled — cancelling")
            cancel_order(client, buy_id)
            return 0

    log(f"   ✅ Bought. Posting sell...")

    # STEP 2: SELL at take profit
    sell_price = round(min(0.99, entry_price + TAKE_PROFIT), 2)

    if not paper:
        sell_id = place_sell(client, token_id, sell_price, shares)
        if not sell_id:
            log(f"   ❌ Sell failed — emergency exit")
            book = get_book(client, token_id)
            if book:
                place_sell(client, token_id, book["bid"], shares)
            return -actual_cost * 0.03

    log(f"   Sell @ {sell_price*100:.0f}¢ (+{TAKE_PROFIT*100:.0f}¢). Monitoring...")

    # STEP 3: MONITOR
    while True:
        now = time.time()
        time_left = round_end_time - now

        if os.path.exists(os.path.expanduser("~/polymarket-bot/STOP")):
            log(f"   🛑 KILL SWITCH")
            if not paper:
                cancel_order(client, sell_id)
                book = get_book(client, token_id)
                if book:
                    place_sell(client, token_id, book["bid"], shares)
            return -actual_cost * 0.03

        # Sell filled = PROFIT
        if not paper:
            if check_filled(client, sell_id):
                profit = TAKE_PROFIT * shares
                log(f"   💰 PROFIT: +${profit:.2f}")
                return profit

        # Emergency exit
        if time_left < EMERGENCY_EXIT_SECONDS:
            log(f"   ⏰ Time exit — {time_left:.0f}s left")
            if not paper:
                cancel_order(client, sell_id)
                book = get_book(client, token_id)
                if book:
                    place_sell(client, token_id, book["bid"], shares)
            return -actual_cost * 0.02

        # Stop loss
        if not paper:
            mid = get_midpoint(client, token_id)
            if mid > 0 and mid < entry_price - STOP_LOSS:
                log(f"   🔴 STOP LOSS — mid={mid*100:.0f}¢")
                cancel_order(client, sell_id)
                book = get_book(client, token_id)
                if book:
                    place_sell(client, token_id, book["bid"], shares)
                return -(entry_price - mid + 0.01) * shares

        # Paper simulation
        if paper:
            import random
            time.sleep(3)
            roll = random.random()
            if roll < 0.55:
                profit = TAKE_PROFIT * shares
                log(f"   [PAPER] 💰 TP: +${profit:.2f}")
                return profit
            elif roll < 0.75:
                loss = STOP_LOSS * shares
                log(f"   [PAPER] 🔴 SL: -${loss:.2f}")
                return -loss
            else:
                log(f"   [PAPER] ⏰ Time exit")
                return -actual_cost * 0.02

        time.sleep(2)


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    paper = "--paper" in sys.argv
    live = "--live" in sys.argv

    if not paper and not live:
        print("Usage: python3 bot_v5.py --paper  OR  --live")
        sys.exit(1)

    log("=" * 55)
    log(f"  BOT v5 — MAKER + MOMENTUM")
    log(f"  Mode: {'PAPER' if paper else 'LIVE'}")
    log(f"  Take profit: +{TAKE_PROFIT*100:.0f}¢  Stop loss: -{STOP_LOSS*100:.0f}¢")
    log(f"  Max trade: ${MAX_TRADE_USD}  Max daily loss: ${MAX_DAILY_LOSS}")
    log("=" * 55)

    client = None
    if live:
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            creds = ApiCreds(
                api_key=os.environ["POLY_API_KEY"],
                api_secret=os.environ["POLY_API_SECRET"],
                api_passphrase=os.environ["POLY_PASSPHRASE"],
            )
            funder = os.environ.get("POLY_FUNDER", "")
            pk = os.environ["POLY_PRIVATE_KEY"]

            if funder:
                client = ClobClient(
                    CLOB_API, key=pk, chain_id=137,
                    creds=creds, signature_type=1, funder=funder,
                )
            else:
                client = ClobClient(CLOB_API, key=pk, chain_id=137, creds=creds)

            log("✅ Polymarket connected")
        except Exception as e:
            log(f"❌ Failed: {e}")
            sys.exit(1)

    daily_pnl = 0.0
    traded_rounds = set()
    trades_today = 0

    while True:
        if os.path.exists(os.path.expanduser("~/polymarket-bot/STOP")):
            log("🛑 STOP")
            break

        if daily_pnl <= -MAX_DAILY_LOSS:
            log(f"🛑 Daily loss: ${daily_pnl:.2f}")
            break

        traded_this_cycle = False

        for symbol in ["btcusdt", "ethusdt"]:
            if traded_this_cycle:
                break

            # 1. Get Binance momentum
            change_1m, change_5m, btc_price = get_binance_momentum(symbol)
            if btc_price <= 0:
                continue

            # 2. Find active round
            rnd = get_active_round(symbol)
            if not rnd:
                continue

            if rnd["condition_id"] in traded_rounds:
                continue

            time_left = rnd["end_time"] - time.time()
            if time_left < NO_ENTRY_BUFFER:
                continue

            # 3. Get real order books
            if live and client:
                up_book = get_book(client, rnd["token_up"])
                down_book = get_book(client, rnd["token_down"])
            else:
                up_book = {"bid": rnd["up_price"] - 0.02, "ask": rnd["up_price"] + 0.02}
                down_book = {"bid": rnd["down_price"] - 0.02, "ask": rnd["down_price"] + 0.02}

            # 4. Find edge
            trade = find_edge(change_1m, change_5m, up_book, down_book)
            if not trade:
                if abs(change_1m) >= 0.0002:
                    log(f'   DEBUG {symbol}: momentum={change_1m*100:.3f}% but no edge. up_ask={up_book["ask"] if up_book else "none"} down_ask={down_book["ask"] if down_book else "none"}')
                continue

            # 5. Cost check
            actual_cost = trade["price"] * MIN_SHARES
            if actual_cost > MAX_TRADE_USD:
                continue

            # 6. TRADE
            token_id = rnd[trade["token_key"]]
            log(f"🎯 {symbol.upper()} │ {trade['side']} │ 1m={change_1m*100:+.2f}% │ "
                f"edge={trade['edge']*100:.1f}¢ │ ask={trade['market_ask']*100:.0f}¢ │ "
                f"{time_left:.0f}s left")

            traded_rounds.add(rnd["condition_id"])
            traded_this_cycle = True

            pnl = execute_trade(
                client=client,
                token_id=token_id,
                entry_price=trade["price"],
                shares=MIN_SHARES,
                round_end_time=rnd["end_time"],
                paper=paper,
            )

            daily_pnl += pnl
            trades_today += 1
            log(f"   Result: ${pnl:+.2f} │ Daily: ${daily_pnl:+.2f} │ Trades: {trades_today}")

        # Status
        if int(time.time()) % 60 < 6:
            m1_btc, _, p_btc = get_binance_momentum("btcusdt")
            m1_eth, _, p_eth = get_binance_momentum("ethusdt")
            log(f"📊 BTC: ${p_btc:,.0f} ({m1_btc*100:+.2f}%) │ ETH: ${p_eth:,.0f} ({m1_eth*100:+.2f}%) │ "
                f"Trades: {trades_today} │ PnL: ${daily_pnl:+.2f}")

        time.sleep(5)


if __name__ == "__main__":
    main()
