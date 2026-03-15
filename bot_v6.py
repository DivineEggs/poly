#!/usr/bin/env python3
"""
Polymarket Bot v6 — True Market Maker
=======================================
Posts limit BUY orders near fair value in empty order books.
We ARE the liquidity. Buy at fair-2¢, sell at fair+2¢.

The book is 1¢ bid / 99¢ ask (empty).
We post a bid at 53¢ when fair value is 55¢.
Someone sells to us. We post a sell at 57¢.
Someone buys from us. Profit: 4¢/share.

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
SPREAD_CAPTURE = 0.02       # post bid 2¢ below fair, ask 2¢ above
STOP_LOSS = 0.04            # exit if mid moves 4¢ against us
EMERGENCY_EXIT_SECONDS = 60
MIN_PRICE = 0.10
MAX_PRICE = 0.90
MIN_SHARES = 5
NO_ENTRY_BUFFER = 120
BUY_FILL_TIMEOUT = 45       # wait up to 45s for our bid to fill
SELL_FILL_TIMEOUT = 60       # wait up to 60s for our ask to fill

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
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
    with open("bot_v6.log", "a") as f:
        f.write(line + "\n")


# ═══════════════════════════════════════════════════════════
# BINANCE
# ═══════════════════════════════════════════════════════════

def get_binance_momentum(symbol):
    try:
        r = requests.get(BINANCE_KLINES, params={
            "symbol": symbol.upper(), "interval": "1m", "limit": 5
        }, timeout=5)
        candles = r.json()
        if len(candles) < 5:
            return 0, 0, 0
        current = float(candles[-1][4])
        price_1m = float(candles[-2][4])
        price_5m = float(candles[0][1])
        return (
            (current - price_1m) / price_1m,
            (current - price_5m) / price_5m,
            current,
        )
    except Exception:
        return 0, 0, 0


# ═══════════════════════════════════════════════════════════
# FAIR VALUE FROM BINANCE MOMENTUM
# ═══════════════════════════════════════════════════════════

def estimate_fair_value(change_1m, change_5m):
    """
    Estimate fair Up/Down probabilities from Binance momentum.
    
    No PriceToBeat needed. We just look at recent momentum:
    - BTC dropping → Down more likely → Down fair value > 50¢
    - BTC rising → Up more likely → Up fair value > 50¢
    
    Scale: 0.1% move in 1min → ~10¢ shift from 50/50
    """
    # Weight 1m change more heavily (more recent)
    combined = change_1m * 0.7 + change_5m * 0.3
    
    # Convert to probability shift
    # 0.1% move → ~10% probability shift
    shift = combined * 100  # 0.001 → 0.1 shift
    shift = max(-0.40, min(0.40, shift))  # cap at 40¢ from midpoint
    
    fair_up = 0.50 + shift
    fair_down = 1.0 - fair_up
    
    return round(fair_up, 3), round(fair_down, 3)


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
            tokens_raw = market.get("clobTokenIds", "[]")
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
            if len(tokens) < 2:
                continue

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
        if not bids and not asks:
            return None
        return {
            "bid": float(bids[0].price) if bids else 0.01,
            "ask": float(asks[0].price) if asks else 0.99,
            "bid_count": len(bids),
            "ask_count": len(asks),
        }
    except Exception:
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
# TRADE LIFECYCLE — TRUE MARKET MAKING
# ═══════════════════════════════════════════════════════════

def execute_trade(client, token_id, bid_price, ask_price, shares, round_end_time, paper=False):
    """
    1. Post BUY bid at bid_price (sit on book, wait for fill)
    2. If filled → post SELL ask at ask_price
    3. If sell fills → profit
    4. If stop loss or time runs out → emergency exit
    """
    actual_cost = bid_price * shares

    # STEP 1: POST BID
    log(f"   POSTING BID: {shares} @ {bid_price*100:.0f}¢ = ${actual_cost:.2f}")

    if paper:
        log(f"   [PAPER] Bid posted")
        buy_id = "paper_buy"
    else:
        buy_id = place_buy(client, token_id, bid_price, shares)
        if not buy_id:
            log(f"   ❌ Bid failed")
            return 0

    # STEP 2: WAIT FOR BID TO FILL
    log(f"   Waiting for fill (max {BUY_FILL_TIMEOUT}s)...")
    bid_filled = False
    wait_start = time.time()

    while time.time() - wait_start < BUY_FILL_TIMEOUT:
        if os.path.exists(os.path.expanduser("~/polymarket-bot/STOP")):
            if not paper:
                cancel_order(client, buy_id)
            return 0

        time_left = round_end_time - time.time()
        if time_left < EMERGENCY_EXIT_SECONDS + 30:
            log(f"   Running out of time — cancelling bid")
            if not paper:
                cancel_order(client, buy_id)
            return 0

        if paper:
            import random
            time.sleep(3)
            if random.random() < 0.4:
                bid_filled = True
                break
        else:
            if check_filled(client, buy_id):
                bid_filled = True
                break
            time.sleep(2)

    if not bid_filled:
        log(f"   Bid not filled — cancelling")
        if not paper:
            cancel_order(client, buy_id)
        return 0

    log(f"   ✅ BID FILLED! Waiting 5s for settlement...")
    time.sleep(5)
    log(f"   Posting ask @ {ask_price*100:.0f}¢")

    # STEP 3: POST ASK (SELL)
    if paper:
        sell_id = "paper_sell"
    else:
        sell_id = place_sell(client, token_id, ask_price, shares)
        if not sell_id:
            log(f"   ❌ Ask failed — emergency exit")
            book = get_book(client, token_id)
            if book and book["bid"] > 0.01:
                place_sell(client, token_id, book["bid"], shares)
            return -actual_cost * 0.03

    # STEP 4: MONITOR SELL
    log(f"   Ask posted. Waiting for take...")

    while True:
        now = time.time()
        time_left = round_end_time - now

        # Kill switch
        if os.path.exists(os.path.expanduser("~/polymarket-bot/STOP")):
            log(f"   🛑 KILL SWITCH")
            if not paper:
                cancel_order(client, sell_id)
                book = get_book(client, token_id)
                if book and book["bid"] > 0.01:
                    place_sell(client, token_id, book["bid"], shares)
            return -actual_cost * 0.03

        # Sell filled = PROFIT
        if not paper:
            if check_filled(client, sell_id):
                profit = (ask_price - bid_price) * shares
                log(f"   💰 SPREAD CAPTURED: +${profit:.2f} ({(ask_price-bid_price)*100:.0f}¢/share)")
                return profit
        else:
            import random
            time.sleep(3)
            roll = random.random()
            if roll < 0.5:
                profit = (ask_price - bid_price) * shares
                log(f"   [PAPER] 💰 +${profit:.2f}")
                return profit
            elif roll < 0.7:
                loss = STOP_LOSS * shares
                log(f"   [PAPER] 🔴 -${loss:.2f}")
                return -loss
            else:
                log(f"   [PAPER] ⏰ Time exit")
                return -actual_cost * 0.02

        # Emergency exit
        if time_left < EMERGENCY_EXIT_SECONDS:
            log(f"   ⏰ Time — selling at bid")
            if not paper:
                cancel_order(client, sell_id)
                book = get_book(client, token_id)
                if book and book["bid"] > 0.01:
                    place_sell(client, token_id, book["bid"], shares)
                    # Estimate loss: bought at bid_price, selling at current bid
                    return (book["bid"] - bid_price) * shares
            return -actual_cost * 0.02

        # Stop loss: check midpoint
        if not paper:
            book = get_book(client, token_id)
            if book:
                mid = (book["bid"] + book["ask"]) / 2
                if mid < bid_price - STOP_LOSS:
                    log(f"   🔴 STOP LOSS — mid={mid*100:.0f}¢ vs entry={bid_price*100:.0f}¢")
                    cancel_order(client, sell_id)
                    if book["bid"] > 0.01:
                        place_sell(client, token_id, book["bid"], shares)
                    return (book["bid"] - bid_price) * shares

        time.sleep(3)


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    paper = "--paper" in sys.argv
    live = "--live" in sys.argv

    if not paper and not live:
        print("Usage: python3 bot_v6.py --paper  OR  --live")
        sys.exit(1)

    log("=" * 55)
    log(f"  BOT v6 — TRUE MARKET MAKER")
    log(f"  Mode: {'PAPER' if paper else 'LIVE'}")
    log(f"  Spread: {SPREAD_CAPTURE*100:.0f}¢ each side")
    log(f"  Stop loss: -{STOP_LOSS*100:.0f}¢")
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

            # 1. Momentum
            change_1m, change_5m, price = get_binance_momentum(symbol)
            if price <= 0:
                continue

            # Need some directional signal
            if abs(change_1m) < 0.0002:
                continue

            # 2. Find round
            rnd = get_active_round(symbol)
            if not rnd:
                continue

            if rnd["condition_id"] in traded_rounds:
                continue

            time_left = rnd["end_time"] - time.time()
            if time_left < NO_ENTRY_BUFFER:
                continue

            # 3. Compute fair value from momentum
            fair_up, fair_down = estimate_fair_value(change_1m, change_5m)

            # 4. Decide which side to make market on
            # We want to buy the side that Binance says is undervalued
            if change_1m < 0:
                # BTC dropping → buy DOWN
                token_id = rnd["token_down"]
                fair = fair_down
                side_name = "DOWN"
            else:
                # BTC rising → buy UP
                token_id = rnd["token_up"]
                fair = fair_up
                side_name = "UP"

            # 5. Set our bid/ask prices
            # Bid below fair, ask above fair
            bid_price = round(fair - SPREAD_CAPTURE, 2)
            ask_price = round(fair + SPREAD_CAPTURE, 2)

            # Price checks
            if bid_price < MIN_PRICE or bid_price > MAX_PRICE:
                continue
            if ask_price < MIN_PRICE or ask_price > MAX_PRICE:
                continue

            # Cost check
            actual_cost = bid_price * MIN_SHARES
            if actual_cost > MAX_TRADE_USD:
                continue

            # 6. Check actual book to make sure we're competitive
            if live and client:
                book = get_book(client, token_id)
                if book:
                    # If someone else is already bidding higher, skip
                    if book["bid"] >= bid_price:
                        log(f"   Skip {symbol}: existing bid {book['bid']*100:.0f}¢ >= our {bid_price*100:.0f}¢")
                        continue

            # 7. TRADE
            log(f"🎯 {symbol.upper()} │ MAKE {side_name} │ 1m={change_1m*100:+.3f}% │ "
                f"fair={fair*100:.0f}¢ │ bid={bid_price*100:.0f}¢ ask={ask_price*100:.0f}¢ │ "
                f"{time_left:.0f}s left")

            traded_rounds.add(rnd["condition_id"])
            traded_this_cycle = True

            pnl = execute_trade(
                client=client,
                token_id=token_id,
                bid_price=bid_price,
                ask_price=ask_price,
                shares=MIN_SHARES,
                round_end_time=rnd["end_time"],
                paper=paper,
            )

            daily_pnl += pnl
            trades_today += 1
            log(f"   Result: ${pnl:+.2f} │ Daily: ${daily_pnl:+.2f} │ Trades: {trades_today}")

        # Status
        if int(time.time()) % 60 < 6:
            m1_btc, m5_btc, p_btc = get_binance_momentum("btcusdt")
            m1_eth, m5_eth, p_eth = get_binance_momentum("ethusdt")
            f_up_btc, f_down_btc = estimate_fair_value(m1_btc, m5_btc)
            f_up_eth, f_down_eth = estimate_fair_value(m1_eth, m5_eth)
            log(f"📊 BTC: ${p_btc:,.0f} ({m1_btc*100:+.2f}%) fair:U{f_up_btc*100:.0f}/D{f_down_btc*100:.0f} │ "
                f"ETH: ${p_eth:,.0f} ({m1_eth*100:+.2f}%) fair:U{f_up_eth*100:.0f}/D{f_down_eth*100:.0f} │ "
                f"Trades: {trades_today} │ PnL: ${daily_pnl:+.2f}")

        time.sleep(5)


if __name__ == "__main__":
    main()
