#!/usr/bin/env python3
"""
Polymarket Bot v7 — FINAL TRUE MAKER
======================================
Confirmed working: Buy ✅ Sell ✅ MetaMask proxy ✅

Strategy:
1. Watch Binance for BTC/ETH momentum
2. Post BUY bid in empty Polymarket book at fair value - 2¢
3. Wait for fill (someone sells to us)
4. Immediately post SELL at entry + 4¢ (2¢ spread each side)
5. Wait for fill → profit captured
6. Stop loss / emergency exit if needed
7. Never hold to resolution

Buy 6 shares, sell 5 (fee adjustment).
Synchronous. Single loop. Cannot duplicate.
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

MAX_TRADE_USD = 3.50            # 6 shares at ~55¢ max
MAX_DAILY_LOSS = 5.0            # conservative with $7 balance
TAKE_PROFIT = 0.04              # sell at entry + 4¢ (2¢ spread each side)
STOP_LOSS = 0.04                # exit if mid drops 4¢ from entry
EMERGENCY_EXIT_SECONDS = 60     # force exit 60s before round end
MIN_PRICE = 0.10                # don't bid below 10¢
MAX_PRICE = 0.55                # don't bid above 55¢ (6 * 0.55 = $3.30)
BUY_SHARES = 6                  # buy 6 (fees take ~0.5-1)
SELL_SHARES = 5                 # sell 5 (minimum order size)
MIN_MOMENTUM = 0.0002           # 0.02% minimum 1-minute move
NO_ENTRY_BUFFER = 120           # don't enter last 2 minutes
BUY_FILL_TIMEOUT = 45           # wait 45s for bid to fill
ROUND_DURATION = 300

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

SLUG_PREFIXES = {
    "btcusdt": "btc-updown-5m",
    "ethusdt": "eth-updown-5m",
}

STOP_FILE = os.path.expanduser("~/polymarket-bot/STOP")

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

def log(msg):
    line = f"{time.strftime('%H:%M:%S')} | {msg}"
    print(line)
    with open("bot_v7.log", "a") as f:
        f.write(line + "\n")


# ═══════════════════════════════════════════════════════════
# BINANCE
# ═══════════════════════════════════════════════════════════

def get_momentum(symbol):
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
# FAIR VALUE
# ═══════════════════════════════════════════════════════════

def estimate_fair(change_1m, change_5m):
    combined = change_1m * 0.7 + change_5m * 0.3
    shift = combined * 100
    shift = max(-0.40, min(0.40, shift))
    fair_up = 0.50 + shift
    fair_down = 1.0 - fair_up
    return round(fair_up, 3), round(fair_down, 3)


# ═══════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════

def get_round(symbol):
    prefix = SLUG_PREFIXES.get(symbol)
    if not prefix:
        return None

    now = time.time()
    ts = int(now - (now % 300))

    for offset in [0, -300]:
        slug = f"{prefix}-{ts + offset}"
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

            end_time = ts + offset + 300
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
                "cid": market.get("conditionId", ""),
                "tok_up": tokens[0],
                "tok_down": tokens[1],
                "end": end_time,
                "slug": slug,
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
        return {
            "bid": float(bids[0].price) if bids else 0.01,
            "ask": float(asks[0].price) if asks else 0.99,
        }
    except Exception:
        return None


def get_mid(client, token_id):
    book = get_book(client, token_id)
    if book:
        return (book["bid"] + book["ask"]) / 2
    return 0


# ═══════════════════════════════════════════════════════════
# ORDERS
# ═══════════════════════════════════════════════════════════

def place_buy(client, token_id, price, size):
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        order = OrderArgs(token_id=token_id, price=round(float(price), 2), size=float(size), side=BUY)
        signed = client.create_order(order)
        result = client.post_order(signed, OrderType.GTC)
        if result and result.get("orderID"):
            return result["orderID"]
        return None
    except Exception as e:
        log(f"  BUY err: {e}")
        return None


def place_sell(client, token_id, price, size):
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL
        order = OrderArgs(token_id=token_id, price=round(float(price), 2), size=float(size), side=SELL)
        signed = client.create_order(order)
        result = client.post_order(signed, OrderType.GTC)
        if result and result.get("orderID"):
            return result["orderID"]
        return None
    except Exception as e:
        log(f"  SELL err: {e}")
        return None


def cancel(client, order_id):
    try:
        client.cancel(order_id=order_id)
    except Exception:
        pass


def is_filled(client, order_id):
    try:
        info = client.get_order(order_id)
        if info:
            matched = float(info.get("size_matched", 0))
            total = float(info.get("original_size", info.get("size", 0)))
            if total > 0 and matched >= total * 0.9:
                return True
        return False
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════
# TRADE LIFECYCLE
# ═══════════════════════════════════════════════════════════

def do_trade(client, token_id, bid_price, ask_price, round_end):
    cost = bid_price * BUY_SHARES

    # STEP 1: POST BID
    log(f"  BID: {BUY_SHARES} @ {bid_price*100:.0f}c = ${cost:.2f}")
    buy_id = place_buy(client, token_id, bid_price, BUY_SHARES)
    if not buy_id:
        log(f"  BID FAILED")
        return 0

    # STEP 2: WAIT FOR FILL
    log(f"  Waiting for fill...")
    filled = False
    start = time.time()

    while time.time() - start < BUY_FILL_TIMEOUT:
        if os.path.exists(STOP_FILE):
            cancel(client, buy_id)
            return 0

        if round_end - time.time() < EMERGENCY_EXIT_SECONDS + 30:
            log(f"  Time running out, cancelling bid")
            cancel(client, buy_id)
            return 0

        if is_filled(client, buy_id):
            filled = True
            break

        time.sleep(2)

    if not filled:
        log(f"  No fill in {BUY_FILL_TIMEOUT}s, cancelling")
        cancel(client, buy_id)
        return 0

    # STEP 3: SETTLEMENT WAIT
    log(f"  FILLED! Waiting 5s for settlement...")
    time.sleep(5)

    # STEP 4: POST ASK
    log(f"  ASK: {SELL_SHARES} @ {ask_price*100:.0f}c")
    sell_id = place_sell(client, token_id, ask_price, SELL_SHARES)
    if not sell_id:
        log(f"  ASK FAILED - emergency sell")
        book = get_book(client, token_id)
        if book and book["bid"] > 0.01:
            place_sell(client, token_id, book["bid"], SELL_SHARES)
        return -cost * 0.05

    # STEP 5: MONITOR
    log(f"  Monitoring...")
    while True:
        # Kill switch
        if os.path.exists(STOP_FILE):
            log(f"  KILL SWITCH")
            cancel(client, sell_id)
            book = get_book(client, token_id)
            if book and book["bid"] > 0.01:
                place_sell(client, token_id, book["bid"], SELL_SHARES)
            return -cost * 0.03

        # Sell filled = PROFIT
        if is_filled(client, sell_id):
            profit = (ask_price - bid_price) * SELL_SHARES
            log(f"  PROFIT: +${profit:.2f} ({(ask_price-bid_price)*100:.0f}c/share)")
            return profit

        # Emergency exit
        time_left = round_end - time.time()
        if time_left < EMERGENCY_EXIT_SECONDS:
            log(f"  TIME EXIT ({time_left:.0f}s left)")
            cancel(client, sell_id)
            book = get_book(client, token_id)
            if book and book["bid"] > 0.01:
                place_sell(client, token_id, book["bid"], SELL_SHARES)
                return (book["bid"] - bid_price) * SELL_SHARES
            return -cost * 0.05

        # Stop loss
        book = get_book(client, token_id)
        if book:
            mid = (book["bid"] + book["ask"]) / 2
            if mid < bid_price - STOP_LOSS:
                log(f"  STOP LOSS (mid={mid*100:.0f}c vs entry={bid_price*100:.0f}c)")
                cancel(client, sell_id)
                if book["bid"] > 0.01:
                    place_sell(client, token_id, book["bid"], SELL_SHARES)
                return (book["bid"] - bid_price) * SELL_SHARES

        time.sleep(3)


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    paper = "--paper" in sys.argv
    live = "--live" in sys.argv

    if not paper and not live:
        print("Usage: python3 bot_v7.py --paper OR --live")
        sys.exit(1)

    log("=" * 55)
    log(f"  BOT v7 - FINAL MAKER")
    log(f"  Mode: {'PAPER' if paper else 'LIVE'}")
    log(f"  Buy {BUY_SHARES} sell {SELL_SHARES} shares")
    log(f"  Take profit: +{TAKE_PROFIT*100:.0f}c | Stop loss: -{STOP_LOSS*100:.0f}c")
    log(f"  Max trade: ${MAX_TRADE_USD} | Max daily loss: ${MAX_DAILY_LOSS}")
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

            client = ClobClient(
                CLOB_API, key=pk, chain_id=137,
                creds=creds, signature_type=2, funder=funder,
            )
            log("Polymarket connected")
        except Exception as e:
            log(f"Connection failed: {e}")
            sys.exit(1)

    daily_pnl = 0.0
    traded_rounds = set()
    trades = 0

    while True:
        if os.path.exists(STOP_FILE):
            log("STOP FILE - exiting")
            break

        if daily_pnl <= -MAX_DAILY_LOSS:
            log(f"DAILY LOSS LIMIT: ${daily_pnl:.2f}")
            break

        traded_this_cycle = False

        for symbol in ["btcusdt", "ethusdt"]:
            if traded_this_cycle:
                break

            # 1. Momentum
            m1, m5, price = get_momentum(symbol)
            if price <= 0 or abs(m1) < MIN_MOMENTUM:
                continue

            # 2. Round
            rnd = get_round(symbol)
            if not rnd:
                continue

            if rnd["cid"] in traded_rounds:
                continue

            time_left = rnd["end"] - time.time()
            if time_left < NO_ENTRY_BUFFER:
                continue

            # 3. Fair value
            fair_up, fair_down = estimate_fair(m1, m5)

            # 4. Pick side
            if m1 < 0:
                token_id = rnd["tok_down"]
                fair = fair_down
                side = "DOWN"
            else:
                token_id = rnd["tok_up"]
                fair = fair_up
                side = "UP"

            # 5. Prices
            bid_price = round(fair - 0.02, 2)
            ask_price = round(fair + 0.02, 2)

            if bid_price < MIN_PRICE or bid_price > MAX_PRICE:
                continue

            cost = bid_price * BUY_SHARES
            if cost > MAX_TRADE_USD:
                continue

            # 6. Check book
            if live and client:
                book = get_book(client, token_id)
                if book and book["bid"] >= bid_price:
                    log(f"  Skip: existing bid {book['bid']*100:.0f}c >= ours {bid_price*100:.0f}c")
                    continue

            # 7. TRADE
            log(f">>> {symbol.upper()} | MAKE {side} | 1m={m1*100:+.3f}% | fair={fair*100:.0f}c | bid={bid_price*100:.0f}c ask={ask_price*100:.0f}c | {time_left:.0f}s")

            traded_rounds.add(rnd["cid"])
            traded_this_cycle = True

            if live and client:
                pnl = do_trade(client, token_id, bid_price, ask_price, rnd["end"])
            else:
                # Paper simulation
                import random
                time.sleep(2)
                r = random.random()
                if r < 0.3:
                    pnl = TAKE_PROFIT * SELL_SHARES
                    log(f"  [PAPER] PROFIT +${pnl:.2f}")
                elif r < 0.5:
                    pnl = -STOP_LOSS * SELL_SHARES
                    log(f"  [PAPER] STOP LOSS -${abs(pnl):.2f}")
                else:
                    pnl = 0
                    log(f"  [PAPER] No fill")

            daily_pnl += pnl
            trades += 1
            log(f"  Result: ${pnl:+.2f} | Daily: ${daily_pnl:+.2f} | Trades: {trades}")

        # Status
        if int(time.time()) % 60 < 6:
            m1b, _, pb = get_momentum("btcusdt")
            m1e, _, pe = get_momentum("ethusdt")
            fb_up, fb_down = estimate_fair(m1b, 0)
            fe_up, fe_down = estimate_fair(m1e, 0)
            log(f"BTC ${pb:,.0f} ({m1b*100:+.2f}%) U{fb_up*100:.0f}/D{fb_down*100:.0f} | ETH ${pe:,.0f} ({m1e*100:+.2f}%) U{fe_up*100:.0f}/D{fe_down*100:.0f} | Trades:{trades} PnL:${daily_pnl:+.2f}")

        time.sleep(5)


if __name__ == "__main__":
    main()
