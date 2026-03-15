#!/usr/bin/env python3
"""
Polymarket Bot v4 — True Maker Strategy
=========================================
Buy underpriced side → immediately post sell order → capture spread.
Never hold to resolution. In and out within seconds.

Flow per trade:
1. Detect edge (Binance price vs Polymarket book)
2. BUY the underpriced token (limit order, maker)
3. Wait for fill
4. Immediately post SELL at entry + TAKE_PROFIT
5. Monitor:
   - Sell fills → profit captured, done
   - Price drops to STOP_LOSS → cancel sell, market sell to exit
   - 60s before round end → emergency exit at any price
6. Move to next round

No async. Synchronous loop. Cannot duplicate.
"""

import json
import math
import os
import sys
import time
import requests
import numpy as np
from scipy import stats
from dotenv import load_dotenv

load_dotenv()

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

MAX_TRADE_USD = 3.0
MAX_DAILY_LOSS = 10.0
TAKE_PROFIT = 0.02          # sell at entry + 2¢
STOP_LOSS = 0.03             # exit if price drops 3¢ from entry
EMERGENCY_EXIT_SECONDS = 60  # force exit 60s before round end
MIN_PRICE = 0.05
MAX_PRICE = 0.40
MIN_SHARES = 5
MEAN_REVERSION = 0.18
MIN_EDGE = 0.02              # 2¢ minimum edge to enter
ROUND_DURATION = 300
NO_ENTRY_BUFFER = 120        # don't enter in last 2 min (need time to exit)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com/api/v3/ticker/price"

SLUG_PREFIXES = {
    "btcusdt": "btc-updown-5m",
    "ethusdt": "eth-up-or-down-5m",
}

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

def log(msg):
    line = f"{time.strftime('%H:%M:%S')} │ {msg}"
    print(line)
    with open("bot_v4.log", "a") as f:
        f.write(line + "\n")


# ═══════════════════════════════════════════════════════════
# BINANCE PRICE
# ═══════════════════════════════════════════════════════════

def get_binance_price(symbol):
    try:
        r = requests.get(BINANCE_API, params={"symbol": symbol.upper()}, timeout=3)
        return float(r.json()["price"])
    except Exception:
        return 0.0


def estimate_vol(symbol):
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol.upper(), "interval": "1m", "limit": 30},
            timeout=5,
        )
        closes = [float(k[4]) for k in r.json()]
        if len(closes) < 10:
            return 0.8
        rets = np.diff(np.log(closes))
        return max(0.2, min(3.0, float(np.std(rets) * np.sqrt(365.25 * 24 * 60))))
    except Exception:
        return 0.8


# ═══════════════════════════════════════════════════════════
# FAIR VALUE
# ═══════════════════════════════════════════════════════════

def fair_value_up(start_price, current_price, time_remaining, vol):
    if start_price <= 0 or time_remaining <= 0:
        return 0.5
    move = (current_price - start_price) / start_price
    t_yr = time_remaining / (365.25 * 24 * 3600)
    sigma = vol * math.sqrt(t_yr)
    if sigma < 1e-10:
        return 1.0 if move > 0 else 0.0
    frac = 1.0 - (time_remaining / ROUND_DURATION)
    adj = move * (1.0 - MEAN_REVERSION * frac)
    return float(stats.norm.cdf(adj / sigma))


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
            tokens = market.get("clobTokenIds", [])
            if len(tokens) < 2:
                continue

            start_price = 0
            try:
                meta = json.loads(event.get("metadata", "{}"))
                start_price = float(meta.get("priceToBeat", 0))
            except Exception:
                pass

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
                "token_yes": tokens[0],
                "token_no": tokens[1],
                "start_price": start_price,
                "end_time": end_time,
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
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None
        return {
            "bid": float(bids[0]["price"]),
            "ask": float(asks[0]["price"]),
            "bid_size": float(bids[0]["size"]),
            "ask_size": float(asks[0]["size"]),
        }
    except Exception:
        return None


def get_midpoint(client, token_id):
    """Get current midpoint price for a token."""
    try:
        mid = client.get_midpoint(token_id)
        return float(mid)
    except Exception:
        book = get_book(client, token_id)
        if book:
            return (book["bid"] + book["ask"]) / 2
        return 0


# ═══════════════════════════════════════════════════════════
# ORDER PLACEMENT
# ═══════════════════════════════════════════════════════════

def place_buy(client, token_id, price, size):
    """Place a BUY limit order. Returns order_id or None."""
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
    """Place a SELL limit order. Returns order_id or None."""
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
    """Cancel an order."""
    try:
        client.cancel(order_id=order_id)
        return True
    except Exception:
        return False


def check_order_filled(client, order_id):
    """Check if an order has been filled."""
    try:
        order = client.get_order(order_id)
        if order:
            size_matched = float(order.get("size_matched", 0))
            original_size = float(order.get("original_size", order.get("size", 0)))
            if size_matched >= original_size * 0.9:  # 90%+ filled = filled
                return True
        return False
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════
# THE TRADE LIFECYCLE
# ═══════════════════════════════════════════════════════════

def execute_trade(client, token_id, entry_price, shares, round_end_time, paper=False):
    """
    Full trade lifecycle:
    1. BUY at entry_price
    2. Post SELL at entry_price + TAKE_PROFIT
    3. Monitor until: sell fills (profit), stop loss hit, or time runs out
    4. Exit one way or another. Never hold to resolution.

    Returns profit/loss in USD.
    """
    actual_cost = entry_price * shares

    # ── STEP 1: BUY ──
    log(f"   BUYING {shares} shares @ {entry_price*100:.0f}¢ = ${actual_cost:.2f}")

    buy_order_id = None
    if paper:
        buy_order_id = "paper_buy"
        log(f"   [PAPER] Buy filled")
    else:
        buy_order_id = place_buy(client, token_id, entry_price, shares)
        if not buy_order_id:
            log(f"   ❌ Buy order failed")
            return 0

        # Wait for buy to fill (max 15 seconds)
        buy_filled = False
        for _ in range(15):
            if check_order_filled(client, buy_order_id):
                buy_filled = True
                break
            time.sleep(1)

        if not buy_filled:
            log(f"   Buy not filled in 15s — cancelling")
            cancel_order(client, buy_order_id)
            return 0

    log(f"   ✅ Buy filled. Now posting sell...")

    # ── STEP 2: SELL at take-profit price ──
    sell_price = round(entry_price + TAKE_PROFIT, 2)
    sell_price = min(0.99, sell_price)

    sell_order_id = None
    if paper:
        sell_order_id = "paper_sell"
        log(f"   [PAPER] Sell posted @ {sell_price*100:.0f}¢ (TP: +{TAKE_PROFIT*100:.0f}¢)")
    else:
        sell_order_id = place_sell(client, token_id, sell_price, shares)
        if not sell_order_id:
            log(f"   ❌ Sell order failed — holding position (dangerous)")
            # Try to market sell as emergency
            emergency_sell(client, token_id, shares)
            return -actual_cost * 0.05  # estimate small loss

    log(f"   Sell posted @ {sell_price*100:.0f}¢. Monitoring...")

    # ── STEP 3: MONITOR ──
    # Check every 2 seconds: sell filled? stop loss? time up?
    while True:
        now = time.time()
        time_left = round_end_time - now

        # Kill switch
        if os.path.exists(os.path.expanduser("~/polymarket-bot/STOP")):
            log(f"   🛑 KILL SWITCH — emergency exit")
            if not paper:
                cancel_order(client, sell_order_id)
                emergency_sell(client, token_id, shares)
            return -actual_cost * 0.03

        # Check if sell filled → PROFIT
        if not paper:
            if check_order_filled(client, sell_order_id):
                profit = TAKE_PROFIT * shares
                log(f"   💰 SELL FILLED — profit: ${profit:.2f}")
                return profit
        else:
            # Paper: simulate fill if midpoint reaches sell price
            # (simplified — real fills depend on someone taking our order)
            pass

        # Emergency exit: too close to round end
        if time_left < EMERGENCY_EXIT_SECONDS:
            log(f"   ⏰ Emergency exit — {time_left:.0f}s left")
            if not paper:
                cancel_order(client, sell_order_id)
                emergency_sell(client, token_id, shares)
            # Estimate: sell at current mid, probably small loss
            return -actual_cost * 0.02

        # Stop loss: check current price
        if not paper:
            current_mid = get_midpoint(client, token_id)
            if current_mid > 0 and current_mid < entry_price - STOP_LOSS:
                log(f"   🔴 STOP LOSS — mid={current_mid*100:.0f}¢, entry={entry_price*100:.0f}¢")
                cancel_order(client, sell_order_id)
                # Market sell to exit
                emergency_sell(client, token_id, shares)
                loss = (entry_price - current_mid + 0.01) * shares  # estimate
                return -loss

        # Paper mode: simulate after 30 seconds
        if paper:
            time.sleep(2)
            # Simulate: 60% chance take profit hits, 20% stop loss, 20% emergency exit
            import random
            roll = random.random()
            if roll < 0.6:
                profit = TAKE_PROFIT * shares
                log(f"   [PAPER] 💰 Take profit: +${profit:.2f}")
                return profit
            elif roll < 0.8:
                loss = STOP_LOSS * shares
                log(f"   [PAPER] 🔴 Stop loss: -${loss:.2f}")
                return -loss
            else:
                log(f"   [PAPER] ⏰ Emergency exit: ~$0")
                return -actual_cost * 0.02

        time.sleep(2)  # check every 2 seconds


def emergency_sell(client, token_id, shares):
    """Market sell — sell at whatever price to exit immediately."""
    try:
        book = get_book(client, token_id)
        if book and book["bid"] > 0:
            # Sell at the bid (immediate fill)
            sell_price = book["bid"]
            log(f"   Emergency sell @ {sell_price*100:.0f}¢")
            place_sell(client, token_id, sell_price, shares)
        else:
            # Sell at 1¢ — basically giving it away to exit
            place_sell(client, token_id, 0.01, shares)
    except Exception as e:
        log(f"   Emergency sell error: {e}")


# ═══════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════

def main():
    paper = "--paper" in sys.argv
    live = "--live" in sys.argv

    if not paper and not live:
        print("Usage: python3 bot_v4.py --paper  OR  python3 bot_v4.py --live")
        sys.exit(1)

    log("=" * 55)
    log(f"  BOT v4 — TRUE MAKER STRATEGY")
    log(f"  Mode: {'PAPER' if paper else 'LIVE'}")
    log(f"  Max trade: ${MAX_TRADE_USD}")
    log(f"  Take profit: +{TAKE_PROFIT*100:.0f}¢/share")
    log(f"  Stop loss: -{STOP_LOSS*100:.0f}¢/share")
    log(f"  Emergency exit: {EMERGENCY_EXIT_SECONDS}s before end")
    log(f"  Max daily loss: ${MAX_DAILY_LOSS}")
    log("=" * 55)

    # Connect
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
            log(f"❌ Connection failed: {e}")
            sys.exit(1)

    daily_pnl = 0.0
    traded_rounds = set()
    trades_today = 0

    while True:
        # Kill switch
        if os.path.exists(os.path.expanduser("~/polymarket-bot/STOP")):
            log("🛑 STOP file. Exiting.")
            break

        # Daily loss limit
        if daily_pnl <= -MAX_DAILY_LOSS:
            log(f"🛑 Daily loss limit: ${daily_pnl:.2f}")
            break

        traded_this_cycle = False

        for symbol in ["btcusdt", "ethusdt"]:
            if traded_this_cycle:
                break

            price = get_binance_price(symbol)
            if price <= 0:
                continue

            rnd = get_active_round(symbol)
            if not rnd:
                continue

            if rnd["condition_id"] in traded_rounds:
                continue

            time_left = rnd["end_time"] - time.time()
            if time_left < NO_ENTRY_BUFFER:
                continue

            if rnd["start_price"] <= 0:
                continue

            # Fair value
            vol = estimate_vol(symbol)
            prob_up = fair_value_up(rnd["start_price"], price, time_left, vol)

            # Get books
            if live and client:
                yes_book = get_book(client, rnd["token_yes"])
                no_book = get_book(client, rnd["token_no"])
                if not yes_book or not no_book:
                    continue
            else:
                spread = 0.04
                yes_book = {"bid": max(0.01, prob_up - spread), "ask": min(0.99, prob_up + spread)}
                no_book = {"bid": max(0.01, (1-prob_up) - spread), "ask": min(0.99, (1-prob_up) + spread)}

            # Find edge
            fair_yes = prob_up
            fair_no = 1.0 - prob_up

            edge_no = fair_no - no_book["ask"]
            edge_yes = fair_yes - yes_book["ask"]

            token_id = None
            entry_price = 0
            side_name = ""

            if edge_no > MIN_EDGE and edge_no >= edge_yes:
                entry_price = round(max(MIN_PRICE, no_book["ask"] - 0.01), 2)
                token_id = rnd["token_no"]
                side_name = "BUY NO"
            elif edge_yes > MIN_EDGE:
                entry_price = round(max(MIN_PRICE, yes_book["ask"] - 0.01), 2)
                token_id = rnd["token_yes"]
                side_name = "BUY YES"

            if not token_id:
                continue

            # Price range check
            if entry_price < MIN_PRICE or entry_price > MAX_PRICE:
                continue

            # Cost check
            actual_cost = entry_price * MIN_SHARES
            if actual_cost > MAX_TRADE_USD:
                continue

            # ── TRADE! ──
            log(f"🎯 {symbol.upper()} │ {side_name} │ edge={edge_no*100:.1f}¢ │ {time_left:.0f}s left")

            # Mark round BEFORE placing order
            traded_rounds.add(rnd["condition_id"])
            traded_this_cycle = True

            # Execute full lifecycle: buy → sell → monitor → exit
            pnl = execute_trade(
                client=client,
                token_id=token_id,
                entry_price=entry_price,
                shares=MIN_SHARES,
                round_end_time=rnd["end_time"],
                paper=paper,
            )

            daily_pnl += pnl
            trades_today += 1
            log(f"   Trade PnL: ${pnl:+.2f} │ Daily: ${daily_pnl:+.2f} │ Trades: {trades_today}")

        # Status every ~60 seconds
        if int(time.time()) % 60 < 6:
            btc = get_binance_price("btcusdt")
            eth = get_binance_price("ethusdt")
            log(f"📊 BTC: ${btc:,.0f} │ ETH: ${eth:,.0f} │ Trades: {trades_today} │ PnL: ${daily_pnl:+.2f}")

        time.sleep(5)


if __name__ == "__main__":
    main()
