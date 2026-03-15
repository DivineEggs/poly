#!/usr/bin/env python3
"""
Polymarket Bot v3 — Minimal Edition
=====================================
One round. One order. Sleep. Repeat.

No async gather. No concurrent loops. No coroutines.
Just a simple while loop that cannot duplicate.
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
MIN_PRICE = 0.05
MAX_PRICE = 0.40
MIN_SHARES = 5
MEAN_REVERSION = 0.18
MIN_EDGE = 0.02  # 2 cents
ROUND_DURATION = 300  # 5 minutes
NO_ENTRY_BUFFER = 60  # don't enter last 60 seconds

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com/api/v3/ticker/price"

SLUG_PREFIXES = {
    "btcusdt": "btc-updown-5m",
    "ethusdt": "eth-up-or-down-5m",
}

# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def log(msg):
    print(f"{time.strftime('%H:%M:%S')} │ {msg}")
    with open("bot_v3.log", "a") as f:
        f.write(f"{time.strftime('%H:%M:%S')} │ {msg}\n")


def get_binance_price(symbol: str) -> float:
    """Get current price from Binance REST API."""
    try:
        r = requests.get(BINANCE_API, params={"symbol": symbol.upper()}, timeout=3)
        return float(r.json()["price"])
    except Exception:
        return 0.0


def get_active_round(symbol: str) -> dict | None:
    """Find the current 5-min round via Gamma API."""
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

            # Get start price
            start_price = 0
            try:
                meta = json.loads(event.get("metadata", "{}"))
                start_price = float(meta.get("priceToBeat", 0))
            except Exception:
                pass

            # Get end time
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


def get_order_book(client, token_id: str) -> dict | None:
    """Get best bid/ask for a token."""
    try:
        book = client.get_order_book(token_id)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return None
        return {
            "bid": float(bids[0]["price"]),
            "ask": float(asks[0]["price"]),
        }
    except Exception:
        return None


def fair_value_up(start_price, current_price, time_remaining, vol):
    """P(price ends above start) using normal CDF."""
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


def estimate_vol(symbol: str) -> float:
    """Quick vol estimate from Binance klines."""
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
        vol = float(np.std(rets) * np.sqrt(365.25 * 24 * 60))
        return max(0.2, min(3.0, vol))
    except Exception:
        return 0.8


# ═══════════════════════════════════════════════════════════
# MAIN LOOP — DEAD SIMPLE
# ═══════════════════════════════════════════════════════════

def main():
    paper = "--paper" in sys.argv
    live = "--live" in sys.argv

    if not paper and not live:
        print("Usage: python3 bot_v3.py --paper  OR  python3 bot_v3.py --live")
        sys.exit(1)

    log("=" * 50)
    log(f"  BOT v3 — {'PAPER' if paper else 'LIVE'}")
    log(f"  Max trade: ${MAX_TRADE_USD}")
    log(f"  Max daily loss: ${MAX_DAILY_LOSS}")
    log("=" * 50)

    # Connect to Polymarket
    client = None
    if live:
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

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
                client = ClobClient(
                    CLOB_API, key=pk, chain_id=137, creds=creds,
                )
            log("✅ Polymarket connected")
        except Exception as e:
            log(f"❌ Connection failed: {e}")
            sys.exit(1)

    daily_pnl = 0.0
    traded_rounds = set()  # NEVER trade same round twice

    # ── THE LOOP ──────────────────────────────────────
    # One iteration = check both symbols, maybe place ONE order, sleep.
    # No async. No threads. No concurrency. Cannot duplicate.

    while True:
        # Kill switch
        if os.path.exists(os.path.expanduser("~/polymarket-bot/STOP")):
            log("🛑 STOP file found. Exiting.")
            break

        # Daily loss check
        if daily_pnl <= -MAX_DAILY_LOSS:
            log(f"🛑 Daily loss limit hit: ${daily_pnl:.2f}. Stopping.")
            break

        order_placed_this_cycle = False

        for symbol in ["btcusdt", "ethusdt"]:
            if order_placed_this_cycle:
                break  # ONE order per cycle, period

            # 1. Get current price
            price = get_binance_price(symbol)
            if price <= 0:
                continue

            # 2. Find active round
            rnd = get_active_round(symbol)
            if not rnd:
                continue

            # 3. Already traded this round?
            if rnd["condition_id"] in traded_rounds:
                continue

            # 4. Check timing
            time_left = rnd["end_time"] - time.time()
            if time_left < NO_ENTRY_BUFFER or time_left <= 0:
                continue

            # 5. Start price set?
            if rnd["start_price"] <= 0:
                continue

            # 6. Compute fair value
            vol = estimate_vol(symbol)
            prob_up = fair_value_up(rnd["start_price"], price, time_left, vol)

            # 7. Get order books
            if live and client:
                yes_book = get_order_book(client, rnd["token_yes"])
                no_book = get_order_book(client, rnd["token_no"])
                if not yes_book or not no_book:
                    continue
            else:
                # Paper mode: simulate
                spread = 0.04
                yes_book = {"bid": max(0.01, prob_up - spread), "ask": min(0.99, prob_up + spread)}
                no_book = {"bid": max(0.01, (1-prob_up) - spread), "ask": min(0.99, (1-prob_up) + spread)}

            # 8. Find edge — BUY the underpriced side
            fair_yes = prob_up
            fair_no = 1.0 - prob_up

            edge_buy_no = fair_no - no_book["ask"]
            edge_buy_yes = fair_yes - yes_book["ask"]

            side = None
            token_id = None
            order_price = 0

            if edge_buy_no > MIN_EDGE and edge_buy_no >= edge_buy_yes:
                # NO is underpriced — buy NO as maker
                order_price = round(max(MIN_PRICE, no_book["ask"] - 0.01), 2)
                side = "buy_no"
                token_id = rnd["token_no"]
                edge = edge_buy_no
            elif edge_buy_yes > MIN_EDGE:
                # YES is underpriced — buy YES as maker
                order_price = round(max(MIN_PRICE, yes_book["ask"] - 0.01), 2)
                side = "buy_yes"
                token_id = rnd["token_yes"]
                edge = edge_buy_yes

            if not side:
                continue

            # 9. Price in range?
            if order_price < MIN_PRICE or order_price > MAX_PRICE:
                continue

            # 10. Calculate shares and cost
            shares = MIN_SHARES  # always 5, keep it simple
            actual_cost = round(order_price * shares, 2)

            if actual_cost > MAX_TRADE_USD:
                continue  # too expensive, skip

            # 11. PLACE THE ORDER
            log(
                f"{'📝' if paper else '🟢'} {symbol.upper()} │ {side} │ "
                f"{shares} shares @ {order_price*100:.0f}¢ = ${actual_cost:.2f} │ "
                f"edge={edge*100:.1f}¢ │ {time_left:.0f}s left"
            )

            if live and client:
                try:
                    from py_clob_client.clob_types import OrderArgs, OrderType
                    from py_clob_client.order_builder.constants import BUY

                    order_args = OrderArgs(
                        token_id=token_id,
                        price=order_price,
                        size=shares,
                        side=BUY,
                    )
                    signed = client.create_order(order_args)
                    result = client.post_order(signed, OrderType.GTC)

                    if result and result.get("orderID"):
                        log(f"   ✅ Order placed: {result['orderID'][:16]}")
                    else:
                        log(f"   ❌ Order failed: {result}")
                except Exception as e:
                    log(f"   ❌ Error: {e}")

            # 12. Mark this round as traded — CANNOT trade it again
            traded_rounds.add(rnd["condition_id"])
            order_placed_this_cycle = True
            daily_pnl -= actual_cost  # pessimistic: assume loss until resolved

        # 13. Sleep before next check
        # Check every 5 seconds. That's it.
        time.sleep(5)

        # Print status every 60 seconds
        if int(time.time()) % 60 < 6:
            btc = get_binance_price("btcusdt")
            eth = get_binance_price("ethusdt")
            log(f"📊 BTC: ${btc:,.0f} │ ETH: ${eth:,.0f} │ Rounds traded: {len(traded_rounds)} │ PnL est: ${daily_pnl:.2f}")


if __name__ == "__main__":
    main()
