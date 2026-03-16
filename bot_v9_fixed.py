#!/usr/bin/env python3
"""
Polymarket Bot v9 — Two-Sided Market Maker with Latency Shield
================================================================
Strategy:
  1. Post BUY bids on BOTH Up and Down tokens
  2. If both fill: guaranteed profit (Up + Down < $1, resolve = $1)
  3. Binance monitors for sudden moves
  4. If BTC spikes → cancel the losing side before it fills
  5. Maker fees: 0% + rebates

The edge: we earn the spread between Up + Down prices.
Binance gives us 1-2 second warning to cancel the bad side.

Synchronous. Single loop. Cannot duplicate.
"""

import json
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

MAX_PAIR_COST = 7.00            # Max $ for both sides combined
MAX_DAILY_LOSS = 5.0
BUY_SHARES = 6                  # Buy 6 each side
MIN_SELL_SHARES = 5
EMERGENCY_EXIT_SECONDS = 60
NO_ENTRY_BUFFER = 150           # Need time for both sides to fill
FILL_TIMEOUT = 60               # Wait up to 60s for fills
CANCEL_THRESHOLD = 0.0003       # 0.03% Binance move triggers cancel
MIN_SPREAD_PROFIT = 0.02        # Minimum $0.02 spread to trade
MIN_BID_PRICE = 0.10
MAX_BID_PRICE = 0.55
MONITOR_INTERVAL = 1            # Check Binance every 1 second during fill wait

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"

SLUG_PREFIXES = {
    "btcusdt": "btc-updown-5m",
    "ethusdt": "eth-updown-5m",
}

STOP_FILE = os.path.expanduser("~/polymarket-bot/STOP")

# CTF contract for redemption
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_REDEEM_ABI = [{"inputs":[{"name":"parentCollectionId","type":"bytes32"},{"name":"conditionId","type":"bytes32"},{"name":"indexSets","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}]
USDC_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"


# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

def log(msg):
    line = f"{time.strftime('%H:%M:%S')} | {msg}"
    print(line)
    with open("bot_v9.log", "a") as f:
        f.write(line + "\n")


# ═══════════════════════════════════════════════════════════
# BINANCE
# ═══════════════════════════════════════════════════════════

def get_price(symbol):
    try:
        r = requests.get(BINANCE_API, params={"symbol": symbol.upper()}, timeout=3)
        return float(r.json()["price"])
    except Exception:
        return 0

def get_momentum(symbol):
    try:
        r = requests.get(BINANCE_KLINES, params={
            "symbol": symbol.upper(), "interval": "1m", "limit": 3
        }, timeout=5)
        candles = r.json()
        if len(candles) < 3:
            return 0, 0
        current = float(candles[-1][4])
        prev = float(candles[-2][4])
        return (current - prev) / prev, current
    except Exception:
        return 0, 0


# ═══════════════════════════════════════════════════════════
# FAIR VALUE
# ═══════════════════════════════════════════════════════════

def estimate_fair(change_1m):
    shift = change_1m * 70
    shift = max(-0.40, min(0.40, shift))
    fair_up = round(0.50 + shift, 3)
    fair_down = round(1.0 - fair_up, 3)
    return fair_up, fair_down


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
            return result
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
            return result
        return None
    except Exception as e:
        log(f"  SELL err: {e}")
        return None


def cancel_order(client, order_id):
    try:
        client.cancel(order_id=order_id)
        return True
    except Exception:
        return False


def check_fill(client, order_id):
    try:
        info = client.get_order(order_id)
        if info:
            matched = float(info.get("size_matched", 0))
            total = float(info.get("original_size", info.get("size", 0)))
            if total > 0 and matched >= total * 0.9:
                return True, matched
            return False, matched
        return False, 0
    except Exception:
        return False, 0


# ═══════════════════════════════════════════════════════════
# AUTO-REDEEM
# ═══════════════════════════════════════════════════════════

def try_redeem(condition_id):
    try:
        from web3 import Web3
        pk = os.environ.get("POLY_PRIVATE_KEY", "")
        if not pk:
            return

        w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
        if not w3.is_connected():
            log(f"  Redeem: RPC not connected")
            return

        account = w3.eth.account.from_key(pk)
        ctf = w3.eth.contract(address=CTF_ADDRESS, abi=CTF_REDEEM_ABI)

        parent = bytes(32)
        cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))

        nonce = w3.eth.get_transaction_count(account.address)
        gas_price = w3.eth.gas_price

        tx = ctf.functions.redeemPositions(
            USDC_ADDRESS,
            cond_bytes,
            [1, 2]
        ).build_transaction({
            "from": account.address,
            "nonce": nonce,
            "gas": 200000,
            "gasPrice": gas_price,
            "chainId": 137,
        })

        signed = w3.eth.account.sign_transaction(tx, pk)
        h = w3.eth.send_raw_transaction(signed.raw_transaction)
        log(f"  REDEEM TX: {h.hex()[:20]}...")
        w3.eth.wait_for_transaction_receipt(h, timeout=30)
        log(f"  REDEEMED!")
    except Exception as e:
        log(f"  Redeem err: {e}")


# ═══════════════════════════════════════════════════════════
# TWO-SIDED TRADE LIFECYCLE
# ═══════════════════════════════════════════════════════════

def do_two_sided_trade(client, symbol, rnd):
    """
    The core strategy:
    1. Post BUY bids on BOTH Up and Down
    2. Monitor Binance for sudden moves
    3. If big move → cancel the losing side
    4. If both fill → guaranteed profit at resolution
    5. If one fills → hold to resolution or exit
    """
    tok_up = rnd["tok_up"]
    tok_down = rnd["tok_down"]
    round_end = rnd["end"]
    condition_id = rnd["cid"]

    # Get current momentum for initial fair value
    m1, current_price = get_momentum(symbol)
    fair_up, fair_down = estimate_fair(m1)

    # Our bid prices: slightly below fair on each side
    bid_up = round(fair_up - 0.02, 2)
    bid_down = round(fair_down - 0.02, 2)

    # Check spread profit
    total_cost = bid_up + bid_down
    spread_profit = 1.0 - total_cost  # per share pair

    if spread_profit < MIN_SPREAD_PROFIT:
        log(f"  Spread too thin: ${spread_profit:.3f} (need ${MIN_SPREAD_PROFIT})")
        return 0

    # Price range checks
    if bid_up < MIN_BID_PRICE or bid_up > MAX_BID_PRICE:
        return 0
    if bid_down < MIN_BID_PRICE or bid_down > MAX_BID_PRICE:
        return 0

    # Cost check
    pair_cost = (bid_up + bid_down) * BUY_SHARES
    if pair_cost > MAX_PAIR_COST:
        return 0

    log(f"  TWO-SIDED: Up@{bid_up*100:.0f}c + Down@{bid_down*100:.0f}c = {total_cost*100:.0f}c (profit: {spread_profit*100:.1f}c/pair)")

    # STEP 1: POST BOTH BIDS
    log(f"  Posting Up bid: {BUY_SHARES} @ {bid_up*100:.0f}c")
    up_result = place_buy(client, tok_up, bid_up, BUY_SHARES)
    if not up_result:
        log(f"  Up bid failed")
        return 0
    up_id = up_result["orderID"]
    up_instant = up_result.get("status") == "matched"

    log(f"  Posting Down bid: {BUY_SHARES} @ {bid_down*100:.0f}c")
    down_result = place_buy(client, tok_down, bid_down, BUY_SHARES)
    if not down_result:
        log(f"  Down bid failed, cancelling Up")
        cancel_order(client, up_id)
        return 0
    down_id = down_result["orderID"]
    down_instant = down_result.get("status") == "matched"

    log(f"  Both bids posted! Monitoring with Binance shield...")

    # Track state
    up_filled = up_instant
    down_filled = down_instant
    up_cancelled = False
    down_cancelled = False
    up_received = float(up_result.get("takingAmount", 0)) if up_instant else 0
    down_received = float(down_result.get("takingAmount", 0)) if down_instant else 0
    baseline_price = current_price if current_price > 0 else get_price(symbol)

    # STEP 2: MONITOR — Binance shield + fill checking
    start_time = time.time()

    while time.time() - start_time < FILL_TIMEOUT:
        if os.path.exists(STOP_FILE):
            log(f"  KILL SWITCH")
            if not up_filled and not up_cancelled:
                cancel_order(client, up_id)
            if not down_filled and not down_cancelled:
                cancel_order(client, down_id)
            return 0

        time_left = round_end - time.time()
        if time_left < EMERGENCY_EXIT_SECONDS + 30:
            log(f"  Time running out")
            break

        # Check Binance for sudden move (THE SHIELD)
        now_price = get_price(symbol)
        if now_price > 0 and baseline_price > 0:
            move = (now_price - baseline_price) / baseline_price

            # BTC spiking UP → cancel Down bid (don't want to be stuck with Down)
            if move > CANCEL_THRESHOLD and not down_filled and not down_cancelled:
                log(f"  SHIELD: BTC +{move*100:.3f}% → cancelling Down bid")
                cancel_order(client, down_id)
                down_cancelled = True

            # BTC dropping → cancel Up bid
            if move < -CANCEL_THRESHOLD and not up_filled and not up_cancelled:
                log(f"  SHIELD: BTC {move*100:.3f}% → cancelling Up bid")
                cancel_order(client, up_id)
                up_cancelled = True

        # Check fills
        if not up_filled and not up_cancelled:
            filled, received = check_fill(client, up_id)
            if filled:
                up_filled = True
                up_received = received
                log(f"  Up FILLED: {received} shares")

        if not down_filled and not down_cancelled:
            filled, received = check_fill(client, down_id)
            if filled:
                down_filled = True
                down_received = received
                log(f"  Down FILLED: {received} shares")

        # Both filled = guaranteed profit!
        if up_filled and down_filled:
            log(f"  BOTH FILLED! Guaranteed profit at resolution.")
            break

        # Both cancelled or one filled one cancelled = done waiting
        if (up_filled or up_cancelled) and (down_filled or down_cancelled):
            break

        time.sleep(MONITOR_INTERVAL)

    # STEP 3: CLEANUP unfilled orders
    if not up_filled and not up_cancelled:
        cancel_order(client, up_id)
        log(f"  Cancelled unfilled Up bid")
    if not down_filled and not down_cancelled:
        cancel_order(client, down_id)
        log(f"  Cancelled unfilled Down bid")

    # STEP 4: EVALUATE RESULT
    if up_filled and down_filled:
        # BOTH FILLED — guaranteed profit at resolution
        total_spent = bid_up * up_received + bid_down * down_received
        # One side resolves to $1/share, other to $0
        # We get back min(up_received, down_received) * $1
        min_shares = min(up_received, down_received)
        guaranteed = min_shares * 1.0
        profit = guaranteed - total_spent
        log(f"  GUARANTEED: spent ${total_spent:.2f}, get back ${guaranteed:.2f}, profit ${profit:.2f}")
        log(f"  Waiting for round to resolve for redemption...")

        # Wait for round to end
        wait_time = round_end - time.time()
        if wait_time > 0:
            log(f"  Waiting {wait_time:.0f}s for resolution...")
            time.sleep(min(wait_time + 10, 360))  # wait up to 6 min

        # Auto-redeem
        try_redeem(condition_id)
        return profit

    elif up_filled and not down_filled:
        # Only Up filled — TP sell posted, Binance triggers SL
        log(f"  Only Up filled ({up_received} shares @ {bid_up*100:.0f}c)")
        sell_size = max(MIN_SELL_SHARES, round(up_received - 0.1, 1))

        # Detect initial side
        current = get_price(symbol)
        if current > 0 and baseline_price > 0:
            move = (current - baseline_price) / baseline_price
        else:
            move = 0

        if move < -0.0001:
            tp_offset = 0.02
            sl_offset = 0.02
            log(f"  WRONG SIDE (BTC {move*100:+.3f}%) — tight: +/-2c")
        else:
            tp_offset = 0.03
            sl_offset = 0.03
            log(f"  RIGHT SIDE (BTC {move*100:+.3f}%) — normal: +/-3c")

        tp_price = round(bid_up + tp_offset, 2)
        sl_price = round(max(0.01, bid_up - sl_offset), 2)

        # Post take-profit sell
        log(f"  TP SELL: {sell_size} @ {tp_price*100:.0f}c")
        sell_result = place_sell(client, tok_up, tp_price, sell_size)
        sell_id = sell_result["orderID"] if sell_result else None
        sl_active = False
        sl_sell_id = None

        while True:
            if os.path.exists(STOP_FILE):
                if sell_id: cancel_order(client, sell_id)
                if sl_sell_id: cancel_order(client, sl_sell_id)
                place_sell(client, tok_up, sl_price, sell_size)
                return -sl_offset * sell_size

            # Check TP fill
            if sell_id and not sl_active:
                filled, _ = check_fill(client, sell_id)
                if filled:
                    profit = tp_offset * sell_size
                    log(f"  TAKE PROFIT: +${profit:.2f}")
                    return profit

            # Check SL fill
            if sl_sell_id and sl_active:
                filled, _ = check_fill(client, sl_sell_id)
                if filled:
                    loss = sl_offset * sell_size
                    log(f"  STOP LOSS FILLED: -${loss:.2f}")
                    return -loss

            # Monitor Binance — if BTC reverses, switch to SL sell
            if not sl_active:
                now_price = get_price(symbol)
                if now_price > 0 and baseline_price > 0:
                    now_move = (now_price - baseline_price) / baseline_price
                    # BTC dropping = bad for Up position
                    if now_move < -CANCEL_THRESHOLD:
                        log(f"  BINANCE SL TRIGGER: BTC {now_move*100:.3f}% — switching to SL sell @ {sl_price*100:.0f}c")
                        if sell_id: cancel_order(client, sell_id)
                        sell_id = None
                        sl_result = place_sell(client, tok_up, sl_price, sell_size)
                        sl_sell_id = sl_result["orderID"] if sl_result else None
                        sl_active = True

            # Emergency time exit
            time_left = round_end - time.time()
            if time_left < EMERGENCY_EXIT_SECONDS:
                log(f"  TIME EXIT ({time_left:.0f}s)")
                if sell_id: cancel_order(client, sell_id)
                if sl_sell_id: cancel_order(client, sl_sell_id)
                # Post at low price to exit fast
                exit_price = round(max(0.01, bid_up - sl_offset - 0.02), 2)
                place_sell(client, tok_up, exit_price, sell_size)
                return -sl_offset * sell_size

            time.sleep(1)

    elif down_filled and not up_filled:
        # Only Down filled — TP sell posted, Binance triggers SL
        log(f"  Only Down filled ({down_received} shares @ {bid_down*100:.0f}c)")
        sell_size = max(MIN_SELL_SHARES, round(down_received - 0.1, 1))

        # Detect initial side
        current = get_price(symbol)
        if current > 0 and baseline_price > 0:
            move = (current - baseline_price) / baseline_price
        else:
            move = 0

        if move > 0.0001:
            tp_offset = 0.02
            sl_offset = 0.02
            log(f"  WRONG SIDE (BTC {move*100:+.3f}%) — tight: +/-2c")
        else:
            tp_offset = 0.03
            sl_offset = 0.03
            log(f"  RIGHT SIDE (BTC {move*100:+.3f}%) — normal: +/-3c")

        tp_price = round(bid_down + tp_offset, 2)
        sl_price = round(max(0.01, bid_down - sl_offset), 2)

        # Post take-profit sell
        log(f"  TP SELL: {sell_size} @ {tp_price*100:.0f}c")
        sell_result = place_sell(client, tok_down, tp_price, sell_size)
        sell_id = sell_result["orderID"] if sell_result else None
        sl_active = False
        sl_sell_id = None

        while True:
            if os.path.exists(STOP_FILE):
                if sell_id: cancel_order(client, sell_id)
                if sl_sell_id: cancel_order(client, sl_sell_id)
                place_sell(client, tok_down, sl_price, sell_size)
                return -sl_offset * sell_size

            # Check TP fill
            if sell_id and not sl_active:
                filled, _ = check_fill(client, sell_id)
                if filled:
                    profit = tp_offset * sell_size
                    log(f"  TAKE PROFIT: +${profit:.2f}")
                    return profit

            # Check SL fill
            if sl_sell_id and sl_active:
                filled, _ = check_fill(client, sl_sell_id)
                if filled:
                    loss = sl_offset * sell_size
                    log(f"  STOP LOSS FILLED: -${loss:.2f}")
                    return -loss

            # Monitor Binance — if BTC rises, bad for Down position
            if not sl_active:
                now_price = get_price(symbol)
                if now_price > 0 and baseline_price > 0:
                    now_move = (now_price - baseline_price) / baseline_price
                    # BTC rising = bad for Down position
                    if now_move > CANCEL_THRESHOLD:
                        log(f"  BINANCE SL TRIGGER: BTC +{now_move*100:.3f}% — switching to SL sell @ {sl_price*100:.0f}c")
                        if sell_id: cancel_order(client, sell_id)
                        sell_id = None
                        sl_result = place_sell(client, tok_down, sl_price, sell_size)
                        sl_sell_id = sl_result["orderID"] if sl_result else None
                        sl_active = True

            # Emergency time exit
            time_left = round_end - time.time()
            if time_left < EMERGENCY_EXIT_SECONDS:
                log(f"  TIME EXIT ({time_left:.0f}s)")
                if sell_id: cancel_order(client, sell_id)
                if sl_sell_id: cancel_order(client, sl_sell_id)
                exit_price = round(max(0.01, bid_down - sl_offset - 0.02), 2)
                place_sell(client, tok_down, exit_price, sell_size)
                return -sl_offset * sell_size

            time.sleep(1)

    else:
        # Neither filled
        log(f"  No fills — no trade")
        return 0


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    paper = "--paper" in sys.argv
    live = "--live" in sys.argv

    if not paper and not live:
        print("Usage: python3 bot_v9.py --paper OR --live")
        sys.exit(1)

    log("=" * 55)
    log(f"  BOT v9 - TWO-SIDED MAKER + BINANCE SHIELD")
    log(f"  Mode: {'PAPER' if paper else 'LIVE'}")
    log(f"  Shares: {BUY_SHARES} each side")
    log(f"  Min spread: {MIN_SPREAD_PROFIT*100:.0f}c")
    log(f"  Cancel threshold: {CANCEL_THRESHOLD*100:.2f}%")
    log(f"  Max pair cost: ${MAX_PAIR_COST}")
    log(f"  Max daily loss: ${MAX_DAILY_LOSS}")
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
    both_filled = 0
    one_filled = 0
    no_fills = 0

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

            # 1. Check momentum (any direction is fine for two-sided)
            m1, price = get_momentum(symbol)
            if price <= 0:
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
            fair_up, fair_down = estimate_fair(m1)
            bid_up = round(fair_up - 0.02, 2)
            bid_down = round(fair_down - 0.02, 2)
            total_cost = bid_up + bid_down
            spread = 1.0 - total_cost

            # 4. Check if spread is profitable
            if spread < MIN_SPREAD_PROFIT:
                if int(time.time()) % 30 < 6:
                    log(f"  {symbol}: spread={spread*100:.1f}c (need {MIN_SPREAD_PROFIT*100:.0f}c) Up@{bid_up*100:.0f}c Down@{bid_down*100:.0f}c")
                continue

            if bid_up < MIN_BID_PRICE or bid_down < MIN_BID_PRICE:
                continue
            if bid_up > MAX_BID_PRICE or bid_down > MAX_BID_PRICE:
                continue

            pair_cost = (bid_up + bid_down) * BUY_SHARES
            if pair_cost > MAX_PAIR_COST:
                continue

            # 5. TRADE
            log(f">>> {symbol.upper()} | TWO-SIDED | spread={spread*100:.1f}c | Up@{bid_up*100:.0f}c Down@{bid_down*100:.0f}c | {time_left:.0f}s")

            traded_rounds.add(rnd["cid"])
            traded_this_cycle = True

            if client:
                pnl = do_two_sided_trade(client, symbol, rnd)
            else:
                import random
                time.sleep(2)
                r = random.random()
                if r < 0.3:
                    pnl = spread * MIN_SELL_SHARES
                    both_filled += 1
                    log(f"  [PAPER] BOTH FILLED +${pnl:.2f}")
                elif r < 0.6:
                    pnl = 0.03 * MIN_SELL_SHARES
                    one_filled += 1
                    log(f"  [PAPER] ONE FILLED +${pnl:.2f}")
                else:
                    pnl = 0
                    no_fills += 1
                    log(f"  [PAPER] NO FILLS")

            daily_pnl += pnl
            trades += 1
            log(f"  Result: ${pnl:+.2f} | Daily: ${daily_pnl:+.2f} | Trades: {trades} | Both:{both_filled} One:{one_filled} None:{no_fills}")

        # Status
        if int(time.time()) % 60 < 6:
            m1b, pb = get_momentum("btcusdt")
            m1e, pe = get_momentum("ethusdt")
            fb_up, fb_down = estimate_fair(m1b)
            fe_up, fe_down = estimate_fair(m1e)
            spread_btc = 1.0 - (fb_up - 0.02 + fb_down - 0.02)
            spread_eth = 1.0 - (fe_up - 0.02 + fe_down - 0.02)
            log(f"BTC ${pb:,.0f} ({m1b*100:+.2f}%) spread:{spread_btc*100:.0f}c | ETH ${pe:,.0f} ({m1e*100:+.2f}%) spread:{spread_eth*100:.0f}c | PnL:${daily_pnl:+.2f} | T:{trades}")

        time.sleep(5)


if __name__ == "__main__":
    main()
