"""
CONFIG — All bot settings in one place.
Adjust these to control risk, position sizes, and behavior.
"""

# ── TRADING SETTINGS ──────────────────────────────────────
MAX_PER_TRADE_USD = 1.0
MAX_TOTAL_EXPOSURE_USD = 50.0
MAX_DAILY_LOSS_USD = 25.0

# ── EDGE SETTINGS ─────────────────────────────────────────
MIN_EDGE_CENTS = 2.0           # Maker fee 0% (rebated), only need to clear taker spread
MIN_CONFIDENCE = 0.55

# ── TIMING ────────────────────────────────────────────────
NO_ENTRY_BUFFER_SECONDS = 45
ORDER_TIMEOUT_SECONDS = 20

# ── SYMBOLS ───────────────────────────────────────────────
SYMBOLS = ["btcusdt", "ethusdt"]

# ── POLYMARKET ────────────────────────────────────────────
POLYMARKET_API_URL = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
CHAIN_ID = 137

# ── SERIES SLUGS (5-minute Up/Down rounds) ────────────────
SERIES_SLUGS = {
    "btcusdt": "btc-up-or-down-5m",
    "ethusdt": "eth-up-or-down-5m",
}

# ── FEES ──────────────────────────────────────────────────
# makerRebatesFeeShareBps=10000 means 100% of maker fee is rebated → net 0%
MAKER_FEE = 0.0
MAKER_REBATE = 0.0
TAKER_FEE = 0.10

# ── VOLATILITY MODEL ─────────────────────────────────────
VOL_WINDOW_SECONDS = 60
MEAN_REVERSION_FACTOR = 0.18

# ── LOGGING ───────────────────────────────────────────────
LOG_LEVEL = "INFO"
LOG_FILE = "bot.log"
