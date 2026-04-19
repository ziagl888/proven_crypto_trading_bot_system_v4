# core/bootstrap.py
# System bootstrap — called once at startup by the watchdog before any bot
# is started. Fetches the active Binance Futures coin list and the maximum
# leverage for each symbol. Both are persisted locally as JSON files.

from __future__ import annotations

import json
import logging
import os
import time

import requests

from core.config import BASE_URL, BINANCE_API_KEY, BINANCE_SECRET

logger = logging.getLogger(__name__)

COINS_FILE       = "coins.json"
MAX_LEVERAGE_FILE = "max_leverage.json"

# USDC-margined pairs are excluded — V4 trades USDT-margined futures only
_EXCLUDE_SUFFIXES = ("USDC",)


# ── Coin list ─────────────────────────────────────────────────────────────────

def fetch_active_coins() -> list[str]:
    """
    Fetches all currently active USDT-margined Binance Futures pairs.
    Falls back to the cached coins.json on network error.
    """
    url = BASE_URL + "/fapi/v1/exchangeInfo"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        pairs = [
            s["symbol"]
            for s in data.get("symbols", [])
            if s.get("status") == "TRADING"
            and not any(s["symbol"].endswith(sfx) for sfx in _EXCLUDE_SUFFIXES)
        ]
        pairs.sort()
        logger.info(f"Fetched {len(pairs)} active Binance Futures pairs.")
        return pairs

    except Exception as e:
        logger.error(f"Failed to fetch coin list from Binance: {e}")
        # Fall back to cached file
        if os.path.exists(COINS_FILE):
            logger.warning(f"Using cached {COINS_FILE} as fallback.")
            with open(COINS_FILE, encoding="utf-8") as f:
                return json.load(f)
        logger.error("No cached coins.json available — returning minimal fallback.")
        return ["BTCUSDT", "ETHUSDT"]


def save_coins(coins: list[str]) -> None:
    """Atomically writes coins.json."""
    tmp = COINS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(coins, f, indent=2)
    os.replace(tmp, COINS_FILE)
    logger.info(f"coins.json updated — {len(coins)} pairs.")


def load_coins(path: str = COINS_FILE) -> list[str]:
    """Loads the coin list from coins.json. Returns [] on error."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load coins from {path}: {e}")
        return []


# ── Max leverage ──────────────────────────────────────────────────────────────

def fetch_max_leverage(coins: list[str]) -> dict[str, int]:
    """
    Queries the Binance Futures leverage bracket endpoint for each symbol
    and returns a dict {symbol: max_leverage}.

    Requires BINANCE_API_KEY + BINANCE_SECRET. If credentials are missing or
    the API call fails, returns an empty dict (callers fall back to a
    conservative default of 20x).
    """
    if not BINANCE_API_KEY or not BINANCE_SECRET:
        logger.warning(
            "BINANCE_API_KEY / BINANCE_SECRET not set — "
            "skipping leverage fetch, defaulting to 20x everywhere."
        )
        return {}

    import hashlib
    import hmac
    from urllib.parse import urlencode

    url = BASE_URL + "/fapi/v1/leverageBracket"
    leverage_map: dict[str, int] = {}
    errors = 0

    # The endpoint supports fetching all symbols at once when symbol param is omitted.
    # We use Binance server time instead of local time to avoid 401 errors caused
    # by clock drift (common on Windows — Binance rejects requests with timestamp
    # more than ±1000ms from server time).
    try:
        server_time_resp = requests.get(BASE_URL + "/fapi/v1/time", timeout=10)
        server_time_resp.raise_for_status()
        server_ts = server_time_resp.json()["serverTime"]

        params = {
            "timestamp":  server_ts,
            "recvWindow": 10000,    # 10s tolerance window — extra safety margin
        }
        query = urlencode(params)
        sig = hmac.new(
            BINANCE_SECRET.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = sig

        headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for entry in data:
            sym = entry.get("symbol", "")
            brackets = entry.get("brackets", [])
            if brackets:
                # First bracket has the highest leverage
                max_lev = int(brackets[0].get("initialLeverage", 20))
                leverage_map[sym] = max_lev

        logger.info(f"Fetched max leverage for {len(leverage_map)} symbols.")
        return leverage_map

    except Exception as e:
        logger.error(f"Failed to fetch leverage brackets: {e}")
        return {}


def save_max_leverage(leverage_map: dict[str, int]) -> None:
    """Atomically writes max_leverage.json."""
    tmp = MAX_LEVERAGE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(leverage_map, f, indent=2, sort_keys=True)
    os.replace(tmp, MAX_LEVERAGE_FILE)
    logger.info(f"max_leverage.json updated — {len(leverage_map)} entries.")


def load_max_leverage(path: str = MAX_LEVERAGE_FILE) -> dict[str, int]:
    """Loads the leverage map from max_leverage.json. Returns {} on error."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load max leverage from {path}: {e}")
        return {}


def get_max_leverage(symbol: str, desired: int = 20) -> str:
    """
    Returns a leverage string (e.g. '20x') capped by the local leverage map.
    File is loaded once per process and cached.
    """
    global _LEV_CACHE
    if _LEV_CACHE is None:
        _LEV_CACHE = load_max_leverage()
    cap = _LEV_CACHE.get(symbol, 20)
    return f"{min(desired, cap)}x"


_LEV_CACHE: dict[str, int] | None = None


# ── Main bootstrap sequence ───────────────────────────────────────────────────

def run() -> tuple[list[str], dict[str, int]]:
    """
    Full bootstrap sequence:
      1. Fetch active coin list from Binance → save coins.json
      2. Fetch max leverage for each coin → save max_leverage.json
      3. Create all DB tables (schema initialisation)

    Returns (coins, leverage_map).
    """
    from core.schema import create_all_tables, verify_schema

    logger.info("=" * 60)
    logger.info("Bootstrap: fetching active coin list...")
    coins = fetch_active_coins()
    save_coins(coins)

    logger.info("Bootstrap: fetching max leverage brackets...")
    leverage_map = fetch_max_leverage(coins)
    if leverage_map:
        save_max_leverage(leverage_map)
    else:
        logger.warning(
            "No leverage data fetched. "
            "Existing max_leverage.json kept (or defaults to 20x)."
        )

    logger.info("Bootstrap: initialising database schema...")
    create_all_tables()

    result = verify_schema()
    if result["missing"]:
        logger.error(f"Schema verification FAILED — missing tables: {result['missing']}")
    else:
        logger.info(f"Schema OK — {len(result['ok'])} tables verified.")

    logger.info("Bootstrap complete.")
    logger.info("=" * 60)
    return coins, leverage_map

