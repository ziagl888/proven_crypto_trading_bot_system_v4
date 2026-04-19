#!/usr/bin/env python3
# 00_main_watchdog.py
# System orchestrator for the V4 Crypto Trading Bot.
#
# Startup sequence:
#   1. Load .env
#   2. Run bootstrap (fetch coins, max leverage, init DB schema)
#   3. Start all configured processes in staggered order
#   4. Monitor processes, restart on crash with exponential backoff
#
# File naming convention:
#   00          – watchdog / entry point
#   10–19       – core services (data ingestion, indicator engine, etc.)
#   20–29       – infrastructure (telegram bot, trade monitor, housekeeping)
#   30–69       – trading bots and strategies
#   70–89       – dashboards, utilities, chart services
#   90–99       – backtest / training (not started by watchdog)

import os
import sys
import time
import logging
import datetime
import subprocess

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - WATCHDOG - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("watchdog.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ── Process registry ──────────────────────────────────────────────────────────
# Each entry:
#   name             – display name for logs
#   script           – filename to launch with sys.executable
#   start_delay      – seconds after t=0 to start (staggered startup)
#   restart_interval – seconds between scheduled RAM-recycle restarts
#                      (None = never scheduled, only restart on crash)
#
# NOTE: Scripts 90–99 are NOT listed here (backtests run manually).

PROCESSES: list[dict] = [
    # ── Core services ─────────────────────────────────────────────────
    {"name": "Data Ingestion",     "script": "10_data_ingestion.py",      "start_delay":   0, "restart_interval": None},
    {"name": "Chart Data Service", "script": "11_chart_data_service.py",  "start_delay":   3, "restart_interval": None},
    {"name": "Indicator Engine",   "script": "12_indicator_engine.py",    "start_delay":   5, "restart_interval": 21600},
    {"name": "Detectors",          "script": "13_detectors.py",           "start_delay":   5, "restart_interval": 21600},

    # ── Infrastructure ────────────────────────────────────────────────
    {"name": "Telegram Bot",       "script": "20_telegram_bot.py",        "start_delay":   5, "restart_interval": None},
    {"name": "Trade Monitor",      "script": "21_trade_monitor.py",       "start_delay":   5, "restart_interval": None},
    {"name": "AI Trade Monitor",   "script": "22_ai_trade_monitor.py",    "start_delay":  10, "restart_interval": None},
    {"name": "Housekeeping",       "script": "23_housekeeping.py",        "start_delay":  10, "restart_interval": None},

    # ── Trading bots ──────────────────────────────────────────────────
    {"name": "Pattern Detector",   "script": "30_pattern_detector.py",    "start_delay":  20, "restart_interval": None},
    {"name": "AI SR Bot",          "script": "31_ai_sr_bot.py",           "start_delay":  28, "restart_interval": None},
    {"name": "Pump Dump Detector", "script": "32_pump_dump_detector.py",  "start_delay":  36, "restart_interval": None},
    {"name": "AI MIS1 Detector",   "script": "33_ai_mis_bot.py",          "start_delay":  44, "restart_interval": None},
    {"name": "AI ATS1 Detector",   "script": "34_ai_ats_bot.py",          "start_delay":  52, "restart_interval": None},
    {"name": "AI RUB1 Detector",   "script": "35_ai_rub_bot.py",          "start_delay":  60, "restart_interval": None},
    {"name": "AI ATB1 Detector",   "script": "36_ai_atb_bot.py",          "start_delay":  68, "restart_interval": None},
    {"name": "AI Master Bot",      "script": "37_ai_master_bot.py",       "start_delay":  76, "restart_interval": None},
    {"name": "SMC Forex Bot",      "script": "38_smc_forex_metals_bot.py","start_delay":  84, "restart_interval": None},
    {"name": "Mayank Bot",         "script": "39_mayank_bot.py",          "start_delay":  92, "restart_interval": None},
    {"name": "AI ABR1 Detector",   "script": "40_ai_abr1_bot.py",         "start_delay": 100, "restart_interval": None},
    {"name": "Whale Logger",       "script": "41_whale_logger_bot.py",    "start_delay": 108, "restart_interval": None},
    {"name": "Funding Logger",     "script": "42_funding_logger_bot.py",  "start_delay": 116, "restart_interval": None},
    {"name": "BTC SMC Bot",        "script": "43_btc_smc_strategy.py",    "start_delay": 124, "restart_interval": None},
    {"name": "Market Tracker",     "script": "44_market_tracker.py",      "start_delay": 132, "restart_interval": None},
    {"name": "Quasimodo Bot",      "script": "45_quasimodo_bot.py",       "start_delay": 140, "restart_interval": None},
    {"name": "SMC ML Sniper",      "script": "46_smc_ml_sniper.py",       "start_delay": 148, "restart_interval": None},
    {"name": "Regime Detector",    "script": "47_regime_detector.py",     "start_delay": 157, "restart_interval": None},
    {"name": "Bot Regime Analyzer","script": "48_bot_regime_analyzer.py", "start_delay": 164, "restart_interval": None},
    {"name": "Signal Orchestrator","script": "49_signal_orchestrator.py", "start_delay": 172, "restart_interval": None},
    {"name": "UFI1 Fib Bot",       "script": "50_ufi1_bot.py",            "start_delay": 180, "restart_interval": None},

    # ── Dashboard ─────────────────────────────────────────────────────
    {"name": "Dashboard",          "script": "70_dashboard.py",           "start_delay":   2, "restart_interval": None},
]

# ── Runtime state ─────────────────────────────────────────────────────────────
_running: dict[str, dict] = {}       # name → {process, info, start_time}
_crashes: dict[str, list[float]] = {} # name → [crash timestamps]


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def _run_bootstrap() -> None:
    """Runs the bootstrap sequence synchronously before any bot starts."""
    logger.info("Running bootstrap (coin list + leverage + DB schema)...")
    try:
        from core.bootstrap import run as bootstrap_run
        coins, leverage_map = bootstrap_run()
        logger.info(
            f"Bootstrap complete — {len(coins)} coins, "
            f"{len(leverage_map)} leverage entries."
        )
    except Exception as e:
        logger.critical(f"Bootstrap FAILED: {e}")
        logger.critical("Cannot start without a working DB and coin list. Aborting.")
        sys.exit(1)


# ── Process management ────────────────────────────────────────────────────────

def _start(info: dict) -> None:
    script = info["script"]
    name   = info["name"]
    if not os.path.exists(script):
        logger.warning(f"Script not found, skipping: {script}")
        return
    logger.info(f"Starting  [{name}]  ({script})")
    p = subprocess.Popen([sys.executable, script])
    _running[name] = {
        "process":    p,
        "info":       info,
        "start_time": time.time(),
    }


def _stop(name: str) -> None:
    if name not in _running:
        return
    p = _running[name]["process"]
    logger.info(f"Stopping  [{name}]...")
    p.terminate()
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        logger.warning(f"[{name}] did not stop cleanly — killing.")
        p.kill()
    del _running[name]


def _backoff_delay(name: str) -> float:
    """Exponential back-off based on crash frequency in the last hour."""
    now = time.time()
    history = _crashes.setdefault(name, [])
    history[:] = [t for t in history if now - t < 3600]
    history.append(now)
    schedule = [0, 15, 60, 300, 900]
    idx = min(len(history) - 1, len(schedule) - 1)
    delay = schedule[idx]
    if len(history) >= 5:
        logger.error(
            f"[{name}] crashed {len(history)}x in the last hour — "
            f"waiting {delay}s before restart."
        )
    return delay


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("V4 Crypto Trading Bot System — Watchdog starting")
    logger.info("=" * 60)

    _run_bootstrap()

    # Staggered start — sorted by start_delay
    sorted_procs = sorted(PROCESSES, key=lambda p: p.get("start_delay", 0))
    last = 0
    for info in sorted_procs:
        delay = info.get("start_delay", 0)
        wait  = delay - last
        if wait > 0:
            time.sleep(wait)
        _start(info)
        last = delay

    total = sorted_procs[-1].get("start_delay", 0) if sorted_procs else 0
    logger.info(
        f"All processes started (staggered over {total}s). "
        f"Monitoring loop active."
    )

    try:
        while True:
            now = time.time()

            for info in PROCESSES:
                name = info["name"]

                # Process not tracked → start it
                if name not in _running:
                    if not os.path.exists(info["script"]):
                        continue
                    logger.error(f"[{name}] missing from registry — restarting.")
                    _start(info)
                    continue

                tracker = _running[name]
                rc = tracker["process"].poll()

                if rc is not None:
                    # Process has exited
                    logger.error(
                        f"[{name}] exited with code {rc}. "
                        f"Scheduling restart..."
                    )
                    del _running[name]
                    delay = _backoff_delay(name)
                    if delay > 0:
                        logger.info(f"[{name}] back-off delay: {delay}s")
                        time.sleep(delay)
                    _start(info)
                    continue

                # Scheduled restart (RAM recycle)
                interval = info.get("restart_interval")
                if interval:
                    uptime = now - tracker["start_time"]
                    if uptime >= interval:
                        logger.info(
                            f"[{name}] scheduled restart "
                            f"(uptime {uptime / 3600:.1f}h ≥ {interval / 3600:.0f}h limit)."
                        )
                        _stop(name)
                        _start(info)

            time.sleep(10)

    except KeyboardInterrupt:
        logger.info("Watchdog stopped (Ctrl+C) — shutting down all processes...")
        for name in list(_running.keys()):
            _stop(name)
        logger.info("System fully offline.")


if __name__ == "__main__":
    main()
