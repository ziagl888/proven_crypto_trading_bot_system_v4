#!/usr/bin/env python3
# 00_main_watchdog.py
# System entry point for the V4 Crypto Trading Bot.
#
# Current state: Bootstrap only.
# Processes will be added incrementally as each module is implemented.
#
# File naming convention:
#   00          – watchdog / entry point
#   10–19       – core services (data ingestion, indicator engine, etc.)
#   20–29       – infrastructure (telegram bot, trade monitor, housekeeping)
#   30–69       – trading bots and strategies
#   70–89       – dashboards, utilities, chart services
#   90–99       – backtest / training (never started by watchdog)

import os
import sys
import time
import logging
import logging.handlers
import signal
import subprocess
import threading

from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - WATCHDOG - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "watchdog.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)


# ── Process registry ──────────────────────────────────────────────────────────
# Processes are added here as each module is implemented and tested.
# Each entry:
#   name             – display name for logs
#   script           – filename to launch with sys.executable
#   start_delay      – seconds after t=0 to start (staggered startup)
#   restart_interval – seconds between scheduled RAM-recycle restarts
#                      (None = only restart on crash)

PROCESSES: list[dict] = [
    # ── Core data services ────────────────────────────────────────────
    {"name": "Data Ingestion",    "script": "10_data_ingestion.py",   "start_delay":  0, "restart_interval": None},
    {"name": "Gap Checker",       "script": "11_gap_checker.py",      "start_delay":  5, "restart_interval": None},
    {"name": "Indicator Engine",  "script": "12_indicator_engine.py", "start_delay": 10, "restart_interval": None},
    {"name": "Housekeeping",      "script": "23_housekeeping.py",     "start_delay": 15, "restart_interval": None},
    # ── Infrastructure ────────────────────────────────────────────────
    {"name": "Telegram Bot",      "script": "20_telegram_bot.py",     "start_delay": 20, "restart_interval": None},
    # ── Market monitoring ─────────────────────────────────────────────
    {"name": "Pump/Dump Detector","script": "30_pump_dump_detector.py","start_delay": 25, "restart_interval": None},
    {"name": "Market Tracker",    "script": "31_market_tracker.py",   "start_delay": 30, "restart_interval": None},
    # ── Trade management ──────────────────────────────────────────────
    {"name": "Trade Monitor",     "script": "21_trade_monitor.py",    "start_delay": 35, "restart_interval": None},
]

# ── Shutdown coordination ─────────────────────────────────────────────────────
# Used to make all time.sleep() calls in the watchdog interruptible.
_SHUTDOWN_EVENT = threading.Event()


# ── Runtime state ─────────────────────────────────────────────────────────────
_running: dict[str, dict] = {}
_crashes: dict[str, list[float]] = {}


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def _run_bootstrap() -> None:
    """
    Runs the bootstrap sequence before any bot starts:
      1. Fetch active coin list from Binance  -> coins.json
      2. Fetch max leverage per symbol        -> max_leverage.json
      3. Initialise DB schema (OHLCV + indicator tables only)
    """
    logger.info("Running bootstrap...")
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
    logger.info(f"Starting [{name}] ({script})")
    # -u = unbuffered so log output appears immediately
    # PYTHONUNBUFFERED=1 ensures the subprocess flushes stdout/stderr
    p = subprocess.Popen(
        [sys.executable, "-u", script],
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    _running[name] = {
        "process":    p,
        "info":       info,
        "start_time": time.time(),
    }


def _stop(name: str) -> None:
    if name not in _running:
        return
    p = _running[name]["process"]
    logger.info(f"Stopping [{name}]...")
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

def _shutdown_all(signum, frame) -> None:
    """Handle SIGTERM gracefully — same as Ctrl+C."""
    logger.info("SIGTERM received — shutting down all processes...")
    _SHUTDOWN_EVENT.set()
    for name in list(_running.keys()):
        _stop(name)
    logger.info("System fully offline.")
    sys.exit(0)


def main() -> None:
    logger.info("=" * 60)
    logger.info("V4 Crypto Trading Bot System — Watchdog starting")
    logger.info("=" * 60)
    signal.signal(signal.SIGTERM, _shutdown_all)

    # Step 1: Bootstrap (always runs)
    _run_bootstrap()

    # Step 2: Start configured processes (none yet)
    if not PROCESSES:
        logger.info("No processes configured yet — bootstrap complete, exiting.")
        logger.info("Add processes to PROCESSES list as modules are implemented.")
        return

    sorted_procs = sorted(PROCESSES, key=lambda p: p.get("start_delay", 0))
    last = 0
    for info in sorted_procs:
        delay = info.get("start_delay", 0)
        wait  = delay - last
        if wait > 0:
            if _SHUTDOWN_EVENT.wait(timeout=wait):
                return  # Ctrl+C during stagger — abort startup
        _start(info)
        last = delay

    total = sorted_procs[-1].get("start_delay", 0) if sorted_procs else 0
    logger.info(f"All processes started (staggered over {total}s). Monitoring active.")

    # Step 3: Monitor loop
    try:
        while True:
            now = time.time()
            for info in PROCESSES:
                name = info["name"]
                if name not in _running:
                    if not os.path.exists(info["script"]):
                        continue
                    logger.error(f"[{name}] missing — restarting.")
                    _start(info)
                    continue
                tracker = _running[name]
                rc = tracker["process"].poll()
                if rc is not None:
                    uptime  = now - tracker["start_time"]
                    crashed = uptime < 30   # died within 30s = likely crash on import
                    level   = "CRITICAL" if crashed else "ERROR"
                    getattr(logger, level.lower())(
                        f"[{name}] exited (code={rc}, uptime={uptime:.0f}s) — "
                        f"{'likely import/startup crash' if crashed else 'scheduling restart'}."
                    )
                    del _running[name]
                    delay = _backoff_delay(name)
                    if delay > 0:
                        logger.info(f"[{name}] waiting {delay}s before restart...")
                        if _SHUTDOWN_EVENT.wait(timeout=delay):
                            break  # Shutdown during backoff — stop monitoring
                    _start(info)
                    continue
                interval = info.get("restart_interval")
                if interval:
                    uptime = now - tracker["start_time"]
                    if uptime >= interval:
                        logger.info(f"[{name}] scheduled restart (uptime {uptime/3600:.1f}h).")
                        _stop(name)
                        _start(info)
            _SHUTDOWN_EVENT.wait(timeout=10)

    except KeyboardInterrupt:
        logger.info("Watchdog stopped (Ctrl+C) — shutting down all processes...")
        _SHUTDOWN_EVENT.set()
        for name in list(_running.keys()):
            _stop(name)
        logger.info("System fully offline.")


if __name__ == "__main__":
    main()










