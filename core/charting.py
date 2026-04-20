# core/charting.py
# Chart generation for V4 — produces mini-charts for Telegram alerts.
#
# V4 changes vs V3:
#   - No chart_data_service dependency — reads directly from ohlcv_5m
#   - Thread-safe via _CHART_LOCK
#   - Unique filenames (timestamp) to prevent race conditions
#   - Charts saved to charts/ directory (auto-created)
#   - Returns file path or None on failure

from __future__ import annotations

import logging
import os
import threading
import time

import numpy as np
import pandas as pd

# Must be set before any other matplotlib import — prevents GUI backend issues
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, MinuteLocator
from matplotlib.patches import Rectangle

logger = logging.getLogger(__name__)

# Suppress font warnings for non-ASCII symbols (Chinese coin names etc.)
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

# Thread lock — prevents concurrent matplotlib state corruption
_CHART_LOCK = threading.Lock()

_SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CHARTS_DIR = os.path.join(_SCRIPT_DIR, "charts")


def _fetch_5m_candles(symbol: str, minutes: int = 240) -> pd.DataFrame:
    """
    Fetches 5m candles from ohlcv_5m for the given symbol.
    Returns DataFrame with columns: open, high, low, close, volume
    indexed by open_time (UTC-aware datetime).
    """
    from core.database import db_connection

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT open_time, open, high, low, close, volume
                    FROM ohlcv_5m
                    WHERE symbol = %s
                      AND open_time >= NOW() - INTERVAL '%s minutes'
                    ORDER BY open_time ASC
                    """,
                    (symbol, minutes + 10),
                )
                rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            rows,
            columns=["open_time", "open", "high", "low", "close", "volume"],
        )
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        df = df.set_index("open_time")
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna()

    except Exception as e:
        logger.debug(f"5m candle fetch error for {symbol}: {e}")
        return pd.DataFrame()


def generate_chart(
    symbol: str,
    minutes: int = 240,
    spike_start=None,
    spike_end=None,
) -> str | None:
    """
    Generates a mini-chart image for the given symbol.

    Args:
        symbol:      e.g. "BTCUSDT"
        minutes:     chart range in minutes (default 240 = 4h)
        spike_start: optional datetime — start of highlighted region
        spike_end:   optional datetime — end of highlighted region

    Returns:
        path to the PNG file, or None on failure.
    """
    with _CHART_LOCK:
        return _generate_locked(symbol, minutes, spike_start, spike_end)


def _generate_locked(
    symbol: str,
    minutes: int,
    spike_start,
    spike_end,
) -> str | None:
    """Internal implementation — must be called inside _CHART_LOCK."""
    fig = None
    try:
        df = _fetch_5m_candles(symbol, minutes)

        if df.empty or len(df) < 3:
            logger.debug(f"Insufficient data for chart: {symbol}")
            return None

        # Price line from close prices
        price  = df["close"]
        volume = df["volume"]

        # ── FIGURE SETUP ──────────────────────────────────────────────────────
        fig = plt.figure(figsize=(16, 9), facecolor="#0d0d0d")
        gs       = fig.add_gridspec(1, 2, width_ratios=[4, 1], wspace=0.05)
        ax_price = fig.add_subplot(gs[0, 0])
        ax_vol   = ax_price.twinx()
        ax_vbp   = fig.add_subplot(gs[0, 1])

        is_up = float(price.iloc[-1]) >= float(price.iloc[0])

        # ── VOLUME BARS ───────────────────────────────────────────────────────
        vol_max = float(volume.quantile(0.99)) if len(volume) > 0 else 1.0
        if vol_max <= 0:
            vol_max = float(volume.max()) or 1.0

        vol_colors = [
            "#00ff88" if i == 0 or price.iloc[i] >= price.iloc[i - 1] else "#ff3040"
            for i in range(len(price))
        ]

        time_diffs = df.index.to_series().diff().dt.total_seconds().median()
        if pd.isna(time_diffs) or time_diffs == 0:
            time_diffs = 300  # 5min default
        width_days = (time_diffs / 86400) * 0.9

        ax_vol.bar(
            price.index, volume,
            color=vol_colors, width=width_days,
            alpha=0.5, align="center", zorder=1,
        )
        ax_vol.set_ylim(0, vol_max * 4.0)
        ax_vol.axis("off")

        # ── 5m CANDLESTICKS ───────────────────────────────────────────────────
        if len(df) >= 2:
            candle_w = (5 * 60 / 86400) * 0.85  # 5min × 85%
            for ts, row in df.iterrows():
                o, h, l, c = float(row["open"]), float(row["high"]), \
                              float(row["low"]),  float(row["close"])
                candle_up  = c >= o
                color      = "#00ff88" if candle_up else "#ff3040"

                # Wick
                ax_price.plot(
                    [ts, ts], [l, h],
                    color=color, linewidth=1.0, alpha=0.70, zorder=2.3,
                )
                # Body
                body_lo = min(o, c)
                body_h  = abs(c - o)
                if body_h < (h - l) * 0.02 and (h - l) > 0:
                    body_h = (h - l) * 0.02
                rect = Rectangle(
                    (ts - pd.Timedelta(seconds=150 * 0.85), body_lo),
                    pd.Timedelta(seconds=300 * 0.85), body_h,
                    facecolor=color, edgecolor=color,
                    alpha=0.55, zorder=2.4, linewidth=0.7,
                )
                ax_price.add_patch(rect)

        # ── PRICE LINE ────────────────────────────────────────────────────────
        fill_color = "#00ff88" if is_up else "#ff3040"
        ax_price.fill_between(
            price.index, price, float(price.min()),
            color=fill_color, alpha=0.12, zorder=2,
        )
        ax_price.plot(price.index, price, color="#00ffff", linewidth=2.0, zorder=3)
        ax_price.axhline(
            float(price.iloc[-1]),
            color="white", linewidth=1, linestyle="--", alpha=0.5, zorder=3.5,
        )
        ax_price.text(
            0.05, float(price.iloc[-1]),
            f"{float(price.iloc[-1]):,.4f}",
            transform=ax_price.get_yaxis_transform(),
            color="white", fontsize=10, fontweight="bold", va="center",
            bbox=dict(facecolor="#1e1e1e", edgecolor="none", pad=5),
            zorder=4,
        )

        # ── SPIKE HIGHLIGHT ───────────────────────────────────────────────────
        if spike_start is not None and spike_end is not None:
            try:
                # Determine color from price movement
                p_at_start = df["close"].asof(pd.Timestamp(spike_start, tz="UTC")) \
                    if hasattr(df["close"], "asof") else float(price.iloc[0])
                p_at_end   = float(price.iloc[-1])
                spike_up   = p_at_end >= p_at_start
                span_color = "#ff8800" if spike_up else "#ff0044"

                ax_price.axvspan(
                    spike_start, spike_end,
                    alpha=0.15, color=span_color, zorder=1.5,
                )
                ax_price.axvline(
                    spike_start, color=span_color, linewidth=1.5,
                    linestyle="--", alpha=0.8, zorder=4,
                )
                ax_price.axvline(
                    spike_end, color=span_color, linewidth=1.5,
                    linestyle="--", alpha=0.8, zorder=4,
                )
            except Exception:
                pass  # spike markers are optional — don't fail the chart

        # ── VOLUME PROFILE (VBP) ──────────────────────────────────────────────
        ax_vbp.set_facecolor("#0d0d0d")
        bins     = np.linspace(float(price.min()) * 0.995, float(price.max()) * 1.005, 45)
        hist, _  = np.histogram(price, bins=bins, weights=volume)
        centers  = (bins[:-1] + bins[1:]) / 2
        bar_h    = (bins[1] - bins[0]) * 0.88

        ax_vbp.barh(centers, hist, height=bar_h, color="#ff69b4",
                    alpha=0.75, edgecolor="#ff1493", linewidth=0.6)
        max_idx = int(np.argmax(hist))
        ax_vbp.barh(
            centers[max_idx], hist[max_idx],
            height=(bins[1] - bins[0]),
            color="#00ffff", alpha=0.9, edgecolor="#ff1493", linewidth=0.6,
        )
        ax_vbp.set_ylim(ax_price.get_ylim())
        ax_vbp.invert_xaxis()
        ax_vbp.set_xlabel("Vol", color="#ff69b4", fontsize=10)
        ax_vbp.tick_params(colors="#ff69b4", labelsize=8)
        ax_vbp.spines[["top", "right", "left", "bottom"]].set_visible(False)

        # ── STYLING ───────────────────────────────────────────────────────────
        coin_str   = symbol.replace("USDT", "")
        actual_min = int((df.index[-1] - df.index[0]).total_seconds() / 60)
        title_time = f"{actual_min}min" if actual_min > 0 else f"{minutes}min"

        ax_price.set_title(
            f"{coin_str} • {title_time} • ${float(price.iloc[-1]):,.4f}",
            color="white", fontsize=20, fontweight="bold",
            loc="center", pad=10,
        )
        ax_price.grid(True, color="#333333", alpha=0.3, linestyle="--")
        ax_price.set_facecolor("#0d0d0d")
        ax_price.spines[["top", "right", "left", "bottom"]].set_visible(False)
        ax_price.tick_params(axis="x", colors="#888888", labelsize=10)
        ax_price.tick_params(axis="y", colors="#888888", labelsize=10)

        interval = max(1, int(actual_min / 6))
        ax_price.xaxis.set_major_locator(MinuteLocator(interval=interval))
        ax_price.xaxis.set_major_formatter(DateFormatter("%H:%M"))
        ax_price.set_xlim(df.index[0], df.index[-1])
        plt.subplots_adjust(left=0.05, right=0.9, top=0.9, bottom=0.1)

        # ── SAVE ──────────────────────────────────────────────────────────────
        # Unique filename with millisecond timestamp to prevent race conditions
        # when multiple bots generate charts for the same symbol simultaneously.
        os.makedirs(_CHARTS_DIR, exist_ok=True)
        chart_path = os.path.join(
            _CHARTS_DIR,
            f"{symbol}_{int(time.time() * 1000)}_chart.png",
        )
        plt.savefig(
            chart_path, format="png", dpi=150,
            facecolor="#0d0d0d", bbox_inches="tight",
        )
        logger.debug(f"Chart saved: {chart_path}")
        return chart_path

    except Exception as e:
        logger.error(f"Chart generation error for {symbol}: {e}")
        return None

    finally:
        if fig is not None:
            plt.close(fig)
        plt.close("all")
