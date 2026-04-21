# core/charting.py
# Chart generation for V4.
#
# Three chart types with a shared design language:
#
#   generate_chart()              — standard mini-chart (PD-1, Whale, etc.)
#   generate_pattern_chart()      — geometric pattern chart (40_pattern_detector)
#   generate_trendbreaker_chart() — trendline break/bounce chart (41_trendbreaker_detector)
#
# Design language (unified across all charts):
#   Background:  #080c14  (deep navy-black)
#   Panel bg:    #0d1421  (slightly lighter for subplots)
#   Grid:        #1a2540  (subtle blue-grey)
#   Bull candle: #00e676  (vivid green)
#   Bear candle: #ff1744  (electric red)
#   Price line:  #00e5ff  (cyan)
#   Accent 1:    #ff9100  (orange  — trendlines, breakouts)
#   Accent 2:    #d500f9  (violet  — pivot markers)
#   Accent 3:    #ffea00  (yellow  — pattern lower boundary)
#   VBP:         #e040fb  (pink    — volume profile)
#   RSI:         #ffd740  (amber)
#   TSI line:    #69ff47  (lime)
#   TSI signal:  #ff6d00  (deep orange)

from __future__ import annotations

import logging
import os
import threading
import time

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
from matplotlib.dates import DateFormatter as _DateFormatter
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
import scipy.signal

logger = logging.getLogger(__name__)
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)
logging.getLogger("matplotlib").setLevel(logging.WARNING)

_CHART_LOCK = threading.Lock()
_SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CHARTS_DIR = os.path.join(_SCRIPT_DIR, "charts")

# ── Design tokens ─────────────────────────────────────────────────────────────
BG       = "#080c14"
PANEL_BG = "#0d1421"
GRID_COL = "#1a2540"
BULL     = "#00e676"
BEAR     = "#ff1744"
PRICE_L  = "#00e5ff"
ACC1     = "#ff9100"
ACC2     = "#d500f9"
ACC3     = "#ffea00"
VBP_COL  = "#e040fb"
RSI_COL  = "#ffd740"
TSI_COL  = "#69ff47"
TSI_SIG  = "#ff6d00"
EMA9_C   = "#ffea00"
EMA21_C  = "#40c4ff"
EMA50_C  = "#f48fb1"
EMA200_C = "#ff6e40"

try:
    plt.rcParams["font.sans-serif"] = [
        "DejaVu Sans", "Microsoft YaHei", "SimHei",
        "Noto Sans CJK SC", "Arial Unicode MS", "sans-serif",
    ]
    plt.rcParams["axes.unicode_minus"] = False
except Exception:
    pass


# ── DB helpers ────────────────────────────────────────────────────────────────

def _fetch_ohlcv(symbol: str, tf: str, limit: int = 300) -> pd.DataFrame:
    from core.database import db_connection
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT open_time,open,high,low,close,volume "
                    f"FROM ohlcv_{tf} WHERE symbol=%s "
                    f"ORDER BY open_time DESC LIMIT %s",
                    (symbol, limit),
                )
                rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume"])
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        for c in ("open","high","low","close","volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna().sort_values("open_time").set_index("open_time")
    except Exception as e:
        logger.debug(f"OHLCV fetch {symbol}/{tf}: {e}")
        return pd.DataFrame()


def _fetch_indicators(symbol: str, tf: str, limit: int = 300) -> pd.DataFrame:
    from core.database import db_connection
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT open_time,ema_9,ema_21,ema_50,ema_200,"
                    f"rsi_14,tsi_fast_12_7_7,tsi_fast_12_7_7_signal "
                    f"FROM indicators_{tf} WHERE symbol=%s "
                    f"ORDER BY open_time DESC LIMIT %s",
                    (symbol, limit),
                )
                rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=[
            "open_time","ema_9","ema_21","ema_50","ema_200",
            "rsi_14","tsi","tsi_signal",
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        for c in df.columns[1:]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["open_time"]).sort_values("open_time").set_index("open_time")
    except Exception as e:
        logger.debug(f"Indicator fetch {symbol}/{tf}: {e}")
        return pd.DataFrame()


# ── Shared primitives ─────────────────────────────────────────────────────────

def _style_ax(ax, grid: bool = True) -> None:
    ax.set_facecolor(PANEL_BG)
    ax.spines[["top","right","left","bottom"]].set_visible(False)
    ax.tick_params(colors="#4a6080", labelsize=9)
    if grid:
        ax.grid(True, color=GRID_COL, alpha=0.6, linewidth=0.5, linestyle="--")
    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_color("#4a6080")


def _draw_candles(ax, df: pd.DataFrame) -> None:
    if df.empty:
        return
    opens  = df["open"].values.astype(float)
    highs  = df["high"].values.astype(float)
    lows   = df["low"].values.astype(float)
    closes = df["close"].values.astype(float)
    times  = df.index
    min_body = (highs.max() - lows.min()) * 0.002
    td = times[1] - times[0] if len(times) > 1 else pd.Timedelta(minutes=5)
    half = td * 0.38

    for i, ts in enumerate(times):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        color = BULL if c >= o else BEAR
        ax.plot([ts, ts], [l, h], color=color, linewidth=0.8, alpha=0.75, zorder=2)
        body_lo = min(o, c)
        body_h  = max(abs(c - o), min_body)
        rect = Rectangle(
            (ts - half, body_lo), half * 2, body_h,
            facecolor=color, edgecolor=color, alpha=0.80, zorder=3, linewidth=0,
        )
        ax.add_patch(rect)


def _draw_volume(ax_price, ax_vol, df: pd.DataFrame) -> None:
    if df.empty:
        return
    times  = df.index
    volume = df["volume"].values.astype(float)
    closes = df["close"].values.astype(float)
    opens  = df["open"].values.astype(float)
    vol_max = float(np.quantile(volume, 0.97)) if len(volume) > 0 else 1.0
    if vol_max <= 0:
        vol_max = float(volume.max()) or 1.0
    td = times[1] - times[0] if len(times) > 1 else pd.Timedelta(minutes=5)
    w  = (td.total_seconds() / 86400) * 0.9
    colors = [BULL if closes[i] >= opens[i] else BEAR for i in range(len(times))]
    ax_vol.bar(times, volume, color=colors, width=w, alpha=0.28, align="center", zorder=1)
    ax_vol.set_ylim(0, vol_max * 5.0)
    ax_vol.axis("off")


def _draw_vbp(ax_vbp, price: pd.Series, volume: pd.Series, y_min: float, y_max: float) -> None:
    ax_vbp.set_facecolor(PANEL_BG)
    bins    = np.linspace(float(price.min()) * 0.995, float(price.max()) * 1.005, 50)
    hist, _ = np.histogram(price, bins=bins, weights=volume)
    centers = (bins[:-1] + bins[1:]) / 2
    bar_h   = (bins[1] - bins[0]) * 0.88
    ax_vbp.barh(centers, hist, height=bar_h, color=VBP_COL, alpha=0.55, linewidth=0)
    max_i = int(np.argmax(hist))
    ax_vbp.barh(centers[max_i], hist[max_i], height=(bins[1]-bins[0]),
                color=PRICE_L, alpha=0.90, linewidth=0)
    ax_vbp.set_ylim(y_min, y_max)
    ax_vbp.invert_xaxis()
    ax_vbp.set_xlabel("VBP", color=VBP_COL, fontsize=8, labelpad=2)
    ax_vbp.tick_params(colors=VBP_COL, labelsize=7)
    ax_vbp.spines[["top","right","left","bottom"]].set_visible(False)
    ax_vbp.set_xticks([])


def _draw_price_line(ax, price: pd.Series) -> None:
    is_up  = float(price.iloc[-1]) >= float(price.iloc[0])
    smooth = price.ewm(span=2, adjust=False).mean()
    ax.fill_between(smooth.index, smooth, float(smooth.min()),
                    color=BULL if is_up else BEAR, alpha=0.08, zorder=2)
    ax.plot(smooth.index, smooth, color=PRICE_L, linewidth=1.8,
            zorder=4, solid_capstyle="round")


def _draw_price_tag(ax, price: float) -> None:
    ax.axhline(price, color="white", linewidth=0.8, linestyle="--", alpha=0.35, zorder=3)
    ax.text(0.01, price, f"  {price:,.5g}",
            transform=ax.get_yaxis_transform(),
            color="white", fontsize=9, fontweight="bold", va="center", zorder=5,
            bbox=dict(facecolor=PANEL_BG, edgecolor="#2a3c60",
                      boxstyle="round,pad=0.3", alpha=0.90))


def _header_bar(fig, text: str, subtext: str, is_bull: bool | None = None) -> None:
    accent = ACC1 if is_bull is None else (BULL if is_bull else BEAR)
    fig.add_artist(Line2D([0.0, 1.0], [0.986, 0.986],
                          transform=fig.transFigure,
                          color=accent, linewidth=2.5, alpha=0.92))
    fig.text(0.012, 0.976, text, fontsize=15, fontweight="bold",
             color="white", va="top", ha="left", transform=fig.transFigure)
    fig.text(0.012, 0.959, subtext, fontsize=8.5,
             color="#4a6080", va="top", ha="left", transform=fig.transFigure)


def _save_chart(fig, symbol: str, tag: str) -> str:
    os.makedirs(_CHARTS_DIR, exist_ok=True)
    path = os.path.join(_CHARTS_DIR, f"{symbol}_{int(time.time()*1000)}_{tag}.png")
    fig.savefig(path, format="png", dpi=150, facecolor=BG, bbox_inches="tight")
    return path


# ── Chart 1: Standard mini-chart ─────────────────────────────────────────────

def generate_chart(
    symbol: str,
    minutes: int = 240,
    spike_start=None,
    spike_end=None,
    tick_data: list | None = None,
) -> str | None:
    """Standard mini-chart — PD-1, Whale, general alerts."""
    logger.info(f"Generating chart for {symbol} ({minutes}min)...")
    with _CHART_LOCK:
        for try_min in [minutes, 120, 60, 30]:
            path = _mini_locked(symbol, try_min, spike_start, spike_end, tick_data)
            if path:
                logger.info(f"Chart generated: {path}")
                return path
        logger.warning(f"No chart data for {symbol}")
        return None


def _mini_locked(symbol, minutes, spike_start, spike_end, tick_data) -> str | None:
    fig = None
    try:
        df = _fetch_ohlcv(symbol, "5m", limit=int(minutes/5)+10)
        if df.empty or len(df) < 3:
            return None

        price  = df["close"]
        volume = df["volume"]
        last_p = float(price.iloc[-1])
        is_up  = last_p >= float(price.iloc[0])

        fig      = plt.figure(figsize=(16, 9), facecolor=BG)
        gs       = fig.add_gridspec(1, 2, width_ratios=[4, 1], wspace=0.04)
        ax_price = fig.add_subplot(gs[0, 0])
        ax_vol   = ax_price.twinx()
        ax_vbp   = fig.add_subplot(gs[0, 1])

        _draw_volume(ax_price, ax_vol, df)
        _draw_candles(ax_price, df)
        _draw_price_line(ax_price, price)
        _draw_price_tag(ax_price, last_p)

        if spike_start and spike_end:
            try:
                up   = last_p >= float(df["close"].asof(pd.Timestamp(spike_start, tz="UTC")))
                sc   = ACC1 if up else BEAR
                ax_price.axvspan(spike_start, spike_end, alpha=0.12, color=sc, zorder=1)
                for t in (spike_start, spike_end):
                    ax_price.axvline(t, color=sc, linewidth=1.2, linestyle="--", alpha=0.7, zorder=4)
            except Exception:
                pass

        if tick_data:
            try:
                rows = []
                for e in tick_data:
                    ts = pd.Timestamp(e["t"].replace("Z","+00:00")).tz_convert("UTC")
                    p  = float(e["p"])
                    if ts >= df.index[0] and p > 0:
                        rows.append((ts, p))
                if rows:
                    rows.sort(key=lambda x: x[0])
                    ax_price.plot([r[0] for r in rows], [r[1] for r in rows],
                                  color=PRICE_L, linewidth=1.5, alpha=0.9, zorder=5)
            except Exception:
                pass

        _style_ax(ax_price)
        ax_price.xaxis.set_major_formatter(_DateFormatter("%H:%M"))
        ax_price.xaxis.set_major_locator(mticker.MaxNLocator(nbins=8, integer=True))
        ax_price.set_xlim(df.index[0], df.index[-1])
        p_range = float(price.max() - price.min())
        ax_price.set_ylim(float(price.min()) - p_range*0.03, float(price.max()) + p_range*0.06)

        y_lo, y_hi = ax_price.get_ylim()
        _draw_vbp(ax_vbp, price, volume, y_lo, y_hi)

        coin   = symbol.replace("USDT","")
        actual = int((df.index[-1] - df.index[0]).total_seconds() / 60)
        chg    = (last_p / float(price.iloc[0]) - 1) * 100
        chg_s  = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"

        _header_bar(fig,
                    f"{coin}/USDT   ${last_p:,.5g}   {chg_s}",
                    f"Last {actual}min  •  5m candles  •  "
                    f"{pd.Timestamp.now('UTC').strftime('%Y-%m-%d %H:%M')} UTC",
                    is_bull=is_up)

        plt.subplots_adjust(left=0.05, right=0.92, top=0.90, bottom=0.08)
        return _save_chart(fig, symbol, "chart")

    except Exception as e:
        logger.error(f"Mini-chart error {symbol}: {e}", exc_info=True)
        return None
    finally:
        if fig:
            plt.close(fig)
        plt.close("all")


# ── Chart 2: Pattern chart ────────────────────────────────────────────────────

def generate_pattern_chart(
    symbol: str,
    tf: str,
    pattern_name: str,
    direction: str,
    slope_high: float,
    intercept_high: float,
    slope_low: float,
    intercept_low: float,
    candles: int = 120,
    event_state: str = "BREAKOUT",
    break_price: float | None = None,
    retest_price: float | None = None,
) -> str | None:
    """
    Pattern detection chart — Triangles, Channels.
    Upper boundary (orange) + lower boundary (yellow) on candlestick chart.
    """
    logger.info(f"Generating pattern chart: {symbol}/{tf} {pattern_name} {event_state}")
    with _CHART_LOCK:
        return _pattern_locked(
            symbol, tf, pattern_name, direction,
            slope_high, intercept_high, slope_low, intercept_low,
            candles, event_state, break_price, retest_price,
        )


def _pattern_locked(
    symbol, tf, pattern_name, direction,
    slope_high, intercept_high, slope_low, intercept_low,
    candles, event_state, break_price, retest_price,
) -> str | None:
    fig = None
    try:
        df = _fetch_ohlcv(symbol, tf, limit=candles+20)
        if df.empty or len(df) < 20:
            return None

        df     = df.tail(candles).copy()
        n      = len(df)
        price  = df["close"]
        volume = df["volume"]
        last_p = float(price.iloc[-1])
        is_bull = direction == "BULLISH"

        fig = plt.figure(figsize=(18, 10), facecolor=BG)
        gs  = fig.add_gridspec(2, 2,
                               height_ratios=[5, 1],
                               width_ratios=[4, 1],
                               hspace=0.06, wspace=0.04)
        ax_main = fig.add_subplot(gs[0, 0])
        ax_vol  = ax_main.twinx()
        ax_vbp  = fig.add_subplot(gs[0, 1])
        ax_vbar = fig.add_subplot(gs[1, 0])

        _draw_volume(ax_main, ax_vol, df)
        _draw_candles(ax_main, df)
        _draw_price_line(ax_main, price)
        _draw_price_tag(ax_main, last_p)

        # ── Trendlines (project +10 candles forward) ──────────────────────────
        td     = df.index[1] - df.index[0] if n > 1 else pd.Timedelta(hours=1)
        t_ext  = [df.index[-1] + td*(i+1) for i in range(10)]
        t_all  = list(df.index) + t_ext
        x_proj = np.arange(len(t_all))

        y_high = slope_high * x_proj + intercept_high
        y_low  = slope_low  * x_proj + intercept_low

        ul_color = ACC1 if not is_bull else "#ffb74d"
        ax_main.plot(t_all, y_high, color=ul_color, linewidth=2.0,
                     linestyle="-", alpha=0.9, zorder=5, label="Upper boundary")
        ax_main.plot(t_all, y_low, color=ACC3, linewidth=2.0,
                     linestyle="-", alpha=0.9, zorder=5, label="Lower boundary")
        ax_main.fill_between(t_all, y_high, y_low,
                             color=BULL if is_bull else BEAR, alpha=0.05, zorder=1)

        if break_price:
            ax_main.axhline(break_price, color=ACC1, linewidth=1.5,
                            linestyle="--", alpha=0.75, zorder=6)
            ax_main.text(0.01, break_price, f"  Breakout ${break_price:,.5g}",
                         transform=ax_main.get_yaxis_transform(),
                         color=ACC1, fontsize=8, va="center", zorder=7,
                         bbox=dict(facecolor=BG, edgecolor="none", alpha=0.7, pad=2))

        if retest_price:
            ax_main.axhline(retest_price, color=PRICE_L, linewidth=1.2,
                            linestyle=":", alpha=0.70, zorder=6)
            ax_main.text(0.01, retest_price, f"  Retest ${retest_price:,.5g}",
                         transform=ax_main.get_yaxis_transform(),
                         color=PRICE_L, fontsize=8, va="center", zorder=7,
                         bbox=dict(facecolor=BG, edgecolor="none", alpha=0.7, pad=2))

        state_colors = {
            "BREAKOUT":       (ACC1,    "BREAKOUT"),
            "WAITING_RETEST": ("#42a5f5","WAITING RETEST"),
            "RETEST":         (PRICE_L, "🔄 RETEST"),
            "CONFIRMED":      (BULL,    "✅ CONFIRMED"),
            "FAKEOUT":        (BEAR,    "❌ FAKEOUT"),
            "EXPIRED":        ("#555",  "EXPIRED"),
        }
        badge_col, badge_txt = state_colors.get(event_state, (ACC1, event_state))
        ax_main.text(0.99, 0.97, badge_txt,
                     transform=ax_main.transAxes,
                     color=badge_col, fontsize=11, fontweight="bold",
                     ha="right", va="top", zorder=10,
                     bbox=dict(facecolor=PANEL_BG, edgecolor=badge_col,
                               boxstyle="round,pad=0.4", linewidth=1.5))

        handles = [
            Line2D([0],[0], color=ul_color, linewidth=2, label="Upper boundary"),
            Line2D([0],[0], color=ACC3,     linewidth=2, label="Lower boundary"),
        ]
        ax_main.legend(handles=handles, loc="upper left",
                       facecolor=PANEL_BG, edgecolor=GRID_COL,
                       labelcolor="#8090b0", fontsize=8)

        # Volume bar below
        td_sec = td.total_seconds() / 86400
        v_colors = [BULL if float(df["close"].iloc[i]) >= float(df["open"].iloc[i])
                    else BEAR for i in range(n)]
        ax_vbar.bar(df.index, df["volume"], color=v_colors,
                    width=td_sec * 0.85, alpha=0.60)
        ax_vbar.set_xlim(ax_main.get_xlim())
        ax_vbar.set_ylabel("Vol", color="#4a6080", fontsize=8)
        _style_ax(ax_vbar, grid=False)
        ax_vbar.xaxis.set_major_formatter(_DateFormatter("%d.%m %H:%M"))
        ax_vbar.xaxis.set_major_locator(mticker.MaxNLocator(nbins=7))

        _style_ax(ax_main)
        ax_main.set_xlim(df.index[0], t_all[-1])
        y_lo, y_hi = ax_main.get_ylim()
        _draw_vbp(ax_vbp, price, volume, y_lo, y_hi)

        dir_emoji = "🟢" if is_bull else "🔴"
        coin = symbol.replace("USDT","")
        _header_bar(
            fig,
            f"{dir_emoji} {coin}/USDT  [{tf}]  •  {pattern_name}",
            f"State: {badge_txt}  •  Last: ${last_p:,.5g}  •  "
            f"{pd.Timestamp.now('UTC').strftime('%Y-%m-%d %H:%M')} UTC",
            is_bull=is_bull,
        )

        plt.subplots_adjust(left=0.04, right=0.92, top=0.90, bottom=0.06)
        return _save_chart(fig, symbol, f"pattern_{tf}")

    except Exception as e:
        logger.error(f"Pattern chart error {symbol}/{tf}: {e}", exc_info=True)
        return None
    finally:
        if fig:
            plt.close(fig)
        plt.close("all")


# ── Chart 3: Trendbreaker chart ───────────────────────────────────────────────

def generate_trendbreaker_chart(
    symbol: str,
    trend_direction: str,
    slope: float,
    intercept: float,
    event_type: str,
    distance_pct: float | None = None,
    volume_ratio: float | None = None,
    ml_score: float | None = None,
    candles: int = 200,
) -> str | None:
    """
    Trendline break/bounce chart — ATB-1.
    1h candles + 90d trendline + pivot markers + EMAs + RSI + TSI panels.
    """
    logger.info(f"Generating trendbreaker chart: {symbol} {event_type}")
    with _CHART_LOCK:
        return _trendbreaker_locked(
            symbol, trend_direction, slope, intercept,
            event_type, distance_pct, volume_ratio, ml_score, candles,
        )


def _trendbreaker_locked(
    symbol, trend_direction, slope, intercept,
    event_type, distance_pct, volume_ratio, ml_score, candles,
) -> str | None:
    fig = None
    try:
        df = _fetch_ohlcv(symbol, "1h", limit=candles+10)
        if df.empty or len(df) < 20:
            return None

        df     = df.tail(candles).copy()
        n      = len(df)
        price  = df["close"]
        volume = df["volume"]
        last_p = float(price.iloc[-1])
        is_up  = "UP" in event_type
        is_bull = is_up

        ind = _fetch_indicators(symbol, "1h", limit=candles+10)
        if not ind.empty:
            ind = ind.reindex(df.index, method="nearest", tolerance="1h")

        fig = plt.figure(figsize=(20, 12), facecolor=BG)
        gs  = fig.add_gridspec(3, 2,
                               height_ratios=[5, 1.2, 1.2],
                               width_ratios=[4, 1],
                               hspace=0.06, wspace=0.04)
        ax_main = fig.add_subplot(gs[0, 0])
        ax_vol  = ax_main.twinx()
        ax_vbp  = fig.add_subplot(gs[0, 1])
        ax_rsi  = fig.add_subplot(gs[1, 0], sharex=ax_main)
        ax_tsi  = fig.add_subplot(gs[2, 0], sharex=ax_main)

        _draw_volume(ax_main, ax_vol, df)
        _draw_candles(ax_main, df)
        _draw_price_line(ax_main, price)
        _draw_price_tag(ax_main, last_p)

        # EMAs
        if not ind.empty:
            for col, color, lw, label in [
                ("ema_9",   EMA9_C,  1.2, "EMA9"),
                ("ema_21",  EMA21_C, 1.2, "EMA21"),
                ("ema_200", EMA200_C,1.4, "EMA200"),
            ]:
                if col in ind.columns:
                    s = ind[col].dropna()
                    if not s.empty:
                        ax_main.plot(s.index, s, color=color, linewidth=lw,
                                     alpha=0.70, zorder=4.5, label=label)

        # Trendline
        td    = df.index[1] - df.index[0] if n > 1 else pd.Timedelta(hours=1)
        t_ext = [df.index[-1] + td*(i+1) for i in range(8)]
        t_all = list(df.index) + t_ext
        x_all = np.arange(len(t_all))
        y_tl  = slope * x_all + intercept

        tl_color = BULL if is_up else BEAR
        ax_main.plot(t_all, y_tl, color=tl_color, linewidth=3.5,
                     alpha=0.88, zorder=6, label=f"{trend_direction} Trend (90d)")
        band = np.abs(y_tl) * 0.005
        ax_main.fill_between(t_all, y_tl-band, y_tl+band,
                             color=tl_color, alpha=0.08, zorder=1)

        # Pivot markers
        try:
            highs = df["high"].values.astype(float)
            lows  = df["low"].values.astype(float)
            if trend_direction == "DOWN":
                peaks = scipy.signal.argrelextrema(highs, np.greater, order=5)[0]
                px = [df.index[i] for i in peaks]
                py = [highs[i] for i in peaks]
            else:
                peaks = scipy.signal.argrelextrema(lows, np.less, order=5)[0]
                px = [df.index[i] for i in peaks]
                py = [lows[i] for i in peaks]
            if px:
                ax_main.scatter(px, py, color=ACC2, s=60, zorder=8,
                                edgecolors="white", linewidth=0.8, marker="o")
        except Exception:
            pass

        # Break/bounce marker
        last_ts    = df.index[-1]
        tl_at_last = float(slope*(n-1) + intercept)
        if "BREAK" in event_type:
            mkr = "^" if is_up else "v"
            ax_main.scatter([last_ts], [last_p], color=tl_color, s=200,
                            zorder=10, marker=mkr, edgecolors="white", linewidth=1.2)
            ax_main.annotate("",
                xy=(last_ts, last_p), xytext=(last_ts, tl_at_last),
                arrowprops=dict(arrowstyle="->", color=tl_color,
                                lw=1.8, mutation_scale=14), zorder=9)
        else:
            ax_main.scatter([last_ts], [tl_at_last], color=ACC2, s=160,
                            zorder=10, marker="o", edgecolors="white", linewidth=1.5)

        # Info label
        parts = []
        if distance_pct is not None:
            parts.append(f"Dist: {distance_pct:+.2f}%")
        if volume_ratio is not None:
            parts.append(f"Vol: {volume_ratio:.1f}×")
        if ml_score is not None:
            parts.append(f"ML: {ml_score:.1%}")
        if parts:
            ax_main.text(0.99, 0.04, "  ".join(parts),
                         transform=ax_main.transAxes,
                         color="#8090b0", fontsize=9, ha="right", va="bottom",
                         zorder=10,
                         bbox=dict(facecolor=PANEL_BG, edgecolor=GRID_COL,
                                   boxstyle="round,pad=0.4"))

        # Badge
        badge_map = {
            "BREAK_UP":   (BULL, "⚡ BREAK UP"),
            "BREAK_DOWN": (BEAR, "⚡ BREAK DOWN"),
            "BOUNCE_UP":  (BULL, "🔄 BOUNCE UP"),
            "BOUNCE_DOWN":(BEAR, "🔄 BOUNCE DOWN"),
        }
        badge_col, badge_txt = badge_map.get(event_type, (ACC1, event_type))
        ax_main.text(0.99, 0.97, badge_txt,
                     transform=ax_main.transAxes,
                     color=badge_col, fontsize=11, fontweight="bold",
                     ha="right", va="top", zorder=10,
                     bbox=dict(facecolor=PANEL_BG, edgecolor=badge_col,
                               boxstyle="round,pad=0.4", linewidth=1.5))

        # Legend
        handles = [Line2D([0],[0], color=tl_color, linewidth=3.0,
                          label=f"{trend_direction} Trend (90d)")]
        if not ind.empty:
            for col, color, label in [("ema_9",EMA9_C,"EMA9"),
                                       ("ema_21",EMA21_C,"EMA21"),
                                       ("ema_200",EMA200_C,"EMA200")]:
                if col in ind.columns:
                    handles.append(Line2D([0],[0], color=color, linewidth=1.2, label=label))
        ax_main.legend(handles=handles, loc="upper left",
                       facecolor=PANEL_BG, edgecolor=GRID_COL,
                       labelcolor="#8090b0", fontsize=8, ncol=2)

        # RSI panel
        if not ind.empty and "rsi_14" in ind.columns:
            rsi = ind["rsi_14"].dropna()
            if not rsi.empty:
                ax_rsi.plot(rsi.index, rsi, color=RSI_COL, linewidth=1.3)
                ax_rsi.axhline(70, color=BEAR, linewidth=0.8, linestyle="--", alpha=0.5)
                ax_rsi.axhline(30, color=BULL, linewidth=0.8, linestyle="--", alpha=0.5)
                ax_rsi.fill_between(rsi.index, rsi, 50,
                                    where=rsi > 50, color=BULL, alpha=0.07)
                ax_rsi.fill_between(rsi.index, rsi, 50,
                                    where=rsi < 50, color=BEAR, alpha=0.07)
        ax_rsi.set_ylim(0, 100)
        ax_rsi.set_ylabel("RSI", color="#4a6080", fontsize=8, labelpad=2)
        _style_ax(ax_rsi)

        # TSI panel
        if not ind.empty and "tsi" in ind.columns:
            tsi = ind["tsi"].dropna()
            sig = ind["tsi_signal"].dropna() if "tsi_signal" in ind.columns else pd.Series(dtype=float)
            if not tsi.empty:
                ax_tsi.plot(tsi.index, tsi, color=TSI_COL, linewidth=1.5, label="TSI")
            if not sig.empty:
                ax_tsi.plot(sig.index, sig, color=TSI_SIG, linewidth=1.1,
                            linestyle="--", label="Signal")
                common = tsi.index.intersection(sig.index)
                if len(common) > 1:
                    t_c = tsi.reindex(common)
                    s_c = sig.reindex(common)
                    ax_tsi.fill_between(common, t_c, s_c, where=t_c>s_c,
                                        color=BULL, alpha=0.12)
                    ax_tsi.fill_between(common, t_c, s_c, where=t_c<s_c,
                                        color=BEAR, alpha=0.12)
        ax_tsi.axhline(0, color="#4a6080", linewidth=0.6, alpha=0.5)
        ax_tsi.set_ylabel("TSI", color="#4a6080", fontsize=8, labelpad=2)
        ax_tsi.xaxis.set_major_formatter(_DateFormatter("%d.%m %H:%M"))
        ax_tsi.xaxis.set_major_locator(mticker.MaxNLocator(nbins=8))
        _style_ax(ax_tsi)

        _style_ax(ax_main)
        ax_main.set_xlim(df.index[0], t_all[-1])
        y_lo, y_hi = ax_main.get_ylim()
        _draw_vbp(ax_vbp, price, volume, y_lo, y_hi)

        plt.setp(ax_main.get_xticklabels(), visible=False)
        plt.setp(ax_rsi.get_xticklabels(), visible=False)

        coin = symbol.replace("USDT","")
        ml_s = f"  •  ML {ml_score:.1%}" if ml_score is not None else ""
        _header_bar(
            fig,
            f"{badge_txt}  •  {coin}/USDT  •  90d {trend_direction} Trend",
            f"Last: ${last_p:,.5g}"
            + (f"  •  Dist: {distance_pct:+.2f}%" if distance_pct else "")
            + (f"  •  Vol: {volume_ratio:.1f}×" if volume_ratio else "")
            + ml_s
            + f"  •  {pd.Timestamp.now('UTC').strftime('%Y-%m-%d %H:%M')} UTC",
            is_bull=is_bull,
        )

        plt.subplots_adjust(left=0.04, right=0.92, top=0.90, bottom=0.06)
        return _save_chart(fig, symbol, f"trendbreaker_{event_type.lower()}")

    except Exception as e:
        logger.error(f"Trendbreaker chart error {symbol}: {e}", exc_info=True)
        return None
    finally:
        if fig:
            plt.close(fig)
        plt.close("all")
