from __future__ import annotations

import io
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd


def render_chart_png(
    df: pd.DataFrame,
    phase: str,
    tomorrow_bias: str,
    flags: Dict[str, str],
) -> bytes:
    """
    Renders a simple annotated price chart.
    (You can upgrade this to candlesticks later; this keeps dependencies light.)
    """

    df_plot = df.tail(126)

    x = df_plot["date"]
    close = df_plot["close"]

    fig = plt.figure(figsize=(12, 6))
    ax = plt.gca()

    ax.plot(x, close, label="Close")

    # overlays if present
    for col, name in [("ema9", "EMA9"), ("ema20", "EMA20"), ("sma50", "SMA50"), ("sma200", "SMA200")]:
        if col in df_plot.columns and df_plot[col].notna().any():
            ax.plot(x, df_plot[col], label=name)

    title = f"{df_plot.iloc[-1].get('symbol','')}  | Phase: {phase} | Tomorrow: {tomorrow_bias}"
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.legend(loc="upper left")

    # Flag block
    y0 = 0.02
    text = " | ".join([f"{k}:{v}" for k, v in flags.items()])
    fig.text(0.01, y0, text, fontsize=9)

    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    return buf.getvalue()