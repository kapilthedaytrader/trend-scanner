import io
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from structure import build_structure_pivots

def render_chart_png(df, phase, tomorrow_bias, flags):
    # Display last ~6 months (≈126 trading days)
    df_plot = df.tail(126).copy()

    # Build pivots using full df for better structure continuity,
    # then keep only pivots that fall in df_plot
    pivots = build_structure_pivots(df.copy())

    # Map pivot indices to df_plot indices
    start_global = len(df) - len(df_plot)
    pivots_plot = [p for p in pivots if p.i >= start_global]

    fig = plt.figure(figsize=(14, 7))
    ax = plt.gca()

    # Optional: faint close line for context (very light)
    ax.plot(df_plot["date"], df_plot["close"], linewidth=0.8, alpha=0.25)

    # Overlay EMAs/SMA if present
    for col, lw in [("ema9", 1.0), ("ema20", 1.0), ("sma50", 1.2), ("sma200", 1.4)]:
        if col in df_plot.columns:
            ax.plot(df_plot["date"], df_plot[col], linewidth=lw, alpha=0.9)

    # Structure polyline (HH/LL connection)
    if len(pivots_plot) >= 2:
        x = [p.date for p in pivots_plot]
        y = [p.price for p in pivots_plot]
        ax.plot(x, y, linewidth=2.0)  # (default color; you can set later if you want)

        # mark pivots (optional)
        for p in pivots_plot:
            ax.scatter([p.date], [p.price], s=18, alpha=0.9)
            ax.text(p.date, p.price, p.kind, fontsize=8, alpha=0.9)

    ax.set_title(f"{df_plot.iloc[-1].get('symbol','')} | Phase: {phase} | Tomorrow: {tomorrow_bias}")
    ax.grid(True, alpha=0.2)

    # Return PNG bytes
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    return buf.getvalue()