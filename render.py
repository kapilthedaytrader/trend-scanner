import io
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Rectangle

from structure import build_structure_pivots


def render_chart_waves(df, phase, tomorrow_bias, flags):
    """Chart with structure waves (HH/LL pivots) - original version"""
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
    ax.plot(df_plot["date"], df_plot["close"], linewidth=0.8, alpha=0.25, color='gray')

    # Overlay EMAs/SMA if present
    colors = {"ema9": "orange", "ema20": "blue", "sma50": "green", "sma200": "red"}
    for col, lw in [("ema9", 1.0), ("ema20", 1.0), ("sma50", 1.2), ("sma200", 1.4)]:
        if col in df_plot.columns:
            ax.plot(df_plot["date"], df_plot[col], linewidth=lw, alpha=0.9,
                    color=colors.get(col), label=col.upper())

    # Structure polyline (HH/LL connection)
    if len(pivots_plot) >= 2:
        x = [p.date for p in pivots_plot]
        y = [p.price for p in pivots_plot]
        ax.plot(x, y, linewidth=2.5, color='purple', label='Structure', marker='o', markersize=6)

        # mark pivots
        for p in pivots_plot:
            ax.text(p.date, p.price, p.kind, fontsize=9, alpha=0.9,
                    verticalalignment='bottom' if p.kind == 'LL' else 'top')

    ax.set_title(f"{df_plot.iloc[-1].get('symbol', '')} | Phase: {phase} | Tomorrow: {tomorrow_bias}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.legend(loc='best', fontsize=9)
    ax.grid(True, alpha=0.2)

    # Return PNG bytes
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    return buf.getvalue()


def render_chart_candlestick(df, phase, tomorrow_bias, flags):
    """Chart with candlesticks"""
    # Display last ~6 months (≈126 trading days)
    df_plot = df.tail(126).copy().reset_index(drop=True)

    fig = plt.figure(figsize=(14, 7))
    ax = plt.gca()

    # Draw candlesticks
    width = 0.6
    for idx in range(len(df_plot)):
        row = df_plot.iloc[idx]
        date_num = idx
        open_price = row['open']
        close_price = row['close']
        high_price = row['high']
        low_price = row['low']

        # Determine color
        color = 'green' if close_price >= open_price else 'red'

        # Draw the high-low line
        ax.plot([date_num, date_num], [low_price, high_price],
                color='black', linewidth=0.8)

        # Draw the body rectangle
        body_height = abs(close_price - open_price)
        body_bottom = min(open_price, close_price)
        rect = Rectangle((date_num - width / 2, body_bottom), width, body_height,
                         facecolor=color, edgecolor='black', linewidth=0.8, alpha=0.8)
        ax.add_patch(rect)

    # Overlay EMAs/SMA if present
    colors = {"ema9": "orange", "ema20": "blue", "sma50": "green", "sma200": "red"}
    for col, lw in [("ema9", 1.0), ("ema20", 1.0), ("sma50", 1.2), ("sma200", 1.4)]:
        if col in df_plot.columns:
            ax.plot(range(len(df_plot)), df_plot[col], linewidth=lw, alpha=0.7,
                    color=colors.get(col), label=col.upper())

    # Format x-axis with dates
    tick_spacing = max(1, len(df_plot) // 10)
    tick_positions = range(0, len(df_plot), tick_spacing)
    tick_labels = [df_plot.iloc[i]['date'].strftime('%Y-%m-%d') for i in tick_positions]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, rotation=45, ha='right')

    ax.set_title(f"{df_plot.iloc[-1].get('symbol', '')} Candlestick | Phase: {phase} | Tomorrow: {tomorrow_bias}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.legend(loc='best', fontsize=9)
    ax.grid(True, alpha=0.2)
    ax.set_xlim(-1, len(df_plot))

    # Return PNG bytes
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    return buf.getvalue()


def render_chart_close(df, phase, tomorrow_bias, flags):
    """Simple close price line chart"""
    # Display last ~6 months (≈126 trading days)
    df_plot = df.tail(126).copy()

    fig = plt.figure(figsize=(14, 7))
    ax = plt.gca()

    # Close price line
    ax.plot(df_plot["date"], df_plot["close"], linewidth=2.0, color='black', label='Close')

    # Overlay EMAs/SMA if present
    colors = {"ema9": "orange", "ema20": "blue", "sma50": "green", "sma200": "red"}
    for col, lw in [("ema9", 1.0), ("ema20", 1.0), ("sma50", 1.2), ("sma200", 1.4)]:
        if col in df_plot.columns:
            ax.plot(df_plot["date"], df_plot[col], linewidth=lw, alpha=0.9,
                    color=colors.get(col), label=col.upper())

    ax.set_title(f"{df_plot.iloc[-1].get('symbol', '')} Close Price | Phase: {phase} | Tomorrow: {tomorrow_bias}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.legend(loc='best', fontsize=9)
    ax.grid(True, alpha=0.2)

    # Return PNG bytes
    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=130)
    plt.close(fig)
    return buf.getvalue()


# Keep backward compatibility
def render_chart_png(df, phase, tomorrow_bias, flags):
    """Default chart - waves version"""
    return render_chart_waves(df, phase, tomorrow_bias, flags)