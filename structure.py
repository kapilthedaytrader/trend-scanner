from dataclasses import dataclass
from typing import List, Optional

import pandas as pd


@dataclass
class Pivot:
    i: int
    date: pd.Timestamp
    price: float
    kind: str  # "HH" or "LL"


def _initial_trend(df: pd.DataFrame, i: int) -> str:
    """Bootstrap initial trend using MA relationship"""
    row = df.iloc[i]
    sma50 = row.get("sma50")
    sma200 = row.get("sma200")
    close = row["close"]

    if pd.notna(sma50) and pd.notna(sma200):
        if close >= sma50 and sma50 >= sma200:
            return "UP"
        if close <= sma50 and sma50 <= sma200:
            return "DOWN"
    return "NEUTRAL"


def build_structure_pivots(df: pd.DataFrame) -> List[Pivot]:
    """
    Build HH/LL pivots using precise rules:

    UPTREND:
    1. Track Higher Lows (HLs) - as long as low > last_HL
    2. Break: when low <= last_HL, uptrend threatened
    3. Find HH: max(high) of last <=3 candles including break candle
    4. Confirm topping: wait for lower-high sequence (next high < prev high)
    5. Find LL: min(low) of last <=3 candles including confirmation candle
    6. Plot HH -> LL, switch to downtrend

    DOWNTREND (mirror):
    1. Track Lower Highs (LHs) - as long as high < last_LH
    2. Break: when high >= last_LH, downtrend threatened
    3. Find LL: min(low) of last <=3 candles including break candle
    4. Confirm bottoming: wait for higher-low sequence (next low > prev low)
    5. Find HH: max(high) of last <=3 candles including confirmation candle
    6. Plot LL -> HH, switch to uptrend
    """
    if df.empty:
        return []

    pivots: List[Pivot] = []

    # Bootstrap trend
    state = "NEUTRAL"
    for i in range(len(df)):
        state = _initial_trend(df, i)
        if state != "NEUTRAL":
            break

    if state == "NEUTRAL":
        return []

    # State variables
    last_HL: Optional[float] = None  # Reference HL in uptrend
    last_LH: Optional[float] = None  # Reference LH in downtrend

    break_index: Optional[int] = None  # Where the break happened
    prev_high: Optional[float] = None  # For tracking lower-high confirmation
    prev_low: Optional[float] = None  # For tracking higher-low confirmation

    pending_HH: Optional[Pivot] = None
    pending_LL: Optional[Pivot] = None

    i = 0
    while i < len(df):
        row = df.iloc[i]
        high = float(row["high"])
        low = float(row["low"])
        date = pd.to_datetime(row["date"])

        # ============================================================
        # UPTREND STATE
        # ============================================================
        if state == "UP":
            # Initialize HL reference
            if last_HL is None:
                last_HL = low
                i += 1
                continue

            # Track higher lows - update HL reference
            if low > last_HL:
                last_HL = low
                i += 1
                continue

            # Break detected: low <= last_HL
            if low <= last_HL:
                break_index = i

                # Find HH from last <=3 candles including break candle
                start = max(0, i - 2)
                window = df.iloc[start:i + 1]
                hh_idx = window["high"].idxmax()
                hh_i = int(df.index.get_loc(hh_idx))
                hh_price = float(df.loc[hh_idx, "high"])

                pending_HH = Pivot(i=hh_i, date=pd.to_datetime(df.iloc[hh_i]["date"]),
                                   price=hh_price, kind="HH")

                # Switch to confirmation state
                state = "UP_CONFIRM_TOPPING"
                prev_high = high
                i += 1
                continue

        # ============================================================
        # UPTREND TOPPING CONFIRMATION STATE
        # ============================================================
        elif state == "UP_CONFIRM_TOPPING":
            # Wait for lower-high sequence
            if prev_high is not None:
                # Check if current high is lower than previous high
                if high < prev_high:
                    # Lower high confirmed! Now find LL
                    # LL from last <=3 candles including this confirmation candle
                    start = max(0, i - 2)
                    window = df.iloc[start:i + 1]
                    ll_idx = window["low"].idxmin()
                    ll_i = int(df.index.get_loc(ll_idx))
                    ll_price = float(df.loc[ll_idx, "low"])

                    pending_LL = Pivot(i=ll_i, date=pd.to_datetime(df.iloc[ll_i]["date"]),
                                       price=ll_price, kind="LL")

                    # Add both pivots
                    if pending_HH:
                        pivots.append(pending_HH)
                    pivots.append(pending_LL)

                    # Switch to downtrend
                    state = "DOWN"
                    last_LH = high  # Initialize LH reference
                    last_HL = None
                    pending_HH = None
                    pending_LL = None
                    prev_high = None
                    prev_low = None
                    break_index = None
                    i += 1
                    continue

                # Equal high or higher high - keep waiting
                prev_high = high
                i += 1
                continue

            prev_high = high
            i += 1
            continue

        # ============================================================
        # DOWNTREND STATE
        # ============================================================
        elif state == "DOWN":
            # Initialize LH reference
            if last_LH is None:
                last_LH = high
                i += 1
                continue

            # Track lower highs - update LH reference
            if high < last_LH:
                last_LH = high
                i += 1
                continue

            # Break detected: high >= last_LH
            if high >= last_LH:
                break_index = i

                # Find LL from last <=3 candles including break candle
                start = max(0, i - 2)
                window = df.iloc[start:i + 1]
                ll_idx = window["low"].idxmin()
                ll_i = int(df.index.get_loc(ll_idx))
                ll_price = float(df.loc[ll_idx, "low"])

                pending_LL = Pivot(i=ll_i, date=pd.to_datetime(df.iloc[ll_i]["date"]),
                                   price=ll_price, kind="LL")

                # Switch to confirmation state
                state = "DOWN_CONFIRM_BOTTOMING"
                prev_low = low
                i += 1
                continue

        # ============================================================
        # DOWNTREND BOTTOMING CONFIRMATION STATE
        # ============================================================
        elif state == "DOWN_CONFIRM_BOTTOMING":
            # Wait for higher-low sequence
            if prev_low is not None:
                # Check if current low is higher than previous low
                if low > prev_low:
                    # Higher low confirmed! Now find HH
                    # HH from last <=3 candles including this confirmation candle
                    start = max(0, i - 2)
                    window = df.iloc[start:i + 1]
                    hh_idx = window["high"].idxmax()
                    hh_i = int(df.index.get_loc(hh_idx))
                    hh_price = float(df.loc[hh_idx, "high"])

                    pending_HH = Pivot(i=hh_i, date=pd.to_datetime(df.iloc[hh_i]["date"]),
                                       price=hh_price, kind="HH")

                    # Add both pivots
                    if pending_LL:
                        pivots.append(pending_LL)
                    pivots.append(pending_HH)

                    # Switch to uptrend
                    state = "UP"
                    last_HL = low  # Initialize HL reference
                    last_LH = None
                    pending_HH = None
                    pending_LL = None
                    prev_high = None
                    prev_low = None
                    break_index = None
                    i += 1
                    continue

                # Equal low or lower low - keep waiting
                prev_low = low
                i += 1
                continue

            prev_low = low
            i += 1
            continue

        # NEUTRAL state - shouldn't reach here but safety
        i += 1

    # ============================================================
    # END OF DATA - Handle pending pivots that weren't confirmed
    # ============================================================
    if state == "UP_CONFIRM_TOPPING" and pending_HH:
        # We have a pending HH but no LL confirmation yet
        # Add the HH and try to find the most recent LL
        pivots.append(pending_HH)

        # Find LL from last few candles
        if len(df) >= 3:
            window = df.iloc[-3:]
            ll_idx = window["low"].idxmin()
            ll_i = int(df.index.get_loc(ll_idx))
            ll_price = float(df.loc[ll_idx, "low"])
            pending_LL = Pivot(i=ll_i, date=pd.to_datetime(df.iloc[ll_i]["date"]),
                               price=ll_price, kind="LL")
            pivots.append(pending_LL)

    elif state == "DOWN_CONFIRM_BOTTOMING" and pending_LL:
        # We have a pending LL but no HH confirmation yet
        # Add the LL and try to find the most recent HH
        pivots.append(pending_LL)

        # Find HH from last few candles
        if len(df) >= 3:
            window = df.iloc[-3:]
            hh_idx = window["high"].idxmax()
            hh_i = int(df.index.get_loc(hh_idx))
            hh_price = float(df.loc[hh_idx, "high"])
            pending_HH = Pivot(i=hh_i, date=pd.to_datetime(df.iloc[hh_i]["date"]),
                               price=hh_price, kind="HH")
            pivots.append(pending_HH)

    # Clean up consecutive same-kind pivots - keep only the most extreme
    # For consecutive HHs, keep the highest; for consecutive LLs, keep the lowest
    cleaned: List[Pivot] = []
    i = 0
    while i < len(pivots):
        current = pivots[i]

        # Look ahead for consecutive same-kind pivots
        j = i + 1
        same_kind_group = [current]
        while j < len(pivots) and pivots[j].kind == current.kind:
            same_kind_group.append(pivots[j])
            j += 1

        # Keep only the most extreme pivot from the group
        if current.kind == "HH":
            # Keep the highest HH
            best = max(same_kind_group, key=lambda p: p.price)
        else:  # LL
            # Keep the lowest LL
            best = min(same_kind_group, key=lambda p: p.price)

        cleaned.append(best)
        i = j  # Skip to next different kind

    # Final safety check: ensure strict alternation HH->LL->HH->LL
    final: List[Pivot] = []
    for p in cleaned:
        if not final:
            final.append(p)
        elif final[-1].kind != p.kind:
            final.append(p)
        else:
            # Same kind consecutive - keep the more extreme one
            if p.kind == "HH" and p.price > final[-1].price:
                final[-1] = p
            elif p.kind == "LL" and p.price < final[-1].price:
                final[-1] = p

    return final