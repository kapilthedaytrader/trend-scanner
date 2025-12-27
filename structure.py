from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd


@dataclass
class Pivot:
    i: int
    date: pd.Timestamp
    price: float
    kind: str  # "HH" or "LL"


def _window_start(i: int, k: int = 3) -> int:
    return max(0, i - (k - 1))


def _hh(df: pd.DataFrame, i: int, k: int = 3) -> Tuple[int, float]:
    s = _window_start(i, k)
    win = df.iloc[s:i + 1]
    j = win["high"].idxmax()
    return int(df.index.get_loc(j)), float(df.loc[j, "high"])


def _ll(df: pd.DataFrame, i: int, k: int = 3) -> Tuple[int, float]:
    s = _window_start(i, k)
    win = df.iloc[s:i + 1]
    j = win["low"].idxmin()
    return int(df.index.get_loc(j)), float(df.loc[j, "low"])


def _initial_trend(df: pd.DataFrame, i: int) -> str:
    # MA bootstrapping (your option 1.B)
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
    Builds HH/LL pivots using your rule engine:
    - Uptrend: HLs hold until low <= prev_HL => compute HH over last <=3 incl break.
              confirm with 1 lower-high (equal = neutral/wait),
              then compute LL over last <=3 incl confirm candle.
    - Downtrend: mirror logic with LH holds until high >= prev_LH.
    """
    if df.empty:
        return []

    pivots: List[Pivot] = []
    state = _initial_trend(df, 0)

    last_HL: Optional[float] = None
    last_LH: Optional[float] = None

    pending_HH: Optional[Pivot] = None
    pending_LL: Optional[Pivot] = None

    prev_high: Optional[float] = None
    prev_low: Optional[float] = None

    for i in range(len(df)):
        row = df.iloc[i]
        high = float(row["high"])
        low = float(row["low"])
        date = pd.to_datetime(row["date"])

        if state == "NEUTRAL":
            # bootstrap once MA condition becomes clear
            state = _initial_trend(df, i)
            prev_high, prev_low = high, low
            continue

        # -------------------------
        # UP state
        # -------------------------
        if state == "UP":
            # update HL reference: we keep raising HL when lows are higher
            if last_HL is None:
                last_HL = low
            else:
                # higher low -> raise HL; equal -> neutral (do nothing)
                if low > last_HL:
                    last_HL = low

            # break condition: low <= last_HL
            if last_HL is not None and low <= last_HL:
                # compute pending HH from last <=3 incl this candle
                hh_i, hh_price = _hh(df, i, k=3)
                pending_HH = Pivot(i=hh_i, date=pd.to_datetime(df.iloc[hh_i]["date"]), price=hh_price, kind="HH")

                # Now wait for 1 lower high confirmation (equal = neutral)
                # We do this by switching to a sub-state using pending_HH not None.
                state = "UP_WAIT_LH"
                prev_high = high
                continue

        # waiting for lower-high confirmation after HH
        if state == "UP_WAIT_LH":
            # Need: current high < previous candle high
            if prev_high is not None:
                if high == prev_high:
                    # equality neutral: keep waiting
                    prev_high = high
                    continue
                if high < prev_high:
                    # confirmed LH with 1 candle
                    ll_i, ll_price = _ll(df, i, k=3)
                    pending_LL = Pivot(i=ll_i, date=pd.to_datetime(df.iloc[ll_i]["date"]), price=ll_price, kind="LL")

                    # finalize HH -> LL
                    if pending_HH:
                        pivots.append(pending_HH)
                    pivots.append(pending_LL)

                    # switch to DOWN mode; set last_LH baseline to current high (start tracking LH)
                    state = "DOWN"
                    last_LH = high
                    last_HL = None
                    pending_HH = None
                    pending_LL = None
                    prev_high = high
                    prev_low = low
                    continue

            prev_high = high
            continue

        # -------------------------
        # DOWN state (mirror)
        # -------------------------
        if state == "DOWN":
            # update LH reference: we keep lowering LH when highs are lower
            if last_LH is None:
                last_LH = high
            else:
                if high < last_LH:
                    last_LH = high

            # break condition: high >= last_LH
            if last_LH is not None and high >= last_LH:
                # compute pending LL from last <=3 incl this candle
                ll_i, ll_price = _ll(df, i, k=3)
                pending_LL = Pivot(i=ll_i, date=pd.to_datetime(df.iloc[ll_i]["date"]), price=ll_price, kind="LL")

                state = "DOWN_WAIT_HH"
                prev_low = low
                continue

        # waiting for higher-low confirmation after LL (mirror: look for 1 higher low?)
        if state == "DOWN_WAIT_HH":
            # Mirror of lower-high confirm: to confirm reversal upward, we want 1 higher low
            if prev_low is not None:
                if low == prev_low:
                    prev_low = low
                    continue
                if low > prev_low:
                    # confirmed HL with 1 candle
                    hh_i, hh_price = _hh(df, i, k=3)
                    pending_HH = Pivot(i=hh_i, date=pd.to_datetime(df.iloc[hh_i]["date"]), price=hh_price, kind="HH")

                    # finalize LL -> HH (note: LL already implied by pending_LL)
                    if pending_LL:
                        pivots.append(pending_LL)
                    pivots.append(pending_HH)

                    state = "UP"
                    last_HL = low
                    last_LH = None
                    pending_HH = None
                    pending_LL = None
                    prev_high = high
                    prev_low = low
                    continue

            prev_low = low
            continue

        prev_high, prev_low = high, low

    # de-dup consecutive same-kind pivots (optional)
    cleaned: List[Pivot] = []
    for p in pivots:
        if cleaned and cleaned[-1].kind == p.kind and cleaned[-1].i == p.i:
            continue
        cleaned.append(p)

    return cleaned