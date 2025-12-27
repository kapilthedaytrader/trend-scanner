from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class PredictionResult:
    phase: str
    tomorrow_bias: str
    reasons: List[str]
    flags: Dict[str, str]


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ema9"] = out["close"].ewm(span=9, adjust=False).mean()
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["sma50"] = out["close"].rolling(50).mean()
    out["sma200"] = out["close"].rolling(200).mean()
    out["atr14"] = (out["high"] - out["low"]).rolling(14).mean()
    return out


def candle_type(df: pd.DataFrame, i: int) -> str:
    """Inside bar / outside bar / long bar based on day i vs i-1."""
    if i < 1:
        return "N/A"
    h, l = df.loc[i, "high"], df.loc[i, "low"]
    ph, pl = df.loc[i - 1, "high"], df.loc[i - 1, "low"]

    inside = (h <= ph) and (l >= pl)
    outside = (h >= ph) and (l <= pl)

    rng = h - l
    atr = df.loc[i, "atr14"]
    long_bar = False
    if pd.notna(atr) and atr > 0:
        long_bar = rng >= 1.5 * atr  # heuristic; tune later

    if inside:
        return "InsideBar"
    if outside:
        return "OutsideBar"
    if long_bar:
        return "LongBar"
    return "Normal"


def classify_phase_simple(df: pd.DataFrame, lookback: int = 20) -> Tuple[str, List[str]]:
    """
    Practical first-pass classifier:
    - Advancing: higher-high/higher-low tendency + above EMA20
    - Declining: lower-high/lower-low tendency + below EMA20
    - Range/Topping/Bottoming: otherwise (with context vs recent trend)
    You can replace this with your exact Rule1/Rule2 actual/potential logic later.
    """
    reasons: List[str] = []
    n = len(df)
    if n < lookback + 5:
        return "Unknown", ["Not enough data"]

    w = df.iloc[-lookback:].copy()

    # Trend slope proxy
    slope = np.polyfit(np.arange(len(w)), w["close"].values, 1)[0]
    above_ema20 = df.iloc[-1]["close"] > df.iloc[-1]["ema20"]
    below_ema20 = df.iloc[-1]["close"] < df.iloc[-1]["ema20"]

    # HH/HL heuristic
    highs = w["high"].values
    lows = w["low"].values
    hh = np.sum(highs[1:] > highs[:-1])
    hl = np.sum(lows[1:] > lows[:-1])
    lh = np.sum(highs[1:] < highs[:-1])
    ll = np.sum(lows[1:] < lows[:-1])

    # Decide
    if slope > 0 and (hh + hl) > (lh + ll) and above_ema20:
        reasons.append("Close trend up and above EMA20")
        return "Advancing", reasons
    if slope < 0 and (lh + ll) > (hh + hl) and below_ema20:
        reasons.append("Close trend down and below EMA20")
        return "Declining", reasons

    # Range-like: decide topping vs bottoming by prior slope
    prior = df.iloc[-(lookback * 2):-lookback]
    if len(prior) >= 10:
        pslope = np.polyfit(np.arange(len(prior)), prior["close"].values, 1)[0]
        if pslope > 0:
            reasons.append("Stalling after uptrend → topping/range")
            return "ToppingRange", reasons
        if pslope < 0:
            reasons.append("Stalling after downtrend → bottoming/range")
            return "BottomingRange", reasons

    reasons.append("Choppy/range-like structure")
    return "Range", reasons


def range_type(df: pd.DataFrame, lookback: int = 15) -> Tuple[str, Dict[str, float]]:
    """
    Wide vs Tight: based on range size relative to ATR.
    """
    w = df.iloc[-lookback:]
    hi = w["high"].max()
    lo = w["low"].min()
    rng = hi - lo
    atr = df.iloc[-1]["atr14"]
    if pd.isna(atr) or atr == 0:
        return "Unknown", {"range_high": float(hi), "range_low": float(lo), "range": float(rng)}
    # heuristic thresholds
    typ = "Tight" if rng <= 1.2 * atr else "Wide"
    return typ, {"range_high": float(hi), "range_low": float(lo), "range": float(rng), "atr14": float(atr)}


def is_extended_from_ema9(df: pd.DataFrame, mult: float = 1.5) -> bool:
    """Extended away from EMA9 using ATR."""
    atr = df.iloc[-1]["atr14"]
    if pd.isna(atr) or atr == 0:
        return False
    dist = abs(df.iloc[-1]["close"] - df.iloc[-1]["ema9"])
    return dist >= mult * atr


def reversal_candle_hint(df: pd.DataFrame) -> bool:
    """
    Basic reversal hint:
    - doji-like body small relative to range OR long wick.
    (You can replace with your exact doji/hammer/reverse-hammer definitions.)
    """
    o, h, l, c = df.iloc[-1][["open", "high", "low", "close"]]
    rng = h - l
    if rng == 0:
        return False
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    doji_like = body <= 0.25 * rng
    wick_like = (upper_wick >= 0.45 * rng) or (lower_wick >= 0.45 * rng)
    return bool(doji_like or wick_like)


def predict_tomorrow(df_in: pd.DataFrame) -> PredictionResult:
    df = add_indicators(df_in)
    phase, preasons = classify_phase_simple(df)

    reasons: List[str] = []
    reasons.extend(preasons)

    flags: Dict[str, str] = {}
    i = len(df) - 1

    # 50SMA context
    if pd.notna(df.iloc[-1]["sma50"]):
        if df.iloc[-1]["close"] > df.iloc[-1]["sma50"]:
            flags["50SMA"] = "Above"
        elif df.iloc[-1]["close"] < df.iloc[-1]["sma50"]:
            flags["50SMA"] = "Below"
        else:
            flags["50SMA"] = "At"
    else:
        flags["50SMA"] = "N/A"

    # Daily bar type
    flags["DailyBarType"] = candle_type(df, i)

    # Prediction based on phase buckets
    tomorrow = "NoSignal"

    if phase in ["Advancing", "Declining"]:
        # “3–4 day follow-through after range break” needs a range-break detector;
        # starter: use recent high/low break of last 15 bars
        rt, rinfo = range_type(df, 15)
        rh, rl = rinfo.get("range_high"), rinfo.get("range_low")
        if rh is not None and rl is not None and len(df) > 16:
            prev_w = df.iloc[-16:-1]
            prev_hi = prev_w["high"].max()
            prev_lo = prev_w["low"].min()
            broke_up = df.iloc[-1]["close"] > prev_hi
            broke_dn = df.iloc[-1]["close"] < prev_lo
        else:
            broke_up = broke_dn = False

        if phase == "Advancing" and broke_up:
            reasons.append("Recent range break up → expect follow-through")
            tomorrow = "ContinuationLikely"
        elif phase == "Declining" and broke_dn:
            reasons.append("Recent range break down → expect follow-through selling")
            tomorrow = "ContinuationLikely"
        else:
            ext = is_extended_from_ema9(df)
            rev = reversal_candle_hint(df)
            if ext and rev:
                reasons.append("Extended from EMA9 + reversal hint → pause/reversal likely")
                tomorrow = "PauseOrReversalLikely"
            else:
                reasons.append("Phase intact → continuation bias")
                tomorrow = "ContinuationLikely"

    else:
        # Range logic: wide vs tight and high/low positioning
        rt, rinfo = range_type(df, 15)
        flags["RangeType"] = rt
        rh = rinfo.get("range_high")
        rl = rinfo.get("range_low")
        if rt == "Tight":
            reasons.append("Tight range → avoid unless fake breakout")
            tomorrow = "AvoidUnlessFakeBreak"
        elif rt == "Wide" and rh is not None and rl is not None:
            px = df.iloc[-1]["close"]
            # near edge heuristic
            near_high = px >= (rl + 0.8 * (rh - rl))
            near_low = px <= (rl + 0.2 * (rh - rl))
            rev = reversal_candle_hint(df)

            if near_high and rev:
                reasons.append("Wide range near high + confirmation hint → 1-day down bias")
                tomorrow = "OneDayDownBias"
            elif near_low and rev:
                reasons.append("Wide range near low + confirmation hint → 1-day up bias")
                tomorrow = "OneDayUpBias"
            else:
                reasons.append("Range but no edge confirmation → no strong prediction")
                tomorrow = "NoStrongPrediction"
        else:
            reasons.append("Range unclear → no strong prediction")
            tomorrow = "NoStrongPrediction"

    # Adjustments based on daily scenario
    bt = flags["DailyBarType"]
    if bt == "InsideBar":
        flags["ExecutionNote"] = "InsideBar → quick partials, do not aim full ATR"
    elif bt == "OutsideBar":
        flags["ExecutionNote"] = "OutsideBar → higher continuation odds, ATR more likely"
    elif bt == "LongBar":
        flags["ExecutionNote"] = "LongBar → next day often pause/retrace, pinpoint or skip"

    return PredictionResult(phase=phase, tomorrow_bias=tomorrow, reasons=reasons, flags=flags)