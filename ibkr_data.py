from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Optional

import pandas as pd
from ib_insync import IB, Stock, util

@dataclass
class IBKRConfig:
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 7
    timeout: float = 10.0


_ib: Optional[IB] = None
_ib_lock = Lock()


async def init_ibkr_async(config: IBKRConfig) -> None:
    """
    Initialize a single shared IB connection using async connect.
    Call from FastAPI async startup.
    """
    global _ib
    if _ib is None:
        _ib = IB()
    if not _ib.isConnected():
        await _ib.connectAsync(
            config.host,
            config.port,
            clientId=config.client_id,
            timeout=config.timeout
        )


def get_ib() -> IB:
    if _ib is None or not _ib.isConnected():
        raise RuntimeError("IBKR not connected. Ensure startup ran and TWS/IBG is running.")
    return _ib


def fetch_daily_ohlcv(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    duration: str = "2 Y",
    bar_size: str = "1 day",
    use_rth: bool = True,
) -> pd.DataFrame:
    ib = get_ib()

    # IBKR API calls should not overlap; keep them serialized
    with _ib_lock:
        contract = Stock(symbol, exchange, currency)
        ib.qualifyContracts(contract)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=use_rth,
            formatDate=1,
        )

    if not bars:
        raise RuntimeError("No bars returned. Check symbol/exchange/permissions/data subscriptions.")

    df = util.df(bars)
    df["date"] = pd.to_datetime(df["date"])
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    df = df.sort_values("date").reset_index(drop=True)
    return df