import asyncio
import threading
from dataclasses import dataclass
from typing import Optional, Any, Dict

import pandas as pd
from ib_insync import IB, Stock, util


@dataclass
class IBKRConfig:
    host: str = "127.0.0.1"
    port: int = 7496
    client_id: int = 77
    timeout: float = 10.0


class IBKRWorker:
    """
    Runs IBKR (ib_insync) in its own thread + its own event loop.
    FastAPI never awaits IBKR futures directly -> no loop mismatch.
    """
    def __init__(self, cfg: IBKRConfig):
        self.cfg = cfg
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ib: Optional[IB] = None
        self._ready = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="IBKRWorker", daemon=True)
        self._thread.start()
        # Wait for ready or fail fast
        if not self._ready.wait(timeout=20):
            raise RuntimeError("IBKRWorker did not start in time. Check TWS/IBG and port.")

    def _run(self) -> None:
        # Dedicated loop in this thread
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        async def init():
            self._ib = IB()
            await self._ib.connectAsync(
                self.cfg.host,
                self.cfg.port,
                clientId=self.cfg.client_id,
                timeout=self.cfg.timeout
            )
            self._ready.set()

        try:
            self._loop.run_until_complete(init())
            self._loop.run_forever()
        except Exception as e:
            # Surface startup failure
            self._ready.set()
            print(f"IBKRWorker init failed: {e!r}")
        finally:
            try:
                if self._ib and self._ib.isConnected():
                    self._ib.disconnect()
            except Exception:
                pass
            try:
                self._loop.close()
            except Exception:
                pass

    def stop(self) -> None:
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def _require(self) -> tuple[asyncio.AbstractEventLoop, IB]:
        if not self._loop or not self._ib or not self._ib.isConnected():
            raise RuntimeError("IBKRWorker not connected. Is TWS/IBG running? Correct port?")
        return self._loop, self._ib

    async def _fetch_daily_async(
        self,
        symbol: str,
        exchange: str,
        currency: str,
        duration: str,
        bar_size: str,
        use_rth: bool,
    ) -> pd.DataFrame:
        _, ib = self._require()
        contract = Stock(symbol, exchange, currency)

        # qualify async if available
        if hasattr(ib, "qualifyContractsAsync"):
            await ib.qualifyContractsAsync(contract)
        else:
            ib.qualifyContracts(contract)

        if hasattr(ib, "reqHistoricalDataAsync"):
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=use_rth,
                formatDate=1,
            )
        else:
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
            raise RuntimeError("No bars returned from IBKR. Check permissions/symbol/exchange.")

        df = util.df(bars)
        df["date"] = pd.to_datetime(df["date"])
        df = df[["date", "open", "high", "low", "close", "volume"]].copy()
        df = df.sort_values("date").reset_index(drop=True)
        return df

    def fetch_daily(
        self,
        symbol: str,
        exchange: str = "SMART",
        currency: str = "USD",
        duration: str = "2 Y",
        bar_size: str = "1 day",
        use_rth: bool = True,
    ) -> pd.DataFrame:
        """
        Thread-safe sync method called by FastAPI.
        Schedules coroutine on IBKR thread's loop, waits for result.
        """
        loop, _ = self._require()
        fut = asyncio.run_coroutine_threadsafe(
            self._fetch_daily_async(symbol, exchange, currency, duration, bar_size, use_rth),
            loop
        )
        return fut.result(timeout=30)
