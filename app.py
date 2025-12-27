from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles

from ibkr_worker import IBKRWorker, IBKRConfig
from analysis import add_indicators, predict_tomorrow
from render import render_chart_waves, render_chart_candlestick, render_chart_close, render_chart_png

app = FastAPI(title="IBKR Market Phase + Candle Prediction")

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.on_event("startup")
def startup():
    # Create worker and connect IBKR in its own thread/loop
    app.state.ibkr = IBKRWorker(
        IBKRConfig(host="127.0.0.1", port=7496, client_id=77, timeout=10.0)
    )
    app.state.ibkr.start()


@app.on_event("shutdown")
def shutdown():
    try:
        app.state.ibkr.stop()
    except Exception:
        pass


@app.get("/", response_class=HTMLResponse)
def home():
    return (BASE_DIR / "static" / "index.html").read_text(encoding="utf-8")


@app.get("/api/analyze")
def analyze(
    symbol: str = Query(...),
    exchange: str = Query("SMART"),
    currency: str = Query("USD"),
    duration: str = Query("2 Y"),
    use_rth: bool = Query(True),
):
    df = app.state.ibkr.fetch_daily(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        duration=duration,
        use_rth=use_rth,
    )
    df["symbol"] = symbol

    result = predict_tomorrow(df)

    return JSONResponse({
        "symbol": symbol,
        "phase": result.phase,
        "tomorrow_bias": result.tomorrow_bias,
        "reasons": result.reasons,
        "flags": result.flags,
        "bars": len(df),
        "last_date": str(df.iloc[-1]["date"]),
        "last_close": float(df.iloc[-1]["close"]),
    })


@app.get("/api/chart.png")
def chart_png(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    duration: str = "2 Y",
    use_rth: bool = True,
):
    """Default chart endpoint - backwards compatible"""
    df = app.state.ibkr.fetch_daily(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        duration=duration,
        use_rth=use_rth,
    )
    df["symbol"] = symbol

    df = add_indicators(df)
    result = predict_tomorrow(df)

    img = render_chart_png(df, result.phase, result.tomorrow_bias, result.flags)
    return Response(content=img, media_type="image/png")


@app.get("/api/chart_waves.png")
def chart_waves_png(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    duration: str = "2 Y",
    use_rth: bool = True,
):
    """Chart with structure waves (HH/LL pivots)"""
    df = app.state.ibkr.fetch_daily(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        duration=duration,
        use_rth=use_rth,
    )
    df["symbol"] = symbol

    df = add_indicators(df)
    result = predict_tomorrow(df)

    img = render_chart_waves(df, result.phase, result.tomorrow_bias, result.flags)
    return Response(content=img, media_type="image/png")


@app.get("/api/chart_candlestick.png")
def chart_candlestick_png(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    duration: str = "2 Y",
    use_rth: bool = True,
):
    """Chart with candlesticks"""
    df = app.state.ibkr.fetch_daily(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        duration=duration,
        use_rth=use_rth,
    )
    df["symbol"] = symbol

    df = add_indicators(df)
    result = predict_tomorrow(df)

    img = render_chart_candlestick(df, result.phase, result.tomorrow_bias, result.flags)
    return Response(content=img, media_type="image/png")


@app.get("/api/chart_close.png")
def chart_close_png(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    duration: str = "2 Y",
    use_rth: bool = True,
):
    """Chart with close price line"""
    df = app.state.ibkr.fetch_daily(
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        duration=duration,
        use_rth=use_rth,
    )
    df["symbol"] = symbol

    df = add_indicators(df)
    result = predict_tomorrow(df)

    img = render_chart_close(df, result.phase, result.tomorrow_bias, result.flags)
    return Response(content=img, media_type="image/png")