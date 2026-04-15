"""
ta_server.py — Render TA Server
================================
FastAPI wrapper around TechnicalAnalysis_v9_6.py.

Endpoints:
  GET  /health      — keep-alive ping (returns 200 OK)
  POST /analyze     — run full 20-layer ICT pipeline for a symbol

Deploy on Render as a Web Service:
  Build command : pip install -r requirements_ta_server.txt
  Start command : uvicorn ta_server:app --host 0.0.0.0 --port $PORT
  Instance type : Standard (512 MB RAM minimum; 1 GB recommended)

Environment variables required on Render dashboard:
  TA_API_SECRET   — a secret token; must match TA_API_SECRET in fps.ms .env
"""

import asyncio
import base64
import io
import json
import logging
import math
import os
import time

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("ta_server")

# ── Secret token (optional but recommended) ──────────────────────────────────
TA_API_SECRET = os.getenv("TA_API_SECRET", "")

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="CryptoBot TA Server",
    description="Remote Technical Analysis pipeline — 20-Layer ICT/MOP/QUANT Stack",
    version="9.6",
)

# ── Lazy-load TechnicalAnalysis_v9_6 once on first request ───────────────────
_TA_LOADED = False
_TA_LOAD_ERROR = None
_run_ict_analysis = None
_build_result_messages = None
_generate_trade_chart = None
_quick_check_markets = None


def _ensure_ta():
    global _TA_LOADED, _TA_LOAD_ERROR
    global _run_ict_analysis, _build_result_messages, _generate_trade_chart, _quick_check_markets
    if _TA_LOADED:
        return True
    if _TA_LOAD_ERROR:
        raise RuntimeError(f"TechnicalAnalysis failed to load: {_TA_LOAD_ERROR}")
    try:
        log.info("[TA_SERVER] Loading TechnicalAnalysis_v9_6 …")
        import TechnicalAnalysis_v9_6 as _ta
        _run_ict_analysis      = _ta.run_ict_analysis
        _build_result_messages = _ta.build_result_messages
        _generate_trade_chart  = _ta.generate_trade_chart
        _quick_check_markets   = _ta.quick_check_markets
        _TA_LOADED = True
        log.info("[TA_SERVER] TechnicalAnalysis_v9_6 loaded successfully.")
        return True
    except Exception as e:
        _TA_LOAD_ERROR = str(e)
        log.error(f"[TA_SERVER] Failed to load TechnicalAnalysis_v9_6: {e}")
        raise


# ── JSON serializer — handles numpy types, bytes, sets ───────────────────────
class _SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        # numpy scalar types
        try:
            import numpy as np
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, np.bool_):
                return bool(obj)
        except ImportError:
            pass
        # Python built-ins
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        if isinstance(obj, io.BytesIO):
            return None   # strip — not serializable
        if math.isnan(obj) if isinstance(obj, float) else False:
            return None
        if math.isinf(obj) if isinstance(obj, float) else False:
            return None
        return super().default(obj)


def _safe_json_dumps(obj) -> str:
    return json.dumps(obj, cls=_SafeEncoder)


def _sanitize(obj):
    """Recursively make a dict/list safe for JSON by stripping non-serializable types."""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, tuple):
        return [_sanitize(v) for v in obj]
    try:
        import numpy as np
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            f = float(obj)
            return None if (math.isnan(f) or math.isinf(f)) else f
        if isinstance(obj, np.ndarray):
            return _sanitize(obj.tolist())
        if isinstance(obj, np.bool_):
            return bool(obj)
    except ImportError:
        pass
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    if isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    if isinstance(obj, set):
        return _sanitize(list(obj))
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    # For any other type, try to convert to string to avoid dropping data
    try:
        return str(obj)
    except Exception:
        return None


# ── Request / response models ─────────────────────────────────────────────────
class AnalyzeRequest(BaseModel):
    symbol: str
    market_type: str = "auto"
    verbose_mode: bool = False


# ── Helper: check secret header ───────────────────────────────────────────────
def _check_auth(x_ta_secret: str):
    if TA_API_SECRET and x_ta_secret != TA_API_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Keep-alive ping endpoint. fps.ms pings this every 10 minutes."""
    return {"status": "ok", "ts": int(time.time()), "ta_loaded": _TA_LOADED}


@app.post("/analyze")
async def analyze(
    req: AnalyzeRequest,
    x_ta_secret: str = Header(default=""),
):
    """
    Run the full 20-layer ICT/MOP/QUANT analysis pipeline.

    Returns JSON with:
      result        — full result dict (all layers, scalars, chart OHLCV data)
      _messages     — formatted Telegram messages (non-verbose)
      _messages_verbose — formatted Telegram messages (verbose)
      _chart_b64    — PNG chart encoded as base64 string (or null)
    """
    _check_auth(x_ta_secret)

    symbol = req.symbol.upper().strip()
    if not symbol or len(symbol) > 20:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    log.info(f"[TA_SERVER] /analyze {symbol} market={req.market_type}")
    t0 = time.time()

    try:
        _ensure_ta()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))

    try:
        # ── Run the full pipeline ─────────────────────────────────────────────
        result = await _run_ict_analysis(symbol, market_type=req.market_type)

        # ── Build formatted messages ──────────────────────────────────────────
        try:
            messages_normal  = _build_result_messages(result, verbose_mode=False)
            messages_verbose = _build_result_messages(result, verbose_mode=True)
        except Exception as _msg_err:
            log.warning(f"[TA_SERVER] build_result_messages failed: {_msg_err}")
            messages_normal  = []
            messages_verbose = []

        # ── Generate chart ────────────────────────────────────────────────────
        chart_b64 = None
        try:
            chart_buf = await asyncio.to_thread(_generate_trade_chart, result)
            if chart_buf is not None:
                chart_buf.seek(0)
                chart_b64 = base64.b64encode(chart_buf.read()).decode("utf-8")
        except Exception as _chart_err:
            log.warning(f"[TA_SERVER] generate_trade_chart failed: {_chart_err}")

        # ── Sanitize result for JSON ──────────────────────────────────────────
        safe_result = _sanitize(result)

        # ── Strip heavy OHLCV raw arrays to reduce payload size ──────────────
        # ohlcv_chart_full can be 5000+ rows — app.py only uses it for ma_closes_full
        # which is precomputed in chart_data; we keep it for fidelity but strip raw bytes
        for _big_key in ("ohlcv_raw", "_raw_candles"):
            safe_result.pop(_big_key, None)

        elapsed = round(time.time() - t0, 2)
        log.info(f"[TA_SERVER] /analyze {symbol} done in {elapsed}s")

        # Return via JSONResponse with custom encoder to handle edge cases
        payload = {
            "ok": True,
            "symbol": symbol,
            "market_type": req.market_type,
            "elapsed_s": elapsed,
            "result": safe_result,
            "_messages": messages_normal,
            "_messages_verbose": messages_verbose,
            "_chart_b64": chart_b64,
        }

        # Use raw json.dumps with SafeEncoder to avoid FastAPI's default encoder
        # rejecting numpy residuals that _sanitize() missed
        raw_json = _safe_json_dumps(payload)
        return JSONResponse(content=json.loads(raw_json))

    except HTTPException:
        raise
    except Exception as e:
        log.exception(f"[TA_SERVER] /analyze {symbol} FAILED: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {e}")


@app.get("/markets/{symbol}")
async def check_markets(symbol: str, x_ta_secret: str = Header(default="")):
    """Quick check whether a symbol has spot/futures markets available."""
    _check_auth(x_ta_secret)
    _ensure_ta()
    result = await _quick_check_markets(symbol.upper())
    return result


# ── Startup event — pre-warm TA module ───────────────────────────────────────
@app.on_event("startup")
async def _startup():
    log.info("[TA_SERVER] Starting up — pre-warming TechnicalAnalysis module…")
    try:
        await asyncio.to_thread(_ensure_ta)
    except Exception as e:
        log.error(f"[TA_SERVER] Pre-warm failed (will retry on first request): {e}")
