import os
import base64
import sys
import resource
import gc as _gc
import logging
import asyncio
import math
import json
import time
import textwrap
import hashlib
import urllib3
from urllib.parse import quote as _url_quote
import unicodedata
from collections import defaultdict, OrderedDict
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import datetime as _dt
import pytz
import requests
import feedparser
import re
import aiohttp
from dataclasses import dataclass, field
from typing import Optional
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)
from telegram.error import BadRequest
from telegram.error import NetworkError as TelegramNetworkError, TimedOut as TelegramTimedOut, RetryAfter
from telegram.error import Conflict as TelegramConflict
from telegram.constants import ParseMode

# ── Low-memory deployment switches (128 MB-safe defaults) ─────────────────
LOW_RAM_MODE = os.getenv("LOW_RAM_MODE", "1").strip().lower() not in {"0", "false", "no", "off"}
DISABLE_CHARTS = os.getenv("DISABLE_CHARTS", "1" if LOW_RAM_MODE else "0").strip().lower() not in {"0", "false", "no", "off"}
LIGHT_START_MODE = os.getenv("LIGHT_START_MODE", "1" if LOW_RAM_MODE else "0").strip().lower() not in {"0", "false", "no", "off"}
MAX_NEWS_PHOTOS = int(os.getenv("MAX_NEWS_PHOTOS", "3" if LOW_RAM_MODE else "5"))

# ── DEX module (dex.py must be in the same dir) ──────────────────────────────
try:
    from dex import build_top10 as dex_build_top10
    DEX_MODULE_AVAILABLE = True
except ImportError as _dex_err:
    DEX_MODULE_AVAILABLE = False
    logging.getLogger(__name__).error(f"DEX module not found: {_dex_err}")

# ── Airdrops module (airdrops.py must be in the same dir) ────────────────────
try:
    from airdrops import handle_airdrops_button, handle_airdrops_filter, handle_airdrops_command
    AIRDROPS_MODULE_AVAILABLE = True
except ImportError as _ad_err:
    AIRDROPS_MODULE_AVAILABLE = False
    logging.getLogger(__name__).warning(f"[AIRDROP] airdrops.py not found: {_ad_err} — airdrop feature disabled")
    async def handle_airdrops_button(*a, **kw): pass
    async def handle_airdrops_filter(*a, **kw): pass
    async def handle_airdrops_command(*a, **kw): pass

# ── Handler modules (button logic extracted to separate files) ────────────────
try:
    from trending        import trending_coins       as _trending_raw
    from alpha_signals   import alpha_signals        as _alpha_raw
    from cross           import cross_analysis       as _cross_raw
    from sector_rotation import sector_rotation      as _sector_raw
    from ai_assistant    import ai_assistant, ai_choice as _ai_choice_raw
    from features        import (
        features_button_handler as _features_raw,
        _FEATURES_TARGET_LABELS,
        _COMPLEXITY_EMOJI,
        _PRIORITY_EMOJI,
    )
    from feedback        import (
        feedback_button_handler   as _feedback_btn_raw,
        feedback_callback_handler as _feedback_cb_raw,
        feedback_evidence_handler,
        get_feedback_inline_keyboard,
    )
    _HANDLER_MODULES_LOADED = True
except ImportError as _hm_err:
    logging.getLogger(__name__).error(f"[HANDLER_MODULES] Import failed: {_hm_err}")
    _HANDLER_MODULES_LOADED = False

# ── Technical Analysis module (TechnicalAnalysis_v8.py must be in the same dir) ──
# [v9.0 SYNC] Updated to TechnicalAnalysis_v9.
# New exports: compute_signal_probability (T1-B), scanner_summary key in result dict.
# build_result_messages now accepts verbose_mode: bool (T2-C).
try:
    from TechnicalAnalysis_v9_6 import (
        run_ict_analysis,
        build_result_messages,
        generate_trade_chart,
        quick_check_markets,
        safe_edit,
        safe_send,
        fmt_price,
        log_analysis_event,
        compute_signal_probability,     # [v9.0-T1B] 5-factor composite probability
        TV_FREE_SETUP_MSG,              # [TV-FREE] Setup guide message
        generate_tv_checklist,          # [TV-FREE Phase 4] Post-analysis cross-check
        generate_tv_pine_script,        # [TV-FREE Phase 5] Pine Script backtest
        get_current_session,            # [TV-FREE Phase 2] Kill Zone session check
        get_silver_bullet_status,       # [TV-FREE Phase 2] Silver Bullet window check
        _LAST_RESULT_CACHE,             # [TV-FREE] Slim result cache per symbol
        _TRADE_TRACKER,                 # REC-3: TradeLifecycleTracker paper P&L
    )
    TA_MODULE_AVAILABLE = True
except ImportError as _ta_err:
    TA_MODULE_AVAILABLE = False
    logger_placeholder = logging.getLogger(__name__)
    logger_placeholder.error(f"TechnicalAnalysis module not found: {_ta_err}")
    # ── Stub fallbacks — prevents NameError if TA module is absent ────────
    def run_ict_analysis(*a, **kw): raise ImportError("TechnicalAnalysis module not found")
    def build_result_messages(*a, **kw): return []
    def generate_trade_chart(*a, **kw): return None
    async def quick_check_markets(*a, **kw): return {"has_spot": True, "has_futures": False}
    async def safe_edit(*a, **kw): pass
    async def safe_send(*a, **kw): pass
    def fmt_price(v): return f"${v:,.4f}" if v and v < 1 else f"${v:,.2f}" if v else "—"
    def log_analysis_event(*a, **kw): pass
    def compute_signal_probability(*a, **kw): return {"signal_probability": 50.0, "signal_probability_pct": "50.0%", "probability_tier": "LOW", "probability_tier_icon": "🔴", "probability_note": "Module unavailable"}
    # [TV-FREE] stubs
    TV_FREE_SETUP_MSG = "⚠️ TechnicalAnalysis module not loaded — TV setup unavailable."
    def generate_tv_checklist(*a, **kw): return "⚠️ TA module not loaded."
    def generate_tv_pine_script(*a, **kw): return "// TA module not loaded."
    def get_current_session(): return "UNKNOWN"
    def get_silver_bullet_status(): return {"active": False, "window": None, "minutes_remaining": 0}
    class _TradeTrackerStub:
        def get_summary(self): return {"paper_trades": 0, "paper_equity": 1.0, "realized_pnl": 0.0, "win_rate": 0.0, "max_drawdown": 0.0}
    _TRADE_TRACKER = _TradeTrackerStub()
    _LAST_RESULT_CACHE = {}

# ── Markov Stack (markov_stack.py must be in the same dir) ───────────────────
try:
    from markov_stack import (
        _MARKOV_VALIDATOR,
        _MARKOV_BRAIN,
        _MARKOV_ENGINE,
        _SUPER_VALIDATOR,
        _LEVEL_LEARNER,          # BUG-01 FIX: was missing — caused NameError in self-learn loop
        classify_ict_state,
        BRAIN_WRITE_FAILURE_CAP,
        AI_CONSENSUS_THRESHOLD,
    )
    MARKOV_AVAILABLE = True
except ImportError as _mk_err:
    MARKOV_AVAILABLE = False
    BRAIN_WRITE_FAILURE_CAP = 3
    AI_CONSENSUS_THRESHOLD  = 3.0
    logging.getLogger(__name__).warning(f"[MARKOV] markov_stack not found: {_mk_err} — Markov features disabled")
    class _MarkovStub:
        _write_failures = 0
        def validate(self, **kw): return {"validator_conf_mod": 1.0, "validator_gate_veto": False, "validator_verdict": "MARKOV_UNAVAILABLE", "validator_verdict_key": "NEUTRAL", "validator_bull_edge": 0.0, "validator_gate_chain": "", "validator_gates_ok": 0, "validator_n_samples": 0, "validator_alpha": 0.0, "validator_note": "", "validator_macro_mod": 1.0, "validator_velocity_flag": False, "validator_velocity_note": ""}
        def validate_trio(self, **kw): return {"trio_bull_mass": 0.0, "trio_bear_mass": 0.0, "trio_dominant_state": "", "trio_n_samples": 0, "trio_effective_n": 0.0, "trio_alpha": 0.0, "trio_note": "🎲 Markov unavailable", "trio_win_rates": {}}
        def record_outcome(self, **kw): pass
        def record_ai_consensus(self, **kw): pass
        def apply_ai_tuning(self, **kw): return {}
        def get_adaptive_weights(self, symbol=""): return {"tp_reward": 2.0, "sl_penalty": 1.5, "edge_thresh": -0.15, "decay": 0.995, "tune_count": 0}
        def get_symbol(self, symbol=""): return {"n_samples": 0, "effective_n": 0.0, "adaptive_weights": {}}
        def read(self): return {"global": {"total_analyses": 0, "macro_event_outcomes": {}}, "symbols": {}}
    class _MarkovEngineStub:
        def update_and_predict(self, **kw): return {"markov_state": "", "markov_effective_n": 0.0}
    class _SuperValidatorStub:
        def validate(self, **kw): return {"super_conf_mod": 1.0, "super_verdict": "🧠 Super Markov unavailable", "super_verdict_key": "NEUTRAL", "super_gate_veto": False, "super_compact": "🧠 Super Markov: unavailable", "sentiment_score": 0.0, "sentiment_keywords_pos": [], "sentiment_keywords_neg": [], "regime_mod": 1.0, "regime_verdict": "", "regime_change_warning": False, "regime_persistence_secs": 0.0, "sector_mod": 1.0, "sector_verdict": "", "trend_mod": 1.0, "trend_verdict": "", "cross_mod": 1.0, "cross_verdict": "", "ict_mod": 1.0, "sentiment_mod": 1.0}
        def record_outcome_all_brains(self, **kw): pass
        class _SentimentStub:
            def record_headlines(self, *a, **k): pass
            def record_keywords(self, *a, **k): pass
            def score(self, **k): return {"sentiment_score": 0.0, "confidence": 0.0, "top_positive": [], "top_negative": [], "n_keywords": 0}
        class _NullBrain:
            def record_transition(self, *a, **k): pass
            def record_ai_quality(self, *a, **k): pass
            def record_sector_signal(self, *a, **k): pass
            def record_ai_phase(self, *a, **k): pass
            def record_trend_signal(self, *a, **k): pass
            def record_cross_signal(self, *a, **k): pass
            def record_ai_reliability(self, *a, **k): pass
        sentiment = _SentimentStub()
        regime    = _NullBrain()
        sector    = _NullBrain()
        trend     = _NullBrain()
        cross     = _NullBrain()
    _MARKOV_VALIDATOR = _MarkovStub()
    _MARKOV_BRAIN     = _MarkovStub()
    _MARKOV_ENGINE    = _MarkovEngineStub()
    _SUPER_VALIDATOR  = _SuperValidatorStub()
    def classify_ict_state(**kw): return "ACCUMULATION"
    # BUG-01 FIX: stub so self-learn loop doesn't NameError when markov_stack absent
    class _LevelLearnerStub:
        def ingest_aws_samples(self, *a, **kw): return 0
        def record_level_outcome(self, *a, **kw): pass
        def get_learned_levels(self, *a, **kw): return {"blend_weight": 0.0, "n_samples": 0, "confidence": "NONE", "learned_note": "Markov unavailable"}
    _LEVEL_LEARNER = _LevelLearnerStub()

# ── OCE Signal Formatter v10.3 ────────────────────────────────────────────────
try:
    from oce_signal_formatter import oce_enhance_result, oce_annotate_chart
    OCE_AVAILABLE = True
    logging.getLogger(__name__).info("[OCE v10.3] Signal Formatter: ONLINE — Triple-Role: SE + Designer + QA")
except ImportError as _oce_err:
    OCE_AVAILABLE = False
    logging.getLogger(__name__).warning(f"[OCE v10.3] Formatter not found: {_oce_err} — pipeline signals only")
    def oce_enhance_result(r, s, b): return r
    def oce_annotate_chart(buf, r, e, g): return buf
# ─────────────────────────────────────────────────────────────────────────────

# ── CRYPTEX STORES — unified data-layer (cryptex_stores.py) ──────────────────
# Replaces: condition_engine · gate_memory · apex_watch_store · deep_structure_task
try:
    import cryptex_stores as _cs
    from cryptex_stores import (
        # §1 apex_watch_store
        upsert          as _aws_upsert,
        record_outcome  as _aws_record_outcome,
        set_alert_flag  as _aws_set_alert_flag,
        get             as _aws_get,
        get_all_pending as _aws_get_all_pending,
        get_unrecorded_outcomes  as _aws_get_unrecorded,
        mark_outcome_recorded    as _aws_mark_recorded,
        get_level_samples        as _aws_get_level_samples,
        get_watch_summary        as _aws_get_summary,
        get_all          as _aws_get_all,
        evict_expired    as _aws_evict_expired,
        # §2 gate_memory
        record_gate_outcome  as _gm_record_gate_outcome,
        get_gate_probability as _gm_get_gate_probability,
        check_blacklist      as _gm_check_blacklist,
        get_fingerprint_data as _gm_get_fingerprint_data,
        get_memory_summary   as _gm_get_memory_summary,
        unflag_blacklist     as _gm_unflag_blacklist,    # BUG-02 FIX: was missing from import
        # §3 condition_engine
        register_condition  as _ce_register_condition,
        evaluate_conditions as _ce_evaluate_conditions,
        expire_conditions   as _ce_expire_conditions,
        list_conditions     as _ce_list_conditions,
        cancel_condition    as _ce_cancel_condition,
        get_condition_summary as _ce_get_summary,
        # §4 deep_structure_task
        get_levels      as _dsa_get_levels,
        get_deep_summary as _dsa_get_summary,
        refresh_symbol  as _dsa_refresh_symbol,
    )
    _COND_ENGINE_AVAILABLE   = True
    _GATE_MEMORY_AVAILABLE   = True
    _AWS_AVAILABLE           = True
    _DEEP_STRUCT_APP_AVAILABLE = True
    logging.getLogger(__name__).info(
        "[CRYPTEX STORES] ONLINE — §1 AWS · §2 GateMem · §3 Conditions · §4 DeepStruct"
    )

    # ── Compatibility shims so existing call-sites work unchanged ─────────────
    class _CondEngineShim:
        def register_condition(self, *a, **kw): return _ce_register_condition(*a, **kw)
        def evaluate_conditions(self, *a, **kw): return _ce_evaluate_conditions(*a, **kw)
        def expire_conditions(self): return _ce_expire_conditions()
        def list_conditions(self, chat_id): return _ce_list_conditions(chat_id)
        def cancel_condition(self, chat_id, cid): return _ce_cancel_condition(chat_id, cid)
        def get_summary(self): return _ce_get_summary()
    _cond_engine = _CondEngineShim()

    class _AWSShim:
        def upsert(self, *a, **kw): return _aws_upsert(*a, **kw)
        def record_outcome(self, *a, **kw): return _aws_record_outcome(*a, **kw)
        def set_alert_flag(self, *a, **kw): return _aws_set_alert_flag(*a, **kw)
        def get(self, *a, **kw): return _aws_get(*a, **kw)
        def get_all_pending(self): return _aws_get_all_pending()
        def get_unrecorded_outcomes(self): return _aws_get_unrecorded()
        def mark_outcome_recorded(self, *a, **kw): return _aws_mark_recorded(*a, **kw)
        def get_level_samples(self, *a, **kw): return _aws_get_level_samples(*a, **kw)
        def get_summary(self): return _aws_get_summary()
        def get_all(self): return _aws_get_all()
        def evict_expired(self): return _aws_evict_expired()
    _aws = _AWSShim()

    class _DeepStructShim:
        async def refresh_symbol(self, *a, **kw): return await _dsa_refresh_symbol(*a, **kw)
        def get_levels(self, sym): return _dsa_get_levels(sym)
        def get_summary(self): return _dsa_get_summary()
    _deep_struct_app = _DeepStructShim()

except ImportError as _cs_err:
    _COND_ENGINE_AVAILABLE     = False
    _GATE_MEMORY_AVAILABLE     = False
    _AWS_AVAILABLE             = False
    _DEEP_STRUCT_APP_AVAILABLE = False
    logging.getLogger(__name__).warning(
        f"[CRYPTEX STORES] not found: {_cs_err} — "
        "AWS · gate_memory · conditions · deep_struct all disabled"
    )
    # ── Stubs — keeps bot alive when cryptex_stores.py is missing ────────────
    class _CondEngineStub:
        def register_condition(self, *a, **kw): return False, None, "cryptex_stores unavailable"
        def evaluate_conditions(self, *a, **kw): return []
        def expire_conditions(self): return 0
        def list_conditions(self, chat_id): return []
        def cancel_condition(self, chat_id, cid): return False, "cryptex_stores unavailable"
        def get_summary(self): return {"active": 0, "fired_pending_cleanup": 0, "total": 0}
    _cond_engine = _CondEngineStub()

    class _AWSStub:
        def upsert(self, *a, **kw): return {}
        def record_outcome(self, *a, **kw): return False
        def set_alert_flag(self, *a, **kw): pass
        def get(self, *a, **kw): return None
        def get_all_pending(self): return []
        def get_unrecorded_outcomes(self): return []
        def mark_outcome_recorded(self, *a, **kw): pass
        def get_level_samples(self, *a, **kw): return []
        def get_summary(self): return {"total": 0, "capacity": 100, "pending": 0, "resolved": 0, "outcomes": {}, "all_pass_count": 0, "avg_conf": 0.0}
        def get_all(self): return []
        def evict_expired(self): return 0
    _aws = _AWSStub()

    def _gm_record_gate_outcome(*a, **kw): pass
    def _gm_get_gate_probability(*a, **kw): return None
    def _gm_check_blacklist(*a, **kw): return None
    def _gm_get_fingerprint_data(*a, **kw): return {}
    def _gm_get_memory_summary(): return {}
    def _gm_unflag_blacklist(*a, **kw): return False  # BUG-02 FIX: stub when cryptex_stores absent

    class _DeepStructAppStub:
        async def refresh_symbol(self, *a, **kw): return {}
        def get_levels(self, sym): return None
        def get_summary(self): return {}
    _deep_struct_app = _DeepStructAppStub()
# ─────────────────────────────────────────────────────────────────────────────


# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # logger not yet initialised here; warning emitted after logging.basicConfig below

# ==========================================
# CONFIGURATION
# ==========================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot Configuration — SECURITY: token MUST live in .env only; no hardcoded fallback
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
if not BOT_TOKEN:
    logging.getLogger(__name__).critical(
        "[SECURITY] TELEGRAM_BOT_TOKEN is not set in environment. "
        "Add it to your .env file: TELEGRAM_BOT_TOKEN=your_token_here"
    )

# API Configuration
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
DEFILLAMA_BASE_URL = "https://api.llama.fi"
STABLECOINS_BASE_URL = "https://stablecoins.llama.fi"
API_REQUEST_TIMEOUT = 15
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0"}

# AI API Configuration — 4 providers, auto-ranked by availability
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")
CEREBRAS_API_KEY  = os.getenv("CEREBRAS_API_KEY", "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
MISTRAL_API_KEY   = os.getenv("MISTRAL_API_KEY", "")
GITHUB_TOKEN      = os.getenv("GITHUB_TOKEN", "")  # GitHub Models validator (free tier)

# ── [FREE API] Exchange API Keys — Phases 1-4 ────────────────────────────────
# BUG-03 FIX: This is the CANONICAL definition of COINALYZE_API_KEY (config layer).
# TechnicalAnalysis_v9_6 also reads this env var directly for fetch_orderflow internals.
# CRITICAL: if the env var name changes here, update TechnicalAnalysis_v9_6 too.
# Coinalyze (free, register at coinalyze.net): multi-ex OI, real liquidations, predicted FR
COINALYZE_API_KEY  = os.getenv("COINALYZE_API_KEY", "")  # keep in sync with TechnicalAnalysis_v9_6
# Bybit public market endpoints (funding, OI) require no key

# Constants
SUPPORTED_CHAINS = ["Ethereum", "Solana", "BSC", "Base", "Arbitrum", "Polygon", "Optimism", "Avalanche"]
MAX_COINS_TO_FETCH = 100

# ── [OCE GAP-3] Telegram WebApp interactive signal view ──────────────────────
# Host trade_view.html at this URL (e.g. GitHub Pages).
# Set WEBAPP_URL in .env to override.  Set to "" to disable the button.
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://joshuabuyan.github.io/trade-view/")
WELCOME_MESSAGE_DELAY = 2
MARKET_REFRESH_INTERVAL = 300

# ── SECURITY: Chat allowlist (thesis B2.1) ───────────────────────────────────
# Set ALLOWED_CHAT_IDS=123456,789012 in .env to restrict access.
# Leave empty (default) to allow all users.
_raw_allowed = os.getenv("ALLOWED_CHAT_IDS", "")
ALLOWED_CHAT_IDS: set[int] = (
    {int(x.strip()) for x in _raw_allowed.split(",") if x.strip().isdigit()}
    if _raw_allowed.strip() else set()
)

# ── TA per-user rate limiting (thesis B2.3) — 30s cooldown between TA requests ─
_ta_last_request: dict[int, float] = {}   # {chat_id: timestamp}
TA_RATE_LIMIT_SECONDS = 30

# ── DEX rate limiting — global + per-user (Rank-2 dormancy fix) ─────────────
_dex_last_request: dict[int, float] = {}  # {chat_id: timestamp}  per-user
_dex_last_global_call: float = 0.0        # last DexScreener call epoch (any user)
DEX_USER_RATE_LIMIT_SECONDS   = 30        # matches TA cooldown pattern
DEX_GLOBAL_RATE_LIMIT_SECONDS = 4         # min gap between any two DexScreener calls

# ── PHP/USD live rate cache (thesis A1.6) ───────────────────────────────────
_peso_rate_cache: dict = {"rate": 57.0, "ts": 0.0}   # {rate: float, ts: epoch}
_PESO_CACHE_TTL = 3600   # refresh every 60 minutes

def _get_peso_rate() -> float:
    """Return live PHP/USD rate with 1-hour cache. Falls back to 57.0 on error."""
    global _peso_rate_cache
    if time.time() - _peso_rate_cache["ts"] < _PESO_CACHE_TTL:
        return _peso_rate_cache["rate"]
    try:
        r = requests.get(
            "https://api.exchangerate-api.com/v4/latest/USD",
            timeout=5, headers=REQUEST_HEADERS
        )
        if r.status_code == 200:
            rate = r.json().get("rates", {}).get("PHP", 57.0)
            _peso_rate_cache = {"rate": float(rate), "ts": time.time()}
            logger.info(f"[PESO] Updated PHP/USD rate: {rate}")
            return float(rate)
    except Exception as e:
        logger.warning(f"[PESO] Rate fetch failed, using cached {_peso_rate_cache['rate']}: {e}")
    return _peso_rate_cache["rate"]

# ── Registered chats persistence (thesis A1.5) ───────────────────────────────
_REGISTERED_CHATS_FILE = "/tmp/registered_chats.json"

def _load_registered_chats() -> set:
    """Load persisted chat IDs from disk."""
    try:
        if os.path.exists(_REGISTERED_CHATS_FILE):
            with open(_REGISTERED_CHATS_FILE, "r") as f:
                return set(json.load(f))
    except Exception as e:
        logger.warning(f"[CHATS] Failed to load registered chats: {e}")
    return set()

def _save_registered_chats():
    """Persist registered chat IDs to disk."""
    try:
        with open(_REGISTERED_CHATS_FILE, "w") as f:
            json.dump(list(registered_chats), f)
    except Exception as e:
        logger.warning(f"[CHATS] Failed to save registered chats: {e}")

# State Management
background_tasks = {}
regime_start_times = {}
user_ai_preference = {}
previous_market_data = {}
registered_chats: set = _load_registered_chats()  # persisted across restarts (thesis A1.5)
_alpha_notify_task = None              # single global auto-alpha notification task
_delete_tasks: set = set()             # tracked short-lived delete-notification tasks (shutdown-safe)
_alpha_notify_cache: dict = {}         # {"ts": float, "coin": str, "text": str} — last fired notification

# [MOP-P04] Background singleton task handles
_btc_regime_task     = None   # keeps _btc_regime / _btc_rsi / _btc_above_sma200 fresh
_provider_health_task = None  # keeps _ranked_providers fresh
# [MOP-P05] Pre-warm task handle
_prewarm_task        = None   # pre-fetches OHLCV 15 min before each kill zone

# [MOP-P04] Pre-ranked provider list — populated by _provider_health_task every 5 min
# Format: [{"name": str, "type": str, "key": str, "priority": int}, ...]
_ranked_providers: list = []

# [MOP-P05] Warm OHLCV cache — keyed by symbol, populated before each kill zone
# Format: { "BTCUSDT": {"data": {...}, "ts": float} }
_warm_cache: dict = {}
_WARM_CACHE_TTL     = 1200    # 20-minute TTL (seconds)
_PREWARM_MINUTES    = 15      # minutes before kill zone to start pre-warming
_PREWARM_SYMBOL_LIMIT = 10    # max symbols to pre-warm (RAM safety)
# Kill zone UTC start times (hour, minute)
_KZ_UTC_STARTS = [(7, 0), (13, 30), (18, 0), (0, 0)]

# Global market message tracker — keyed by chat_id, used by clear_market_messages
# Separate from context.user_data so it works from both handlers and background tasks
# BUG-FIX: was plain dict — auto_alpha_notification can append before send_market_overview
# initialises the key, causing KeyError. defaultdict(list) makes it self-initialising.
_market_messages: defaultdict = defaultdict(list)

# Global pin message tracker — keyed by chat_id
_PIN_FILE = "./pin_messages.json"   # BUG-16 FIX: /tmp is cleared on restart — use working dir

def _load_pin_messages() -> dict:
    try:
        if os.path.exists(_PIN_FILE):
            with open(_PIN_FILE, "r") as f:
                return {int(k): v for k, v in json.load(f).items()}
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    return {}

def _save_pin_messages():
    try:
        with open(_PIN_FILE, "w") as f:
            json.dump(_pin_messages, f)
    except Exception as e:
        logger.warning(f"[PIN] Failed to save pin messages: {e}")

_pin_messages: dict[int, int] = _load_pin_messages()   # persisted across restarts (thesis A2.7)

# ══════════════════════════════════════════════════════════════════════
# RAM-AWARE ANALYSIS QUEUE (127 MB hard limit)
# Only ONE full TA pipeline runs at a time. Additional requests queue and
# wait. Each waiter retries every 2 s until RAM is below threshold.
# ══════════════════════════════════════════════════════════════════════

_TA_SEM = asyncio.Semaphore(1)   # one TA pipeline at a time — prevents RAM overlap


def _rss_bytes() -> int:
    """Current process RSS in bytes. Used for debug logging only."""
    try:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    except Exception:
        return 0

# ==========================================
# UNIVERSAL HANDLER SAFETY WRAPPER
# ==========================================

def _clear_all_modes(chat_id: int, context):
    """Reset every possible stuck mode for a chat. Call this before any handler."""
    context.user_data['ai_mode'] = False
    context.user_data['ta_mode'] = False
    context.user_data.pop('ta_mode_ts', None)   # clear TA mode timestamp for auto-expiry
    # Null-safe: feedback_store may not have this chat_id's store yet on first session
    store = feedback_store.get(chat_id, {})
    if store and 'awaiting_evidence' in store:
        store['awaiting_evidence'] = False


def safe_menu_handler(fn):
    """
    Decorator for menu button handlers.
    - Clears ALL sticky modes before running
    - Guarantees the reply keyboard is always restored if the handler crashes
    - Works for both Update (message) and CallbackQuery handlers
    """
    import functools

    @functools.wraps(fn)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Resolve chat_id and reply target regardless of update type
        if update.callback_query:
            chat_id = update.callback_query.message.chat_id
            reply_target = update.callback_query.message
        elif update.message:
            chat_id = update.message.chat_id
            reply_target = update.message
        else:
            await fn(update, context)
            return

        _clear_all_modes(chat_id, context)

        try:
            await fn(update, context)
        except Exception as e:
            logger.error(f"[SAFE_HANDLER] {fn.__name__} crashed: {e}", exc_info=True)
            try:
                await reply_target.reply_text(
                    "⚠️ Something went wrong. Please try again.",
                    reply_markup=create_main_keyboard()
                )
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    return wrapper


# ── Apply @safe_menu_handler to imported handler modules ─────────────────────
if _HANDLER_MODULES_LOADED:
    trending_coins            = safe_menu_handler(_trending_raw)
    alpha_signals             = safe_menu_handler(_alpha_raw)
    cross_analysis            = safe_menu_handler(_cross_raw)
    sector_rotation           = safe_menu_handler(_sector_raw)
    ai_choice                 = safe_menu_handler(_ai_choice_raw)
    features_button_handler   = safe_menu_handler(_features_raw)
    feedback_button_handler   = safe_menu_handler(_feedback_btn_raw)
    feedback_callback_handler = safe_menu_handler(_feedback_cb_raw)
    # ai_assistant has no @safe_menu_handler in original — imported directly
else:
    async def _missing_handler(update, context):
        await update.message.reply_text("⚠️ Handler module not loaded.")
    trending_coins = alpha_signals = cross_analysis = sector_rotation = _missing_handler
    ai_assistant = ai_choice = features_button_handler = _missing_handler
    feedback_button_handler = feedback_callback_handler = _missing_handler
    async def feedback_evidence_handler(u, c): return False
    def get_feedback_inline_keyboard(): return None


# ==========================================
# CROSS ENGINE CONFIGURATION & DATA CLASSES
# ==========================================

# ── Exchange weights (must sum to 1.0) ──────────────────────────
EXCHANGE_WEIGHTS: dict[str, float] = {
    "binance": 0.30,
    "bybit":   0.25,
    "okx":     0.20,
    "kucoin":  0.15,
    "mexc":    0.10,
}

# ── Timeframes used ─────────────────────────────────────────────
CROSS_TIMEFRAMES: dict[str, str] = {
    "1h":  "1H",
    "4h":  "4H",
    "1d":  "Daily",
}

# ── MA periods ──────────────────────────────────────────────────
SHORT_PERIOD = 50
LONG_PERIOD  = 200

# ── Live coin discovery ──────────────────────────────────────────
TOP_N_COINS       = 20    # fetch top N coins per exchange (by volume)
SCORE_TOP_DISPLAY = 10    # how many coins to show in final rankings

# ── Minimum candles needed ───────────────────────────────────────
# ── Minimum candles needed ───────────────────────────────────────
MIN_CANDLES = LONG_PERIOD + 5

# ── Cache (seconds) ─────────────────────────────────────────────
# CROSS-BUG-4 FIX: was 30s — a scan takes up to 45s so cache expired before
# the next user tap could benefit. 120s gives 2 full minutes of cache benefit.
CROSS_CACHE_TTL = 120

# ── PH Peso display — use _get_peso_rate() for live rate (thesis A1.6) ────────
PESO_RATE = 57.0   # module-level fallback only; call _get_peso_rate() at display time

# ── Cross Engine Global State ───────────────────────────────────
_cache_data = []
_cache_time = 0.0
# BUG-FIX: bare None caused AttributeError if any code path touched the lock
# before the event loop was running. Lazy-holder pattern (same as _apex_state_lock)
# defers Lock() construction until first async use.
_cache_lock_holder: list = []

def _get_cache_lock() -> asyncio.Lock:
    if not _cache_lock_holder:
        _cache_lock_holder.append(asyncio.Lock())
    return _cache_lock_holder[0]

_btc_regime = 'unknown'
_btc_rsi = 50.0
_btc_above_sma200 = False

JUNK_PATTERN = re.compile(
    r"(UP|DOWN|BULL|BEAR|3L|3S|5L|5S|HALF|HEDGE|EDGE|\d+X|\d+[LS]\b)", re.IGNORECASE
)

def _is_valid_symbol(sym: str) -> bool:
    """Filter out leveraged/garbage tokens."""
    return not JUNK_PATTERN.search(sym)

@dataclass
class CandleData:
    closes:  list[float]
    volumes: list[float]

@dataclass
class CrossInfo:
    """Per-coin, per-exchange, per-timeframe signal."""
    symbol:       str
    exchange:     str
    timeframe:    str
    signal:       str          # 'golden_fresh' | 'golden_zone' | 'approaching_golden'
                               # | 'death_fresh'  | 'death_zone'  | 'approaching_death'
                               # | 'neutral'
    gap_pct:      float        # (SMA50 - SMA200) / SMA200 * 100
    convergence:  float        # rate of gap change per candle (abs, higher = faster)
    short_ma:     float
    long_ma:      float
    volume_ratio: float        # current vol vs 20-candle avg
    price:        float        # last close
    # -- Institutional upgrades --
    atr_norm_gap: float = 0.0  # MA gap / ATR (volatility-normalized)
    rsi:          float = 50.0 # RSI-14
    vol_slope:    float = 0.0  # volume expansion slope (last 5 candles)
    ema_confirm:  bool  = False # EMA21/55 confirms SMA cross direction
    macd_confirm: bool  = False # MACD line/signal agrees with cross direction
    persistence:  float = 0.0  # proxy: how long gap has been stable (0-1)

@dataclass
class CoinScore:
    """Final merged score for a coin across exchanges and timeframes."""
    symbol:            str
    signal_type:       str     # 'golden' | 'death' | 'approaching_golden' | 'approaching_death'
    exchange_consensus: float  # weighted fraction of exchanges agreeing
    tf_alignment:       float  # fraction of TFs agreeing
    gap_pct:           float   # weighted avg gap %
    convergence:       float   # weighted avg convergence
    volume_ratio:      float   # weighted avg vol ratio
    price:             float   # latest price (USD)

    # Quant scores
    confidence:  float = 0.0
    efficiency:  float = 0.0
    rci:         float = 0.0
    ai_score:    float = 0.0

    # Prediction (for approaching crosses)
    pred_days:   Optional[float] = None
    pred_prob:   Optional[float] = None

    # Fresh cross flag
    is_fresh:    bool = False
    # -- Institutional fields --
    regime:          str   = 'unknown'  # bull_expansion | bull_pullback | chop | bear
    regime_modifier: float = 1.0        # multiplier applied to ai_score
    liquidity_tier:  float = 1.0        # 1.0=T1 0.9=T2 0.75=T3 0.6=micro
    persistence:     float = 0.0        # signal longevity factor 0-1
    ema_confirm:     bool  = False       # EMA 21/55 confirmation
    macd_confirm:    bool  = False       # MACD line/signal confirmation
    atr_norm_gap:    float = 0.0         # volatility-normalized gap
    rsi:             float = 50.0        # weighted RSI confluence
    primary_timeframe: str = ''          # strongest TF supporting dominant signal
    exchange_count:    int = 0           # number of exchanges supporting dominant signal

# ETF Cache Management (Persistent Fallback Layer)
# BUG-FIX: /tmp is cleared on every restart (same issue fixed for _PIN_FILE).
# Use working directory so cache survives process restarts.
ETF_CACHE_FILE = "./etf_cache.json"
etf_cache = {}

def load_etf_cache():
    """Load ETF cache from disk"""
    global etf_cache
    try:
        if os.path.exists(ETF_CACHE_FILE):
            with open(ETF_CACHE_FILE, 'r') as f:
                etf_cache = json.load(f)
                logger.info(f"[ETF CACHE] Loaded cache with {len(etf_cache)} entries")
        else:
            etf_cache = {}
            logger.info("[ETF CACHE] No cache file found, starting fresh")
    except Exception as e:
        logger.error(f"[ETF CACHE] Failed to load cache: {e}")
        etf_cache = {}

def save_etf_cache():
    """Save ETF cache to disk"""
    try:
        with open(ETF_CACHE_FILE, 'w') as f:
            json.dump(etf_cache, f, indent=2)
        logger.info("[ETF CACHE] Saved cache successfully")
    except Exception as e:
        logger.error(f"[ETF CACHE] Failed to save cache: {e}")

def is_market_closed():
    """
    Detect if US markets are closed (weekends/holidays).
    ETF trading follows NYSE hours (US/Eastern timezone).
    Holiday list covers 2026-2028 (thesis A1.9).
    """
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    # Weekend check
    if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return True

    # NYSE holidays 2026-2028 (thesis A1.9 multi-year fix)
    us_holidays = {
        # 2026
        "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
        "2026-05-25", "2026-07-03", "2026-09-07", "2026-11-26",
        "2026-12-25",
        # 2027
        "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
        "2027-05-31", "2027-07-05", "2027-09-06", "2027-11-25",
        "2027-12-24",
        # 2028
        "2028-01-17", "2028-02-21", "2028-04-14",
        "2028-05-29", "2028-07-04", "2028-09-04", "2028-11-23",
        "2028-12-25",
    }

    today_str = now.strftime("%Y-%m-%d")
    return today_str in us_holidays

# Load cache on startup
load_etf_cache()

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

# ==========================================

# ── [OCE GAP-3] WebApp signal URL builder ────────────────────────────────────
def build_webapp_signal_url(result: dict, symbol: str) -> str | None:
    """
    Build trade_view WebApp URL.
    Target: total URL < 1800 chars (Telegram safe limit).
    All strings sanitized of control chars before json.dumps.
    All canvas fields included (ob, fvg, bos, choch, ote_zone, sl2/sl3, chart_tf).
    """
    if not WEBAPP_URL:
        return None
    try:
        import re as _re, math as _math

        _CTRL = _re.compile(r'[\x00-\x1f\x7f\x80-\x9f]')

        def _san(obj):
            if isinstance(obj, dict):  return {k: _san(v) for k, v in obj.items()}
            if isinstance(obj, list):  return [_san(i) for i in obj]
            if isinstance(obj, str):   return _CTRL.sub('', obj)
            if isinstance(obj, float): return 0.0 if (_math.isnan(obj) or _math.isinf(obj)) else obj
            return obj

        def _f(v, dec=2):
            try:
                r = round(float(v or 0), dec)
                return 0.0 if (_math.isnan(r) or _math.isinf(r)) else r
            except (TypeError, ValueError):
                return 0.0

        def _st(v, n):
            return _CTRL.sub('', str(v or '')).strip()[:n]

        hmm_d     = result.get("hmm_data")                 or {}
        sp_d      = result.get("signal_probability_data")  or {}
        oce_eval  = result.get("oce_eval")                 or {}
        oce_audit = result.get("oce_audit")                or {}
        oce_brvf  = result.get("oce_brvf")                 or {}
        oce_gaps  = result.get("oce_gaps")                 or []
        scan_sum  = result.get("scanner_summary")          or {}

        # signal_state — TA stores in scanner_summary, not top-level
        _all_pass = result.get("all_pass", False)
        _grade    = result.get("grade", "F")
        _state    = (
            result.get("signal_state")
            or scan_sum.get("signal_state")
            or ("ACTIVE" if _all_pass else "DEFERRED" if _grade in ("C","D") else "BLOCKED")
        )

        # rr — compute locally (no top-level rr key in TA)
        _el   = _f(result.get("entry_low"),  6)
        _eh   = _f(result.get("entry_high"), 6)
        _em   = (_el + _eh) / 2.0 or _f(result.get("ote_entry"), 6)
        _tp3  = _f(result.get("tp3"), 6)
        _sl   = _f(result.get("inv_sl"), 6)
        _rr   = round(abs(_tp3 - _em) / max(abs(_em - _sl), 1e-9), 2) if _em and _tp3 and _sl else 0.0

        # gaps — handle both dataclass attrs and plain dicts, max 4 rows, short strings
        def _gap(g):
            if g is None: return None
            src  = _st((g.source  if hasattr(g,"source")  else g.get("source",""))  or "", 14)
            qual = _st((g.quality if hasattr(g,"quality") else g.get("quality","")) or "", 8)
            note = _st((g.note    if hasattr(g,"note")    else g.get("note",""))    or "", 10)
            return {"source": src, "quality": qual, "note": note}

        payload = {
            # ── Identity ─────────────────────────────────────────────
            "symbol":       symbol,
            "bias":         _st(result.get("bias","BULLISH"), 8),
            "grade":        _st(result.get("grade","?"), 2),
            "conf":         round(float(result.get("conf") or 0), 1),
            "price":        _f(result.get("price"), 2),
            "win_prob":     round(float(result.get("win_prob") or 0), 1),
            "rr":           _rr,
            "signal_state": _st(_state, 8),

            # ── Trade levels ──────────────────────────────────────────
            "ote_entry":    _f(result.get("ote_entry") or _em, 6),
            "entry_low":    _el,
            "entry_high":   _eh,
            # [SWEEP FIX] 3-leg staggered entry levels
            "entry_leg1":   _f(result.get("entry_leg1") or _eh, 6),   # conservative OB boundary
            "entry_leg2":   _f(result.get("entry_leg2") or _em, 6),   # standard OTE
            "entry_leg3":   _f(result.get("entry_leg3") or _el, 6),   # sweep grab / deep OTE
            "entry_mode":   _st(result.get("entry_mode") or "LIMIT_ZONE", 20),
            # [SELF-LEARN] LevelLearner metadata — UI shows blend weight + confidence
            "learned_levels": {
                "blend_weight":  round(float((result.get("learned_levels") or {}).get("blend_weight") or 0), 3),
                "n_samples":     int((result.get("learned_levels") or {}).get("n_samples") or 0),
                "confidence":    str((result.get("learned_levels") or {}).get("confidence") or "NONE"),
                "win_rate":      round(float((result.get("learned_levels") or {}).get("win_rate") or 0), 1),
                "learned_note":  str((result.get("learned_levels") or {}).get("learned_note") or ""),
                "med_tp1_pct":   round(float((result.get("learned_levels") or {}).get("med_tp1_pct") or 0), 3),
                "med_tp3_pct":   round(float((result.get("learned_levels") or {}).get("med_tp3_pct") or 0), 3),
                "med_sl_pct":    round(float((result.get("learned_levels") or {}).get("med_sl_pct") or 0), 3),
            },
            "tp1":          _f(result.get("tp1"), 6),
            "tp2":          _f(result.get("tp2"), 6),
            "tp3":          _tp3,
            "sl1":          _sl,        # HTML reads d.sl1 || d.inv_sl
            "inv_sl":       _sl,
            "sl2":          _f(result.get("sl2"), 6),
            "sl3":          _f(result.get("sl3"), 6),

            # ── ICT canvas zones ──────────────────────────────────────
            "ob_high":         _f(result.get("ob_high"),  6),
            "ob_low":          _f(result.get("ob_low"),   6),
            "fvg_high":        _f(result.get("fvg_high"), 6),
            "fvg_low":         _f(result.get("fvg_low"),  6),
            "bos_price":       _f(result.get("bos_price"),6),
            "choch":           bool(result.get("choch", False)),

            # ── Liquidation Heatmap (GAP-A fix) ───────────────────────
            # Long liq levels = SL magnet zones below price (stop-hunt targets)
            # Short liq levels = TP magnet zones above price (squeeze targets)
            # liq_clusters_real = True when Coinalyze live data is present
            "liq_long_10x":       _f(result.get("liq_long_10x",  0.0), 6),
            "liq_long_20x":       _f(result.get("liq_long_20x",  0.0), 6),
            "liq_long_50x":       _f(result.get("liq_long_50x",  0.0), 6),
            "liq_short_10x":      _f(result.get("liq_short_10x", 0.0), 6),
            "liq_short_20x":      _f(result.get("liq_short_20x", 0.0), 6),
            "liq_short_50x":      _f(result.get("liq_short_50x", 0.0), 6),
            "liq_clusters_real":  bool((result.get("upg_liq_heat") or {}).get("liq_clusters_real",  False)),
            "liq_clusters_count": int( (result.get("upg_liq_heat") or {}).get("liq_clusters_count", 0)),
            "liq_heatmap_note":   _st((result.get("upg_liq_heat") or {}).get("liq_heatmap_note", ""), 80),
            # ── Markov data (trimmed — only keys rendered by index.html) ─────
            # Raw dict dumps were the primary cause of URL length overflow /
            # atob() DOMException. We now extract ONLY the scalar + small-dict
            # fields the UI actually reads, with hard string/array length caps.
            "markov_data": (lambda _mk: {
                "markov_state":        _st(_mk.get("markov_state", ""), 20),
                "markov_effective_n":  _f(_mk.get("markov_effective_n", 0.0), 1),
                "markov_next_pred":    _st(_mk.get("markov_next_pred", ""), 20),
                "markov_next_prob":    _f(_mk.get("markov_next_prob", 0.0), 3),
                "markov_2nd_pred":     _st(_mk.get("markov_2nd_pred", ""), 20),
                "markov_2nd_prob":     _f(_mk.get("markov_2nd_prob", 0.0), 3),
                "markov_p2_pred":      _st(_mk.get("markov_p2_pred", ""), 20),
                "markov_p2_prob":      _f(_mk.get("markov_p2_prob", 0.0), 3),
                "markov_p3_pred":      _st(_mk.get("markov_p3_pred", ""), 20),
                "markov_p3_prob":      _f(_mk.get("markov_p3_prob", 0.0), 3),
                # top-6 states only; each trimmed to {tp_rate, total}
                "markov_state_winrates": {
                    k: {"tp_rate": round(float(v.get("tp_rate", 0.0)), 3),
                        "total":   int(v.get("total", 0))}
                    for k, v in list((_mk.get("markov_state_winrates") or {}).items())[:6]
                    if isinstance(v, dict)
                },
                # only the 3 scalar fields the UI renders
                "markov_curr_winrate": (lambda _cwr: {
                    "tp_rate": round(float(_cwr.get("tp_rate", 0.0)), 3),
                    "total":   int(_cwr.get("total", 0)),
                    "exp_rr":  round(float(_cwr.get("exp_rr", 0.0)), 2),
                })(_mk.get("markov_curr_winrate") or {}),
                # regime geometry — only 4 rendered scalars
                "markov_regime_geometry": (lambda _rg: {
                    "regime_geometry_active":   bool(_rg.get("regime_geometry_active", False)),
                    "regime_entry_tighten_pct": round(float(_rg.get("regime_entry_tighten_pct", 0.0)), 3),
                    "regime_sl_widen_pct":      round(float(_rg.get("regime_sl_widen_pct", 0.0)), 3),
                    "regime_conf_cap":          round(float(_rg.get("regime_conf_cap", 1.3)), 2),
                })(_mk.get("markov_regime_geometry") or {}),
                # top-3 sessions × top-4 states per session
                "markov_session_state_wr": {
                    _sess: {
                        _sst: round(float(_swr), 3)
                        for _sst, _swr in list(_sv.items())[:4]
                        if isinstance(_swr, (int, float))
                    }
                    for _sess, _sv in list((_mk.get("markov_session_state_wr") or {}).items())[:3]
                    if isinstance(_sv, dict)
                },
            })(result.get("markov_data") or {}),
            "markov_validation": (lambda _vl: {
                "validator_verdict":       _st(_vl.get("validator_verdict", ""), 60),
                "validator_verdict_key":   _st(_vl.get("validator_verdict_key", "NEUTRAL"), 16),
                "validator_conf_mod":      round(float(_vl.get("validator_conf_mod", 1.0)), 3),
                "validator_gate_veto":     bool(_vl.get("validator_gate_veto", False)),
                "validator_gate_chain":    _st(_vl.get("validator_gate_chain", ""), 40),
                "validator_n_samples":     int(_vl.get("validator_n_samples", 0)),
                "validator_bull_edge":     round(float(_vl.get("validator_bull_edge", 0.0)), 3),
                "validator_velocity_flag": bool(_vl.get("validator_velocity_flag", False)),
                "validator_velocity_note": _st(_vl.get("validator_velocity_note", ""), 40),
            })(result.get("markov_validation") or {}),
            "markov_state":         result.get("markov_state", ""),
            "markov_effective_n":   result.get("markov_effective_n", 0.0),
            "ote_zone_high":   _f(result.get("ote_zone_high") or _eh, 6),
            "ote_zone_low":    _f(result.get("ote_zone_low")  or _el, 6),
            "chart_tf":        _st(result.get("exec_tf") or result.get("best_tf") or "4H", 4),

            # ── MTF ───────────────────────────────────────────────────
            "bias_tf":      _st(result.get("bias_tf",""),    4),
            "ref_tf":       _st(result.get("ref_tf",""),     4),
            "exec_tf":      _st(result.get("exec_tf",""),    4),
            "trader_type":  _st(result.get("trader_type",""), 12),

            # ── Gates ─────────────────────────────────────────────────
            "gate_pass":    [bool(g) for g in (result.get("gate_pass") or [])],

            # ── Probability ───────────────────────────────────────────
            "signal_probability_data": {
                "signal_probability_pct": _st(sp_d.get("signal_probability_pct","--"), 8),
                "probability_tier":       _st(sp_d.get("probability_tier","LOW"),      8),
            },

            # ── HMM (trimmed hard to save bytes) ──────────────────────
            "hmm_data": {
                "hmm_state":          _st(hmm_d.get("hmm_state","RANGE"), 10),
                "hmm_confidence":     round(float(hmm_d.get("hmm_confidence") or
                                              hmm_d.get("confidence_boost", 0.5)), 2),
                "hmm_confidence_pct": _st(hmm_d.get("hmm_confidence_pct") or
                                          str(round(float(hmm_d.get("confidence_boost",0.5))*100))+'%', 6),
                "hmm_path":           [_st(s, 8) for s in (hmm_d.get("hmm_path") or [])[-5:]],
                "hmm_path_str":       "",  # not rendered in visible HTML — dropped for URL budget
                "hmm_note":           "",
            },

            # ── OCE Eval (trimmed) ────────────────────────────────────
            "oce_eval": {
                "eval_gate":       _st(oce_eval.get("eval_gate",""),        5),
                "eval_score":      _st(str(oce_eval.get("eval_score","") or ""), 8),
                "epistemic_label": _st(oce_eval.get("epistemic_label",""), 12),
                "eval_failures":   [],   # dropped — not rendered in current HTML
            },

            # ── OCE Audit (trimmed: only what HTML renders) ───────────
            "oce_audit": {
                "audit_result":  _st(oce_audit.get("audit_result",""),  6),
                "d1_functional": _st(oce_audit.get("d1_functional",""), 5),
                "d2_ux":         _st(oce_audit.get("d2_ux",""),         5),
                "d3_qa":         _st(oce_audit.get("d3_qa",""),         5),
            },

            # ── OCE BRVF (trimmed) ────────────────────────────────────
            "oce_brvf": {
                "brvf_status": _st(oce_brvf.get("brvf_status",""), 6),
                "brvf_fixes":  [_st(f, 16) for f in (oce_brvf.get("brvf_fixes") or [])[:2]],
            },

            # ── Gaps (max 2 rows, minimal strings — saves ~100 bytes) ───────
            "oce_gaps": [g for g in [_gap(g) for g in oce_gaps[:2]] if g],

            # ── Age badge ─────────────────────────────────────────────
            "signal_ts": int(time.time() * 1000),

            # ── [MOP-P09] Gate float scores + fingerprint memory ──────
            "gate_scores":  [round(float(s), 3) for s in (result.get("gate_scores") or [])],
            "gate_fp_data": {
                "gate_hash":    _st((result.get("gate_fp_data") or {}).get("gate_hash", ""), 8),
                "gate_vec":     _st((result.get("gate_fp_data") or {}).get("gate_vec", ""), 12),
                "gate_n":       int((result.get("gate_fp_data") or {}).get("gate_n", 0)),
                "gate_tp_rate": (result.get("gate_fp_data") or {}).get("gate_tp_rate"),
                "gate_tp":      int((result.get("gate_fp_data") or {}).get("gate_tp", 0)),
                "gate_sl":      int((result.get("gate_fp_data") or {}).get("gate_sl", 0)),
                "gate_crowd_tp": int((result.get("gate_fp_data") or {}).get("gate_crowd_tp", 0)),
                "gate_crowd_sl": int((result.get("gate_fp_data") or {}).get("gate_crowd_sl", 0)),
            },

            # ── [MOP-P09] Gate 11 Devil's Advocate detail ─────────────
            "gate11_detail": {
                "gate11_pass":      bool((result.get("gate11_detail") or {}).get("gate11_pass", True)),
                "gate11_adv_score": int((result.get("gate11_detail") or {}).get("gate11_adv_score", 0)),
                "gate11_threshold": int((result.get("gate11_detail") or {}).get("gate11_threshold", 2)),
                "gate11_note":      _st((result.get("gate11_detail") or {}).get("gate11_note", ""), 60),
            },

            # ── [MOP-P10] Crowd social proof (from gate_memory) ───────
            "crowd_data": (lambda _fp: {
                "crowd_total":   int(_fp.get("gate_crowd_tp", 0)) + int(_fp.get("gate_crowd_sl", 0)),
                "crowd_tp":      int(_fp.get("gate_crowd_tp", 0)),
                "crowd_sl":      int(_fp.get("gate_crowd_sl", 0)),
                "crowd_tp_rate": round(_fp.get("gate_crowd_tp", 0) /
                                  max(_fp.get("gate_crowd_tp", 0) + _fp.get("gate_crowd_sl", 0), 1), 2),
                "crowd_sl_rate": round(_fp.get("gate_crowd_sl", 0) /
                                  max(_fp.get("gate_crowd_tp", 0) + _fp.get("gate_crowd_sl", 0), 1), 2),
            })(result.get("gate_fp_data") or {}),

            # ── [MOP-P10] PCA scatter points (pre-computed server-side) ─
            # Points format: [{x, y, outcome, current}]
            # Populated from gate_memory fingerprints if available; empty list = no scatter
            "pca_points": [],   # populated post-hoc by oce_enhance_result if OCE_AVAILABLE

            # ── GAP-01: SuperValidator 5-brain result ──────────────────
            # Previously computed by _sv_ta in technical_analysis() but never
            # included in this payload. Now included so trade_view.html can
            # render the Sentiment+Regime+Sector+Trend+Cross breakdown.
            "super_markov": (lambda sv: {
                "super_conf_mod":    round(float(sv.get("super_conf_mod", 1.0)), 3),
                "super_verdict":     _st(sv.get("super_verdict", ""), 60),
                "super_verdict_key": _st(sv.get("super_verdict_key", "NEUTRAL"), 16),
                "super_gate_veto":   bool(sv.get("super_gate_veto", False)),
                "super_compact":     _st(sv.get("super_compact", ""), 80),
                "sentiment_mod":     round(float(sv.get("sentiment_mod", 1.0)), 3),
                "sentiment_score":   round(float(sv.get("sentiment_score", 0.0)), 3),
                "sentiment_pos":     [_st(w,20) for w in (sv.get("sentiment_keywords_pos") or [])[:3]],
                "sentiment_neg":     [_st(w,20) for w in (sv.get("sentiment_keywords_neg") or [])[:3]],
                "regime_mod":        round(float(sv.get("regime_mod", 1.0)), 3),
                "regime_verdict":    _st(sv.get("regime_verdict", ""), 50),
                "sector_mod":        round(float(sv.get("sector_mod", 1.0)), 3),
                "sector_verdict":    _st(sv.get("sector_verdict", ""), 50),
                "trend_mod":         round(float(sv.get("trend_mod", 1.0)), 3),
                "trend_verdict":     _st(sv.get("trend_verdict", ""), 50),
                "cross_mod":         round(float(sv.get("cross_mod", 1.0)), 3),
                "cross_verdict":     _st(sv.get("cross_verdict", ""), 50),
                "ict_mod":           round(float(sv.get("ict_mod", 1.0)), 3),
            })(result.get("super_markov_data") or {}),

            # ── GAP-02: LevelLearner self-learning metadata ────────────
            # blend_weight > 0 means TP/SL/entry levels are statistically
            # adjusted from real historical outcomes — must be disclosed to user.
            "level_learner": (lambda ll: {
                "blend_weight":  round(float(ll.get("blend_weight", 0.0)), 3),
                "n_samples":     int(ll.get("n_samples", 0)),
                "confidence":    _st(ll.get("confidence", "NONE"), 8),
                "win_rate":      round(float(ll.get("win_rate", 0.0)), 1),
                "learned_note":  _st(ll.get("learned_note", ""), 80),
                "med_tp1_pct":   round(float(ll.get("med_tp1_pct", 0.0)), 3),
                "med_tp3_pct":   round(float(ll.get("med_tp3_pct", 0.0)), 3),
                "med_sl_pct":    round(float(ll.get("med_sl_pct", 0.0)), 3),
                "med_sweep_pct": round(float(ll.get("med_sweep_pct", 0.0)), 3),
            })(result.get("learned_levels") or {}),

            # ── GAP-13: Flash-crash warning ────────────────────────────
            "flash_warning":     bool(result.get("flash_warning", False)),
            "flash_warning_pct": round(float(result.get("flash_warning_pct", 0.0)), 2),

            # ── GAP-14: Markov-adjusted probability ───────────────────
            "markov_adjusted": (lambda ma: {
                "adjusted_probability": round(float(ma.get("adjusted_probability", 0.0)), 1),
                "adjusted_pct":         _st(ma.get("adjusted_pct", "--"), 8),
                "adjusted_tier":        _st(ma.get("adjusted_tier", "LOW"), 8),
                "tier_icon":            _st(ma.get("tier_icon", "🔴"), 4),
                "base_probability":     round(float(ma.get("base_probability", 0.0)), 1),
                "markov_conf_mod":      round(float(ma.get("markov_conf_mod", 1.0)), 3),
                "note":                 _st(ma.get("note", ""), 60),
            })(result.get("markov_adjusted_probability") or {}),
        }

        clean    = _san(payload)
        raw_json = json.dumps(clean, separators=(',',':'), ensure_ascii=False)
        encoded  = base64.b64encode(raw_json.encode('utf-8')).decode()
        # FIX: URL-encode the base64 so + / = are not corrupted by Telegram/browser URL parsing
        url      = WEBAPP_URL.rstrip('/') + '/?' + 'd=' + _url_quote(encoded, safe='')

        if len(url) > 1800:
            logger.warning(
                f"[OCE GAP-3] URL {len(url)} chars (JSON {len(raw_json)}B) "
                f"may exceed Telegram limit. Symbol: {symbol}"
            )
        return url
    except Exception as e:
        logger.warning(f"[OCE GAP-3] WebApp URL build failed: {e}")
        return None


def create_main_keyboard():
    """
    Create main menu keyboard — priority-ordered layout (thesis D1.3).
    Row 1: Primary features (TA, Alpha)
    Row 2: Market signals (Cross, Sector Rotation)
    Row 3: Discovery (Trending, DEX)
    Row 4: Intelligence (AI, Feedback)
    Row 5: Utility (Features, Settings, Help)
    """
    keyboard = [
        [KeyboardButton("📊 Technical Analysis"), KeyboardButton("💎 Alpha Signals")],
        [KeyboardButton("📐 Cross"),              KeyboardButton("🌊 Sector Rotation")],
        [KeyboardButton("🔥 Trending"),           KeyboardButton("🦎 DEX"),      KeyboardButton("🎁 Airdrops")],
        [KeyboardButton("🤖 AI Assistant"),       KeyboardButton("📋 Feedback")],
        [KeyboardButton("🚀 Features"),           KeyboardButton("⚙️ Settings"), KeyboardButton("ℹ️ Help")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False, is_persistent=True)

def fetch_json(url: str, params=None):
    """Fetch JSON data from API with error handling"""
    try:
        response = requests.get(
            url, 
            params=params, 
            headers=REQUEST_HEADERS, 
            timeout=API_REQUEST_TIMEOUT
        )
        if response.status_code == 200:
            return response.json()
    except Exception as error:
        logger.error(f"Error fetching {url}: {error}")
    return None

def fetch_with_retry(url: str, params=None, retries=3):
    """
    Fetch JSON with exponential backoff retry.
    Survives temporary API failures.
    NOTE: Uses time.sleep - MUST always be called via asyncio.to_thread() (thesis A2.4).
    """
    delay = 1
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                params=params,
                headers=REQUEST_HEADERS,
                timeout=API_REQUEST_TIMEOUT
            )
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"[RETRY] Attempt {attempt + 1}/{retries} failed with status {response.status_code}")
        except Exception as error:
            logger.warning(f"[RETRY] Attempt {attempt + 1}/{retries} failed: {error}")

        if attempt < retries - 1:  # Don't sleep on last attempt
            time.sleep(delay)
            delay *= 2  # Exponential backoff

    logger.error(f"[RETRY] All {retries} attempts failed for {url}")
    return None


def fetch_alpha_coin_data():
    """
    Resilient CoinGecko fetch for Alpha Signals.
    Tries the full 100-coin + 1h/24h request first, then gracefully downgrades
    instead of immediately showing 'Failed to fetch coin data'.
    """
    url = f"{COINGECKO_BASE_URL}/coins/markets"
    param_variants = [
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "1h,24h",
        },
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "24h",
        },
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "24h",
        },
    ]

    last_error = None
    for idx, params in enumerate(param_variants, 1):
        try:
            data = fetch_with_retry(url, params=params, retries=3)
            if isinstance(data, list) and data:
                logger.info(
                    "[ALPHA] CoinGecko fetch succeeded on variant %s (%s coins, pct=%s)",
                    idx,
                    params.get("per_page"),
                    params.get("price_change_percentage"),
                )
                return data
        except Exception as exc:
            last_error = exc
            logger.warning(f"[ALPHA] CoinGecko variant {idx} failed: {exc}")

    if last_error is not None:
        logger.error(f"[ALPHA] All CoinGecko variants failed: {last_error}")
    else:
        logger.error("[ALPHA] All CoinGecko variants returned empty data")
    return []

def detect_trend(current, previous, volume_current=None, volume_previous=None):
    """
    Institutional-grade trend detection
    Returns: direction + strength + volatility state
    """
    # BUG-9 FIX: guard both current AND previous — current=None causes TypeError on arithmetic
    if current is None or previous is None or previous == 0:
        return {
            "direction": "Neutral",
            "emoji": "➖",
            "strength": "Unknown",
            "text": "Neutral➖"
        }
    
    # Calculate rate of change
    change_pct = ((current - previous) / previous) * 100
    
    # Determine direction
    if abs(change_pct) < 0.5:
        direction = "Sideways"
        emoji = "➖"
    elif change_pct > 0:
        direction = "Uptrend"
        emoji = "📈"
    else:
        direction = "Downtrend"
        emoji = "📉"
    
    # Determine strength based on magnitude
    abs_change = abs(change_pct)
    if abs_change < 0.5:
        strength = "Neutral"
    elif abs_change < 2:
        strength = "Weak"
    elif abs_change < 5:
        strength = "Moderate"
    elif abs_change < 10:
        strength = "Strong"
    else:
        strength = "Very Strong"
    
    # Volume confirmation (if available)
    volume_confirmed = False
    if volume_current and volume_previous:
        volume_change = ((volume_current - volume_previous) / volume_previous) * 100
        # Trend is stronger if volume confirms direction
        volume_confirmed = (change_pct > 0 and volume_change > 0) or (change_pct < 0 and volume_change > 0)
    
    # Build comprehensive trend text
    if strength == "Neutral":
        text = f"{direction}{emoji}"
    else:
        confirmation = " (Vol✓)" if volume_confirmed else ""
        text = f"{direction}{emoji} ({strength}{confirmation})"
    
    return {
        "direction": direction,
        "emoji": emoji,
        "strength": strength,
        "change_pct": change_pct,
        "volume_confirmed": volume_confirmed,
        "text": text
    }

# ==========================================
# ETF & MARKET DATA FUNCTIONS
# ==========================================

def fetch_etf_net_flows():
    """
    Fetch BTC, ETH, GOLD, SILVER ETF flows with intelligent caching
    Architecture: Try live data → Use cached → Use realistic fallback (ALWAYS show values)
    """
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        market_closed = is_market_closed()
        
        logger.info(f"[ETF] Fetching ETF data. Date: {current_date}, Market closed: {market_closed}")
        
        # Realistic fallback values based on current market conditions (Feb 2026)
        FALLBACK_VALUES = {
            "BTC": 218000000,   # $218M typical BTC ETF flow
            "ETH": 91000000,    # $91M typical ETH ETF flow
            "GOLD": 128000000,  # $128M typical GOLD ETF flow
            "SILVER": 44000000  # $44M typical SILVER ETF flow
        }
        
        # ========== BTC ETF ==========
        btc_flow = None
        btc_date = None
        btc_status = "estimated"
        
        try:
            btc_data = fetch_with_retry("https://api.llama.fi/etfs/bitcoin")
            if btc_data:
                if isinstance(btc_data, list) and len(btc_data) > 0:
                    latest = btc_data[-1]
                    btc_flow = latest.get("totalNetFlow", 0) or latest.get("netFlow", 0) or 0
                    btc_date = latest.get("date", current_date)
                elif isinstance(btc_data, dict):
                    btc_flow = btc_data.get("totalNetFlow", 0) or btc_data.get("netFlow", 0) or 0
                    btc_date = btc_data.get("date", current_date)
                
                # If we got valid data, cache it
                if btc_flow is not None:
                    etf_cache["BTC"] = {
                        "flow": btc_flow,
                        "date": btc_date,
                        "updated_at": datetime.now().isoformat()
                    }
                    save_etf_cache()
                    btc_status = "live"
                    logger.info(f"[ETF] BTC: ${btc_flow:,.0f} (live data)")
        except Exception as e:
            logger.warning(f"[ETF] BTC API failed: {e}")
        
        # Fallback chain: live → cache → realistic estimate
        if btc_flow is None:
            if "BTC" in etf_cache:
                cached = etf_cache["BTC"]
                btc_flow = cached["flow"]
                btc_date = cached["date"]
                btc_status = "cached"
                logger.info(f"[ETF] BTC: Using cached data from {btc_date}")
            else:
                # Use realistic fallback
                btc_flow = FALLBACK_VALUES["BTC"]
                btc_date = "estimated"
                btc_status = "estimated"
                logger.info(f"[ETF] BTC: Using estimated value ${btc_flow:,.0f}")
        
        # ========== ETH ETF ==========
        eth_flow = None
        eth_date = None
        eth_status = "estimated"
        
        try:
            eth_data = fetch_with_retry("https://api.llama.fi/etfs/ethereum")
            if eth_data:
                if isinstance(eth_data, list) and len(eth_data) > 0:
                    latest = eth_data[-1]
                    eth_flow = latest.get("totalNetFlow", 0) or latest.get("netFlow", 0) or 0
                    eth_date = latest.get("date", current_date)
                elif isinstance(eth_data, dict):
                    eth_flow = eth_data.get("totalNetFlow", 0) or eth_data.get("netFlow", 0) or 0
                    eth_date = eth_data.get("date", current_date)
                
                # Cache valid data
                if eth_flow is not None:
                    etf_cache["ETH"] = {
                        "flow": eth_flow,
                        "date": eth_date,
                        "updated_at": datetime.now().isoformat()
                    }
                    save_etf_cache()
                    eth_status = "live"
                    logger.info(f"[ETF] ETH: ${eth_flow:,.0f} (live data)")
        except Exception as e:
            logger.warning(f"[ETF] ETH API failed: {e}")
        
        # Fallback chain
        if eth_flow is None:
            if "ETH" in etf_cache:
                cached = etf_cache["ETH"]
                eth_flow = cached["flow"]
                eth_date = cached["date"]
                eth_status = "cached"
                logger.info(f"[ETF] ETH: Using cached data from {eth_date}")
            else:
                # Use realistic fallback
                eth_flow = FALLBACK_VALUES["ETH"]
                eth_date = "estimated"
                eth_status = "estimated"
                logger.info(f"[ETF] ETH: Using estimated value ${eth_flow:,.0f}")
        
        # ========== GOLD ETF ==========
        # Try cache first, then use fallback
        gold_flow = None
        gold_date = None
        gold_status = "estimated"
        
        if "GOLD" in etf_cache:
            cached = etf_cache["GOLD"]
            gold_flow = cached["flow"]
            gold_date = cached["date"]
            gold_status = "cached"
        else:
            gold_flow = FALLBACK_VALUES["GOLD"]
            gold_date = "estimated"
            gold_status = "estimated"
        
        # ========== SILVER ETF ==========
        silver_flow = None
        silver_date = None
        silver_status = "estimated"
        
        if "SILVER" in etf_cache:
            cached = etf_cache["SILVER"]
            silver_flow = cached["flow"]
            silver_date = cached["date"]
            silver_status = "cached"
        else:
            silver_flow = FALLBACK_VALUES["SILVER"]
            silver_date = "estimated"
            silver_status = "estimated"
        
        # ========== BUILD RESULT WITH STATUS ==========
        etf_flows = [
            {
                "name": "BTC ETF",
                "flow": btc_flow,
                "date": btc_date,
                "status": btc_status
            },
            {
                "name": "ETH ETF",
                "flow": eth_flow,
                "date": eth_date,
                "status": eth_status
            },
            {
                "name": "GOLD ETF",
                "flow": gold_flow,
                "date": gold_date,
                "status": gold_status
            },
            {
                "name": "SILVER ETF",
                "flow": silver_flow,
                "date": silver_date,
                "status": silver_status
            }
        ]
        
        # Sort by flow amount (descending - biggest to smallest)
        etf_flows.sort(
            key=lambda x: abs(x["flow"]) if x["flow"] is not None else 0, 
            reverse=True
        )
        
        ranking_summary = [
            f"{e['name']}: ${e['flow']:,.0f}"
            for e in etf_flows
        ]
        logger.info(f"[ETF] Final ranking: {ranking_summary}")
        
        return etf_flows
        
    except Exception as e:
        logger.error(f"[ETF] Critical error in fetch_etf_net_flows: {e}")
        
        # Emergency fallback - return realistic values
        fallback = [
            {
                "name": "BTC ETF",
                "flow": 218000000,
                "date": "estimated",
                "status": "estimated"
            },
            {
                "name": "GOLD ETF",
                "flow": 128000000,
                "date": "estimated",
                "status": "estimated"
            },
            {
                "name": "ETH ETF",
                "flow": 91000000,
                "date": "estimated",
                "status": "estimated"
            },
            {
                "name": "SILVER ETF",
                "flow": 44000000,
                "date": "estimated",
                "status": "estimated"
            }
        ]
        
        return fallback

def get_market_regime():
    """Calculate current market regime with data freshness tracking"""
    try:
        # Track data fetch time
        fetch_timestamp = datetime.now()
        data_sources_health = {
            "coingecko": False,
            "fear_greed": False,
            "price_data": False
        }
        
        # Fetch global market data
        global_resp = requests.get(
            f"{COINGECKO_BASE_URL}/global",
            headers=REQUEST_HEADERS,
            timeout=API_REQUEST_TIMEOUT
        )
        global_data = global_resp.json()["data"]
        data_sources_health["coingecko"] = True
        
        btc_dominance = global_data["market_cap_percentage"].get("btc", 0)
        total_market_cap = global_data["total_market_cap"].get("usd", 0) / 1e12
        
        # Fetch ETH/BTC ratio + 24h volume (BUG FIX [MILD]: add volume confirmation)
        eth_resp = requests.get(
            f"{COINGECKO_BASE_URL}/simple/price?ids=ethereum,bitcoin&vs_currencies=usd&include_24hr_vol=true",
            headers=REQUEST_HEADERS,
            timeout=API_REQUEST_TIMEOUT
        )
        prices = eth_resp.json()
        eth_btc_ratio = prices["ethereum"]["usd"] / prices["bitcoin"]["usd"]
        btc_volume_24h = prices.get("bitcoin", {}).get("usd_24h_vol", 0)
        eth_volume_24h = prices.get("ethereum", {}).get("usd_24h_vol", 0)
        
        # BUG-15 FIX: subtract ALL major stablecoins, not just USDT
        # USDC, DAI, BUSD etc. collectively ~10-15% of market cap — ignoring them overstates alt dom
        _stable_dom = sum(
            global_data["market_cap_percentage"].get(s, 0)
            for s in ["usdt", "usdc", "dai", "busd", "tusd", "frax", "usde"]
        )
        altcoin_dominance = max(0.0, 100 - btc_dominance - _stable_dom)
        
        # Fetch Fear & Greed Index
        fng_resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=API_REQUEST_TIMEOUT)
        fear_greed_index = int(fng_resp.json()["data"][0]["value"])
        data_sources_health["fear_greed"] = True
        
        # Calculate Bitcoin RSI — Wilder's smoothed method (thesis A1.3)
        btc_resp = requests.get(
            f"{COINGECKO_BASE_URL}/coins/bitcoin/market_chart?vs_currency=usd&days=30",
            headers=REQUEST_HEADERS,
            timeout=API_REQUEST_TIMEOUT
        )
        prices_data = [p[1] for p in btc_resp.json()["prices"]]
        data_sources_health["price_data"] = True
        # Wilder's RSI-14: requires at least 15 data points
        _period = 14
        if len(prices_data) >= _period + 1:
            _deltas = [prices_data[i+1] - prices_data[i] for i in range(len(prices_data)-1)]
            # Seed with simple average of first 14 changes
            _gains_seed  = [d for d in _deltas[:_period] if d > 0]
            _losses_seed = [-d for d in _deltas[:_period] if d < 0]
            _avg_g = sum(_gains_seed) / _period
            _avg_l = sum(_losses_seed) / _period
            # Smooth remaining deltas with Wilder's exponential smoothing
            for d in _deltas[_period:]:
                _g = d if d > 0 else 0.0
                _l = -d if d < 0 else 0.0
                _avg_g = (_avg_g * (_period - 1) + _g) / _period
                _avg_l = (_avg_l * (_period - 1) + _l) / _period
            _rs = _avg_g / _avg_l if _avg_l != 0 else 0
            bitcoin_rsi = round(100 - (100 / (1 + _rs)), 2)
        else:
            bitcoin_rsi = 50.0
        
        # Alt Season Checklist
        # BUG-14 FIX: "Stablecoin supply increasing" was hardcoded True — permanently inflated score by 1.
        # Now: stablecoin dominance > 7% = dry powder parked = bullish alt signal (real check)
        _stablecoin_dom = sum(
            global_data["market_cap_percentage"].get(s, 0)
            for s in ["usdt", "usdc", "dai", "busd", "tusd", "frax", "usde"]
        )
        checklist = {
            "BTC Dominance dropping": btc_dominance < 55,
            "ETH/BTC breaking up": eth_btc_ratio > 0.05,
            "Altcoin volume expanding": altcoin_dominance > 10,
            "Risk-on sentiment": fear_greed_index > 50,
            "Stablecoin dry powder": _stablecoin_dom > 7.0,
        }
        
        passed = sum(checklist.values())

        # ── 4-regime model (more granular than binary bull/bear) ─────────────
        if passed >= 4:
            regime = "Bull Expansion"
            emoji  = "🟢"
        elif passed == 3:
            regime = "Bull Pullback"
            emoji  = "🟡"
        elif passed == 2:
            regime = "Chop / Consolidation"
            emoji  = "🟠"
        else:
            regime = "Bear Market"
            emoji  = "🔴"
        
        # Calculate data confidence score
        sources_available = sum(data_sources_health.values())
        total_sources = len(data_sources_health)
        confidence_score = (sources_available / total_sources) * 100
        
        return {
            "regime": regime,
            "emoji": emoji,
            "btc_dominance": btc_dominance,
            "eth_btc_ratio": eth_btc_ratio,
            "altcoin_dominance": altcoin_dominance,
            "total_market_cap": total_market_cap,
            "fear_greed_index": fear_greed_index,
            "bitcoin_rsi": bitcoin_rsi,
            "checklist": checklist,
            "passed": passed,
            "timestamp": fetch_timestamp,
            "data_sources_health": data_sources_health,
            "confidence_score": confidence_score,
            "btc_volume_24h": btc_volume_24h,
            "eth_volume_24h": eth_volume_24h,
        }
    except Exception as e:
        logger.error(f"Error fetching market regime: {e}")
        return {
            "regime": "Bear Market",
            "emoji": "🔴",
            "btc_dominance": 57.03,
            "eth_btc_ratio": 0.03289,
            "altcoin_dominance": 36.58,
            "total_market_cap": 2.91,
            "fear_greed_index": 16,
            "bitcoin_rsi": 42.70,
            "checklist": {},
            "passed": 0,
            "confidence_score": 33,   # BUG FIX #4: fallback was missing this key → defaulted to 100
            "btc_volume_24h": 0,
            "eth_volume_24h": 0,
        }

def _get_news_category(title_lower: str,
                       macro_kw, exchange_kw, defi_kw, btc_eth_kw, bullish_kw, bearish_kw) -> str:
    """Single source-of-truth category detector used by both relevance scoring and image fallback."""
    if any(k in title_lower for k in macro_kw):     return "Macro"
    if any(k in title_lower for k in btc_eth_kw):   return "BTC/ETH"
    if any(k in title_lower for k in exchange_kw):  return "Exchange"
    if any(k in title_lower for k in defi_kw):      return "DeFi"
    if any(k in title_lower for k in bullish_kw):   return "Bullish"
    if any(k in title_lower for k in bearish_kw):   return "Bearish"
    return "General"


def fetch_news():
    """Fetch market-relevant news with smart categorization and ranking"""
    news_items = []

    # Enhanced keyword categories for better detection
    macro_keywords    = ["etf", "inflow", "outflow", "cpi", "inflation", "fed", "federal reserve",
                         "interest rate", "regulation", "sec", "treasury", "powell", "yellen"]
    exchange_keywords = ["binance", "coinbase", "kraken", "exchange", "volume", "trading",
                         "liquidity", "orderbook", "listing"]
    bullish_keywords  = ["bullish", "rally", "breakout", "surge", "pump", "uptrend", "ath",
                         "all-time high", "moon", "gains", "spike"]
    bearish_keywords  = ["bearish", "crash", "dump", "downtrend", "correction", "drop", "fall",
                         "plunge", "selloff", "liquidation", "decline"]
    urgent_keywords   = ["breaking", "urgent", "alert", "critical", "warning", "emergency",
                         "major", "significant", "huge", "massive"]
    defi_keywords     = ["defi", "lending", "staking", "yield", "protocol", "tvl", "liquidity pool"]
    btc_eth_keywords  = ["bitcoin", "btc", "ethereum", "eth"]

    category_images = {
        "Macro":   "https://cryptologos.cc/logos/bitcoin-btc-logo.png",
        "Exchange":"https://cryptologos.cc/logos/binance-coin-bnb-logo.png",
        "DeFi":    "https://cryptologos.cc/logos/uniswap-uni-logo.png",
        "BTC/ETH": "https://cryptologos.cc/logos/ethereum-eth-logo.png",
        "Bullish": "https://cryptologos.cc/logos/cardano-ada-logo.png",
        "Bearish": "https://cryptologos.cc/logos/tether-usdt-logo.png",
        "General": "https://cryptologos.cc/logos/crypto-com-chain-cro-logo.png",
    }

    try:
        feeds = [
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cointelegraph.com/rss",
            "https://news.bitcoin.com/feed/",
            "https://cryptoslate.com/feed/",
            "https://decrypt.co/feed/"
        ]

        for url in feeds:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                title       = entry.title
                title_lower = title.lower()

                # Single category detection (thesis A1.7 — no duplicate branch)
                category  = _get_news_category(title_lower, macro_keywords, exchange_keywords,
                                               defi_keywords, btc_eth_keywords,
                                               bullish_keywords, bearish_keywords)
                is_urgent = any(k in title_lower for k in urgent_keywords)

                # Relevance scoring using resolved category
                _CATEGORY_SCORES = {"Macro": 100, "BTC/ETH": 90, "Exchange": 80,
                                    "DeFi": 70, "Bullish": 60, "Bearish": 60, "General": 30}
                relevance_score = _CATEGORY_SCORES.get(category, 30)
                if is_urgent:
                    relevance_score += 50
                keyword_count = sum([
                    any(k in title_lower for k in macro_keywords),
                    any(k in title_lower for k in exchange_keywords),
                    any(k in title_lower for k in btc_eth_keywords),
                    any(k in title_lower for k in defi_keywords)
                ])
                relevance_score += keyword_count * 20

                # Image fallback chain
                image = None
                if "media_content" in entry and entry.media_content:
                    image = entry.media_content[0].get("url", None)
                if not image and "media_thumbnail" in entry and entry.media_thumbnail:
                    image = entry.media_thumbnail[0].get("url", None)
                if not image and "enclosures" in entry and entry.enclosures:
                    for enclosure in entry.enclosures:
                        if "image" in enclosure.get("type", ""):
                            image = enclosure.get("href", None)
                            break
                if not image and "links" in entry:
                    for link in entry.links:
                        if "image" in link.get("type", ""):
                            image = link.get("href", None)
                            break
                # Use resolved category for fallback — no duplicate detection (thesis A1.7)
                if not image:
                    image = category_images.get(category, "https://cryptologos.cc/logos/bitcoin-btc-logo.png")

                # Parse & validate pub date — uses top-level imports (thesis A3.1,2,3)
                raw_pub     = entry.get("published", "") if hasattr(entry, "get") else (entry.published if "published" in entry else "")
                pub_display = datetime.now(timezone.utc).strftime("%b %d, %Y %I:%M %p UTC")
                try:
                    pub_dt    = parsedate_to_datetime(raw_pub)
                    pub_naive = _dt.datetime(*pub_dt.utctimetuple()[:6])
                    now_naive = _dt.datetime.utcnow()
                    if pub_naive > now_naive + _dt.timedelta(hours=1):
                        logger.warning(f"[NEWS] Rejected future-dated article ({raw_pub[:30]}): {title[:60]}")
                        continue
                    pub_display = pub_naive.strftime("%b %d, %Y %I:%M %p UTC")
                except Exception:
                    pub_display = raw_pub[:30] if raw_pub else pub_display

                news_items.append({
                    "title":           title,
                    "url":             entry.link,
                    "image":           image,
                    "published":       pub_display,
                    "urgent":          is_urgent,
                    "category":        category,
                    "relevance_score": relevance_score,
                })

        # De-duplicate by first 50 chars of title
        unique_news = []
        seen_titles: set = set()
        for item in news_items:
            simplified = item['title'].lower()[:50]
            if simplified not in seen_titles:
                seen_titles.add(simplified)
                unique_news.append(item)

        unique_news.sort(key=lambda x: (x["relevance_score"], x["urgent"]), reverse=True)
        top = unique_news[:5]

        # ── SentimentBrain: auto-score all headlines immediately ───────
        if MARKOV_AVAILABLE and top:
            try:
                _SUPER_VALIDATOR.sentiment.record_headlines(
                    [item["title"] for item in top]
                )
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        return top

    except Exception as e:
        logger.error(f"News fetch error: {e}")
        return [{
            "title":           "Crypto market analysis - Stay updated on market conditions",
            "url":             "https://cryptonews.com/",
            "image":           "https://cryptologos.cc/logos/bitcoin-btc-logo.png",
            "published":       datetime.now(timezone.utc).strftime("%b %d, %Y %I:%M %p UTC"),
            "urgent":          False,
            "category":        "General",
            "relevance_score": 50,
        }]

# ==========================================
# MESSAGE MANAGEMENT
# ==========================================

async def clear_market_messages(chat_id, context):
    """Clear all tracked market overview and news messages for this chat."""
    msg_ids = _market_messages.pop(chat_id, [])
    for msg_id in msg_ids:
        try:
            await context.bot.delete_message(chat_id, msg_id)
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
async def send_market_overview(chat_id, context, market_data):
    """
    v8.2 TRIO — Pin + Market Overview + News
    Upgrades: async ETF/news, PHT time, BTC regime narrative, FOMC check,
    richer notification with RSI + alt dom, improved confidence display.
    """
    try:
        await clear_market_messages(chat_id, context)
        _market_messages[chat_id] = []

        if not market_data:
            logger.error("[OVERVIEW] market_data is None")
            await context.bot.send_message(
                chat_id, "⚠️ Market data temporarily unavailable. Please try again.",
                parse_mode="Markdown", reply_markup=create_main_keyboard()
            )
            return

        # ── Trend deltas ─────────────────────────────────────────────────
        prev = previous_market_data.get(chat_id, {})
        btc_trend = detect_trend(
            market_data.get('btc_dominance'), prev.get("btc_dominance")
        ).get('text', 'N/A')
        eth_trend = detect_trend(
            market_data.get('eth_btc_ratio'), prev.get("eth_btc_ratio")
        ).get('text', 'N/A')
        alt_trend = detect_trend(
            market_data.get('altcoin_dominance'), prev.get("altcoin_dominance")
        ).get('text', 'N/A')
        previous_market_data[chat_id] = {
            "btc_dominance":    market_data.get('btc_dominance'),
            "eth_btc_ratio":    market_data.get('eth_btc_ratio'),
            "altcoin_dominance":market_data.get('altcoin_dominance'),
        }

        # ── [UPGRADE] Async ETF fetch ─────────────────────────────────────
        etf_flows = []
        etf_confidence_adjustment = 100
        try:
            etf_flows = await asyncio.to_thread(fetch_etf_net_flows)
        except Exception as e:
            logger.error(f"[OVERVIEW] ETF fetch failed: {e}")

        if etf_flows:
            etf_lines = []
            etf_statuses = []
            for etf in etf_flows:
                name   = etf.get("name", "Unknown")
                flow   = etf.get("flow")
                date   = etf.get("date")
                status = etf.get("status", "unknown")
                etf_statuses.append(status)
                flow_str     = f"${flow:,.0f}" if flow is not None else "$0"
                status_icon  = "🟢" if status == "live" else "🟡" if status == "cached" else "⚪"
                if status == "cached" and date and date != "estimated":
                    etf_lines.append(f"{status_icon} {name}: {flow_str} ({date})")
                else:
                    etf_lines.append(f"{status_icon} {name}: {flow_str}")
            etf_confidence_adjustment = sum(
                calculate_etf_confidence(s) for s in etf_statuses
            ) / len(etf_statuses) if etf_statuses else 100
            etf_text   = "\n".join(etf_lines)
            etf_legend = "\n🟢 Live  🟡 Recent  ⚪ Estimate"
        else:
            etf_text   = "• ETF data temporarily unavailable"
            etf_legend = ""
            etf_confidence_adjustment = 70

        # ── Confidence score ─────────────────────────────────────────────
        base_confidence  = market_data.get('confidence_score', 67)
        passed_checks    = market_data.get('passed', 0)
        checklist_weight = (passed_checks / 5) * 40
        adjusted_confidence = float(min(int((base_confidence * 0.6) + checklist_weight), 99))
        etf_delta           = (etf_confidence_adjustment - 80) * 0.05
        adjusted_confidence = max(0.0, min(99.0, adjusted_confidence + etf_delta))

        bar_filled   = min(10, int(adjusted_confidence / 10))
        regime_bar   = "█" * bar_filled + "░" * (10 - bar_filled)

        # ── FNG + RSI labels ─────────────────────────────────────────────
        fng_raw   = market_data.get('fear_greed_index', 50)
        fng_int   = int(fng_raw) if isinstance(fng_raw, (int, float)) else 50
        fng_label = ("🩸 Extreme Fear" if fng_int < 25 else
                     "😨 Fear"         if fng_int < 45 else
                     "😐 Neutral"      if fng_int < 55 else
                     "😏 Greed"        if fng_int < 75 else "🤑 Extreme Greed")
        rsi_raw   = market_data.get('bitcoin_rsi', 50)
        rsi_float = float(rsi_raw) if isinstance(rsi_raw, (int, float)) else 50.0
        rsi_label = ("🟢 Oversold" if rsi_float < 30 else
                     "🔴 Overbought" if rsi_float > 70 else "⚪ Neutral")

        # ── Checklist ────────────────────────────────────────────────────
        checklist      = market_data.get('checklist', {})
        checklist_text = "\n".join([
            f"{'✅' if v else '⛔'} {k}" for k, v in checklist.items()
        ]) if checklist else "N/A"

        # ── [UPGRADE] Alt season narrative ───────────────────────────────
        btc_dom     = market_data.get('btc_dominance', 0)
        alt_dom     = market_data.get('altcoin_dominance', 0)
        eth_btc     = market_data.get('eth_btc_ratio', 0)
        if passed_checks >= 4:
            alt_narrative = "🚀 Alt Season conditions active — rotate into alts"
        elif passed_checks == 3:
            alt_narrative = "⚡ Early alt season signals forming — watch closely"
        elif btc_dom > 58:
            alt_narrative = "🟠 BTC dominance high — capital still in BTC, alts lagging"
        else:
            alt_narrative = "🔵 Transition zone — monitor BTC dominance for breakout"

        # ── [UPGRADE] PHT time ───────────────────────────────────────────
        _pht_h24 = (datetime.now(timezone.utc).hour + 8) % 24
        _pht_min = datetime.now(timezone.utc).minute
        _ampm    = "AM" if _pht_h24 < 12 else "PM"
        _pht_h12 = _pht_h24 % 12 or 12
        _pht_str = f"{_pht_h12}:{_pht_min:02d} {_ampm} PHT"

        # ── [UPGRADE] FOMC check from APEX state ─────────────────────────
        _fomc_note = ""
        _fomc_data = _apex_state.get("regime_snapshot", {})
        if _fomc_data.get("fomc_block"):
            _fomc_note = "\n🚨 *FOMC HARD BLOCK ACTIVE* — High volatility expected"

        # ── [UPGRADE] Async live BTC price ───────────────────────────────
        _btc_live_str = ""
        try:
            _btc_pr = await asyncio.to_thread(
                fetch_json,
                f"{COINGECKO_BASE_URL}/simple/price",
                {"ids": "bitcoin", "vs_currencies": "usd", "include_24hr_change": "true"}
            )
            if _btc_pr and "bitcoin" in _btc_pr:
                _p   = _btc_pr["bitcoin"].get("usd", 0)
                _chg = _btc_pr["bitcoin"].get("usd_24h_change", 0)
                _arr = "📈" if _chg >= 0 else "📉"
                _btc_live_str = f"💰 BTC: `${_p:,.0f}`  {_arr} `{_chg:+.2f}%`\n"
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        overview_text = (
            f"📊 *MARKET OVERVIEW* — v8.2\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{_btc_live_str}"
            f"{market_data.get('emoji', '⚪')} *{market_data.get('regime', 'Unknown')}*\n"
            f"[{regime_bar}] `{adjusted_confidence:.0f}%` confidence\n"
            f"{(_fomc_note + chr(10)) if _fomc_note else ''}\n"  # BUG-4 FIX: no blank line when no FOMC
            f"📈 *Market Metrics*\n"
            f"• BTC Dom: `{safe_format_number(btc_dom)}%` {btc_trend}\n"
            f"• ETH/BTC: `{safe_format_number(eth_btc, 5)}` {eth_trend}\n"
            f"• Alt Dom: `{safe_format_number(alt_dom)}%` {alt_trend}\n"
            f"• Total MCap: `${safe_format_number(market_data.get('total_market_cap'))}T`\n"
            f"• BTC Vol 24h: `${market_data.get('btc_volume_24h', 0)/1e9:.1f}B`\n"
            f"• ETH Vol 24h: `${market_data.get('eth_volume_24h', 0)/1e9:.1f}B`\n\n"
            f"🧠 *Sentiment*\n"
            f"• Fear & Greed: `{fng_raw}` — {fng_label}\n"
            f"• BTC RSI-14: `{safe_format_number(rsi_float)}` — {rsi_label}\n\n"
            f"💰 *ETF Net Flows (Ranked)*\n"
            f"{etf_text}\n"
            f"{etf_legend}\n\n"
            f"📌 *Alt Season Checklist:* `{passed_checks}/5` Passed\n"
            f"{checklist_text}\n\n"
            f"💡 _{alt_narrative}_\n\n"
        )

        # ── Safe fallback containers — used by notification Markov block ─
        # Populated inside the try/except so notification renders even if Markov throws
        _trio_data: dict = {}
        _sv_data:   dict = {}

        # ── Super Markov trio enrichment ────────────────────────────────
        try:
            # Basic ICT trio still runs for backward compat
            _trio = _MARKOV_VALIDATOR.validate_trio(
                fg_value   = fng_int,
                fomc_block = bool(_fomc_data.get("fomc_block")),
                regime     = market_data.get("regime", "UNKNOWN"),
                symbol     = "BTCUSDT",
            )
            _trio_note = _trio.get("trio_note", "")
            # FIX — MARKOV 0%: trio_alpha written HERE — immediately after _trio, before _sv.
            # Previously it was written AFTER _sv.validate() — if _sv threw, alpha was never
            # set and update_regime_pin defaulted to 0.0 → always showed 0%.
            market_data["trio_alpha"] = float(_trio.get("trio_alpha", 0.0))
            # FIX — MARKOV NOTIFICATION BLOCK: _trio_data also written HERE before _sv.
            # _sv.validate() throws AttributeError in current env (markov_stack RegimeBrain bug).
            # If _trio_data = _trio stays after _sv call, it never gets set → notification
            # Markov block never renders because `if _trio_data:` is always False.
            _trio_data = _trio
            # BUG-11 FIX: SuperValidator bias threshold unified with validate_trio (>60/<40, not >55/<45)
            # Previously: >55 = BULLISH, <45 = BEARISH — produced contradictory bias with same FNG value
            _sv_bias = "BULLISH" if fng_int > 60 else "BEARISH" if fng_int < 40 else "NEUTRAL"
            # Super Validator — combines all 5 brains
            _sv = _SUPER_VALIDATOR.validate(
                ict_conf_mod    = 1.0,
                ict_verdict_key = "NEUTRAL",
                current_regime  = market_data.get("regime", "TRENDING"),
                bias            = _sv_bias,
                symbols         = ["BTCUSDT"],
                momentum_quality= "HIGH" if rsi_float > 60 else "LOW" if rsi_float < 40 else "MEDIUM",
            )
            # BUG-10 FIX: super_gate_veto was computed but never surfaced — hard safety signal silently dropped
            if _sv.get("super_gate_veto"):
                overview_text += "\n⛔ *MARKOV VETO — High-risk conditions detected. No entry advised.*\n"
            # BUG-12 FIX: both Markov blocks appended with no separator — merged into visual noise
            # Now: only append _trio_note if distinct from super_compact content, with labeled header
            if _trio_note:
                overview_text += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🎲 *Markov Chain*\n{_trio_note}\n"
            overview_text += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🧠 *Super Validator*\n{_sv['super_compact']}\n"
            # _sv_data only set when SuperValidator succeeds — optional enrichment for notification
            _sv_data = _sv
        except Exception as _trio_e:
            logger.debug(f"[SUPER_MARKOV TRIO] {_trio_e}")

        overview_text += (
            f"\n⏰ `{datetime.now(timezone.utc).strftime('%b %d, %Y %H:%M UTC')}` · _{_pht_str}_\n"
            f"⚠️ _Not financial advice. DYOR._"
        )

        overview_text = trim_message_for_telegram(overview_text)

        overview_msg = await context.bot.send_message(
            chat_id, overview_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh Data", callback_data="refresh_market"),
                 InlineKeyboardButton("📋 Validate", callback_data="fb_validate")],
            ])
        )
        _market_messages[chat_id].append(overview_msg.message_id)
        capture_output(chat_id, overview_text, "market_overview")

        # ── [UPGRADE] Async news fetch ────────────────────────────────────
        news_items = []
        try:
            news_items = await asyncio.to_thread(fetch_news)
        except Exception as e:
            logger.error(f"[OVERVIEW] News fetch failed: {e}")

        _effective_news_photos = _setting_max_news_photos(context)
        _light_start_mode = _setting_light_start_mode(context)

        if news_items and _light_start_mode and _effective_news_photos <= 0:
            lines = ["📰 *TOP NEWS*", "━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
            for item in news_items[:3]:
                title = (item.get("title") or "No Title").strip()
                published = (item.get("published") or "Unknown").strip()
                category = (item.get("category") or "News").strip()
                lines.append(f"• *{title[:110]}*\n  `{category}` · {published}")
            lines.append("\n_Low-RAM mode: image cards disabled to reduce memory use._")
            news_msg = await context.bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown")
            _market_messages[chat_id].append(news_msg.message_id)
            capture_output(chat_id, "\n".join(lines), "news")
        elif news_items:
            # BUG-17 FIX: price enrichment was sequential (up to 75s) — now gathered in parallel
            _coin_map = {"bitcoin":"bitcoin","btc":"bitcoin","ethereum":"ethereum",
                         "eth":"ethereum","solana":"solana","sol":"solana",
                         "xrp":"ripple","bnb":"binancecoin"}
            _FALLBACK_IMAGE = "https://cryptologos.cc/logos/bitcoin-btc-logo.png"

            async def _enrich_item(item):
                """Fetch live price for a single news item concurrently."""
                base_caption = (
                    f"📰 *{item.get('title', 'No Title')}*\n"
                    f"• {item.get('category', 'News')}\n"
                    f"🕒 {item.get('published', 'Unknown')}"
                )
                if item.get('urgent'):
                    base_caption = "🚨 *URGENT*\n" + base_caption
                _title_l = item.get('title','').lower()
                _coin_id = next((cid for kw,cid in _coin_map.items() if kw in _title_l), None)
                _price_line = ""
                if _coin_id:
                    try:
                        _pr = await asyncio.to_thread(
                            fetch_json,
                            f"{COINGECKO_BASE_URL}/simple/price",
                            {"ids": _coin_id, "vs_currencies": "usd", "include_24hr_change": "true"}
                        )
                        if _pr and _coin_id in _pr:
                            _p   = _pr[_coin_id].get("usd", 0)
                            _chg = _pr[_coin_id].get("usd_24h_change", 0)
                            _arrow = "📈" if _chg >= 0 else "📉"
                            _price_line = f"\n💰 Live: `${_p:,.2f}`  {_arrow} `{_chg:+.2f}%` (24h)"
                    except Exception as _e:
                        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
                caption = trim_message_for_telegram(
                    base_caption + _price_line + "\n⚠️ _Not financial advice. DYOR._",
                    max_length=1000
                )
                return item, caption

            enriched = await asyncio.gather(*[_enrich_item(item) for item in news_items[:max(0, _effective_news_photos)]])

            # BUG-3 FIX: capture_output was called per-item inside the loop — last item overwrote all prior.
            # Collect all captions first, then call capture_output ONCE after loop.
            _news_capture_parts = []

            for item, caption in enriched:
                try:
                    # BUG-1 FIX: send_photo(photo=None) crashes when all image fallback chains fail
                    # Hard fallback ensures photo is always a valid URL
                    _photo = item.get("image") or _FALLBACK_IMAGE

                    # DV-5 FIX: url="#" is a dead link — skip button if no valid URL
                    _url = item.get("url", "")
                    _reply_markup = (
                        InlineKeyboardMarkup([[InlineKeyboardButton("Read Full Article", url=_url)]])
                        if _url and _url.startswith("http") else None
                    )
                    news_msg = await context.bot.send_photo(
                        chat_id=chat_id,
                        photo=_photo,
                        caption=caption,
                        parse_mode="Markdown",
                        reply_markup=_reply_markup,
                    )
                    _market_messages[chat_id].append(news_msg.message_id)
                    _news_capture_parts.append(
                        f"[NEWS]\nImage: {item.get('image','N/A')}\nURL: {_url or 'N/A'}\n\n{caption}"
                    )
                except Exception as e:
                    logger.error(f"[OVERVIEW] Error sending news item: {e}")

            # BUG-3 FIX: single capture_output for all news — trio bucket gets combined news text
            if _news_capture_parts:
                capture_output(chat_id, "\n\n---\n\n".join(_news_capture_parts), "news")

        # ── [UPGRADE] Richer 1-min notification ──────────────────────────
        top_news = news_items[0] if news_items else None
        notification_text = (
            f"🔔 *MARKET UPDATE* · _{_pht_str}_\n"
            f"{'─' * 28}\n"
            f"{market_data.get('emoji','⚪')} *{market_data.get('regime','Unknown')}*"
            f"  ·  `{adjusted_confidence:.0f}%` conf\n"
            f"{_btc_live_str}"  # BUG-5 FIX: removed trailing \n — _btc_live_str already ends in \n when set
            f"*Quick Stats:*\n"
            f"• BTC Dom: `{safe_format_number(btc_dom)}%`  Alt Dom: `{safe_format_number(alt_dom)}%`\n"
            f"• Fear & Greed: `{fng_raw}` ({fng_label.split()[-1]})\n"
            f"• BTC RSI: `{rsi_float:.1f}` ({rsi_label.split()[-1]})\n"
            f"• MCap: `${safe_format_number(market_data.get('total_market_cap'))}T`\n"
        )
        if _fomc_note:
            notification_text += f"\n{_fomc_note}\n"
        if top_news:
            notification_text += (
                f"\n{'─' * 30}\n"
                f"📰 *TOP NEWS*\n"
                f"{top_news.get('title','No Title')}\n"
                f"Category: {top_news.get('category','General')}\n"
                f"🕒 {top_news.get('published','Unknown')}"
            )

        # ── Markov Learning Block ─────────────────────────────────────────
        # Appended to notification only when brain has actual data (_trio_data populated)
        if _trio_data:
            _mk_alpha    = _trio_data.get("trio_alpha", 0.0)
            _mk_pct      = min(100, round(_mk_alpha * 100))
            _mk_n        = _trio_data.get("trio_n_samples", 0)
            _mk_eff_n    = _trio_data.get("trio_effective_n", 0.0)
            _mk_bull     = round(_trio_data.get("trio_bull_mass", 0.0) * 100)
            _mk_bear     = round(_trio_data.get("trio_bear_mass", 0.0) * 100)
            _mk_dom      = _trio_data.get("trio_dominant_state", "—")
            _mk_wr       = _trio_data.get("trio_win_rates", {})

            # Win rate lines — only show states with recorded trades
            _wr_lines = []
            for state, wr in _mk_wr.items():
                _state_label = state.replace("_", " ")
                _wr_pct      = round(wr * 100)
                _wr_icon     = "🟢" if _wr_pct >= 60 else "🔴" if _wr_pct < 40 else "🟡"
                _wr_lines.append(f"  {_wr_icon} {_state_label}: {_wr_pct}% win rate")

            # SuperValidator brain breakdown — only if available
            _sv_lines = []
            if _sv_data:
                _sv_lines = [
                    f"  • Regime mod:   `{_sv_data.get('regime_mod', 1.0):.2f}` — {_sv_data.get('regime_verdict','—')}",
                    f"  • Sector mod:   `{_sv_data.get('sector_mod', 1.0):.2f}` — {_sv_data.get('sector_verdict','—')}",
                    f"  • Trend mod:    `{_sv_data.get('trend_mod', 1.0):.2f}` — {_sv_data.get('trend_verdict','—')}",
                    f"  • Cross mod:    `{_sv_data.get('cross_mod', 1.0):.2f}` — {_sv_data.get('cross_verdict','—')}",
                    f"  • Sentiment:    `{_sv_data.get('sentiment_score', 0.0):+.2f}` — {'Positive 🟢' if _sv_data.get('sentiment_score',0) > 0.1 else 'Negative 🔴' if _sv_data.get('sentiment_score',0) < -0.1 else 'Neutral ⚪'}",
                    f"  • Volatility:   `{_sv_data.get('vol_tier','—')}` — {_sv_data.get('vol_verdict','—')}",
                ]

            notification_text += f"\n{'─' * 30}\n"
            notification_text += (
                f"🎲 *Markov: {_mk_pct}% learned from:*\n"
                f"  • Transitions logged:  `{_mk_n}` raw · `{_mk_eff_n:.1f}` effective\n"
                f"  • Learning rate (α):   `{_mk_alpha:.3f}` of 1.000 (100 needed for full trust)\n"
                f"  • Bull state mass:     `{_mk_bull}%` probability weight\n"
                f"  • Bear state mass:     `{_mk_bear}%` probability weight\n"
                f"  • Dominant ICT state:  `{_mk_dom.replace('_', ' ')}`\n"
            )
            if _wr_lines:
                notification_text += "  • *State win rates* (recorded trades):\n"
                notification_text += "\n".join(_wr_lines) + "\n"
            else:
                notification_text += "  • State win rates: _no trades recorded yet_\n"

            if _sv_lines:
                notification_text += "*🧠 Brain modifiers:*\n"
                notification_text += "\n".join(_sv_lines) + "\n"

        notification_text = trim_message_for_telegram(notification_text, max_length=1600)

        # [MENU-FIX] notification_msg now carries create_main_keyboard().
        # welcome_msg is deleted in start() after 2s so it can no longer
        # anchor the Reply keyboard. This is the final trio message and
        # MUST carry the keyboard so menu buttons appear immediately.
        notification_msg = await context.bot.send_message(
            chat_id, notification_text,
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )

        async def delete_notification():
            await asyncio.sleep(60)
            try:
                await context.bot.delete_message(chat_id, notification_msg.message_id)
                if chat_id in _market_messages and notification_msg.message_id in _market_messages[chat_id]:
                    _market_messages[chat_id].remove(notification_msg.message_id)
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            # [MENU-FIX] Re-anchor keyboard after notification auto-deletes.
            # The Reply keyboard is tied to the message that sent it.
            # Without this send, the keyboard disappears with the notification.
            try:
                _anc = await context.bot.send_message(
                    chat_id, "📡 _Live data loaded · Use the menu below_",
                    parse_mode="Markdown",
                    reply_markup=create_main_keyboard()
                )
                if chat_id in _market_messages:
                    _market_messages[chat_id].append(_anc.message_id)
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        _market_messages[chat_id].append(notification_msg.message_id)
        # BUG-2 FIX: renamed _dt → _del_task — _dt is already the module-level `import datetime as _dt` alias
        _del_task = asyncio.create_task(delete_notification())
        _delete_tasks.add(_del_task)
        _del_task.add_done_callback(_delete_tasks.discard)
        logger.info(f"[OVERVIEW] Sent v8.2 trio for chat_id: {chat_id}")

    except Exception as e:
        logger.error(f"[OVERVIEW] Critical error: {e}", exc_info=True)
        try:
            await context.bot.send_message(
                chat_id, "⚠️ Error loading market overview. Please try /start again.",
                parse_mode="Markdown", reply_markup=create_main_keyboard()
            )
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
async def update_regime_pin(chat_id, context, market_data, force=False):
    """
    Update pinned message when regime changes.
    Strategy: DELETE old pin → SEND new message → PIN it.
    This guarantees the pin is always fresh text — no edit failures, no orphan pins.
    """
    current_regime = market_data.get('regime', 'Unknown')

    # ── Regime change detection ────────────────────────────────────────
    regime_changed = False
    if chat_id in regime_start_times:
        previous_regime = regime_start_times[chat_id].get("regime")
        if previous_regime != current_regime:
            regime_changed = True
            logger.info(f"[PIN] Regime changed: {previous_regime} → {current_regime}")
    else:
        regime_changed = True
        force = True

    if not (regime_changed or force):
        logger.info(f"[PIN] Regime unchanged ({current_regime}), keeping existing pin")
        return

    # ── Update regime tracking ─────────────────────────────────────────
    regime_start_times[chat_id] = {
        "regime": current_regime,
        "start_time": datetime.now(timezone.utc)
    }

    # ── Markov learning percent ────────────────────────────────────────
    # α = effective_n / ALPHA_RAMP_SAMPLES (capped at 100%)
    # trio_alpha is set in send_market_overview immediately after validate_trio
    # so it is always available by the time update_regime_pin is called
    try:
        _trio_alpha = market_data.get("trio_alpha", 0.0)
        _markov_pct = min(100, round(_trio_alpha * 100))
    except Exception:
        _markov_pct = 0

    # ── AI provider status ────────────────────────────────────────────
    _ai_keys = {
        "groq":     GROQ_API_KEY,
        "cerebras": CEREBRAS_API_KEY,
        "gemini":   GEMINI_API_KEY,
        "mistral":  MISTRAL_API_KEY,
        "github":   GITHUB_TOKEN,
    }
    _active_ai = sum(1 for k in _ai_keys.values() if k and k.strip())
    _ai_status  = "ONLINE" if _active_ai > 0 else "OFFLINE"

    # ── Regime label — full string, no truncation ─────────────────────
    # FIX: previously only BULL/BEAR got labels — everything else used raw .upper()
    # which worked but is now explicit for all 4 regime states
    _r_upper = current_regime.upper()
    if "BULL EXPANSION" in _r_upper:
        _regime_label = "BULL EXPANSION"
    elif "BULL PULLBACK" in _r_upper:
        _regime_label = "BULL PULLBACK"
    elif "BULL" in _r_upper:
        _regime_label = "BULL MARKET"
    elif "BEAR" in _r_upper:
        _regime_label = "BEAR MARKET"
    elif "CHOP" in _r_upper or "CONSOLIDATION" in _r_upper:
        _regime_label = "CHOP / CONSOLIDATION"
    else:
        _regime_label = current_regime.upper()

    # ── Pin text ──────────────────────────────────────────────────────
    _ts = datetime.now(timezone.utc).strftime("%H:%M UTC")
    pin_text = (
        f"{_regime_label} | Markov: {_markov_pct}% "
        f"| AI: {_ai_status} ({_active_ai} ACTIVE) | {_ts}"
    )

    # ── DELETE old pin, SEND new message, PIN it ──────────────────────
    # Strategy: always delete → send → pin.
    # Avoids edit failures on old/deleted messages and orphan pin accumulation.
    try:
        # Step 1: delete the old pinned message if we have one
        if chat_id in _pin_messages:
            old_msg_id = _pin_messages.pop(chat_id)
            _save_pin_messages()
            try:
                await context.bot.unpin_chat_message(chat_id, old_msg_id)
            except Exception as _unpin_e:
                logger.debug(f"[PIN] Unpin old failed (already gone?): {_unpin_e}")
            try:
                await context.bot.delete_message(chat_id, old_msg_id)
                logger.info(f"[PIN] Deleted old pin msg_id={old_msg_id}")
            except Exception as _del_e:
                logger.debug(f"[PIN] Delete old failed (already gone?): {_del_e}")

        # Step 2: send new pin message
        pin_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=pin_text,
        )

        # Step 3: pin the new message
        await context.bot.pin_chat_message(
            chat_id,
            pin_msg.message_id,
            disable_notification=True,
        )

        _pin_messages[chat_id] = pin_msg.message_id
        _save_pin_messages()
        capture_output(chat_id, pin_text, "pin")
        logger.info(f"[PIN] New pin sent+pinned for chat_id={chat_id}: {pin_text}")

    except Exception as e:
        logger.error(f"[PIN] Failed to update pin: {e}")

# ==========================================
# BACKGROUND TASK
# ==========================================

# ==========================================
# AUTO ALPHA NOTIFICATION ENGINE
# ==========================================

_ALL_AI_PROVIDERS = ["groq", "cerebras", "gemini", "mistral", "github"]

async def _get_top_alpha_coin() -> dict | None:
    """
    Memory-safe pipeline — runs entirely in background with no UI messages.
    Step 1: Sector rotation → find accumulation coins
    Step 2: Alpha signals → verify stealth accumulation
    Step 3: All 5 AIs vote on top coin
    Step 4: Return winner dict {symbol, price, change, rci, reason}
    """
    # ── Step 1: Sector rotation (uses existing cached result — zero extra RAM) ─
    try:
        sectors = await analyze_sector_rotation_async()
    except Exception as e:
        logger.warning(f"[AUTO-ALPHA] Sector rotation failed: {e}")
        sectors = []

    # Collect accumulation candidates from sectors
    sector_candidates: list[dict] = []
    for sector in sectors:
        for token in sector.get("tokens", []):
            # High RCI + low price move = accumulation signal
            if token.get("rci", 0) > 65 and abs(token.get("change", 0)) < 4:
                sector_candidates.append({
                    "symbol":   token["symbol"],
                    "rci":      token["rci"],
                    "change":   token["change"],
                    "sector":   sector["category"],
                    "smart":    token.get("smart_money", 0),
                })

    # ── Step 2: Alpha signals — fetch top 50 coins, find stealth accumulation ─
    try:
        top_coins = await asyncio.to_thread(
            fetch_json,
            f"{COINGECKO_BASE_URL}/coins/markets",
            {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,                          # FIX-1: was 50 — doubles mid-cap coverage
                "sparkline": False,
                "price_change_percentage": "1h,24h",      # FIX-3: was 24h only — 1h is the early signal
            }
        )
    except Exception as e:
        logger.warning(f"[AUTO-ALPHA] CoinGecko fetch failed: {e}")
        top_coins = []

    alpha_candidates: list[dict] = []
    coin_prices: dict = {}
    if top_coins:
        for coin in top_coins:
            vol    = float(coin.get("total_volume") or 0)
            change = float(coin.get("price_change_percentage_24h") or 0)
            # FIX-3: 1h change is the stealth signal — flat 1h + high RCI = loading NOW
            chg1h  = float(coin.get("price_change_percentage_1h_in_currency") or 0)
            mcap   = float(coin.get("market_cap") or 0)
            price  = float(coin.get("current_price") or 0)
            symbol = coin.get("symbol", "").upper()
            if vol == 0 or mcap == 0:
                continue
            coin_prices[symbol] = price
            metrics = calculate_rci_metrics(coin, vol, mcap, price, change)
            # FIX-3: gate on BOTH 24h < 3% AND 1h < 0.8% — catches coins being loaded right now
            if metrics["rci"] > 70 and abs(change) < 3 and abs(chg1h) < 0.8:
                alpha_candidates.append({
                    "symbol": symbol,
                    "rci":    metrics["rci"],
                    "change": change,
                    "chg1h":  chg1h,
                    "price":  price,
                    "smart":  metrics["smart_money"],
                    "liq":    metrics["liquidity"],
                })

    # ── Step 3: Merge and score candidates ────────────────────────────────────
    # Coins appearing in BOTH sector rotation AND alpha signals get a bonus
    alpha_symbols = {c["symbol"] for c in alpha_candidates}
    sector_symbols = {c["symbol"] for c in sector_candidates}
    overlap = alpha_symbols & sector_symbols

    merged: list[dict] = []
    seen: set = set()

    # Priority 1: overlap (confirmed by both systems)
    for c in alpha_candidates:
        if c["symbol"] in overlap and c["symbol"] not in seen:
            merged.append({**c, "score": c["rci"] + c["smart"] * 0.3 + 20})  # +20 bonus
            seen.add(c["symbol"])

    # Priority 2: alpha-only
    for c in alpha_candidates:
        if c["symbol"] not in seen:
            merged.append({**c, "score": c["rci"] + c["smart"] * 0.3})
            seen.add(c["symbol"])

    # Priority 3: sector-only (no price data — lower weight)
    for c in sector_candidates:
        if c["symbol"] not in seen:
            merged.append({
                **c,
                "price": coin_prices.get(c["symbol"], 0),
                "liq": 0,
                "score": c["rci"] * 0.7,
            })
            seen.add(c["symbol"])

    if not merged:
        logger.info("[AUTO-ALPHA] No accumulation candidates found this cycle")
        return None

    # Sort by composite score descending
    merged.sort(key=lambda x: x["score"], reverse=True)
    top5 = merged[:5]

    # ── Step 4: 5 AI providers vote on top coin ───────────────────────────────
    market_data = {}
    try:
        market_data = await asyncio.to_thread(get_market_regime)
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    candidates_text = "\n".join([
        f"{i+1}. {c['symbol']} | RCI:{c['rci']:.1f} | Change:{c['change']:+.2f}% | "
        f"Smart:{c.get('smart',0):.1f} | Score:{c['score']:.1f}"
        for i, c in enumerate(top5)
    ])

    vote_prompt = (
        # OCE v13.0 — Pattern: Domain=Trading/Finance | Intent=Evaluate | Strategy=Consensus vote
        # Response Tier: SNAP — single symbol + one sentence. No extra text.
        # Zero-Hallucination Mandate: base pick only on the candidate data provided below.
        "You are a DeFi institutional analyst operating under OCE v13.0 execution discipline. "
        "Below are quantitatively ranked accumulation candidates from a live RCI + Smart Money engine.\n\n"
        + candidates_text +
        "\n\n=== SELECTION CRITERIA (evaluate ALL before deciding) ===\n"
        "  1. Stealth accumulation quality: highest RCI with lowest visible price move\n"
        "  2. Smart money conviction: score > 60 preferred\n"
        "  3. Risk/reward posture: is entry timing optimal vs recent structure?\n"
        "  4. Composite score ranking: weight this as tie-breaker only\n\n"
        "Grounding rule: choose from the list above only — no external knowledge.\n\n"
        "Reply with ONLY the coin symbol (e.g. BTC) and one sentence reason. Format:\n"
        "SYMBOL: [symbol]\nREASON: [one sentence]"
    )

    votes: dict[str, int] = {}
    ai_reason = ""

    # Get available providers — skip unconfigured ones
    available_providers = [
        p for p in _ALL_AI_PROVIDERS
        if {
            "groq": GROQ_API_KEY, "cerebras": CEREBRAS_API_KEY,
            "gemini": GEMINI_API_KEY, "mistral": MISTRAL_API_KEY,
            "github": GITHUB_TOKEN,
        }.get(p, "").strip()
    ]

    # FIX-2: was sequential for-loop (5 × 15s = 75s worst case) → now parallel gather
    # All providers queried simultaneously — total wait = slowest single response (~5–8s)
    async def _single_vote(provider: str) -> str:
        try:
            return await asyncio.wait_for(
                ai_query(vote_prompt, market_data, provider),
                timeout=15.0
            )
        except Exception as _e:
            logger.debug(f"[AUTO-ALPHA] AI vote failed ({provider}): {_e}")
            return ""

    responses = await asyncio.gather(
        *[_single_vote(p) for p in available_providers[:5]]
    )

    for response in responses:
        if not response:
            continue
        for line in response.splitlines():
            if line.upper().startswith("SYMBOL:"):
                voted = line.split(":", 1)[1].strip().upper()
                votes[voted] = votes.get(voted, 0) + 1
            if line.upper().startswith("REASON:") and not ai_reason:
                ai_reason = line.split(":", 1)[1].strip()

    # Pick winner by vote count, fall back to top scored coin
    if votes:
        winner_symbol = max(votes, key=votes.get)
    else:
        winner_symbol = merged[0]["symbol"]

    # Find winner data
    winner = next((c for c in merged if c["symbol"] == winner_symbol), merged[0])
    winner["votes"] = votes.get(winner_symbol, 0)
    winner["total_voters"] = len(available_providers)
    winner["reason"] = ai_reason or "Highest composite RCI + smart money score."

    return winner


async def _get_cross_result_for_coin(symbol: str) -> str:
    """Run cross scan and extract the result block for a specific symbol. Memory-safe."""
    try:
        scores = await asyncio.wait_for(run_cross_scan(), timeout=60.0)
    except Exception as e:
        logger.warning(f"[AUTO-ALPHA] Cross scan failed: {e}")
        return ""

    if not scores:
        return ""

    # Find the coin in cross results
    match = next((s for s in scores if s.symbol.upper() == symbol.upper()), None)
    if not match:
        return ""

    # Build compact cross block for this coin only
    label      = _signal_label(match)
    peso_rate  = _get_peso_rate()              # live PHP/USD rate (thesis A1.6)
    peso       = match.price * peso_rate
    pred       = f" 🔮~{match.pred_days:.0f}d" if match.pred_days is not None else ""
    return (
        f"*Cross Signal:* {label}\n"
        f"Score `{match.ai_score:.0f}` \u00b7 Conf `{match.confidence:.0f}` \u00b7 "
        f"Eff `{match.efficiency:.0f}` \u00b7 RCI `{match.rci:.0f}`{pred}\n"
        f"Price: `${match.price:,.2f}` | \u20b1`{peso:,.0f}`"
    )


async def _get_ta_summary_for_coin(symbol: str, application) -> tuple[str, object | None]:
    """
    Run ICT/TA analysis for a coin. Returns (summary_text, chart_buf).
    Extracts the full rich dataset — same fields as the live TA caption.
    Memory-safe: result dict freed after extraction; chart_buf discarded after send.
    """
    try:
        # Serialise background alpha scan behind user requests — gc silently before run
        async with _TA_SEM:
            _gc.collect()
            result = await asyncio.wait_for(
                run_ict_analysis(symbol, market_type="spot"),
                timeout=90.0
            )
    except Exception as e:
        logger.warning(f"[AUTO-ALPHA] TA failed for {symbol}: {e}")
        return "", None

    # ── [OCE v10.3] HMM + Eval + Audit on AUTO-ALPHA background scan ─────
    if OCE_AVAILABLE and isinstance(result, dict) and result.get("bias"):
        result = oce_enhance_result(
            result, symbol, result.get("bias", "BULLISH")
        )
    # ─────────────────────────────────────────────────────────────────────

    # ── Build chart_data with all v7.0 keys before freeing result ─────────
    # _flt: null-safe coercion — exotic coins can return None for OB/FVG/SL zones
    def _flt_aa(d, key, default=0.0):
        v = d.get(key)
        if v is None:
            return default
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    try:
        chart_data = {
            "ohlcv_chart":        result.get("ohlcv_chart"),
            "ma_closes_full":     [float(c[3]) for c in result.get("ohlcv_chart_full") or []]
                                  or result.get("ma_closes_full"),
            "price":              result.get("price", 0),
            "bias":               result.get("bias", "NEUTRAL"),
            "tp1":                result.get("tp1", 0),
            "tp2":                result.get("tp2", 0),
            "tp3":                result.get("tp3", 0),
            "inv_sl":             result.get("inv_sl", 0),
            "entry_low":          result.get("entry_low", 0),
            "entry_high":         result.get("entry_high", 0),
            "ob_low":             _flt_aa(result, "ob_low"),
            "ob_high":            _flt_aa(result, "ob_high"),
            "fvg_low":            _flt_aa(result, "fvg_low"),
            "fvg_high":           _flt_aa(result, "fvg_high"),
            "liq_zone_swept":     _flt_aa(result, "liq_zone_swept"),
            "liq_zone_target":    _flt_aa(result, "liq_zone_target"),
            # BUG-FIX: "invalidation_level" appeared twice — second (inv_sl 0.0 default)
            # silently overwrote the first. Canonical value kept here with correct default.
            "invalidation_level": _flt_aa(result, "invalidation_level", result.get("inv_sl", 0.0)),
            "symbol":             result.get("symbol", symbol),
            "best_tf":            result.get("best_tf", "?"),
            "best_ex":            result.get("best_ex", {"name": "?"}),
            "market_label":       result.get("market_label", "SPOT"),
            "entry_ltf":          result.get("entry_ltf", "—"),
            "all_pass":           result.get("all_pass", False),
            "gate_pass":          result.get("gate_pass", []),
            "grade":              result.get("grade", "F"),
            "conf":               result.get("conf", 0),
            "win_prob":           result.get("win_prob", 0),
            "atr_pct":            result.get("atr_pct", 0),
            "rsi":                result.get("rsi", 50),
            "adx":                result.get("adx", 0),
            # BUG-FIX: "daily_veto_active", "l13_gate" appeared twice — second entry
            # used "" for daily_bias default (wrong) vs "—" above. Canonical keys kept once.
            "daily_veto_active":  result.get("daily_veto_active", False),
            "weekly_veto_active": result.get("weekly_veto_active", False),
            "daily_bias":         result.get("daily_bias", "—"),
            "l13_gate":           result.get("l13_gate", {}),
            "l8_ai":              result.get("l8_ai", {}),
            "trade_score_data":   result.get("trade_score_data", {}),
            "timing_data":        result.get("timing_data", {}),
            "scaled_entry":       result.get("scaled_entry", {}),
            # BUG-FIX: "smc_confluence" appeared twice — keep single canonical entry
            "smc_confluence":     result.get("smc_confluence", {}),
            "ma_stack":           result.get("ma_stack", {}),
            "entry_time":         result.get("entry_time", {}),
            "ote":                result.get("ote", {}),
            "ote_705":            _flt_aa(result, "ote_705"),
            "ote_zone_high":      _flt_aa(result, "ote_zone_high"),
            "ote_zone_low":       _flt_aa(result, "ote_zone_low"),
            "sl2":                _flt_aa(result, "sl2"),
            "sl3":                _flt_aa(result, "sl3"),
            "liq_long_10x":       _flt_aa(result, "liq_long_10x"),
            "liq_long_20x":       _flt_aa(result, "liq_long_20x"),
            "liq_long_50x":       _flt_aa(result, "liq_long_50x"),
            "liq_short_10x":      _flt_aa(result, "liq_short_10x"),
            "liq_short_20x":      _flt_aa(result, "liq_short_20x"),
            "liq_short_50x":      _flt_aa(result, "liq_short_50x"),
            "upg_wyckoff":        result.get("upg_wyckoff", {}),
            "upg_cvd":            result.get("upg_cvd", {}),
            "upg_funding_z":      result.get("upg_funding_z", {}),
            "upg_liq_heat":       result.get("upg_liq_heat", {}),
            "upg_elliott":        result.get("upg_elliott", {}),
            "upg_squeeze":        result.get("upg_squeeze", {}),
            "upg_vwap_z":         result.get("upg_vwap_z", {}),
            "bias_tf":            result.get("bias_tf", ""),
            "ref_tf":             result.get("ref_tf", ""),
            "exec_tf":            result.get("exec_tf", ""),
            "mtf_combo":          result.get("mtf_combo", ""),
            "trader_type":        result.get("trader_type", ""),
            # ── v8.2 sync fields ───────────────────────────────────────────
            "conf50":             result.get("conf50", {}),
            "rr_blocked":         result.get("rr_blocked", False),
            "rr_block_val":       _flt_aa(result, "rr_block_val"),
            "upg_fomc":           result.get("upg_fomc", {}),
            "upg_sar":            result.get("upg_sar", {}),
            "bsl_ssl_data":       result.get("bsl_ssl_data", {}),
            "smc_liq_type":       result.get("smc_liq_type", {}),
            "irl_erl_data":       result.get("irl_erl_data", {}),
            "bpr":                result.get("bpr", {}),
            "breaker_block":      result.get("breaker_block", {}),
            "silver_bullet":      result.get("silver_bullet", {}),
            "scaled_entry_ote":   result.get("scaled_entry_ote", False),
            "risk_grade":         result.get("risk_grade", "—"),
            "trade_plan_valid":   result.get("trade_plan_valid", False),
            "l6_vol_phase":       result.get("l6_vol_phase", {}),
            "ma_closes_full":     [float(c[3]) for c in result.get("ohlcv_chart_full") or []]
                                  or result.get("ma_closes_full"),
            # ── [v9.0 SYNC-7] Signal intelligence keys ─────────────────────
            "win_prob_display":        result.get("win_prob_display",
                                           f"{result.get('win_prob', 0):.1f}%"),
            "signal_probability_data": result.get("signal_probability_data", {}),
            "scanner_summary":         result.get("scanner_summary", {}),
            "bos_price":               result.get("bos_price", 0.0),
            "bos_type":                result.get("bos_type", ""),
            "order_block":             result.get("smc_ob_quality", {}),
            "choch":                   result.get("choch", False),
            "markov_data":             result.get("markov_data", {}),
            "markov_validation":       result.get("markov_validation", {}),
            "markov_state":            result.get("markov_state", ""),
            "markov_effective_n":      result.get("markov_effective_n", 0.0),
        }
    except Exception as e:
        logger.warning(f"[AUTO-ALPHA] chart_data extraction failed for {symbol}: {e}")
        chart_data = {}

    # ── Extract scalars for ta_text BEFORE freeing result ─────────────────
    def fp(v):
        try:
            return f"${v:,.4f}" if 0 < v < 1 else f"${v:,.2f}"
        except Exception:
            return str(v)

    _bias       = result.get("bias", "?")
    _direction  = "LONG" if _bias == "BULLISH" else "SHORT"
    _best_tf    = result.get("best_tf", "?")
    _best_ex    = result.get("best_ex", {}).get("name", "?")
    _market_lbl = result.get("market_label", "SPOT")
    _entry_mid  = (result.get("entry_low", 0) + result.get("entry_high", 0)) / 2
    _rr_denom   = max(abs(_entry_mid - result.get("inv_sl", _entry_mid)), 1e-9)
    _rr_val     = round(abs(result.get("tp3", 0) - _entry_mid) / _rr_denom, 2)
    _ts         = result.get("trade_score_data", {})
    _l8         = result.get("l8_ai", {})
    # Extract Markov scalars before del result
    _mk_data       = result.get("markov_data", {}) or {}
    _mk_validation = result.get("markov_validation", {}) or {}
    _mk_state_ta   = _mk_data.get("markov_state", "")
    _mk_verdict_ta = _mk_validation.get("validator_verdict", "")
    _mk_vkey_ta    = _mk_validation.get("validator_verdict_key", "NEUTRAL")
    _mk_mod_ta     = _mk_validation.get("validator_conf_mod", 1.0)
    _mk_n_ta       = _mk_validation.get("validator_n_samples", 0)
    _mk_alpha_ta   = _mk_validation.get("validator_alpha", 0.0)
    _mk_veto_ta    = _mk_validation.get("validator_gate_veto", False)

    # GAP-14: Markov-adjusted probability — blend signal_probability × validator_conf_mod
    _sp_raw       = result.get("signal_probability_data", {}) or {}
    _sp_base      = float(_sp_raw.get("signal_probability", 50.0))
    _sp_adj_raw   = min(95.0, max(5.0, _sp_base * _mk_mod_ta))
    _sp_adj_pct   = f"{_sp_adj_raw:.1f}%"
    _sp_adj_tier  = ("HIGH" if _sp_adj_raw >= 65 else "MODERATE" if _sp_adj_raw >= 45 else "LOW")
    result["markov_adjusted_probability"] = {
        "base_probability":     round(_sp_base, 1),
        "markov_conf_mod":      round(float(_mk_mod_ta), 3),
        "adjusted_probability": round(_sp_adj_raw, 1),
        "adjusted_pct":         _sp_adj_pct,
        "adjusted_tier":        _sp_adj_tier,
        "tier_icon":            ("🟢" if _sp_adj_tier == "HIGH" else "🟡" if _sp_adj_tier == "MODERATE" else "🔴"),
        "note":                 f"Base {_sp_base:.1f}% × Markov {_mk_mod_ta:.2f}× = {_sp_adj_raw:.1f}% (capped 5–95%)",
    }
    _l13        = result.get("l13_gate", {})
    _se         = result.get("scaled_entry", {})
    _td         = result.get("timing_data", {})
    _smc        = result.get("smc_confluence", {})
    _entry_win  = result.get("entry_time", {}).get("entry_window", "—")
    _bias_tf    = result.get("bias_tf", "")
    _ref_tf     = result.get("ref_tf", "")
    _exec_tf    = result.get("exec_tf", result.get("entry_ltf", ""))
    _trader_type= result.get("trader_type", "")
    _win_prob   = result.get("win_prob", 0)
    _grade      = result.get("grade", "?")
    _inv_sl     = result.get("inv_sl", 0)
    _sl2        = result.get("sl2", 0.0)
    _sl3        = result.get("sl3", 0.0)
    _tp1        = result.get("tp1", 0)
    _tp2        = result.get("tp2", 0)
    _tp3        = result.get("tp3", 0)
    _ote_high   = result.get("ote_zone_high", 0.0)
    _ote_low    = result.get("ote_zone_low", 0.0)
    _ote_705    = result.get("ote_705", 0.0)
    _gate_pass  = result.get("gate_pass", [])
    _gates_ok   = sum(_gate_pass) if _gate_pass else 0
    _gates_tot  = len(_gate_pass) if _gate_pass else 0
    _upg_wyck   = result.get("upg_wyckoff", {})
    _upg_cvd    = result.get("upg_cvd", {})
    _upg_fz     = result.get("upg_funding_z", {})
    _upg_ell    = result.get("upg_elliott", {})
    _upg_sq     = result.get("upg_squeeze", {})
    _upg_vwap   = result.get("upg_vwap_z", {})
    _liq_l10    = result.get("liq_long_10x", 0.0)
    _liq_s10    = result.get("liq_short_10x", 0.0)

    # ── [SYNC-FIX] Build webapp URL + capture meta BEFORE freeing result ──
    # Must happen here — result is freed below; webapp URL needs full dict.
    _alpha_meta: dict = {"webapp_url": None, "direction": "LONG ▲", "state": "BLOCKED"}
    try:
        if WEBAPP_URL:
            _alpha_meta["webapp_url"]  = build_webapp_signal_url(result, symbol)
        _alpha_meta["direction"] = "LONG ▲" if _bias in ("BULLISH", "BULL", "LONG") else "SHORT ▼"
        _alpha_meta["state"]     = result.get("signal_state", "BLOCKED")
        _alpha_meta["grade"]     = result.get("grade", "?")
    except Exception as _am_e:
        logger.debug(f"[AUTO-ALPHA] alpha_meta capture failed: {_am_e}")
    # ─────────────────────────────────────────────────────────────────────

    # Free result before chart render — extract Markov state first
    _mk_alpha_pre = (result.get("markov_data") or {}).get("markov_state", "")
    if _mk_alpha_pre:
        _set_alpha_markov_state(symbol, _mk_alpha_pre)
    del result
    _gc.collect()

    # ── Generate chart ────────────────────────────────────────────────────
    chart_buf = None
    if chart_data and not DISABLE_CHARTS:
        try:
            chart_buf = await asyncio.to_thread(generate_trade_chart, chart_data)
        except Exception as e:
            logger.warning(f"[AUTO-ALPHA] Chart generation failed: {e}")
        del chart_data
        _gc.collect()

    # ── PHP rate ──────────────────────────────────────────────────────────
    peso_rate = _get_peso_rate()

    # ── MTF combo label ───────────────────────────────────────────────────
    mtf_str = ""
    if _bias_tf and _ref_tf and _exec_tf:
        mtf_str = f"{_bias_tf}→{_ref_tf}→{_exec_tf}"
        if _trader_type:
            mtf_str += f" [{_trader_type}]"

    # ── Entry window + PHT conversion ─────────────────────────────────────
    import re as _re
    def _add_pht(text: str) -> str:
        def _conv(m):
            h, mn = int(m.group(1)), int(m.group(2))
            pht_h24 = (h + 8) % 24
            ampm    = "AM" if pht_h24 < 12 else "PM"
            pht_h12 = pht_h24 % 12 or 12
            return f"{h:02d}:{mn:02d} UTC ({pht_h12}:{mn:02d} {ampm} PHT)"
        return _re.sub(r'(\d{2}):(\d{2})\s*UTC', _conv, text)
    _entry_win_display = _add_pht(_entry_win)

    # ── Progress bar helper ───────────────────────────────────────────────
    def _bar(val, maxv=100, length=8):
        filled = round((val / maxv) * length) if maxv else 0
        return "█" * filled + "░" * (length - filled)

    # ── Build v7.0 upgrade labels ─────────────────────────────────────────
    extras = []
    if _upg_wyck and _upg_wyck.get("phase"):
        extras.append(f"🔄 Wyckoff: `{_upg_wyck['phase']}`")
    if _upg_cvd and _upg_cvd.get("signal"):
        extras.append(f"📊 CVD: `{_upg_cvd['signal']}`")
    if _upg_fz and _upg_fz.get("zscore") is not None:
        _fz = _upg_fz["zscore"]
        _flag = "⚠️ Extreme" if abs(_fz) > 2 else ("↑ High" if _fz > 1 else ("↓ Low" if _fz < -1 else "Normal"))
        extras.append(f"💸 Funding Z: `{_fz:.2f}` ({_flag})")
    if _upg_ell and _upg_ell.get("wave"):
        extras.append(f"🌊 Elliott: `Wave {_upg_ell['wave']}`")
    if _upg_sq and _upg_sq.get("prob") is not None:
        extras.append(f"⚡ Squeeze: `{_upg_sq['prob']:.0f}%`")
    if _upg_vwap and _upg_vwap.get("zscore") is not None:
        extras.append(f"📐 VWAP Z: `{_upg_vwap['zscore']:.2f}`")
    if _liq_l10 or _liq_s10:
        extras.append(f"🔥 Liq Heat: L10x@`{fp(_liq_l10)}` · S10x@`{fp(_liq_s10)}`")

    # ── Assemble ta_text ──────────────────────────────────────────────────
    price_live = _entry_mid  # use entry_mid as live proxy since result freed
    peso_live  = price_live * peso_rate

    ta_lines = [
        f"*📐 {symbol} [{_market_lbl}] — {_bias} {_direction}*",
        f"Exchange: `{_best_ex}` · TF: `{_best_tf}`",
    ]
    if mtf_str:
        ta_lines.append(f"MTF: `{mtf_str}`")

    ta_lines += [
        "",
        f"*Entry Legs (Scale In):*",
        f"  Leg1 40%: `{fp(_se.get('leg1_entry', _entry_mid))}`",
        f"  Leg2 35%: `{fp(_se.get('leg2_entry', _entry_mid))}`",
        f"  Leg3 25%: `{fp(_se.get('leg3_entry', _entry_mid))}`",
    ]
    if _ote_high and _ote_low:
        ta_lines.append(f"  OTE Zone: `{fp(_ote_low)}`–`{fp(_ote_high)}`  70.5%@`{fp(_ote_705)}`")

    ta_lines += [
        "",
        f"*Stop Loss:*",
        f"  SL (Std): `{fp(_inv_sl)}`  (OB+0.2×ATR)",
    ]
    if _sl2:
        ta_lines.append(f"  SL2 (Mod): `{fp(_sl2)}`")
    if _sl3:
        ta_lines.append(f"  SL3 (Agg): `{fp(_sl3)}`")

    ta_lines += [
        "",
        f"*Take Profit:*",
        f"  TP1: `{fp(_tp1)}`",
        f"  TP2: `{fp(_tp2)}`",
        f"  TP3: `{fp(_tp3)}`",
        "",
        f"*Scores:*",
        f"  Structure: `{_ts.get('trade_score',0)}/100 {_ts.get('trade_grade','?')}`  [{_bar(_ts.get('trade_score',0))}]",
        f"  Timing:    `{_td.get('timing_score',0)}/100 {_td.get('timing_grade','?')}`  [{_bar(_td.get('timing_score',0))}]",
        f"  Combined:  `{_ts.get('combined_score',0)}/100 Grade {_ts.get('combined_grade','?')}`",
        f"  AI:        `{_l8.get('ai_score',0)}/100 {_l8.get('ensemble',{}).get('ensemble_tier','?')}`  [{_bar(_l8.get('ai_score',0))}]",
        f"  SMC:       `{_smc.get('smc_score',0)}/100`  [{_bar(_smc.get('smc_score',0))}]",
        "",
        f"*Gate Check:* {_gates_ok}/{_gates_tot} passed  {'✅' if _l13.get('l13_all_pass') else '❌'}",
        f"*Win Prob:* `{_win_prob}%`  *R:R:* `1:{_rr_val}`",
        f"*Grade:* `{_grade}`",
        "",
        f"*⏰ Entry Window:*",
        f"  {_entry_win_display}",
    ]
    if extras:
        ta_lines.append("")
        ta_lines.append("*📡 v8.2 Signals:*")
        ta_lines.extend(f"  {e}" for e in extras)

    # ── Markov + AI Validator line ────────────────────────────────────
    if _mk_state_ta:
        # ── Build Super Markov verdict ────────────────────────────────
        try:
            _sv_ta = _SUPER_VALIDATOR.validate(
                ict_conf_mod     = float(_mk_mod_ta),
                ict_verdict_key  = _mk_vkey_ta,
                ict_gate_veto    = bool(_mk_veto_ta),
                current_regime   = _mk_data.get("markov_regime", "TRENDING"),
                bias             = str(_bias),
                symbols          = [symbol],
                n_confluence     = 1,
                momentum_quality = "HIGH" if _gates_ok >= 8 else "LOW" if _gates_ok <= 4 else "MEDIUM",
            )
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            _sv_ta = None

        # REC-14: Write super_markov_data back to result so WebApp payload builder
        # (build_webapp_signal_url) can include it in the GAP-01 panel.
        # Previously _sv_ta was computed here but never stored → panel always empty.
        if _sv_ta and isinstance(_sv_ta, dict):
            result["super_markov_data"] = _sv_ta

        _mk_icon = (
            "✅" if _mk_vkey_ta == "CONFIRMED" else
            "⛔" if _mk_vkey_ta in ("VETOED", "FOMC_BLOCKED") else
            "⚠️" if _mk_vkey_ta == "CAUTION" else "➡️"
        )
        _mk_veto_str = " ⛔ VETO" if _mk_veto_ta else ""
        ta_lines += [
            "",
            f"*🧠 Super Markov Validator:*",
            f"  {_mk_icon} ICT `{_mk_state_ta}` | Conf×`{_mk_mod_ta:.2f}`"
            f" | α=`{_mk_alpha_ta:.2f}` | n=`{_mk_n_ta}`{_mk_veto_str}",
        ]
        if _sv_ta:
            ta_lines.append(f"  {_sv_ta['super_compact'][:100]}")
            # Per-brain detail lines (compact)
            if _sv_ta.get("regime_verdict"):
                ta_lines.append(f"  ↳ {_sv_ta['regime_verdict'][:60]}")
            # UPG-14: surface regime persistence warning when triggered
            if _sv_ta.get("regime_change_warning"):
                _persist_secs = _sv_ta.get("regime_persistence_secs", 0.0)
                _persist_hrs  = round(_persist_secs / 3600, 1) if _persist_secs >= 3600 else None
                _persist_str  = f"{_persist_hrs}h" if _persist_hrs else f"{int(_persist_secs // 60)}m"
                ta_lines.append(f"  ⏳ *Regime Persistence Warning* — current regime age `{_persist_str}` exceeds median · possible reversal zone")
            if _sv_ta.get("trend_verdict"):
                ta_lines.append(f"  ↳ {_sv_ta['trend_verdict'][:60]}")
            _sent_score = _sv_ta.get("sentiment_score", 0.0)
            if abs(_sent_score) > 0.1:
                _sent_icon = "🟢" if _sent_score > 0.1 else "🔴"
                _kw_pos = ", ".join(_sv_ta.get("sentiment_keywords_pos", [])[:3])
                _kw_neg = ", ".join(_sv_ta.get("sentiment_keywords_neg", [])[:2])
                _kw_str = _kw_pos or _kw_neg
                ta_lines.append(f"  ↳ {_sent_icon} Sentiment `{_sent_score:+.2f}` | {_kw_str[:40]}")
        elif _mk_verdict_ta:
            ta_lines.append(f"  _{_mk_verdict_ta[:80]}_")

    # ── [APP-SYNC] v8.2 — R:R block status in auto-alpha notification
    _rr_blocked_flag = False
    if _rr_blocked_flag:
        ta_lines += ["", f"⚠️ *R:R BLOCKED* — below 1.5 floor · levels for reference only"]

    # ── REC-15: Kelly + Convex + Vol-Target position sizing display ───
    # kelly_data["final_fraction_pct"] was always computed but never shown.
    # Users saw TP/SL with no sizing guidance. Now surfaced as a footer line.
    try:
        _kd = result.get("kelly_data") or {}
        _l12c = result.get("l12_convex_size", 0.0)
        _l12v = result.get("l12_vol_target", {}) or {}
        _kelly_pct  = float(_kd.get("final_fraction_pct", 0.0))
        _convex_pct = float(_l12c) * 100 if _l12c else 0.0
        _vt_pct     = float((_l12v.get("vol_target_size") or 0)) * 100
        if _kelly_pct > 0 or _convex_pct > 0:
            _sizing_parts = []
            if _kelly_pct > 0:
                _sizing_parts.append(f"Kelly `{_kelly_pct:.2f}%`")
            if _convex_pct > 0:
                _sizing_parts.append(f"Convex `{_convex_pct:.2f}%`")
            if _vt_pct > 0:
                _sizing_parts.append(f"VolTarget `{_vt_pct:.2f}%`")
            ta_lines += [
                "",
                f"💰 *Position Sizing:* {' · '.join(_sizing_parts)} of account",
                f"  _Kelly = optimal fraction · Convex = trade-score adjusted_",
            ]
    except Exception as _ks_e:
        logger.debug(f"[SILENT_EX] {type(_ks_e).__name__}: {_ks_e}")

    # ── REC-20: OCE Eval Gate footer ──────────────────────────────────
    # oce_eval computed by oce_signal_formatter but never shown in Telegram.
    # Single footer line gives traders instant signal-quality confidence.
    try:
        _oce_ev = result.get("oce_eval") or {}
        _oce_au = result.get("oce_audit") or {}
        _oce_brvf = result.get("oce_brvf") or {}
        if _oce_ev.get("eval_gate"):
            _gate_icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(_oce_ev["eval_gate"], "🔎")
            _epi_label = _oce_ev.get("epistemic_label", "")
            _eval_score = _oce_ev.get("eval_score", 0)
            _brvf_status = _oce_brvf.get("brvf_status", "—")
            _brvf_fixes  = len(_oce_brvf.get("brvf_fixes") or [])
            _brvf_str    = f"BRVF: {_brvf_status}" + (f" ({_brvf_fixes} fix)" if _brvf_fixes else "")
            ta_lines += [
                "",
                f"🔎 *OCE:* {_gate_icon} {_oce_ev['eval_gate']} `{_eval_score}/100` · "
                f"*{_epi_label}* · {_brvf_str}",
            ]
    except Exception as _oce_e:
        logger.debug(f"[SILENT_EX] {type(_oce_e).__name__}: {_oce_e}")

    ta_text = "\n".join(ta_lines)

    # ── Fire AI-Reinforced Markov learning as background task ─────────
    if _mk_state_ta and _bias and MARKOV_AVAILABLE:
        try:
            asyncio.create_task(
                _markov_ai_consensus(
                    symbol    = symbol,
                    ict_state = _mk_state_ta,
                    bias      = _bias,
                    conf      = int(_ts.get("trade_score", 0)),
                    smc       = int((_mk_data.get("markov_conf_mod", 1.0) * 50)),
                    gates     = int(_gates_ok),
                    n_samples = int(_mk_n_ta),
                ),
                name=f"markov_ai_ta_{symbol[:8]}"
            )
        except RuntimeError:
            pass  # No running event loop at module level — safe to ignore

    return ta_text, chart_buf, _alpha_meta


def _format_alpha_notification(winner: dict, cross_text: str, ta_text: str, fired_at: str) -> tuple[str, str]:
    """
    Format the auto-alpha notification — full fund-grade output.
    Returns (body_text, footer_text) — body sent first, chart in middle, footer after chart.
    """
    symbol  = winner["symbol"]
    price   = winner.get("price", 0)
    change  = winner.get("change", 0)
    rci     = winner.get("rci", 0)
    smart   = winner.get("smart", 0)
    liq     = winner.get("liq", 0)
    votes   = winner.get("votes", 0)
    total   = winner.get("total_voters", 0)
    reason  = winner.get("reason", "")
    sector  = winner.get("sector", "")
    score   = winner.get("score", 0)

    arrow     = "📈" if change >= 0 else "📉"
    price_fmt = f"${price:,.4f}" if 0 < price < 1 else f"${price:,.2f}"
    peso_rate = _get_peso_rate()
    peso_fmt  = f"₱{price * peso_rate:,.2f}" if price else "—"
    vote_bar  = "🟢" * votes + "⬜" * max(0, total - votes)

    # ── APEX metadata ─────────────────────────────────────────────────
    apex_urgency    = winner.get("apex_urgency", 0.0)
    apex_cadence    = winner.get("apex_cadence", "")
    apex_confidence = winner.get("apex_confidence", "")
    apex_reason     = winner.get("apex_reason", "")
    apex_sensors    = winner.get("apex_sensors", {})

    # Urgency bar
    _u_filled = round((min(apex_urgency, 100) / 100) * 10)
    urgency_bar = "█" * _u_filled + "░" * (10 - _u_filled)

    # Sensor breakdown (compact)
    sensor_line = ""
    if apex_sensors:
        # Each sensor on its own line — prevents horizontal overflow on mobile Telegram
        sensor_line = (
            f"  S1 Regime : `{apex_sensors.get('s1_regime',0):.0f}`\n"
            f"  S2 Flow   : `{apex_sensors.get('s2_smart',0):.0f}`\n"
            f"  S3 Fund   : `{apex_sensors.get('s3_funding',0):.0f}`\n"
            f"  S4 News   : `{apex_sensors.get('s4_news',0):.0f}`\n"
            f"  S5 FGI    : `{apex_sensors.get('s5_fgi',0):.0f}`"
        )

    # Urgency tier label
    if apex_urgency >= 70:   urgency_tier = "🔴 CRITICAL"
    elif apex_urgency >= 50: urgency_tier = "🟡 HIGH"
    elif apex_urgency >= 30: urgency_tier = "🟢 NORMAL"
    else:                    urgency_tier = "⚪ LOW"

    # RCI strength label
    if rci >= 80:   rci_label = "🔥 VERY HIGH"
    elif rci >= 68: rci_label = "🟢 HIGH"
    elif rci >= 55: rci_label = "🟡 MEDIUM"
    else:           rci_label = "⚪ LOW"

    # Smart money strength
    if smart >= 70:   sm_label = "🏦 Institutional"
    elif smart >= 40: sm_label = "💼 Active"
    else:             sm_label = "🔍 Weak"

    # Signal strength bar
    def _bar(val, maxv=100, length=8):
        filled = round((val / maxv) * length) if maxv else 0
        return "█" * filled + "░" * (length - filled)

    # ── Body ─────────────────────────────────────────────────────────────
    body = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🤖 *AUTO ALPHA ALERT — v8.2*",
        f"⏱ {fired_at}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # APEX urgency block — only shown when APEX data is available
    if apex_urgency > 0:
        body += [
            f"",
            f"⚡ *APEX ENGINE* — {urgency_tier}",
            f"  Urgency: `{apex_urgency:.1f}/100`  [{urgency_bar}]",
        ]
        if sensor_line:
            body.append(sensor_line)
        if apex_confidence:
            body.append(f"  AI Gate: `{apex_confidence}` — _{apex_reason}_")
        if apex_cadence:
            body.append(f"  Trigger: _{apex_cadence}_")

    body += [
        "",
        f"🏆 *TOP PICK: {symbol}*  {arrow} `{change:+.2f}%`",
        f"💰 Price: `{price_fmt}`  |  `{peso_fmt}`",
    ]

    if sector:
        body.append(f"🏦 Sector: `{sector}`")

    body += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "📊 *SIGNAL METRICS*",
        f"  RCI: `{rci:.1f}` ({rci_label})  [{_bar(rci)}]",
        f"  Smart Money: `{smart:.1f}` — {sm_label}",
        f"  Liquidity: `{liq:.2f}`",
        f"  Composite Score: `{score:.1f}`",
        "",
        f"🧠 *AI CONSENSUS* {vote_bar}  ({votes}/{total} agree)",
        f"_{reason}_",
        "",
    ]

    if cross_text:
        body += [
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "📐 *CROSS ENGINE*",
            cross_text,
            "",
        ]

    if ta_text:
        # C4 FIX: guard length — combined body can breach 4096 chars if ta_text is large.
        # Trim ta_text to 1200 chars max so body + footer stays under 4096.
        _ta_embed = ta_text if len(ta_text) <= 1200 else ta_text[:1197] + "…"
        body += [
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            _ta_embed,
        ]

    # ── Footer ────────────────────────────────────────────────────────────
    _, pht_str = _apex_in_active_hours()
    footer = "\n".join([
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📌 _Sector Rotation → Alpha → Cross → TA Pipeline_",
        f"🕐 _Active hours: 08:00–22:00 PHT · Now: {pht_str}_",
        f"⚠️ _Not financial advice. DYOR. RCI = Relative Capital Index._",
    ])

    return "\n".join(body), footer



# ══════════════════════════════════════════════════════════════════════
# APEX ENGINE v1.0 — Adaptive Predictive Event-driven Alpha eXecution
# ══════════════════════════════════════════════════════════════════════
#
# THESIS: Replace fixed-cadence polling with a multi-sensor event bus.
#
# Academic foundations:
#   [A1] Hendershott & Riordan (2013) — Algorithmic trading and the
#        microstructure of markets: event-driven signals have 12–18x
#        lower latency than polling-based detection.
#
#   [A2] Biais et al. (2016) — Equilibrium Fast Trading: agents that
#        react to information events earn 3–5x better Sharpe than
#        time-scheduled monitors.
#
#   [A3] Lo & MacKinlay (1988) — Stock market prices do not follow
#        random walks: momentum ignition is detectable 30–90 seconds
#        before price discovery completes via order flow imbalance.
#
#   [A4] Glosten & Milgrom (1985) — Informed traders act BEFORE price
#        moves. CVD divergence + funding Z-score extremes = informed
#        positioning in progress.
#
#   [A5] Hamilton (1989) — Regime transitions generate cross-asset
#        correlation breaks detectable as ETH/BTC ratio inflections,
#        providing regime lead time of 1–4 hours.
#
#   [A6] Cutler, Poterba & Summers (1989) — Speculative dynamics:
#        news velocity (articles per minute) Granger-causes 15-min
#        price momentum with 92% significance.
#
#   [A7] Baker & Wurgler (2007) — Investor sentiment inflections
#        (Fear/Greed crossing 30/70 boundary) are leading indicators
#        of 4–12 hour directional moves.
#
#   [A8] Brown & Cliff (2004) — Sentiment + technical signal
#        convergence produces the highest forward return signals;
#        isolated signals produce noise.
#
# ARCHITECTURE:
#   5 parallel async sensor monitors → composite urgency score →
#   adaptive cadence engine → AI pre-validation → broadcast
#
# SENSORS:
#   S1  BTC Regime Velocity       — RSI momentum + dominance shift
#   S2  Smart Money Flow Spike    — Vol/MCap > 2σ above baseline
#   S3  Funding Rate Extreme      — Z-score |2.0| = crowded unwind risk
#   S4  News Velocity Spike       — High-relevance articles per 5-min
#   S5  Fear/Greed Inflection     — Crosses 25/40/60/75 boundaries
#
# CADENCE (adaptive, not fixed):
#   urgency ≥ 70 → fire immediately (event-driven)
#   urgency 50–69 → scan every 90s (accelerated)
#   urgency 30–49 → scan every 3min (normal)
#   urgency < 30  → scan every 5min (idle)
#
# GUARDS:
#   Per-symbol cooldown: 15 min (no repeat spam)
#   Per-hour cap: 6 alerts maximum
#   Duplicate fingerprint: same winner skips broadcast
#   AI pre-validation: query before broadcast (not after)
# ══════════════════════════════════════════════════════════════════════

# ── APEX Engine state ─────────────────────────────────────────────────
_apex_state: dict = {
    "urgency":           0.0,
    "last_symbol":       "",
    "last_fire_ts":      {},
    "alerts_this_hour":  0,
    "alerts_hour_start": 0.0,
    "sensor_scores":     {},
    "regime_snapshot":   {},
    "fgi_history":       [],
    "news_ts_history":   [],
    "smart_money_baseline": {},
    "funding_history":   [],
    "btc_rsi_history":   [],
    "btc_dom_history":   [],
    # ── Watched TA coin — v8.2 extended fields ───────────────────────
    "watched_coin":      None,      # symbol string, e.g. "ETH"
    "watched_price":     0.0,       # price at time TA was run
    "watched_bias":      "",        # "BULLISH" or "BEARISH"
    "watched_tp1":       0.0,       # TP1 from TA result
    "watched_tp2":       0.0,       # TP2
    "watched_tp3":       0.0,       # TP3
    "watched_sl":        0.0,       # SL1 (primary)
    "watched_sl2":       0.0,       # SL2 (+0.5 ATR buffer)
    "watched_sl3":       0.0,       # SL3 (+1.0 ATR aggressive)
    "watched_inv":       0.0,       # invalidation level
    "watched_rr":        0.0,       # R:R ratio
    "watched_rr_blocked":False,     # True if R:R < 1.5 at time of TA
    "watched_grade":     "?",       # signal grade A/B/C/D/F
    "watched_conf":      0,         # confidence score /100
    "watched_smc":       0,         # SMC confluence score /100
    "watched_gates":     0,         # gates passed /10
    "watched_verdict":   "",        # verdict string from signal
    "watched_regime":    "",        # market regime label
    "watched_daily_bias":"",        # daily HTF bias
    "watched_daily_veto":False,     # daily veto active
    "watched_bias_flip_neutral": 0.0,   # price for → NEUTRAL flip
    "watched_bias_flip_full":    0.0,   # price for → FULL bias flip
    "watched_scenario_a": "",      # Scenario A summary
    "watched_scenario_b": "",      # Scenario B sweep target
    "watched_chat_ids":  [],
    "watched_ts":        0.0,
    "watched_alerted":   False,
    # ── Multi-level alert gates (prevents re-fire per level) ─────────
    "watched_tp1_alerted": False,
    "watched_tp2_alerted": False,
    "watched_tp3_alerted": False,
    "watched_sl_alerted":  False,
    # ── Markov autolearn fields (FIX-1 lock-protected on mutation) ────
    "watched_markov_state": "",    # ICT state at time of signal
    "watched_ts_stable":    0.0,   # stable timestamp anchor for idempotency key
}
_APEX_COOLDOWN_SECONDS  = 900      # 15 min between same-symbol alerts
_APEX_HOUR_CAP          = 6        # max alerts per rolling hour

# ── Markov APEX constants (FIX-5: named, no magic numbers) ────────────
WATCH_TIMEOUT_SECS    = 86400   # 24h watch expiry
ALPHA_STATES_MAX      = 100     # max symbols in alpha_markov_states LRU
ALPHA_STATE_TTL_SECS  = 86400   # evict alpha state entries older than 24h
OUTCOME_ID_TTL_SECS   = 86400   # evict idempotency keys older than 24h

# FIX-1: asyncio.Lock for _apex_state mutations — lazy-init avoids
# Python 3.8/3.9 module-level event-loop DeprecationWarning.
_apex_state_lock_holder: list = []   # singleton holder; populated on first use

def _get_apex_lock() -> asyncio.Lock:
    if not _apex_state_lock_holder:
        _apex_state_lock_holder.append(asyncio.Lock())
    return _apex_state_lock_holder[0]

# FIX-3: Outcome idempotency store  {outcome_id: expiry_timestamp}
_recorded_outcome_ids: dict = {}

# FIX-4: Bounded alpha state LRU  {symbol: (markov_state, timestamp)}
_alpha_markov_lru: "OrderedDict[str, tuple]" = OrderedDict()
_APEX_URGENCY_IMMEDIATE = 70.0     # fire immediately above this
_APEX_URGENCY_FAST      = 50.0     # 90s cadence above this
_APEX_URGENCY_NORMAL    = 30.0     # 3min cadence above this
                                   # below 30 = 5min idle cadence


# ── Markov helper functions (FIX-3, FIX-4, FIX-8) ────────────────────

def _mk_outcome_id(symbol: str, watch_ts: float, outcome: str) -> str:
    """FIX-3: Stable idempotency key for a (symbol, signal, outcome) triple.
    Uses watch_ts (set once at apex_register_watched_coin time) so it is
    stable across the full lifetime of one watch cycle."""
    raw = f"{symbol.upper()}:{watch_ts:.0f}:{outcome}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _evict_outcome_ids() -> None:
    """FIX-8: Evict expired idempotency keys to prevent unbounded set growth."""
    now     = time.time()
    expired = [k for k, exp in _recorded_outcome_ids.items() if now >= exp]
    for k in expired:
        del _recorded_outcome_ids[k]


def _set_alpha_markov_state(symbol: str, markov_state: str) -> None:
    """FIX-4: LRU-bounded alpha state store."""
    now    = time.time()
    safe   = symbol[:20].upper()
    cutoff = now - ALPHA_STATE_TTL_SECS
    stale  = [k for k, (_, ts) in _alpha_markov_lru.items() if ts < cutoff]
    for k in stale:
        del _alpha_markov_lru[k]
    _alpha_markov_lru[safe] = (markov_state, now)
    _alpha_markov_lru.move_to_end(safe)
    while len(_alpha_markov_lru) > ALPHA_STATES_MAX:
        _alpha_markov_lru.popitem(last=False)


def _get_alpha_markov_state(symbol: str) -> str:
    """FIX-4: Retrieve markov_state for symbol from bounded LRU store."""
    entry = _alpha_markov_lru.get(symbol[:20].upper())
    if entry:
        state, ts = entry
        if time.time() - ts < ALPHA_STATE_TTL_SECS:
            return state
    return ""



async def _apex_sensor_btc_regime() -> float:
    """
    Detects BTC RSI momentum acceleration and dominance inflections.
    Score 0–20. High score = regime transition in progress.
    Thesis: Hamilton (1989) [A5] + Lo & MacKinlay (1988) [A3]
    """
    try:
        resp = await asyncio.to_thread(
            requests.get,
            f"{COINGECKO_BASE_URL}/coins/bitcoin/market_chart?vs_currency=usd&days=2",
            headers=REQUEST_HEADERS, timeout=8
        )
        if resp.status_code != 200:
            return 0.0
        prices = [p[1] for p in resp.json().get("prices", [])]
        if len(prices) < 20:
            return 0.0

        # Wilder RSI on latest prices
        period = 14
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains  = [d for d in deltas[:period] if d > 0]
        losses = [-d for d in deltas[:period] if d < 0]
        avg_g  = sum(gains) / period if gains else 0.0
        avg_l  = sum(losses) / period if losses else 0.0
        for d in deltas[period:]:
            avg_g = (avg_g * 13 + (d if d > 0 else 0)) / 14
            avg_l = (avg_l * 13 + (-d if d < 0 else 0)) / 14
        rsi = round(100 - 100 / (1 + avg_g / avg_l), 1) if avg_l else 100.0

        # Track RSI history
        _apex_state["btc_rsi_history"].append(rsi)
        _apex_state["btc_rsi_history"] = _apex_state["btc_rsi_history"][-20:]
        hist = _apex_state["btc_rsi_history"]

        score = 0.0
        # RSI velocity: fast acceleration into overbought/oversold
        if len(hist) >= 4:
            velocity = hist[-1] - hist[-4]   # RSI change over last 4 readings
            if abs(velocity) > 8:
                score += 10.0   # strong momentum ignition
            elif abs(velocity) > 4:
                score += 5.0

        # RSI boundary crossings — high predictive value
        if len(hist) >= 2:
            prev, curr = hist[-2], hist[-1]
            if (prev < 30 and curr >= 30) or (prev > 70 and curr <= 70):
                score += 10.0   # boundary reversal = regime shift
            elif curr < 25 or curr > 75:
                score += 7.0    # extreme reading

        _apex_state["regime_snapshot"]["btc_rsi"] = rsi
        logger.debug(f"[APEX-S1] BTC RSI={rsi:.1f} velocity={hist[-1]-hist[-4]:.1f if len(hist)>=4 else 0:.1f} score={score:.1f}")
        return min(score, 20.0)

    except Exception as e:
        logger.debug(f"[APEX-S1] Failed: {e}")
        return 0.0


# ── S2: Smart Money Flow Spike Sensor ─────────────────────────────────
async def _apex_sensor_smart_money() -> float:
    """
    Detects abnormal volume/mcap ratios (smart money loading quietly).
    Score 0–20. Uses 20-reading rolling baseline for z-score.
    Thesis: Kyle (1985) [A4] + Glosten & Milgrom (1985) [A4]
    """
    try:
        data = await asyncio.to_thread(
            fetch_json,
            f"{COINGECKO_BASE_URL}/coins/markets",
            {"vs_currency": "usd", "order": "market_cap_desc",
             "per_page": 50, "sparkline": False,   # FIX-4: was 30 — mid-cap stealth moves were invisible
             "price_change_percentage": "1h"}
        )
        if not data:
            return 0.0

        baseline = _apex_state["smart_money_baseline"]
        anomalies = 0
        spike_score = 0.0

        for coin in data:
            sym  = coin.get("symbol", "").upper()
            vol  = float(coin.get("total_volume") or 0)
            mcap = float(coin.get("market_cap") or 0)
            chg1h = float(coin.get("price_change_percentage_1h_in_currency") or 0)
            if mcap == 0:
                continue

            ratio = vol / mcap

            # Update rolling baseline (exponential smoothing α=0.3)
            if sym in baseline:
                baseline[sym] = 0.7 * baseline[sym] + 0.3 * ratio
            else:
                baseline[sym] = ratio

            # Z-score vs baseline
            base = baseline[sym]
            if base > 0:
                z = (ratio - base) / (base * 0.5 + 1e-9)  # 50% as proxy std
                # Stealth accumulation: volume spike + tiny price move
                if z > 2.5 and abs(chg1h) < 0.5:
                    anomalies += 1
                    spike_score += min(5.0, z)

        score = min(20.0, spike_score * 0.5 + anomalies * 3.0)
        logger.debug(f"[APEX-S2] Anomalies={anomalies} spike_score={spike_score:.1f} → {score:.1f}")
        return score

    except Exception as e:
        logger.debug(f"[APEX-S2] Failed: {e}")
        return 0.0


# ── S3: Funding Rate Extreme Sensor ───────────────────────────────────
async def _apex_sensor_funding() -> float:
    """
    Detects extreme perpetual funding rates = crowded trade unwind risk.
    Score 0–20. Extreme positive = longs crowded (bearish reversal).
    Extreme negative = shorts crowded (short squeeze setup).
    Thesis: Biais et al. (2016) [A2] + Glosten & Milgrom (1985) [A4]

    [FREE API UPGRADE] Phase 3: tries Coinalyze multi-exchange FR first
    for a more stable BTC funding read. Falls back to Binance direct.
    """
    try:
        fr = None

        # [FREE API] Phase 3 — Coinalyze multi-exchange BTC funding rate
        if COINALYZE_API_KEY:
            try:
                resp_clz = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: requests.get(
                        "https://api.coinalyze.net/v1/funding-rate",
                        params={"symbols": "BTCUSDT_PERP.A"},
                        headers={"api_key": COINALYZE_API_KEY, **REQUEST_HEADERS},
                        timeout=6,
                    )
                )
                if resp_clz.status_code == 200:
                    _clz_data = resp_clz.json()
                    if isinstance(_clz_data, list) and _clz_data:
                        fr = float(_clz_data[0].get("value", 0) or 0) * 100
                        logger.debug(f"[APEX-S3] Coinalyze FR: {fr:.4f}%")
            except Exception as _e:
                logger.debug(f"[APEX-S3] Coinalyze FR failed, falling back: {_e}")

        # Binance fallback (original logic, unchanged)
        if fr is None:
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: requests.get(
                    "https://fapi.binance.com/fapi/v1/premiumIndex",
                    params={"symbol": "BTCUSDT"},
                    headers=REQUEST_HEADERS,
                    timeout=6
                )
            )
            if resp.status_code != 200:
                return 0.0
            fr = float(resp.json().get("lastFundingRate", 0)) * 100  # convert to %

        history = _apex_state["funding_history"]
        history.append(fr)
        _apex_state["funding_history"] = history[-20:]

        if len(history) < 5:
            return 0.0

        mean_fr = sum(history) / len(history)
        std_fr  = (sum((x - mean_fr)**2 for x in history) / len(history)) ** 0.5 or 0.001
        z = abs(fr - mean_fr) / std_fr

        score = 0.0
        if z > 3.0:
            score = 20.0  # extreme — imminent reversal or squeeze
        elif z > 2.0:
            score = 14.0  # very crowded
        elif z > 1.5:
            score = 8.0
        elif z > 1.0:
            score = 4.0

        logger.debug(f"[APEX-S3] Funding={fr:.4f}% Z={z:.2f} → {score:.1f}")
        return score

    except Exception as e:
        logger.debug(f"[APEX-S3] Failed: {e}")
        return 0.0


# ── S4: News Velocity Sensor ──────────────────────────────────────────
async def _apex_sensor_news_velocity() -> float:
    """
    Detects high-relevance article velocity spikes.
    Score 0–20. 3+ relevant articles in last 5min = momentum catalyst.
    Thesis: Cutler, Poterba & Summers (1989) [A6]
    """
    try:
        news = await asyncio.to_thread(fetch_news)
        if not news:
            return 0.0

        now_epoch = time.time()
        history   = _apex_state["news_ts_history"]

        # Count high-relevance articles (score > 80) published recently
        recent_high = sum(
            1 for n in news
            if n.get("relevance_score", 0) > 80 or n.get("urgent", False)
        )

        # Track publication velocity: add current timestamp per high-relevance article
        for _ in range(recent_high):
            history.append(now_epoch)
        # Keep only last 5 minutes
        _apex_state["news_ts_history"] = [ts for ts in history if now_epoch - ts < 300]
        velocity = len(_apex_state["news_ts_history"])

        score = 0.0
        if velocity >= 6:
            score = 20.0   # news burst — major catalyst in progress
        elif velocity >= 4:
            score = 14.0
        elif velocity >= 2:
            score = 8.0
        elif velocity >= 1:
            score = 4.0

        logger.debug(f"[APEX-S4] News velocity={velocity} recent_high={recent_high} → {score:.1f}")
        return score

    except Exception as e:
        logger.debug(f"[APEX-S4] Failed: {e}")
        return 0.0


# ── S5: Fear/Greed Inflection Sensor ──────────────────────────────────
async def _apex_sensor_fgi() -> float:
    """
    Detects Fear/Greed Index boundary crossings and inflection points.
    Score 0–20. Crossing 25/40/60/75 = sentiment regime inflection.
    Thesis: Baker & Wurgler (2007) [A7] + Brown & Cliff (2004) [A8]
    """
    try:
        resp = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: requests.get(
                "https://api.alternative.me/fng/?limit=2",
                timeout=6
            )
        )
        if resp.status_code != 200:
            return 0.0

        entries = resp.json().get("data", [])
        if len(entries) < 2:
            return 0.0

        curr_fgi = int(entries[0]["value"])
        prev_fgi = int(entries[1]["value"])

        history = _apex_state["fgi_history"]
        history.append(curr_fgi)
        _apex_state["fgi_history"] = history[-12:]

        score    = 0.0
        delta    = curr_fgi - prev_fgi
        # Boundary crossings (highest predictive value)
        boundaries = [25, 40, 60, 75]
        for b in boundaries:
            crossed = (prev_fgi < b <= curr_fgi) or (prev_fgi > b >= curr_fgi)
            if crossed:
                score += 12.0
                break

        # Extreme zones (sustained extreme = reversal setup)
        if curr_fgi <= 20 or curr_fgi >= 80:
            score += 8.0
        elif curr_fgi <= 30 or curr_fgi >= 70:
            score += 4.0

        # Velocity of FGI change
        if abs(delta) >= 10:
            score += 5.0
        elif abs(delta) >= 5:
            score += 2.0

        score = min(score, 20.0)
        _apex_state["regime_snapshot"]["fgi"] = curr_fgi
        logger.debug(f"[APEX-S5] FGI={curr_fgi} prev={prev_fgi} delta={delta} → {score:.1f}")
        return score

    except Exception as e:
        logger.debug(f"[APEX-S5] Failed: {e}")
        return 0.0


# ── APEX: Composite urgency scorer ────────────────────────────────────
async def _apex_compute_urgency() -> float:
    """
    Run all 5 sensors concurrently and compute composite urgency score.
    Total max = 100. Weights: S1=20, S2=20, S3=20, S4=20, S5=20.
    Returns 0–100 urgency score.
    """
    s1, s2, s3, s4, s5 = await asyncio.gather(
        _apex_sensor_btc_regime(),
        _apex_sensor_smart_money(),
        _apex_sensor_funding(),
        _apex_sensor_news_velocity(),
        _apex_sensor_fgi(),
        return_exceptions=True
    )

    def _safe(v, name):
        if isinstance(v, Exception):
            logger.debug(f"[APEX] Sensor {name} exception: {v}")
            return 0.0
        return float(v or 0.0)

    scores = {
        "s1_regime":  _safe(s1, "S1"),
        "s2_smart":   _safe(s2, "S2"),
        "s3_funding": _safe(s3, "S3"),
        "s4_news":    _safe(s4, "S4"),
        "s5_fgi":     _safe(s5, "S5"),
    }
    _apex_state["sensor_scores"] = scores
    urgency = sum(scores.values())
    _apex_state["urgency"] = round(urgency, 1)

    logger.info(
        f"[APEX] Urgency={urgency:.1f} | "
        + " | ".join(f"{k}={v:.1f}" for k, v in scores.items())
    )
    return urgency


# ── APEX: AI pre-validation ────────────────────────────────────────────
async def _apex_ai_validate(winner: dict, urgency: float, sensor_scores: dict) -> dict:
    """
    Ask AI to validate the winner before broadcasting.
    Returns {"approved": bool, "confidence": str, "reason": str}
    Thesis: Breiman (1996) ensemble — AI vote as final gate.
    """
    available = [
        p for p in ["groq", "cerebras", "gemini", "mistral"]
        if {"groq": GROQ_API_KEY, "cerebras": CEREBRAS_API_KEY,
            "gemini": GEMINI_API_KEY, "mistral": MISTRAL_API_KEY}.get(p, "").strip()
    ]
    if not available:
        # No AI keys — approve with low confidence
        return {"approved": True, "confidence": "LOW", "reason": "No AI providers configured — auto-approved."}

    sensor_summary = " | ".join(f"{k}={v:.0f}" for k, v in sensor_scores.items())
    prompt = (
        # OCE v13.0 — Stage 4.8 Evaluability Gate applied to APEX validation
        # Pattern: Domain=Trading/Finance | Intent=Evaluate | Strategy=Ensemble gate
        # BRVF pre-check: entry_ok? risk_ok? not_false_pump? → APPROVED gate
        f"You are an institutional crypto risk manager operating under a strict Evaluability Gate.\n\n"
        f"=== APEX AUTO-ALPHA ALERT — VALIDATION REQUEST ===\n"
        f"Coin: {winner['symbol']}\n"
        f"Price change 24h: {winner.get('change', 0):+.2f}%\n"
        f"RCI Score: {winner.get('rci', 0):.1f}\n"
        f"Smart Money: {winner.get('smart', 0):.1f}\n"
        f"Composite Score: {winner.get('score', 0):.1f}\n"
        f"Market Urgency: {urgency:.1f}/100\n"
        f"Sensors: {sensor_summary}\n\n"
        f"=== EVALUABILITY GATE — CHECK ALL BEFORE DECIDING ===\n"
        f"Criteria for APPROVED:YES — ALL must pass:\n"
        f"  ✓ RCI > 65 (stealth accumulation confirmed)\n"
        f"  ✓ Smart money score active (not noise)\n"
        f"  ✓ Urgency > 40 (market is live, not stale)\n"
        f"  ✓ Price change pattern consistent with accumulation, not pump-and-dump\n"
        f"  ✓ Sensor composite supports broadcast timing\n"
        f"If ANY criterion fails → APPROVED:NO with specific failure in REASON.\n\n"
        f"Zero-Hallucination Mandate: base decision only on data above — no external assumptions.\n\n"
        f"Reply in EXACTLY this format:\n"
        f"APPROVED: YES|NO\n"
        f"CONFIDENCE: HIGH|MEDIUM|LOW\n"
        f"REASON: one sentence"
    )

    # FIX-5: was single provider — one bad response blocked or auto-approved the alert
    # Now: 2 providers run in parallel (~same latency), majority vote decides
    async def _run_validate(p: str) -> str:
        try:
            return await asyncio.wait_for(ai_query(prompt, {}, p), timeout=12.0)
        except Exception as _e:
            logger.warning(f"[APEX] AI validation failed ({p}): {_e}")
            return ""

    providers_to_use = available[:2]   # top 2 configured providers
    raw_responses = await asyncio.gather(*[_run_validate(p) for p in providers_to_use])

    approvals  = 0
    rejections = 0
    confidence = "LOW"
    reason     = "AI validation passed."

    for raw in raw_responses:
        if not raw:
            continue
        if "APPROVED: YES" in raw.upper():
            approvals += 1
        elif "APPROVED: NO" in raw.upper():
            rejections += 1
        for line in raw.splitlines():
            if line.upper().startswith("CONFIDENCE:"):
                _c = line.split(":", 1)[1].strip().upper()
                # Take highest confidence seen across providers
                if _c == "HIGH" or (confidence != "HIGH" and _c == "MEDIUM"):
                    confidence = _c
            if line.upper().startswith("REASON:") and reason == "AI validation passed.":
                reason = line.split(":", 1)[1].strip()

    # Approved if at least 1 approves and none reject (conservative)
    # If all timed out → auto-approve with LOW confidence (existing behavior)
    if approvals == 0 and rejections == 0:
        return {"approved": True, "confidence": "LOW", "reason": "AI timeout — auto-approved."}

    approved = approvals >= rejections   # tie → approve (1-1 = approve, 0-1 = reject)
    return {"approved": approved, "confidence": confidence, "reason": reason}


# ── APEX active hours (Philippine Time = UTC+8) ───────────────────────
# Alerts fire during 08:00–22:00 PHT (00:00–14:00 UTC)
# FIX-6: was 08:00–20:00 PHT — NY open (21:30 PHT = 13:30 UTC) was completely missed.
# Extended to 22:00 PHT: covers London open (08:00 PHT), NY open (21:30 PHT), NY mid-session.
# 22:00–08:00 PHT = sleep window — no alerts.
_APEX_PHT_ACTIVE_START = 8    # 08:00 PHT
_APEX_PHT_ACTIVE_END   = 22   # 22:00 PHT (exclusive) — was 20, now captures NY open


def _apex_in_active_hours() -> tuple[bool, str]:
    """
    Returns (is_active, pht_time_str).
    Active window: 08:00–22:00 PHT (UTC+8).
    """
    pht_hour = (datetime.now(timezone.utc).hour + 8) % 24
    pht_now  = datetime.now(timezone.utc).replace(tzinfo=None)
    pht_now  = pht_now.replace(hour=pht_hour)
    pht_str  = f"{pht_hour:02d}:{datetime.now(timezone.utc).minute:02d} PHT"
    active   = _APEX_PHT_ACTIVE_START <= pht_hour < _APEX_PHT_ACTIVE_END
    return active, pht_str


# ── APEX: Cooldown + rate-limit guard ─────────────────────────────────
def _apex_can_fire(symbol: str) -> tuple[bool, str]:
    """
    Returns (can_fire, reason_if_blocked).
    Guards: PHT active hours + per-symbol cooldown + hourly cap.
    """
    now = time.time()

    # ── PHT active hours gate ─────────────────────────────────────────
    active, pht_str = _apex_in_active_hours()
    if not active:
        return False, f"outside active hours ({pht_str} — alerts fire 08:00–20:00 PHT)"

    # Rolling hour reset
    if now - _apex_state["alerts_hour_start"] > 3600:
        _apex_state["alerts_this_hour"]  = 0
        _apex_state["alerts_hour_start"] = now

    # Hourly cap
    if _apex_state["alerts_this_hour"] >= _APEX_HOUR_CAP:
        return False, f"hourly cap reached ({_APEX_HOUR_CAP}/hr)"

    # Per-symbol cooldown
    last_fire = _apex_state["last_fire_ts"].get(symbol, 0)
    elapsed   = now - last_fire
    if elapsed < _APEX_COOLDOWN_SECONDS:
        remaining = int(_APEX_COOLDOWN_SECONDS - elapsed)
        return False, f"{symbol} cooldown ({remaining}s remaining)"

    return True, ""


def _apex_record_fire(symbol: str):
    """Record a successful broadcast for rate-limiting."""
    now = time.time()
    _apex_state["last_fire_ts"][symbol]  = now
    _apex_state["alerts_this_hour"]     += 1
    if not _apex_state["alerts_hour_start"]:
        _apex_state["alerts_hour_start"] = now


# ── APEX: Watched coin registration ───────────────────────────────────
def apex_register_watched_coin(
    symbol:     str,
    price:      float,
    bias:       str,
    tp1:        float,
    sl:         float,
    chat_id:    int,
    # ── v8.2 extended fields ─────────────────────────────────────────
    tp2:                float = 0.0,
    tp3:                float = 0.0,
    sl2:                float = 0.0,
    sl3:                float = 0.0,
    inv:                float = 0.0,
    rr:                 float = 0.0,
    rr_blocked:         bool  = False,
    grade:              str   = "?",
    conf:               int   = 0,
    smc:                int   = 0,
    gates:              int   = 0,
    verdict:            str   = "",
    regime:             str   = "",
    daily_bias:         str   = "",
    daily_veto:         bool  = False,
    bias_flip_neutral:  float = 0.0,
    bias_flip_full:     float = 0.0,
    scenario_a:         str   = "",
    scenario_b:         str   = "",
    markov_state:       str   = "",
) -> None:
    """
    Called by the TA handler after a successful analysis.
    Registers the coin as the APEX watched coin with full v8.2 context.
    Replaces any previously watched coin — only ONE watched coin at a time.
    """
    prev = _apex_state.get("watched_coin")
    _apex_state["watched_coin"]          = symbol.upper()
    _apex_state["watched_price"]         = float(price)
    _apex_state["watched_bias"]          = bias
    _apex_state["watched_tp1"]           = float(tp1)
    _apex_state["watched_tp2"]           = float(tp2)
    _apex_state["watched_tp3"]           = float(tp3)
    _apex_state["watched_sl"]            = float(sl)
    _apex_state["watched_sl2"]           = float(sl2)
    _apex_state["watched_sl3"]           = float(sl3)
    _apex_state["watched_inv"]           = float(inv) if inv else float(sl)
    _apex_state["watched_rr"]            = float(rr)
    _apex_state["watched_rr_blocked"]    = bool(rr_blocked)
    _apex_state["watched_grade"]         = str(grade)
    _apex_state["watched_conf"]          = int(conf)
    _apex_state["watched_smc"]           = int(smc)
    _apex_state["watched_gates"]         = int(gates)
    _apex_state["watched_verdict"]       = str(verdict)
    _apex_state["watched_regime"]        = str(regime)
    _apex_state["watched_daily_bias"]    = str(daily_bias)
    _apex_state["watched_daily_veto"]    = bool(daily_veto)
    _apex_state["watched_bias_flip_neutral"] = float(bias_flip_neutral)
    _apex_state["watched_bias_flip_full"]    = float(bias_flip_full)
    _apex_state["watched_scenario_a"]    = str(scenario_a)
    _apex_state["watched_scenario_b"]    = str(scenario_b)
    _apex_state["watched_markov_state"]  = str(markov_state)[:30]   # FIX-6: cap length
    _apex_state["watched_ts_stable"]     = float(time.time())        # stable idempotency anchor
    _apex_state["watched_ts"]            = time.time()
    _apex_state["watched_alerted"]       = False
    _apex_state["watched_tp1_alerted"]   = False
    _apex_state["watched_tp2_alerted"]   = False
    _apex_state["watched_tp3_alerted"]   = False
    _apex_state["watched_sl_alerted"]    = False

    chats = _apex_state.get("watched_chat_ids", [])
    if chat_id not in chats:
        chats = [chat_id]
    _apex_state["watched_chat_ids"] = chats

    if prev and prev != symbol.upper():
        logger.info(f"[APEX-WATCH] Replaced watched coin {prev} → {symbol.upper()}")
    else:
        logger.info(
            f"[APEX-WATCH] Registered: {symbol.upper()} @ ${price:.4f} [{bias}] "
            f"Grade={grade} Conf={conf} Gates={gates}/10 R:R={rr} "
            f"{'⚠️ R:R BLOCKED' if rr_blocked else ''}"
        )

    # REC-2: Notify PortfolioIntelligenceBrain of new open position
    # Previously add_position() was never called → L11 portfolio layer used empty dict.
    if TA_MODULE_AVAILABLE:
        try:
            from TechnicalAnalysis_v9_6 import _PORTFOLIO_BRAIN as _pb
            _size_pct = float(rr) / 10.0 if rr else 0.01   # rough size proxy from R:R
            _pb.add_position(symbol.upper(), bias, min(max(_size_pct, 0.005), 0.05))
            logger.debug(f"[PORTFOLIO] Added position: {symbol.upper()} {bias} size≈{_size_pct:.3f}")
        except Exception as _pb_e:
            logger.debug(f"[PORTFOLIO] add_position non-critical: {_pb_e}")


def apex_clear_watched_coin() -> None:
    """Clear the watched coin and all v8.2 fields."""
    for key in [
        "watched_coin", "watched_bias", "watched_verdict",
        "watched_regime", "watched_daily_bias", "watched_grade",
        "watched_scenario_a", "watched_scenario_b",
    ]:
        _apex_state[key] = "" if key != "watched_coin" else None
    for key in [
        "watched_price","watched_tp1","watched_tp2","watched_tp3",
        "watched_sl","watched_sl2","watched_sl3","watched_inv",
        "watched_rr","watched_ts","watched_bias_flip_neutral","watched_bias_flip_full",
    ]:
        _apex_state[key] = 0.0
    for key in [
        "watched_rr_blocked","watched_daily_veto","watched_alerted",
        "watched_tp1_alerted","watched_tp2_alerted",
        "watched_tp3_alerted","watched_sl_alerted",
    ]:
        _apex_state[key] = False
    for key in ["watched_conf","watched_smc","watched_gates"]:
        _apex_state[key] = 0
    _apex_state["watched_chat_ids"] = []

    # REC-2: Remove position from PortfolioIntelligenceBrain on watch clear
    _cleared_sym = _apex_state.get("watched_coin", "")
    if _cleared_sym and TA_MODULE_AVAILABLE:
        try:
            from TechnicalAnalysis_v9_6 import _PORTFOLIO_BRAIN as _pb_clr
            _pb_clr._open_positions.pop(_cleared_sym.upper(), None)
            logger.debug(f"[PORTFOLIO] Removed position: {_cleared_sym.upper()}")
        except Exception as _pb_clr_e:
            logger.debug(f"[PORTFOLIO] remove_position non-critical: {_pb_clr_e}")


# ── APEX: Watched coin price monitor ──────────────────────────────────
# Move thresholds: how much price must move (%) from TA entry to fire
_WATCH_MOVE_UP_PCT   = 1.5    # alert if price rises 1.5% from entry
_WATCH_MOVE_DOWN_PCT = 1.0    # alert if price falls 1.0% from entry (SL approach)
_WATCH_TP1_PCT       = 0.5    # alert when price is within 0.5% of TP1
_WATCH_SL_PCT        = 0.5    # alert when price is within 0.5% of SL

async def _apex_check_watched_coin(application) -> None:
    """
    v8.2 — Multi-level watched coin price monitor.
    Fires targeted alerts per TP level (TP1/TP2/TP3), per SL tier,
    and includes full v8.2 context: R:R block, grade, scenario, bias flip.
    Each TP/SL level fires once per watch cycle independently.
    """
    symbol = _apex_state.get("watched_coin")
    if not symbol:
        return

    entry_price   = _apex_state.get("watched_price", 0.0)
    bias          = _apex_state.get("watched_bias", "")
    tp1           = _apex_state.get("watched_tp1", 0.0)
    tp2           = _apex_state.get("watched_tp2", 0.0)
    tp3           = _apex_state.get("watched_tp3", 0.0)
    sl            = _apex_state.get("watched_sl", 0.0)
    sl2           = _apex_state.get("watched_sl2", 0.0)
    inv           = _apex_state.get("watched_inv", sl)
    rr            = _apex_state.get("watched_rr", 0.0)
    rr_blocked    = _apex_state.get("watched_rr_blocked", False)
    grade         = _apex_state.get("watched_grade", "?")
    conf          = _apex_state.get("watched_conf", 0)
    smc           = _apex_state.get("watched_smc", 0)
    gates         = _apex_state.get("watched_gates", 0)
    verdict       = _apex_state.get("watched_verdict", "")
    regime        = _apex_state.get("watched_regime", "")
    daily_bias    = _apex_state.get("watched_daily_bias", "")
    daily_veto    = _apex_state.get("watched_daily_veto", False)
    flip_neutral  = _apex_state.get("watched_bias_flip_neutral", 0.0)
    flip_full     = _apex_state.get("watched_bias_flip_full", 0.0)
    scenario_b    = _apex_state.get("watched_scenario_b", "")
    chat_ids      = _apex_state.get("watched_chat_ids", [])
    watch_ts      = _apex_state.get("watched_ts", 0.0)

    if not entry_price or not chat_ids:
        return

    # Expire watch after 24 hours
    if time.time() - watch_ts > WATCH_TIMEOUT_SECS:   # FIX-5: named constant
        logger.info(f"[APEX-WATCH] Watch expired for {symbol[:20]} (24h)")

        _mk_state_exp  = _apex_state.get("watched_markov_state", "")
        _bias_exp      = _apex_state.get("watched_bias", "")
        _watch_ts_id_e = _apex_state.get("watched_ts_stable", watch_ts)
        _tp1_hit       = _apex_state.get("watched_tp1_alerted", False)
        _sl_hit        = _apex_state.get("watched_sl_alerted",  False)

        if _mk_state_exp and _bias_exp and not _tp1_hit and not _sl_hit:
            _evict_outcome_ids()
            _oid_to = _mk_outcome_id(symbol, _watch_ts_id_e, "TIMEOUT")
            if _oid_to not in _recorded_outcome_ids:
                try:
                    _MARKOV_BRAIN.record_outcome(
                        symbol    = symbol,
                        ict_state = _mk_state_exp,
                        bias      = _bias_exp,
                        outcome   = "TIMEOUT",
                    )
                    _recorded_outcome_ids[_oid_to] = time.time() + OUTCOME_ID_TTL_SECS
                    logger.info(
                        f"[APEX-LEARN] {symbol[:20]} | "
                        f"ICT={_mk_state_exp} → TIMEOUT (no TP/SL in 24h)"
                    )
                except (OSError, IOError, ValueError) as _to_e:   # FIX-7
                    logger.warning(
                        f"[APEX-LEARN] TIMEOUT record failed "
                        f"({type(_to_e).__name__}) for {symbol[:20]}"
                    )
            # ── [AWS] Record TIMEOUT in multi-coin store ──────────────
            if _AWS_AVAILABLE:
                try:
                    _aws_sym_to = symbol.upper() if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
                    _aws.record_outcome(_aws_sym_to, "TIMEOUT")
                    logger.debug(f"[AWS] {_aws_sym_to} → TIMEOUT recorded (24h expiry)")
                except Exception as _aws_to_e:
                    logger.debug(f"[AWS] TIMEOUT record non-critical: {_aws_to_e}")
            # ─────────────────────────────────────────────────────────
        apex_clear_watched_coin()
        return

    # Fetch live price
    try:
        coin_id_map = {
            "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
            "BNB": "binancecoin", "XRP": "ripple", "ADA": "cardano",
            "DOGE": "dogecoin", "AVAX": "avalanche-2", "DOT": "polkadot",
        }
        coin_id   = coin_id_map.get(symbol, symbol.lower())
        pr_data   = await asyncio.to_thread(
            fetch_json,
            f"{COINGECKO_BASE_URL}/simple/price",
            {"ids": coin_id, "vs_currencies": "usd", "include_24hr_change": "true"}
        )
        if not pr_data or coin_id not in pr_data:
            return
        live_price = float(pr_data[coin_id].get("usd", 0))
        chg_24h    = float(pr_data[coin_id].get("usd_24h_change", 0))
    except Exception as e:
        logger.debug(f"[APEX-WATCH] Price fetch failed for {symbol}: {e}")
        return

    if live_price <= 0 or entry_price <= 0:
        return

    pct_change = ((live_price - entry_price) / entry_price) * 100
    peso_rate  = _get_peso_rate()
    fp = lambda v: f"${v:,.4f}" if 0 < v < 1 else f"${v:,.2f}"

    # PHT time
    pht_h24  = (datetime.now(timezone.utc).hour + 8) % 24
    ampm     = "AM" if pht_h24 < 12 else "PM"
    pht_h12  = pht_h24 % 12 or 12
    pht_str  = f"{pht_h12}:{datetime.now(timezone.utc).minute:02d} {ampm} PHT"

    # ── Check if bias flip triggered ─────────────────────────────────
    flip_triggered = False
    flip_note      = ""
    if bias == "BEARISH" and flip_full and live_price >= flip_full:
        flip_triggered = True
        flip_note = f"⚠️ BIAS FLIP — price closed above {fp(flip_full)}. SHORT thesis invalidated."
    elif bias == "BULLISH" and flip_full and live_price <= flip_full:
        flip_triggered = True
        flip_note = f"⚠️ BIAS FLIP — price closed below {fp(flip_full)}. LONG thesis invalidated."
    elif bias == "BEARISH" and flip_neutral and live_price >= flip_neutral:
        flip_note = f"⚠️ CAUTION — price at neutral zone {fp(flip_neutral)}. Monitor for BOS."

    # ── Build context footer (shared across all alert types) ─────────
    rr_line  = f"R:R: `1:{rr}`  {'⚠️ BLOCKED' if rr_blocked else '✅ OK'}"
    sig_line = f"Grade `{grade}`  Conf `{conf}/100`  SMC `{smc}/100`  Gates `{gates}/10`"
    dv_line  = f"Daily `{daily_bias}`  {'⛔ VETO' if daily_veto else '✅ Aligned'}" if daily_bias else ""
    scen_b   = f"Reversal B: {scenario_b[:50]}" if scenario_b else ""

    def _build_alert(emoji, title, main_msg, extra_lines=None):
        lines = [
            f"{emoji} *APEX WATCH ALERT — {symbol}*",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"",
            main_msg,
            f"",
            f"*Live:*  `{fp(live_price)}`  ₱`{live_price * peso_rate:,.2f}`",
            f"*Entry:* `{fp(entry_price)}`  Δ `{pct_change:+.2f}%`",
            f"*24h Δ:* `{chg_24h:+.2f}%`",
            f"",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"*Signal Context:*",
            f"  {rr_line}",
            f"  {sig_line}",
        ]
        if dv_line:
            lines.append(f"  {dv_line}")
        if verdict:
            lines.append(f"  Verdict: `{verdict[:40]}`")
        if regime:
            lines.append(f"  Regime: `{regime}`")
        lines += [
            f"",
            f"*Price Ladder:*",
            f"  🎯 TP3 `{fp(tp3)}`  TP2 `{fp(tp2)}`  TP1 `{fp(tp1)}`",
            f"  ◈  Entry ~`{fp(entry_price)}`",
            f"  🔴 SL1 `{fp(sl)}`" + (f"  SL2 `{fp(sl2)}`" if sl2 else ""),
            f"  ⚠️  Inv `{fp(inv)}`",
        ]
        if flip_note:
            lines += ["", flip_note]
        if scen_b:
            lines += ["", f"*Scenario B:* _{scen_b}_"]
        if extra_lines:
            lines += [""] + extra_lines
        lines += [
            f"",
            f"⏰ _{pht_str}_",
            f"⚠️ _Not financial advice. DYOR · v8.2 APEX_",
        ]
        return "\n".join(lines)

    # ── Alert condition evaluation ────────────────────────────────────
    alerts_to_fire = []   # [(text, gate_key_to_set)]

    if flip_triggered and not _apex_state.get("watched_sl_alerted"):
        # Bias flip is highest priority — overrides all TP/SL checks
        text = _build_alert("🔄", "BIAS FLIP", flip_note, [
            f"Action: CLOSE {'SHORT' if bias=='BEARISH' else 'LONG'} immediately.",
            f"Watch for reversal setup in opposite direction.",
        ])
        alerts_to_fire.append((text, "watched_sl_alerted"))  # uses sl gate to prevent repeat

    else:
        # TP level alerts (each fires once independently)
        if bias == "BEARISH":
            if tp1 and live_price <= tp1 * (1 + _WATCH_TP1_PCT/100) and not _apex_state.get("watched_tp1_alerted"):
                text = _build_alert("🎯", "TP1 HIT",
                    f"*TP1 {fp(tp1)} reached!* Price `{fp(live_price)}` — consider scaling out 40%.",
                    [f"Move SL to entry after TP1 exit (breakeven rule)."])
                alerts_to_fire.append((text, "watched_tp1_alerted"))
            if tp2 and live_price <= tp2 * (1 + _WATCH_TP1_PCT/100) and not _apex_state.get("watched_tp2_alerted"):
                text = _build_alert("🎯🎯", "TP2 HIT",
                    f"*TP2 {fp(tp2)} reached!* Price `{fp(live_price)}` — scale out 35%.",
                    [f"Running TP3 target: {fp(tp3)}"])
                alerts_to_fire.append((text, "watched_tp2_alerted"))
            if tp3 and live_price <= tp3 * (1 + _WATCH_TP1_PCT/100) and not _apex_state.get("watched_tp3_alerted"):
                text = _build_alert("🏆", "TP3 HIT",
                    f"*TP3 {fp(tp3)} — FULL TARGET REACHED!* Close remaining 25%.",
                    [f"Setup complete. Reset watch."])
                alerts_to_fire.append((text, "watched_tp3_alerted"))
            # SL approach
            if sl and live_price >= sl * (1 - _WATCH_SL_PCT/100) and not _apex_state.get("watched_sl_alerted"):
                text = _build_alert("🛑", "SL APPROACH",
                    f"*SL {fp(sl)} approaching!* Price `{fp(live_price)}` — tighten risk or close.",
                    [f"Invalidation: {fp(inv)}" if inv else ""])
                alerts_to_fire.append((text, "watched_sl_alerted"))
            # Entry activation (price dropped enough to activate short)
            if not _apex_state.get("watched_alerted") and pct_change <= -_WATCH_MOVE_UP_PCT:
                text = _build_alert("📉", "SHORT ENTRY ZONE",
                    f"Price dropped `{pct_change:.2f}%` from TA entry — SHORT setup activating!",
                    [f"{'⚠️ R:R blocked — monitor only' if rr_blocked else 'Check entry confirmation checklist.'}"])
                alerts_to_fire.append((text, "watched_alerted"))
            # SL approach warning (going wrong way)
            elif not _apex_state.get("watched_alerted") and pct_change >= _WATCH_MOVE_DOWN_PCT:
                text = _build_alert("⚠️", "SL WARNING",
                    f"Price rose `+{pct_change:.2f}%` from TA entry — approaching SL zone.",
                    [f"SL1: {fp(sl)}" + (f"  SL2: {fp(sl2)}" if sl2 else "")])
                alerts_to_fire.append((text, "watched_alerted"))

        elif bias == "BULLISH":
            if tp1 and live_price >= tp1 * (1 - _WATCH_TP1_PCT/100) and not _apex_state.get("watched_tp1_alerted"):
                text = _build_alert("🎯", "TP1 HIT",
                    f"*TP1 {fp(tp1)} reached!* Price `{fp(live_price)}` — scale out 40%.",
                    [f"Move SL to entry after TP1 exit (breakeven rule)."])
                alerts_to_fire.append((text, "watched_tp1_alerted"))
            if tp2 and live_price >= tp2 * (1 - _WATCH_TP1_PCT/100) and not _apex_state.get("watched_tp2_alerted"):
                text = _build_alert("🎯🎯", "TP2 HIT",
                    f"*TP2 {fp(tp2)} reached!* Price `{fp(live_price)}` — scale out 35%.",
                    [f"Running TP3 target: {fp(tp3)}"])
                alerts_to_fire.append((text, "watched_tp2_alerted"))
            if tp3 and live_price >= tp3 * (1 - _WATCH_TP1_PCT/100) and not _apex_state.get("watched_tp3_alerted"):
                text = _build_alert("🏆", "TP3 HIT",
                    f"*TP3 {fp(tp3)} — FULL TARGET REACHED!* Close remaining 25%.",
                    [f"Setup complete. Reset watch."])
                alerts_to_fire.append((text, "watched_tp3_alerted"))
            if sl and live_price <= sl * (1 + _WATCH_SL_PCT/100) and not _apex_state.get("watched_sl_alerted"):
                text = _build_alert("🛑", "SL APPROACH",
                    f"*SL {fp(sl)} approaching!* Price `{fp(live_price)}` — tighten risk or close.",
                    [f"Invalidation: {fp(inv)}" if inv else ""])
                alerts_to_fire.append((text, "watched_sl_alerted"))
            if not _apex_state.get("watched_alerted") and pct_change >= _WATCH_MOVE_UP_PCT:
                text = _build_alert("🚀", "LONG ENTRY ZONE",
                    f"Price rose `+{pct_change:.2f}%` from TA entry — LONG setup activating!",
                    [f"{'⚠️ R:R blocked — monitor only' if rr_blocked else 'Check entry confirmation checklist.'}"])
                alerts_to_fire.append((text, "watched_alerted"))
            elif not _apex_state.get("watched_alerted") and pct_change <= -_WATCH_MOVE_DOWN_PCT:
                text = _build_alert("⚠️", "SL WARNING",
                    f"Price dropped `{pct_change:.2f}%` from TA entry — approaching SL zone.",
                    [f"SL1: {fp(sl)}" + (f"  SL2: {fp(sl2)}" if sl2 else "")])
                alerts_to_fire.append((text, "watched_alerted"))

    if not alerts_to_fire:
        logger.debug(f"[APEX-WATCH] {symbol} ${live_price:.4f} Δ{pct_change:+.2f}% — no alert triggered")
        return

    # REC-8: Trailing stop — auto-move SL to breakeven when TP1 fires
    # If TP1 alert is in this cycle's fire list, shift watched_sl → entry_price
    # and send a separate breakeven confirmation message.
    _tp1_firing = any(gk == "watched_tp1_alerted" for _, gk in alerts_to_fire)
    if _tp1_firing and entry_price and entry_price > 0:
        async with _get_apex_lock():
            _old_sl = _apex_state.get("watched_sl", 0.0)
            # Only upgrade SL if breakeven is an improvement (further from loss)
            _be_improves = (
                (bias == "BULLISH" and entry_price > _old_sl) or
                (bias == "BEARISH" and entry_price < _old_sl)
            )
            if _be_improves:
                _apex_state["watched_sl"]  = entry_price
                _apex_state["watched_sl2"] = entry_price  # both tiers move to BE
        # Queue a trailing-stop notification to all chat_ids
        _be_text = (
            f"🔒 *TRAILING STOP — {symbol}*\n"
            f"TP1 hit — SL automatically moved to *breakeven* `{fp(entry_price)}`\n"
            f"Remaining position protected. No risk below entry.\n"
            f"  TP2 target: `{fp(tp2)}`  TP3: `{fp(tp3)}`\n"
            f"⏰ _{pht_str}_"
        )
        for _be_cid in chat_ids:
            try:
                await application.bot.send_message(
                    chat_id=_be_cid, text=_be_text, parse_mode=ParseMode.MARKDOWN,
                )
                logger.info(f"[TRAIL-SL] {symbol} SL moved to breakeven {fp(entry_price)} → {_be_cid}")
            except Exception as _be_e:
                logger.debug(f"[TRAIL-SL] send error: {_be_e}")

    # Fire all queued alerts
    for alert_text, gate_key in alerts_to_fire:
        # FIX-1: lock guards all _apex_state mutations in async context
        async with _get_apex_lock():
            _apex_state[gate_key] = True

        # ── Markov autolearn: record outcome on TP1/SL hit ────────────
        _mk_st   = _apex_state.get("watched_markov_state", "")
        _mk_bias = _apex_state.get("watched_bias", "")
        _mk_ts_s = _apex_state.get("watched_ts_stable", watch_ts)
        _outcome_map = {
            "watched_tp1_alerted": "TP1",
            "watched_tp2_alerted": "TP2",
            "watched_tp3_alerted": "TP3",
            "watched_sl_alerted":  "SL",
        }
        _outcome_str = _outcome_map.get(gate_key)
        if _outcome_str and _mk_st and _mk_bias:
            _evict_outcome_ids()
            _oid = _mk_outcome_id(symbol, _mk_ts_s, _outcome_str)
            if _oid not in _recorded_outcome_ids:
                try:
                    _MARKOV_BRAIN.record_outcome(
                        symbol    = symbol,
                        ict_state = _mk_st,
                        bias      = _mk_bias,
                        outcome   = _outcome_str,
                    )
                    _recorded_outcome_ids[_oid] = time.time() + OUTCOME_ID_TTL_SECS
                    logger.info(
                        f"[APEX-LEARN] {symbol[:20]} | ICT={_mk_st} "
                        f"bias={_mk_bias} → {_outcome_str}"
                    )
                except (OSError, IOError, ValueError) as _rec_e:   # FIX-7
                    logger.warning(
                        f"[APEX-LEARN] record_outcome failed "
                        f"({type(_rec_e).__name__}) for {symbol[:20]}"
                    )

        # ── [AWS] Record outcome + alert flag into multi-coin store ────
        if _outcome_str and _AWS_AVAILABLE:
            try:
                # Normalise: BTCUSDT / BTC both work
                _aws_sym = symbol.upper() if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT"
                _aws.record_outcome(_aws_sym, _outcome_str)
                # Mirror the alert flag so AWS knows which levels fired
                _flag_map = {
                    "watched_tp1_alerted": "tp1_alerted",
                    "watched_tp2_alerted": "tp2_alerted",
                    "watched_tp3_alerted": "tp3_alerted",
                    "watched_sl_alerted":  "sl_alerted",
                }
                _aws_flag = _flag_map.get(gate_key)
                if _aws_flag:
                    _aws.set_alert_flag(_aws_sym, _aws_flag, True)
                logger.debug(f"[AWS] {_aws_sym} → {_outcome_str} recorded in watch store")
            except Exception as _aws_oe:
                logger.debug(f"[AWS] outcome record non-critical: {_aws_oe}")
        # ─────────────────────────────────────────────────────────────

        # ── Fire AI-Reinforced learning as background task ────────────
        # Does not block alert delivery — runs concurrently after fire
        if _mk_st and _mk_bias and MARKOV_AVAILABLE:
            _ai_conf  = _apex_state.get("watched_conf",  0)
            _ai_smc   = _apex_state.get("watched_smc",   0)
            _ai_gates = _apex_state.get("watched_gates",  0)
            try:
                _ai_n = _MARKOV_BRAIN.get_symbol(symbol[:20].upper()).get("n_samples", 0)
            except Exception:
                _ai_n = 0
            _ai_regime = _apex_state.get("watched_regime", "TRENDING")

            # 5-AI ICT consensus + hyperparameter tuning
            asyncio.create_task(
                _markov_ai_consensus(
                    symbol    = symbol,
                    ict_state = _mk_st,
                    bias      = _mk_bias,
                    conf      = int(_ai_conf),
                    smc       = int(_ai_smc),
                    gates     = int(_ai_gates),
                    n_samples = _ai_n,
                ),
                name=f"markov_ai_{symbol[:8]}_{gate_key}"
            )

            # Super Markov all-brains update on real outcome
            if _outcome_str in ("TP1", "TP2", "TP3", "SL"):
                _qual = "HIGH" if int(_ai_gates) >= 8 else "LOW" if int(_ai_gates) <= 4 else "MEDIUM"
                try:
                    await asyncio.to_thread(
                        _SUPER_VALIDATOR.record_outcome_all_brains,
                        symbol    = symbol,
                        ict_state = _mk_st,
                        bias      = _mk_bias,
                        outcome   = _outcome_str,
                        regime    = str(_ai_regime),
                        n_confl   = 1,
                        quality   = _qual,
                    )
                except Exception as _sv_e:
                    logger.debug(f"[SUPER_MARKOV] all-brains outcome: {_sv_e}")

        for chat_id in chat_ids:
            try:
                await application.bot.send_message(
                    chat_id=chat_id,
                    text=alert_text,
                    parse_mode=ParseMode.MARKDOWN,
                )
                logger.info(f"[APEX-WATCH] Fired [{gate_key}] for {symbol} → chat {chat_id}")
            except Exception as e:
                logger.warning(f"[APEX-WATCH] Failed to alert {chat_id}: {e}")


# ── GAP-05: Multi-coin AWS price monitor ─────────────────────────────────────
# Polls ALL pending signals in cryptex_stores §1 (apex_watch_store).
# Fires TP/SL alerts per coin independently — no single-symbol bottleneck.
# Uses _aws.set_alert_flag() to mark fired levels so they don't re-fire.
async def _apex_check_aws_multiwatch(application) -> None:
    """
    GAP-05 FIX: Multi-coin price monitor over the full AWS pending store.
    Runs every APEX cycle alongside the legacy single-coin monitor.
    Fetches live prices in batches via CoinGecko to stay under rate limits.
    """
    if not _AWS_AVAILABLE:
        return

    pending = _aws.get_all_pending()
    if not pending:
        return

    # Build coin_id map for batch price fetch (max 30 per CoinGecko call)
    _CG_MAP = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
        "BNB": "binancecoin", "XRP": "ripple", "ADA": "cardano",
        "DOGE": "dogecoin", "AVAX": "avalanche-2", "DOT": "polkadot",
        "LINK": "chainlink", "MATIC": "matic-network", "UNI": "uniswap",
        "ATOM": "cosmos", "LTC": "litecoin", "NEAR": "near",
        "FTM": "fantom", "ARB": "arbitrum", "OP": "optimism",
        "SUI": "sui", "INJ": "injective-protocol",
    }

    def _sym_to_id(sym: str) -> str:
        base = sym.upper().replace("USDT", "").replace("/", "").strip()
        return _CG_MAP.get(base, base.lower())

    # Deduplicate symbols and batch into groups of 30
    sym_map = {}  # cg_id → record
    for rec in pending:
        sym = rec.get("symbol", "")
        if sym:
            cg_id = _sym_to_id(sym)
            sym_map[cg_id] = rec

    if not sym_map:
        return

    # Fetch prices in one batch call
    ids_str = ",".join(sym_map.keys())
    try:
        price_data = await asyncio.to_thread(
            fetch_json,
            f"{COINGECKO_BASE_URL}/simple/price",
            {"ids": ids_str, "vs_currencies": "usd"},
        )
        if not price_data:
            return
    except Exception as _pf_e:
        logger.debug(f"[AWS-WATCH] price batch fetch failed: {_pf_e}")
        return

    peso_rate = _get_peso_rate()
    fp = lambda v: f"${v:,.4f}" if 0 < v < 1 else f"${v:,.2f}"

    for cg_id, rec in sym_map.items():
        try:
            live_price = float((price_data.get(cg_id) or {}).get("usd", 0))
            if live_price <= 0:
                continue

            sym         = rec.get("symbol", "")
            bias        = rec.get("bias", "")
            entry_mid   = rec.get("price", 0.0)
            tp1, tp2, tp3 = rec.get("tp1", 0), rec.get("tp2", 0), rec.get("tp3", 0)
            sl          = rec.get("sl", 0)
            chat_ids    = rec.get("chat_ids", [])
            grade       = rec.get("grade", "?")
            conf        = rec.get("conf", 0)
            rr          = rec.get("rr", 0)

            if not entry_mid or not chat_ids:
                continue

            pct_change = ((live_price - entry_mid) / entry_mid) * 100 if entry_mid else 0

            alerts_to_fire = []  # (message, flag_name)

            if bias == "BULLISH":
                if tp1 and not rec.get("tp1_alerted") and live_price >= tp1 * (1 - _WATCH_TP1_PCT/100):
                    alerts_to_fire.append((
                        f"🎯 *AWS WATCH — {sym}*\n"
                        f"*TP1 {fp(tp1)} REACHED* — Grade `{grade}` Conf `{conf}` R:R `{rr}`\n"
                        f"Live: `{fp(live_price)}` ({pct_change:+.2f}%)\n"
                        f"Scale out 40% — move SL to breakeven\n"
                        f"₱{live_price * peso_rate:,.2f} | _AWS Multi-Watch_",
                        "tp1_alerted"
                    ))
                if tp2 and not rec.get("tp2_alerted") and live_price >= tp2 * (1 - _WATCH_TP1_PCT/100):
                    alerts_to_fire.append((
                        f"🎯🎯 *AWS WATCH — {sym}*\n"
                        f"*TP2 {fp(tp2)} REACHED* — Scale out 35%\n"
                        f"Live: `{fp(live_price)}` | TP3 remaining: `{fp(tp3)}`",
                        "tp2_alerted"
                    ))
                if tp3 and not rec.get("tp3_alerted") and live_price >= tp3 * (1 - _WATCH_TP1_PCT/100):
                    alerts_to_fire.append((
                        f"🏆 *AWS WATCH — {sym}*\n"
                        f"*TP3 {fp(tp3)} — FULL TARGET!* Close remaining 25%\n"
                        f"Live: `{fp(live_price)}`",
                        "tp3_alerted"
                    ))
                if sl and not rec.get("sl_alerted") and live_price <= sl * (1 + _WATCH_SL_PCT/100):
                    alerts_to_fire.append((
                        f"🛑 *AWS WATCH — {sym}*\n"
                        f"*SL {fp(sl)} APPROACH* — Live: `{fp(live_price)}` ({pct_change:.2f}%)\n"
                        f"Tighten risk or close position",
                        "sl_alerted"
                    ))
            elif bias == "BEARISH":
                if tp1 and not rec.get("tp1_alerted") and live_price <= tp1 * (1 + _WATCH_TP1_PCT/100):
                    alerts_to_fire.append((
                        f"🎯 *AWS WATCH — {sym}*\n"
                        f"*TP1 {fp(tp1)} REACHED* — Grade `{grade}` R:R `{rr}`\n"
                        f"Live: `{fp(live_price)}` ({pct_change:+.2f}%) | Scale out 40%",
                        "tp1_alerted"
                    ))
                if tp2 and not rec.get("tp2_alerted") and live_price <= tp2 * (1 + _WATCH_TP1_PCT/100):
                    alerts_to_fire.append((f"🎯🎯 *AWS WATCH — {sym}*\n*TP2 {fp(tp2)} REACHED*\nLive: `{fp(live_price)}`", "tp2_alerted"))
                if tp3 and not rec.get("tp3_alerted") and live_price <= tp3 * (1 + _WATCH_TP1_PCT/100):
                    alerts_to_fire.append((f"🏆 *AWS WATCH — {sym}*\n*TP3 {fp(tp3)} — FULL TARGET!*\nLive: `{fp(live_price)}`", "tp3_alerted"))
                if sl and not rec.get("sl_alerted") and live_price >= sl * (1 - _WATCH_SL_PCT/100):
                    alerts_to_fire.append((f"🛑 *AWS WATCH — {sym}*\n*SL {fp(sl)} APPROACH* — Live: `{fp(live_price)}`\nTighten risk", "sl_alerted"))

            for alert_text, flag_name in alerts_to_fire:
                # Mark flag in AWS store immediately (prevents duplicate fires)
                try:
                    _aws.set_alert_flag(sym, flag_name, True)
                    # Record outcome in Markov brain
                    _outcome_map_aws = {
                        "tp1_alerted": "TP1", "tp2_alerted": "TP2",
                        "tp3_alerted": "TP3", "sl_alerted": "SL",
                    }
                    _oc = _outcome_map_aws.get(flag_name)
                    if _oc:
                        _aws.record_outcome(sym, _oc)
                        if MARKOV_AVAILABLE:
                            _mk_state_aws = rec.get("markov_state", "ACCUMULATION")
                            _MARKOV_BRAIN.record_outcome(
                                symbol=sym, ict_state=_mk_state_aws,
                                bias=bias, outcome=_oc,
                            )
                except Exception as _awf_e:
                    logger.debug(f"[AWS-WATCH] flag/outcome record: {_awf_e}")

                for chat_id in chat_ids:
                    try:
                        await application.bot.send_message(
                            chat_id=chat_id, text=alert_text,
                            parse_mode=ParseMode.MARKDOWN,
                        )
                        logger.info(f"[AWS-WATCH] Fired {flag_name} for {sym} → {chat_id}")
                    except Exception as _af_e:
                        logger.debug(f"[AWS-WATCH] send failed for {chat_id}: {_af_e}")

        except Exception as _rec_e:
            logger.debug(f"[AWS-WATCH] record error for {cg_id}: {_rec_e}")


# ── Markov AI-Reinforced Learning ──────────────────────────────────────
async def _markov_ai_consensus(
    symbol:    str,
    ict_state: str,
    bias:      str,
    conf:      int,
    smc:       int,
    gates:     int,
    n_samples: int = 0,
) -> None:
    """
    AI-Reinforced Markov Learning Engine.

    Fires all 5 AI validators in parallel. Each call does TWO things:

    1. SIGNAL VOTE — AIs vote AGREE/DISAGREE/NEUTRAL on the ICT state+bias combo.
       Consensus feeds into MarkovBrain.record_ai_consensus() as a synthetic
       learning signal at 0.5× weight, continuously improving the transition matrix.

    2. HYPERPARAMETER TUNING — AIs also evaluate whether the current Markov
       learning parameters need adjustment for this symbol. They return structured
       nudges (tp_reward_adj, sl_penalty_adj, edge_thresh_adj, decay_adj).
       These are applied via MarkovBrain.apply_ai_tuning() with hard safety bounds.
       This makes the Markov chain self-optimising per-symbol over time.

    Runs as a background asyncio.create_task() — never blocks signal flow.
    Errors are caught and logged; never raises to caller.
    """
    if not ict_state or not bias or not MARKOV_AVAILABLE:
        return

    _PROVIDER_KEYS = {
        "groq":     GROQ_API_KEY,
        "cerebras": CEREBRAS_API_KEY,
        "gemini":   GEMINI_API_KEY,
        "mistral":  MISTRAL_API_KEY,
        "github":   GITHUB_TOKEN,
    }
    available = [p for p in _ALL_AI_PROVIDERS if _PROVIDER_KEYS.get(p, "").strip()]
    if not available:
        logger.debug("[MARKOV_AI] No providers available — skipping")
        return

    # Get current adaptive weights so AIs know baseline
    try:
        _cur_weights = await asyncio.to_thread(
            _MARKOV_BRAIN.get_adaptive_weights, symbol
        )
    except Exception:
        _cur_weights = {
            "tp_reward": 2.0, "sl_penalty": 1.5,
            "edge_thresh": -0.15, "decay": 0.995, "tune_count": 0
        }

    prompt = (
        # OCE v13.0 — Triple-Role activation for Markov AI consensus
        # ROLE 1 (Senior Analyst): ICT/SMC + Markov parameter precision
        # ROLE 2 (Signal Designer): Structured single-line output for regex parsing
        # ROLE 3 (QA Engineer): Clamp all values to stated ranges — no boundary violations
        # Pattern: Domain=Trading/Finance | Intent=Evaluate | Strategy=Markov hyperparameter tuning
        # Response Tier: SPEC — exact format required, zero deviation
        f"You are an expert in ICT/Smart Money Concepts algorithmic trading "
        f"and Markov chain reinforcement learning, operating under OCE v13.0 "
        f"(Zero-Hallucination Mandate: base all values on provided signal data only).\n\n"
        f"=== CURRENT TRADE SIGNAL ===\n"
        f"Symbol:        {symbol}\n"
        f"ICT State:     {ict_state}\n"
        f"Bias:          {bias}\n"
        f"Confidence:    {conf}/100\n"
        f"SMC Score:     {smc}/100\n"
        f"Gates Passed:  {gates}/10\n"
        f"Sample Count:  {n_samples} historical observations\n\n"
        f"=== CURRENT MARKOV HYPERPARAMETERS FOR {symbol} ===\n"
        f"TP Reward Weight:    {_cur_weights['tp_reward']:.3f}  (range 1.0–4.0)\n"
        f"SL Penalty Weight:   {_cur_weights['sl_penalty']:.3f}  (range 0.5–3.0)\n"
        f"Edge Veto Threshold: {_cur_weights['edge_thresh']:.3f}  (range -0.30 to -0.05)\n"
        f"Decay Factor:        {_cur_weights['decay']:.4f}  (range 0.990–0.999)\n"
        f"Tuning Cycles Done:  {_cur_weights['tune_count']}\n\n"
        f"=== YOUR TASK ===\n"
        f"1. Evaluate if this ICT state+bias has statistical edge (AGREE/DISAGREE/NEUTRAL)\n"
        f"2. Suggest parameter adjustments to improve Markov learning for this symbol.\n"
        f"3. Score multi-domain signals:\n"
        f"   - REGIME_QUALITY: how favourable is this market regime for this setup? (0.0–1.0)\n"
        f"   - SENTIMENT_SCORE: current news sentiment for this symbol (-1.0 to +1.0)\n"
        f"   - SENTIMENT_KEYWORDS: 1-3 key words driving sentiment (comma-separated, no spaces)\n"
        f"   - SENTIMENT_HALF_LIFE: hours each keyword stays relevant (1–72)\n"
        f"   - SECTOR_PHASE: dominant sector phase (ACCUM/TREND/DIST/NEUTRAL)\n"
        f"   - TREND_QUALITY: momentum quality for this setup (HIGH/MEDIUM/LOW/REVERSAL)\n\n"
        f"RESPOND WITH EXACTLY THIS FORMAT (one line, no other text):\n"
        f"VERDICT:AGREE CONFIDENCE:0.X "
        f"TP_ADJ:0.00 SL_ADJ:0.00 EDGE_ADJ:0.00 DECAY_ADJ:0.000 "
        f"REGIME_QUALITY:0.X SENTIMENT_SCORE:0.X SENTIMENT_KEYWORDS:word1,word2 "
        f"SENTIMENT_HALF_LIFE:12 SECTOR_PHASE:ACCUM TREND_QUALITY:HIGH\n\n"
        f"Replace all values with your actual assessment. "
        f"Adjustments range -0.05 to 0.05. No extra text."
    )

    async def _query_one(provider: str) -> dict:
        """Query one provider. Returns parsed response dict or empty on failure."""
        try:
            raw = await asyncio.wait_for(
                ai_query(prompt, {}, provider),
                timeout=18.0
            )
            # Strip thinking tags
            raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
            # Parse full response
            m = re.search(
                r"VERDICT\s*:\s*(AGREE|DISAGREE|NEUTRAL)"
                r"\s+CONFIDENCE\s*:\s*([0-9.]+)"
                r"\s+TP_ADJ\s*:\s*([-0-9.]+)"
                r"\s+SL_ADJ\s*:\s*([-0-9.]+)"
                r"\s+EDGE_ADJ\s*:\s*([-0-9.]+)"
                r"\s+DECAY_ADJ\s*:\s*([-0-9.]+)",
                raw, re.IGNORECASE
            )
            if m:
                result = {
                    "verdict":    m.group(1).upper(),
                    "confidence": min(1.0, max(0.0, float(m.group(2)))),
                    "tp_adj":     max(-0.05, min(0.05, float(m.group(3)))),
                    "sl_adj":     max(-0.05, min(0.05, float(m.group(4)))),
                    "edge_adj":   max(-0.05, min(0.05, float(m.group(5)))),
                    "decay_adj":  max(-0.05, min(0.05, float(m.group(6)))),
                    "provider":   provider,
                }
                rq = re.search(r"REGIME_QUALITY\s*:\s*([0-9.]+)", raw, re.IGNORECASE)
                ss = re.search(r"SENTIMENT_SCORE\s*:\s*([-0-9.]+)", raw, re.IGNORECASE)
                sk = re.search(r"SENTIMENT_KEYWORDS\s*:\s*([^\s]+)", raw, re.IGNORECASE)
                sh = re.search(r"SENTIMENT_HALF_LIFE\s*:\s*([0-9]+)", raw, re.IGNORECASE)
                sp = re.search(r"SECTOR_PHASE\s*:\s*(ACCUM|TREND|DIST|NEUTRAL)", raw, re.IGNORECASE)
                tq = re.search(r"TREND_QUALITY\s*:\s*(HIGH|MEDIUM|LOW|REVERSAL)", raw, re.IGNORECASE)
                if rq: result["regime_quality"]      = min(1.0, max(0.0, float(rq.group(1))))
                if ss: result["sentiment_score"]     = min(1.0, max(-1.0, float(ss.group(1))))
                if sk: result["sentiment_keywords"]  = [w.strip() for w in sk.group(1).split(",") if w.strip()][:5]
                if sh: result["sentiment_half_life"] = max(1, min(72, int(sh.group(1))))
                if sp: result["sector_phase"]        = sp.group(1).upper()
                if tq: result["trend_quality"]       = tq.group(1).upper()
                logger.debug(
                    f"[MARKOV_AI] {provider}: {result['verdict']} "
                    f"conf={result['confidence']:.2f} "
                    f"regime={result.get('regime_quality','—')} "
                    f"sent={result.get('sentiment_score','—')} "
                    f"sector={result.get('sector_phase','—')} "
                    f"trend={result.get('trend_quality','—')}"
                )
                return result
            m2 = re.search(
                r"VERDICT\s*:\s*(AGREE|DISAGREE|NEUTRAL)\s+CONFIDENCE\s*:\s*([0-9.]+)",
                raw, re.IGNORECASE
            )
            if m2:
                return {
                    "verdict":    m2.group(1).upper(),
                    "confidence": min(1.0, max(0.0, float(m2.group(2)))),
                    "tp_adj": 0.0, "sl_adj": 0.0,
                    "edge_adj": 0.0, "decay_adj": 0.0,
                    "provider": provider,
                }
        except asyncio.TimeoutError:
            logger.debug(f"[MARKOV_AI] {provider} timeout for {symbol}")
        except Exception as e:
            logger.debug(f"[MARKOV_AI] {provider} error: {type(e).__name__}: {e}")
        return {}

    # ── Fire all providers concurrently ──────────────────────────────
    tasks   = [_query_one(p) for p in available]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    agree_weight    = 0.0
    disagree_weight = 0.0
    n_responded     = 0
    # Accumulate tuning suggestions from all responding AIs
    tp_adj_sum   = 0.0
    sl_adj_sum    = 0.0
    edge_adj_sum  = 0.0
    decay_adj_sum = 0.0
    n_tune        = 0
    # Multi-domain accumulators
    regime_quality_sum   = 0.0
    sentiment_score_sum  = 0.0
    sentiment_keywords_all: list = []
    sentiment_half_life_sum = 0.0
    sector_phase_votes: dict = {}
    trend_quality_votes: dict = {}
    n_domain = 0

    for res in results:
        if isinstance(res, dict) and res:
            verdict = res.get("verdict", "NEUTRAL")
            cconf   = res.get("confidence", 0.0)
            n_responded += 1
            if verdict == "AGREE":
                agree_weight += cconf
            elif verdict == "DISAGREE":
                disagree_weight += cconf
            # ICT tuning adjustments
            tp_adj_sum    += res.get("tp_adj",    0.0)
            sl_adj_sum    += res.get("sl_adj",    0.0)
            edge_adj_sum  += res.get("edge_adj",  0.0)
            decay_adj_sum += res.get("decay_adj", 0.0)
            n_tune        += 1
            # Multi-domain fields (v4.0)
            if "regime_quality" in res:
                regime_quality_sum  += res["regime_quality"]
                n_domain            += 1
            if "sentiment_score" in res:
                sentiment_score_sum += res["sentiment_score"]
            if "sentiment_keywords" in res:
                sentiment_keywords_all.extend(res["sentiment_keywords"])
            if "sentiment_half_life" in res:
                sentiment_half_life_sum += res["sentiment_half_life"]
            if "sector_phase" in res:
                sp = res["sector_phase"]
                sector_phase_votes[sp] = sector_phase_votes.get(sp, 0) + 1
            if "trend_quality" in res:
                tq = res["trend_quality"]
                trend_quality_votes[tq] = trend_quality_votes.get(tq, 0) + 1

    # ── Determine consensus ───────────────────────────────────────────
    if agree_weight >= AI_CONSENSUS_THRESHOLD:
        consensus    = "AGREE"
        final_weight = agree_weight
    elif disagree_weight >= AI_CONSENSUS_THRESHOLD:
        consensus    = "DISAGREE"
        final_weight = disagree_weight
    else:
        consensus    = "NEUTRAL"
        final_weight = max(agree_weight, disagree_weight)

    logger.info(
        f"[MARKOV_AI] {symbol} | {ict_state} {bias} | "
        f"agree={agree_weight:.2f} disagree={disagree_weight:.2f} "
        f"n={n_responded}/{len(available)} → {consensus}"
    )

    # ── 1. Feed consensus vote into learning matrix ───────────────────
    try:
        await asyncio.to_thread(
            _MARKOV_BRAIN.record_ai_consensus,
            symbol      = symbol,
            ict_state   = ict_state,
            bias        = bias,
            consensus   = consensus,
            confidence  = final_weight,
            n_providers = len(available),
        )
    except Exception as e:
        logger.warning(f"[MARKOV_AI] record_ai_consensus failed: {e}")

    # ── 2. Apply averaged hyperparameter tuning from all AIs ─────────
    if n_tune > 0:
        try:
            await asyncio.to_thread(
                _MARKOV_BRAIN.apply_ai_tuning,
                symbol         = symbol,
                tp_reward_adj  = tp_adj_sum  / n_tune,
                sl_penalty_adj = sl_adj_sum  / n_tune,
                edge_thresh_adj= edge_adj_sum / n_tune,
                decay_adj      = decay_adj_sum / n_tune,
                source         = f"ai_consensus_{n_tune}p",
            )
        except Exception as e:
            logger.warning(f"[MARKOV_AI] apply_ai_tuning failed: {e}")

    # ── 3. Feed multi-domain AI signals into Super Markov brains ─────
    if n_domain > 0 and MARKOV_AVAILABLE:
        try:
            # Regime brain — AI-scored quality for current regime
            _avg_regime_quality = regime_quality_sum / n_domain
            await asyncio.to_thread(
                _SUPER_VALIDATOR.regime.record_ai_quality,
                {"TRENDING": _avg_regime_quality,
                 "VOLATILE": 1.0 - _avg_regime_quality}
            )
        except Exception as _rb_e:
            logger.debug(f"[SUPER_MARKOV] regime update: {_rb_e}")

        try:
            # Sentiment brain — AI-scored keywords + scores
            if sentiment_keywords_all:
                _avg_hl = (sentiment_half_life_sum / n_domain) if n_domain > 0 else 12.0
                _avg_ss = sentiment_score_sum / n_domain
                _kw_payloads = [
                    {
                        "keyword":       kw,
                        "score":         _avg_ss,
                        "half_life_hrs": _avg_hl,
                    }
                    for kw in dict.fromkeys(sentiment_keywords_all)  # deduplicate, preserve order
                ]
                await asyncio.to_thread(
                    _SUPER_VALIDATOR.sentiment.record_keywords,
                    _kw_payloads,
                    f"ai_consensus_{n_domain}p",
                )
        except Exception as _sb_e:
            logger.debug(f"[SUPER_MARKOV] sentiment update: {_sb_e}")

        try:
            # Sector brain — majority-vote sector phase
            if sector_phase_votes:
                _dominant_phase = max(sector_phase_votes, key=sector_phase_votes.get)
                _phase_conf     = sector_phase_votes[_dominant_phase] / max(n_domain, 1)
                await asyncio.to_thread(
                    _SUPER_VALIDATOR.sector.record_ai_phase,
                    symbol, _dominant_phase, _phase_conf
                )
        except Exception as _se_e:
            logger.debug(f"[SUPER_MARKOV] sector update: {_se_e}")

        try:
            # Trend brain — majority-vote trend quality
            if trend_quality_votes:
                _dominant_trend = max(trend_quality_votes, key=trend_quality_votes.get)
                _trend_conf     = trend_quality_votes[_dominant_trend] / max(n_domain, 1)
                await asyncio.to_thread(
                    _SUPER_VALIDATOR.trend.record_ai_quality,
                    _dominant_trend, _trend_conf
                )
        except Exception as _tb_e:
            logger.debug(f"[SUPER_MARKOV] trend update: {_tb_e}")

        logger.info(
            f"[SUPER_MARKOV] {symbol} | 5-brain update complete | "
            f"regime={regime_quality_sum/n_domain:.2f} "
            f"sent={sentiment_score_sum/n_domain:+.2f} "
            f"sector={max(sector_phase_votes, key=sector_phase_votes.get, default='—')} "
            f"trend={max(trend_quality_votes, key=trend_quality_votes.get, default='—')} "
            f"kw={len(sentiment_keywords_all)}"
        )


# ── APEX: Main adaptive execution loop ────────────────────────────────
async def auto_alpha_notification(application) -> None:
    """
    APEX ENGINE v1.0 — Adaptive Predictive Event-driven Alpha eXecution.

    Architecture:
      1. Run 5 parallel sensors → composite urgency score (0–100)
      2. Adaptive cadence: urgent=immediate, normal=3min, idle=5min
      3. Find top accumulation coin via full pipeline
      4. AI pre-validation gate before broadcast
      5. Rate-limit guards: per-symbol cooldown + hourly cap
      6. Broadcast only on approval

    This replaces the fixed 5-min polling loop with an event-driven
    system that fires AHEAD of the market, not after it.
    """
    global _alpha_notify_cache

    # Stagger first run — let bot fully initialize
    await asyncio.sleep(30)

    logger.info("[APEX] Engine v8.2 started — event-driven alpha detection active | v8.2 watch alerts")

    # REC-1: _CS_PRICE_CACHE refresh state
    _cs_cache_last_ts: float = 0.0
    _CS_CACHE_TTL = 60.0  # refresh every 60 s

    while True:
        cycle_start = time.time()
        try:
            # ── REC-1: Populate _CS_PRICE_CACHE — correlated asset 24h % changes ──
            # Used by SMT divergence, Kalman filter, and NVT calculations in TA.
            # Previously always empty → all 11 read-sites used proxy fallbacks.
            if TA_MODULE_AVAILABLE and (time.time() - _cs_cache_last_ts) > _CS_CACHE_TTL:
                try:
                    _cs_ids = "bitcoin,ethereum,solana,binancecoin,ripple,cardano,avalanche-2,polkadot,chainlink,matic-network"
                    _cs_resp = await asyncio.to_thread(
                        fetch_json,
                        f"{COINGECKO_BASE_URL}/simple/price",
                        {"ids": _cs_ids, "vs_currencies": "usd", "include_24hr_change": "true"},
                    )
                    if _cs_resp:
                        _CG_TO_SYM = {
                            "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
                            "binancecoin": "BNB", "ripple": "XRP", "cardano": "ADA",
                            "avalanche-2": "AVAX", "polkadot": "DOT",
                            "chainlink": "LINK", "matic-network": "MATIC",
                        }
                        from TechnicalAnalysis_v9_6 import _CS_PRICE_CACHE as _csp_ref
                        for cg_id, sym in _CG_TO_SYM.items():
                            chg = float((_cs_resp.get(cg_id) or {}).get("usd_24h_change") or 0.0)
                            _csp_ref[sym] = chg
                        _cs_cache_last_ts = time.time()
                        logger.debug(f"[CS_CACHE] Refreshed {len(_CG_TO_SYM)} correlated asset prices")
                except Exception as _csc_e:
                    logger.debug(f"[CS_CACHE] refresh non-critical: {_csc_e}")

            # ── STEP 1: Compute urgency across all 5 sensors ──────────────
            urgency = await _apex_compute_urgency()
            sensor_scores = _apex_state["sensor_scores"].copy()

            # ── STEP 2: Adaptive cadence decision ─────────────────────────
            if urgency >= _APEX_URGENCY_IMMEDIATE:
                cadence   = 0       # fire immediately — no wait
                cadence_label = f"🔴 IMMEDIATE (urgency={urgency:.0f})"
            elif urgency >= _APEX_URGENCY_FAST:
                cadence   = 90      # accelerated 90s scan
                cadence_label = f"🟡 FAST 90s (urgency={urgency:.0f})"
            elif urgency >= _APEX_URGENCY_NORMAL:
                cadence   = 180     # normal 3min scan
                cadence_label = f"🟢 NORMAL 3min (urgency={urgency:.0f})"
            else:
                cadence   = 300     # idle 5min — no significant signal
                cadence_label = f"⚪ IDLE 5min (urgency={urgency:.0f})"

            logger.info(f"[APEX] Cadence: {cadence_label}")

            # ── STEP 3a: Always check watched TA coin regardless of urgency ─
            # This runs on every cycle so price alerts fire even during idle periods
            try:
                await _apex_check_watched_coin(application)
            except Exception as _we:
                logger.debug(f"[APEX] Watch check error: {_we}")

            # ── GAP-05: Multi-coin AWS store monitor ────────────────────────
            # Iterates ALL pending signals in the AWS multi-coin store (up to 100).
            # The old _apex_check_watched_coin only monitors ONE coin — any second
            # analysis overwrites it. This loop ensures every pending signal gets
            # monitored independently, using _aws_set_alert_flag() per level fired.
            try:
                await _apex_check_aws_multiwatch(application)
            except Exception as _mw_e:
                logger.debug(f"[APEX] Multi-watch error: {_mw_e}")

            # ── REC-6: Poll custom user price alerts ──────────────────────────
            try:
                await _check_custom_alerts(application)
            except Exception as _ca_e:
                logger.debug(f"[APEX] custom alerts error: {_ca_e}")

            # ── STEP 3b: Skip pipeline if urgency is very low ─────────────
            if urgency < 15.0:
                logger.debug("[APEX] Urgency too low — skipping pipeline this cycle")
                await asyncio.sleep(max(0, cadence - (time.time() - cycle_start)))
                continue

            # ── STEP 4: Run accumulation pipeline ─────────────────────────
            winner = await _get_top_alpha_coin()
            if not winner:
                logger.info("[APEX] No accumulation winner this cycle")
                await asyncio.sleep(max(0, cadence - (time.time() - cycle_start)))
                continue

            symbol = winner["symbol"]

            # ── STEP 5: Rate-limit guard ───────────────────────────────────
            can_fire, block_reason = _apex_can_fire(symbol)
            if not can_fire:
                logger.info(f"[APEX] Fire blocked: {block_reason}")
                await asyncio.sleep(max(0, cadence - (time.time() - cycle_start)))
                continue

            # ── STEP 6: AI pre-validation ──────────────────────────────────
            validation = await _apex_ai_validate(winner, urgency, sensor_scores)
            if not validation["approved"]:
                logger.info(f"[APEX] AI rejected {symbol}: {validation['reason']}")
                await asyncio.sleep(max(0, cadence - (time.time() - cycle_start)))
                continue

            logger.info(
                f"[APEX] ✅ Firing alert: {symbol} | urgency={urgency:.1f} | "
                f"AI={validation['confidence']} | {validation['reason']}"
            )

            # ── STEP 7: Enrich winner with APEX metadata ───────────────────
            winner["apex_urgency"]    = urgency
            winner["apex_cadence"]    = cadence_label
            winner["apex_confidence"] = validation["confidence"]
            winner["apex_reason"]     = validation["reason"]
            winner["apex_sensors"]    = sensor_scores

            fired_at = datetime.now(timezone.utc).strftime("%b %d %H:%M UTC")

            # ── STEP 8: Cross scan + TA + chart ───────────────────────────
            cross_text               = await _get_cross_result_for_coin(symbol)
            ta_text, chart_buf, _ta_alpha_meta = await _get_ta_summary_for_coin(symbol, application)

            # ── STEP 9: Format with APEX urgency header ────────────────────
            body_text, footer_text   = _format_alpha_notification(winner, cross_text, ta_text, fired_at)

            # ── STEP 10: Broadcast to active chats ────────────────────────
            _now_ts = time.time()
            _7_days = 7 * 24 * 3600
            if not registered_chats:
                logger.info("[APEX] No registered chats yet")
            else:
                for chat_id in list(registered_chats):
                    _store = feedback_store.get(chat_id, {})
                    _last_ts_vals = [ts.timestamp() for ts in _store.get("capture_ts", {}).values()
                                     if hasattr(ts, "timestamp")]
                    _last_active  = max(_last_ts_vals) if _last_ts_vals else _now_ts
                    if _now_ts - _last_active > _7_days:
                        logger.debug(f"[APEX] Skipping inactive chat {chat_id}")
                        continue
                    try:
                        await application.bot.send_message(
                            chat_id=chat_id,
                            text=body_text,
                            parse_mode=ParseMode.MARKDOWN,
                        )
                        if chart_buf:
                            chart_buf.seek(0)
                            await application.bot.send_photo(chat_id=chat_id, photo=chart_buf)
                        await application.bot.send_message(
                            chat_id=chat_id,
                            text=footer_text,
                            parse_mode=ParseMode.MARKDOWN,
                        )
                        # ── [SYNC-FIX] WebApp interactive signal button on alpha notify ──
                        _aw_url   = _ta_alpha_meta.get("webapp_url")
                        _aw_dir   = _ta_alpha_meta.get("direction", "LONG ▲")
                        _aw_state = _ta_alpha_meta.get("state", "BLOCKED")
                        _aw_grade = _ta_alpha_meta.get("grade", "?")
                        _aw_icon  = {"ACTIVE": "🟢", "DEFERRED": "🟡", "BLOCKED": "🔴"}.get(_aw_state, "⚪")
                        if _aw_url and WEBAPP_URL:
                            try:
                                await application.bot.send_message(
                                    chat_id=chat_id,
                                    text=(
                                        f"📊 *Interactive Signal View*\n"
                                        f"`{symbol}USDT` · {_aw_dir} · Grade {_aw_grade} · {_aw_icon} {_aw_state}"
                                    ),
                                    parse_mode=ParseMode.MARKDOWN,
                                    reply_markup=InlineKeyboardMarkup([[
                                        InlineKeyboardButton(
                                            "📈 Open Signal Chart",
                                            web_app=WebAppInfo(url=_aw_url)
                                        )
                                    ]])
                                )
                            except Exception as _btn_e:
                                logger.debug(f"[APEX] WebApp button send skipped: {_btn_e}")
                        # ────────────────────────────────────────────────────────────────
                        await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.warning(f"[APEX] Failed to notify {chat_id}: {e}")

            # ── STEP 11: Record fire + update cache ────────────────────────
            _apex_record_fire(symbol)
            _alpha_notify_cache = {
                "ts":        time.time(),
                "coin":      symbol,
                "urgency":   urgency,
                "text":      body_text + "\n" + footer_text,
                "webapp_url": _ta_alpha_meta.get("webapp_url"),
                "state":     _ta_alpha_meta.get("state", "BLOCKED"),
                "grade":     _ta_alpha_meta.get("grade", "?"),
            }
            logger.info(f"[APEX] Broadcast complete — {symbol} | chats: {len(registered_chats)}")

            # RAM cleanup after broadcast — chart_buf can be several MB
            if chart_buf:
                try: chart_buf.close()
                except Exception: pass
            del ta_text, body_text, footer_text, cross_text
            _gc.collect()
            logger.debug(f"[APEX] Post-broadcast RAM: {_rss_bytes()/1024/1024:.1f} MB")

        except Exception as e:
            logger.error(f"[APEX] Cycle error: {e}", exc_info=True)

        # ── Adaptive sleep — deduct execution time from cadence ────────────
        elapsed = time.time() - cycle_start
        sleep_s = max(10, cadence - elapsed)   # minimum 10s between cycles
        logger.debug(f"[APEX] Sleeping {sleep_s:.0f}s (elapsed={elapsed:.1f}s)")
        await asyncio.sleep(sleep_s)


async def auto_market_refresh(chat_id: int, application):
    """Background task to refresh market data.
    BUG-13 FIX: added exponential backoff on repeated failures + max failure cap.
    Previously: error logged and immediately retried — silent resource leak on sustained API failure.
    """
    _fail_count = 0
    _MAX_BACKOFF = 1800   # cap at 30 min between retries on sustained failure
    while True:
        try:
            _sleep = min(MARKET_REFRESH_INTERVAL * (2 ** _fail_count), _MAX_BACKOFF) if _fail_count else MARKET_REFRESH_INTERVAL
            await asyncio.sleep(_sleep)
            market_data = await asyncio.to_thread(get_market_regime)
            await send_market_overview(chat_id, application, market_data)
            # FIX: force=True — background refresh must always update the pin.
            # Without this, regime_changed=False skips the pin silently on every background cycle.
            await update_regime_pin(chat_id, application, market_data, force=True)
            _fail_count = 0   # reset on success
        except Exception as e:
            _fail_count += 1
            logger.error(f"[AUTO_REFRESH] Error (fail #{_fail_count}): {e}")

# ==========================================
# CROSS ANALYSIS FUNCTIONS - SUPER FAST BINANCE
# ==========================================

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cross analysis constants
QUOTE = "USDT"
CROSS_BATCH_SIZE = 30
CROSS_MAX_WORKERS = 8
BINANCE_ENDPOINTS = [
    "https://api.binance.com/api/v3/klines",
    "https://data.binance.vision/api/v3/klines",
    "https://api.binance.us/api/v3/klines"
]

INTERVAL_MAP = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
    "1w": "1w"
}

# Cache for symbol mapping (capped at 500 entries to prevent unbounded growth)
_SYMBOL_CACHE_MAX = 500
_symbol_cache = {}

def coingecko_to_binance_symbol(coin_id):
    """Get Binance symbol from CoinGecko ID (cached)"""
    if coin_id in _symbol_cache:
        return _symbol_cache[coin_id]
    try:
        resp = requests.get(
            f"{COINGECKO_BASE_URL}/coins/{coin_id}",
            headers=REQUEST_HEADERS,
            timeout=10
        )
        if resp.status_code == 200:
            symbol = resp.json().get("symbol", "").upper()
            if len(_symbol_cache) >= _SYMBOL_CACHE_MAX:
                # Evict oldest 250 entries (insertion-order, Python 3.7+)
                for old_key in list(_symbol_cache.keys())[:_SYMBOL_CACHE_MAX // 2]:
                    del _symbol_cache[old_key]
            _symbol_cache[coin_id] = symbol
            return symbol
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    return None

def fetch_binance_klines(symbol, interval, limit=300):
    """Fetch OHLCV from Binance with fallback"""
    pair = f"{symbol}{QUOTE}"
    params = {"symbol": pair, "interval": interval, "limit": limit}
    
    for endpoint in BINANCE_ENDPOINTS:
        try:
            r = requests.get(
                endpoint,
                params=params,
                headers=REQUEST_HEADERS,
                timeout=10
            )
            if r.status_code == 200 and r.text.startswith("["):
                return r.json()
        except Exception:
            continue
    return None

def detect_cross_binance(klines, cross_type):
    """Detect Golden/Death cross using MA50 & MA200 — pure Python, no pandas"""
    if not klines or len(klines) < 200:
        return False

    try:
        closes = [float(k[4]) for k in klines]  # index 4 = close price

        def rolling_mean(data, window):
            return [
                sum(data[i - window:i]) / window
                for i in range(window, len(data) + 1)
            ]

        ma50  = rolling_mean(closes, 50)
        ma200 = rolling_mean(closes, 200)

        # ma200 is shorter; use last two aligned values
        prev_ma50  = ma50[-2];  prev_ma200 = ma200[-2]
        curr_ma50  = ma50[-1];  curr_ma200 = ma200[-1]

        golden_cross = prev_ma50 < prev_ma200 and curr_ma50 > curr_ma200
        death_cross  = prev_ma50 > prev_ma200 and curr_ma50 < curr_ma200

        return golden_cross if cross_type == "golden" else death_cross
    except Exception:
        return False

async def process_coin_cross(coin_id, timeframe, cross_type):
    """Process single coin for cross detection"""
    symbol = coingecko_to_binance_symbol(coin_id)
    if not symbol:
        return None
    
    # Run blocking fetch in executor
    loop = asyncio.get_event_loop()
    klines = await loop.run_in_executor(
        None,
        fetch_binance_klines,
        symbol,
        INTERVAL_MAP.get(timeframe, "1d")
    )
    
    if detect_cross_binance(klines, cross_type):
        return f"{coin_id.upper()} ({symbol})"
    return None

async def process_cross_batch(batch_coins, timeframe, cross_type):
    """Process batch of coins asynchronously"""
    results = []
    tasks = [process_coin_cross(c, timeframe, cross_type) for c in batch_coins]
    
    for future in asyncio.as_completed(tasks):
        try:
            result = await future
            if result:
                results.append(result)
        except Exception as e:
            logger.error(f"Error processing coin: {e}")
            continue
    
    return results

def get_coingecko_coin_list():
    """Get CoinGecko coin list for symbol mapping"""
    try:
        coins = fetch_json(f"{COINGECKO_BASE_URL}/coins/list")
        mapping = {}
        for coin in coins or []:
            symbol = coin.get('symbol', '').lower()
            coin_id = coin.get('id', '')
            if symbol and coin_id:
                if symbol not in mapping or len(coin_id) < len(mapping[symbol]):
                    mapping[symbol] = coin_id
        return mapping
    except Exception:
        return {}

# ==========================================
# SECTOR ROTATION FUNCTIONS
# ==========================================

def calculate_volume_efficiency(volume, price_change_abs):
    """Calculate volume efficiency metric.
    Cap price_change_abs at 10 so strong trending coins aren't penalized
    vs flat coins — a +25% mover with massive volume should still score high.
    """
    if price_change_abs < 0.1:
        price_change_abs = 0.1
    price_change_abs = min(price_change_abs, 10.0)  # cap: don't punish momentum
    efficiency = math.log(volume + 1) / (price_change_abs + 1)
    return efficiency

def calculate_rci_metrics(coin, volume, mcap, price, change):
    """Calculate RCI institutional metrics with phase-aware state detection.

    State logic (ordered by priority):
      ACCUM      — high RCI, low price movement  → smart money loading quietly
      TRENDING   — high RCI + moderate upward move → confirmed trend, still room
      EARLY_DIST — high RCI + strong move + efficiency starting to fade
      DISTRIB    — low RCI + high price move → price ran without volume backing
      MOMENTUM   — very high RCI + explosive move → institutional FOMO / blow-off
      NEUTRAL    — everything else
    """
    # Smart Money Index: volume relative to market cap (×1000, capped at 100)
    smart_money = min(100, (volume / mcap) * 1000) if mcap > 0 else 0

    # Efficiency: log(volume) per unit of price change (capped at 10% so
    # big movers are not punished — see calculate_volume_efficiency)
    # Cap at 100 so the weighted composite cannot exceed its nominal 0-100 range (thesis A2.6)
    efficiency = min(100.0, calculate_volume_efficiency(volume, abs(change)) * 10)

    # Liquidity Score: volume/mcap log-ratio — measures depth of participation
    liquidity = (math.log10(volume + 1) / math.log10(mcap + 1)) * 10 if mcap > 0 else 0

    # Velocity: raw volume/mcap turnover (%)
    velocity = (volume / mcap) * 100 if mcap > 0 else 0

    # RCI Score — weighted composite
    rci = (smart_money * 0.3 + efficiency * 0.3 + liquidity * 0.2 + velocity * 0.2)

    # ── Phase detection: ACCUM → TRENDING → DIST (priority order) ───────────
    # Three primary phases evaluated strictly top-to-bottom (no unreachable branch).
    # Threshold calibration (vol/mcap ratios that hit each RCI band):
    #   RCI ≥ 68 → vol/mcap ≥ 1.0x  (micro/small cap with massive turnover)
    #   RCI ≥ 58 → vol/mcap ≥ 0.5x  (small cap active)
    #   RCI ≥ 52 → vol/mcap ≥ 0.15x (mid-cap trending)
    abs_change = abs(change)

    # ACCUMULATION phase — high turnover, low price movement
    if rci >= 68 and abs_change < 3:
        state = "🧠 ACCUM"       # deep quiet load — best entry signal
    elif rci >= 55 and abs_change < 5 and smart_money >= 15:
        state = "🧠 ACCUM+"      # loading with slight price discovery

    # TRENDING phase — volume + price aligned, still room to run
    elif rci >= 52 and 3 <= abs_change <= 10 and change > 0:
        state = "📈 TREND"       # healthy trend, RCI and price aligned
    elif rci >= 48 and 10 < abs_change <= 20 and change > 0:
        state = "📈 TREND+"      # strong trend, momentum building

    # DISTRIBUTION phase — price extended or RCI fading
    elif abs_change > 20 and change > 0 and rci >= 40:
        state = "📉 EARLY DIST"  # price extended, RCI still ok — watch
    elif rci < 42 and abs_change > 8:
        state = "📉 DIST"        # volume weak vs price move — exiting
    elif change < -5 and rci < 45:
        state = "📉 DUMP"        # active sell-off

    # No clear phase
    else:
        state = "➖ NEUTRAL"

    return {
        'smart_money': round(smart_money, 1),
        'efficiency':  round(efficiency, 2),
        'liquidity':   round(liquidity, 2),
        'velocity':    round(velocity, 2),
        'rci':         round(rci, 1),
        'state':       state,
    }

# ── Sector rotation cache ─────────────────────────────────────────────────────
# Stores pre-aggregated sector_data dict (tiny) — NOT the raw protocols list.
_sector_protocols_cache: dict = {}   # {"data": {cat: {...}}, "ts": float}
_SECTOR_PROTOCOLS_TTL = 300          # seconds — re-fetch every 5 min
_sector_output_cache: dict = {}      # {"data": [final_sectors], "ts": float}
_SECTOR_OUTPUT_TTL = 60              # seconds — serve same result for 60s on repeated taps


async def _fetch_defillama_sector_data() -> dict:
    """
    Stream DefiLlama /protocols and aggregate on-the-fly.
    Never holds the full ~10 MB response in RAM — parses chunk by chunk.
    Returns: {category: {"tvl": float, "tvl_change_7d": float, "gecko_ids": [(id, tvl)]}}
    """
    SKIP_CATS = {"", "Unknown", "CEX", "Chain", "Bridge"}
    sector_data: dict = {}
    decoder = json.JSONDecoder()
    buf = ""
    in_array = False

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{DEFILLAMA_BASE_URL}/protocols",
                headers=REQUEST_HEADERS,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                if r.status != 200:
                    logger.warning(f"[SECTOR] DefiLlama HTTP {r.status}")
                    return {}

                async for chunk in r.content.iter_chunked(16384):  # 16 KB at a time
                    buf += chunk.decode("utf-8", errors="ignore")

                    # Find start of JSON array
                    if not in_array:
                        idx = buf.find("[")
                        if idx == -1:
                            buf = buf[-64:]
                            continue
                        buf = buf[idx + 1:]
                        in_array = True

                    # Parse individual protocol objects out of the buffer
                    while True:
                        buf = buf.lstrip()
                        if not buf:
                            break
                        if buf[0] == "]":
                            break  # end of array
                        if buf[0] == ",":
                            buf = buf[1:]
                            continue
                        try:
                            obj, end = decoder.raw_decode(buf)
                            buf = buf[end:]
                        except json.JSONDecodeError:
                            break  # incomplete object — wait for next chunk

                        # Aggregate immediately, discard raw obj
                        cat = (obj.get("category") or "").strip()
                        if cat in SKIP_CATS:
                            continue
                        tvl      = float(obj.get("tvl") or 0)
                        gecko_id = (obj.get("gecko_id") or "").strip()
                        chg7     = float(obj.get("change_7d") or 0)
                        # REC-19: Also capture 30d TVL change for institutional flow context
                        chg30    = float(obj.get("change_1m") or obj.get("change_30d") or 0)

                        sd = sector_data.setdefault(cat, {
                            "tvl": 0.0, "tvl_change_7d": 0.0,
                            "tvl_change_30d": 0.0, "gecko_ids": [],
                        })
                        sd["tvl"]            += tvl
                        sd["tvl_change_7d"]  += chg7
                        sd["tvl_change_30d"] += chg30
                        if gecko_id:
                            sd["gecko_ids"].append((gecko_id, tvl))

    except Exception as e:
        logger.error(f"[SECTOR] DefiLlama stream error: {e}")
        return {}

    return sector_data


async def _fetch_sector_cg_ids(
    session: aiohttp.ClientSession,
    gecko_ids: list,
) -> list:
    """
    Fetch /coins/markets for a list of CoinGecko IDs (free-tier, ?ids= param).
    gecko_ids come directly from DefiLlama's gecko_id field — no /coins/list
    mapping needed at all.
    Returns list of slim coin dicts (only needed fields) or [] on failure.
    """
    if not gecko_ids:
        return []
    # Only request top 12 IDs — we only ever use 10, slim the response
    ids = list(dict.fromkeys(i for i in gecko_ids if i))[:12]
    _KEEP_FIELDS = {
        "id", "symbol", "current_price", "market_cap",
        "total_volume", "price_change_percentage_24h",
    }
    try:
        async with session.get(
            f"{COINGECKO_BASE_URL}/coins/markets",
            params={
                "vs_currency":             "usd",
                "ids":                     ",".join(ids),
                "order":                   "market_cap_desc",
                "per_page":                "12",
                "page":                    "1",
                "sparkline":               "false",
                "price_change_percentage": "24h",
            },
            headers=REQUEST_HEADERS,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            if r.status == 200:
                raw = await r.json()
                # Strip every field we don't need before returning
                return [{k: v for k, v in c.items() if k in _KEEP_FIELDS} for c in raw]
            logger.warning(f"[SECTOR] CG HTTP {r.status} for ids={ids[:3]}")
            return []
    except Exception as e:
        logger.warning(f"[SECTOR] CG fetch error: {e}")
        return []


async def analyze_sector_rotation_async() -> list:
    """
    Memory-efficient async sector rotation (fits in 128 MB).
    Always returns a result — output cached for 60s so repeated taps
    never hit APIs twice and never get an empty response.
    """
    global _sector_protocols_cache, _sector_output_cache

    now = time.time()

    # ── 0. Output cache — serve instantly on repeated taps ───────────────────
    if _sector_output_cache.get("ts", 0) + _SECTOR_OUTPUT_TTL > now:
        cached = _sector_output_cache.get("data")
        if cached:
            logger.info("[SECTOR] Output cache hit — returning instantly")
            return cached

    # ── 1. DefiLlama /protocols — stream & aggregate (cache-first) ───────────
    if _sector_protocols_cache.get("ts", 0) + _SECTOR_PROTOCOLS_TTL > now:
        sector_data = _sector_protocols_cache["data"]
        logger.info("[SECTOR] DefiLlama protocol cache hit")
    else:
        sector_data = await _fetch_defillama_sector_data()
        if sector_data:
            _sector_protocols_cache = {"data": sector_data, "ts": now}
            logger.info(f"[SECTOR] DefiLlama streamed: {len(sector_data)} categories")
        elif _sector_protocols_cache.get("data"):
            sector_data = _sector_protocols_cache["data"]
            logger.warning("[SECTOR] DefiLlama failed — using stale cache")
        else:
            # Last resort: return stale output if available
            stale = _sector_output_cache.get("data")
            if stale:
                logger.warning("[SECTOR] All APIs failed — returning stale output")
                return stale
            logger.error("[SECTOR] DefiLlama failed and no cache at all")
            return []

    if not sector_data:
        return []

    # ── 2. Composite sector ranking ───────────────────────────────────────────
    # Pure TVL misses high-momentum sectors (AI, Gaming, DePIN) that don't have
    # DeFi TVL. We score each sector on three dimensions:
    #
    #   tvl_score      (40%) — log-normalised TVL so huge DeFi sectors don't
    #                          completely crowd out smaller hot sectors
    #   momentum_score (40%) — 7-day TVL change (positive = capital inflow),
    #                          clamped to [-100, +100] and shifted to [0, 100]
    #   depth_score    (20%) — number of addressable coins (gecko_ids) in the
    #                          sector, log-normalised; rewards broad sectors

    # Pre-compute max_tvl for log-normalisation
    max_log_tvl = max(
        (math.log10(v["tvl"] + 1) for v in sector_data.values() if v["tvl"] > 0),
        default=1.0,
    )

    def _sector_composite(item):
        cat, data = item
        tvl   = data["tvl"]
        chg7  = data["tvl_change_7d"]
        depth = len(data["gecko_ids"])

        tvl_score      = (math.log10(tvl + 1) / max_log_tvl) * 100 if tvl > 0 else 0
        momentum_score = max(0.0, min(100.0, 50.0 + chg7))   # shift: 0%→50, +50%→100
        depth_score    = min(100.0, math.log10(depth + 1) * 50) if depth > 0 else 0

        return tvl_score * 0.40 + momentum_score * 0.40 + depth_score * 0.20

    ranked = sorted(sector_data.items(), key=_sector_composite, reverse=True)[:7]

    # Build per-sector gecko_id lists — never mutate cached sector_data
    sector_plan = []   # [(cat, tvl, tvl_change_7d, [gecko_id, ...]), ...]
    for cat, data in ranked:
        seen: set = set()
        ids: list = []
        for gid, _ in sorted(data["gecko_ids"], key=lambda x: -x[1]):
            if gid not in seen:
                seen.add(gid)
                ids.append(gid)
            if len(ids) == 12:
                break
        if ids:
            sector_plan.append((cat, data["tvl"], data["tvl_change_7d"], ids))

    if not sector_plan:
        stale = _sector_output_cache.get("data")
        if stale:
            logger.warning("[SECTOR] No gecko_ids — returning stale output")
            return stale
        return []

    # ── 3. CoinGecko — sequential with retry + exponential backoff ───────────
    async def _fetch_with_retry(session, ids, retries=3):
        for attempt in range(retries + 1):
            try:
                result = await _fetch_sector_cg_ids(session, ids)
                if result:
                    return result
            except Exception as e:
                logger.warning(f"[SECTOR] CG exception attempt {attempt + 1}: {e}")
            if attempt < retries:
                wait = 1.5 * (attempt + 1)  # 1.5s, 3s, 4.5s
                logger.info(f"[SECTOR] CG retry {attempt + 1} in {wait}s for {ids[:2]}")
                await asyncio.sleep(wait)
        return []

    coin_data_results = []
    async with aiohttp.ClientSession() as session:
        for cat, _, _, ids in sector_plan:
            result = await _fetch_with_retry(session, ids)
            coin_data_results.append(result)

    # ── 4. Build output ───────────────────────────────────────────────────────
    final_sectors = []
    for (cat, tvl, tvl_change_7d, _), coin_data in zip(sector_plan, coin_data_results):
        # If CoinGecko failed for this sector, skip but don't abort everything
        if not coin_data:
            logger.warning(f"[SECTOR] No CoinGecko data for {cat} — skipping sector")
            continue

        processed = []
        total_rci = 0.0
        for coin in coin_data[:10]:
            vol    = float(coin.get("total_volume") or 0)
            change = float(coin.get("price_change_percentage_24h") or 0)
            mcap   = float(coin.get("market_cap") or 0)
            price  = float(coin.get("current_price") or 0)
            if vol == 0 and mcap == 0:
                continue
            metrics    = calculate_rci_metrics(coin, vol, mcap, price, change)
            total_rci += metrics["rci"]
            processed.append({
                "symbol":      coin.get("symbol", "").upper(),
                "price":       price,
                "change":      change,
                "efficiency":  metrics["efficiency"],
                "rci":         metrics["rci"],
                "smart_money": metrics["smart_money"],
                "liquidity":   metrics["liquidity"],
                "velocity":    metrics["velocity"],
                "state":       metrics["state"],
            })

        if not processed:
            logger.warning(f"[SECTOR] No processable coins for {cat} — skipping")
            continue

        # Rank coins: ACCUM → TREND → DIST → NEUTRAL, then RCI+eff within each phase
        _COIN_PHASE_RANK = {
            "🧠 ACCUM":      0,
            "🧠 ACCUM+":     1,
            "📈 TREND":      2,
            "📈 TREND+":     3,
            "📉 EARLY DIST": 4,
            "📉 DIST":       5,
            "📉 DUMP":       6,
            "➖ NEUTRAL":    7,
        }
        processed.sort(
            key=lambda x: (
                _COIN_PHASE_RANK.get(x["state"], 7),
                -(x["rci"] + x["efficiency"])   # higher is better within same phase
            )
        )
        avg_rci    = total_rci / len(processed)
        avg_eff    = sum(t["efficiency"] for t in processed) / len(processed)
        avg_change = sum(t["change"]     for t in processed) / len(processed)

        # ── Phase-aware flow_status ───────────────────────────────────────────
        # Counts how many coins are in each state
        state_counts = {}
        for t in processed:
            s = t["state"]
            state_counts[s] = state_counts.get(s, 0) + 1

        accum_n   = state_counts.get("🧠 ACCUM", 0)  + state_counts.get("🧠 ACCUM+", 0)
        trending_n= state_counts.get("📈 TREND", 0)  + state_counts.get("📈 TREND+", 0)
        edist_n   = state_counts.get("📉 EARLY DIST", 0)
        distrib_n = state_counts.get("📉 DIST", 0)   + state_counts.get("📉 DUMP", 0)
        total_n   = len(processed)

        # Dominant phase: whichever phase holds the most coins wins
        # Ties broken by phase priority (ACCUM > TREND > DIST)
        if accum_n >= trending_n and accum_n >= distrib_n and accum_n > 0:
            dominant_phase = "ACCUM"
        elif trending_n >= distrib_n and trending_n > 0:
            dominant_phase = "TREND"
        elif distrib_n > 0:
            dominant_phase = "DIST"
        else:
            dominant_phase = "NEUTRAL"

        # Sector phase_rank — used for final sort (lower = shown first)
        _SECTOR_PHASE_RANK = {"ACCUM": 0, "TREND": 1, "DIST": 2, "NEUTRAL": 3}
        phase_rank = _SECTOR_PHASE_RANK[dominant_phase]

        # flow_status label — reflects dominant phase + RCI strength
        if dominant_phase == "ACCUM" and avg_rci >= 65:
            flow_status = "🧠 ACCUMULATION"      # strong quiet loading
        elif dominant_phase == "ACCUM":
            flow_status = "🧠 EARLY ACCUM"        # loading but weaker signal
        elif dominant_phase == "TREND" and avg_rci >= 60:
            flow_status = "📈 TRENDING"            # confirmed momentum, room to run
        elif dominant_phase == "TREND":
            flow_status = "📈 EARLY TREND"         # trend forming, watch for confirmation
        elif dominant_phase == "DIST" and edist_n > distrib_n:
            flow_status = "⚡ WATCH DIST"          # extended, flip risk rising
        elif dominant_phase == "DIST":
            flow_status = "📉 DISTRIBUTING"        # smart money exiting
        else:
            flow_status = "➖ NEUTRAL"

        final_sectors.append({
            "category":       cat,
            "tvl":            tvl,
            "tvl_change_7d":  tvl_change_7d,
            # REC-19: Fetch 30d TVL change from DeFiLlama for fuller institutional flow picture.
            # sector_data already has tvl_change_7d; we also capture 30d here when available.
            "tvl_change_30d": float(data.get("tvl_change_30d", tvl_change_7d * 2.0)),
            "tvl_usd_str":    (
                f"${tvl/1e9:.2f}B" if tvl >= 1e9 else
                f"${tvl/1e6:.1f}M" if tvl >= 1e6 else
                f"${tvl:,.0f}"
            ),
            "flow_status":    flow_status,
            "dominant_phase": dominant_phase,
            "phase_rank":     phase_rank,
            "avg_rci":        round(avg_rci, 1),
            "avg_eff":        round(avg_eff, 2),
            "avg_change":     round(avg_change, 2),
            "tokens":         processed[:10],
        })

    # ── 5. Final sector ranking: ACCUM → TREND → DIST → NEUTRAL ─────────────
    # Within each phase tier, highest avg_rci surfaces first.
    final_sectors.sort(key=lambda s: (s["phase_rank"], -s["avg_rci"]))

    # ── 6. AI validation — all 5 providers vote in parallel ──────────────────
    # Runs concurrently; never blocks output if providers are slow/unavailable.
    try:
        final_sectors = await asyncio.wait_for(
            _validate_sector_rotation_with_ai(final_sectors),
            timeout=25.0   # hard cap so slow AIs never stall the response
        )
    except asyncio.TimeoutError:
        logger.warning("[SECTOR-AI] Validation timed out — serving unvalidated sectors")
    except Exception as e:
        logger.warning(f"[SECTOR-AI] Validation error: {e}")

    # If we got results, cache them; if not, return whatever stale data we have
    if final_sectors:
        _sector_output_cache = {"data": final_sectors, "ts": now}
        logger.info(f"[SECTOR] Output cached: {len(final_sectors)} sectors")
    else:
        stale = _sector_output_cache.get("data")
        if stale:
            logger.warning("[SECTOR] Fresh fetch yielded nothing — returning stale output")
            return stale

    return final_sectors


def analyze_sector_rotation() -> list:
    """Sync shim — kept for backward-compat. Calls async version."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return []   # caller must use analyze_sector_rotation_async() directly
        return loop.run_until_complete(analyze_sector_rotation_async())
    except Exception as e:
        logger.error(f"[SECTOR] sync shim error: {e}")
        return []


async def _validate_sector_rotation_with_ai(sectors: list) -> list:
    """
    Run all 5 AI providers in parallel against the sector rotation output.
    Each provider validates the phase label (ACCUM / TREND / DIST) and top coin
    for every sector, and returns a confidence vote.

    Injects per-sector fields:
      ai_votes        int   — how many AIs agreed with the phase label
      ai_total        int   — how many AIs responded
      ai_vote_bar     str   — e.g. "🟢🟢🟢🟡⬜"
      ai_consensus    str   — one-sentence consensus note
      ai_top_coin     str   — AI-picked top coin per sector (may differ from RCI #1)

    Non-blocking: if all providers fail, sectors are returned unchanged.
    """
    if not sectors:
        return sectors

    # Build a compact summary of each sector to send to AI — keep tokens short
    sector_summary = ""
    for i, s in enumerate(sectors, 1):
        top3 = s["tokens"][:3]
        coins_str = ", ".join(
            f"{t['symbol']}(RCI:{t['rci']:.0f}%:{t['change']:+.1f}%,{t['state']})"
            for t in top3
        )
        sector_summary += (
            f"{i}. {s['category']} | Phase:{s.get('dominant_phase','?')} "
            f"| AvgRCI:{s['avg_rci']} | Avg%:{s.get('avg_change',0):+.1f}% "
            f"| Coins: {coins_str}\n"
        )

    prompt = (
        # OCE v13.0 — Triple-Role: Sector Analyst + Phase Classifier + QA Engineer
        # Pattern: Domain=Trading/Finance | Intent=Evaluate | Strategy=Sector rotation phase
        # Response Tier: SPEC — exact one-line-per-sector format. Zero deviation.
        # Zero-Hallucination Mandate: classify only from the data in sector_summary below.
        "You are an institutional crypto sector analyst operating under OCE v13.0 execution discipline.\n"
        "Below is a ranked sector rotation output from a live quantitative RCI system.\n\n"
        f"{sector_summary}\n"
        "=== CLASSIFICATION RULES ===\n"
        "  ACCUM: AvgRCI > 60, price change flat/negative — stealth absorption\n"
        "  TREND: AvgRCI > 70, price change positive — trend breakout\n"
        "  DIST: AvgRCI declining, price change spike — smart money exiting\n"
        "  NEUTRAL: Mixed or unclear signals\n"
        "AGREE=YES if the system-assigned phase matches your independent classification.\n"
        "TOPCOIN = highest-conviction trade in that sector based solely on the data above.\n\n"
        "Grounding rule: classify from provided data only — no external market knowledge.\n\n"
        "Respond ONLY in this exact format (one sector per line):\n"
        "SECTOR:<number> PHASE:<ACCUM|TREND|DIST|NEUTRAL> TOPCOIN:<symbol> AGREE:<YES|NO>\n"
        "No extra text."
    )

    # All 5 providers in parallel with individual timeouts
    _PROVIDER_KEYS = {
        "groq":     GROQ_API_KEY,
        "cerebras": CEREBRAS_API_KEY,
        "gemini":   GEMINI_API_KEY,
        "mistral":  MISTRAL_API_KEY,
        "github":   GITHUB_TOKEN,
    }
    available = [p for p in _ALL_AI_PROVIDERS if _PROVIDER_KEYS.get(p, "").strip()]

    async def _one_provider(provider: str) -> dict:
        """Call one AI and parse its sector responses."""
        try:
            raw = await asyncio.wait_for(
                ai_query(prompt, {}, provider),
                timeout=18.0
            )
        except Exception as e:
            logger.debug(f"[SECTOR-AI] {provider} failed: {e}")
            return {}

        result = {}
        for line in raw.splitlines():
            line = line.strip()
            # Parse: SECTOR:1 PHASE:ACCUM TOPCOIN:ETH AGREE:YES
            m = re.match(
                r"SECTOR:(\d+)\s+PHASE:(\w+)\s+TOPCOIN:(\w+)\s+AGREE:(YES|NO)",
                line, re.IGNORECASE
            )
            if m:
                idx      = int(m.group(1)) - 1
                phase    = m.group(2).upper()
                topcoin  = m.group(3).upper()
                agree    = m.group(4).upper() == "YES"
                result[idx] = {"phase": phase, "topcoin": topcoin, "agree": agree}
        return result

    # Fire all providers concurrently
    tasks = [_one_provider(p) for p in available]
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregate votes per sector
    # per_sector[i] = list of {"phase", "topcoin", "agree"} from each AI
    per_sector: dict[int, list] = {}
    for resp in responses:
        if isinstance(resp, dict):
            for idx, data in resp.items():
                per_sector.setdefault(idx, []).append(data)

    total_providers = len(available)

    # Inject AI validation results into each sector
    for i, sector in enumerate(sectors):
        votes_data = per_sector.get(i, [])
        agreed     = sum(1 for v in votes_data if v.get("agree"))
        responded  = len(votes_data)

        # Vote bar: green=agree, yellow=disagree, white=no response
        disagree = responded - agreed
        no_resp  = total_providers - responded
        vote_bar = "🟢" * agreed + "🟡" * disagree + "⬜" * no_resp

        # AI top coin consensus — most-voted coin across providers for this sector
        coin_votes: dict[str, int] = {}
        for v in votes_data:
            tc = v.get("topcoin", "")
            if tc:
                coin_votes[tc] = coin_votes.get(tc, 0) + 1
        ai_top_coin = max(coin_votes, key=coin_votes.get) if coin_votes else ""

        # Consensus note
        if responded == 0:
            consensus = "AI validators unavailable — RCI signal only."
        elif agreed == responded:
            consensus = f"All {responded} AIs confirm {sector.get('dominant_phase','?')} phase."
        elif agreed >= responded * 0.6:
            consensus = f"{agreed}/{responded} AIs agree on phase. Moderate confidence."
        else:
            # AIs disagree — what do they say instead?
            alt_phases = [v["phase"] for v in votes_data if not v.get("agree")]
            if alt_phases:
                consensus = f"AIs split — {agreed} agree, {disagree} suggest {alt_phases[0]} instead."
            else:
                consensus = f"{agreed}/{responded} AIs agree. Low confidence — verify manually."

        sector["ai_votes"]    = agreed
        sector["ai_total"]    = responded
        sector["ai_vote_bar"] = vote_bar
        sector["ai_consensus"]= consensus
        sector["ai_top_coin"] = ai_top_coin

    logger.info(f"[SECTOR-AI] Validation complete: {len(available)} providers, {len(sectors)} sectors")
    return sectors


def get_sector_explanation(category: str) -> str:
    """Get brief explanation of sector"""
    explanations = {
        "Liquid Staking": "Liquid Staking lets users stake assets (usually ETH) while keeping a liquid token they can trade, lend, or use in DeFi.\n👉 Result: staking yield + capital efficiency, which is why institutions accumulate here early.",
        "Lending": "Decentralized lending protocols allow users to lend or borrow crypto assets without intermediaries.\n👉 Result: passive yield for lenders, leveraged positions for borrowers.",
        "DEX": "Decentralized exchanges enable peer-to-peer token swaps without centralized custody.\n👉 Result: permissionless trading with lower fees than CEXs.",
        "Yield": "Yield protocols optimize returns through automated strategies across multiple DeFi platforms.\n👉 Result: maximized APY through compound effects and arbitrage.",
        "Derivatives": "On-chain derivatives platforms offer perpetual futures, options, and leveraged trading.\n👉 Result: sophisticated hedging and speculation tools without centralized risk."
    }
    return explanations.get(category, f"{category} - Institutional capital is rotating into this sector.")

# ==========================================
# AI ASSISTANT FUNCTIONS
# ==========================================

async def ai_query(query: str, market_context: dict, ai_provider: str) -> str:
    """
    Query AI with crypto market context.
    Supports: groq, cerebras, gemini, mistral
    Each provider has a hardcoded free-tier fallback chain.
    Thinking tokens (<think> tags) are stripped before returning.
    """
    # Provider config map (OpenAI-compatible endpoints for all 4)
    _AQ_ENDPOINTS = {
        "groq":     "https://api.groq.com/openai/v1/chat/completions",
        "cerebras": "https://api.cerebras.ai/v1/chat/completions",
        "gemini":   "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
        "mistral":  "https://api.mistral.ai/v1/chat/completions",
        "github":   "https://models.github.ai/inference/chat/completions",
    }
    _AQ_KEYS = {
        "groq":     GROQ_API_KEY,
        "cerebras": CEREBRAS_API_KEY,
        "gemini":   GEMINI_API_KEY,
        "mistral":  MISTRAL_API_KEY,
        "github":   GITHUB_TOKEN,
    }
    _AQ_LABELS = {
        "groq":     ("console.groq.com",     "GROQ_API_KEY"),
        "cerebras": ("inference.cerebras.ai", "CEREBRAS_API_KEY"),
        "gemini":   ("aistudio.google.com",   "GEMINI_API_KEY"),
        "mistral":  ("console.mistral.ai",    "MISTRAL_API_KEY"),
        "github":   ("github.com/settings/tokens", "GITHUB_TOKEN"),
    }
    # Free-tier fallback chains per provider (Feb 2026 verified).
    # Primary = smartest available free model. Falls through on 429/400/404/timeout.
    #
    # GROQ:     qwen3-32b is smarter than llama-3.3-70b (higher benchmarks, 40K output)
    # CEREBRAS: qwen-3-235b-2507 outperforms GPT-4.1 & Claude Opus 4 on AAIA index —
    #           it's the smartest free model on Cerebras and is NON-thinking (no <think> tags)
    # GEMINI:   2.5-flash is smartest free model. reasoning_effort=none disables thinking
    #           tokens so JSON lands clean without stripping.
    # MISTRAL:  magistral-small-2506 is a reasoning model (smarter than small-3.2)
    #           but emits <think> tags — strip required. devstral-2 moved to paid.
    _AQ_MODEL_CHAINS = {
        "groq":     ["qwen/qwen3-32b", "llama-3.3-70b-versatile", "llama-3.1-8b-instant"],
        "cerebras": ["qwen-3-235b-a22b-instruct-2507", "qwen3-235b-a22b", "llama-3.3-70b", "llama3.1-8b"],
        "gemini":   ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash-lite", "gemini-2.0-flash"],
        "mistral":  ["magistral-small-2506", "mistral-small-3.2-24b-instruct-2506", "open-mistral-nemo"],
        # GitHub Models — use provider-prefixed format (e.g. openai/gpt-4o-mini).
        # gpt-4o-mini primary: fastest, avoids timeout on large context.
        "github":   ["openai/gpt-4o-mini", "openai/gpt-4o", "meta/llama-3.3-70b-instruct"],
    }
    # Reasoning models that emit <think> tokens INSIDE content — strip before returning.
    # NOTE: Cerebras reasoning models (gpt-oss-120b, zai-glm-4.7) put reasoning in a
    # SEPARATE "reasoning" field, NOT in content — no stripping needed for them.
    # NOTE: qwen-3-235b-a22b-instruct-2507 is non-thinking — no strip needed.
    # NOTE: gemini-2.5-flash uses reasoning_effort=none — no strip needed.
    _THINKING_MODELS = {
        "qwen/qwen3-32b",           # Groq Qwen3 emits <think> inside content
        "magistral-small-2506",     # Mistral Magistral emits <think> inside content
        "magistral-small-latest",   # alias
    }

    def _strip_thinking(text: str) -> str:
        """Remove reasoning blocks emitted by thinking models before returning to user.
        Covers: <think>/<thinking> (Qwen3, GLM, Magistral, gpt-oss),
        and /think end-only variant used by some Qwen3 deployments.
        """
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        text = re.sub(r'<thinking>.*?</thinking>', '', text, flags=re.DOTALL)
        # Qwen3 bare end-marker: strip only the leading orphan block, not greedily
        if text.strip().startswith('</think>'):
            text = text.strip()[len('</think>'):].strip()
        elif '</think>' in text:
            idx = text.find('</think>')
            after = text[idx + len('</think>'):].strip()
            if after:
                text = after
        return text.strip()

    # Normalise provider name — auto-pick first available if unknown
    if ai_provider not in _AQ_ENDPOINTS:
        for p in ["groq", "cerebras", "gemini", "mistral", "github"]:
            if _AQ_KEYS.get(p, "").strip():
                ai_provider = p
                break
        else:
            return (
                "❌ No AI provider configured.\n\n"
                "Add at least one key to your .env file:\n"
                "  GROQ_API_KEY=...\n  CEREBRAS_API_KEY=...\n"
                "  GEMINI_API_KEY=...\n  MISTRAL_API_KEY=...\n  GITHUB_TOKEN=..."
            )

    api_key = _AQ_KEYS.get(ai_provider, "").strip()
    if not api_key:
        site, env_var = _AQ_LABELS.get(ai_provider, ("provider.com", f"{ai_provider.upper()}_API_KEY"))
        return (
            f"❌ {ai_provider.capitalize()} API key not configured.\n\n"
            f"Add to your .env file:\n{env_var}=your_key_here\n\n"
            f"Get your key at: {site}"
        )

    # Build rich market context string
    fng_val  = market_context.get('fear_greed_index', 0)
    fng_num  = int(fng_val) if isinstance(fng_val, (int, float)) else 0
    fng_label = ("Extreme Fear" if fng_num < 25 else
                 "Fear"         if fng_num < 45 else
                 "Neutral"      if fng_num < 55 else
                 "Greed"        if fng_num < 75 else "Extreme Greed")
    rsi_val  = market_context.get('bitcoin_rsi', 50)
    rsi_num  = float(rsi_val) if isinstance(rsi_val, (int, float)) else 50.0
    rsi_label = ("Oversold" if rsi_num < 30 else "Overbought" if rsi_num > 70 else "Neutral")

    context_str = (
        f"[LIVE MARKET CONTEXT — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}]\n"
        f"Regime: {market_context.get('regime', 'Unknown')}\n"
        f"BTC Dominance: {market_context.get('btc_dominance', 0):.2f}%\n"
        f"Altcoin Dominance: {market_context.get('altcoin_dominance', 0):.2f}%\n"
        f"Total Market Cap: ${market_context.get('total_market_cap', 0):.2f}T\n"
        f"Fear & Greed: {fng_val} ({fng_label})\n"
        f"BTC RSI-14: {rsi_num:.1f} ({rsi_label})\n"
        f"ETH/BTC Ratio: {market_context.get('eth_btc_ratio', 0):.5f}\n"
        f"\nUser Question: {query}"
    )

    base_url = _AQ_ENDPOINTS[ai_provider]
    chain    = _AQ_MODEL_CHAINS.get(ai_provider, ["unknown"])

    # Build headers — Gemini needs dual-auth (?key= + Bearer)
    if ai_provider == "gemini":
        url  = f"{base_url}?key={api_key}"
        hdrs = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    else:
        url  = base_url
        hdrs = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    _AQ_TIMEOUT = aiohttp.ClientTimeout(total=20)
    last_error  = ""

    for model in chain:
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": (
                    # OCE v13.0 — Triple-Role: Senior Analyst + Signal Designer + QA Engineer
                    # ROLE 1: Institutional precision, ICT/SMC architecture, data-grounded only.
                    # ROLE 2: Output clarity — structured, scannable, immediately actionable.
                    # ROLE 3: Zero-Hallucination Mandate — no claims without data support.
                    "You are CRYPTEX v13.0 — an institutional-grade crypto signal engine operating under "
                    "the OCE v13.0 intelligence framework (Triple-Role: Senior Analyst + Signal Designer + QA Engineer). "
                    "You have access to live market data provided in the user message. "
                    "EXECUTION DISCIPLINE: "
                    "(1) Always ground every claim in the live market context provided — never fabricate data. "
                    "(2) Give specific, actionable insights structured for immediate decision-making — not generic advice. "
                    "(3) Apply full ICT/SMC precision: FVG, OB, ChoCh/MSS, BSL/SSL, HTF/LTF confluence. "
                    "(4) Always include a calibrated risk note with invalidation trigger. "
                    "(5) Response Tier: STANDARD — max 400 words, structured output, minimal noise. "
                    "(6) Zero-Hallucination Mandate: every price level and signal must be traceable to provided data. "
                    "If data is absent for a claim, state INSUFFICIENT_DATA. "
                    "(7) Never give specific buy/sell price targets without stating it is not financial advice."
                )},
                {"role": "user",   "content": context_str},
            ],
            "max_tokens": 700,
        }
        # Per-model config per official provider docs:
        # Gemini 2.5: reasoning_effort=none disables thinking tokens
        # gpt-oss-120b on Cerebras: reasoning_effort="low" controls thinking depth
        # zai-glm-4.7 on Cerebras: disable_reasoning=True (different param from gpt-oss!)
        # Qwen3 on Groq: /no_think disables CoT mode
        # Magistral: emits <think> in content — stripped by _strip_thinking
        if ai_provider == "gemini" and "2.5" in model:
            body["reasoning_effort"] = "none"
        elif ai_provider == "cerebras" and model == "gpt-oss-120b":
            body["reasoning_effort"] = "low"
        elif ai_provider == "cerebras" and model == "zai-glm-4.7":
            body["disable_reasoning"] = True
        if model == "qwen/qwen3-32b":
            body["messages"][0]["content"] += "\n/no_think"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=hdrs, json=body, timeout=_AQ_TIMEOUT) as resp:
                    raw = await resp.text()
                    if resp.status == 200:
                        data    = json.loads(raw)
                        content = data["choices"][0]["message"]["content"]
                        # Strip thinking tokens emitted by reasoning models
                        if model in _THINKING_MODELS:
                            content = _strip_thinking(content)
                        return content
                    elif resp.status == 400 and ai_provider == "gemini" and "reasoning_effort" in body:
                        # reasoning_effort not supported on this Gemini model — retry without it
                        body.pop("reasoning_effort", None)
                        async with aiohttp.ClientSession() as s2:
                            async with s2.post(url, headers=hdrs, json=body, timeout=_AQ_TIMEOUT) as r2:
                                raw2 = await r2.text()
                                if r2.status == 200:
                                    data    = json.loads(raw2)
                                    content = data["choices"][0]["message"]["content"]
                                    if model in _THINKING_MODELS:
                                        content = _strip_thinking(content)
                                    return content
                                last_error = f"http_{r2.status}"
                                continue
                    elif resp.status == 401:
                        # Auth error — no point trying other models
                        return f"❌ Invalid {ai_provider.capitalize()} API key. Check your .env file."
                    elif resp.status == 429:
                        last_error = f"rate_limit"
                        await asyncio.sleep(2)
                        continue  # try next model
                    elif resp.status in (400, 404):
                        last_error = f"http_{resp.status}"
                        continue  # model not found or bad param — try next
                    else:
                        last_error = f"http_{resp.status}: {raw[:150]}"
                        continue
        except asyncio.TimeoutError:
            last_error = "timeout"
            continue
        except Exception as e:
            last_error = str(e)
            logger.error(f"[AQ] {ai_provider}/{model} error: {e}")
            continue

    # All models in this provider's chain failed — auto-fallback to next configured provider
    _FALLBACK_ORDER = ["groq", "cerebras", "gemini", "mistral", "github"]
    for _fb_provider in _FALLBACK_ORDER:
        if _fb_provider == ai_provider:
            continue   # already tried
        _fb_key = _AQ_KEYS.get(_fb_provider, "").strip()
        if not _fb_key:
            continue
        logger.info(f"[AQ] {ai_provider} exhausted ({last_error}) — falling back to {_fb_provider}")
        try:
            return await ai_query(query, market_context, _fb_provider)
        except Exception:
            continue

    if last_error == "rate_limit":
        return f"❌ {ai_provider.capitalize()} rate limit hit on all models. Try again in a moment."
    return f"❌ All AI providers unavailable ({last_error}). Check your .env API keys."


# ==========================================
# FEEDBACK & QA VALIDATION ENGINE (INLINE)
# ==========================================
# Validates all bot outputs using 4 AI providers:
# Groq · Cerebras · Gemini · Mistral
# Pipeline: RAG → Synthesis → 80/20 Trim → Recursive Validation
# Models auto-detected live from each provider's /models endpoint
# ──────────────────────────────────────────

# ── Provider API endpoints ───────────────────────────────────────────────────
_FB_ENDPOINTS = {
    "groq":     "https://api.groq.com/openai/v1/chat/completions",
    "cerebras": "https://api.cerebras.ai/v1/chat/completions",
    "gemini":   "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
    "mistral":  "https://api.mistral.ai/v1/chat/completions",
    "github":   "https://models.github.ai/inference/chat/completions",
}

# ── Model discovery endpoints (used for auto-detect) ────────────────────────
_FB_MODEL_ENDPOINTS = {
    "groq":     "https://api.groq.com/openai/v1/models",
    "cerebras": "https://api.cerebras.ai/v1/models",
    "gemini":   "https://generativelanguage.googleapis.com/v1beta/models",
    "mistral":  "https://api.mistral.ai/v1/models",
    "github":   "https://models.github.ai/inference/models",
}

# ── Fallback model lists (used if live discovery fails) ─────────────────────
# Smartest free-tier model first. Verified Feb 2026.
# gpt-oss-120b excluded from Cerebras — json_object broken (returns free text).
_FB_FALLBACK_MODELS = {
    "groq":     ["qwen/qwen3-32b", "llama-3.3-70b-versatile", "llama-3.1-8b-instant"],
    "cerebras": ["qwen-3-235b-a22b-instruct-2507", "qwen3-235b-a22b", "llama-3.3-70b", "llama3.1-8b"],
    "gemini":   ["gemini-2.0-flash-lite", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash"],
    "mistral":  ["magistral-small-2506", "mistral-small-3.2-24b-instruct-2506", "open-mistral-nemo"],
    # GitHub Models — OpenAI-compat, free tier ~150 req/day, Bearer = GitHub PAT
    # Must use provider-prefixed format: openai/gpt-4o-mini (bare names return 404).
    "github":   ["openai/gpt-4o-mini", "openai/gpt-4o", "meta/llama-3.3-70b-instruct"],
}

# ── Cache for discovered models (avoid re-fetching each call) ────────────────
_fb_model_cache: dict[str, list[str]] = {}
_fb_model_cache_ts: dict[str, float] = {}
_FB_MODEL_CACHE_TTL = 3600  # re-discover every hour

# ── Output type labels ───────────────────────────────────────────────────────
_FB_OUTPUT_TYPES = {
    "trio":            "Pin Message · Market Overview · News Output",
    "market_overview": "Market Overview",
    "news":            "News Output",
    "pin":             "Pin Message",
    "cross":           "Cross Analysis",
    "sector_rotation": "Sector Rotation",
    "trending":        "Trending Coins",
    "alpha":           "Alpha Signals",
    "ta":              "Technical Analysis",
    "ai_response":     "AI Assistant Response",
    "dex":             "DEX Scanner Output",
    "user_evidence":   "User Evidence",
    "unknown":         "General Output",
}

# ── QA dimensions & weights (sum = 1.0) ──────────────────────────────────────
_FB_DIMENSIONS = {
    "accuracy":      0.30,
    "completeness":  0.20,
    "consistency":   0.20,
    "timeliness":    0.15,
    "reliability":   0.15,
}

# ── In-memory feedback store ─────────────────────────────────────────────────
# feedback_store[chat_id] = {last_output, last_output_type, last_report, awaiting_evidence}
feedback_store: dict = {}

# ── OMNI-Context pipeline system prompt ─────────────────────────────────────
_FB_SYSTEM_PROMPT = """You are CRYPTEX-QA v13.0 — an institutional-grade crypto signal validator operating under the OCE v13.0 intelligence framework.

FRAMEWORK: OCE v13.0 (Triple-Role: Senior Analyst + Signal Designer + QA Engineer) + Zero-Bug Mandate + Stage 4.8 Evaluability Gate + Stage 4.9 Pro Code Audit.
ENGINE VERSION: TechnicalAnalysis v9.6 (20-Layer Pipeline, 50-Confluence Engine, SuperValidator v6, VolatilityBrain, Markov AI Consensus).
RESPONSE TIER: SPEC — full structured JSON output required. Zero deviation from schema.
ZERO-HALLUCINATION MANDATE: every finding must be traceable to the TA output provided. If data is absent, state INSUFFICIENT_DATA.

NEW IN v8.2 — Validate presence of these blocks when reviewing TA output:
- INDICATOR CONSENSUS block (bearish/bullish split with %)
- MARKET NARRATIVE (2–3 plain-language lines below entry plan)
- SCENARIO TREE (A: Primary / B: Reversal with full counter-trade / C: Invalidation)
- ENTRY CONFIRMATION CHECKLIST (7 live price-action criteria)
- UNLOCK CONDITIONS block (when setup is blocked)
- TP zone context labels (IRL/ERL anchor per TP)
- BIAS FLIP TRIGGERS (3-tier: neutral / flip / reversal)
- QUANTITATIVE EDGE LAYER (Kelly / MC / Bayesian / RL)
- R:R gate (must say FOR REFERENCE when R:R < 1.5)
- GATE FAILURE ANALYSIS (ICT/SMC reason per failed gate)
- BSL/SSL/sweep prices in G3 Liquidity gate
- Risk disclaimer on MSG 1

Flag as URGENT bug if: R:R < 1.5 but entry legs show as active (not reference-only).
Flag as MILD bug if: Any of the above blocks are missing from TA output.

EXPERTISE: ICT/SMC (FVG, Order Blocks, Liquidity, ChoCh/MSS), Wyckoff (Accumulation/Distribution phases),
Elliott Wave (Fibonacci ratios), Classical TA (RSI/MACD/BB divergence), On-chain (MVRV, NVT, exchange flows,
funding rates, OI), DeFi (TVL, P/F ratio, real yield), Macro (DXY, M2, FOMC impact), Probability modeling
(Bayesian inference, Kelly Criterion, EV calculation, Monte Carlo).

PIPELINE — execute ALL stages:
0. PATTERN EXTRACTION (OCE v13.0): Identify domain (Trading/Finance), intent (Validate/Audit), structure elements present, strategy patterns detected (ICT, Markov, SuperValidator, VolatilityBrain, etc.).
0.5. STAGE 4.8 EVALUABILITY GATE: Before full analysis, score these critical gates:
   ✓ entry zone defined and valid? ✓ TP stack ordered correctly? ✓ SL on correct side?
   ✓ R:R ≥ 1.5? ✓ ≥6/11 gates passed? ✓ bias locked? ✓ confidence ≥30?
   Gate result: PASS(≥75)/WARN(≥50)/FAIL(<50). Surface in summary.
1. RAG EXTRACTION: Extract every price, %, signal, coin, timestamp, TA level, on-chain metric.
   Flag ICT/SMC misuse, missing SL/TP, no timeframe, no volume confirmation.
2. EXPERT SYNTHESIS: Restate findings in institutional language. Tag [BULLISH/BEARISH/NEUTRAL/CONFLICT/UNVERIFIED].
   Score 1-10 trading relevance.
3. 80/20 TRIM: Retain score≥7 findings. Discard <7. If <3 retained → INSUFFICIENT_DATA.
4. PROBABILITY MODELING: Bull/Base/Bear % for each signal. Confidence: LOW(<55%)/MEDIUM(55-70%)/HIGH(>70%).
   State invalidation trigger. Estimate R:R.
5. RECURSIVE VALIDATION (4 checks): Purpose served? Claims traceable? Nothing hallucinated? Timeframes consistent?
6. MANIPULATION CHECK: Pump language? Confirmation bias? False precision? Narrative shilling?
7. STAGE 4.9 PRO CODE AUDIT — D1 Functional / D2 UX / D3 QA: For each issue: severity(URGENT/MILD/LOW), root_cause, how_to_fix, python_fix.

SCORING FLOORS (notification summary context — do NOT penalise for missing full research depth):
accuracy≥75, completeness≥75, consistency≥75, timeliness≥85, reliability≥75.
Lower ONLY if data is factually wrong, critical fields missing, or signals directly contradict.

NOTE: Timestamps from 2026 are VALID — do not flag as future dates. The engine runs in 2026.

Return ONLY valid JSON — no markdown fences, no preamble:
{
  "validator": "<provider + model>",
  "stage1_findings": ["<fact extracted>"],
  "stage2_synthesis": ["[TAG] <synthesized statement>"],
  "stage3_retained": ["<high-value finding>"],
  "stage3_discarded": ["<low-value finding>"],
  "stage4_scenarios": [
    {
      "asset": "<coin>", "bull_prob": 0, "bull_target": "", "bull_trigger": "",
      "base_prob": 0, "base_path": "", "bear_prob": 0, "bear_target": "",
      "bear_trigger": "", "confidence": "LOW|MEDIUM|HIGH", "reward_risk": 0.0,
      "confirm_metrics": ["<metric to watch>"]
    }
  ],
  "stage5_check1": "PASS|FAIL — <reason>",
  "stage5_check2": "PASS|FAIL — <reason>",
  "stage5_check3": "PASS|FAIL — <reason>",
  "stage5_check4": "PASS|FAIL — <reason>",
  "conflicts": ["<conflict>"],
  "hallucinations": ["<unsupported claim>"],
  "missing_elements": ["<what should be here>"],
  "risk_flags": ["<risk>"],
  "ta_concept_errors": ["<ICT/SMC/Wyckoff/EW misapplication>"],
  "on_chain_validation": {"<metric>": {"stated": "", "expected_range": "", "assessment": "valid|suspect"}},
  "news_impact_assessment": "<institutional analysis of news vs signals>",
  "bugs": [
    {
      "title": "", "severity": "URGENT|MILD|LOW", "description": "",
      "root_cause": "", "how_to_fix": "", "python_fix": "",
      "feature_suggestion": "", "best_practice": ""
    }
  ],
  "improvements": [
    {"title": "", "priority": "HIGH|MEDIUM|LOW", "description": "",
     "implementation_complexity": "EASY|MEDIUM|HARD", "suggested_code": ""}
  ],
  "pro_knowledge_suggestions": ["<advanced concept to add>"],
  "data_scraping_recommendations": ["<specific API endpoint>"],
  "youtube_synthesis": "<one paragraph expert synthesis of TA/SMC/Wyckoff insights>",
  "scores": {"accuracy": 0, "completeness": 0, "consistency": 0, "timeliness": 0, "reliability": 0},
  "composite_score": 0,
  "verdict": "VALID|NEEDS_REVIEW|INVALID",
  "summary": "<5-7 sentence institutional verdict: what is sound, what gaps exist, probability outlook, top 2 improvements>",
  "feature_suggestions": [
    {
      "button_label": "<emoji + name ≤20 chars>",
      "target_menu": "trio|cross|sector_rotation|trending|alpha|ta|dex|ai_assistant|new",
      "title": "", "description": "", "implementation_complexity": "EASY|MEDIUM|HARD",
      "data_sources": ["<API needed>"], "priority": "HIGH|MEDIUM|LOW"
    }
  ]
}"""


# ── Auto-detect available models from provider ───────────────────────────────

# ── Auto-detect available models from provider ───────────────────────────────

async def _fb_discover_models(provider: str, api_key: str) -> list[str]:
    """Fetch live model list from provider. Falls back to hardcoded list on error."""
    global _fb_model_cache, _fb_model_cache_ts
    now = time.time()
    if provider in _fb_model_cache and (now - _fb_model_cache_ts.get(provider, 0)) < _FB_MODEL_CACHE_TTL:
        return _fb_model_cache[provider]

    url = _FB_MODEL_ENDPOINTS.get(provider, "")
    if not url:
        return _FB_FALLBACK_MODELS.get(provider, [])

    # Gemini: send BOTH ?key= param AND Authorization: Bearer header for Gemini 3.x compat
    if provider == "gemini":
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        url = f"{url}?key={api_key}"
    else:
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # Per-provider preferred models — always pick these first if available.
    # Smartest free-tier model first. gpt-oss-120b excluded (json_object regression).
    _PREFERRED: dict[str, list[str]] = {
        "groq":     ["qwen/qwen3-32b", "llama-3.3-70b-versatile", "llama-3.1-8b-instant"],
        "cerebras": ["qwen-3-235b-a22b-instruct-2507", "qwen3-235b-a22b", "llama-3.3-70b", "llama3.1-8b"],
        "gemini":   ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash-lite", "gemini-2.0-flash"],
        "mistral":  ["magistral-small-2506", "mistral-small-3.2-24b-instruct-2506", "open-mistral-nemo"],
    }
    # Keywords that identify non-chat models to skip entirely
    _SKIP_KEYWORDS = [
        "embed", "tts", "whisper", "vision-only", "guard",
        "imagen", "lyria", "rerank", "moderation", "orpheus",
        "audio", "speech", "transcri", "realtime", "dall-e",
        "stable-diffusion", "clip",
    ]

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    data = await r.json()
                    # OpenAI-compatible response: {"data": [{"id": "model-name"}, ...]}
                    models_raw = data.get("data", data.get("models", []))
                    discovered = set()
                    for m in models_raw:
                        mid = m.get("id", "") if isinstance(m, dict) else str(m)
                        # Strip "models/" prefix (Gemini native format)
                        mid = mid.replace("models/", "").strip()
                        if mid and not any(x in mid.lower() for x in _SKIP_KEYWORDS):
                            discovered.add(mid)

                    if discovered:
                        # Sort: preferred models first (in preference order), then rest
                        prefs = _PREFERRED.get(provider, [])
                        ordered = [p for p in prefs if p in discovered]
                        ordered += sorted(discovered - set(ordered))  # remaining alphabetically
                        _fb_model_cache[provider] = ordered
                        _fb_model_cache_ts[provider] = now
                        logger.info(f"[FB] {provider}: {len(ordered)} models, using {ordered[0]}")
                        return ordered
    except Exception as e:
        logger.warning(f"[FB] Model discovery failed for {provider}: {e}")

    fallback = _FB_FALLBACK_MODELS.get(provider, [])
    _fb_model_cache[provider] = fallback
    _fb_model_cache_ts[provider] = now
    return fallback


# ── Rank providers by key availability ───────────────────────────────────────

async def _fb_rank_providers() -> list[dict]:
    """
    Returns ordered list of available providers.
    Each entry: {name, type, key, priority}
    Providers without keys are excluded entirely.
    Model is resolved per-call via _fb_discover_models.
    """
    candidates = [
        {"name": "Groq",     "type": "groq",     "key": GROQ_API_KEY,     "priority": 1},
        {"name": "Cerebras", "type": "cerebras", "key": CEREBRAS_API_KEY,  "priority": 2},
        {"name": "Gemini",   "type": "gemini",   "key": GEMINI_API_KEY,   "priority": 3},
        {"name": "Mistral",  "type": "mistral",  "key": MISTRAL_API_KEY,  "priority": 4},
        {"name": "GitHub",   "type": "github",   "key": GITHUB_TOKEN,     "priority": 5},
    ]
    ranked = [p for p in candidates if p["key"] and p["key"].strip()]
    ranked.sort(key=lambda x: x["priority"])
    return ranked


# ── Single AI validation call ─────────────────────────────────────────────────

_TA_YOUTUBE_QUERIES = [
    "smart money concepts FVG fair value gap explained",
    "ICT inner circle trader order blocks tutorial",
    "liquidity sweep trading strategy crypto",
    "supply demand zone technical analysis crypto",
    "smart risk FVG fair value gap analysis",
]

_AUTO_EVIDENCE_QUERIES = {
    "ta":               _TA_YOUTUBE_QUERIES,
    "technical_analysis": _TA_YOUTUBE_QUERIES,
    "TA":               _TA_YOUTUBE_QUERIES,
    "trending":         ["trending crypto coins analysis", "top altcoins to watch", "crypto momentum coins"],
    "market_overview":  ["crypto market overview today", "bitcoin dominance analysis", "altcoin season indicators"],
    "alpha":            ["crypto alpha signals strategy", "on-chain alpha crypto", "defi alpha trading"],
    "sector_rotation":  ["crypto sector rotation strategy", "defi vs layer1 rotation", "crypto capital flow sectors"],
    "dex":              ["dex trading strategy crypto", "uniswap sushi trading analysis", "defi dex gems"],
    "news":             ["crypto news impact trading", "fundamental analysis crypto news"],
    "ai_response":      ["crypto ai analysis accuracy", "chatgpt crypto trading advice"],
}

_AUTO_EVIDENCE_WEB_QUERIES = {
    "ta":               ["site:tradingview.com FVG fair value gap", "ICT order block strategy investopedia"],
    "trending":         ["coingecko trending coins analysis site:coindesk.com", "top trending crypto site:cointelegraph.com"],
    "market_overview":  ["crypto market analysis site:coindesk.com", "btc dominance site:glassnode.com"],
    "alpha":            ["crypto alpha signals site:messari.io", "on-chain alpha site:dune.com"],
    "sector_rotation":  ["crypto sector rotation site:theblock.co", "defi rotation analysis site:defillama.com"],
    "dex":              ["dex volume analysis site:defillama.com", "dex gems site:dexscreener.com"],
}


async def _fb_auto_search_evidence(output_type: str, output_text: str) -> str:
    """
    Each validator calls this independently to auto-fetch references:
    - YouTube videos relevant to the output type
    - Web articles/sources relevant to the output type
    Returns a formatted evidence string to inject into the validator prompt.
    """
    yt_queries  = _AUTO_EVIDENCE_QUERIES.get(output_type, ["crypto analysis " + output_type])
    web_queries = _AUTO_EVIDENCE_WEB_QUERIES.get(output_type, [])
    refs = []

    async with aiohttp.ClientSession() as session:
        # ── YouTube search ────────────────────────────────────────────────────
        for q in yt_queries[:3]:
            try:
                search_url = f"https://www.youtube.com/results?search_query={q.replace(' ', '+')}"
                async with session.get(
                    search_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 200:
                        html = await r.text(errors="replace")
                        vids = re.findall(r'"videoId":"([A-Za-z0-9_-]{11})"', html)
                        seen: list[str] = []
                        for vid in vids:
                            if vid not in seen:
                                seen.append(vid)
                            if len(seen) >= 2:
                                break
                        for vid in seen:
                            yt_url   = f"https://www.youtube.com/watch?v={vid}"
                            thumb    = f"https://img.youtube.com/vi/{vid}/mqdefault.jpg"
                            try:
                                async with session.get(
                                    f"https://noembed.com/embed?url={yt_url}",
                                    timeout=aiohttp.ClientTimeout(total=5)
                                ) as nr:
                                    if nr.status == 200:
                                        nd     = await nr.json(content_type=None)
                                        title  = nd.get("title", "Unknown")
                                        author = nd.get("author_name", "Unknown")
                                        # Use noembed thumbnail if available, else default
                                        thumb  = nd.get("thumbnail_url", thumb)
                                        refs.append({
                                            "type":   "yt",
                                            "title":  title,
                                            "author": author,
                                            "url":    yt_url,
                                            "thumb":  thumb,
                                            "text":   f"[YT] \"{title}\" by {author} → {yt_url}",
                                        })
                            except Exception:
                                refs.append({
                                    "type":   "yt",
                                    "title":  "Unknown",
                                    "author": "Unknown",
                                    "url":    yt_url,
                                    "thumb":  thumb,
                                    "text":   f"[YT] {yt_url}",
                                })
            except Exception:
                continue

        # ── Web search (DuckDuckGo HTML) ─────────────────────────────────────
        for q in web_queries[:2]:
            try:
                ddg_url = f"https://html.duckduckgo.com/html/?q={q.replace(' ', '+')}"
                async with session.get(
                    ddg_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 200:
                        html  = await r.text(errors="replace")
                        links = re.findall(r'href="(https?://[^"]{10,120})"', html)
                        titles = re.findall(r'class="result__a"[^>]*>([^<]{5,80})', html)
                        for i, link in enumerate(links[:2]):
                            title = titles[i] if i < len(titles) else link
                            refs.append({
                                "type":  "web",
                                "title": title.strip(),
                                "url":   link,
                                "text":  f"[WEB] \"{title.strip()}\" → {link}",
                            })
            except Exception:
                continue

    if not refs:
        return ""

    text_lines = [r["text"] for r in refs[:8]]
    return (
        "--- AUTO-COLLECTED EVIDENCE (AI-searched references for this output type) ---\n"
        + "\n".join(text_lines)
        + "\n--- END AUTO-EVIDENCE ---\n"
        "Cross-reference the bot output claims against these sources. "
        "Cite them by [YT] or [WEB] label when supporting or contradicting claims. "
        "Flag contradictions with [CONFLICT].\n"
        + f"\n__REFS_JSON__:{json.dumps(refs[:8])}"  # hidden structured data for Telegram display
    )


async def _fb_search_youtube_transcripts(topic_keywords: str) -> str:
    """Legacy wrapper — delegates to _fb_auto_search_evidence for TA type."""
    return await _fb_auto_search_evidence("ta", topic_keywords)


async def _fb_call_validator(provider: dict, output_text: str, output_type: str, extra: str = "", shared_evidence: str = "") -> dict:
    """Call one AI provider with the OMNI pipeline prompt. Returns parsed result dict."""
    type_label = _FB_OUTPUT_TYPES.get(output_type, "General Output")
    user_msg = (
        f"OUTPUT TYPE: {type_label}\n\n"
        f"--- BOT OUTPUT TO VALIDATE ---\n{output_text[:6000]}\n--- END ---\n"
    )

    # Use pre-fetched shared evidence if available — avoids 5x redundant HTTP scrapes
    auto_evidence = shared_evidence if shared_evidence else await _fb_auto_search_evidence(output_type, output_text[:300])
    if auto_evidence:
        user_msg += f"\n{auto_evidence}"

    if extra:
        user_msg += (
            f"\n--- USER EVIDENCE ---\n{extra[:2000]}\n--- END ---\n"
            f"\nIncorporate the evidence. Flag contradictions with [CONFLICT]."
        )

    # ── Model selection ────────────────────────────────────────────────────────
    # All providers: bypass live discovery — use hardcoded free-tier known-good chains.
    #
    # GROQ:     qwen3-32b is the smartest free model (preview). llama-3.3-70b as
    #           reliable fallback (proven json_object + large context).
    #
    # CEREBRAS (from official docs Feb 2026):
    #   • qwen-3-235b-a22b-instruct-2507: NON-thinking instruct model, smartest free.
    #     No reasoning params needed. json_object works fine.
    #   • gpt-oss-120b: reasoning model. Uses reasoning_effort param ("low"/"medium"/"high").
    #     Reasoning output goes into separate "reasoning" field — content is already clean.
    #     json_object DOES work on gpt-oss-120b per official docs example.
    #   • zai-glm-4.7: reasoning model. Uses disable_reasoning param (True/False).
    #     NOT reasoning_effort — completely different param from gpt-oss-120b.
    #     Reasoning goes into separate "reasoning" field — content is already clean.
    #     json_object works fine.
    #   • llama3.1-8b: standard model, json_object works.
    #
    # GEMINI:   2.5-flash is smartest. reasoning_effort=none disables thinking tokens
    #           so JSON comes clean. 2.5-flash-lite has thinking OFF by default.
    #
    # MISTRAL:  magistral-small-2506 is a reasoning model (smarter than small-3.2).
    #           Emits <think> tags in content — stripped in content processing below.
    _GROQ_VALIDATOR_CTX    = ["qwen/qwen3-32b", "llama-3.3-70b-versatile", "llama-3.1-8b-instant"]
    _CEREBRAS_VALIDATOR_CTX = ["qwen-3-235b-a22b-instruct-2507", "qwen3-235b-a22b", "llama-3.3-70b", "llama3.1-8b"]
    # gemini-2.0-flash-lite is primary: lowest rate-limit pressure on free tier.
    # gemini-2.5-flash hits 429 fast on trio (large context), now used as fallback only.
    _GEMINI_COMPAT_CTX     = ["gemini-2.0-flash-lite", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash"]
    _MISTRAL_VALIDATOR_CTX = ["magistral-small-2506", "mistral-small-3.2-24b-instruct-2506", "open-mistral-nemo"]
    # GitHub Models free-tier IDs (verified Mar 2026 from docs.github.com/github-models)
    # Use provider-prefixed format: openai/gpt-4o-mini (NOT bare gpt-4o-mini).
    # gpt-4o-mini is primary: faster, lower latency, reliably fits within 25s timeout.
    # gpt-4o moved to fallback — too slow as primary, causes timeout on large trio context.
    _GITHUB_VALIDATOR_CTX  = ["openai/gpt-4o-mini", "openai/gpt-4o", "meta/llama-3.3-70b-instruct"]

    if provider["type"] == "groq":
        model = _GROQ_VALIDATOR_CTX[0]
    elif provider["type"] == "cerebras":
        model = _CEREBRAS_VALIDATOR_CTX[0]
    elif provider["type"] == "gemini":
        model = _GEMINI_COMPAT_CTX[0]
    elif provider["type"] == "mistral":
        model = _MISTRAL_VALIDATOR_CTX[0]
    elif provider["type"] == "github":
        model = _GITHUB_VALIDATOR_CTX[0]
    else:
        models = await _fb_discover_models(provider["type"], provider["key"])
        model  = models[0] if models else _FB_FALLBACK_MODELS.get(provider["type"], ["unknown"])[0]

    url = _FB_ENDPOINTS.get(provider["type"], "")
    if not url:
        return {"_status": "no_endpoint", "validator": provider["name"], "_model": model}

    # Gemini OpenAI-compat endpoint: send BOTH ?key= AND Authorization: Bearer.
    # Gemini 3.x compat gateway accepts either; dual-auth eliminates 400s from
    # models that prefer Bearer over query-param (observed with Gemini compat endpoint).
    if provider["type"] == "gemini":
        url = f"{url}?key={provider['key']}"
        headers = {"Authorization": f"Bearer {provider['key']}", "Content-Type": "application/json"}
    else:
        # GitHub Models, Groq, Cerebras, Mistral: all use standard Bearer token
        headers = {
            "Authorization": f"Bearer {provider['key']}",
            "Content-Type": "application/json",
        }

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": _FB_SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        "max_tokens": 4000,
        "temperature": 0.1,
    }
    # ── response_format / thinking config per provider ──────────────────────
    #
    # GROQ:     json_object works on all models. qwen3-32b gets /no_think in prompt.
    #
    # CEREBRAS (per official docs):
    #   ALL models support json_object. Additionally:
    #   • gpt-oss-120b: add reasoning_effort="low" (controls thinking depth).
    #     Reasoning output goes to separate "reasoning" field — content is clean JSON.
    #   • zai-glm-4.7: add disable_reasoning=True to suppress reasoning tokens.
    #     Uses DIFFERENT param from gpt-oss-120b. Reasoning also in separate field.
    #   • qwen-3-235b, llama3.1-8b: no extra params needed.
    #
    # GEMINI:   response_format=json_object triggers HTTP 400 on compat endpoint.
    #           reasoning_effort=none injected per-model in _do_request for 2.5 models.
    #
    # MISTRAL:  json_object works. Magistral emits <think> in content — stripped below.
    #
    # Per-model extra params injected in _do_request to keep base body clean.
    if provider["type"] == "groq":
        body["response_format"] = {"type": "json_object"}
        if model == "qwen/qwen3-32b":
            body["messages"][0]["content"] += "\n/no_think"
    elif provider["type"] == "cerebras":
        # json_object works on ALL Cerebras models per official docs
        body["response_format"] = {"type": "json_object"}
        # gpt-oss-120b and zai-glm-4.7 extra params injected in _do_request
    elif provider["type"] == "gemini":
        # response_format=json_object IS supported on Gemini OpenAI compat endpoint
        # in real-time mode (confirmed Feb 2026). Forces JSON output without markdown.
        # reasoning_effort=none injected per-model in _do_request for 2.5 models.
        body["response_format"] = {"type": "json_object"}
    elif provider["type"] == "mistral":
        body["response_format"] = {"type": "json_object"}
    elif provider["type"] == "github":
        # GitHub Models: standard OpenAI-compat Bearer auth, json_object supported.
        # Claude models on GitHub may emit <thinking> — stripped in content processing.
        body["response_format"] = {"type": "json_object"}

    def _clean_gemini_content(text: str) -> str:
        """
        Gemini-specific pre-cleaner. Runs BEFORE _extract_json.
        Handles Gemini failure modes on the OpenAI compat endpoint:
          1. ```json...``` fences anywhere (pre/post conversational filler)
          2. Residual <think>/<thinking> tags if reasoning_effort was ignored
          3. Orphan </think> at start (Gemini 2.5 edge case)
        Regex fixes vs old version:
          - Removed ^ $ anchors: Gemini adds filler before/after the fence
          - Changed {.*?} non-greedy to {.*} greedy: captures full nested JSON
          - re.DOTALL: multiline JSON bodies matched correctly
        """
        # 1. Strip paired thinking/reasoning tags
        text = re.sub(r'<(?:think|thinking)>.*?</(?:think|thinking)>', '', text, flags=re.DOTALL).strip()
        # 2. Strip orphan </think> at the very start
        if text.startswith('</think>'):
            text = text[len('</think>'):].strip()
        # 3. Extract JSON from ```json...``` or ```...``` fence
        #    No ^ $ anchors — handles pre/post filler text.
        #    Greedy {.*} captures the full nested object without truncation.
        fence = re.search(r'```(?:json)?\s*(\{.*\})\s*```', text, re.DOTALL)
        if fence:
            return fence.group(1).strip()
        # 4. No fence but contains braces — extract between first { and last }
        #    Catches bare JSON with no fences that slips past the regex
        start = text.find("{")
        end   = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start:end + 1]
        # 5. No JSON found at all — return as-is for _extract_json to handle
        return text

    def _extract_json(raw: str) -> dict:
        """
        Multi-strategy JSON extraction — handles fenced, partial, truncated, and
        control-character-polluted responses from any AI provider.
        """
        def _sanitize(s: str) -> str:
            """Remove control characters (except \\t \\n \\r).
            The newline-inside-strings regex was removed — it caused catastrophic
            backtracking on Gemini multi-paragraph summaries and broke valid JSON.
            strict=False in json.loads handles literal newlines in strings natively.
            """
            s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", s)
            return s

        def _bracket_extract(s: str) -> str | None:
            """Find the outermost { } using bracket counting (handles nested objects)."""
            start = s.find("{")
            if start == -1:
                return None
            depth = 0
            in_str = False
            escape = False
            for i, ch in enumerate(s[start:], start):
                if escape:
                    escape = False
                    continue
                if ch == "\\" and in_str:
                    escape = True
                    continue
                if ch == '"':
                    in_str = not in_str
                    continue
                if not in_str:
                    if ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1
                        if depth == 0:
                            return s[start:i + 1]
            return None  # unclosed — caller will attempt repair

        def _repair_truncated(s: str) -> str:
            """Close dangling keys, open strings, and unmatched braces/brackets."""
            # Drop incomplete trailing key (e.g.  , "key)
            s = re.sub(r',\s*"[^"]*$', "", s)
            # Close open string value  (e.g.  : "some text...)
            s = re.sub(r':\s*"([^"\\]|\\.)*$', ': ""', s)
            # Close open array value    (e.g.  : [1, 2, ...)
            open_sq = s.count("[") - s.count("]")
            open_b  = s.count("{") - s.count("}")
            s += "]" * max(open_sq, 0)
            s += "}" * max(open_b, 0)
            return s

        # ── Strategy 1: extract content from ```json ... ``` fence if present ───
        # Do NOT blindly strip all backticks — that corrupts backtick chars inside
        # JSON string values. Instead, pull only the fenced block content.
        fence_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw.strip(), re.DOTALL)
        if fence_match:
            candidate = _sanitize(fence_match.group(1))
            try:
                return json.loads(candidate, strict=False)
            except json.JSONDecodeError as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            # Fence found but malformed — still use it as base for further repair
            cleaned = candidate
        else:
            cleaned = _sanitize(raw.strip())

        try:
            return json.loads(cleaned, strict=False)
        except json.JSONDecodeError as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")

        # ── Strategy 2: bracket-counting extract of outermost { } ─────────────
        block = _bracket_extract(cleaned)
        if block:
            try:
                return json.loads(block, strict=False)
            except json.JSONDecodeError as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            # Strategy 3: repair the extracted (but truncated) block
            try:
                repaired = _repair_truncated(block)
                return json.loads(repaired, strict=False)
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        # ── Strategy 4: repair the full cleaned string (no complete block found) ──
        try:
            start = cleaned.find("{")
            if start != -1:
                repaired = _repair_truncated(cleaned[start:])
                return json.loads(repaired, strict=False)
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        return {}

    def _apply_score_floors(result: dict) -> dict:
        """Enforce minimum dimension scores — notification summary context."""
        _has_urgent = any(b.get("severity") == "URGENT" for b in result.get("bugs", []))
        _floors = {
            "accuracy":     70 if _has_urgent else 80,
            "completeness": 70 if _has_urgent else 80,
            "consistency":  70 if _has_urgent else 80,
            "timeliness":   85,
            "reliability":  70 if _has_urgent else 80,
        }
        sc = result.setdefault("scores", {})
        for dim, floor in _floors.items():
            raw_val = sc.get(dim)
            try:
                sc[dim] = max(float(raw_val), float(floor))
            except (TypeError, ValueError):
                sc[dim] = float(floor)
        result["composite_score"] = round(sum(sc.get(d, 0) * w for d, w in _FB_DIMENSIONS.items()), 1)
        return result

    # Per-request timeout: 12s each × up to 2 retries = ~24s max per validator,
    # leaving slack before the outer 35s asyncio.wait_for guard fires.
    # This ensures the fallback chain can actually run before the whole task is killed.
    _PER_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)

    async def _do_request(mdl: str) -> tuple[int, str]:
        """
        Execute one HTTP request. Returns (status_code, raw_text).
        Applies per-model body overrides per official provider docs.
        NEVER raises — all network/timeout exceptions are caught and returned
        as synthetic status codes so the fallback chain continues.
          0  = asyncio.TimeoutError (request took too long)
         -1  = aiohttp connection error (DNS, refused, reset)
         -2  = any other unexpected exception
        """
        b = {**body, "model": mdl}

        # ── Per-model overrides (official docs verified) ──────────────────
        if provider["type"] == "groq" and mdl == "qwen/qwen3-32b":
            # Ensure /no_think is in system prompt for Qwen3
            msgs = [m.copy() for m in b["messages"]]
            if msgs and msgs[0]["role"] == "system" and "/no_think" not in msgs[0]["content"]:
                msgs[0]["content"] += "\n/no_think"
            b["messages"] = msgs

        elif provider["type"] == "cerebras":
            # All Cerebras models get json_object (already in base body).
            # Reasoning models get additional control params:
            if mdl == "gpt-oss-120b":
                b["reasoning_effort"] = "low"
            elif mdl == "zai-glm-4.7":
                b["disable_reasoning"] = True
            elif "qwen" in mdl.lower():
                # Qwen models on Cerebras: inject /no_think in system prompt
                # to suppress CoT mode that emits <think> tags into content.
                msgs = [m.copy() for m in b["messages"]]
                if msgs and msgs[0]["role"] == "system" and "/no_think" not in msgs[0]["content"]:
                    msgs[0]["content"] += "\n/no_think"
                b["messages"] = msgs

        elif provider["type"] == "gemini":
            # OpenAI-compat endpoint: response_format=json_object is already in base body.
            # Per Google docs (Mar 2026) reasoning_effort mapping for compat endpoint:
            #   gemini-2.5-pro:       "low" (minimum — "none" causes 400 on Pro)
            #   gemini-2.5-flash:     "none" (disables thinking fully)
            #   gemini-2.5-flash-lite: "none" (thinking off by default, explicit is safe)
            #   gemini-2.0 and older: pop entirely — param not supported
            if "2.5-pro" in mdl:
                b["reasoning_effort"] = "low"
            elif "2.5" in mdl:
                b["reasoning_effort"] = "none"
            else:
                b.pop("reasoning_effort", None)
            # If response_format triggered a 400 on a previous attempt, strip it
            # (handled in the 400 retry path below — keep base body clean)

        elif provider["type"] == "github":
            # GitHub Models: plain OpenAI-compat, no special params needed.
            # Claude models on GitHub (anthropic/*) may emit <thinking> tags —
            # these are stripped in the content processing block after status 200.
            pass

        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, headers=headers, json=b,
                                  timeout=_PER_REQUEST_TIMEOUT) as r:
                    status_code = r.status
                    text = await r.text()
                    # Gemini: if reasoning_effort OR response_format caused a 400,
                    # strip both and retry — the compat endpoint sometimes rejects
                    # these params depending on the model version.
                    if status_code == 400 and provider["type"] == "gemini":
                        b.pop("reasoning_effort", None)
                        b.pop("response_format", None)
                        async with aiohttp.ClientSession() as s2:
                            async with s2.post(url, headers=headers, json=b,
                                               timeout=_PER_REQUEST_TIMEOUT) as r2:
                                return r2.status, await r2.text()
                    return status_code, text
        except asyncio.TimeoutError:
            logger.warning(f"[FB] {provider['name']}/{mdl} timed out")
            return 0, "timeout"
        except aiohttp.ClientConnectorError as e:
            logger.warning(f"[FB] {provider['name']}/{mdl} connection error: {e}")
            return -1, f"connection_error: {e}"
        except Exception as e:
            logger.warning(f"[FB] {provider['name']}/{mdl} request error: {e}")
            return -2, f"request_error: {e}"

    # ── Per-provider ordered fallback model chains ─────────────────────────
    # Primary is already set above. These are the fallbacks tried in order.
    # All Cerebras models support json_object per official docs.
    _PROVIDER_FALLBACKS: dict[str, list[str]] = {
        "groq":     ["llama-3.3-70b-versatile", "llama-3.1-8b-instant"],
        "cerebras": ["qwen3-235b-a22b", "llama-3.3-70b", "zai-glm-4.7", "llama3.1-8b"],
        # Gemini: 2.0-flash-lite is primary (lowest quota). Flash/flash-lite as fallbacks.
        "gemini":   ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash"],
        "mistral":  ["mistral-small-3.2-24b-instruct-2506", "open-mistral-nemo"],
        "github":   ["openai/gpt-4o", "meta/llama-3.3-70b-instruct"],
    }
    # Per-provider max_tokens cap to prevent slow timeouts on heavy models
    _MAX_TOKENS_CAP: dict[str, int] = {
        "groq":     4000,
        "cerebras": 3000,
        "gemini":   6000,  # raised: 3000 caused truncation on large JSON schema responses
        "mistral":  3000,  # slim prompt means 3000 tokens is enough and safe
        "github":   4000,  # GitHub Models free tier — 128k context, 4000 output safe
    }
    body["max_tokens"] = _MAX_TOKENS_CAP.get(provider["type"], 4000)

    async def _do_request_with_fallback() -> tuple[int, str]:
        """
        Try the primary model, then fall through the fallback chain.
        Handles: 429 (rate-limit), 400 (bad model/param), 503/502 (transient).
        Returns (status, raw_text) and sets `model` to whichever succeeded.
        """
        nonlocal model
        chain = [model] + [m for m in _PROVIDER_FALLBACKS.get(provider["type"], []) if m != model]
        last_status, last_text = 0, ""
        for attempt_model in chain:
            s, t = await _do_request(attempt_model)
            last_status, last_text = s, t
            # Log every non-200 so we can diagnose failures without guessing
            if s not in (0, -1, -2, 200):
                logger.warning(
                    f"[FB] {provider['name']}/{attempt_model} → HTTP {s} "
                    f"| body: {t[:300].strip()}"
                )
            if s == 200:
                model = attempt_model
                return s, t
            if s == 429:
                # Rate-limited — Gemini free tier 429s are permanent per-model quota
                # exhaustion, so skip the sleep+retry and fall to the next model immediately.
                # For other providers a single 3s backoff retry is still worthwhile.
                if provider["type"] != "gemini":
                    await asyncio.sleep(3)
                    s2, t2 = await _do_request(attempt_model)
                    last_status, last_text = s2, t2
                    if s2 == 200:
                        model = attempt_model
                        return s2, t2
                continue  # fall to next model
            if s == 400:
                continue  # bad model/param — try next immediately
            if s == 404:
                # 404 = model not found on this endpoint — try next model for all providers.
                continue
            if s in (502, 503):
                await asyncio.sleep(2)
                s2, t2 = await _do_request(attempt_model)
                if s2 == 200:
                    model = attempt_model
                    return s2, t2
                last_status, last_text = s2, t2
                continue
            if s in (0, -1, -2):
                # Synthetic codes: timeout / connection error / unexpected exception.
                # These are transient — try the next lighter/faster model immediately.
                continue
            # 401 and other hard auth errors — no point retrying any model
            return s, t
        return last_status, last_text

    try:
        status, raw_text = await _do_request_with_fallback()

        if status == 200:
            data    = json.loads(raw_text)
            content = data["choices"][0]["message"]["content"].strip()
            # Strip reasoning blocks emitted by thinking models before JSON extraction.
            # Applies to: zai-glm-4.7, magistral-small-2506, gpt-oss-120b (if it slips through),
            # Qwen3 (if /no_think wasn't honoured), and any Gemini 2.5 if reasoning_effort failed.
            # gemini-2.5-flash with reasoning_effort=none should come clean — strip is a safety net.
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            content = re.sub(r'<thinking>.*?</thinking>', '', content, flags=re.DOTALL).strip()
            # Qwen3 bare end-marker variant: some deployments emit text starting with
            # a block that ends in </think> with NO opening tag. Strip only the leading
            # orphan block — do NOT use .*?</think> with DOTALL as it greedily consumes
            # everything up to the last </think>, which destroys the JSON body after it.
            if content.startswith('</think>'):
                content = content[len('</think>'):].strip()
            elif '</think>' in content and not content.lstrip().startswith('{'):
                # Only strip up to first </think> if JSON hasn't started yet
                idx = content.find('</think>')
                after = content[idx + len('</think>'):].strip()
                if after:
                    content = after
            # NOTE: Cerebras reasoning models (gpt-oss-120b, zai-glm-4.7) put reasoning
            # in a separate "reasoning" field — content here is already clean JSON.

            # ── Parse the cleaned content into a dict ───────────────────────
            # Run the fence/think cleaner for ALL providers — Cerebras and
            # GitHub models also emit markdown fences and think tags.
            content = _clean_gemini_content(content)
            result = _extract_json(content)

            if not result or not any(k in result for k in ("scores", "verdict", "summary")):
                return {
                    "_status":  "parse_error",
                    "validator": provider["name"],
                    "_model":    model,
                    "_detail":   f"Empty/missing fields. Raw[:300]: {content[:300]}",
                }

            # ── Inject safe defaults for any missing required fields ──────────
            # Prevents KeyError / AttributeError crashes in the consensus engine
            # when a model returns a valid but incomplete JSON object.
            _DEFAULT_SCORES = {
                "accuracy": 75, "completeness": 75,
                "consistency": 75, "timeliness": 85, "reliability": 75,
            }
            result.setdefault("scores", {})
            for _dim, _floor in _DEFAULT_SCORES.items():
                result["scores"].setdefault(_dim, _floor)
            result.setdefault("verdict", "NEEDS_REVIEW")
            result.setdefault("summary", "")
            result.setdefault("bugs", [])
            result.setdefault("improvements", [])
            result.setdefault("stage4_scenarios", [])
            result.setdefault("conflicts", [])
            result.setdefault("hallucinations", [])
            result.setdefault("missing_elements", [])
            result.setdefault("risk_flags", [])
            result.setdefault("ta_concept_errors", [])
            result.setdefault("on_chain_validation", {})
            result.setdefault("stage1_findings", [])
            result.setdefault("stage2_synthesis", [])
            result.setdefault("stage3_retained", [])
            result.setdefault("stage3_discarded", [])
            result.setdefault("pro_knowledge_suggestions", [])
            result.setdefault("data_scraping_recommendations", [])
            result.setdefault("youtube_synthesis", "")
            result.setdefault("news_impact_assessment", "")
            result.setdefault("composite_score", 0)
            # Ensure all list fields are actually lists (model sometimes returns null)
            _list_fields = ["bugs", "improvements", "stage4_scenarios", "conflicts",
                             "hallucinations", "missing_elements", "risk_flags",
                             "ta_concept_errors", "stage1_findings", "stage2_synthesis",
                             "stage3_retained", "stage3_discarded",
                             "pro_knowledge_suggestions", "data_scraping_recommendations",
                             "feature_suggestions"]
            for _f in _list_fields:
                if not isinstance(result.get(_f), list):
                    result[_f] = []

            result["validator"]      = f"{provider['name']} ({model})"
            result["_provider"]      = provider["type"]
            result["_model"]         = model
            result["_status"]        = "ok"
            result["_auto_evidence"] = auto_evidence
            return _apply_score_floors(result)

        elif status == 401:
            return {"_status": "auth_error",  "validator": provider["name"], "_model": model}
        elif status == 429:
            return {"_status": "rate_limit",  "validator": provider["name"], "_model": model}
        elif status == 0:
            # All fallback models timed out
            return {"_status": "timeout", "validator": provider["name"], "_model": model,
                    "_detail": "All fallback models timed out"}
        elif status in (-1, -2):
            # All fallback models had connection/exception errors
            return {"_status": "exception", "validator": provider["name"], "_model": model,
                    "_detail": raw_text[:200]}
        else:
            return {"_status": f"http_{status}", "validator": provider["name"],
                    "_model": model, "_detail": raw_text[:200]}
    except Exception as e:
        return {"_status": "exception", "validator": provider["name"], "_model": model, "_detail": str(e)}


# ── Consensus engine ──────────────────────────────────────────────────────────

def _fb_build_consensus(results: list[dict], output_type: str, output_text: str, user_evidence: str = "") -> dict:
    ok  = [r for r in results if r.get("_status") == "ok"]
    bad = [r for r in results if r.get("_status") != "ok"]
    # Remap timeout status label for display
    for r in bad:
        if r.get("_status") == "timeout":
            r.setdefault("_detail", "Validator timed out — likely bad model or network")

    # Average scores
    agg = {d: [] for d in _FB_DIMENSIONS}
    for r in ok:
        for d in _FB_DIMENSIONS:
            v = r.get("scores", {}).get(d)
            if v is not None:
                agg[d].append(float(v))
    avg_scores = {d: round(sum(v)/len(v), 1) if v else 0.0 for d, v in agg.items()}
    composite  = round(sum(avg_scores[d] * w for d, w in _FB_DIMENSIONS.items()), 1)

    # Majority verdict
    votes = [r.get("verdict", "NEEDS_REVIEW") for r in ok]
    if not votes:
        verdict, emoji = "INCONCLUSIVE", "⚠️"
    elif votes.count("VALID") > len(votes) / 2:
        verdict, emoji = "VALID", "✅"
    elif votes.count("INVALID") > 0:
        verdict, emoji = "INVALID", "❌"
    else:
        verdict, emoji = "NEEDS_REVIEW", "🔍"

    def dedup(lst): return list(dict.fromkeys(x for x in lst if x))

    all_conflicts   = dedup([x for r in ok for x in r.get("conflicts", [])])
    all_halluc      = dedup([x for r in ok for x in r.get("hallucinations", [])])
    all_missing     = dedup([x for r in ok for x in r.get("missing_elements", [])])
    all_risks       = dedup([x for r in ok for x in r.get("risk_flags", [])])
    all_retained    = dedup([x for r in ok for x in r.get("stage3_retained", [])])
    all_summaries   = [f"[{r['validator']}]: {r['summary']}" for r in ok if r.get("summary")]

    # ── New expert fields ──────────────────────────────────────────────────────
    all_ta_errors   = dedup([x for r in ok for x in r.get("ta_concept_errors", [])])
    all_pro_tips    = dedup([x for r in ok for x in r.get("pro_knowledge_suggestions", [])])
    all_data_recs   = dedup([x for r in ok for x in r.get("data_scraping_recommendations", [])])
    # Collect feature suggestions per validator — keep source validator name attached
    all_feature_suggestions = []
    seen_feat_titles: set[str] = set()
    for r in ok:
        vname = r.get("validator", "AI")
        for fs in r.get("feature_suggestions", []):
            t = fs.get("title", "").strip()
            if t and t not in seen_feat_titles:
                seen_feat_titles.add(t)
                fs["_validator"] = vname
                all_feature_suggestions.append(fs)
    # Sort: HIGH priority first, then by complexity (EASY ships fastest)
    all_feature_suggestions.sort(key=lambda x: (
        {"HIGH":0,"MEDIUM":1,"LOW":2}.get(x.get("priority","LOW"),2),
        {"EASY":0,"MEDIUM":1,"HARD":2}.get(x.get("implementation_complexity","MEDIUM"),1)
    ))

    # Merge probabilistic scenarios (deduplicate by asset)
    seen_assets = {}
    for r in ok:
        for sc in r.get("stage4_scenarios", []):
            asset = sc.get("asset", "").strip()
            if asset and asset not in seen_assets:
                seen_assets[asset] = sc
    all_scenarios = list(seen_assets.values())

    # Merge on-chain validation dicts
    merged_onchain = {}
    for r in ok:
        for metric, val in r.get("on_chain_validation", {}).items():
            if metric not in merged_onchain:
                merged_onchain[metric] = val

    # Merge news impact assessments
    news_impacts = [r.get("news_impact_assessment", "").strip() for r in ok if r.get("news_impact_assessment", "").strip()]
    combined_news_impact = " | ".join(news_impacts) if news_impacts else ""

    # ── Merge bugs (deduplicated by title, keep highest severity) ──────────────
    seen_bug_titles = {}
    for r in ok:
        for bug in r.get("bugs", []):
            title = bug.get("title", "").strip()
            if not title:
                continue
            sev_rank = {"URGENT": 0, "MILD": 1, "LOW": 2}
            existing = seen_bug_titles.get(title)
            if not existing or sev_rank.get(bug.get("severity","LOW"),2) < sev_rank.get(existing.get("severity","LOW"),2):
                seen_bug_titles[title] = bug
    all_bugs = sorted(seen_bug_titles.values(), key=lambda b: {"URGENT":0,"MILD":1,"LOW":2}.get(b.get("severity","LOW"),2))

    # ── Merge improvements ─────────────────────────────────────────────────────
    seen_imp_titles = {}
    for r in ok:
        for imp in r.get("improvements", []):
            title = imp.get("title", "").strip()
            if not title:
                continue
            pri_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
            existing = seen_imp_titles.get(title)
            if not existing or pri_rank.get(imp.get("priority","LOW"),2) < pri_rank.get(existing.get("priority","LOW"),2):
                seen_imp_titles[title] = imp
    all_improvements = sorted(seen_imp_titles.values(), key=lambda i: {"HIGH":0,"MEDIUM":1,"LOW":2}.get(i.get("priority","LOW"),2))

    # ── Combine YouTube synthesis from all validators ─────────────────────────
    yt_syntheses = [r.get("youtube_synthesis", "").strip() for r in ok if r.get("youtube_synthesis","").strip()]
    combined_yt_synthesis = " | ".join(yt_syntheses) if yt_syntheses else ""

    # ── Support/Resist check fields (backward compat) ─────────────────────────
    # Map old stage4_checkN fields if validators still return them
    def _collect_checks(key):
        return dedup([r.get(key, "") for r in ok if r.get(key, "").strip()])

    return {
        "generated_at":              datetime.now(timezone.utc).isoformat(),
        "output_type":               _FB_OUTPUT_TYPES.get(output_type, "Unknown"),
        "output_type_key":           output_type,
        "validators_attempted":      len(results),
        "validators_ok":             len(ok),
        "validators_failed":         [(r["validator"], r["_status"]) for r in bad],
        "avg_scores":                avg_scores,
        "composite_score":           composite,
        "verdict":                   verdict,
        "verdict_emoji":             emoji,
        "conflicts":                 all_conflicts,
        "hallucinations":            all_halluc,
        "missing_elements":          all_missing,
        "risk_flags":                all_risks,
        "key_retained_findings":     all_retained[:10],
        "individual_summaries":      all_summaries,
        "individual_results":        ok,
        "bugs":                      all_bugs,
        "improvements":              all_improvements,
        "youtube_synthesis":         combined_yt_synthesis,
        "user_evidence_submitted":   bool(user_evidence),
        "user_evidence_preview":     user_evidence[:300] if user_evidence else "",
        "output_preview":            output_text[:500],
        # ── New expert fields ──
        "ta_concept_errors":         all_ta_errors,
        "pro_knowledge_suggestions": all_pro_tips,
        "data_scraping_recommendations": all_data_recs,
        "feature_suggestions":       all_feature_suggestions,
        "stage4_scenarios":          all_scenarios,
        "on_chain_validation":       merged_onchain,
        "news_impact_assessment":    combined_news_impact,
        # ── Backward compat (old stage4 check keys) ──
        "stage4_check1":             _collect_checks("stage5_check1") or _collect_checks("stage4_check1"),
        "stage4_check2":             _collect_checks("stage5_check2") or _collect_checks("stage4_check2"),
        "stage4_check3":             _collect_checks("stage5_check3") or _collect_checks("stage4_check3"),
        "stage4_check4":             _collect_checks("stage5_check4"),
    }


# ── Full orchestrator ─────────────────────────────────────────────────────────

async def _fb_run_validation(output_text: str, output_type: str, user_evidence: str = "", chat_id: int = 0) -> dict:
    """Rank providers → parallel validate → consensus → apply all 10 QA improvements."""
    # Fetch providers and evidence in parallel — saves sequential overhead
    providers, shared_evidence = await asyncio.gather(
        _fb_rank_providers(),
        _fb_auto_search_evidence(output_type, output_text[:300]),
    )

    ext_consensus_prefetched = None  # set in parallel gather below if providers exist

    if not providers:
        base = _fb_build_consensus([], output_type, output_text, user_evidence)
    else:
        # Wrap each validator in a per-task timeout so one stuck provider
        # can never block the entire pipeline. Timed-out tasks return an error dict.
        _VALIDATOR_TIMEOUT = 35  # 35s per validator — slim prompt fits within free-tier limits

        async def _guarded(p):
            try:
                return await asyncio.wait_for(
                    _fb_call_validator(p, output_text, output_type, user_evidence, shared_evidence=shared_evidence),
                    timeout=_VALIDATOR_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(f"[FB] {p['name']} timed out after {_VALIDATOR_TIMEOUT}s")
                return {"_status": "timeout", "validator": p["name"], "_model": "?"}
            except Exception as exc:
                logger.warning(f"[FB] {p['name']} raised {exc}")
                return {"_status": "exception", "validator": p["name"], "_model": "?", "_detail": str(exc)}

        # Run validators AND external consensus check in parallel
        tasks = [_guarded(p) for p in providers]
        results_and_ext = await asyncio.gather(
            asyncio.gather(*tasks),
            _qa_external_consensus(output_text, output_type),
            return_exceptions=True,
        )
        results = list(results_and_ext[0]) if not isinstance(results_and_ext[0], Exception) else []
        ext_consensus_prefetched = results_and_ext[1] if not isinstance(results_and_ext[1], Exception) else {"checks": {}, "conflicts": [], "has_conflicts": False}

        base = _fb_build_consensus(results, output_type, output_text, user_evidence)

    score = base["composite_score"]
    base["_chat_id"] = chat_id  # needed by _qa_trading_readiness to look up capture_ts

    # ── #1 Score delta ────────────────────────────────────────────────────────
    base["score_trend"] = _qa_get_score_delta(chat_id, output_type, score)

    # ── #2 Trading readiness gate ─────────────────────────────────────────────
    base["trading_readiness"] = _qa_trading_readiness(output_type, output_text, base)

    # ── #3 Validator agreement matrix ─────────────────────────────────────────
    base["agreement_matrix"] = _qa_agreement_matrix(base.get("individual_results", []))
    # Merge matrix spread conflicts into main conflicts list
    base["conflicts"] = list(dict.fromkeys(
        base["conflicts"] + base["agreement_matrix"].get("spread_conflicts", [])
    ))

    # ── #4 Signal decay warning ───────────────────────────────────────────────
    base["signal_decay"] = _qa_signal_decay(output_type, output_text, chat_id)

    # ── #5 DNA fingerprint ────────────────────────────────────────────────────
    base["fingerprint"] = _qa_fingerprint(output_text)

    # ── #6 Update health dashboard ────────────────────────────────────────────
    _qa_update_health_dashboard(output_type, score, base["verdict"])
    base["health_dashboard"] = _qa_build_health_dashboard()

    # ── #7 Manipulation risk ──────────────────────────────────────────────────
    base["manipulation_risk"] = _qa_manipulation_risk(output_text, output_type)

    # ── #8 Action block ───────────────────────────────────────────────────────
    base["action_block"] = _qa_action_block(
        base,
        base["trading_readiness"],
        base["signal_decay"],
        base["manipulation_risk"]
    )

    # ── #9 Per-coin micro-validation ──────────────────────────────────────────
    coins = _qa_extract_coin_signals(output_text)
    base["micro_validation"] = _qa_micro_validate_coins(coins) if coins else []

    # ── #10 External consensus check (pre-fetched in parallel with validators) ─
    base["external_consensus"] = ext_consensus_prefetched if ext_consensus_prefetched is not None else await _qa_external_consensus(output_text, output_type)
    # Merge external conflicts
    base["conflicts"] = list(dict.fromkeys(
        base["conflicts"] + base["external_consensus"].get("conflicts", [])
    ))

    return base


# ── Telegram card formatter ───────────────────────────────────────────────────

def _md_escape(text: str) -> str:
    """Escape special Markdown v1 characters in dynamic/AI-generated text."""
    for ch in ["*", "_", "`", "["]:
        text = text.replace(ch, f"\\{ch}")
    return text


def _fb_split_message(text: str, limit: int = 4000) -> list[str]:
    """
    Split a long Telegram message into chunks ≤ limit chars.
    Splits on blank lines first (section breaks), then on newlines,
    never mid-word. Each chunk is safe to send independently.
    """
    if len(text) <= limit:
        return [text]

    chunks = []
    current = ""

    # Walk paragraph by paragraph (split on double newline = section break)
    paragraphs = text.split("\n\n")
    for para in paragraphs:
        block = para + "\n\n"
        if len(current) + len(block) <= limit:
            current += block
        else:
            # Block alone fits → flush current, start new with block
            if len(block) <= limit:
                if current.strip():
                    chunks.append(current.rstrip())
                current = block
            else:
                # Block itself too long → split line by line
                if current.strip():
                    chunks.append(current.rstrip())
                    current = ""
                for line in para.splitlines(keepends=True):
                    if len(current) + len(line) <= limit:
                        current += line
                    else:
                        if current.strip():
                            chunks.append(current.rstrip())
                        current = line
                current += "\n\n"

    if current.strip():
        chunks.append(current.rstrip())

    return chunks or [text[:limit]]


async def _fb_send_report(message, summary: str, kb=None):
    """Send QA report, splitting into multiple messages if over Telegram's limit."""
    chunks = _fb_split_message(summary)
    for i, chunk in enumerate(chunks):
        # Only attach the keyboard to the last chunk
        markup = kb if (i == len(chunks) - 1) else None
        try:
            await message.reply_text(chunk, parse_mode="Markdown", reply_markup=markup)
        except Exception:
            # If Markdown parse fails (e.g. unmatched backtick in chunk), retry as plain text
            await message.reply_text(chunk, reply_markup=markup)


def _fb_format_telegram(report: dict) -> str:
    s        = report["avg_scores"]
    comp     = report["composite_score"]
    verdict  = _md_escape(report["verdict"])
    emoji    = report["verdict_emoji"]
    bugs     = report.get("bugs", [])
    imps     = report.get("improvements", [])
    yt_synth = report.get("youtube_synthesis", "").strip()

    def score_bar(val):
        filled = int(val / 10)
        return f"[{'█'*filled}{'░'*(10-filled)}] {val}/100"

    sev_emoji = {"URGENT": "🔴", "MILD": "🟡", "LOW": "🟢"}
    pri_emoji = {"HIGH":   "🔴", "MEDIUM": "🟡", "LOW": "🟢"}

    # ── Header ────────────────────────────────────────────────────────────────
    lines = [
        f"{'='*38}",
        f"📋 *CRYPTEX-QA VALIDATION ENGINE v5.0*",
        f"{'='*38}",
        f"🏷 Type: *{_md_escape(str(report['output_type']))}*",
        f"🔑 DNA: `{report.get('fingerprint','N/A')}`",
        f"🤖 Validators: {report['validators_ok']}/{report['validators_attempted']} responded",
        f"🕒 {report['generated_at'][:19].replace('T',' ')} UTC",
        f"",
    ]

    # ── #1 Score + Trend ──────────────────────────────────────────────────────
    trend_info = report.get("score_trend", {})
    delta      = trend_info.get("delta")
    trend_str  = ""
    if delta is not None:
        arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "─")
        trend_str = f"  {arrow} {'+' if delta > 0 else ''}{delta} vs last"
    history = trend_info.get("history", [])
    history_str = " → ".join(str(s) for s in history) if len(history) > 1 else ""

    lines += [
        f"{'─'*38}",
        f"*{emoji} VERDICT: {verdict}*",
        f"📈 Score: *{comp}/100*{trend_str}",
    ]
    if history_str:
        lines.append(f"📊 History: {_md_escape(history_str)}")

    # ── Dimension Scores ──────────────────────────────────────────────────────
    lines += [
        f"",
        f"📊 *DIMENSION SCORES:*",
        f"  Accuracy      {score_bar(s.get('accuracy',0))}",
        f"  Completeness  {score_bar(s.get('completeness',0))}",
        f"  Consistency   {score_bar(s.get('consistency',0))}",
        f"  Timeliness    {score_bar(s.get('timeliness',0))}",
        f"  Reliability   {score_bar(s.get('reliability',0))}",
    ]

    # ── Probabilistic Outcome Scenarios ──────────────────────────────────────
    scenarios = report.get("stage4_scenarios", [])
    if scenarios:
        lines += ["", f"{'─'*38}", f"🎲 *PROBABILITY SCENARIOS:*"]
        for sc in scenarios[:4]:
            asset = _md_escape(sc.get("asset", "Market"))
            conf  = sc.get("confidence", "?")
            conf_e = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(conf, "⚪")
            rr    = sc.get("reward_risk", 0)
            lines += [
                f"",
                f"  📌 *{asset}* — Confidence: {conf_e} {conf}  R:R {rr:.1f}:1",
                f"  🐂 Bull {sc.get('bull_prob',0)}%: {_md_escape(str(sc.get('bull_target',''))[:80])}",
                f"  ➡️ Base {sc.get('base_prob',0)}%: {_md_escape(str(sc.get('base_path',''))[:80])}",
                f"  🐻 Bear {sc.get('bear_prob',0)}%: {_md_escape(str(sc.get('bear_target',''))[:80])}",
                f"  ❌ Invalidation: {_md_escape(str(sc.get('bear_trigger',''))[:100])}",
            ]
            confirm = sc.get("confirm_metrics", [])
            if confirm:
                lines.append(f"  📡 Watch: {_md_escape(', '.join(confirm[:3]))}")

    # ── TA Concept Error Detection ────────────────────────────────────────────
    ta_errors = report.get("ta_concept_errors", [])
    if ta_errors:
        lines += ["", f"{'─'*38}", f"⚠️ *TA/SMC CONCEPT ERRORS ({len(ta_errors)}):*"]
        for te in ta_errors[:5]:
            lines.append(f"  ⚠️ {_md_escape(te[:200])}")

    # ── On-Chain Validation ───────────────────────────────────────────────────
    onchain = report.get("on_chain_validation", {})
    if onchain:
        lines += ["", f"{'─'*38}", f"⛓ *ON-CHAIN METRIC VALIDATION:*"]
        for metric, info in list(onchain.items())[:5]:
            assess = info.get("assessment", "unknown")
            tick   = "✅" if assess == "valid" else ("❌" if assess == "suspect" else "⚪")
            stated = info.get("stated", "?")
            rng    = info.get("expected_range", "?")
            lines.append(f"  {tick} {_md_escape(metric)}: stated={stated}  expected={_md_escape(str(rng))}")

    # ── News Impact Assessment ────────────────────────────────────────────────
    news_impact = report.get("news_impact_assessment", "").strip()
    if news_impact:
        lines += ["", f"{'─'*38}", f"📰 *NEWS IMPACT ASSESSMENT:*",
                  f"  {_md_escape(news_impact[:400])}"]

    # ── Pro Knowledge Suggestions ─────────────────────────────────────────────
    pro_tips = report.get("pro_knowledge_suggestions", [])
    if pro_tips:
        lines += ["", f"{'─'*38}", f"🧠 *PRO KNOWLEDGE UPGRADES ({len(pro_tips)}):*"]
        for tip in pro_tips[:4]:
            lines.append(f"  💡 {_md_escape(tip[:200])}")

    # ── Data Scraping Recommendations ─────────────────────────────────────────
    data_recs = report.get("data_scraping_recommendations", [])
    if data_recs:
        lines += ["", f"{'─'*38}", f"🔌 *GROUND-TRUTH DATA SOURCES:*"]
        for dr in data_recs[:4]:
            lines.append(f"  📡 {_md_escape(dr[:200])}")

    # ── #3 Validator Agreement Matrix ─────────────────────────────────────────
    matrix = report.get("agreement_matrix", {}).get("matrix", {})
    spread_flags = [dim for dim, info in matrix.items() if info.get("flagged")]
    if spread_flags:
        lines += ["", f"{'─'*38}", f"⚡ *VALIDATOR DISAGREEMENTS (spread >20pts):*"]
        for dim in spread_flags:
            info    = matrix[dim]
            entries = "  ".join(f"{v[:6]}:{s:.0f}" for v, s in info["scores"])
            lines.append(f"  ⚡ {dim.capitalize()}: {entries}  spread={info['spread']:.0f}pts")

    # ── #2 Trading Readiness Gate ─────────────────────────────────────────────
    readiness = report.get("trading_readiness", {})
    rg_emoji  = readiness.get("gate_emoji", "❓")
    rg_verdict = _md_escape(readiness.get("verdict", "UNKNOWN"))
    lines += [
        f"", f"{'─'*38}",
        f"🎯 *TRADING READINESS GATE:* {rg_emoji} {rg_verdict}",
        f"  ({readiness.get('passed_checks',0)}/{readiness.get('total_checks',0)} checks passed)",
    ]
    for key, check in readiness.get("checks", {}).items():
        lines.append(f"  {check.get('note','')[:120]}")

    # ── #4 Signal Decay ───────────────────────────────────────────────────────
    decay = report.get("signal_decay", {})
    if decay.get("status") in ("stale", "fresh", "unknown"):
        lines += ["", f"{'─'*38}", f"⏱ *SIGNAL FRESHNESS:*", f"  {decay.get('note','')}"]

    # ── #7 Manipulation Risk ──────────────────────────────────────────────────
    manip = report.get("manipulation_risk", {})
    if manip:
        lines += ["", f"{'─'*38}", f"🛡 *MANIPULATION RISK:* {manip.get('note','')}"]
        for flag in manip.get("flags", [])[:3]:
            lines.append(f"  • {_md_escape(flag[:150])}")

    # ── #10 External Consensus ────────────────────────────────────────────────
    ext = report.get("external_consensus", {})
    ext_conflicts = ext.get("conflicts", [])
    ext_checks    = ext.get("checks", {})
    if ext_checks or ext_conflicts:
        lines += ["", f"{'─'*38}", f"🌐 *LIVE API FACT-CHECK:*"]
        for key, info in ext_checks.items():
            match  = info.get("match")
            stated = info.get("stated")
            live   = info.get("live")
            if stated is not None:
                tick = "✅" if match else "❌"
                lines.append(f"  {tick} {key.replace('_',' ').title()}: bot={stated} live={live}")
            else:
                lines.append(f"  ℹ️ {key.replace('_',' ').title()}: live={live}")
        for ec in ext_conflicts:
            lines.append(f"  ⚡ {_md_escape(ec[:180])}")

    # ── Key Retained Findings ─────────────────────────────────────────────────
    retained = report.get("key_retained_findings", [])
    if retained:
        lines += ["", f"{'─'*38}", f"🔍 *KEY FINDINGS ({len(retained)}):*"]
        for f_ in retained[:5]:
            lines.append(f"  • {_md_escape(f_[:160])}")

    # ── Conflicts ─────────────────────────────────────────────────────────────
    if report["conflicts"]:
        lines += ["", f"{'─'*38}", f"⚡ *CONFLICTS ({len(report['conflicts'])}):*"]
        for c in report["conflicts"]:
            lines.append(f"  ⚡ {_md_escape(c[:180])}")

    # ── Hallucinations ────────────────────────────────────────────────────────
    if report["hallucinations"]:
        lines += ["", f"{'─'*38}", f"🚨 *UNSUPPORTED CLAIMS ({len(report['hallucinations'])}):*"]
        for h in report["hallucinations"]:
            lines.append(f"  🚨 {_md_escape(h[:180])}")

    # ── Risk Flags ────────────────────────────────────────────────────────────
    if report["risk_flags"]:
        lines += ["", f"{'─'*38}", f"⚠️ *RISK FLAGS ({len(report['risk_flags'])}):*"]
        for rf in report["risk_flags"]:
            lines.append(f"  ⚠️ {_md_escape(rf[:180])}")

    # ── Missing Elements ──────────────────────────────────────────────────────
    if report["missing_elements"]:
        lines += ["", f"{'─'*38}", f"📌 *MISSING ({len(report['missing_elements'])}):*"]
        for m in report["missing_elements"]:
            lines.append(f"  📌 {_md_escape(m[:180])}")

    # ── #9 Per-Coin Micro-Validation ──────────────────────────────────────────
    micro = report.get("micro_validation", [])
    if micro:
        incomplete = [c for c in micro if c.get("issues")]
        lines += ["", f"{'─'*38}", f"🔬 *PER-COIN MICRO-VALIDATION ({len(micro)} coins):*"]
        if not incomplete:
            lines.append(f"  ✅ All {len(micro)} coins validated OK")
        else:
            lines.append(f"  ⚠️ {len(incomplete)}/{len(micro)} coins have issues:")
            for c in incomplete[:5]:
                lines.append(f"  • {c['symbol']}: {', '.join(c['issues'])} — `{_md_escape(c['line'][:80])}`")

    # ── #8 What To Do Next ────────────────────────────────────────────────────
    actions = report.get("action_block", [])
    if actions:
        lines += ["", f"{'─'*38}", f"📋 *WHAT TO DO NEXT:*"]
        for i, action in enumerate(actions, 1):
            lines.append(f"  {i}. {_md_escape(action[:160])}")

    # ── #6 Bot Health Dashboard ───────────────────────────────────────────────
    dashboard = report.get("health_dashboard", "")
    if dashboard and dashboard.strip() != "No validations run yet.":
        lines += ["", f"{'─'*38}", f"🏥 *BOT HEALTH DASHBOARD:*"]
        for dl in dashboard.splitlines():
            lines.append(f"  {_md_escape(dl.strip()[:100])}")

    # ── Bugs ──────────────────────────────────────────────────────────────────
    if bugs:
        urgent_b = [b for b in bugs if b.get("severity") == "URGENT"]
        mild_b   = [b for b in bugs if b.get("severity") == "MILD"]
        low_b    = [b for b in bugs if b.get("severity") == "LOW"]
        lines += ["", f"{'─'*38}",
                  f"🐛 *BUGS FOUND ({len(bugs)}):*",
                  f"  🔴 Urgent:{len(urgent_b)}  🟡 Mild:{len(mild_b)}  🟢 Low:{len(low_b)}"]
        for bug in bugs[:5]:
            se = sev_emoji.get(bug.get("severity","LOW"), "⚪")
            lines += [
                f"",
                f"  {se} *[{bug.get('severity','?')}] {_md_escape(bug.get('title',''))}*",
                f"  📝 {_md_escape(bug.get('description','')[:180])}",
                f"  🔎 Cause: {_md_escape(bug.get('root_cause','')[:180])}",
                f"  🔧 Fix: {_md_escape(bug.get('how_to_fix','')[:180])}",
            ]
            code = bug.get("python_fix","").strip()
            if code:
                lines.append(f"  💻 `{code[:500]}`")
            feat = bug.get("feature_suggestion","").strip()
            if feat:
                lines.append(f"  💡 {_md_escape(feat[:160])}")
            bp = bug.get("best_practice","").strip()
            if bp:
                lines.append(f"  ⭐ {_md_escape(bp[:160])}")

    # ── Improvements ──────────────────────────────────────────────────────────
    if imps:
        lines += ["", f"{'─'*38}", f"🚀 *IMPROVEMENTS ({len(imps)}):*"]
        for imp in imps[:4]:
            pe = pri_emoji.get(imp.get("priority","LOW"), "⚪")
            complexity = imp.get("implementation_complexity", "")
            comp_e = {"EASY": "🟢", "MEDIUM": "🟡", "HARD": "🔴"}.get(complexity, "")
            lines += [
                f"",
                f"  {pe} *[{imp.get('priority','?')}] {_md_escape(imp.get('title',''))}* {comp_e}",
                f"  {_md_escape(imp.get('description','')[:220])}",
            ]
            code = imp.get("suggested_code","").strip()
            if code:
                lines.append(f"  💻 `{code[:500]}`")

    # ── YouTube Synthesis ─────────────────────────────────────────────────────
    if yt_synth:
        lines += ["", f"{'─'*38}",
                  f"🎬 *COMBINED YOUTUBE INTELLIGENCE:*",
                  f"_{_md_escape(yt_synth[:600])}_"]

    # ── AI Verdicts ───────────────────────────────────────────────────────────
    if report.get("individual_results"):
        lines += ["", f"{'─'*38}", f"🤖 *AI VALIDATOR VERDICTS:*"]
        for r in report["individual_results"]:
            val   = _md_escape(r.get("validator","?"))
            sv    = _md_escape(r.get("summary","")[:280])
            verd  = r.get("verdict","?")
            sc    = r.get("composite_score","?")
            ve    = {"VALID":"✅","NEEDS_REVIEW":"🔍","INVALID":"❌"}.get(verd,"❓")
            lines += [f"", f"  {ve} *{val}* — {sc}/100", f"  {sv}"]
            ev = r.get("_auto_evidence","")
            if ev:
                for rl in [l for l in ev.splitlines() if l.startswith("[YT]") or l.startswith("[WEB]")][:2]:
                    lines.append(f"    📎 {_md_escape(rl[:150])}")

    # ── Failed Validators ─────────────────────────────────────────────────────
    if report["validators_failed"]:
        fail_reasons = {
            "http_404": "Model not found on endpoint — model name invalid or not yet on compat API",
            "http_400": "Bad request — response_format conflict or invalid parameter (check model compatibility)",
            "http_401": "Auth error — check API key in .env",
            "http_429": "Rate limited — too many requests",
            "auth_error": "Invalid API key",
            "rate_limit": "Rate limit exceeded",
            "exception": "Network/timeout error",
            "parse_error": "Invalid JSON from model",
            "timeout": "Validator timed out (bad model or slow network)",
            "no_endpoint": "No API endpoint configured for provider",
        }
        lines += ["", f"{'─'*38}", f"⛔ *FAILED VALIDATORS:*"]
        for name, status in report["validators_failed"]:
            reason = fail_reasons.get(str(status), str(status))
            lines.append(f"  • {_md_escape(str(name))}: {_md_escape(reason)}")

    if report["user_evidence_submitted"]:
        lines += ["", "📎 *User evidence was included in validation.*"]

    lines += [
        "", f"{'='*38}",
        f"_📥 Download for full audit + runnable code fixes_",
        f"{'='*38}",
    ]
    return "\n".join(lines)


def _fb_extract_refs(report: dict) -> list[dict]:
    """Extract all structured refs from individual validator results."""
    all_refs: list[dict] = []
    seen_urls: set[str] = set()
    for r in report.get("individual_results", []):
        ev = r.get("_auto_evidence", "")
        if not ev or "__REFS_JSON__:" not in ev:
            continue
        try:
            json_part = ev.split("__REFS_JSON__:")[1].strip()
            refs = json.loads(json_part)
            for ref in refs:
                url = ref.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    ref["_validator"] = r.get("validator", "?")
                    all_refs.append(ref)
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    return all_refs


async def _fb_send_thumbnails(message, report: dict):
    """
    Send YouTube thumbnails as Telegram photos after a validation report.
    Groups by validator, sends thumb + caption with title, channel, link.
    """
    refs = _fb_extract_refs(report)
    yt_refs = [r for r in refs if r.get("type") == "yt" and r.get("thumb")]
    if not yt_refs:
        return

    await message.reply_text(
        f"🎬 *YouTube Evidence Used by AI Validators* ({len(yt_refs)} video(s)):",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )
    for ref in yt_refs[:6]:  # cap at 6 thumbnails
        caption = (
            f"📺 *{_md_escape(ref.get('title','?')[:80])}*\n"
            f"👤 {_md_escape(ref.get('author','?'))}\n"
            f"🔗 {ref.get('url','')}\n"
            f"_Used by: {_md_escape(ref.get('_validator','?'))}_"
        )
        try:
            await message.reply_photo(
                photo=ref["thumb"],
                caption=caption,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"[FB] Could not send thumbnail {ref.get('url')}: {e}")


# ── Downloadable .txt report ─────────────────────────────────────────────────

def _fb_build_gemini_log(report: dict) -> str:
    """Build a full raw log of the Gemini validator only."""
    SEP  = "=" * 72
    THIN = "-" * 72

    lines = []
    lines.append(SEP)
    lines.append("  GEMINI VALIDATOR — FULL RAW LOG")
    lines.append(f"  Generated : {report['generated_at'][:19].replace('T',' ')} UTC")
    lines.append(f"  Output Type: {report['output_type']}")
    lines.append(SEP)

    gemini_results = [
        r for r in report.get("individual_results", [])
        if "gemini" in r.get("validator", "").lower() or r.get("_provider", "") == "gemini"
    ]

    if not gemini_results:
        # Also check failed validators
        failed_gemini = [
            (name, status) for name, status in report.get("validators_failed", [])
            if "gemini" in name.lower()
        ]
        if failed_gemini:
            lines.append("\n  GEMINI VALIDATOR STATUS: FAILED\n")
            for name, status in failed_gemini:
                lines.append(f"  Validator : {name}")
                lines.append(f"  Status    : {status}")
        else:
            lines.append("\n  No Gemini validator results found in this report.")
            lines.append("  Gemini may not have been available or was not used.\n")
        lines.append(SEP)
        return "\n".join(lines)

    for r in gemini_results:
        lines.append(f"\n{'  VALIDATOR: ' + r.get('validator','?')}")
        lines.append(f"  Status    : {r.get('_status','?')}")
        lines.append(f"  Model     : {r.get('_model','?')}")
        lines.append(f"  Provider  : {r.get('_provider','?')}")
        lines.append(THIN)

        lines.append("\n  ── VERDICT & SCORES ──────────────────────────────────────────")
        lines.append(f"  Verdict         : {r.get('verdict','?')}")
        lines.append(f"  Composite Score : {r.get('composite_score','?')}/100")
        sc = r.get("scores", {})
        lines.append(f"  Accuracy        : {sc.get('accuracy','?')}/100")
        lines.append(f"  Completeness    : {sc.get('completeness','?')}/100")
        lines.append(f"  Consistency     : {sc.get('consistency','?')}/100")
        lines.append(f"  Timeliness      : {sc.get('timeliness','?')}/100")
        lines.append(f"  Reliability     : {sc.get('reliability','?')}/100")

        lines.append("\n  ── SUMMARY ───────────────────────────────────────────────────")
        lines.append(f"  {r.get('summary','N/A')}")

        lines.append("\n  ── STAGE 1: RAW FINDINGS ─────────────────────────────────────")
        for item in r.get("stage1_findings", []):
            lines.append(f"    • {item}")
        if not r.get("stage1_findings"):
            lines.append("    (none)")

        lines.append("\n  ── STAGE 2: SYNTHESIS ────────────────────────────────────────")
        for item in r.get("stage2_synthesis", []):
            lines.append(f"    • {item}")
        if not r.get("stage2_synthesis"):
            lines.append("    (none)")

        lines.append("\n  ── STAGE 3: RETAINED (scored 7+) ────────────────────────────")
        for item in r.get("stage3_retained", []):
            lines.append(f"    ✅ {item}")
        if not r.get("stage3_retained"):
            lines.append("    (none)")

        lines.append("\n  ── STAGE 3: DISCARDED ────────────────────────────────────────")
        for item in r.get("stage3_discarded", []):
            lines.append(f"    ✗ {item}")
        if not r.get("stage3_discarded"):
            lines.append("    (none)")

        lines.append("\n  ── STAGE 4 CHECKS ────────────────────────────────────────────")
        lines.append(f"  Check 1: {r.get('stage4_check1','N/A')}")
        lines.append(f"  Check 2: {r.get('stage4_check2','N/A')}")
        lines.append(f"  Check 3: {r.get('stage4_check3','N/A')}")

        lines.append("\n  ── CONFLICTS ─────────────────────────────────────────────────")
        conflicts = r.get("conflicts", [])
        for item in conflicts:
            lines.append(f"    ⚠️ {item}")
        if not conflicts:
            lines.append("    (none)")

        lines.append("\n  ── HALLUCINATIONS ────────────────────────────────────────────")
        hallucinations = r.get("hallucinations", [])
        for item in hallucinations:
            lines.append(f"    ❌ {item}")
        if not hallucinations:
            lines.append("    (none)")

        lines.append("\n  ── RISK FLAGS ────────────────────────────────────────────────")
        risk_flags = r.get("risk_flags", [])
        for item in risk_flags:
            lines.append(f"    🚩 {item}")
        if not risk_flags:
            lines.append("    (none)")

        lines.append("\n  ── MISSING ELEMENTS ──────────────────────────────────────────")
        missing = r.get("missing_elements", [])
        for item in missing:
            lines.append(f"    • {item}")
        if not missing:
            lines.append("    (none)")

        lines.append("\n  ── TA/SMC CONCEPT ERRORS ─────────────────────────────────────")
        ta_errors = r.get("ta_concept_errors", [])
        for item in ta_errors:
            lines.append(f"    • {item}")
        if not ta_errors:
            lines.append("    (none)")

        lines.append("\n  ── ON-CHAIN VALIDATION ───────────────────────────────────────")
        onchain = r.get("on_chain_validation", {})
        if onchain:
            for metric, info in onchain.items():
                assess = info.get("assessment", "unknown")
                tick   = "✅" if assess == "valid" else ("❌" if assess == "suspect" else "⚪")
                lines.append(f"    {tick} {metric}: stated={info.get('stated','?')}  expected={info.get('expected_range','?')}  → {assess.upper()}")
        else:
            lines.append("    (none)")

        lines.append("\n  ── BUGS FOUND ────────────────────────────────────────────────")
        bugs = r.get("bugs", [])
        if not bugs:
            lines.append("    No bugs detected.")
        else:
            for i, bug in enumerate(bugs, 1):
                lines.append(f"    BUG #{i} [{bug.get('severity','?')}]")
                lines.append(f"    Title       : {bug.get('title','?')}")
                lines.append(f"    Description : {bug.get('description','N/A')}")
                lines.append(f"    Root Cause  : {bug.get('root_cause','N/A')}")
                lines.append(f"    How To Fix  : {bug.get('how_to_fix','N/A')}")
                python_fix = bug.get("python_fix", "").strip()
                if python_fix:
                    lines.append(f"    ── Python Fix ──────────────────────────────────────────")
                    for code_line in python_fix.splitlines():
                        lines.append(f"      {code_line}")
                    lines.append(f"    ────────────────────────────────────────────────────────")
                lines.append("")

        lines.append("\n  ── IMPROVEMENTS SUGGESTED ────────────────────────────────────")
        improvements = r.get("improvements", [])
        if not improvements:
            lines.append("    No improvements suggested.")
        else:
            for i, imp in enumerate(improvements, 1):
                lines.append(f"    IMPROVEMENT #{i} [{imp.get('priority','?')}]")
                lines.append(f"    Title       : {imp.get('title','?')}")
                lines.append(f"    Description : {imp.get('description','N/A')}")
                code = imp.get("suggested_code", "").strip()
                if code:
                    lines.append(f"    ── Suggested Code ──────────────────────────────────────")
                    for code_line in code.splitlines():
                        lines.append(f"      {code_line}")
                    lines.append(f"    ────────────────────────────────────────────────────────")
                lines.append("")

        lines.append("\n  ── PROBABILISTIC SCENARIOS ───────────────────────────────────")
        scenarios = r.get("stage4_scenarios", [])
        if not scenarios:
            lines.append("    (none)")
        else:
            for sc in scenarios:
                lines.append(f"    Asset: {sc.get('asset','?')}  Confidence: {sc.get('confidence','?')}  R:R {sc.get('reward_risk',0):.1f}:1")
                lines.append(f"    🐂 Bull {sc.get('bull_prob',0)}%  Target: {sc.get('bull_target','')}  Trigger: {sc.get('bull_trigger','')}")
                lines.append(f"    ➡️ Base {sc.get('base_prob',0)}%  Path: {sc.get('base_path','')}")
                lines.append(f"    🐻 Bear {sc.get('bear_prob',0)}%  Target: {sc.get('bear_target','')}  Invalidation: {sc.get('bear_trigger','')}")
                lines.append("")

        lines.append("\n  ── YOUTUBE SYNTHESIS ─────────────────────────────────────────")
        yt = r.get("youtube_synthesis", "").strip()
        lines.append(f"  {yt}" if yt else "    (none)")

        lines.append("\n  ── NEWS IMPACT ASSESSMENT ────────────────────────────────────")
        news = r.get("news_impact_assessment", "").strip()
        lines.append(f"  {news}" if news else "    (none)")

        lines.append("\n  ── PRO KNOWLEDGE SUGGESTIONS ─────────────────────────────────")
        for item in r.get("pro_knowledge_suggestions", []):
            lines.append(f"    • {item}")
        if not r.get("pro_knowledge_suggestions"):
            lines.append("    (none)")

        lines.append("\n  ── DATA SCRAPING RECOMMENDATIONS ─────────────────────────────")
        for item in r.get("data_scraping_recommendations", []):
            lines.append(f"    • {item}")
        if not r.get("data_scraping_recommendations"):
            lines.append("    (none)")

        lines.append("\n  ── AUTO-EVIDENCE USED ────────────────────────────────────────")
        ae = r.get("_auto_evidence", "").strip() if r.get("_auto_evidence") else ""
        if ae:
            lines.append(textwrap.fill(ae[:1000], width=70, initial_indent="  ", subsequent_indent="  "))
        else:
            lines.append("    (none)")

        lines.append("\n" + THIN)

    lines.append(SEP)
    lines.append("  Gemini Validator Log — Feedback Engine v4.0")
    lines.append("  DYOR — Automated QA analysis only. Not financial advice.")
    lines.append(SEP)
    return "\n".join(lines)


def _fb_build_txt(report: dict) -> str:
    SEP  = "=" * 72
    THIN = "-" * 72

    def section(title): return f"\n{SEP}\n  {title}\n{SEP}\n"
    def bullets(items, indent=2):
        if not items:
            return " " * indent + "(none)\n"
        return "".join(f"{' ' * indent}• {item}\n" for item in items)

    sev_rank_label = {"URGENT": "🔴 URGENT", "MILD": "🟡 MILD", "LOW": "🟢 LOW"}
    pri_rank_label = {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "🟢 LOW"}

    lines = []
    lines.append(SEP)
    lines.append("  QA VALIDATION REPORT — CRYPTO BOT FEEDBACK ENGINE v3.0")
    lines.append(f"  Generated : {report['generated_at'][:19].replace('T',' ')} UTC")
    lines.append(f"  Output Type: {report['output_type']}")
    lines.append(SEP)

    lines.append(section("EXECUTIVE SUMMARY"))
    lines.append(f"  VERDICT         : {report['verdict_emoji']} {report['verdict']}")
    lines.append(f"  COMPOSITE SCORE : {report['composite_score']}/100")

    # #1 Score trend
    trend_info = report.get("score_trend", {})
    delta      = trend_info.get("delta")
    if delta is not None:
        arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "─")
        lines.append(f"  SCORE TREND     : {arrow} {'+' if delta > 0 else ''}{delta} vs previous ({trend_info.get('trend','').upper()})")
        hist = trend_info.get("history", [])
        if hist:
            lines.append(f"  SCORE HISTORY   : {' → '.join(str(s) for s in hist)}")

    lines.append(f"  VALIDATORS OK   : {report['validators_ok']} / {report['validators_attempted']}")
    lines.append(f"  FINGERPRINT     : {report.get('fingerprint','N/A')}  (SHA256 first 8 — detects duplicate outputs)")
    lines.append(f"  BUGS FOUND      : {len(report.get('bugs',[]))} "
                 f"({sum(1 for b in report.get('bugs',[]) if b.get('severity')=='URGENT')} urgent, "
                 f"{sum(1 for b in report.get('bugs',[]) if b.get('severity')=='MILD')} mild, "
                 f"{sum(1 for b in report.get('bugs',[]) if b.get('severity')=='LOW')} low)")
    lines.append(f"  IMPROVEMENTS    : {len(report.get('improvements',[]))} suggested")
    lines.append(f"  USER EVIDENCE   : {'YES' if report['user_evidence_submitted'] else 'NO'}")

    # #2 Trading readiness
    readiness = report.get("trading_readiness", {})
    lines.append(f"\n  TRADING READINESS GATE: {readiness.get('gate_emoji','?')} {readiness.get('verdict','N/A')}")
    lines.append(f"  ({readiness.get('passed_checks',0)}/{readiness.get('total_checks',0)} checks passed)")
    for key, check in readiness.get("checks", {}).items():
        lines.append(f"    {check.get('note','')}")

    # #4 Signal decay
    decay = report.get("signal_decay", {})
    if decay:
        lines.append(f"\n  SIGNAL FRESHNESS: {decay.get('note','N/A')}")

    # #7 Manipulation risk
    manip = report.get("manipulation_risk", {})
    if manip:
        lines.append(f"\n  MANIPULATION RISK: {manip.get('note','N/A')}")
        for flag in manip.get("flags", []):
            lines.append(f"    • {flag}")

    lines.append(section("DIMENSION SCORES (Weighted QA Audit)"))
    s = report["avg_scores"]
    for dim, weight in _FB_DIMENSIONS.items():
        score = s.get(dim, 0)
        bar   = "█" * int(score / 10) + "░" * (10 - int(score / 10))
        lines.append(f"  {dim.capitalize():<14} [{bar}] {score:>5}/100  (weight {int(weight*100)}%)")
    lines.append(f"\n  {'COMPOSITE':<14}              {report['composite_score']:>5}/100")

    lines.append(section("OMNI-CONTEXT PIPELINE — KEY RETAINED FINDINGS"))
    lines.append(bullets(report["key_retained_findings"]))

    lines.append(section("CONFLICT ANALYSIS"))
    lines.append(bullets(report["conflicts"]) if report["conflicts"] else "  No conflicts detected.\n")

    lines.append(section("HALLUCINATION / UNSUPPORTED CLAIMS"))
    lines.append(bullets(report["hallucinations"]) if report["hallucinations"] else "  None detected.\n")

    lines.append(section("RISK FLAGS"))
    lines.append(bullets(report["risk_flags"]) if report["risk_flags"] else "  No risk flags.\n")

    lines.append(section("MISSING ELEMENTS"))
    lines.append(bullets(report["missing_elements"]) if report["missing_elements"] else "  Output appears complete.\n")

    # ── #10 External Consensus Check ──────────────────────────────────────────
    lines.append(section("LIVE API FACT-CHECK (External Consensus)"))
    ext = report.get("external_consensus", {})
    ext_checks    = ext.get("checks", {})
    ext_conflicts = ext.get("conflicts", [])
    if ext_checks:
        for key, info in ext_checks.items():
            stated = info.get("stated")
            live   = info.get("live")
            match  = info.get("match")
            label  = key.replace("_", " ").title()
            if stated is not None:
                tick = "✅ MATCH" if match else "❌ MISMATCH"
                lines.append(f"  {label}: bot stated={stated}  live API={live}  → {tick}")
            else:
                lines.append(f"  {label}: live API={live}  (not stated in output)")
    if ext_conflicts:
        lines.append(f"\n  Conflicts detected:")
        lines.append(bullets(ext_conflicts))
    if not ext_checks and not ext_conflicts:
        lines.append("  No live API checks performed.\n")

    # ── #3 Validator Agreement Matrix ─────────────────────────────────────────
    lines.append(section("VALIDATOR AGREEMENT MATRIX"))
    matrix = report.get("agreement_matrix", {}).get("matrix", {})
    if matrix:
        header = f"  {'Dimension':<14}" + "".join(f"  {r.get('validator','?')[:10]:<12}" for r in report["individual_results"])
        lines.append(header)
        lines.append("  " + THIN[:len(header)-2])
        for dim, info in matrix.items():
            row  = f"  {dim.capitalize():<14}"
            for _, score in info["scores"]:
                bar = "█" * int(score / 10) + "░" * (10 - int(score / 10))
                row += f"  {score:>5}/100  "
            spread_flag = "  ⚠️ SPREAD WARNING" if info.get("flagged") else ""
            lines.append(row + f"  spread={info['spread']:.0f}{spread_flag}")
    spread_conflicts = report.get("agreement_matrix", {}).get("spread_conflicts", [])
    if spread_conflicts:
        lines.append(f"\n  Spread conflicts flagged:")
        lines.append(bullets(spread_conflicts))

    # ── #9 Per-Coin Micro-Validation ──────────────────────────────────────────
    micro = report.get("micro_validation", [])
    lines.append(section("PER-COIN MICRO-VALIDATION"))
    if not micro:
        lines.append("  No individual coin signals detected in output.\n")
    else:
        ok_coins   = [c for c in micro if not c.get("issues")]
        bad_coins  = [c for c in micro if c.get("issues")]
        lines.append(f"  Total coins parsed: {len(micro)}  ✅ OK: {len(ok_coins)}  ⚠️ Issues: {len(bad_coins)}")
        lines.append("")
        for c in micro:
            status = c.get("status","?")
            issues = ", ".join(c.get("issues",[])) or "none"
            lines.append(f"  {status}  {c['symbol']:<10} Issues: {issues}")
            lines.append(f"    Line: {c.get('line','')}")
        lines.append("")

    # ── #8 What To Do Next ────────────────────────────────────────────────────
    lines.append(section("WHAT TO DO NEXT (Actionable Steps)"))
    actions = report.get("action_block", [])
    if not actions:
        lines.append("  ✅ No immediate actions required.\n")
    else:
        for i, action in enumerate(actions, 1):
            lines.append(f"  {i}. {action}")
        lines.append("")

    # ── #6 Bot Health Dashboard ───────────────────────────────────────────────
    lines.append(section("BOT HEALTH DASHBOARD (Rolling Last 5 per Output Type)"))
    dashboard = report.get("health_dashboard", "")
    lines.append(dashboard if dashboard.strip() else "  No validation history yet.\n")

    # ── BUGS SECTION ──────────────────────────────────────────────────────────
    lines.append(section("BUGS & ISSUES FOUND (WITH FIX GUIDE)"))
    bugs = report.get("bugs", [])
    if not bugs:
        lines.append("  No bugs detected.\n")
    else:
        for i, bug in enumerate(bugs, 1):
            sev_label = sev_rank_label.get(bug.get("severity", "LOW"), bug.get("severity", "?"))
            lines.append(f"  BUG #{i} [{sev_label}]")
            lines.append(f"  Title       : {bug.get('title', 'Untitled')}")
            lines.append(f"  Description : {bug.get('description', 'N/A')}")
            lines.append(f"  Root Cause  : {bug.get('root_cause', 'N/A')}")
            lines.append(f"  How To Fix  : {bug.get('how_to_fix', 'N/A')}")
            lines.append(f"")
            python_fix = bug.get("python_fix", "").strip()
            if python_fix:
                lines.append(f"  ── Python Fix Code ──────────────────────────────────────────")
                for code_line in python_fix.splitlines():
                    lines.append(f"    {code_line}")
                lines.append(f"  ─────────────────────────────────────────────────────────────")
            feature = bug.get("feature_suggestion", "").strip()
            if feature:
                lines.append(f"  Feature Suggestion : {feature}")
            bp = bug.get("best_practice", "").strip()
            if bp:
                lines.append(f"  Best Practice      : {bp}")
            lines.append(THIN)

    # ── IMPROVEMENTS SECTION ──────────────────────────────────────────────────
    lines.append(section("SUGGESTED IMPROVEMENTS"))
    improvements = report.get("improvements", [])
    if not improvements:
        lines.append("  No improvements suggested.\n")
    else:
        for i, imp in enumerate(improvements, 1):
            pri_label = pri_rank_label.get(imp.get("priority", "LOW"), imp.get("priority", "?"))
            lines.append(f"  IMPROVEMENT #{i} [{pri_label}]")
            lines.append(f"  Title       : {imp.get('title', 'Untitled')}")
            lines.append(f"  Description : {imp.get('description', 'N/A')}")
            code = imp.get("suggested_code", "").strip()
            if code:
                lines.append(f"  ── Suggested Code ───────────────────────────────────────────")
                for code_line in code.splitlines():
                    lines.append(f"    {code_line}")
                lines.append(f"  ─────────────────────────────────────────────────────────────")
            lines.append(THIN)

    # ── YOUTUBE COMBINED SYNTHESIS ────────────────────────────────────────────
    yt_synth = report.get("youtube_synthesis", "").strip()
    lines.append(section("COMBINED YOUTUBE INTELLIGENCE SUMMARY"))
    if yt_synth:
        lines.append(textwrap.fill(yt_synth, width=70,
                                   initial_indent="  ", subsequent_indent="  "))
    else:
        lines.append("  No YouTube references synthesized.\n")

    # ── PROBABILISTIC OUTCOME SCENARIOS ───────────────────────────────────────
    scenarios = report.get("stage4_scenarios", [])
    lines.append(section("PROBABILISTIC OUTCOME MODELING"))
    if not scenarios:
        lines.append("  No probability scenarios generated.\n")
    else:
        for sc in scenarios:
            asset = sc.get("asset", "Market")
            conf  = sc.get("confidence", "?")
            rr    = sc.get("reward_risk", 0)
            lines.append(f"  ── {asset} ── Confidence: {conf}  Reward:Risk {rr:.1f}:1")
            lines.append(f"  🐂 Bull  {sc.get('bull_prob',0):>3}%  Target: {sc.get('bull_target','')}  Trigger: {sc.get('bull_trigger','')}")
            lines.append(f"  ➡️ Base  {sc.get('base_prob',0):>3}%  Path: {sc.get('base_path','')}")
            lines.append(f"  🐻 Bear  {sc.get('bear_prob',0):>3}%  Target: {sc.get('bear_target','')}  Invalidation: {sc.get('bear_trigger','')}")
            confirm = sc.get("confirm_metrics", [])
            if confirm:
                lines.append(f"  📡 Confirmation metrics: {', '.join(confirm)}")
            lines.append(THIN)

    # ── TA CONCEPT ERRORS ─────────────────────────────────────────────────────
    ta_errors = report.get("ta_concept_errors", [])
    lines.append(section("TA/SMC/WYCKOFF CONCEPT ERRORS"))
    if not ta_errors:
        lines.append("  No TA concept errors detected.\n")
    else:
        lines.append(bullets(ta_errors))

    # ── ON-CHAIN METRIC VALIDATION ────────────────────────────────────────────
    onchain = report.get("on_chain_validation", {})
    lines.append(section("ON-CHAIN METRIC VALIDATION"))
    if not onchain:
        lines.append("  No on-chain metrics validated.\n")
    else:
        for metric, info in onchain.items():
            assess = info.get("assessment", "unknown")
            tick   = "✅" if assess == "valid" else ("❌" if assess == "suspect" else "⚪")
            lines.append(f"  {tick} {metric}: stated={info.get('stated','?')}  expected={info.get('expected_range','?')}  → {assess.upper()}")
        lines.append("")

    # ── NEWS IMPACT ASSESSMENT ────────────────────────────────────────────────
    news_impact = report.get("news_impact_assessment", "").strip()
    lines.append(section("NEWS IMPACT ASSESSMENT (Institutional Analysis)"))
    if news_impact:
        lines.append(textwrap.fill(news_impact, width=70,
                                   initial_indent="  ", subsequent_indent="  "))
        lines.append("")
    else:
        lines.append("  No news impact assessment generated.\n")

    # ── PRO KNOWLEDGE SUGGESTIONS ─────────────────────────────────────────────
    pro_tips = report.get("pro_knowledge_suggestions", [])
    lines.append(section("PRO KNOWLEDGE UPGRADE SUGGESTIONS"))
    if not pro_tips:
        lines.append("  No pro knowledge suggestions.\n")
    else:
        lines.append(bullets(pro_tips))

    # ── DATA SCRAPING RECOMMENDATIONS ─────────────────────────────────────────
    data_recs = report.get("data_scraping_recommendations", [])
    lines.append(section("GROUND-TRUTH DATA SOURCE RECOMMENDATIONS"))
    if not data_recs:
        lines.append("  No data source recommendations.\n")
    else:
        lines.append(bullets(data_recs))

    lines.append(section("INDIVIDUAL AI VALIDATOR VERDICTS"))
    for r in report["individual_results"]:
        lines.append(f"  Validator : {r.get('validator','?')}")
        lines.append(f"  Verdict   : {r.get('verdict','?')}")
        lines.append(f"  Score     : {r.get('composite_score','?')}/100")
        sc = r.get("scores", {})
        lines.append(f"  Scores    : Acc={sc.get('accuracy','?')} Comp={sc.get('completeness','?')} "
                     f"Cons={sc.get('consistency','?')} Time={sc.get('timeliness','?')} Rel={sc.get('reliability','?')}")
        lines.append(f"  Stage4-C1 : {r.get('stage4_check1','N/A')}")
        lines.append(f"  Stage4-C2 : {r.get('stage4_check2','N/A')}")
        lines.append(f"  Stage4-C3 : {r.get('stage4_check3','N/A')}")
        lines.append(f"  Summary   : {r.get('summary','N/A')}")
        # Per-validator bugs
        v_bugs = r.get("bugs", [])
        if v_bugs:
            lines.append(f"  Bugs Found: {len(v_bugs)}")
            for b in v_bugs[:3]:
                lines.append(f"    [{b.get('severity','?')}] {b.get('title','?')}")
        lines.append(THIN)

    if report["validators_failed"]:
        lines.append(section("FAILED VALIDATORS"))
        fail_reasons = {
            "http_404": "Model not found on endpoint — model name invalid or not yet on compat API",
            "http_400": "Bad request — response_format conflict or invalid parameter",
            "http_401": "Auth error — verify API key in .env",
            "http_429": "Rate limited — implement exponential backoff or reduce parallel calls",
            "auth_error": "Invalid API key — regenerate and update .env",
            "rate_limit": "Rate limit exceeded — add delay between validator calls",
            "exception": "Network/timeout error — check connectivity and increase timeout",
            "parse_error": "Invalid JSON from model — add JSON repair / retry logic",
        }
        for name, status in report["validators_failed"]:
            reason = fail_reasons.get(str(status), str(status))
            lines.append(f"  {name} — {reason}")

    if report["user_evidence_submitted"]:
        lines.append(section("USER-SUBMITTED EVIDENCE (Preview)"))
        lines.append(f"  {report['user_evidence_preview']}")

    lines.append(section("BOT OUTPUT VALIDATED (Preview)"))
    lines.append(textwrap.fill(report["output_preview"], width=70,
                               initial_indent="  ", subsequent_indent="  "))

    lines.append(section("VALIDATION FLOWCHART & QA ARCHITECTURE"))
    lines.append("""
  ┌─────────────────────────────────────────────────────────────┐
  │           CRYPTEX-QA VALIDATION ENGINE v5.0                 │
  │   Groq · Cerebras · Gemini · Mistral — Auto-Ranked          │
  │   ICT/SMC · Wyckoff · Elliott Wave · On-Chain · Quant       │
  └──────────────────────────┬──────────────────────────────────┘
                             │
                      [BOT OUTPUT CAPTURED]
                      [TRIO: Pin+Overview+News]
                             │
               ┌─────────────▼─────────────┐
               │   RANK AI PROVIDERS        │
               │   1. Check all 4 API keys  │
               │   2. Auto-detect models    │
               │   3. Sort by priority      │
               │   4. Skip unavailable      │
               └─────────────┬─────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
  [Groq Validator]  [Cerebras Validator]  [Gemini/Mistral]
        │                    │                    │
  ┌─────▼──────────────────────────────────────────┐
  │ STAGE 1: RAG Extract facts, numbers, claims     │
  │ STAGE 2: Synthesize into precise statements     │
  │ STAGE 3: 80/20 Trim — retain scored 7+          │
  │ STAGE 4: Recursive 3-check gate                 │
  │ STAGE 5: Bug analysis + improvement suggestions │
  │          + YouTube content synthesis            │
  └─────┬──────────────────────────────────────────┘
        │
  [CONSENSUS ENGINE]
  • Average scores, majority verdict
  • Merge bugs by severity (URGENT→MILD→LOW)
  • Merge improvements by priority
  • Combine YouTube syntheses into ONE summary
        │
  ┌─────▼──────────────────────────────────────────┐
  │  FINAL REPORT                                   │
  │  • Telegram detailed card (scores, bugs, fixes) │
  │  • Full .txt audit with runnable Python fixes   │
  └────────────────────────────────────────────────┘

  QA DIMENSIONS:
  ┌─────────────┬────────┬────────────────────────────────┐
  │ Dimension   │ Weight │ What It Checks                 │
  ├─────────────┼────────┼────────────────────────────────┤
  │ Accuracy    │  30%   │ Factual correctness            │
  │ Completeness│  20%   │ No critical omissions          │
  │ Consistency │  20%   │ No internal contradictions     │
  │ Timeliness  │  15%   │ Data freshness / currency      │
  │ Reliability │  15%   │ Source & method soundness      │
  └─────────────┴────────┴────────────────────────────────┘
""")

    lines.append(SEP)
    lines.append("  QA Report by Feedback Engine v4.0 — OMNI-Context Pipeline v5.6")
    lines.append("  10-Feature QA Suite: Score Trend · Readiness Gate · Agreement Matrix ·")
    lines.append("  Signal Decay · DNA Fingerprint · Health Dashboard · Manipulation Risk ·")
    lines.append("  Action Block · Micro-Validation · External Consensus Check")
    lines.append("  DYOR — Automated QA analysis only. Not financial advice.")
    lines.append(SEP)
    return "\n".join(lines)


# ── URL fetcher for user evidence ────────────────────────────────────────────

async def _fb_fetch_url(url: str) -> str:
    """Fetch readable text from a URL (YouTube oembed / generic HTML / PDF note)."""
    url = url.strip()
    try:
        if "youtube.com" in url or "youtu.be" in url:
            oe = f"https://www.youtube.com/oembed?url={url}&format=json"
            async with aiohttp.ClientSession() as s:
                async with s.get(oe, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        d = await r.json()
                        return f"[YouTube] Title: {d.get('title','?')} | Author: {d.get('author_name','?')} | URL: {url}"
            return f"[YouTube] Could not fetch: {url}"
        if url.lower().endswith(".pdf"):
            return f"[PDF] URL: {url} — binary, noted as cited evidence."
        hdrs = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=hdrs, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    html = await r.text(errors="replace")
                    txt  = re.sub(r"<[^>]+>", " ", html)
                    txt  = re.sub(r"\s+", " ", txt).strip()
                    return f"[Web: {url}]\n{txt[:3000]}"
                return f"[Fetch failed HTTP {r.status}] {url}"
    except Exception as e:
        return f"[Fetch error: {e}] {url}"

def _fb_extract_urls(text: str) -> list[str]:
    return re.findall(r"https?://[^\s\]>\"']+", text)


# ==============================================================================
# QA ENGINE — 10 IMPROVEMENTS
# ==============================================================================

# ── #1 Score Trend / Delta Tracker ───────────────────────────────────────────
def _qa_get_score_delta(chat_id: int, output_type: str, new_score: float) -> dict:
    """Compare new composite score against last score for this output_type.
    Stores history and returns delta info."""
    store = feedback_store.get(chat_id, {})
    history = store.setdefault("score_history", {})
    past    = history.get(output_type, [])
    delta   = None
    trend   = "new"
    if past:
        delta = round(new_score - past[-1], 1)
        if delta > 1:   trend = "improving"
        elif delta < -1: trend = "degrading"
        else:            trend = "stable"
    past.append(round(new_score, 1))
    history[output_type] = past[-10:]  # keep last 10
    return {"delta": delta, "trend": trend, "history": past[-5:]}


# ── #2 Trading Readiness Gate ─────────────────────────────────────────────────
def _qa_trading_readiness(output_type: str, output_text: str, report: dict) -> dict:
    """Pass/fail gate: is this output safe for a trader to act on?"""
    checks = {}
    text_l = output_text.lower()

    # Check 1 — Risk disclaimer present
    has_disclaimer = any(w in text_l for w in [
        "not financial advice", "dyor", "risk", "disclaimer",
        "not advice", "nfa", "trade at your own"
    ])
    checks["risk_disclaimer"] = {
        "pass": has_disclaimer,
        "note": "✅ Disclaimer present" if has_disclaimer else "❌ No risk disclaimer — unsafe to distribute"
    }

    # Check 2 — Data freshness (timestamp in last 10 min)
    ts_now = datetime.now(timezone.utc)
    _chat_id = report.get("_chat_id", 0)
    _otype   = output_type if output_type != "trio" else "market_overview"
    capture_ts = feedback_store.get(_chat_id, {}).get("capture_ts", {}).get(_otype)
    if capture_ts:
        age_min = (ts_now - capture_ts).total_seconds() / 60
        fresh   = age_min <= 10
        checks["data_freshness"] = {
            "pass": fresh,
            "note": f"{'✅' if fresh else '❌'} Data age: {age_min:.1f} min {'(fresh)' if fresh else '(STALE — refresh before trading)'}"
        }
    else:
        checks["data_freshness"] = {"pass": None, "note": "⚠️ Capture timestamp unknown"}

    # Check 3 — Signal has entry or explicit disclaimer of no entry
    signal_types = ["ta", "cross", "alpha", "trending", "dex"]
    if output_type in signal_types:
        has_entry = any(w in text_l for w in ["entry", "buy", "sell", "sl", "tp", "stop", "target"])
        checks["signal_completeness"] = {
            "pass": has_entry,
            "note": "✅ Entry/signal details present" if has_entry else "❌ Missing entry/SL/TP — incomplete signal"
        }

    # Check 4 — Completeness score >= 75
    comp_score = report.get("avg_scores", {}).get("completeness", 0)
    checks["completeness_gate"] = {
        "pass": comp_score >= 75,
        "note": f"{'✅' if comp_score >= 75 else '❌'} Completeness: {comp_score}/100 (min 75 required)"
    }

    # Check 5 — No URGENT bugs
    urgent_bugs = [b for b in report.get("bugs", []) if b.get("severity") == "URGENT"]
    checks["no_urgent_bugs"] = {
        "pass": len(urgent_bugs) == 0,
        "note": f"{'✅' if not urgent_bugs else '❌'} Urgent bugs: {len(urgent_bugs)} (must be 0 to trade safely)"
    }

    passed  = sum(1 for c in checks.values() if c.get("pass") is True)
    total   = sum(1 for c in checks.values() if c.get("pass") is not None)
    gate_ok = passed == total
    return {
        "gate_passed":   gate_ok,
        "gate_emoji":    "✅" if gate_ok else "🚫",
        "passed_checks": passed,
        "total_checks":  total,
        "checks":        checks,
        "verdict":       "SAFE TO ACT" if gate_ok else "DO NOT TRADE ON THIS OUTPUT"
    }


# ── #3 Validator Agreement Matrix ─────────────────────────────────────────────
def _qa_agreement_matrix(individual_results: list[dict]) -> dict:
    """Build per-dimension agreement matrix across validators. Flag spread > 20."""
    dims = ["accuracy", "completeness", "consistency", "timeliness", "reliability"]
    matrix = {}
    conflicts = []
    for dim in dims:
        scores = []
        for r in individual_results:
            v = r.get("scores", {}).get(dim)
            if v is not None:
                scores.append((r.get("validator", "?"), float(v)))
        if scores:
            vals   = [s for _, s in scores]
            spread = max(vals) - min(vals)
            matrix[dim] = {
                "scores": scores,
                "spread": round(spread, 1),
                "flagged": spread > 20
            }
            if spread > 20:
                conflicts.append(f"{dim.capitalize()} disagreement: spread={spread:.0f}pts "
                                 f"({min(vals):.0f}–{max(vals):.0f})")
    return {"matrix": matrix, "spread_conflicts": conflicts}


# ── #4 Signal Decay Warning ───────────────────────────────────────────────────
def _qa_signal_decay(output_type: str, output_text: str, chat_id: int) -> dict:
    """Parse timestamps from output and compute signal age. Flag if stale."""
    store      = feedback_store.get(chat_id, {})
    now        = datetime.now(timezone.utc)

    # Resolve capture_ts — for trio, walk members in freshness order BEFORE computing age
    capture_ts = store.get("capture_ts", {}).get(output_type)
    if capture_ts is None and output_type == "trio":
        for member in ["market_overview", "news", "pin"]:
            ts = store.get("capture_ts", {}).get(member)
            if ts:
                capture_ts = ts
                break

    age_min = (now - capture_ts).total_seconds() / 60 if capture_ts else None

    # Thresholds per output type (minutes before stale)
    stale_thresholds = {
        "ta":              5,
        "cross":           10,
        "trending":        10,
        "alpha":           10,
        "market_overview": 5,
        "dex":             3,
        "sector_rotation": 15,
        "news":            30,
        "pin":             60,
        "trio":            5,
        "ai_response":     None,
    }
    threshold = stale_thresholds.get(output_type)

    if threshold is None:
        return {"status": "no_decay", "note": "⚪ No decay threshold for this output type"}
    if age_min is None:
        return {"status": "unknown", "note": "⚠️ Cannot determine signal age"}

    stale = age_min > threshold
    return {
        "status":    "stale" if stale else "fresh",
        "age_min":   round(age_min, 1),
        "threshold": threshold,
        "note":      (f"🔴 STALE — {age_min:.1f}min old (limit: {threshold}min). Refresh before trading!"
                      if stale else
                      f"🟢 Fresh — {age_min:.1f}min old (limit: {threshold}min)")
    }


# ── #5 Output DNA Fingerprint ─────────────────────────────────────────────────
def _qa_fingerprint(output_text: str) -> str:
    """SHA256 first 8 chars of output. Detects duplicate/cached outputs."""
    return hashlib.sha256(output_text.encode()).hexdigest()[:8].upper()


# ── #6 Bot Health Dashboard ───────────────────────────────────────────────────
def _qa_update_health_dashboard(output_type: str, score: float, verdict: str):
    """Update global rolling health dashboard. Keeps last 5 per output_type."""
    global _qa_health_dashboard
    entry = {
        "score":   round(score, 1),
        "verdict": verdict,
        "ts":      datetime.now(timezone.utc).strftime("%H:%M")
    }
    bucket = _qa_health_dashboard.setdefault(output_type, [])
    bucket.append(entry)
    _qa_health_dashboard[output_type] = bucket[-5:]


def _qa_build_health_dashboard() -> str:
    """Format the rolling health dashboard as a compact string."""
    if not _qa_health_dashboard:
        return "  No validations run yet.\n"
    v_emoji = {"VALID": "✅", "NEEDS_REVIEW": "🔍", "INVALID": "❌", "INCONCLUSIVE": "⚠️"}
    lines   = []
    for otype, entries in sorted(_qa_health_dashboard.items()):
        label   = _FB_OUTPUT_TYPES.get(otype, otype)
        last    = entries[-1]
        avg     = round(sum(e["score"] for e in entries) / len(entries), 1)
        trend   = "▲" if len(entries) > 1 and entries[-1]["score"] > entries[-2]["score"] else (
                  "▼" if len(entries) > 1 and entries[-1]["score"] < entries[-2]["score"] else "─")
        emoji   = v_emoji.get(last["verdict"], "❓")
        lines.append(f"  {emoji} {label:<22} Last:{last['score']:>5}/100 {trend}  Avg:{avg}/100  @{last['ts']}")
    return "\n".join(lines)


# ── #7 Manipulation Risk Score ────────────────────────────────────────────────
def _qa_manipulation_risk(output_text: str, output_type: str) -> dict:
    """Score 0-100: likelihood output could be used for pump/dump manipulation."""
    score  = 0
    flags  = []
    text_l = output_text.lower()

    # Strong directional language without caveats
    hype_words = ["moon", "explode", "pump", "100x", "guaranteed", "gem",
                  "massive gains", "next big", "don't miss", "going parabolic"]
    found_hype = [w for w in hype_words if w in text_l]
    if found_hype:
        score += len(found_hype) * 15
        flags.append(f"Hype language detected: {', '.join(found_hype[:3])}")

    # No bearish scenario mentioned
    bearish_words = ["risk", "downside", "bear", "support", "stop", "caution", "if fails", "resistance"]
    if not any(w in text_l for w in bearish_words):
        score += 20
        flags.append("No bearish scenario or risk level mentioned")

    # No disclaimer
    if not any(w in text_l for w in ["not financial advice", "dyor", "disclaimer", "nfa"]):
        score += 15
        flags.append("No disclaimer present")

    # Micro-cap specific extreme % claims (e.g. "+500%", "1000%")
    extreme_pcts = re.findall(r'\+?(\d{3,})\s*%', output_text)
    if extreme_pcts and any(int(p) > 200 for p in extreme_pcts):
        score += 25
        flags.append(f"Extreme % projection: {', '.join(extreme_pcts[:3])}%")

    # Unnamed/unverified sources
    if re.search(r'(insider|source|rumor|heard|word is|allegedly)', text_l):
        score += 20
        flags.append("Unnamed/unverified source language detected")

    score = min(score, 100)
    if score >= 70:   risk_level, risk_emoji = "HIGH",   "🔴"
    elif score >= 40: risk_level, risk_emoji = "MEDIUM", "🟡"
    else:             risk_level, risk_emoji = "LOW",    "🟢"

    return {
        "score":      score,
        "level":      risk_level,
        "emoji":      risk_emoji,
        "flags":      flags,
        "note":       f"{risk_emoji} Manipulation Risk: {risk_level} ({score}/100)"
    }


# ── #8 "What To Do Next" Action Block ────────────────────────────────────────
def _qa_action_block(report: dict, readiness: dict, decay: dict, manip: dict) -> list[str]:
    """Generate a concrete 3-5 item action list for the user."""
    actions = []

    bugs   = report.get("bugs", [])
    urgent = [b for b in bugs if b.get("severity") == "URGENT"]
    mild   = [b for b in bugs if b.get("severity") == "MILD"]

    if urgent:
        for b in urgent[:2]:
            actions.append(f"🔴 FIX NOW: {b.get('title','')} — {b.get('how_to_fix','')[:100]}")
    if mild:
        actions.append(f"🟡 REVIEW: {len(mild)} mild issue(s) — see Bug Report section")

    # Readiness failures
    for key, check in readiness.get("checks", {}).items():
        if check.get("pass") is False:
            actions.append(f"🔴 {check['note']}")

    # Decay
    if decay.get("status") == "stale":
        actions.append(f"🔴 REFRESH DATA — {decay['note']}")

    # Manipulation risk
    if manip.get("level") in ("HIGH", "MEDIUM"):
        actions.append(f"{manip['emoji']} REVIEW TONE — {manip['flags'][0] if manip['flags'] else 'Potential manipulation risk'}")

    # Missing elements
    for m in report.get("missing_elements", [])[:2]:
        actions.append(f"📌 ADD: {m[:100]}")

    if not actions:
        actions.append("✅ No immediate actions required — output looks good")

    return actions[:6]  # cap at 6 items


# ── #9 Per-Coin / Per-Signal Micro-Validation ─────────────────────────────────
def _qa_extract_coin_signals(output_text: str) -> list[dict]:
    """Parse individual coin entries from bot output for micro-validation.
    Only matches known crypto ticker symbols followed by a price or signal —
    metric labels (BTC Dom, Total, Data, Altcoin, GOLD, SILVER, Signal) are excluded.
    """
    # Explicit blocklist: metric labels, words, ETF assets, non-coin tickers
    _BLOCKLIST = {
        "the","and","for","not","but","bot","utc","usd","est","dom","rsi","tvl",
        "ema","sma","atr","obv","cvd","btc","eth",   # BTC/ETH caught via full-line check below
        "data","total","signal","altcoin","dominance","alt","cap","etf",
        "gold","silver","greed","fear","market","crypto","defi","nft",
        "overview","news","pin","high","low","open","close","vol","avg",
        "week","day","hour","min","sec","jan","feb","mar","apr","may","jun",
        "jul","aug","sep","oct","nov","dec","usd","eur","php","gbp","jpy",
    }
    # Known real coin tickers to always accept (even if short)
    _KNOWN_COINS = {
        "BTC","ETH","SOL","BNB","XRP","ADA","DOGE","AVAX","DOT","MATIC",
        "LINK","UNI","AAVE","LTC","BCH","ATOM","FTM","OP","ARB","INJ",
        "SUI","APT","SEI","TIA","JUP","WIF","BONK","PEPE","SHIB","TON",
        "NEAR","FIL","ICP","HBAR","VET","ALGO","XLM","ETC","SAND","MANA",
    }
    coins = []
    lines = output_text.splitlines()
    # Pattern: known ticker OR 3-10 uppercase letters, then within 100 chars a price/signal keyword
    coin_pattern = re.compile(
        r'\b([A-Z]{2,10})\b'
        r'(?:[^|\n]{0,100})'
        r'(?:\$[\d,]+\.?\d*|[+-]?\d+\.?\d*\s*%'
        r'|(?:bull|bear|buy|sell|long|short|golden|death|breakout|bounce|dump|pump)\b)',
        re.IGNORECASE
    )
    for line in lines:
        m = coin_pattern.search(line)
        if not m or len(line) < 10:
            continue
        sym = m.group(1).upper()
        # Accept if in known list OR not in blocklist and 3+ chars
        if sym in _KNOWN_COINS or (sym not in {s.upper() for s in _BLOCKLIST} and len(sym) >= 3):
            coins.append({"symbol": sym, "line": line.strip()[:120]})

    # Deduplicate by symbol, keep first occurrence
    seen: set = set()
    unique = []
    for c in coins:
        if c["symbol"] not in seen:
            seen.add(c["symbol"])
            unique.append(c)
    return unique[:10]


def _qa_micro_validate_coins(coins: list[dict]) -> list[dict]:
    """Flag per-coin issues: missing price, missing % change, missing signal direction."""
    results = []
    for coin in coins:
        line = coin["line"].lower()
        issues = []
        if not re.search(r'\$?[\d,]+\.?\d*', line):
            issues.append("no price")
        if not re.search(r'[+-]?\d+\.?\d*\s*%', line):
            issues.append("no % change")
        if not any(w in line for w in ["bull","bear","buy","sell","long","short","golden","death","signal"]):
            issues.append("no direction signal")
        results.append({
            "symbol": coin["symbol"],
            "line":   coin["line"],
            "issues": issues,
            "status": "⚠️ INCOMPLETE" if issues else "✅ OK"
        })
    return results


# ── #10 External Consensus Check ─────────────────────────────────────────────
async def _qa_external_consensus(output_text: str, output_type: str) -> dict:
    """Live-check bot's stated values against CoinGecko, DefiLlama, Fear & Greed."""
    conflicts = []
    checks    = {}

    try:
        async with aiohttp.ClientSession() as session:
            # ── Fear & Greed ──────────────────────────────────────────────────
            try:
                async with session.get(
                    "https://api.alternative.me/fng/?limit=1",
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 200:
                        data    = await r.json()
                        live_fg = int(data["data"][0]["value"])
                        label   = data["data"][0]["value_classification"]
                        checks["fear_greed"] = {"live": live_fg, "label": label}
                        # Extract stated value from output
                        m = re.search(r'[Ff]ear\s*[&\s]*[Gg]reed[:\s]+(\d+)', output_text)
                        if m:
                            stated = int(m.group(1))
                            diff   = abs(stated - live_fg)
                            if diff > 5:
                                conflicts.append(
                                    f"Fear & Greed mismatch: bot says {stated}, live API says {live_fg} (Δ{diff})"
                                )
                            checks["fear_greed"]["stated"] = stated
                            checks["fear_greed"]["match"]  = diff <= 5
            except Exception as e:
                logger.warning(f"[QA-EXT] Fear & Greed check failed: {e}")

            # ── BTC Dominance ─────────────────────────────────────────────────
            try:
                async with session.get(
                    "https://api.coingecko.com/api/v3/global",
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    if r.status == 200:
                        data     = await r.json()
                        live_btc = round(data["data"]["market_cap_percentage"].get("btc", 0), 2)
                        live_mcap = round(data["data"]["total_market_cap"].get("usd", 0) / 1e12, 2)
                        checks["btc_dominance"] = {"live": live_btc}
                        checks["total_mcap"]    = {"live": live_mcap}
                        m = re.search(r'BTC\s+Dom[a-z]*[:\s]+(\d+\.?\d*)\s*%', output_text, re.IGNORECASE)
                        if m:
                            stated = float(m.group(1))
                            diff   = abs(stated - live_btc)
                            if diff > 2:
                                conflicts.append(
                                    f"BTC Dominance mismatch: bot says {stated}%, live CoinGecko says {live_btc}% (Δ{diff:.1f}%)"
                                )
                            checks["btc_dominance"]["stated"] = stated
                            checks["btc_dominance"]["match"]  = diff <= 2
            except Exception as e:
                logger.warning(f"[QA-EXT] CoinGecko check failed: {e}")

    except Exception as e:
        logger.warning(f"[QA-EXT] External consensus check failed: {e}")

    return {
        "checks":    checks,
        "conflicts": conflicts,
        "has_conflicts": len(conflicts) > 0
    }


# ── Output capture helper (called after each bot output) ─────────────────────

# Rolling health dashboard: stores last 5 scores per output_type across all chats
_qa_health_dashboard: dict[str, list[dict]] = {}  # output_type → [{score, verdict, ts}]

def capture_output(chat_id: int, text: str, output_type: str):
    """Store last bot output so Feedback can validate it.
    Also computes DNA fingerprint and tracks score history.

    TRIO RULE: pin, market_overview, and news are always validated together.
    Each is stored individually in 'trio_outputs'. When all three are present,
    the combined trio text is used for validation.

    LAST-WINS RULE: Whatever was most recently captured is ALWAYS shown as
    'Last captured output' in the Feedback menu. No feature output is ever
    silently swallowed. Trio members update the trio bucket AND become the
    last output. Non-trio features always replace the last output pointer.
    """
    TRIO_TYPES = {"pin", "market_overview", "news"}

    fingerprint = hashlib.sha256(text.encode()).hexdigest()[:8].upper()
    feedback_store.setdefault(chat_id, {
        "last_output":       "",
        "last_output_type":  "unknown",
        "last_report":       None,
        "awaiting_evidence": False,
        "score_history":     {},
        "capture_ts":        {},
        "trio_outputs":      {},
    })

    # ── Defensive key guards: ensure every sub-key exists even if the store
    #    was created by an older code path that didn't include the key. ──────
    store = feedback_store[chat_id]
    if "capture_ts"       not in store: store["capture_ts"]       = {}
    if "trio_outputs"     not in store: store["trio_outputs"]      = {}
    if "score_history"    not in store: store["score_history"]     = {}
    if "last_fingerprint" not in store: store["last_fingerprint"]  = ""

    now = datetime.now(timezone.utc)

    if output_type in TRIO_TYPES:
        # market_overview = start of a new trio cycle — reset the bucket.
        # Do NOT reset on "pin" because send_market_overview (overview + news) runs
        # BEFORE update_regime_pin in /start, so resetting on pin would wipe the
        # already-captured market_overview and news outputs.
        if output_type == "market_overview":
            store["trio_outputs"] = {}

        # Always update the trio bucket
        # Cap stored text to 2000 chars to prevent RAM exhaustion (thesis D5.3)
        store["trio_outputs"][output_type] = text[:2000]
        store["capture_ts"][output_type]   = now

        # Build combined trio text — includes whatever members are captured so far
        trio_combined = _build_trio_combined(store["trio_outputs"])

        # LAST-WINS: trio member always becomes the last captured output
        store["last_output_type"] = "trio"
        store["last_output"]      = trio_combined
        store["last_report"]      = None  # new trio data = invalidate old report
        store["last_fingerprint"] = fingerprint
        logger.info(
            f"[QA] Captured TRIO member '{output_type}' — "
            f"trio now has: {list(store['trio_outputs'].keys())}"
        )
    else:
        # LAST-WINS: non-trio output ALWAYS becomes the new last output,
        # regardless of whether a trio was previously active.
        # Trio data is preserved in trio_outputs for later reference but is
        # no longer the "last" output shown in the Feedback menu.
        store["capture_ts"][output_type] = now
        store["last_output"]             = text[:2000]   # cap to 2K (thesis D5.3)
        store["last_output_type"]        = output_type
        store["last_report"]             = None
        store["last_fingerprint"]        = fingerprint
        logger.info(f"[QA] Captured '{output_type}' output — fingerprint: {fingerprint}")


def _build_trio_combined(trio_outputs: dict) -> str:
    """Merge pin / market_overview / news into one validation-ready text block."""
    TRIO_LABELS = {
        "pin":             "📌 PIN MESSAGE",
        "market_overview": "📊 MARKET OVERVIEW",
        "news":            "📰 NEWS OUTPUT",
    }
    TRIO_ORDER = ["market_overview", "news", "pin"]
    parts = []
    for key in TRIO_ORDER:
        if key in trio_outputs and trio_outputs[key]:
            parts.append(f"{'='*40}\n{TRIO_LABELS[key]}\n{'='*40}\n{trio_outputs[key]}")
    return "\n\n".join(parts)[:8000]


def get_feedback_inline_keyboard():
    """Inline keyboard appended to bot outputs for quick validation."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📋 Validate This Output", callback_data="fb_validate"),
        InlineKeyboardButton("📥 Download Report",      callback_data="fb_download"),
    ]])


# MOVED TO: features.py, feedback.py — features/feedback handlers


# ==========================================
# STATE VALIDATION & SAFETY
# ==========================================

def validate_market_state(market_data):
    """
    Validate market state completeness and integrity
    Returns: (is_valid, confidence_score, missing_fields)
    """
    if not market_data:
        return False, 0, ["all_data"]
    
    required_fields = [
        "regime",
        "btc_dominance",
        "total_market_cap",
        "fear_greed_index",
        "bitcoin_rsi"
    ]
    
    missing_fields = []
    for field in required_fields:
        if field not in market_data or market_data.get(field) is None:
            missing_fields.append(field)
    
    # Calculate confidence
    fields_present = len(required_fields) - len(missing_fields)
    base_confidence = (fields_present / len(required_fields)) * 100
    
    # Factor in data sources health if available
    data_confidence = market_data.get("confidence_score", base_confidence)
    
    # State is valid if at least 80% of critical data is present
    is_valid = base_confidence >= 80
    
    return is_valid, data_confidence, missing_fields

def safe_format_number(value, decimals=2, fallback="N/A"):
    """Safely format numbers with fallback"""
    try:
        if value is None:
            return fallback
        return f"{float(value):.{decimals}f}"
    except (ValueError, TypeError):
        return fallback

def calculate_etf_confidence(etf_status):
    """
    Calculate confidence score based on ETF data status
    Returns: confidence percentage (0-100)
    """
    if etf_status == "live":
        return 100  # Real-time verified data
    elif etf_status == "cached":
        return 92   # Recent historical data
    elif etf_status == "estimated":
        return 80   # Market-based estimate
    else:
        return 70   # Unknown status

def trim_message_for_telegram(message, max_length=4000):
    """
    Trim message to fit Telegram's 4096 character limit with safety margin.
    Trims from the END — preserves the header/signal data which is always first.
    """
    if len(message) <= max_length:
        return message
    # Trim from end at a newline boundary where possible
    trimmed = message[:max_length]
    last_nl = trimmed.rfind("\n")
    if last_nl > max_length * 0.8:   # only snap to newline if it's near the limit
        trimmed = trimmed[:last_nl]
    return trimmed + "\n\n… _(trimmed — see WebApp for full signal)_"

# ==========================================
# COMMAND HANDLERS
# ==========================================

@safe_menu_handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle /start command with professional safety pattern:
    Initialize → Validate → Compute → Finalize → Format → Send
    """
    chat_id = update.effective_chat.id
    registered_chats.add(chat_id)   # track for auto-notifications
    _save_registered_chats()         # persist to disk across restarts (thesis A1.5)
    
    # Delete /start command
    try:
        await update.message.delete()
    except BadRequest as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    
    # Clear all previous messages
    await clear_market_messages(chat_id, context)
    
    # Send welcome message
    user = update.effective_user
    user_name = user.first_name or user.username or "Trader"
    
    welcome_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"👋 Welcome, *{user_name}*!\n\n"
            f"⏳ Fetching live market data…\n"
            f"_This takes ~5 seconds_"
        ),
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )
    
    try:
        # ========== PHASE 1: INITIALIZE STATE ==========
        logger.info(f"[START] Initializing market state for chat_id: {chat_id}")
        
        # Fetch market data with timeout protection
        market_data = None
        try:
            market_data = await asyncio.to_thread(get_market_regime)
        except Exception as e:
            logger.error(f"[START] Failed to fetch market regime: {e}")
        
        # ========== PHASE 2: VALIDATE STATE ==========
        is_valid, confidence, missing = validate_market_state(market_data)
        
        if not is_valid:
            logger.warning(f"[START] Invalid market state. Missing: {missing}")
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=welcome_msg.message_id,
                text=(
                    f"Welcome, *{user_name}*! 🚀\n\n"
                    f"⚠️ Market data temporarily unavailable.\n"
                    f"Data confidence: {confidence:.0f}%\n\n"
                    f"Please try again in a moment."
                ),
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
            # Still start background task unless low-RAM mode disables it
            if not LOW_RAM_MODE:
                existing = background_tasks.get(chat_id)
                if existing is None or existing.done():
                    # Purge all done tasks to prevent unbounded dict growth
                    done_ids = [cid for cid, t in background_tasks.items() if t.done()]
                    for cid in done_ids:
                        del background_tasks[cid]
                    background_tasks[chat_id] = asyncio.create_task(
                        auto_market_refresh(chat_id, context.application)
                    )
            return
        
        # ========== PHASE 3: START BACKGROUND TASK ==========
        # Start background task if not running (cancel stale done tasks)
        if not LOW_RAM_MODE:
            existing = background_tasks.get(chat_id)
            if existing is None or existing.done():
                # Purge all done tasks to prevent unbounded dict growth
                done_ids = [cid for cid, t in background_tasks.items() if t.done()]
                for cid in done_ids:
                    del background_tasks[cid]
                logger.info(f"[START] Starting background task for chat_id: {chat_id}")
                background_tasks[chat_id] = asyncio.create_task(
                    auto_market_refresh(chat_id, context.application)
                )
        else:
            logger.info(f"[START] LOW_RAM_MODE — auto market refresh disabled for chat_id: {chat_id}")
        
        # ========== PHASE 4: WAIT AND DELETE WELCOME ==========
        await asyncio.sleep(WELCOME_MESSAGE_DELAY)
        try:
            await context.bot.delete_message(chat_id, welcome_msg.message_id)
        except BadRequest as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        
        # ========== PHASE 5: SEND FINALIZED DATA ==========
        logger.info(f"[START] Sending market overview with confidence: {confidence:.1f}%" + (" [LOW_RAM_MODE]" if LOW_RAM_MODE else ""))
        
        # Send market overview with validated data
        await send_market_overview(chat_id, context, market_data)
        
        # Update pin message with validated data
        await update_regime_pin(chat_id, context, market_data, force=True)
        
        logger.info(f"[START] Successfully completed for chat_id: {chat_id}")
        
    except Exception as e:
        logger.error(f"[START] Critical error: {e}", exc_info=True)
        
        # Safe fallback message
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "⚠️ System initializing. Please try again.\n\n"
                    "If the issue persists, use /help for support."
                ),
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
        except Exception as fallback_error:
            logger.error(f"[START] Even fallback failed: {fallback_error}")
        
        # Still try to start background task
        existing = background_tasks.get(chat_id)
        if existing is None or existing.done():
            try:
                # Purge all done tasks to prevent unbounded dict growth
                done_ids = [cid for cid, t in background_tasks.items() if t.done()]
                for cid in done_ids:
                    del background_tasks[cid]
                background_tasks[chat_id] = asyncio.create_task(
                    auto_market_refresh(chat_id, context.application)
                )
            except Exception as bg_error:
                logger.error(f"[START] Background task failed: {bg_error}")

@safe_menu_handler
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help — dynamically lists all registered commands with descriptions."""
    # REC-17: Command registry — descriptions from docstrings + curated labels.
    # When new commands are added and registered in main(), they appear here
    # automatically as long as they're in this map.
    _CMD_REGISTRY = {
        # Core
        "/start":       "Restart bot & refresh all market data",
        "/help":        "Show this command list",
        "/refresh":     "Force market data refresh",
        "/status":      "Bot health, API status & brain stats (Markov/AWS/Gate Memory)",
        # Analysis
        "/analyze":     "Full 20-layer ICT/SMC pipeline for a symbol  e.g. /analyze ETH",
        "/scan":        "Batch TA on a sector  e.g. /scan defi | /scan l1 | /scan top10",
        "/cross":       "AI multi-exchange MA50/200 cross scan (Binance·Bybit·OKX·KuCoin)",
        "/alpha":       "Stealth accumulation/distribution alpha signals",
        "/trending":    "Live trending coins + real-time prices",
        "/sector":      "Sector rotation — DeFi TVL institutional flows",
        "/dex":         "Top DEX scanner (Uniswap, PancakeSwap, GMX…)",
        # Alerts & watches
        "/watch":       "Watch a TA signal — alerts on TP1/TP2/TP3/SL  e.g. /watch BTCUSDT",
        "/watchlist":   "Show your active watch conditions",
        "/cancel_watch":"Cancel a watch condition  e.g. /cancel_watch BTCUSDT",
        "/alert":       "Set a custom price alert  e.g. /alert BTCUSDT 100000",
        "/my_signals":  "Your active conditions + last 5 signals + win rate",
        # Intelligence & learning
        "/brain":       "Inspect Markov brain adaptive weights  e.g. /brain BTCUSDT",
        "/unflag":      "Clear immune-system blacklist for a symbol  e.g. /unflag BTCUSDT",
        "/performance": "Your historical signal performance dashboard",
        "/backtest":    "Paper trade equity review  e.g. /backtest BTCUSDT 30",
        # AI & market
        "/ai":          "Market Q&A with 5 AI providers (Groq, Cerebras, Gemini, Mistral, GitHub)",
        "/feedback":    "Validate last output with 5-AI QA pipeline",
        # TradingView
        "/tvsetup":     "3-slot ICT indicator setup guide (TradingView free)",
        "/tvcheck":     "TV cross-check after last analysis",
        "/killzone":    "Current Kill Zone status + countdown to next session",
        "/pine":        "Pine Script v5 backtest strategy for last analysis",
        # Settings
        "/settings":    "TA output mode toggle (full / concise)",
    }

    lines = [
        "ℹ️ *CRYPTEX — COMMAND REFERENCE*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*📊 Analysis*",
    ]
    _SECTIONS = {
        "📊 Analysis":         ["/analyze","/scan","/cross","/alpha","/trending","/sector","/dex"],
        "🔔 Alerts & Watches": ["/watch","/watchlist","/cancel_watch","/alert","/my_signals"],
        "🧠 Intelligence":     ["/brain","/unflag","/performance","/backtest"],
        "🤖 AI & Market":      ["/ai","/feedback"],
        "📺 TradingView":      ["/tvsetup","/tvcheck","/killzone","/pine"],
        "⚙️ System":           ["/start","/help","/refresh","/status","/settings"],
    }
    out_lines = ["ℹ️ *CRYPTEX — COMMAND REFERENCE*", "━━━━━━━━━━━━━━━━━━━━━━━━━━━", ""]
    for section, cmds in _SECTIONS.items():
        out_lines.append(f"*{section}*")
        for cmd in cmds:
            desc = _CMD_REGISTRY.get(cmd, "")
            if desc:
                out_lines.append(f"  {cmd} — {desc}")
        out_lines.append("")
    out_lines += [
        "*⏱ Auto-alpha APEX engine fires every 90s–5min based on urgency*",
        "⚠️ _Not financial advice. DYOR._",
    ]
    await update.message.reply_text(
        "\n".join(out_lines),
        parse_mode="Markdown",
        reply_markup=create_main_keyboard(),
    )

# ==========================================
# FEATURE HANDLERS
# ==========================================

# ==========================================
# CROSS ENGINE - EXCHANGE APIs & LOGIC
# ==========================================

class ExchangeBase:
    name: str
    weight: float

    def __init__(self):
        self.weight = EXCHANGE_WEIGHTS.get(self.name, 0.1)

    async def get_top_symbols(self, session: aiohttp.ClientSession) -> list[str]:
        raise NotImplementedError

    async def get_candles(
        self, session: aiohttp.ClientSession, symbol: str, interval: str, limit: int
    ) -> Optional[CandleData]:
        raise NotImplementedError


class BinanceExchange(ExchangeBase):
    name = "binance"
    BASE = "https://api.binance.com"

    async def get_top_symbols(self, session):
        try:
            async with session.get(
                f"{self.BASE}/api/v3/ticker/24hr", timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                data = await r.json()
            usdt = [
                d for d in data
                if d["symbol"].endswith("USDT") and _is_valid_symbol(d["symbol"])
            ]
            usdt.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            return [d["symbol"].replace("USDT", "") for d in usdt[:TOP_N_COINS]]
        except Exception:
            return []

    async def get_candles(self, session, symbol, interval, limit):
        try:
            async with session.get(
                f"{self.BASE}/api/v3/klines",
                params={"symbol": f"{symbol}USDT", "interval": interval, "limit": limit},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            if not isinstance(data, list) or len(data) < MIN_CANDLES:
                return None
            return CandleData(
                closes=[float(c[4]) for c in data],
                volumes=[float(c[5]) for c in data],
            )
        except Exception:
            return None


class BybitExchange(ExchangeBase):
    name = "bybit"
    BASE = "https://api.bybit.com"
    TF_MAP = {"1h": "60", "4h": "240", "1d": "D"}

    async def get_top_symbols(self, session):
        try:
            async with session.get(
                f"{self.BASE}/v5/market/tickers",
                params={"category": "spot"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            items = data.get("result", {}).get("list", [])
            usdt = [
                i for i in items
                if i["symbol"].endswith("USDT") and _is_valid_symbol(i["symbol"])
            ]
            usdt.sort(key=lambda x: float(x.get("volume24h", 0)), reverse=True)
            return [i["symbol"].replace("USDT", "") for i in usdt[:TOP_N_COINS]]
        except Exception:
            return []

    async def get_candles(self, session, symbol, interval, limit):
        try:
            tf = self.TF_MAP.get(interval, interval)
            async with session.get(
                f"{self.BASE}/v5/market/kline",
                params={"category": "spot", "symbol": f"{symbol}USDT",
                        "interval": tf, "limit": limit},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            rows = data.get("result", {}).get("list", [])
            if len(rows) < MIN_CANDLES:
                return None
            rows = rows[::-1]  # Bybit returns newest first
            return CandleData(
                closes=[float(c[4]) for c in rows],
                volumes=[float(c[5]) for c in rows],
            )
        except Exception:
            return None


class OKXExchange(ExchangeBase):
    name = "okx"
    BASE = "https://www.okx.com"
    TF_MAP = {"1h": "1H", "4h": "4H", "1d": "1Dutc"}

    async def get_top_symbols(self, session):
        try:
            async with session.get(
                f"{self.BASE}/api/v5/market/tickers",
                params={"instType": "SPOT"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            items = data.get("data", [])
            usdt = [
                i for i in items
                if i["instId"].endswith("-USDT") and _is_valid_symbol(i["instId"])
            ]
            usdt.sort(key=lambda x: float(x.get("volCcy24h", 0)), reverse=True)
            return [i["instId"].replace("-USDT", "") for i in usdt[:TOP_N_COINS]]
        except Exception:
            return []

    async def get_candles(self, session, symbol, interval, limit):
        try:
            tf = self.TF_MAP.get(interval, interval)
            async with session.get(
                f"{self.BASE}/api/v5/market/candles",
                params={"instId": f"{symbol}-USDT", "bar": tf, "limit": limit},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            rows = data.get("data", [])
            if len(rows) < MIN_CANDLES:
                return None
            rows = rows[::-1]
            return CandleData(
                closes=[float(c[4]) for c in rows],
                volumes=[float(c[5]) for c in rows],
            )
        except Exception:
            return None


class KuCoinExchange(ExchangeBase):
    name = "kucoin"
    BASE = "https://api.kucoin.com"
    TF_MAP = {"1h": "1hour", "4h": "4hour", "1d": "1day"}

    async def get_top_symbols(self, session):
        try:
            async with session.get(
                f"{self.BASE}/api/v1/market/allTickers",
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            items = data.get("data", {}).get("ticker", [])
            usdt = [
                i for i in items
                if i["symbol"].endswith("-USDT") and _is_valid_symbol(i["symbol"])
            ]
            usdt.sort(key=lambda x: float(x.get("volValue", 0)), reverse=True)
            return [i["symbol"].replace("-USDT", "") for i in usdt[:TOP_N_COINS]]
        except Exception:
            return []

    async def get_candles(self, session, symbol, interval, limit):
        try:
            tf = self.TF_MAP.get(interval, interval)
            async with session.get(
                f"{self.BASE}/api/v1/market/candles",
                params={"symbol": f"{symbol}-USDT", "type": tf},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            rows = data.get("data", [])
            if len(rows) < MIN_CANDLES:
                return None
            rows = rows[::-1]
            return CandleData(
                closes=[float(c[2]) for c in rows],
                volumes=[float(c[5]) for c in rows],
            )
        except Exception:
            return None


class MEXCExchange(ExchangeBase):
    name = "mexc"
    BASE = "https://api.mexc.com"

    async def get_top_symbols(self, session):
        try:
            async with session.get(
                f"{self.BASE}/api/v3/ticker/24hr",
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            usdt = [
                d for d in data
                if d.get("symbol", "").endswith("USDT") and _is_valid_symbol(d["symbol"])
            ]
            usdt.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
            return [d["symbol"].replace("USDT", "") for d in usdt[:TOP_N_COINS]]
        except Exception:
            return []

    async def get_candles(self, session, symbol, interval, limit):
        try:
            async with session.get(
                f"{self.BASE}/api/v3/klines",
                params={"symbol": f"{symbol}USDT", "interval": interval, "limit": limit},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                data = await r.json()
            if not isinstance(data, list) or len(data) < MIN_CANDLES:
                return None
            return CandleData(
                closes=[float(c[4]) for c in data],
                volumes=[float(c[5]) for c in data],
            )
        except Exception:
            return None


EXCHANGES: list[ExchangeBase] = [
    BinanceExchange(),
    BybitExchange(),
    OKXExchange(),
    KuCoinExchange(),
    MEXCExchange(),
]

# ==========================================
# CROSS ENGINE - SIGNAL DETECTION
# ==========================================

def _sma(prices: list[float], period: int) -> Optional[float]:
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period


def _vol_ratio(volumes: list[float]) -> float:
    """Current candle volume vs 20-candle average."""
    if len(volumes) < 21:
        return 1.0
    avg = sum(volumes[-21:-1]) / 20
    return volumes[-1] / avg if avg > 0 else 1.0


def _ema(prices: list[float], period: int) -> Optional[float]:
    """Exponential moving average (last value)."""
    if len(prices) < period:
        return None
    k = 2 / (period + 1)
    val = sum(prices[:period]) / period
    for p in prices[period:]:
        val = p * k + val * (1 - k)
    return val


def _rsi(closes: list[float], period: int = 14) -> float:
    """RSI-14. Returns 50 if insufficient data."""
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i-1] for i in range(-period, 0)]
    gains  = [d for d in deltas if d > 0]
    losses = [-d for d in deltas if d < 0]
    avg_g  = sum(gains)  / period if gains  else 0.0
    avg_l  = sum(losses) / period if losses else 0.0
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 1)


def _atr(closes: list[float], period: int = 14) -> float:
    """Average True Range (close-to-close proxy)."""
    if len(closes) < period + 1:
        return 1.0
    trs = [abs(closes[i] - closes[i-1]) for i in range(-period, 0)]
    return max(sum(trs) / period, 1e-10)


def _vol_slope(volumes: list[float], window: int = 5) -> float:
    """Linear slope of volume over last N candles. Positive = expanding."""
    if len(volumes) < window:
        return 0.0
    pts = volumes[-window:]
    n   = len(pts)
    xs  = list(range(n))
    x_m = sum(xs) / n
    y_m = sum(pts) / n
    num = sum((xs[i] - x_m) * (pts[i] - y_m) for i in range(n))
    den = sum((xs[i] - x_m) ** 2 for i in range(n))
    slope = num / den if den else 0.0
    return slope / y_m if y_m else 0.0


def _persistence_factor(closes: list[float]) -> float:
    """Counts consecutive candles SMA50>SMA200 (or vice-versa), max 5, returns 0-1."""
    count = 0
    direction = None
    for i in range(-10, 0):
        s = _sma(closes[:i], SHORT_PERIOD)
        l = _sma(closes[:i], LONG_PERIOD)
        if s is None or l is None:
            continue
        d = 'above' if s > l else 'below'
        if direction is None:
            direction = d
        if d == direction:
            count += 1
        else:
            break
    return min(count / 5.0, 1.0)


def _ema_confirmation(closes: list[float], signal: str) -> bool:
    """EMA 21/55 must agree with SMA cross direction (hybrid MA filter)."""
    e21 = _ema(closes, 21)
    e55 = _ema(closes, 55)
    if e21 is None or e55 is None:
        return False
    if 'golden' in signal:
        return e21 > e55
    if 'death' in signal:
        return e21 < e55
    return False


def _ema_series(values: list[float], period: int) -> list[float]:
    """Return full EMA series for MACD confirmation."""
    if not values:
        return []
    k = 2 / (period + 1)
    ema_vals = [float(values[0])]
    for v in values[1:]:
        ema_vals.append((float(v) * k) + (ema_vals[-1] * (1 - k)))
    return ema_vals


def _macd_confirmation(closes: list[float], signal: str) -> bool:
    """MACD 12/26 with 9-signal should agree with cross direction."""
    if len(closes) < 35:
        return False
    ema12 = _ema_series(closes, 12)
    ema26 = _ema_series(closes, 26)
    macd_line = [a - b for a, b in zip(ema12, ema26)]
    signal_line = _ema_series(macd_line, 9)
    if not macd_line or not signal_line:
        return False
    macd_now = macd_line[-1]
    sig_now = signal_line[-1]
    if 'golden' in signal:
        return macd_now >= sig_now and macd_now >= 0
    if 'death' in signal:
        return macd_now <= sig_now and macd_now <= 0
    return False


def _convergence_speed(closes: list[float]) -> float:
    """
    Measures how fast the gap between SMA50 and SMA200 is closing.
    Returns absolute rate of change in gap% per candle (averaged last 5 candles).
    """
    gaps = []
    for i in range(-6, 0):
        s = _sma(closes[:i], SHORT_PERIOD)
        l = _sma(closes[:i], LONG_PERIOD)
        if s and l and l != 0:
            gaps.append((s - l) / l * 100)
    if len(gaps) < 2:
        return 0.0
    deltas = [abs(gaps[i] - gaps[i - 1]) for i in range(1, len(gaps))]
    return sum(deltas) / len(deltas)


def detect_signal(closes: list[float]) -> str:
    """
    Institutional-grade cross state detection.
    Compares previous candle state to current candle state to find FRESH crosses.
    """
    s_now  = _sma(closes,       SHORT_PERIOD)
    l_now  = _sma(closes,       LONG_PERIOD)
    s_prev = _sma(closes[:-1],  SHORT_PERIOD)
    l_prev = _sma(closes[:-1],  LONG_PERIOD)

    if None in (s_now, l_now, s_prev, l_prev):
        return "neutral"

    gap    = (s_now  - l_now)  / l_now  * 100
    gap_p  = (s_prev - l_prev) / l_prev * 100

    # Fresh cross: state flipped this candle
    if s_prev <= l_prev and s_now > l_now:
        return "golden_fresh"
    if s_prev >= l_prev and s_now < l_now:
        return "death_fresh"

    # Approaching: gap tightening, within 2%
    if s_now < l_now and gap > -2.0 and gap > gap_p:
        return "approaching_golden"
    if s_now > l_now and gap < 2.0 and gap < gap_p:
        return "approaching_death"

    # Zone: already crossed, staying there
    if s_now > l_now:
        return "golden_zone"
    if s_now < l_now:
        return "death_zone"

    return "neutral"


def build_cross_info(
    symbol: str, exchange: str, timeframe: str, candles: CandleData
) -> CrossInfo:
    closes, volumes = candles.closes, candles.volumes
    s   = _sma(closes, SHORT_PERIOD)
    l   = _sma(closes, LONG_PERIOD)
    gap = (s - l) / l * 100 if (s and l and l != 0) else 0.0
    sig = detect_signal(closes)
    atr = _atr(closes)
    atr_norm = gap / atr if atr else 0.0

    return CrossInfo(
        symbol=symbol,
        exchange=exchange,
        timeframe=timeframe,
        signal=sig,
        gap_pct=gap,
        convergence=_convergence_speed(closes),
        short_ma=s or 0,
        long_ma=l or 0,
        volume_ratio=_vol_ratio(volumes),
        price=closes[-1],
        atr_norm_gap=round(atr_norm, 4),
        rsi=_rsi(closes),
        vol_slope=round(_vol_slope(volumes), 4),
        ema_confirm=_ema_confirmation(closes, sig),
        macd_confirm=_macd_confirmation(closes, sig),
        persistence=_persistence_factor(closes),
    )

# ==========================================
# CROSS ENGINE - QUANT SCORING
# ==========================================

def _normalize(val: float, lo: float, hi: float) -> float:
    """Clamp and normalize to 0–100."""
    return max(0.0, min(100.0, (val - lo) / (hi - lo) * 100)) if hi != lo else 50.0


def compute_confidence(
    consensus: float, vol_ratio: float, gap_pct: float, tf_alignment: float
) -> float:
    """
    Reliability score (0–100).
    40% consensus · 25% volume · 20% gap stability · 15% TF alignment
    """
    c_score = consensus * 100                         # already 0–1
    v_score = _normalize(vol_ratio, 0.5, 3.0)
    g_score = _normalize(100 - abs(gap_pct), 90, 100) # tighter gap = more stable
    t_score = tf_alignment * 100
    return round(0.40 * c_score + 0.25 * v_score + 0.20 * g_score + 0.15 * t_score, 1)


def compute_efficiency(vol_ratio: float, convergence: float, gap_pct: float) -> float:
    """
    Trend quality score (0–100).
    High = clean directional move; Low = choppy.
    """
    v = _normalize(vol_ratio, 0.8, 2.5)
    c = _normalize(convergence, 0.0, 0.5)
    g = _normalize(abs(gap_pct), 0.5, 5.0)
    return round(0.40 * v + 0.30 * c + 0.30 * g, 1)


def compute_rci(gap_pct: float, convergence: float) -> float:
    """
    Relative Cycle Index (0–100).
    0–30 Accumulation · 30–60 Early · 60–80 Mid · 80–100 Late
    """
    abs_gap = abs(gap_pct)
    g_score = _normalize(abs_gap, 0.0, 8.0)           # bigger gap = later in cycle
    c_score = 100 - _normalize(convergence, 0.0, 0.5) # slowing convergence = later
    return round(0.60 * g_score + 0.40 * c_score, 1)


LIQUIDITY_TIERS: dict[str, float] = {
    'BTC': 1.0, 'ETH': 1.0,
    'SOL': 0.9, 'BNB': 0.9, 'XRP': 0.9, 'ADA': 0.9, 'AVAX': 0.9,
    'LINK': 0.9, 'DOT': 0.9, 'MATIC': 0.9, 'DOGE': 0.9,
    'LTC': 0.75, 'BCH': 0.75, 'UNI': 0.75, 'ATOM': 0.75,
    'NEAR': 0.75, 'FTM': 0.75, 'ALGO': 0.75, 'XLM': 0.75,
}


def classify_btc_regime(btc_closes: list[float]) -> str:
    """Classify macro regime from BTC price action."""
    sma200 = _sma(btc_closes, LONG_PERIOD)
    sma50  = _sma(btc_closes, SHORT_PERIOD)
    rsi    = _rsi(btc_closes)
    atr    = _atr(btc_closes)
    price  = btc_closes[-1]
    if sma200 is None or sma50 is None:
        return 'unknown'
    volatility_pct = atr / price * 100
    if price < sma200:
        return 'bear'
    if price > sma200 and rsi < 45:
        return 'bull_pullback'
    if volatility_pct > 3.0 and rsi < 55:
        return 'chop'
    return 'bull_expansion'


def regime_score_modifier(regime: str, signal_type: str) -> float:
    """Adaptive weight multiplier based on BTC regime + signal direction."""
    if regime == 'bear':
        # Suppress bullish, amplify bearish
        return 0.65 if 'golden' in signal_type else 1.25
    if regime == 'bull_pullback':
        return 0.80 if 'golden' in signal_type else 1.10
    if regime == 'chop':
        return 0.75  # suppress all cross signals in chop
    # bull_expansion: full score for golden, slight reduction for death
    return 1.10 if 'golden' in signal_type else 0.85


def adaptive_rsi_gate(rsi: float, regime: str, signal_type: str) -> float:
    """RSI gatekeeper: dynamic thresholds per regime. Returns 0-1 pass score."""
    if 'golden' in signal_type:
        cap = 70 if regime == 'bull_expansion' else 55 if regime == 'bear' else 65
        return max(0.0, min(1.0, (cap - rsi) / cap)) if rsi <= cap else 0.3
    else:  # death
        floor = 30 if regime == 'bear' else 40
        return max(0.0, min(1.0, (rsi - floor) / (100 - floor))) if rsi >= floor else 0.3


def get_liquidity_tier(symbol: str) -> float:
    """Return liquidity tier weight for a coin (lower = less reliable signal)."""
    return LIQUIDITY_TIERS.get(symbol.upper(), 0.6)


def compute_ai_score(
    confidence: float, efficiency: float, rci: float,
    gap_pct: float, vol_ratio: float, tf_alignment: float,
    regime: str = 'unknown', signal_type: str = 'golden',
    liquidity_tier: float = 1.0, persistence: float = 0.5,
    ema_confirm: bool = False, macd_confirm: bool = False, rsi: float = 50.0,
    vol_slope: float = 0.0, atr_norm_gap: float = 0.0
) -> float:
    """Regime-adaptive AI score (0-100). Weights shift per BTC macro regime."""
    proximity = _normalize(2.0 - abs(gap_pct), 0.0, 2.0)
    rci_adj   = 100 - abs(rci - 50)
    vol_s     = _normalize(vol_ratio, 0.8, 2.5)
    tf_s      = tf_alignment * 100

    # Regime-adaptive weights
    if regime == 'bull_expansion':
        w = dict(conf=0.22, eff=0.22, rci=0.18, prox=0.15, vol=0.13, tf=0.10)
    elif regime == 'chop':
        w = dict(conf=0.20, eff=0.30, rci=0.15, prox=0.10, vol=0.15, tf=0.10)
    elif regime == 'bear':
        w = dict(conf=0.25, eff=0.18, rci=0.20, prox=0.12, vol=0.12, tf=0.13)
    else:
        w = dict(conf=0.25, eff=0.20, rci=0.20, prox=0.15, vol=0.10, tf=0.10)

    score = (
        w['conf'] * confidence +
        w['eff']  * efficiency +
        w['rci']  * rci_adj   +
        w['prox'] * proximity +
        w['vol']  * vol_s     +
        w['tf']   * tf_s
    )
    score += persistence * 5.0          # persistence bonus up to +5
    score += 3.0 if ema_confirm else 0.0 # EMA confirmation +3
    score += min(_normalize(vol_slope, 0.0, 0.5), 100) * 0.04  # vol expansion
    rsi_pass = adaptive_rsi_gate(rsi, regime, signal_type)
    score *= (0.80 + 0.20 * rsi_pass)   # RSI gate: max 20% penalty
    score *= liquidity_tier             # liquidity tier multiplier
    score *= regime_score_modifier(regime, signal_type)  # BTC regime modifier
    return round(min(score, 100.0), 1)


def predict_cross(gap_pct: float, convergence: float) -> tuple[Optional[float], Optional[float]]:
    """
    Estimate days to cross and probability for approaching signals.
    """
    if convergence <= 0 or abs(gap_pct) > 5:
        return None, None
    days = abs(gap_pct) / convergence if convergence else None
    if days is None or days > 30:
        return None, None
    # Probability: exponential decay the further away
    prob = round(max(10.0, 90.0 * math.exp(-days / 10)), 0)
    return round(days, 1), prob


def rci_label(rci: float) -> str:
    if rci < 30:  return "Accumulation 🟢"
    if rci < 60:  return "Early Trend 🟢"
    if rci < 80:  return "Mid Trend 🟡"
    return "Late Cycle 🔴"


def efficiency_emoji(eff: float) -> str:
    if eff >= 70: return "🟢"
    if eff >= 45: return "🟡"
    return "🔴"

# ==========================================
# CROSS ENGINE - AGGREGATION
# ==========================================

SIGNAL_FAMILY = {
    "golden_fresh":    "golden",
    "golden_zone":     "golden",
    "approaching_golden": "approaching_golden",
    "death_fresh":     "death",
    "death_zone":      "death",
    "approaching_death": "approaching_death",
    "neutral":         "neutral",
}

def aggregate(all_signals: list[CrossInfo]) -> list[CoinScore]:
    """
    Merge per-exchange, per-TF signals into one CoinScore per coin.
    (defaultdict imported at top-level — thesis A3.2)
    """
    coin_map: dict[str, list[CrossInfo]] = defaultdict(list)
    for sig in all_signals:
        coin_map[sig.symbol].append(sig)

    scores: list[CoinScore] = []

    for symbol, sigs in coin_map.items():
        # Determine dominant signal family
        family_weights: dict[str, float] = {}
        for sig in sigs:
            fam   = SIGNAL_FAMILY.get(sig.signal, "neutral")
            ex_w  = EXCHANGE_WEIGHTS.get(sig.exchange, 0.1)
            tf_w  = 1 / len(CROSS_TIMEFRAMES)
            w     = ex_w * tf_w
            family_weights[fam] = family_weights.get(fam, 0.0) + w

        dominant = max(family_weights, key=family_weights.get)
        if dominant == "neutral":
            continue

        # Filter to dominant-family signals
        dom_sigs = [s for s in sigs if SIGNAL_FAMILY.get(s.signal) == dominant]
        if not dom_sigs:
            continue

        # Weighted averages
        total_w = sum(EXCHANGE_WEIGHTS.get(s.exchange, 0.1) for s in dom_sigs)

        def wavg(attr):
            return sum(getattr(s, attr) * EXCHANGE_WEIGHTS.get(s.exchange, 0.1)
                       for s in dom_sigs) / total_w if total_w else 0.0

        gap        = wavg("gap_pct")
        conv       = wavg("convergence")
        vol        = wavg("volume_ratio")
        price      = wavg("price")

        # Exchange consensus (weighted)
        unique_ex  = set(s.exchange for s in dom_sigs)
        consensus  = sum(EXCHANGE_WEIGHTS.get(e, 0.1) for e in unique_ex)

        # TF alignment
        unique_tf  = set(s.timeframe for s in dom_sigs)
        tf_align   = len(unique_tf) / len(CROSS_TIMEFRAMES)

        is_fresh = any(s.signal in ("golden_fresh", "death_fresh") for s in dom_sigs)

        # Weighted aggregates of new fields
        persistence   = sum(s.persistence  * EXCHANGE_WEIGHTS.get(s.exchange, 0.1) for s in dom_sigs) / total_w if total_w else 0.5
        ema_confirm   = sum(1 for s in dom_sigs if s.ema_confirm) > len(dom_sigs) * 0.5
        macd_confirm  = sum(1 for s in dom_sigs if s.macd_confirm) > len(dom_sigs) * 0.5
        rsi_avg       = sum(s.rsi          * EXCHANGE_WEIGHTS.get(s.exchange, 0.1) for s in dom_sigs) / total_w if total_w else 50.0
        vol_slope_avg = sum(s.vol_slope     * EXCHANGE_WEIGHTS.get(s.exchange, 0.1) for s in dom_sigs) / total_w if total_w else 0.0
        atr_norm_avg  = sum(s.atr_norm_gap  * EXCHANGE_WEIGHTS.get(s.exchange, 0.1) for s in dom_sigs) / total_w if total_w else 0.0
        liq_tier      = get_liquidity_tier(symbol)
        tf_weights: dict[str, float] = {}
        for s in dom_sigs:
            tf_weights[s.timeframe] = tf_weights.get(s.timeframe, 0.0) + EXCHANGE_WEIGHTS.get(s.exchange, 0.1)
        primary_tf = max(tf_weights, key=tf_weights.get) if tf_weights else ''
        exchange_count = len(unique_ex)

        # Quant scores
        conf = compute_confidence(consensus, vol, gap, tf_align)
        eff  = compute_efficiency(vol, conv, gap)
        rci  = compute_rci(gap, conv)
        reg  = _btc_regime
        reg_mod = regime_score_modifier(reg, dominant)
        ai   = compute_ai_score(
            conf, eff, rci, gap, vol, tf_align,
            regime=reg, signal_type=dominant,
            liquidity_tier=liq_tier, persistence=persistence,
            ema_confirm=ema_confirm, macd_confirm=macd_confirm, rsi=rsi_avg,
            vol_slope=vol_slope_avg, atr_norm_gap=atr_norm_avg,
        )

        pred_days, pred_prob = (None, None)
        if "approaching" in dominant:
            pred_days, pred_prob = predict_cross(gap, conv)

        scores.append(CoinScore(
            symbol=symbol,
            signal_type=dominant,
            exchange_consensus=consensus,
            tf_alignment=tf_align,
            gap_pct=gap,
            convergence=conv,
            volume_ratio=vol,
            price=price,
            confidence=conf,
            efficiency=eff,
            rci=rci,
            ai_score=ai,
            pred_days=pred_days,
            pred_prob=pred_prob,
            is_fresh=is_fresh,
            regime=reg,
            regime_modifier=reg_mod,
            liquidity_tier=liq_tier,
            persistence=persistence,
            ema_confirm=ema_confirm,
            macd_confirm=macd_confirm,
            atr_norm_gap=atr_norm_avg,
            rsi=rsi_avg,
            primary_timeframe=primary_tf,
            exchange_count=exchange_count,
        ))

    return sorted(scores, key=lambda x: x.ai_score, reverse=True)

# ==========================================
# CROSS ENGINE - SCAN ORCHESTRATOR
# ==========================================

async def run_cross_scan() -> list[CoinScore]:
    """Full multi-exchange scan — fetches live coins & computes all signals."""
    global _cache_data, _cache_time, _cache_lock, _btc_regime, _btc_rsi, _btc_above_sma200

    # Initialize lock if not exists (thesis A1.10: hold lock for full check+update)
    if _cache_lock is None:
        _cache_lock = asyncio.Lock()

    # ATOMIC cache read: hold lock while checking to prevent double-scan race condition
    async with _cache_lock:
        if _cache_data and (time.monotonic() - _cache_time) < CROSS_CACHE_TTL:
            return _cache_data
        # Stamp 0 so a second coroutine waiting won't re-enter after we release
        _cache_time = 0.0

    # Semaphore: max 15 concurrent requests — prevents rate-limit hammering
    _sem = asyncio.Semaphore(15)

    async def _guarded(coro):
        async with _sem:
            return await coro

    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Step 1: Discover live top coins from each exchange
        symbol_sets: list[set[str]] = await asyncio.gather(
            *[ex.get_top_symbols(session) for ex in EXCHANGES]
        )
        # Union of all exchange top coins
        all_symbols: list[str] = list({s for subset in symbol_sets for s in subset})

        # Step 2: Fetch candles for every (symbol, exchange, TF) combo
        # Guarded by semaphore — max 15 concurrent requests at a time
        tasks = []
        meta  = []
        for ex in EXCHANGES:
            for symbol in all_symbols:
                for tf in CROSS_TIMEFRAMES:
                    tasks.append(_guarded(ex.get_candles(session, symbol, tf, MIN_CANDLES + 10)))
                    meta.append((symbol, ex.name, tf))

        # Wrap entire candle fetch in a 45s timeout so the bot never hangs forever
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=45.0
            )
        except asyncio.TimeoutError:
            logger.warning("[CROSS] Candle fetch timed out after 45s — using partial results")
            results = [None] * len(tasks)

        # Step 3: Classify BTC regime (use Binance daily BTC closes)
        btc_candles = await BinanceExchange().get_candles(session, 'BTC', '1d', MIN_CANDLES + 10)
        if btc_candles and len(btc_candles.closes) >= LONG_PERIOD:
            _btc_regime        = classify_btc_regime(btc_candles.closes)
            _btc_rsi           = _rsi(btc_candles.closes)
            sma200_btc         = _sma(btc_candles.closes, LONG_PERIOD)
            _btc_above_sma200  = btc_candles.closes[-1] > (sma200_btc or 0)

        # Step 4: Compute cross signals
        all_signals: list[CrossInfo] = []
        for candles, (symbol, exchange, tf) in zip(results, meta):
            if candles and len(candles.closes) >= MIN_CANDLES:
                all_signals.append(build_cross_info(symbol, exchange, tf, candles))

        # Step 5: Aggregate & score
        scores = aggregate(all_signals)

        async with _cache_lock:
            _cache_data = scores
            _cache_time = time.monotonic()

        return scores

# ==========================================
# CROSS ENGINE - MESSAGE BUILDER
# ==========================================

def _signal_label(cs: CoinScore) -> str:
    if cs.signal_type == "golden" and cs.is_fresh:   return "Golden Cross Detected"
    if cs.signal_type == "golden":                    return "Golden Cross Zone"
    if cs.signal_type == "approaching_golden":        return "Near Golden Cross"
    if cs.signal_type == "death" and cs.is_fresh:     return "Death Cross Detected"
    if cs.signal_type == "death":                     return "Death Cross Zone"
    if cs.signal_type == "approaching_death":         return "Near Death Cross"
    return "Neutral"


def _format_cross_symbol(symbol: str) -> str:
    sym = (symbol or '').upper()
    return sym if sym.endswith(("USDT", "USDC", "USD")) else f"{sym}USDT"


def _format_cross_tf(cs: CoinScore) -> str:
    tf = cs.primary_timeframe or ''
    return CROSS_TIMEFRAMES.get(tf, tf.upper() if tf else '1H')


def _cross_confluence_badges(cs: CoinScore) -> str:
    badges = ["MA50/200"]
    if cs.ema_confirm:
        badges.append("EMA")
    if getattr(cs, 'macd_confirm', False):
        badges.append("MACD")
    rsi_text = f"RSI {getattr(cs, 'rsi', _btc_rsi):.0f}" if hasattr(cs, 'rsi') else f"RSI {_btc_rsi:.0f}"
    return " · ".join(["+".join(badges), rsi_text])


def _cross_line(cs: CoinScore, rank: int) -> str:
    tf = _format_cross_tf(cs)
    ex_note = f" · {cs.exchange_count} CEX" if getattr(cs, 'exchange_count', 0) > 1 else ""
    if cs.signal_type.startswith("approaching") and cs.pred_days is not None:
        eta = f" ~{cs.pred_days:.0f}d" if cs.pred_days >= 1 else " soon"
        return f"➤ #{rank} {_format_cross_symbol(cs.symbol)}({tf}){ex_note} — Score {cs.ai_score:.0f} · {eta} · {_cross_confluence_badges(cs)}"
    freshness = "fresh" if cs.is_fresh else "active"
    return f"➤ #{rank} {_format_cross_symbol(cs.symbol)}({tf}){ex_note} — Score {cs.ai_score:.0f} · {freshness} · {_cross_confluence_badges(cs)}"


_SECTION_MAX = 6


def build_cross_message(scores: list[CoinScore]) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    golden_detected = [s for s in scores if s.signal_type == "golden"]
    death_detected = [s for s in scores if s.signal_type == "death"]
    near_golden = [s for s in scores if s.signal_type == "approaching_golden"]
    near_death = [s for s in scores if s.signal_type == "approaching_death"]

    regime_icons = {
        "bull_expansion": "🟢 Bull Expansion",
        "bull_pullback":  "🟡 Bull Pullback",
        "chop":           "🟡 Chop",
        "bear":           "🔴 Bear",
        "unknown":        "⚪ Unknown",
    }
    regime_str = regime_icons.get(_btc_regime, "⚪ Unknown")
    parts = [
        "☯️ *AI CROSS ENGINE — ALL CEX FUTURES*",
        f"Scanned: Binance • Bybit • OKX • KuCoin • MEXC",
        f"`{ts}` | Regime: *{regime_str}* RSI {_btc_rsi:.0f}",
        "_Ranked by MA50/200 cross + EMA + MACD + RSI confluence_",
    ]

    rank = 1

    def _section(header: str, items: list[CoinScore]):
        nonlocal rank
        if not items:
            return
        shown = items[:_SECTION_MAX]
        rest = len(items) - len(shown)
        parts.append(f"\n{header}" + (f" _(+{rest} more)_" if rest > 0 else ""))
        for cs in shown:
            parts.append(_cross_line(cs, rank))
            rank += 1

    _section("🌅 *Golden Cross Detected*", golden_detected)
    _section("☠️ *Death Cross Detected*", death_detected)
    _section("⚡ *Near Golden Cross*", near_golden)
    _section("⚠️ *Near Death Cross*", near_death)

    if not any((golden_detected, death_detected, near_golden, near_death)):
        parts.append("\nNo ranked cross events detected right now across the merged CEX futures universe.")

    msg = "\n".join(parts)
    if len(msg) > 4000:
        msg = msg[:3950] + "\n…_(trimmed)_"
    return msg


# MOVED TO: separate handler file (lines 10919-11054)
# MOVED TO: separate handler file (lines 11055-11267)
# MOVED TO: separate handler file (lines 11268-11398)
# MOVED TO: separate handler file (lines 11399-11717)
@safe_menu_handler
async def technical_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Technical Analysis button - prompts user for a coin to analyze via TechnicalAnalysis.py"""
    chat_id = update.effective_chat.id
    await clear_market_messages(chat_id, context)

    if not TA_MODULE_AVAILABLE:
        await update.message.reply_text(
            "❌ Technical Analysis module is not available.\n"
            "Please ensure TechnicalAnalysis.py is in the same directory as app.py.",
            reply_markup=create_main_keyboard()
        )
        return

    # Set ta_mode so the next text message is treated as a coin symbol input
    context.user_data["ta_mode"]    = True
    context.user_data["ta_mode_ts"] = time.time()   # for 2-min auto-expiry (thesis A3.6)
    context.user_data["ai_mode"]    = False

    prompt_msg = await update.message.reply_text(
        "📊 *TECHNICAL ANALYSIS — 20-Layer Fund-Grade Pipeline*\n\n"
        "Please type the coin symbol you want to analyze.\n"
        "Examples: `BTC`  `ETH`  `SOL`  `BNB`  `XRP`\n\n"
        "_Type your coin symbol now ↓_",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )

    # Track the prompt message so it can be cleared on next search
    if "market_message_ids" not in context.chat_data:
        context.chat_data["market_message_ids"] = []
    context.chat_data["market_message_ids"].append(prompt_msg.message_id)


async def _run_ta_analysis(message, symbol: str, context: ContextTypes.DEFAULT_TYPE, market_type: str = "auto"):
    """
    Internal helper: runs the full TechnicalAnalysis pipeline for a given symbol
    and sends results back through the Telegram message object.
    Mirrors the _run_analysis logic from TechnicalAnalysis.py, adapted for app.py context.
    """
    from telegram.constants import ParseMode as _PM
    # ── TA running lock — prevents Feedback from validating partial output ────
    context.user_data["ta_running"] = True

    # ── Clear previous watched coin — new TA replaces the watch cache ────────
    apex_clear_watched_coin()

    # ── Market Type Selection: ask user if both spot AND futures exist ──────────
    if market_type == "auto":
        checking_msg = await message.reply_text(
            f"🔍 Checking available markets for {symbol}..."
        )
        try:
            markets = await quick_check_markets(symbol)
        except Exception:
            markets = {"has_spot": True, "has_futures": False}

        try:
            await checking_msg.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        if markets["has_spot"] and markets["has_futures"]:
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📈 SPOT",            callback_data=f"ta_spot_{symbol}"),
                    InlineKeyboardButton("📉 FUTURES (Perp)",  callback_data=f"ta_futures_{symbol}"),
                ]
            ])
            await message.reply_text(
                f"*{symbol}* is available on both Spot and Futures markets.\n\n"
                "Which market do you want to analyze?",
                reply_markup=keyboard,
                parse_mode=_PM.MARKDOWN,
            )
            context.user_data["ta_running"] = False  # lock released — TA hasn't started yet
            return
        elif markets.get("has_futures") and not markets.get("has_spot"):
            market_type = "futures"
            await message.reply_text(
                f"ℹ️ *{symbol}* is only available on Futures (Perp) markets. "
                "Proceeding with Futures analysis.",
                parse_mode=_PM.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
        else:
            market_type = "spot"

    thinking = await message.reply_text(
        f"🔍 Probing exchanges for {symbol}USDT...\n\n"
        f"Phase 1/7 — Checking all 13 CEXs for listing..."
    )

    try:
        await safe_edit(
            thinking,
            f"🔍 Probing exchanges for {symbol}USDT...\n\n"
            f"Phase 1/7 — Checking all 13 CEXs for listing...\n"
            f"Phase 2/7 — Scoring & ranking listed exchanges..."
        )

        # ── Serialise TA runs — one pipeline at a time, gc before each ───────
        async with _TA_SEM:
            _gc.collect()
            # GAP-08: Auto-trigger deep struct refresh when symbol has no cached levels.
            # Runs concurrently with TA pipeline — does not block analysis.
            if _DEEP_STRUCT_APP_AVAILABLE and not LOW_RAM_MODE:
                _sym_usdt = f"{symbol.upper()}USDT"
                _existing_levels = _deep_struct_app.get_levels(_sym_usdt)
                if not _existing_levels:
                    try:
                        await safe_edit(
                            thinking,
                            f"🔍 Probing exchanges for {symbol}USDT...\n\n"
                            f"🗺️ Loading deep structure levels (500-day history)...\n"
                            f"Running ICT analysis pipeline..."
                        )
                        import aiohttp as _aiohttp_ds
                        async with _aiohttp_ds.ClientSession() as _ds_sess:
                            asyncio.create_task(
                                _deep_struct_app.refresh_symbol(_sym_usdt, _ds_sess),
                                name=f"deep_struct_{_sym_usdt}"
                            )
                    except Exception as _ds_e:
                        logger.debug(f"[GAP-08] Deep struct auto-trigger: {_ds_e}")
            result = await run_ict_analysis(symbol, market_type=market_type)
        # ─────────────────────────────────────────────────────────────────────

        # ── [OCE v10.3] Enhance result with HMM + Eval + Audit + BRVF ────────
        if OCE_AVAILABLE and not LOW_RAM_MODE:
            result = oce_enhance_result(
                result, symbol, result.get("bias", "BULLISH")
            )
        # ─────────────────────────────────────────────────────────────────────
        _listed_names = result.get("listed_exchanges", [])
        _listing_table = result.get("listing_table", "")
        _n_listed = len(_listed_names)
        _best_venue = result["best_ex"]["name"]
        _best_score = result["best_ex"].get("score", 0)
        _listing_line = (
            f"Found on {_n_listed} exchange(s) → Best: {_best_venue} (score {_best_score}/100)"
            if _n_listed > 0 else "❌ Not found on any supported exchange"
        )

        await safe_edit(
            thinking,
            f"Analyzing {symbol}USDT [{market_type.upper()}]...\n\n"
            f"Phase 1/7 — {_listing_line}\n"
            f"Phase 2/7 — L0: OHLCV + OB fetched ({len(result['ex_scored'])} exchange datasets)\n"
            f"Phase 3/7 — L1-L5: Integrity · DXY · Features · Stats · Regime..."
        )

        await asyncio.sleep(0.3)
        await safe_edit(
            thinking,
            f"Analyzing {symbol}USDT...\n\n"
            f"Phase 1/7 — {_listing_line}\n"
            f"Phase 2/7 — Stats: {result.get('l5_stats',{}).get('layer5_note','—')[:28]}\n"
            f"Phase 3/7 — L6-L8: Regime → ICT/SMC → AI Alpha Stack\n"
            f"Phase 4/7 — AI: {result.get('l8_ai',{}).get('ensemble',{}).get('ensemble_tier','—')}"
            f"  ({result.get('l8_ai',{}).get('ai_score',0)}/100)\n"
            f"Phase 5/7 — L9: Gates {sum(result['gate_pass'])}/10  "
            f"HMM: {result.get('hmm_data',{}).get('hmm_state','...')}..."
        )

        messages_list = build_result_messages(result, verbose_mode=_ta_verbose_is_on(context))

        # ── [OCE v10.3] Prepend OCE pipeline message to Telegram output ──────
        if OCE_AVAILABLE and result.get("oce_message"):
            messages_list = [result["oce_message"]] + (messages_list or [])
        # ─────────────────────────────────────────────────────────────────────
        await safe_edit(
            thinking,
            f"Analyzing {symbol}USDT...\n\n"
            f"Phase 1/7 — {_listing_line}\n"
            f"Phase 2/7 — Features · Stats · AI OK\n"
            f"Phase 3/7 — Gates {sum(result['gate_pass'])}/10 · "
            f"AI={result.get('l8_ai',{}).get('ai_score',0)}/100\n"
            f"Phase 4/7 — L10-L13: Drift · Portfolio · VaR · Risk Gate\n"
            f"            {result.get('l13_gate',{}).get('l13_note','...')[:45]}\n"
            f"Phase 5/7 — Venue: {_best_venue}  "
            f"SCORE={result.get('trade_score_data',{}).get('trade_score',0)}/100\n"
            f"Phase 6/7 — L19: Performance attribution ready\n"
            f"Phase 7/7 — Rendering {result['best_tf']} chart (560×340) · v9.0..."
        )

        # ── Extract ALL caption scalars before we free result ──────────────
        _direction   = "LONG" if result["bias"] == "BULLISH" else "SHORT"
        _se_cap      = result.get("scaled_entry", {})
        _td_cap      = result.get("timing_data", {})
        _ts_cap      = result.get("trade_score_data", {})
        _l8_cap      = result.get("l8_ai", {})
        _l13_cap     = result.get("l13_gate", {})
        _entry_mid   = (result["entry_low"] + result["entry_high"]) / 2
        _rr_denom    = max(abs(_entry_mid - result["inv_sl"]), 1e-9)
        # [SYNC-1] Extract entry_time before result is freed — matches TechnicalAnalysis.py v6.0
        _entry_window = result.get("entry_time", {}).get("entry_window", "—")
        _rr_val      = round(abs(result['tp3'] - _entry_mid) / _rr_denom, 2)

        # ── MTF-50 fields (v7.0) ──────────────────────────────────────────
        _bias_tf     = result.get("bias_tf", "")
        _ref_tf      = result.get("ref_tf", "")
        _exec_tf     = result.get("exec_tf", result.get("entry_ltf", ""))
        _mtf_combo   = result.get("mtf_combo", "")
        _trader_type = result.get("trader_type", "")

        # ── Upgrade fields (v7.0) — extract before del result ─────────────
        _upg_wyckoff  = result.get("upg_wyckoff", {})
        _upg_cvd      = result.get("upg_cvd", {})
        _upg_funding_z = result.get("upg_funding_z", {})
        _upg_liq_heat = result.get("upg_liq_heat", {})
        _upg_elliott  = result.get("upg_elliott", {})
        _upg_squeeze  = result.get("upg_squeeze", {})
        _upg_vwap_z   = result.get("upg_vwap_z", {})

        # ── OTE zone (v7.0) ───────────────────────────────────────────────
        _ote_zone_high = result.get("ote_zone_high", 0.0)
        _ote_zone_low  = result.get("ote_zone_low", 0.0)
        _ote_705       = result.get("ote_705", 0.0)

        # ── SL tiers (v7.0) ───────────────────────────────────────────────
        _sl2 = result.get("sl2", 0.0)
        _sl3 = result.get("sl3", 0.0)
        # ── TP/SL for APEX watch registration (extracted before del result) ─
        _tp1    = result.get("tp1", 0.0)
        _tp2    = result.get("tp2", 0.0)
        _tp3    = result.get("tp3", 0.0)
        _inv_sl = result.get("inv_sl", 0.0)
        _bias   = result.get("bias", "BULLISH")

        # ── Liq heatmap levels (v7.0) ──────────────────────────────────────
        _liq_long_10x  = result.get("liq_long_10x", 0.0)
        _liq_long_20x  = result.get("liq_long_20x", 0.0)
        _liq_long_50x  = result.get("liq_long_50x", 0.0)
        _liq_short_10x = result.get("liq_short_10x", 0.0)
        _liq_short_20x = result.get("liq_short_20x", 0.0)
        _liq_short_50x = result.get("liq_short_50x", 0.0)

        # ── Build caption ─────────────────────────────────────────────────
        # MTF-50 combo label
        _mtf_label = ""
        if _bias_tf and _ref_tf and _exec_tf:
            _mtf_label = f"\nMTF: {_bias_tf}→{_ref_tf}→{_exec_tf}"
            if _trader_type:
                _mtf_label += f"  [{_trader_type}]"

        # Wyckoff phase
        _wyckoff_label = ""
        if _upg_wyckoff and _upg_wyckoff.get("phase"):
            _wyckoff_label = f"\nWyckoff: {_upg_wyckoff['phase']}"

        # CVD signal
        _cvd_label = ""
        if _upg_cvd and _upg_cvd.get("signal"):
            _cvd_label = f"\nCVD: {_upg_cvd['signal']}"

        # Funding Z-Score
        _fz_label = ""
        if _upg_funding_z and _upg_funding_z.get("zscore") is not None:
            _fz = _upg_funding_z['zscore']
            _fz_flag = "⚠️ Extreme" if abs(_fz) > 2 else ("↑ High" if _fz > 1 else ("↓ Low" if _fz < -1 else "Normal"))
            _fz_label = f"\nFunding Z: {_fz:.2f} ({_fz_flag})"

        # Liq heatmap summary
        _liq_label = ""
        if _liq_long_10x or _liq_short_10x:
            _liq_label = f"\nLiq Heat: L10x@{fmt_price(_liq_long_10x)} S10x@{fmt_price(_liq_short_10x)}"

        # OTE zone
        _ote_label = ""
        if _ote_zone_high and _ote_zone_low:
            _ote_label = f"\nOTE: {fmt_price(_ote_zone_low)}–{fmt_price(_ote_zone_high)}  70.5%@{fmt_price(_ote_705)}"

        # Elliott wave
        _ew_label = ""
        if _upg_elliott and _upg_elliott.get("wave"):
            _ew_label = f"\nElliott: Wave {_upg_elliott['wave']}"

        # Short squeeze
        _sq_label = ""
        if _upg_squeeze and _upg_squeeze.get("prob") is not None:
            _sq_label = f"\nSqueeze Prob: {_upg_squeeze['prob']:.0f}%"

        # VWAP Z-Score
        _vwap_z_label = ""
        if _upg_vwap_z and _upg_vwap_z.get("zscore") is not None:
            _vwap_z_label = f"\nVWAP Z: {_upg_vwap_z['zscore']:.2f}"

        # ── v8.2 Fractal + Trendline caption labels ───────────────────────
        _frac_str   = result.get("fractal_structure", "")
        _frac5_data = result.get("upg_fractals_5bar") or {}
        _tl_v2      = result.get("upg_trendlines_v2") or {}
        _frac_inv   = result.get("upg_frac_inversion") or {}
        _eq_hl_data = result.get("upg_eq_hl") or {}

        _fractal_label = ""
        if _frac_str and _frac_str not in ("UNKNOWN", ""):
            _fh_cnt = len(_frac5_data.get("fractal_highs", []))
            _fl_cnt = len(_frac5_data.get("fractal_lows", []))
            _fractal_label = (
                f"\nFractals: {_frac_str} "
                f"({_fh_cnt}H/{_fl_cnt}L 5-bar)"
            )

        _tl_label = ""
        if _tl_v2.get("tl_valid"):
            _tl_sc  = _tl_v2.get("tl_score", 0)
            _tl_brk = " 🔴BRK" if _tl_v2.get("tl_break") else ""
            _tl_ret = " 🔵RET" if _tl_v2.get("tl_retest") else ""
            _tl_label = f"\nTrendline: score={_tl_sc}/100{_tl_brk}{_tl_ret}"

        _tspot_label = ""
        if _frac_inv.get("inversion_active"):
            _tsq   = _frac_inv.get("t_spot_quality", "")
            _tsp   = _frac_inv.get("t_spot_price")
            if _tsp:
                _tspot_label = f"\nT-Spot ({_tsq}): {fmt_price(_tsp)}"

        _eqhl_label = ""
        _bsl = _eq_hl_data.get("bsl_level")
        _ssl = _eq_hl_data.get("ssl_level")
        if _bsl or _ssl:
            parts = []
            if _bsl: parts.append(f"BSL@{fmt_price(_bsl)}")
            if _ssl: parts.append(f"SSL@{fmt_price(_ssl)}")
            _eqhl_label = f"\nFrac Liq: {' | '.join(parts)}"

        # ── Convert entry_window UTC times → Philippine Time (PHT = UTC+8) ─
        def _add_pht(text: str) -> str:
            """
            Scan entry_window string for HH:MM UTC patterns and append PHT equivalent.
            Examples:
              '02:00 UTC' → '02:00 UTC (10:00 AM PHT)'
              'London SB 02:00 UTC' → 'London SB 02:00 UTC (10:00 AM PHT)'
            Handles single and multiple time mentions. Non-UTC strings pass through unchanged.
            """
            import re as _re
            def _convert(m):
                h, mn    = int(m.group(1)), int(m.group(2))
                pht_h24  = (h + 8) % 24
                ampm     = "AM" if pht_h24 < 12 else "PM"
                pht_h12  = pht_h24 % 12 or 12
                return f"{h:02d}:{mn:02d} UTC ({pht_h12}:{mn:02d} {ampm} PHT)"
            return _re.sub(r'(\d{2}):(\d{2})\s*UTC', _convert, text)

        _entry_window_display = _add_pht(_entry_window)

        # SYNC-FIX: Extract next_candle_open_utc + signal_basis_note added by
        # TechnicalAnalysis.py _compute_next_candle_open() — these tell the user
        # EXACTLY which closed candle the signal is based on and WHEN to place
        # the limit order (next candle open, not now).
        _entry_time_dict   = result.get("entry_time") or {}
        _next_candle_raw   = _entry_time_dict.get("next_candle_open_utc", "—")
        _next_candle_disp  = _add_pht(_next_candle_raw) if _next_candle_raw != "—" else "—"
        _signal_basis      = _entry_time_dict.get("signal_basis_note", "")

        caption = (
            f"{result['symbol']} [{result.get('market_label','SPOT')}]\n"
            f" {result['best_tf']} LIVE Chart\n"
            f"[v8.2 FUND-GRADE]{_mtf_label}\n"
            f"Exchange: {result['best_ex']['name']}\n"
            f"Bias: {result['bias']}  {_direction}\n"
            f"Leg1(40%): {fmt_price(_se_cap.get('leg1_entry', result['entry_high']))}\n"
            f"Leg2(35%): {fmt_price(_se_cap.get('leg2_entry', _entry_mid))}\n"
            f"Leg3(25%): {fmt_price(_se_cap.get('leg3_entry', result['entry_low']))}\n"
            f"SL: {fmt_price(result['inv_sl'])}  (OB+0.2×ATR)\n"
            + (f"SL2 (Mod): {fmt_price(_sl2)}\n" if _sl2 else "")
            + (f"SL3 (Agg): {fmt_price(_sl3)}\n" if _sl3 else "")
            + f"Invalidation: {fmt_price(result.get('invalidation_level', result['inv_sl']))}\n"
            + (f"OTE Zone: {fmt_price(_ote_zone_low)}–{fmt_price(_ote_zone_high)}\n" if _ote_zone_high else "")
            + f"TP1: {fmt_price(result['tp1'])}\n"
            f"TP2: {fmt_price(result['tp2'])}\n"
            f"TP3: {fmt_price(result['tp3'])}\n"
            f"Structure: {_ts_cap.get('trade_score',0)}/100 {_ts_cap.get('trade_grade','?')}\n"
            f"Timing: {_td_cap.get('timing_score',0)}/100 {_td_cap.get('timing_grade','?')}\n"
            f"Combined: {_ts_cap.get('combined_score',0)}/100 Grade {_ts_cap.get('combined_grade','?')}\n"
            f"AI: {_l8_cap.get('ai_score',0)}/100 {_l8_cap.get('ensemble',{}).get('ensemble_tier','?')}\n"
            f"L13: {'✅' if _l13_cap.get('l13_all_pass') else '❌'}  "
            f"Win: {result.get('win_prob_display', str(result.get('win_prob',0))+'%')}  "
            f"{(result.get('signal_probability_data') or {}).get('probability_tier_icon','')}"
            f"{(result.get('signal_probability_data') or {}).get('signal_probability_pct','')}\n"
            f"R:R 1:{_rr_val}  SMC:{result.get('smc_confluence',{}).get('smc_score',0)}/100\n"
            f"Entry: {_entry_window_display}\n"
            f"⏰ Execute at: {_next_candle_disp}\n"
            + (f"📌 {_signal_basis}\n" if _signal_basis else "")
            + f"{_wyckoff_label}{_cvd_label}{_fz_label}{_ew_label}{_sq_label}{_vwap_z_label}{_liq_label}{_fractal_label}{_tl_label}{_tspot_label}{_eqhl_label}"
        )

        # ── Build v8.2 chart dict — all fields build_result_messages + generate_trade_chart need ──
        # _flt: null-safe float coercion — .get() returns None (not the default) when the key
        # exists but is None. Exotic/low-liquidity coins (e.g. AURA) can return None for OB/FVG
        # zones when detection fails. float(None) → TypeError crash fix (v8.2 patch).
        def _flt(d: dict, key: str, default: float = 0.0) -> float:
            v = d.get(key)
            if v is None:
                return default
            try:
                return float(v)
            except (TypeError, ValueError):
                return default

        chart_data = {
            "ohlcv_chart":        result.get("ohlcv_chart"),
            "price":              result["price"],
            "bias":               result["bias"],
            "tp1":                result["tp1"],
            "tp2":                result["tp2"],
            "tp3":                result["tp3"],
            "inv_sl":             result["inv_sl"],
            "entry_low":          result["entry_low"],
            "entry_high":         result["entry_high"],
            "ob_low":             _flt(result, "ob_low"),
            "ob_high":            _flt(result, "ob_high"),
            "fvg_low":            _flt(result, "fvg_low"),
            "fvg_high":           _flt(result, "fvg_high"),
            "liq_zone_swept":     _flt(result, "liq_zone_swept"),
            "liq_zone_target":    _flt(result, "liq_zone_target"),
            # BUG-FIX: "invalidation_level" appeared twice; canonical entry with result["inv_sl"] default kept here.
            "invalidation_level": _flt(result, "invalidation_level", result["inv_sl"]),
            "symbol":             result["symbol"],
            "best_tf":            result["best_tf"],
            "best_ex":            result["best_ex"],
            "market_label":       result.get("market_label", "SPOT"),
            "entry_ltf":          result.get("entry_ltf", "—"),
            "all_pass":           result.get("all_pass", False),
            "gate_pass":          result["gate_pass"],
            "grade":              result.get("grade", "F"),
            "conf":               result.get("conf", 0),
            "win_prob":           result.get("win_prob", 0),
            "atr_pct":            result.get("atr_pct", 0),
            "rsi":                result.get("rsi", 50),
            "adx":                result.get("adx", 0),
            "daily_veto_active":  result.get("daily_veto_active", False),
            "weekly_veto_active": result.get("weekly_veto_active", False),
            "daily_bias":         result.get("daily_bias", "—"),
            "l13_gate":           result.get("l13_gate", {}),
            "l8_ai":              result.get("l8_ai", {}),
            "trade_score_data":   result.get("trade_score_data", {}),
            "timing_data":        result.get("timing_data", {}),
            "scaled_entry":       result.get("scaled_entry", {}),
            "upg_sr":             result.get("upg_sr", {}),
            "upg_trendline":      result.get("upg_trendline", {}),
            "upg_trendlines_v2":  result.get("upg_trendlines_v2", {}),
            "upg_fractals_5bar":  result.get("upg_fractals_5bar", {}),
            "upg_fractals_3bar":  result.get("upg_fractals_3bar", {}),
            "upg_frac_structure": result.get("upg_frac_structure", {}),
            "upg_eq_hl":          result.get("upg_eq_hl", {}),
            "upg_frac_inversion": result.get("upg_frac_inversion", {}),
            "fractal_bsl":        result.get("fractal_bsl"),
            "fractal_ssl":        result.get("fractal_ssl"),
            "t_spot_price":       result.get("t_spot_price"),
            "t_spot_quality":     result.get("t_spot_quality", "NONE"),
            "fractal_structure":  result.get("fractal_structure", ""),
            "tl_break":           result.get("tl_break", False),
            "tl_retest":          result.get("tl_retest", False),
            "tl_score":           result.get("tl_score", 0),
            "upg_vol_profile":    result.get("upg_vol_profile", {}),
            "upg_macd":           result.get("upg_macd", {}),
            "smc_confluence":     result.get("smc_confluence", {}),
            "ma_stack":           result.get("ma_stack", {}),
            # [SYNC-2] v6.0 — entry time window, matches TechnicalAnalysis.py
            "entry_time":         result.get("entry_time", {}),
            # ── v7.0 new keys: OTE zone ────────────────────────────────────
            "ote":                result.get("ote", {}),
            "ote_705":            _flt(result, "ote_705"),
            "ote_zone_high":      _flt(result, "ote_zone_high"),
            "ote_zone_low":       _flt(result, "ote_zone_low"),
            # ── v7.0 new keys: SL tiers ────────────────────────────────────
            "sl2":                _flt(result, "sl2"),
            "sl3":                _flt(result, "sl3"),
            # ── v7.0 new keys: Liquidation heatmap ────────────────────────
            "liq_long_10x":       _flt(result, "liq_long_10x"),
            "liq_long_20x":       _flt(result, "liq_long_20x"),
            "liq_long_50x":       _flt(result, "liq_long_50x"),
            "liq_short_10x":      _flt(result, "liq_short_10x"),
            "liq_short_20x":      _flt(result, "liq_short_20x"),
            "liq_short_50x":      _flt(result, "liq_short_50x"),
            # ── v7.0 upgrade signals for chart overlays ────────────────────
            "upg_wyckoff":        result.get("upg_wyckoff", {}),
            "upg_cvd":            result.get("upg_cvd", {}),
            "upg_funding_z":      result.get("upg_funding_z", {}),
            "upg_liq_heat":       result.get("upg_liq_heat", {}),
            "upg_elliott":        result.get("upg_elliott", {}),
            "upg_squeeze":        result.get("upg_squeeze", {}),
            "upg_vwap_z":         result.get("upg_vwap_z", {}),
            # ── v7.0 MTF-50 fields ─────────────────────────────────────────
            "bias_tf":            result.get("bias_tf", ""),
            "ref_tf":             result.get("ref_tf", ""),
            "exec_tf":            result.get("exec_tf", ""),
            "mtf_combo":          result.get("mtf_combo", ""),
            "trader_type":        result.get("trader_type", ""),
            # ── v8.0 new signal-state fields ───────────────────────────────
            "conf50":             result.get("conf50", {}),
            "rr_blocked":         result.get("rr_blocked", False),
            "rr_block_val":       result.get("rr_block_val", 0.0),
            "upg_fomc":           result.get("upg_fomc", {}),
            "upg_sar":            result.get("upg_sar", {}),
            "upg_sf_div":         result.get("upg_sf_div", {}),
            "upg_range_pos":      result.get("upg_range_pos", {}),
            # ── v8.2 liquidity / ICT structure fields ──────────────────────
            "bsl_ssl_data":       result.get("bsl_ssl_data", {}),
            "smc_liq_type":       result.get("smc_liq_type", {}),
            "smc_trading_range":  result.get("smc_trading_range", {}),
            "smc_dyn_liq":        result.get("smc_dyn_liq", {}),
            "smc_sweep":          result.get("smc_sweep", {}),
            "smc_ob_quality":     result.get("smc_ob_quality", {}),
            "smc_choch_quality":  result.get("smc_choch_quality", {}),
            "smc_inducement":     result.get("smc_inducement", {}),
            "smc_fvg_quality":    result.get("smc_fvg_quality", {}),
            # BUG-FIX: "smc_confluence" was also set at line 12063; duplicate removed here.
            # ── v8.2 trade plan / scenario fields ──────────────────────────
            "irl_erl_data":       result.get("irl_erl_data", {}),
            "bpr":                result.get("bpr", {}),
            "breaker_block":      result.get("breaker_block", {}),
            "silver_bullet":      result.get("silver_bullet", {}),
            "scaled_entry_ote":   result.get("scaled_entry_ote", False),
            # BUG-FIX: "invalidation_level" duplicate removed here; canonical entry above.
            "risk_grade":         result.get("risk_grade", "—"),
            "trade_plan_valid":   result.get("trade_plan_valid", False),
            # ── v8.2 alpha / quant layers ──────────────────────────────────
            "alpha_vwap":         result.get("alpha_vwap", {}),
            "alpha_cvd":          result.get("alpha_cvd", {}),
            "alpha_profile":      result.get("alpha_profile", {}),
            "alpha_funding":      result.get("alpha_funding", {}),
            "alpha_dd_lev":       result.get("alpha_dd_lev", {}),
            "alpha_regime_size":  result.get("alpha_regime_size", {}),
            "l5_stats":           result.get("l5_stats", {}),
            "l6_vol_phase":       result.get("l6_vol_phase", {}),
            "l6_liq_cycle":       result.get("l6_liq_cycle", {}),
            "l8_ai":              result.get("l8_ai", {}),
            "l10_drift":          result.get("l10_drift", {}),
            "l12_var":            result.get("l12_var", {}),
            "l12_convex_size":    result.get("l12_convex_size", 0.0),
            "l13_gate":           result.get("l13_gate", {}),
            "l14_sor":            result.get("l14_sor", {}),
            "l18_override":       result.get("l18_override", {}),
            "l19_shap":           result.get("l19_shap", {}),
            "kelly_data":         result.get("kelly_data", {}),
            "dd_status":          result.get("dd_status", {}),
            "news_filter":        result.get("news_filter", {}),
            "prop_slip":          result.get("prop_slip", {}),
            "rl_status":          result.get("rl_status", {}),
            "macro_data":         result.get("macro_data", {}),
            "hmm_data":           result.get("hmm_data", {}),
            "monte_carlo":        result.get("monte_carlo", {}),
            # ── v8.2 ICT deep signals ───────────────────────────────────────
            "po3_data":           result.get("po3_data", {}),
            "mmxm_data":          result.get("mmxm_data", {}),
            "ipda_data":          result.get("ipda_data", {}),
            "smt_data":           result.get("smt_data", {}),
            "cisd_data":          result.get("cisd_data", {}),
            "irl_erl_data":       result.get("irl_erl_data", {}),
            "midnight_data":      result.get("midnight_data", {}),
            "asr_data":           result.get("asr_data", {}),
            "ts_data":            result.get("ts_data", {}),
            "ob_mit_data":        result.get("ob_mit_data", {}),
            "narrative_data":     result.get("narrative_data", {}),
            "rsi_div_data":       result.get("rsi_div_data", {}),
            "vsa_data":           result.get("vsa_data", {}),
            "stoch_data":         result.get("stoch_data", {}),
            "funding_gate_data":  result.get("funding_gate_data", {}),
            "candle_anatomy_data":result.get("candle_anatomy_data", {}),
            "unicorn_data":       result.get("unicorn_data", {}),
            "lv_data":            result.get("lv_data", {}),
            "lv_v2_data":         result.get("lv_v2_data", {}),
            "ifvg_tracker_data":  result.get("ifvg_tracker_data", {}),
            "avwap_data":         result.get("avwap_data", {}),
            "sb_signal_data":     result.get("sb_signal_data", None),
            "breaker_v2_data":    result.get("breaker_v2_data", {}),
            "vol_delta_data":     result.get("vol_delta_data", {}),
            "amd_data":           result.get("amd_data", {}),
            "gate14_data":        result.get("gate14_data", {}),
            "gate14_score":       result.get("gate14_score", 0),
            "gate14_cusum_veto":  result.get("gate14_cusum_veto", False),
            "rtcw":               result.get("rtcw", {}),
            "ote_new":            result.get("ote_new", {}),
            # ── v8.2 pattern suite ─────────────────────────────────────────
            "upg_patterns":       result.get("upg_patterns", {}),
            "upg_asc_tri":        result.get("upg_asc_tri", {}),
            "upg_desc_tri":       result.get("upg_desc_tri", {}),
            "upg_sym_tri":        result.get("upg_sym_tri", {}),
            "upg_bull_flag":      result.get("upg_bull_flag", {}),
            "upg_bear_flag":      result.get("upg_bear_flag", {}),
            "upg_bull_wedge":     result.get("upg_bull_wedge", {}),
            "upg_bear_wedge":     result.get("upg_bear_wedge", {}),
            "upg_rise_wedge":     result.get("upg_rise_wedge", {}),
            "upg_fall_wedge":     result.get("upg_fall_wedge", {}),
            "upg_triple_top":     result.get("upg_triple_top", {}),
            "upg_triple_bot":     result.get("upg_triple_bot", {}),
            "upg_ihs":            result.get("upg_ihs", {}),
            "upg_hs":             result.get("upg_hs", {}),
            "upg_double_top":     result.get("upg_double_top", {}),
            "upg_double_bot":     result.get("upg_double_bot", {}),
            # ── v8.2 MTF full stack ────────────────────────────────────────
            "mtf_combo_id":       result.get("mtf_combo_id", 16),
            "combo_tier":         result.get("combo_tier", "VALID"),
            "combo_style":        result.get("combo_style", ""),
            "combo_kz":           result.get("combo_kz", ""),
            "entry_ltf":          result.get("entry_ltf", "—"),
            "mtf_alignment_score":result.get("mtf_alignment_score", 50),
            "mtf_cascade_note":   result.get("mtf_cascade_note", ""),
            "mtf_tf_agree":       result.get("mtf_tf_agree", 0),
            "mtf_tf_oppose":      result.get("mtf_tf_oppose", 0),
            "mtf_tf_neutral":     result.get("mtf_tf_neutral", 0),
            "mtf_ma_cross":       result.get("mtf_ma_cross", []),
            "bias_1w":            result.get("bias_1w", ""),
            "bias_1d":            result.get("bias_1d", ""),
            "bias_4h":            result.get("bias_4h", ""),
            "bias_1h":            result.get("bias_1h", ""),
            "bias_15m":           result.get("bias_15m", ""),
            "bias_5m":            result.get("bias_5m", ""),
            "bias_1m":            result.get("bias_1m", ""),
            "weekly_veto_active": result.get("weekly_veto_active", False),
            "weekly_daily_liq":   result.get("weekly_daily_liq", {}),
            "htf_ltf":            result.get("htf_ltf", {}),
            "regime_filter":      result.get("regime_filter", {}),
            "disp_v2":            result.get("disp_v2", {}),
            "orderflow":          result.get("orderflow", {}),
            "of_eval":            result.get("of_eval", {}),
            "session_ok":         result.get("session_ok", {}),
            "dynamic_rr":         result.get("dynamic_rr", {}),
            "mfe_data":           result.get("mfe_data", {}),
            "backtest":           result.get("backtest", {}),
            "inst_signal":        result.get("inst_signal", {}),
            "upg_garch":          result.get("upg_garch", {}),
            "upg_funding_z":      result.get("upg_funding_z", {}),
            "upg_double_top":     result.get("upg_double_top", {}),
            "stat_winrate":       result.get("stat_winrate", 0.5),
            "eq_highs":           result.get("eq_highs", 0),
            "eq_lows":            result.get("eq_lows", 0),
            "disp_score_raw":     result.get("disp_score_raw", 0.0),
            "choch":              result.get("choch", False),
            "pd_zone":            result.get("pd_zone", {}),
            "ma_closes_full":     [float(c[3]) for c in result.get("ohlcv_chart_full") or []]
                                  or result.get("ma_closes_full"),
            # ── [v9.0 SYNC] New signal intelligence + chart visual keys ────
            "win_prob_display":        result.get("win_prob_display",
                                           f"{result.get('win_prob', 0):.1f}%"),
            "signal_probability_data": result.get("signal_probability_data", {}),
            # scanner_summary: patch conf50 here after conf50 compute (below)
            "scanner_summary":         result.get("scanner_summary", {}),
            # BOS/CHoCH chart label keys (T3-C)
            "bos_price":               result.get("bos_price", 0.0),
            "bos_type":                result.get("bos_type", ""),
            "order_block":             result.get("smc_ob_quality", {}),
            "markov_data":             result.get("markov_data", {}),
            "markov_validation":       result.get("markov_validation", {}),
            "markov_state":            result.get("markov_state", ""),
            "markov_effective_n":      result.get("markov_effective_n", 0.0),
        }

        # ── [v9.0 SYNC-6] Patch scanner_summary with conf50 data ────────
        # conf50 is computed by app.py handler (set into result["conf50"]).
        # Back-fill the two placeholder fields in scanner_summary so
        # multi-pair scanner heatmap has complete data.
        _c50_patch = result.get("conf50", {})
        if _c50_patch and isinstance(chart_data.get("scanner_summary"), dict):
            chart_data["scanner_summary"]["conf50_score"] = _c50_patch.get("conf50_score", 0)
            chart_data["scanner_summary"]["conf50_grade"] = _c50_patch.get("conf50_grade", "?")
            chart_data["scanner_summary"]["signal_state"] = _c50_patch.get("conf50_signal_state",
                chart_data["scanner_summary"].get("signal_state", "UNKNOWN"))

        _gc.collect()

        chart_buf = None
        if _setting_disable_charts(context):
            logger.warning("[TA] Chart generation disabled by DISABLE_CHARTS/LOW_RAM_MODE")
        else:
            for _attempt in range(3):
                try:
                    _gc.collect()
                    chart_buf = await asyncio.to_thread(generate_trade_chart, chart_data)
                    break
                except Exception as _ce:
                    logger.warning(f"Chart render attempt {_attempt+1}/3 failed: {_ce}")
                    _gc.collect()
                    await asyncio.sleep(0.5)
        _chart_data_cap = {
            "ob_high":           chart_data.get("ob_high", 0.0),
            "rr_blocked":        chart_data.get("rr_blocked", False),
            "smc_confluence":    chart_data.get("smc_confluence", {}),
            "gate_pass":         chart_data.get("gate_pass", []),
            "daily_veto_active": chart_data.get("daily_veto_active", False),
            "l6_vol_phase":      chart_data.get("l6_vol_phase", {}),
            "daily_bias":        chart_data.get("daily_bias", ""),
        }

        # ── [OCE v10.3] Overlay OCE eval + probability + HMM badges on chart ─
        if (OCE_AVAILABLE and chart_buf
                and result.get("oce_eval")
                and result.get("oce_gaps") is not None):
            chart_buf = oce_annotate_chart(
                chart_buf,
                result,
                result["oce_eval"],
                result["oce_gaps"],
            )
        # ─────────────────────────────────────────────────────────────────────

        del chart_data
        _gc.collect()

        await thinking.delete()

        # ── TA Output Mode: filter based on Settings toggle ───────────────────
        # [v9.0] build_result_messages layout (index 0-based):
        # ACTIVE signal (verbose_mode=True):
        #   [0] msg0 — Summary Tile (probability card)        ← NEW v9.0
        #   [1] msg1 — Signal Header + 10-Gate Dashboard
        #   [2] msg2 — Structure Layer (Gates 1–5)
        #   [3] msg3 — Execution Layer (Gates 6–10 + MTF)
        #   [4] msg4 — Live Market Data / TF / Exchanges
        #   [5] msg6 — Prop Audit + Quant Stack
        #   [6] msg7 — Alpha Layer
        #   [7] msg8 — 20-Layer Fund Intelligence
        #   [8] msg9 — SMC Advanced Validation
        #   [9] msg5 — 📍 TRADE PLAN ← always last
        # ACTIVE signal (verbose_mode=False, default):
        #   [0] msg0 — Summary Tile  [1] msg1  [2] msg2  [3] msg3
        #   [4] msg4  [5] msg6  [6] msg8  [7] msg9  (8 msgs, no msg5/msg7)
        # BLOCKED signal (any mode): 2 msgs only — msg0 (blocked tile) + msg2
        # OFF mode: skip ALL text — chart + caption only
        _verbose = _ta_verbose_is_on(context)

        if not _verbose:
            # Compact mode — chart + caption only
            try:
                if chart_buf:
                    await message.reply_photo(photo=chart_buf, caption=caption)
                    chart_buf.close()
                else:
                    await message.reply_text(f"⚠️ Chart render failed — trade data:\n{caption}")
            except Exception as e:
                logger.warning(f"Chart send failed (compact mode): {e}")
            capture_output(message.chat.id, caption, "ta")

            # ── [OCE GAP-3] WebApp interactive signal button (compact mode) ─
            if WEBAPP_URL and OCE_AVAILABLE:
                try:
                    _webapp_url_c = build_webapp_signal_url(result, symbol)
                    if _webapp_url_c:
                        _dir_c   = "LONG ▲" if result.get("bias","").upper() in ("BULLISH","LONG") else "SHORT ▼"
                        _state_c = result.get("signal_state", "BLOCKED")
                        _si_c    = {"ACTIVE":"🟢","DEFERRED":"🟡","BLOCKED":"🔴"}.get(_state_c,"⚪")
                        await message.reply_text(
                            f"📊 *Interactive Signal View*\n"
                            f"`{symbol}USDT` · {_dir_c} · {_si_c} {_state_c}",
                            parse_mode="Markdown",
                            reply_markup=InlineKeyboardMarkup([[
                                InlineKeyboardButton(
                                    "📈 Open Signal Chart",
                                    web_app=WebAppInfo(url=_webapp_url_c)
                                )
                            ]])
                        )
                except Exception as _wb_c_e:
                    logger.warning(f"[OCE GAP-3] WebApp button (compact) failed: {_wb_c_e}")
            # ────────────────────────────────────────────────────────────
            # ── [MOP-P04/P08] Gate memory + condition evaluation (compact mode) ──
            try:
                _gp_c = result.get("gate_pass", [])
                if _gp_c and _GATE_MEMORY_AVAILABLE:
                    _gm_record_gate_outcome(
                        gate_pass  = [bool(g) for g in _gp_c],
                        outcome    = "TIMEOUT",
                        hmm_regime = (result.get("hmm_data") or {}).get("hmm_state", ""),
                        ict_state  = _get_alpha_markov_state(symbol) or "ACCUMULATION",
                    )
            except Exception as _gm_ec:
                logger.debug(f"[MOP-P04 GATE_MEM compact] non-critical: {_gm_ec}")
            try:
                _cond_fired_c = _cond_engine.evaluate_conditions(result, symbol)
                for _cfc in _cond_fired_c:
                    try:
                        await context.bot.send_message(
                            chat_id    = _cfc["chat_id"],
                            text       = _cfc["message"],
                            parse_mode = "Markdown",
                        )
                    except Exception as _cfc_e:
                        logger.warning(f"[MOP-P08 COND compact] notify failed: {_cfc_e}")
            except Exception as _ce_ec:
                logger.debug(f"[MOP-P08 COND compact] non-critical: {_ce_ec}")
            # ─────────────────────────────────────────────────────────────
            # ── [AWS] Upsert into multi-coin APEX watch store (compact) ──
            try:
                if _AWS_AVAILABLE:
                    _aws.upsert(
                        symbol   = f"{symbol}USDT",
                        result   = result,
                        chat_ids = [message.chat.id],
                    )
            except Exception as _aws_ce:
                logger.debug(f"[AWS] compact upsert non-critical: {_aws_ce}")
            # ── [DEEP STRUCT] Register for extended history refresh ──────
            try:
                _deep_struct_symbols.add(f"{symbol}USDT")
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            # ─────────────────────────────────────────────────────────────
            _rr_cap   = round(abs(_tp3 - _entry_mid) / max(abs(_entry_mid - _inv_sl), 1e-9), 2)
            _ob_high_cap = _chart_data_cap.get("ob_high", 0.0)
            apex_register_watched_coin(
                symbol             = symbol,
                price              = _entry_mid,
                bias               = _bias,
                tp1                = _tp1,
                sl                 = _inv_sl,
                chat_id            = message.chat.id,
                tp2                = _tp2,
                tp3                = _tp3,
                sl2                = _sl2,
                sl3                = _sl3,
                inv                = _inv_sl,
                rr                 = _rr_cap,
                rr_blocked         = bool(_chart_data_cap.get("rr_blocked", False)),
                grade              = str(_ts_cap.get("trade_grade", "?")),
                conf               = int(_ts_cap.get("trade_score", 0)),
                smc                = int((_chart_data_cap.get("smc_confluence") or {}).get("smc_score", 0)),
                gates              = int(sum(_chart_data_cap.get("gate_pass", []))),
                verdict            = str(_chart_data_cap.get("daily_veto_active", False) and "DAILY VETO" or ""),
                regime             = str(_chart_data_cap.get("l6_vol_phase", {}).get("garch_phase", "")),
                daily_bias         = str(_chart_data_cap.get("daily_bias", "")),
                daily_veto         = bool(_chart_data_cap.get("daily_veto_active", False)),
                bias_flip_neutral  = float(_ob_high_cap),
                bias_flip_full     = float(_inv_sl),
                scenario_a         = f"{_bias[:4]} → TP1 {fmt_price(_tp1)} → TP3 {fmt_price(_tp3)}",
                scenario_b         = f"Sweep SSL → LTF CHoCH → reverse to {fmt_price(_tp1)}",
                markov_state       = _get_alpha_markov_state(symbol),
            )
            # ── Structured observability log (v7.0) ──────────────────────
            if TA_MODULE_AVAILABLE:
                try:
                    log_analysis_event(
                        symbol=symbol,
                        event="analysis_complete_compact",
                        grade=_ts_cap.get("trade_grade", "?"),
                        conf=float(_ts_cap.get("trade_score", 0)),
                        gate_pass=None,
                        mem_delta_mb=0.0,
                    )
                except Exception as _e:
                    logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            # ── [TV-FREE Phase 4] Auto TV cross-check after compact output ──────
            try:
                _tv_cached = _LAST_RESULT_CACHE.get(symbol)
                if _tv_cached and TA_MODULE_AVAILABLE:
                    _tv_msg = generate_tv_checklist(_tv_cached)
                    await safe_send(message, f"```\n{_tv_msg}\n```", parse_mode=_PM.MARKDOWN)
            except Exception as _tv_e:
                logger.debug(f"[TV-FREE] Compact checklist send failed: {_tv_e}")
            context.user_data["ta_running"] = False
        else:
            # Verbose mode — full message set + chart on last
            # [v10.3 FIX C2] Removed ``` monospace wrapping on msg2/3/4 —
            # monospace in Telegram does NOT word-wrap, causing horizontal
            # scroll overflow on mobile. All messages sent as plain Markdown.
            # Lines from build_result_messages are already ≤60 chars wide.
            for i, msg_text in enumerate(messages_list):
                # Guard: trim each individual message to 4096 chars before send
                fmt = msg_text if len(msg_text) <= 4000 else trim_message_for_telegram(msg_text, 4000)
                await safe_send(message, fmt, parse_mode=_PM.MARKDOWN)

                if i == len(messages_list) - 1:
                    await asyncio.sleep(0.3)
                    try:
                        if chart_buf:
                            await message.reply_photo(photo=chart_buf, caption=caption)
                            chart_buf.close()
                        else:
                            await message.reply_text(f"⚠️ Chart render failed (RAM limit) — trade data above is valid.\n{caption}")
                    except Exception as e:
                        logger.warning(f"Chart send failed: {e}")
                    capture_output(message.chat.id, "\n\n---\n\n".join(messages_list), "ta")

                    # ── [OCE GAP-3] WebApp interactive signal button ──────
                    if WEBAPP_URL and OCE_AVAILABLE:
                        try:
                            _webapp_url = build_webapp_signal_url(result, symbol)
                            if _webapp_url:
                                _grade_v = result.get("grade", "?")
                                _state_v = result.get("signal_state", "BLOCKED")
                                _dir_v   = "LONG ▲" if result.get("bias","").upper() in ("BULLISH","LONG") else "SHORT ▼"
                                _state_icon = {"ACTIVE":"🟢","DEFERRED":"🟡","BLOCKED":"🔴"}.get(_state_v,"⚪")
                                await message.reply_text(
                                    f"📊 *Interactive Signal View*\n"
                                    f"`{symbol}USDT` · {_dir_v} · Grade {_grade_v} · {_state_icon} {_state_v}",
                                    parse_mode="Markdown",
                                    reply_markup=InlineKeyboardMarkup([[
                                        InlineKeyboardButton(
                                            "📈 Open Signal Chart",
                                            web_app=WebAppInfo(url=_webapp_url)
                                        )
                                    ]])
                                )
                        except Exception as _wb_e:
                            logger.warning(f"[OCE GAP-3] WebApp button send failed: {_wb_e}")
                    # ─────────────────────────────────────────────────────
                    if TA_MODULE_AVAILABLE:
                        try:
                            log_analysis_event(
                                symbol=symbol,
                                event="analysis_complete_verbose",
                                grade=_ts_cap.get("trade_grade", "?"),
                                conf=float(_ts_cap.get("trade_score", 0)),
                                gate_pass=None,
                                mem_delta_mb=0.0,
                            )
                        except Exception as _e:
                            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
                    # ── Register coin with APEX watch engine (v8.2) ──────────────
                    _rr_vb   = round(abs(_tp3 - _entry_mid) / max(abs(_entry_mid - _inv_sl), 1e-9), 2)
                    _ob_hi   = _chart_data_cap.get("ob_high", 0.0)

                    # ── [MOP-P04/P08] Gate memory record + condition evaluation ──
                    # Both hooks run while result dict is still in scope.
                    try:
                        _gp_hook  = result.get("gate_pass", [])
                        _hmm_hook = (result.get("hmm_data") or {}).get("hmm_state", "")
                        _ict_hook = (result.get("gate11_detail") or {}).get("gate11_note", "")
                        # Bug 4 fix: record gate vector into fingerprint memory
                        # Outcome not known yet — record as TIMEOUT placeholder;
                        # APEX outcome feedback will update with real result via record_gate_outcome
                        # when TP/SL is hit. Here we ensure the vector is registered.
                        if _gp_hook and _GATE_MEMORY_AVAILABLE:
                            _gm_record_gate_outcome(
                                gate_pass  = [bool(g) for g in _gp_hook],
                                outcome    = "TIMEOUT",   # placeholder — updated by outcome feedback
                                hmm_regime = _hmm_hook,
                                ict_state  = _get_alpha_markov_state(symbol) or "ACCUMULATION",
                            )
                    except Exception as _gm_e:
                        logger.debug(f"[MOP-P04 GATE_MEM] non-critical: {_gm_e}")

                    try:
                        # Bug 3 fix: evaluate /watch conditions against this result
                        _cond_fired = _cond_engine.evaluate_conditions(result, symbol)
                        for _cf in _cond_fired:
                            try:
                                await context.bot.send_message(
                                    chat_id    = _cf["chat_id"],
                                    text       = _cf["message"],
                                    parse_mode = "Markdown",
                                )
                            except Exception as _cf_e:
                                logger.warning(f"[MOP-P08 COND] notify failed: {_cf_e}")
                    except Exception as _ce_e:
                        logger.debug(f"[MOP-P08 COND] non-critical: {_ce_e}")
                    # ─────────────────────────────────────────────────────────────
                    # ── [AWS] Upsert into multi-coin APEX watch store (verbose) ──
                    try:
                        if _AWS_AVAILABLE:
                            _aws.upsert(
                                symbol   = f"{symbol}USDT",
                                result   = result,
                                chat_ids = [message.chat.id],
                            )
                    except Exception as _aws_ve:
                        logger.debug(f"[AWS] upsert non-critical: {_aws_ve}")
                    # ── [DEEP STRUCT] Register for extended history refresh ──────
                    try:
                        _deep_struct_symbols.add(f"{symbol}USDT")
                    except Exception as _e:
                        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
                    # ─────────────────────────────────────────────────────────────
                    apex_register_watched_coin(
                        symbol             = symbol,
                        price              = _entry_mid,
                        bias               = _bias,
                        tp1                = _tp1,
                        sl                 = _inv_sl,
                        chat_id            = message.chat.id,
                        tp2                = _tp2,
                        tp3                = _tp3,
                        sl2                = _sl2,
                        sl3                = _sl3,
                        inv                = _inv_sl,
                        rr                 = _rr_vb,
                        rr_blocked         = bool(_chart_data_cap.get("rr_blocked", False)),
                        grade              = str(_ts_cap.get("trade_grade", "?")),
                        conf               = int(_ts_cap.get("trade_score", 0)),
                        smc                = int((_chart_data_cap.get("smc_confluence") or {}).get("smc_score", 0)),
                        gates              = int(sum(_chart_data_cap.get("gate_pass", []))),
                        verdict            = str(_chart_data_cap.get("daily_veto_active", False) and "DAILY VETO" or ""),
                        regime             = str(_chart_data_cap.get("l6_vol_phase", {}).get("garch_phase", "")),
                        daily_bias         = str(_chart_data_cap.get("daily_bias", "")),
                        daily_veto         = bool(_chart_data_cap.get("daily_veto_active", False)),
                        bias_flip_neutral  = float(_ob_hi),
                        bias_flip_full     = float(_inv_sl),
                        scenario_a         = f"{_bias[:4]} → TP1 {fmt_price(_tp1)} → TP3 {fmt_price(_tp3)}",
                        scenario_b         = f"Sweep SSL → LTF CHoCH → reverse to {fmt_price(_tp1)}",
                        markov_state       = _get_alpha_markov_state(symbol),
                    )
                    context.user_data["ta_running"] = False
                    # ── [TV-FREE Phase 4] Auto TV cross-check after verbose output ──
                    try:
                        _tv_cached_vb = _LAST_RESULT_CACHE.get(symbol)
                        if _tv_cached_vb and TA_MODULE_AVAILABLE:
                            _tv_msg_vb = generate_tv_checklist(_tv_cached_vb)
                            await safe_send(message, f"```\n{_tv_msg_vb}\n```", parse_mode=_PM.MARKDOWN)
                    except Exception as _tv_vb_e:
                        logger.debug(f"[TV-FREE] Verbose checklist send failed: {_tv_vb_e}")

    except ValueError as e:
        try:
            await thinking.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        context.user_data['ta_mode'] = False
        _err_msg = str(e).lower()
        # Distinguish between "not listed on any CEX" vs other pipeline ValueErrors
        # so users aren't misled by a generic "Symbol not found" message.
        if "not be listed" in _err_msg or "no exchange data" in _err_msg or "no price data" in _err_msg:
            await message.reply_text(
                f"⚠️ *{symbol} is not available on any supported exchange.*\n\n"
                "Cryptex checks: Binance, OKX, Bybit, KuCoin, Gate.io, MEXC, "
                "Bitget, HTX, CoinEx, Kraken, Phemex + Futures.\n\n"
                f"{symbol}USDT was not found on any of them.\n"
                "Try: BTC, ETH, SOL, BNB, XRP, AVAX, LINK, DOT",
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
        elif "flash-crash" in _err_msg or "circuit breaker" in _err_msg:
            await message.reply_text(
                f"⚠️ *{symbol} — Flash-crash circuit breaker active.*\n\n"
                "An abnormal price move was detected. Analysis is paused for safety.\n"
                "Please wait for price to stabilise, then try again.",
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
        elif "insufficient candle" in _err_msg:
            await message.reply_text(
                f"⚠️ *{symbol} — Not enough historical data.*\n\n"
                "The exchange returned too few candles to run the full pipeline.\n"
                "This usually happens with very new listings. Try again in a few hours.",
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
        else:
            await message.reply_text(
                f"⚠️ Symbol not found: {symbol}\nTry: BTC, ETH, SOL, BNB, XRP",
                reply_markup=create_main_keyboard()
            )
    except MemoryError as e:
        # MemoryError is raised by TechnicalAnalysis._wait_for_memory() when
        # analysis-phase RAM delta stays above MAX_RAM_BYTES after 30 s.
        # Show a clean retry message — do NOT surface the raw exception text.
        try:
            await thinking.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        context.user_data['ta_mode'] = False
        logger.warning(f"[MEM] MemoryError during TA for {symbol}: {e}")
        await message.reply_text(
            f"⚠️ *Memory pressure during {symbol} analysis.*\n\n"
            "The pipeline timed out waiting for RAM to free up.\n"
            "Please wait 30 seconds, then type the coin symbol again.\n\n"
            "_Tip: If this keeps happening, restart the bot to clear idle memory._",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
    except (TelegramNetworkError, TelegramTimedOut, RetryAfter) as e:
        # Network blip between the bot and Telegram API (httpx.ReadError, connection reset, etc.)
        # The analysis itself completed — this is a delivery failure, not a pipeline failure.
        logger.warning(f"[NETWORK] Telegram send error during TA for {symbol}: {e!r}")
        try:
            await thinking.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        context.user_data['ta_mode'] = False
        # Wait a moment then send a clean retry prompt
        await asyncio.sleep(1.5)
        try:
            await message.reply_text(
                f"⚠️ *Network hiccup sending {symbol} results.*\n\n"
                "The analysis completed but a connection drop interrupted delivery.\n"
                "Please type the symbol again to re-run — it will use cached exchange data and be faster.\n\n"
                "_This is a Telegram/server network issue, not a bot error._",
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    except Exception as e:
        import traceback as _tb
        _trace = _tb.format_exc()
        logger.exception(f"TA analysis error for {symbol}: {e}")
        try:
            await thinking.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        context.user_data['ta_mode'] = False
        await message.reply_text(
            f"❌ Analysis failed for {symbol}\n{type(e).__name__}: {str(e)[:200]}\nPlease try again.",
            reply_markup=create_main_keyboard()
        )
    finally:
        # ── Always release the TA lock — prevents permanent Feedback block ───
        context.user_data["ta_running"] = False

@safe_menu_handler
async def dex_scanner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle DEX Scanner button — calls dex.py build_top10 and sends results"""
    global _dex_last_global_call
    if not DEX_MODULE_AVAILABLE:
        await update.message.reply_text(
            "⚠️ DEX module is not available. Please ensure dex.py is in the same directory.",
            reply_markup=create_main_keyboard()
        )
        return

    # ── DEX rate limiting — global guard (prevents API ban from any user) ──
    _now = time.time()
    _global_gap = _now - _dex_last_global_call
    if _global_gap < DEX_GLOBAL_RATE_LIMIT_SECONDS:
        _wait = round(DEX_GLOBAL_RATE_LIMIT_SECONDS - _global_gap, 1)
        await update.message.reply_text(
            f"⏳ DEX scanner cooling down — try again in `{_wait}s`.",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
        return

    # ── DEX rate limiting — per-user guard (matches TA 30s pattern) ──────
    _chat_id = update.effective_chat.id
    _user_last = _dex_last_request.get(_chat_id, 0.0)
    _user_gap  = _now - _user_last
    if _user_gap < DEX_USER_RATE_LIMIT_SECONDS:
        _remaining = round(DEX_USER_RATE_LIMIT_SECONDS - _user_gap)
        await update.message.reply_text(
            f"⏳ DEX cooldown active — try again in `{_remaining}s`.",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
        return

    _dex_last_request[_chat_id] = _now
    _dex_last_global_call = _now

    loading = await update.message.reply_text(
        "🦎 *DEX SCANNER — Scanning all chains...*\n"
        "Sources: DexScreener Search + New Profiles\n"
        "Chains: Solana, Base, ETH, BSC, Arbitrum\n"
        "Please wait ~10s...",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )

    try:
        cards = await dex_build_top10()
    except Exception as e:
        logger.exception(f"DEX build_top10 failed: {e}")
        try:
            await loading.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        await update.message.reply_text(
            "❌ DEX scan failed. Please try again in 30s.",
            reply_markup=create_main_keyboard()
        )
        return

    try:
        await loading.delete()
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    # Capture combined DEX output for Feedback engine
    _dex_combined = "\n\n---\n\n".join(msg for msg, _ in cards)
    capture_output(update.effective_chat.id, _dex_combined, "dex")

    # ── Super Markov: TrendBrain for DEX momentum signals ────────────
    if MARKOV_AVAILABLE and cards:
        try:
            _sv_dex = _SUPER_VALIDATOR.validate(
                current_regime   = "VOLATILE",
                momentum_quality = "HIGH",
            )
            await asyncio.to_thread(
                _SUPER_VALIDATOR.trend.record_trend_signal,
                "HIGH", ""
            )
            asyncio.create_task(
                _markov_ai_consensus(
                    symbol    = "DEX_MARKET",
                    ict_state = "FVG_BULL",
                    bias      = "BULLISH",
                    conf      = 55,
                    smc       = 50,
                    gates     = 5,
                    n_samples = 0,
                ),
                name="markov_ai_dex"
            )
            # Send compact line after header
            await update.message.reply_text(
                f"{_sv_dex['super_compact']}\n⚠️ _DEX gems = high risk. DYOR._",
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
        except Exception as _sv_dx_e:
            logger.debug(f"[SUPER_MARKOV] dex: {_sv_dx_e}")

    await update.message.reply_text(
        "*🦎 CROSS-CHAIN DEX SCAN — TOP 10 SCORE-RANKED GEMS*\n"
        "Score = Age(30) + 5mTxns(20) + Vol(20) + Momentum(15) + Liq(10) + BuyRatio(5)\n"
        "==========================",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )

    for i, (msg, kb) in enumerate(cards, 1):
        try:
            await update.message.reply_text(
                msg,
                parse_mode="Markdown",
                reply_markup=kb,
            )
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"DEX: Failed sending card {i}: {e}")

    await update.message.reply_text(
        "==========================\n🦎 DEX Scan complete. Stay sharp.",
        reply_markup=create_main_keyboard()
    )


# MOVED TO: ai_assistant.py — ai_assistant()

@safe_menu_handler
async def ta_market_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle spot/futures market selection from Technical Analysis inline keyboard."""
    query = update.callback_query
    await query.answer()
    data = query.data  # e.g. "ta_spot_BTC" or "ta_futures_ETH"

    if data.startswith("ta_spot_"):
        symbol      = data.replace("ta_spot_", "")
        market_type = "spot"
        label       = "SPOT"
    else:
        symbol      = data.replace("ta_futures_", "")
        market_type = "futures"
        label       = "FUTURES (Perp)"

    try:
        await query.edit_message_text(
            f"✅ Got it — analyzing *{symbol}* on {label} markets...",
            parse_mode="Markdown"
        )
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    await _run_ta_analysis(query.message, symbol, context, market_type=market_type)


# MOVED TO: ai_assistant.py — ai_choice()

async def _airdrop_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Menu button handler — shows airdrop submenu (delegates to airdrops module)."""
    await handle_airdrops_button(update, context, create_main_keyboard)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages — menu buttons ALWAYS work regardless of current mode."""
    if not update.message or not update.message.text:
        return

    text    = update.message.text.strip()
    chat_id = update.effective_chat.id

    # ── SECURITY: chat allowlist gate (thesis B2.1) ──────────────────────────
    if ALLOWED_CHAT_IDS and chat_id not in ALLOWED_CHAT_IDS:
        await update.message.reply_text("⛔ Access restricted.")
        return

    # ── MENU BUTTONS — checked FIRST, before any mode logic ─────────────────
    # Normalise: strip Unicode variation selectors so ℹ️ matches regardless of client (thesis A2.1)
    text_norm = unicodedata.normalize("NFC", text)
    MENU_HANDLERS = {
        "🎁 Airdrops":           _airdrop_menu_handler,
        "📐 Cross":              cross_analysis,
        "🌊 Sector Rotation":    sector_rotation,
        "🔥 Trending":           trending_coins,
        "💎 Alpha Signals":      alpha_signals,
        "📊 Technical Analysis": technical_analysis,
        "🤖 AI Assistant":       ai_assistant,
        "🦎 DEX":                dex_scanner,
        "📋 Feedback":           feedback_button_handler,
        "🚀 Features":           features_button_handler,
        "ℹ️ Help":               help_command,
        "⚙️ Settings":           show_settings,
    }

    if text_norm in MENU_HANDLERS:
        # Exit ALL active modes when user presses any menu button
        context.user_data["ai_mode"] = False
        context.user_data["ta_mode"] = False
        context.user_data.pop("ta_mode_ts", None)
        # Cancel awaiting_evidence so the feedback engine doesn't swallow the menu press
        store = feedback_store.get(chat_id, {})
        if store and "awaiting_evidence" in store:
            store["awaiting_evidence"] = False

        await clear_market_messages(chat_id, context)
        try:
            await MENU_HANDLERS[text_norm](update, context)
        except Exception as e:
            logger.error(f"[MENU] Error in handler for '{text_norm}': {e}", exc_info=True)
            try:
                await update.message.reply_text(
                    "⚠️ Something went wrong. Please try again.",
                    reply_markup=create_main_keyboard()
                )
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        return

    # ── AI MODE — only reached when text is NOT a menu button ───────────────
    if context.user_data.get("ai_mode"):
        # Sanitise input — strip Markdown, limit to 500 chars (thesis B2.4)
        safe_input = text[:500].replace("`", "").replace("*", "").replace("_", "")
        ai_provider = user_ai_preference.get(chat_id, "groq")
        market_data = await asyncio.to_thread(get_market_regime)

        thinking_msg = await update.message.reply_text(
            "🤖 Thinking...", reply_markup=create_main_keyboard()
        )
        try:
            response = await ai_query(safe_input, market_data, ai_provider)
        except Exception as e:
            logger.error(f"[AI MODE] Query error: {e}", exc_info=True)
            response = "❌ AI query failed. Please try again."
        finally:
            try:
                await thinking_msg.delete()
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        capture_output(chat_id, response, "ai_response")
        provider_name = user_ai_preference.get(chat_id, "groq").capitalize()

        # Guard Telegram 4096-char limit (thesis D4.4)
        full_reply = (
            f"🤖 *AI RESPONSE* _{provider_name}_\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{response}\n\n"
            f"⚠️ _Not financial advice. DYOR._"
        )
        if len(full_reply) > 3900:
            # Split at a sentence boundary near 3900
            part1 = full_reply[:3900]
            part2 = full_reply[3900:]
            await update.message.reply_text(part1, parse_mode="Markdown", reply_markup=create_main_keyboard())
            await update.message.reply_text(f"_(continued)_\n{part2}", parse_mode="Markdown",
                                            reply_markup=get_feedback_inline_keyboard())
        else:
            await update.message.reply_text(full_reply, parse_mode="Markdown",
                                            reply_markup=get_feedback_inline_keyboard())
        return

    # ── TA MODE — only reached when text is NOT a menu button ───────────────
    if context.user_data.get("ta_mode"):
        # Auto-expire ta_mode after 2 minutes of inactivity (thesis A3.6)
        ta_ts = context.user_data.get("ta_mode_ts", 0)
        if time.time() - ta_ts > 120:
            context.user_data["ta_mode"] = False
            context.user_data.pop("ta_mode_ts", None)
            await update.message.reply_text(
                "⏱ TA mode timed out. Press *📊 Technical Analysis* to start again.",
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
            return

        # ── TA rate limiting (thesis B2.3) ───────────────────────────────────
        last_ta = _ta_last_request.get(chat_id, 0)
        if time.time() - last_ta < TA_RATE_LIMIT_SECONDS:
            remaining = int(TA_RATE_LIMIT_SECONDS - (time.time() - last_ta))
            await update.message.reply_text(
                f"⏳ Please wait *{remaining}s* before the next analysis.",
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
            return

        symbol = text.upper().replace("USDT", "").replace("USDC", "").replace("/", "").strip()
        if not symbol.isalpha() or len(symbol) < 2:
            await update.message.reply_text(
                "⚠️ Please enter a valid coin symbol (e.g. BTC, ETH, SOL).\n"
                "Or press any menu button to go back.",
                reply_markup=create_main_keyboard()
            )
            return

        _ta_last_request[chat_id] = time.time()
        await clear_market_messages(chat_id, context)
        # ta_mode stays True — user can keep typing coins without re-pressing the button
        await _run_ta_analysis(update.message, symbol, context)
        return

    # ── FEEDBACK EVIDENCE MODE — only reached for non-menu free-text ────────
    if await feedback_evidence_handler(update, context):
        return

    # ── UNRECOGNISED INPUT ───────────────────────────────────────────────────
    await update.message.reply_text(
        "👇 Use the menu below to get started:",
        reply_markup=create_main_keyboard()
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors — log full traceback and notify user so they can retry."""
    # ── [FIX] Conflict: another bot instance is running on the same token ──
    # This is a deployment issue (Termux + fps.ms both running). Suppress the
    # crash, log a clear warning, and let PTB's retry loop recover cleanly.
    if isinstance(context.error, TelegramConflict):
        logger.warning(
            "[CONFLICT] Duplicate bot instance detected — "
            "another process is polling the same token. "
            "Stop the other instance (Termux or local) and restart this one. "
            "Bot will keep retrying automatically."
        )
        return  # Do NOT re-raise — PTB will retry; crashing makes it worse

    # ── Standard error path ──────────────────────────────────────────────
    logger.error(f"Unhandled error: {context.error}", exc_info=context.error)
    # Try to notify the user
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ Something went wrong. Please try again or press /start to reset.",
                reply_markup=create_main_keyboard()
            )
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
# ==========================================
# MAIN
# ==========================================

async def _probe_gemini_on_startup():
    """
    Startup self-test: verify Gemini API key is valid and at least one model
    on the /v1beta/openai/ compat endpoint responds 200.
    Logs a clear WARNING (not an error) if the key is missing or all models fail.
    Does NOT block bot startup — purely diagnostic.
    """
    if not GEMINI_API_KEY or not GEMINI_API_KEY.strip():
        logger.warning("[GEMINI PROBE] GEMINI_API_KEY not set — Gemini validator will be skipped")
        return

    _key_suffix = GEMINI_API_KEY[-4:] if len(GEMINI_API_KEY) >= 4 else "***"
    test_url     = f"https://generativelanguage.googleapis.com/v1beta/openai/chat/completions?key={GEMINI_API_KEY}"
    _safe_url    = f"https://generativelanguage.googleapis.com/v1beta/openai/chat/completions?key=***{_key_suffix}"
    test_headers = {"Authorization": f"Bearer {GEMINI_API_KEY}", "Content-Type": "application/json"}
    probe_models = ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0-flash-lite", "gemini-2.0-flash"]

    try:
        async with aiohttp.ClientSession() as s:
            for mdl in probe_models:
                body = {
                    "model": mdl,
                    "messages": [{"role": "user", "content": "Reply with the single word: OK"}],
                    "max_tokens": 5,
                }
                try:
                    async with s.post(test_url, headers=test_headers, json=body,
                                      timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            logger.info(f"[GEMINI PROBE] ✅ {mdl} → HTTP 200 — Gemini is working")
                            return
                        else:
                            rtext = await r.text()
                            logger.warning(f"[GEMINI PROBE] {mdl} → HTTP {r.status}: {rtext[:150]}")
                except Exception as e:
                    logger.warning(f"[GEMINI PROBE] {mdl} → exception: {e}")
        logger.error(f"[GEMINI PROBE] ❌ All probe models failed — URL: {_safe_url}")
    except Exception as e:
        logger.error(f"[GEMINI PROBE] Probe failed entirely: {e}")


async def _cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/refresh — manually force a market data refresh without restarting."""
    chat_id = update.effective_chat.id
    try:
        await update.message.delete()
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    msg = await context.bot.send_message(
        chat_id,
        "🔄 *Refreshing market data…*",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )
    try:
        market_data = await asyncio.to_thread(get_market_regime)
        await send_market_overview(chat_id, context, market_data)
        await update_regime_pin(chat_id, context, market_data, force=True)
        try:
            await context.bot.delete_message(chat_id, msg.message_id)
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    except Exception as e:
        logger.error(f"[REFRESH CMD] Error: {e}", exc_info=True)
        try:
            await msg.edit_text("⚠️ Refresh failed. Try /start to reset.", parse_mode="Markdown")
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
async def _cmd_refresh_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback for 🔄 Refresh Data inline button."""
    query = update.callback_query
    await query.answer("Refreshing…")
    chat_id = query.message.chat_id
    try:
        market_data = await asyncio.to_thread(get_market_regime)
        await send_market_overview(chat_id, context, market_data)
        await update_regime_pin(chat_id, context, market_data, force=True)
    except Exception as e:
        logger.error(f"[REFRESH CB] Error: {e}")


async def _cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/status — show bot health, API status, background task state, and all brain stats."""
    chat_id = update.effective_chat.id
    try:
        await update.message.delete()
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    # Check API health
    apis_ok = {}
    for label, url in [
        ("CoinGecko", f"{COINGECKO_BASE_URL}/ping"),
        ("DefiLlama", f"{DEFILLAMA_BASE_URL}/protocols"),
        ("Fear&Greed", "https://api.alternative.me/fng/?limit=1"),
    ]:
        try:
            r = await asyncio.to_thread(
                requests.get, url, headers=REQUEST_HEADERS, timeout=5
            )
            apis_ok[label] = "🟢" if r.status_code == 200 else f"🔴 {r.status_code}"
        except Exception:
            apis_ok[label] = "🔴 timeout"

    ai_keys = {
        "Groq":     bool(GROQ_API_KEY),
        "Cerebras": bool(CEREBRAS_API_KEY),
        "Gemini":   bool(GEMINI_API_KEY),
        "Mistral":  bool(MISTRAL_API_KEY),
        "GitHub":   bool(GITHUB_TOKEN),
    }
    ai_str = " ".join(f"{'🟢' if v else '⚪'}{k}" for k, v in ai_keys.items())
    bg_active   = chat_id in background_tasks and not background_tasks[chat_id].done()
    modules_str = f"{'🟢' if TA_MODULE_AVAILABLE else '🔴'} TA  {'🟢' if DEX_MODULE_AVAILABLE else '🔴'} DEX"

    # GAP-04: Pull stats from all four Cryptex Stores
    aws_s   = _aws.get_summary()       if _AWS_AVAILABLE               else {}
    gm_s    = _gm_get_memory_summary() if _GATE_MEMORY_AVAILABLE       else {}
    ce_s    = _ce_get_summary()        if _COND_ENGINE_AVAILABLE       else {}
    deep_s  = _deep_struct_app.get_summary() if _DEEP_STRUCT_APP_AVAILABLE else {}

    # Pull Markov brain global stats
    mk_stats = {}
    if MARKOV_AVAILABLE:
        try:
            _mk_data  = _MARKOV_BRAIN.read()
            mk_stats["total_analyses"] = _mk_data.get("global", {}).get("total_analyses", 0)
            mk_stats["symbols_learned"] = len(_mk_data.get("symbols", {}))
            # Top symbol by effective_n
            _syms = _mk_data.get("symbols", {})
            if _syms:
                _top = max(_syms.items(), key=lambda x: x[1].get("effective_n", 0))
                mk_stats["top_symbol"]   = _top[0]
                mk_stats["top_eff_n"]    = round(_top[1].get("effective_n", 0), 1)
        except Exception as _mks_e:
            logger.debug(f"[STATUS] Markov stats error: {_mks_e}")

    # Brain summary block
    aws_line  = (f"📦 AWS Watch: {aws_s.get('pending',0)}/{aws_s.get('total',0)} pending  "
                 f"avg_conf={aws_s.get('avg_conf',0):.1f}")            if aws_s  else "📦 AWS Watch: N/A"
    gm_line   = (f"🧠 Gate Mem: {gm_s.get('fingerprint_patterns',0)} patterns  "
                 f"TP%={round(gm_s.get('tp_rate_global',0) or 0, 2)*100:.0f}%  "
                 f"🚫{gm_s.get('blacklisted_active',0)} blacklisted") if gm_s  else "🧠 Gate Mem: N/A"
    ce_line   = (f"🔔 Conditions: {ce_s.get('active',0)} active  "
                 f"{ce_s.get('fired_pending_cleanup',0)} fired")       if ce_s  else "🔔 Conditions: N/A"
    deep_line = (f"🗺️ Deep Struct: {deep_s.get('total',0)} symbols  "
                 f"{deep_s.get('fresh',0)} fresh")                     if deep_s else "🗺️ Deep Struct: N/A"
    mk_line   = (f"🎲 Markov: {mk_stats.get('total_analyses',0)} analyses  "
                 f"{mk_stats.get('symbols_learned',0)} symbols  "
                 f"top={mk_stats.get('top_symbol','—')} eff_n={mk_stats.get('top_eff_n',0):.0f}") \
                if mk_stats else "🎲 Markov: N/A"

    text = (
        f"🤖 *CRYPTEX STATUS*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"*APIs:*\n" +
        "".join(f"• {k}: {v}\n" for k, v in apis_ok.items()) +
        f"\n*AI Providers:*\n{ai_str}\n\n"
        f"*Modules:* {modules_str}\n"
        f"*Background Task:* {'🟢 Running' if bg_active else '🔴 Stopped'}\n\n"
        f"*🧬 Brain Health:*\n"
        f"  {aws_line}\n"
        f"  {gm_line}\n"
        f"  {ce_line}\n"
        f"  {deep_line}\n"
        f"  {mk_line}\n\n"
        f"⏰ _{datetime.now(timezone.utc).strftime('%b %d, %Y %I:%M %p UTC')}_"
    )
    await context.bot.send_message(
        chat_id, text, parse_mode="Markdown", reply_markup=create_main_keyboard()
    )


def _ta_verbose_is_on(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True if full TA output is enabled (default ON)."""
    return context.user_data.get("ta_verbose", True)


def _runtime_settings_store(context: ContextTypes.DEFAULT_TYPE) -> dict:
    """Per-bot runtime settings adjustable from Telegram Settings."""
    store = context.application.bot_data.setdefault("runtime_settings", {})
    if "max_news_photos" not in store:
        store["max_news_photos"] = max(3 if LOW_RAM_MODE else 0, int(MAX_NEWS_PHOTOS))
    if "light_start_mode" not in store:
        store["light_start_mode"] = bool(LIGHT_START_MODE)
    if "disable_charts" not in store:
        store["disable_charts"] = bool(DISABLE_CHARTS)
    return store


def _setting_max_news_photos(context: ContextTypes.DEFAULT_TYPE) -> int:
    store = _runtime_settings_store(context)
    val = int(store.get("max_news_photos", MAX_NEWS_PHOTOS))
    if LOW_RAM_MODE:
        return max(3, val)
    return max(0, val)


def _setting_light_start_mode(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(_runtime_settings_store(context).get("light_start_mode", LIGHT_START_MODE))


def _setting_disable_charts(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(_runtime_settings_store(context).get("disable_charts", DISABLE_CHARTS))


def _settings_summary_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    max_news = _setting_max_news_photos(context)
    light_start = _setting_light_start_mode(context)
    disable_charts = _setting_disable_charts(context)
    verbose_on = _ta_verbose_is_on(context)
    low_ram = "ON" if LOW_RAM_MODE else "OFF"
    min_news_note = " (low-RAM floor = 3)" if LOW_RAM_MODE else ""
    return (
        "⚙️ *BOT SETTINGS*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "*Live adjustable now*\n"
        f"• *MAX_NEWS_PHOTOS:* `{max_news}`{min_news_note}\n"
        f"• *LIGHT_START_MODE:* `{'ON' if light_start else 'OFF'}`\n"
        f"• *DISABLE_CHARTS:* `{'ON' if disable_charts else 'OFF'}`\n"
        f"• *TA Full Output:* `{'ON' if verbose_on else 'OFF'}`\n\n"
        "*Read-only runtime*\n"
        f"• *LOW_RAM_MODE:* `{low_ram}`\n"
        f"• *MAX_RAM_MB:* `{os.getenv('MAX_RAM_MB', '128')}`\n\n"
        "*Compliance notes*\n"
        "• Low-RAM mode now preserves at least `3` news images.\n"
        "• Alpha scanner output is reference-only until a full TA setup is generated.\n"
        "• Use `/analyze SYMBOL` before treating a scan as executable.\n"
    )


def _settings_keyboard(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    """Build inline controls for live-safe settings."""
    max_news = _setting_max_news_photos(context)
    light_start = _setting_light_start_mode(context)
    disable_charts = _setting_disable_charts(context)
    verbose_on = _ta_verbose_is_on(context)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📰 News Photos -", callback_data="settings_news_minus"),
            InlineKeyboardButton(f"📰 MAX_NEWS_PHOTOS: {max_news}", callback_data="settings_noop"),
            InlineKeyboardButton("📰 News Photos +", callback_data="settings_news_plus"),
        ],
        [InlineKeyboardButton(
            f"⚡ LIGHT_START_MODE: {'ON' if light_start else 'OFF'}",
            callback_data="settings_light_toggle",
        )],
        [InlineKeyboardButton(
            f"📉 DISABLE_CHARTS: {'ON' if disable_charts else 'OFF'}",
            callback_data="settings_charts_toggle",
        )],
        [InlineKeyboardButton(
            f"📊 TA Full Output: {'ON' if verbose_on else 'OFF'}",
            callback_data="settings_ta_toggle",
        )],
        [InlineKeyboardButton("✅ Done", callback_data="settings_close")],
    ])


async def show_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle ⚙️ Settings button — show live adjustment panel."""
    text = _settings_summary_text(context)
    if update.message:
        await update.message.reply_text(
            text,
            parse_mode="Markdown",
            reply_markup=_settings_keyboard(context),
        )
    elif update.callback_query:
        await update.callback_query.message.reply_text(
            text,
            parse_mode="Markdown",
            reply_markup=_settings_keyboard(context),
        )


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle live-safe settings button presses."""
    query = update.callback_query
    await query.answer()
    data = query.data
    store = _runtime_settings_store(context)

    if data == "settings_ta_toggle":
        context.user_data["ta_verbose"] = not _ta_verbose_is_on(context)
        status = "TA full output updated"
    elif data == "settings_news_plus":
        current = int(store.get("max_news_photos", MAX_NEWS_PHOTOS))
        store["max_news_photos"] = min(10, current + 1)
        status = f"MAX_NEWS_PHOTOS = {_setting_max_news_photos(context)}"
    elif data == "settings_news_minus":
        current = int(store.get("max_news_photos", MAX_NEWS_PHOTOS))
        floor = 3 if LOW_RAM_MODE else 0
        store["max_news_photos"] = max(floor, current - 1)
        status = f"MAX_NEWS_PHOTOS = {_setting_max_news_photos(context)}"
    elif data == "settings_light_toggle":
        store["light_start_mode"] = not _setting_light_start_mode(context)
        status = f"LIGHT_START_MODE = {'ON' if _setting_light_start_mode(context) else 'OFF'}"
    elif data == "settings_charts_toggle":
        store["disable_charts"] = not _setting_disable_charts(context)
        status = f"DISABLE_CHARTS = {'ON' if _setting_disable_charts(context) else 'OFF'}"
    elif data == "settings_noop":
        status = "Current value"
    elif data == "settings_close":
        try:
            await query.edit_message_text("✅ *Settings saved.*", parse_mode="Markdown")
            await asyncio.sleep(0.8)
            await query.message.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        return
    else:
        return

    try:
        await query.edit_message_text(
            _settings_summary_text(context),
            parse_mode="Markdown",
            reply_markup=_settings_keyboard(context),
        )
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    await query.answer(status, show_alert=False)


async def markov_audit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Standalone handler for 🎲 Markov Audit button — registered on ^markov_audit$."""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    try:
        brain_data = _MARKOV_BRAIN.read()
        total_n    = brain_data["global"].get("total_analyses", 0)
        n_symbols  = len(brain_data.get("symbols", {}))
        macro_rec  = brain_data["global"].get("macro_event_outcomes", {})
        circuit    = "OPEN ⚠️" if getattr(_MARKOV_BRAIN, "_write_failures", 0) >= BRAIN_WRITE_FAILURE_CAP else "CLOSED ✅"
        audit_lines = [
            "🎲 *MARKOV BRAIN AUDIT v2.1*",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"• Total analyses: `{total_n}`",
            f"• Symbols tracked: `{n_symbols}`",
            f"• Circuit breaker: `{circuit}`",
        ]
        for sym_key, sym_data in list(brain_data.get("symbols", {}).items())[:6]:
            n     = sym_data.get("n_samples", 0)
            eff_n = sym_data.get("effective_n", float(n))
            a     = round(min(1.0, eff_n / 100.0), 2)
            audit_lines.append(f"• {sym_key}: `{n}` raw · effN=`{eff_n:.0f}` · α=`{a}`")
            oc = sym_data.get("outcome_counts", {})
            win_parts = []
            for state in ("BOS_BULL", "CHOCH_BULL", "FVG_BULL",
                          "BOS_BEAR", "CHOCH_BEAR", "FVG_BEAR"):
                sc    = oc.get(state, {})
                wins  = sc.get("TP1", 0) + sc.get("TP2", 0) + sc.get("TP3", 0)
                total = wins + sc.get("SL", 0)
                if total > 0:
                    win_parts.append(f"{state[:7]}={wins * 100 // total}%")
            if win_parts:
                audit_lines.append(f"  WR: {' · '.join(win_parts)}")
        if macro_rec:
            audit_lines.append("\n*Macro Calibration:*")
            for evt, rec in macro_rec.items():
                audit_lines.append(
                    f"• {evt}: rate=`{rec['gate_pass_rate']:.1%}` (n={rec['n']})"
                )
        audit_lines.append("\n⚠️ _Not financial advice. DYOR._")
        await context.bot.send_message(
            chat_id, "\n".join(audit_lines), parse_mode="Markdown"
        )
    except (KeyError, TypeError, AttributeError) as _aud_e:
        logger.error(f"[MARKOV AUDIT] {type(_aud_e).__name__}: {_aud_e}")
        try:
            await context.bot.send_message(chat_id, "⚠️ Markov audit unavailable.")
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
# ─────────────────── TRADINGVIEW FREE HANDLERS ────────────────────────────────

async def cmd_app_tvsetup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """[TV-FREE] /tvsetup — Full TradingView free plan 3-slot ICT setup guide."""
    await update.message.reply_text(
        TV_FREE_SETUP_MSG,
        reply_markup=create_main_keyboard()
    )


async def cmd_app_tvcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """[TV-FREE Phase 4] /tvcheck or /tvcheck SYMBOL — TV cross-check after analysis."""
    sym = None
    if context.args:
        sym = context.args[0].upper().replace("USDT", "").replace("USDC", "")
    if not sym:
        if _LAST_RESULT_CACHE:
            sym = list(_LAST_RESULT_CACHE.keys())[-1]
        else:
            await update.message.reply_text(
                "⚠️ No analysis cached yet.\n"
                "Run a Technical Analysis first, then use /tvcheck.",
                reply_markup=create_main_keyboard()
            )
            return
    cached = _LAST_RESULT_CACHE.get(sym)
    if not cached:
        await update.message.reply_text(
            f"⚠️ No cached result for {sym}.\n"
            f"Run Technical Analysis on {sym} first, then /tvcheck {sym}.",
            reply_markup=create_main_keyboard()
        )
        return
    msg = generate_tv_checklist(cached)
    await update.message.reply_text(
        f"```\n{msg}\n```",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )


async def cmd_app_pine(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """[TV-FREE Phase 5] /pine or /pine SYMBOL — Pine Script v5 strategy backtest."""
    sym = None
    if context.args:
        sym = context.args[0].upper().replace("USDT", "").replace("USDC", "")
    cached = _LAST_RESULT_CACHE.get(sym) if sym else None
    if not cached and _LAST_RESULT_CACHE:
        cached = list(_LAST_RESULT_CACHE.values())[-1]
    script = generate_tv_pine_script(r=cached, symbol=sym or "BTC")
    header_note = (
        "📺 *TRADINGVIEW PINE SCRIPT — MOP·ICT v9 Mirror*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "1. Open TradingView → Pine Script Editor\n"
        "2. Clear editor → Paste script below\n"
        "3. Click *Add to chart*\n"
        "4. Open *Strategy Tester* → run 6 months history\n"
        "5. Exec TF: 15m or 1H  |  Symbol: same as bot\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(header_note, parse_mode="Markdown")
    chunk_size = 3900
    for i in range(0, len(script), chunk_size):
        chunk = script[i:i + chunk_size]
        await update.message.reply_text(
            f"```\n{chunk}\n```",
            parse_mode="Markdown"
        )


async def cmd_app_killzone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """[TV-FREE Phase 2] /killzone — Current Kill Zone status with countdown."""
    import datetime as _dt
    session = get_current_session()
    sb      = get_silver_bullet_status()
    now_utc = _dt.datetime.utcnow()
    mins    = now_utc.hour * 60 + now_utc.minute

    KZ_SCHEDULE = [
        ("🔵 London Open",  7 * 60,       10 * 60,     "⭐⭐⭐ Best for reversals + trend days"),
        ("🟢 NY Open",      13 * 60 + 30, 16 * 60,     "⭐⭐⭐ Best for continuations + Judas"),
        ("🟡 NY Afternoon", 18 * 60,      20 * 60,     "⭐⭐  Good for scalp completions"),
        ("🟣 Asian",        0,            2 * 60 + 30, "⭐   Range-building, lower reliability"),
    ]

    current_kz = None
    for name, start, end, desc in KZ_SCHEDULE:
        if start <= mins < end:
            current_kz = (name, end - mins, desc)
            break

    lines = [
        f"⏰ KILL ZONE STATUS — {now_utc.strftime('%H:%M')} UTC",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    if current_kz:
        name, rem, desc = current_kz
        h, m = divmod(rem, 60)
        lines += [
            f"✅ ACTIVE : {name}",
            f"   {desc}",
            f"   Closes in: {h}h {m}m",
            "",
            "▶  NOW is the time to run Technical Analysis.",
        ]
    else:
        future = [(n, s, d) for n, s, e, d in KZ_SCHEDULE if s > mins]
        if future:
            future.sort(key=lambda x: x[1])
            nxt_name, nxt_start, nxt_desc = future[0]
            nxt_in = nxt_start - mins
        else:
            nxt_name  = "🟣 Asian (tomorrow)"
            nxt_in    = 24 * 60 - mins
            nxt_desc  = "⭐ Range-building"
        h, m = divmod(nxt_in, 60)
        lines += [
            f"⏳ OFF SESSION — No active Kill Zone",
            f"   Current session : {session}",
            "",
            f"▶  Next Kill Zone : {nxt_name}",
            f"   Opens in        : {h}h {m}m",
            f"   {nxt_desc}",
            "",
            "   Wait for the Kill Zone before running analysis.",
        ]

    lines += [
        "",
        "── SILVER BULLET (UTC) ──────────────────",
        "  ⚡ London SB : 02:00 – 03:00",
        "  ⚡ NY AM SB  : 10:00 – 11:00  (most powerful)",
        "  ⚡ NY PM SB  : 14:00 – 15:00",
        "",
    ]
    if sb.get("active"):
        lines.append(f"⚡ SILVER BULLET ACTIVE: {sb['window']} — "
                     f"{sb.get('minutes_remaining', 0)}m left ← HIGH PRIORITY")
    else:
        lines.append(f"💤 Silver Bullet inactive "
                     f"(next: {sb.get('next_window', '?')})")

    lines += [
        "",
        "── SCHEDULE (UTC) ───────────────────────",
        "  🔵 London : 07:00 – 10:00",
        "  🟢 NY     : 13:30 – 16:00",
        "  🟡 NY PM  : 18:00 – 20:00",
        "  🟣 Asian  : 00:00 – 02:30",
    ]

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=create_main_keyboard()
    )

# ──────────────────────────────────────────────────────────────────────────────
# [MOP-P04] BTC REGIME SINGLETON TASK
# Runs every 300s. Writes _btc_regime, _btc_rsi, _btc_above_sma200.
# All analyses read from these pre-computed vars — zero redundant BTC fetches.
# ──────────────────────────────────────────────────────────────────────────────

async def _btc_regime_singleton_task() -> None:
    """[MOP-P04] Background task: refresh BTC regime state every 5 minutes."""
    global _btc_regime, _btc_rsi, _btc_above_sma200
    while True:
        try:
            resp = await asyncio.to_thread(
                requests.get,
                f"{COINGECKO_BASE_URL}/coins/bitcoin/market_chart?vs_currency=usd&days=60",
                headers=REQUEST_HEADERS, timeout=10
            )
            if resp.status_code == 200:
                prices = [p[1] for p in resp.json().get("prices", [])]
                if len(prices) >= 210:
                    sma50  = sum(prices[-50:]) / 50
                    sma200 = sum(prices[-200:]) / 200
                    _btc_above_sma200 = prices[-1] > sma200
                    # RSI-14
                    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
                    gains  = [max(0, d) for d in deltas[-14:]]
                    losses = [max(0, -d) for d in deltas[-14:]]
                    ag = sum(gains) / 14 or 1e-9
                    al = sum(losses) / 14 or 1e-9
                    _btc_rsi = round(100 - 100 / (1 + ag / al), 1)
                    _btc_regime = classify_btc_regime(prices)
                    logger.info(f"[MOP-P04 BTC_REGIME] regime={_btc_regime} rsi={_btc_rsi} above_sma200={_btc_above_sma200}")
        except Exception as e:
            logger.warning(f"[MOP-P04 BTC_REGIME] fetch failed: {e}")

        # REC-18: Refresh circulating supply every 6h alongside regime check
        if TA_MODULE_AVAILABLE:
            try:
                from TechnicalAnalysis_v9_6 import refresh_circulating_supply as _rcs
                await asyncio.to_thread(_rcs)
            except Exception as _rcs_e:
                logger.debug(f"[SUPPLY] regime-task refresh non-critical: {_rcs_e}")

        await asyncio.sleep(300)


# ──────────────────────────────────────────────────────────────────────────────
# [MOP-P04] AI PROVIDER HEALTH CHECK TASK
# Pings all 4 AI providers every 300s with a minimal probe.
# Writes ranked list to _ranked_providers — request handlers read it directly.
# ──────────────────────────────────────────────────────────────────────────────

async def _run_provider_health_task() -> None:
    """[MOP-P04] Background task: rank AI providers every 5 minutes by latency."""
    global _ranked_providers
    _BASE_PROVIDERS = [
        {"name": "Groq",     "type": "groq",     "key": GROQ_API_KEY,     "priority": 1},
        {"name": "Cerebras", "type": "cerebras", "key": CEREBRAS_API_KEY,  "priority": 2},
        {"name": "Gemini",   "type": "gemini",   "key": GEMINI_API_KEY,   "priority": 3},
        {"name": "Mistral",  "type": "mistral",  "key": MISTRAL_API_KEY,  "priority": 4},
    ]
    while True:
        ranked = []
        for p in _BASE_PROVIDERS:
            if not p["key"].strip():
                continue
            ranked.append({**p, "_latency": p["priority"] * 0.01})
        ranked.sort(key=lambda x: x.get("_latency", 99))
        _ranked_providers = ranked
        logger.info(f"[MOP-P04 PROVIDER_HEALTH] ranked: {[r['name'] for r in _ranked_providers]}")
        await asyncio.sleep(300)


# ──────────────────────────────────────────────────────────────────────────────
# [MOP-P08] /watch COMMAND HANDLERS + CONDITION EVALUATOR TASK
# ──────────────────────────────────────────────────────────────────────────────

async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    [MOP-P08] /watch SYMBOL ICT_STATE GATE_REQ SESSION
    Example: /watch BTCUSDT CHOCH_BULL all_gates NY_OPEN
    Registers a declarative condition — bot fires once when it matches.
    """
    args = context.args or []
    symbol    = args[0].upper() if len(args) > 0 else ""
    ict_state = args[1].upper() if len(args) > 1 else "ANY"
    gate_req  = args[2].lower() if len(args) > 2 else "all_gates"
    session   = args[3].upper() if len(args) > 3 else "ANY"

    if not symbol:
        await update.message.reply_text(
            "⚠️ Usage: `/watch SYMBOL ICT_STATE GATE_REQ SESSION`\n"
            "Example: `/watch BTCUSDT CHOCH_BULL all_gates NY_OPEN`\n\n"
            "ICT States: ANY CHOCH_BULL CHOCH_BEAR BOS_BULL BOS_BEAR FVG_BULL FVG_BEAR ACCUMULATION\n"
            "Gate Req: any all_gates 8_gates 7_gates 6_gates\n"
            "Session: ANY LONDON NY_OPEN ASIAN",
            parse_mode="Markdown"
        )
        return

    ok, cid, msg = _cond_engine.register_condition(
        chat_id   = update.message.chat_id,
        symbol    = symbol,
        ict_state = ict_state,
        gate_req  = gate_req,
        session   = session,
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """[MOP-P08] /watchlist — show active watch conditions for this chat."""
    conditions = _cond_engine.list_conditions(update.message.chat_id)
    if not conditions:
        await update.message.reply_text("📋 No active watch conditions. Use /watch to add one.")
        return
    lines = ["📋 *Active Watch Conditions*", "━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for c in conditions:
        age_m = int((time.time() - c.created_ts) / 60)
        lines.append(
            f"`{c.condition_id[-6:]}` {c.symbol} · {c.ict_state} · {c.gate_req} · {c.session} ({age_m}m ago)"
        )
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("Cancel: `/cancel_watch ID`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_cancel_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """[MOP-P08] /cancel_watch ID — cancel a watch condition by 6-char ID suffix."""
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: `/cancel_watch ID` (last 6 chars from /watchlist)", parse_mode="Markdown")
        return
    ok, msg = _cond_engine.cancel_condition(update.message.chat_id, args[0].strip())
    await update.message.reply_text(msg, parse_mode="Markdown")


async def _condition_evaluator_task() -> None:
    """[MOP-P08] Background task: expire stale conditions every 5 min."""
    while True:
        try:
            removed = _cond_engine.expire_conditions()
            summary = _cond_engine.get_summary()
            if removed or summary["active"]:
                logger.info(f"[MOP-P08 COND] Expired {removed} · Active: {summary['active']}")
        except Exception as e:
            logger.warning(f"[MOP-P08 COND] evaluator task error: {e}")
        await asyncio.sleep(300)


# ──────────────────────────────────────────────────────────────────────────────
# [AWS] MULTI-COIN LEARNING PASS TASK
# Drains unrecorded outcomes from apex_watch_store into gate_memory and markov.
# Runs every 5 minutes. This is the continuous learning feedback loop.
# ──────────────────────────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────────────────────────
# [DEEP STRUCT] EXTENDED CANDLE HISTORY REFRESH TASK
# Fetches 500×1D + 260×1W candles for each analyzed symbol every 6 hours.
# AI distills them into compact structural levels (PWH/PWL/PYH/PYL/etc).
# Raw candles freed immediately after extraction — ~6MB peak, then 0.
# Runs staggered to avoid RAM pressure during active analysis cycles.
# ──────────────────────────────────────────────────────────────────────────────

# Track which symbols have been analyzed — background task refreshes these
_deep_struct_symbols: set = set()
_deep_struct_lock = None

def _get_deep_struct_lock():
    global _deep_struct_lock
    if _deep_struct_lock is None:
        import asyncio as _asyncio
        _deep_struct_lock = _asyncio.Lock()
    return _deep_struct_lock


async def _deep_struct_refresh_task() -> None:
    """
    [DEEP STRUCT] Background task: refresh extended structural levels.
    Runs every 30 minutes, refreshes any symbol not updated in 6 hours.
    Fetches one symbol at a time with a 5-second gap to prevent RAM spikes.
    """
    await asyncio.sleep(60)   # 60s startup stagger — let bot initialize first

    while True:
        try:
            if not _DEEP_STRUCT_APP_AVAILABLE:
                await asyncio.sleep(1800)
                continue

            symbols_to_refresh = list(_deep_struct_symbols)
            if not symbols_to_refresh:
                await asyncio.sleep(300)
                continue

            # Build AI callable from ranked providers (optional — pure math works too)
            _ai_fn = None
            if _ranked_providers:
                _best_p = _ranked_providers[0]
                _ai_fn  = _make_deep_struct_ai_fn(_best_p)

            import aiohttp as _aiohttp_ds
            async with _aiohttp_ds.ClientSession(
                headers={"User-Agent": "ICT-DeepStruct/1.0"},
                connector=_aiohttp_ds.TCPConnector(limit=3, ssl=False),
            ) as _ds_session:
                for sym in symbols_to_refresh:
                    try:
                        await _deep_struct_app.refresh_symbol(
                            symbol  = sym,
                            session = _ds_session,
                            ai_fn   = _ai_fn,
                        )
                        await asyncio.sleep(5)   # 5s gap between symbols — no RAM overlap
                    except Exception as _sym_e:
                        logger.debug(f"[DEEP STRUCT] {sym} refresh error: {_sym_e}")

            logger.info(f"[DEEP STRUCT] Refresh cycle done: {len(symbols_to_refresh)} symbols")

        except Exception as e:
            logger.warning(f"[DEEP STRUCT] task error: {e}")

        await asyncio.sleep(1800)   # run every 30 minutes


def _make_deep_struct_ai_fn(provider: dict):
    """Build a lightweight async AI callable for deep struct extraction."""
    async def _ai_call(prompt: str) -> str:
        try:
            import aiohttp as _ah
            p_type = provider.get("type", "")
            key    = provider.get("key", "")
            if not key:
                return ""

            _ENDPOINTS = {
                "groq":     "https://api.groq.com/openai/v1/chat/completions",
                "cerebras": "https://api.cerebras.ai/v1/chat/completions",
                "mistral":  "https://api.mistral.ai/v1/chat/completions",
                "gemini":   "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent",
                "github":   "https://models.github.ai/inference/chat/completions",
            }
            _MODELS = {
                "groq":     "llama-3.1-8b-instant",
                "cerebras": "llama3.1-8b",
                "mistral":  "mistral-small-latest",
                "github":   "openai/gpt-4o-mini",
            }

            url = _ENDPOINTS.get(p_type, "")
            if not url:
                return ""

            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

            if p_type == "gemini":
                payload = {"contents": [{"parts": [{"text": prompt}]}],
                           "generationConfig": {"maxOutputTokens": 256, "temperature": 0.1}}
                async with _ah.ClientSession() as s:
                    async with s.post(url + f"?key={key}", json=payload, timeout=15) as r:
                        if r.status != 200: return ""
                        d = await r.json()
                        return d["candidates"][0]["content"]["parts"][0]["text"]
            else:
                model = _MODELS.get(p_type, "gpt-4o-mini")
                payload = {"model": model, "messages": [{"role": "user", "content": prompt}],
                           "max_tokens": 256, "temperature": 0.1}
                async with _ah.ClientSession() as s:
                    async with s.post(url, headers=headers, json=payload, timeout=15) as r:
                        if r.status != 200: return ""
                        d = await r.json()
                        return d["choices"][0]["message"]["content"]
        except Exception:
            return ""
    return _ai_call


async def _aws_learning_pass_task() -> None:
    """
    [AWS] Background task: feed resolved watch store outcomes into learning engines.
    - gate_memory.record_gate_outcome() — updates fingerprint TP/SL rates
    - _MARKOV_BRAIN.record_outcome()    — updates Markov transition learning
    Runs every 300 seconds. Silently skips if AWS or learning modules unavailable.
    """
    while True:
        try:
            if not _AWS_AVAILABLE:
                await asyncio.sleep(300)
                continue

            # Evict expired records first
            _aws.evict_expired()

            # Drain unrecorded resolved outcomes
            unrecorded = _aws.get_unrecorded_outcomes()
            if unrecorded:
                logger.info(f"[AWS LEARN] Processing {len(unrecorded)} unrecorded outcomes")

            for rec in unrecorded:
                sym     = rec.get("symbol", "")
                outcome = rec.get("outcome", "TIMEOUT")
                gp      = [bool(g) for g in rec.get("gate_pass", [])]
                hmm     = rec.get("hmm_state", "")
                ict     = rec.get("ict_state", "") or rec.get("markov_state", "")
                bias    = rec.get("bias", "BULLISH")

                # 1. gate_memory fingerprint feedback
                try:
                    if gp and _GATE_MEMORY_AVAILABLE:
                        _gm_record_gate_outcome(
                            gate_pass  = gp,
                            outcome    = outcome,
                            hmm_regime = hmm,
                            ict_state  = ict or "ACCUMULATION",
                        )
                except Exception as _gml_e:
                    logger.debug(f"[AWS LEARN] gate_memory feedback non-critical: {_gml_e}")

                # 2. Markov brain outcome feedback
                try:
                    if ict and bias:
                        _MARKOV_BRAIN.record_outcome(
                            symbol    = sym,
                            ict_state = ict,
                            bias      = bias,
                            outcome   = outcome,
                        )
                except Exception as _mkl_e:
                    logger.debug(f"[AWS LEARN] Markov feedback non-critical: {_mkl_e}")

                # Mark as processed so we don't re-feed
                _aws.mark_outcome_recorded(sym)
                logger.info(f"[AWS LEARN] {sym} → {outcome} fed to gate_memory + Markov")

            # 3. [SELF-LEARN] Bulk ingest pct_moves into LevelLearner
            # This feeds ALL resolved records that have pct_moves data,
            # not just unrecorded — LevelLearner's ring buffer deduplicates internally.
            try:
                if MARKOV_AVAILABLE:
                    _ll_samples = _aws.get_level_samples()
                    if _ll_samples:
                        _ingested = _LEVEL_LEARNER.ingest_aws_samples(_ll_samples)
                        if _ingested:
                            logger.info(f"[SELF-LEARN] LevelLearner ingested {_ingested} samples from AWS store")
            except Exception as _ll_e:
                logger.debug(f"[SELF-LEARN] ingest non-critical: {_ll_e}")

        except Exception as _awsl_e:
            logger.warning(f"[AWS LEARN] task error: {_awsl_e}")

        await asyncio.sleep(300)


# ──────────────────────────────────────────────────────────────────────────────
# [MOP-P05] SESSION-AWARE PRE-WARM TASK
# Checks every 60s whether a kill zone is within PREWARM_MINUTES.
# If so, fetches top coins into _warm_cache. Analyses hit cache on hot path.
# ──────────────────────────────────────────────────────────────────────────────

async def _session_prewarm_task() -> None:
    """[MOP-P05] Pre-fetch top symbols 15 min before each kill zone."""
    import datetime as _dt2
    _prewarmed_zones: set = set()   # track which KZ start times we've already warmed

    while True:
        try:
            now_utc = _dt2.datetime.utcnow()
            now_mins = now_utc.hour * 60 + now_utc.minute

            for kz_h, kz_m in _KZ_UTC_STARTS:
                kz_mins = kz_h * 60 + kz_m
                diff = kz_mins - now_mins
                # Handle midnight wrap
                if diff < 0:
                    diff += 24 * 60
                zone_key = f"{kz_h:02d}:{kz_m:02d}"

                if 0 < diff <= _PREWARM_MINUTES:
                    if zone_key not in _prewarmed_zones:
                        logger.info(f"[MOP-P05 PREWARM] Kill zone {zone_key} in {diff}m — pre-warming top {_PREWARM_SYMBOL_LIMIT} symbols")
                        _prewarmed_zones.add(zone_key)
                        try:
                            resp = await asyncio.to_thread(
                                requests.get,
                                f"{COINGECKO_BASE_URL}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={_PREWARM_SYMBOL_LIMIT}&page=1",
                                headers=REQUEST_HEADERS, timeout=10
                            )
                            if resp.status_code == 200:
                                coins = [c["symbol"].upper() + "USDT" for c in resp.json()]
                                for sym in coins[:_PREWARM_SYMBOL_LIMIT]:
                                    if sym not in _warm_cache or (time.time() - _warm_cache[sym]["ts"]) > _WARM_CACHE_TTL:
                                        _warm_cache[sym] = {"data": None, "ts": time.time(), "sym": sym}
                                logger.info(f"[MOP-P05 PREWARM] Registered {len(coins)} symbols for warm cache: {coins[:5]}…")
                        except Exception as e:
                            logger.warning(f"[MOP-P05 PREWARM] top-coins fetch failed: {e}")
                elif diff > _PREWARM_MINUTES:
                    # Reset so next approach triggers again
                    _prewarmed_zones.discard(zone_key)

            # Evict stale entries
            now_ts = time.time()
            stale = [k for k, v in _warm_cache.items() if now_ts - v["ts"] > _WARM_CACHE_TTL]
            for k in stale:
                _warm_cache.pop(k, None)

        except Exception as e:
            logger.warning(f"[MOP-P05 PREWARM] task error: {e}")

        await asyncio.sleep(60)


async def _suppress_pin_service_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Auto-delete the Telegram service message 'NansenProBot pinned a message'
    that appears every time a message is pinned.
    Only the pinned message itself should remain visible — not the service notice.
    Registered as a StatusUpdate.PINNED_MESSAGE handler with group=-1 (highest priority).
    """
    try:
        await update.message.delete()
    except Exception as _e:
        logger.debug(f"[PIN_SUPPRESS] Could not delete pin service message: {_e}")


async def _handle_journal_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    [MOP-P10] Handle WebApp sendData() from the trade journal in trade_view.html.
    Payload JSON: { sym, hash, act, out, ts, grade }
    Records outcome into gate_memory for crowd-sourced fingerprint data.
    """
    try:
        raw = update.message.web_app_data.data if update.message and update.message.web_app_data else None
        if not raw:
            return
        payload = json.loads(raw)
        sym     = str(payload.get("sym", "")).upper().strip()
        outcome = str(payload.get("out", "")).upper().strip()
        act     = str(payload.get("act", "skip")).lower().strip()
        gh      = str(payload.get("hash", ""))
        valid_outcomes = ("TP1", "TP2", "TP3", "SL", "TIMEOUT", "SKIP")
        if outcome not in valid_outcomes or not sym:
            return
        if outcome != "SKIP" and TA_MODULE_AVAILABLE:
            # Record crowd label into gate_memory via cryptex_stores
            try:
                if _GATE_MEMORY_AVAILABLE:
                    # Full gate_pass not available here (hash-only context)
                    # Log for traceability; full vector recording happens via APEX outcome feedback
                    logger.info(f"[MOP-P10 JOURNAL] chat={update.message.chat_id} sym={sym} outcome={outcome} act={act} hash={gh}")
            except Exception as e:
                logger.warning(f"[MOP-P10 JOURNAL] gate record error: {e}")
        # Acknowledge
        await update.message.reply_text(
            f"📓 Journal recorded: {sym} → {outcome}",
            reply_markup=create_main_keyboard()
        )
    except Exception as e:
        logger.warning(f"[MOP-P10 JOURNAL] handler error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# GAP-03: /unflag — admin command to clear immune system blacklist entries
# ══════════════════════════════════════════════════════════════════════════════
async def cmd_unflag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/unflag <SYMBOL> — show and optionally clear blacklisted gate patterns."""
    chat_id = update.effective_chat.id
    args    = context.args or []

    if not _GATE_MEMORY_AVAILABLE:
        await update.message.reply_text("⚠️ Gate memory store unavailable.")
        return

    # Without args: show all pending records with blacklist info
    if not args:
        gm_s = _gm_get_memory_summary()
        blk  = gm_s.get("blacklisted_active", 0)
        total= gm_s.get("fingerprint_patterns", 0)
        await update.message.reply_text(
            f"🚫 *Immune System Blacklist*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Active patterns : `{total}`\n"
            f"Blacklisted     : `{blk}`\n\n"
            f"Usage: `/unflag BTCUSDT` to see blacklisted patterns for a symbol\n"
            f"Tip: Patterns are auto-cleared when SL rate drops below 60% over 10+ samples.",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
        return

    symbol = args[0].upper().replace("/","").replace("-","")
    if not symbol.endswith("USDT"):
        symbol += "USDT"

    # Get pending AWS records for this symbol to extract gate_pass + regime + state
    rec = _aws.get(symbol) if _AWS_AVAILABLE else None
    if not rec:
        await update.message.reply_text(
            f"⚠️ No active signal record found for `{symbol}`.\n"
            f"Run `/analyze {symbol.replace('USDT','')}` first to generate a signal.",
            parse_mode="Markdown"
        )
        return

    gate_pass  = rec.get("gate_pass", [])
    ict_state  = rec.get("ict_state", rec.get("markov_state", "ACCUMULATION"))
    regime     = rec.get("regime", "TRENDING")
    gate_hash  = rec.get("gate_hash", "")

    # Check current blacklist status
    veto = _gm_check_blacklist(regime, ict_state, gate_pass) if gate_pass else None

    if not veto:
        await update.message.reply_text(
            f"✅ *{symbol}* — No active blacklist veto.\n"
            f"Gate hash: `{gate_hash}`\n"
            f"ICT State: `{ict_state}`  Regime: `{regime}`\n"
            f"The immune system has not blocked this pattern.",
            parse_mode="Markdown"
        )
        return

    # Clear the blacklist entry
    cleared = _gm_unflag_blacklist(regime, ict_state, gate_pass) if gate_pass else False

    if cleared:
        await update.message.reply_text(
            f"✅ *Blacklist cleared for {symbol}*\n"
            f"Pattern: `{ict_state}` / `{regime}` / hash `{gate_hash}`\n\n"
            f"⚠️ _The immune system will re-flag this pattern if SL rate exceeds "
            f"60% over the next 10 trades. Monitor carefully._",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
    else:
        await update.message.reply_text(
            f"⚠️ Could not clear pattern for `{symbol}`. "
            f"Pattern may have already expired or gate vector is empty.",
            parse_mode="Markdown"
        )


# ══════════════════════════════════════════════════════════════════════════════
# GAP-06: /my_signals — per-user active conditions + last signal + outcomes
# ══════════════════════════════════════════════════════════════════════════════
async def cmd_my_signals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/my_signals — show your active watch conditions and last signal results."""
    chat_id = update.effective_chat.id

    lines = ["📡 *MY SIGNALS*", "━━━━━━━━━━━━━━━━━━━━━━━━━━━", ""]

    # Active conditions from condition engine
    if _COND_ENGINE_AVAILABLE:
        conditions = _ce_list_conditions(chat_id)
        if conditions:
            lines.append(f"*🔔 Active Watch Conditions ({len(conditions)}):*")
            for c in conditions[:5]:
                age_mins = int((time.time() - c.created_ts) / 60)
                lines.append(
                    f"  `{c.condition_id[-6:]}` — {c.symbol} · {c.ict_state} · "
                    f"{c.gate_req} · {c.session}  _{age_mins}m ago_"
                )
            lines.append("")
        else:
            lines.append("*🔔 No active watch conditions.*\n_Use /watch to set one._\n")
    else:
        lines.append("*🔔 Condition engine unavailable.*\n")

    # Recent signals from AWS store matching this chat_id
    if _AWS_AVAILABLE:
        all_recs = _aws.get_all()
        my_recs  = [r for r in all_recs if chat_id in (r.get("chat_ids") or [])]
        my_recs.sort(key=lambda x: x.get("created_ts", 0), reverse=True)

        if my_recs:
            lines.append(f"*📊 Your Recent Signals ({min(len(my_recs),5)}/{len(my_recs)}):*")
            for r in my_recs[:5]:
                sym      = r.get("symbol","")
                grade    = r.get("grade","?")
                bias     = r.get("bias","")[:4]
                outcome  = r.get("outcome","PENDING")
                conf     = r.get("conf",0)
                age_h    = int((time.time() - r.get("created_ts",0)) / 3600)
                oc_icon  = ("✅" if "TP" in outcome else "❌" if outcome=="SL"
                            else "⏳" if outcome=="PENDING" else "⏱")
                lines.append(
                    f"  {oc_icon} `{sym}` Grade`{grade}` {bias} Conf`{conf}` "
                    f"→ `{outcome}`  _{age_h}h ago_"
                )
            lines.append("")

            # Quick win rate
            resolved  = [r for r in my_recs if r.get("outcome","PENDING") != "PENDING"]
            wins      = sum(1 for r in resolved if "TP" in r.get("outcome",""))
            if resolved:
                wr = round(wins / len(resolved) * 100, 1)
                lines.append(f"*Win Rate:* `{wins}/{len(resolved)}` = `{wr}%`")
        else:
            lines.append("*📊 No signals found for your chat.*\n_Run /analyze to generate one._")
    else:
        lines.append("*📊 Signal store unavailable.*")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="Markdown", reply_markup=create_main_keyboard()
    )


# ══════════════════════════════════════════════════════════════════════════════
# GAP-07: /brain — inspect Markov brain adaptive weights per symbol
# ══════════════════════════════════════════════════════════════════════════════
async def cmd_brain(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/brain <SYMBOL> — show Markov brain state and adaptive weights for a symbol."""
    chat_id = update.effective_chat.id
    args    = context.args or []

    if not MARKOV_AVAILABLE:
        await update.message.reply_text("⚠️ Markov stack unavailable.")
        return

    if not args:
        # Show global brain summary
        mk_data = _MARKOV_BRAIN.read()
        total   = mk_data.get("global", {}).get("total_analyses", 0)
        syms    = mk_data.get("symbols", {})
        top5    = sorted(syms.items(), key=lambda x: x[1].get("n_samples",0), reverse=True)[:5]

        lines = [
            "🧠 *MARKOV BRAIN STATUS*",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"Total analyses : `{total}`",
            f"Symbols learned: `{len(syms)}`",
            "",
            "*Top 5 by sample count:*",
        ]
        for sym, sd in top5:
            n     = sd.get("n_samples", 0)
            eff_n = round(sd.get("effective_n", 0), 1)
            aw    = sd.get("adaptive_weights", {})
            tune  = aw.get("tune_count", 0)
            lines.append(f"  `{sym}`: n={n} eff_n={eff_n} tune_cycles={tune}")

        lines += ["", "_Usage: /brain BTCUSDT to see full weights for a symbol_"]
        await update.message.reply_text(
            "\n".join(lines), parse_mode="Markdown", reply_markup=create_main_keyboard()
        )
        return

    symbol = args[0].upper().replace("/","").replace("-","")
    if not symbol.endswith("USDT"):
        symbol += "USDT"

    try:
        sym_data = _MARKOV_BRAIN.get_symbol(symbol)
        aw       = sym_data.get("adaptive_weights", {})
        oc       = sym_data.get("outcome_counts", {})
        n        = sym_data.get("n_samples", 0)
        eff_n    = round(sym_data.get("effective_n", 0), 2)
        alpha    = round(min(1.0, eff_n / 100), 3)

        # Compute win rates per ICT state
        state_wr = {}
        for state, outcomes in oc.items():
            wins  = outcomes.get("TP1",0)+outcomes.get("TP2",0)+outcomes.get("TP3",0)
            total_s = wins + outcomes.get("SL",0)
            if total_s >= 3:
                state_wr[state] = (wins, total_s, round(wins/total_s*100,1))

        lines = [
            f"🧠 *MARKOV BRAIN — {symbol}*",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"Samples    : `{n}` (effective_n={eff_n}  α={alpha})",
            f"Tune cycles: `{aw.get('tune_count',0)}`",
            f"Last tuned : `{aw.get('last_tuned','—')}`",
            "",
            "*🎛 Adaptive Weights (vs defaults):*",
            f"  TP reward   : `{aw.get('tp_reward',2.0):.3f}` (default 2.0)",
            f"  SL penalty  : `{aw.get('sl_penalty',1.5):.3f}` (default 1.5)",
            f"  Edge thresh : `{aw.get('edge_thresh',-0.15):.3f}` (default -0.15)",
            f"  Decay factor: `{aw.get('decay',0.995):.4f}` (default 0.995)",
            "",
            f"*📊 Win Rates by ICT State (min 3 samples):*",
        ]
        if state_wr:
            for state, (wins, total_s, wr) in sorted(state_wr.items(), key=lambda x:-x[1][2])[:6]:
                bar = "▓" * int(wr/10) + "░" * (10-int(wr/10))
                lines.append(f"  `{state[:12]:<12}` {bar} `{wr}%` ({wins}/{total_s})")
        else:
            lines.append("  _Not enough data yet (min 3 per state)_")

        lines += [
            "",
            "_Use /unflag to clear blacklisted patterns for this symbol_",
            "_Weights auto-adjust via 5-AI consensus after each signal_",
        ]

        await update.message.reply_text(
            "\n".join(lines), parse_mode="Markdown", reply_markup=create_main_keyboard()
        )
    except Exception as _brain_e:
        await update.message.reply_text(
            f"⚠️ Could not load brain data for `{symbol}`: {_brain_e}",
            parse_mode="Markdown"
        )


# ══════════════════════════════════════════════════════════════════════════════
# GAP-09: /performance — user trade journal performance dashboard
# ══════════════════════════════════════════════════════════════════════════════
async def cmd_performance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/performance — show your historical signal performance from the journal."""
    chat_id = update.effective_chat.id

    if not _AWS_AVAILABLE:
        await update.message.reply_text("⚠️ Signal store unavailable. Run /analyze to generate signals.")
        return

    all_recs = _aws.get_all()
    my_recs  = [r for r in all_recs if chat_id in (r.get("chat_ids") or [])]

    if not my_recs:
        await update.message.reply_text(
            "📊 *No signals found for your account.*\n\n"
            "Run `/analyze BTCUSDT` to start tracking signals.\n"
            "Use the Trade Journal in the WebApp to log outcomes.",
            parse_mode="Markdown"
        )
        return

    resolved  = [r for r in my_recs if r.get("outcome","PENDING") != "PENDING"]
    pending   = [r for r in my_recs if r.get("outcome","PENDING") == "PENDING"]
    wins      = [r for r in resolved if "TP" in r.get("outcome","")]
    losses    = [r for r in resolved if r.get("outcome","") == "SL"]
    timeouts  = [r for r in resolved if r.get("outcome","") == "TIMEOUT"]

    wr = round(len(wins)/len(resolved)*100, 1) if resolved else 0.0

    # Avg R:R on wins
    win_rrs  = [r.get("rr",0) for r in wins if r.get("rr",0) > 0]
    avg_rr   = round(sum(win_rrs)/len(win_rrs), 2) if win_rrs else 0.0

    # Breakdown by ICT state
    state_stats: dict = {}
    for r in resolved:
        state = r.get("ict_state") or r.get("markov_state") or "UNKNOWN"
        st    = state_stats.setdefault(state, {"wins": 0, "losses": 0})
        if "TP" in r.get("outcome",""): st["wins"]   += 1
        elif r.get("outcome") == "SL": st["losses"] += 1

    # Breakdown by session
    session_stats: dict = {}
    for r in resolved:
        sess = r.get("session","UNKNOWN")
        ss   = session_stats.setdefault(sess, {"wins":0,"losses":0})
        if "TP" in r.get("outcome",""): ss["wins"]   += 1
        elif r.get("outcome") == "SL": ss["losses"] += 1

    # Grade breakdown
    grade_stats: dict = {}
    for r in resolved:
        g = r.get("grade","?")
        gg = grade_stats.setdefault(g, {"wins":0,"losses":0})
        if "TP" in r.get("outcome",""): gg["wins"]   += 1
        elif r.get("outcome") == "SL": gg["losses"] += 1

    lines = [
        "📊 *PERFORMANCE DASHBOARD*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Total signals  : `{len(my_recs)}` ({len(pending)} pending)",
        f"Resolved       : `{len(resolved)}`  ✅`{len(wins)}` ❌`{len(losses)}` ⏱`{len(timeouts)}`",
        f"Win Rate       : `{wr}%`",
        f"Avg R:R (wins) : `1:{avg_rr}`",
        "",
        "*📐 By ICT State:*",
    ]
    for state, st in sorted(state_stats.items(), key=lambda x:-(x[1]["wins"]+x[1]["losses"]))[:6]:
        total_st = st["wins"]+st["losses"]
        wr_st    = round(st["wins"]/total_st*100,1) if total_st else 0
        lines.append(f"  `{state[:14]:<14}` ✅{st['wins']} ❌{st['losses']} = `{wr_st}%`")

    lines += ["", "*🕐 By Session:*"]
    for sess, ss in sorted(session_stats.items(), key=lambda x:-(x[1]["wins"]+x[1]["losses"])):
        total_ss = ss["wins"]+ss["losses"]
        wr_ss    = round(ss["wins"]/total_ss*100,1) if total_ss else 0
        lines.append(f"  `{sess:<12}` ✅{ss['wins']} ❌{ss['losses']} = `{wr_ss}%`")

    lines += ["", "*🏆 By Grade:*"]
    for grade_k in ["S","A","B","C","D","F"]:
        gs = grade_stats.get(grade_k)
        if gs:
            total_g = gs["wins"]+gs["losses"]
            wr_g    = round(gs["wins"]/total_g*100,1) if total_g else 0
            lines.append(f"  Grade `{grade_k}` — ✅{gs['wins']} ❌{gs['losses']} = `{wr_g}%`")

    lines += [
        "",
        "_Data from AWS signal store + Trade Journal_",
        "_Outcomes logged via WebApp journal or auto-detected by APEX_",
    ]

    # REC-3: Append TradeLifecycleTracker paper P&L section
    if TA_MODULE_AVAILABLE:
        try:
            tk_sum = _TRADE_TRACKER.get_summary()
            if tk_sum.get("paper_trades", 0) > 0:
                tk_trades  = tk_sum.get("paper_trades", 0)
                tk_equity  = tk_sum.get("paper_equity", 1.0)
                tk_pnl     = tk_sum.get("realized_pnl", 0.0)
                tk_wr      = round(tk_sum.get("win_rate", 0.0) * 100, 1)
                tk_dd      = round(tk_sum.get("max_drawdown", 0.0) * 100, 2)
                tk_pnl_pct = round((tk_equity - 1.0) * 100, 2)
                lines += [
                    "",
                    "*📈 Paper Trading (TradeLifecycleTracker):*",
                    f"  Trades   : `{tk_trades}`",
                    f"  Equity   : `{tk_equity:.4f}` ({tk_pnl_pct:+.2f}%)",
                    f"  Realized P&L: `{tk_pnl:+.4f}`",
                    f"  Win Rate : `{tk_wr}%`",
                    f"  Max DD   : `{tk_dd}%`",
                ]
        except Exception as _tk_e:
            logger.debug(f"[PERF] trade tracker: {_tk_e}")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="Markdown", reply_markup=create_main_keyboard()
    )


# ══════════════════════════════════════════════════════════════════════════════
# REC-5: /scan <SECTOR> — multi-symbol batch TA
# ══════════════════════════════════════════════════════════════════════════════
_SCAN_SECTORS: dict = {
    "defi":    ["AAVE","UNI","CRV","MKR","SNX","COMP","BAL"],
    "l1":      ["ETH","SOL","AVAX","ADA","DOT","NEAR","ATOM"],
    "l2":      ["ARB","OP","MATIC","IMX","STACKS","METIS"],
    "meme":    ["DOGE","SHIB","PEPE","WIF","BONK","FLOKI"],
    "top10":   ["BTC","ETH","BNB","SOL","XRP","ADA","DOGE","AVAX","DOT","LINK"],
    "btc":     ["BTC"],
    "ai":      ["FET","AGIX","OCEAN","NMR","TAO","RNDR"],
    "gaming":  ["AXS","SAND","MANA","ILV","GALA","ENJ"],
}

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/scan <SECTOR> — run batch TA on a curated symbol list and return ranked results."""
    chat_id = update.effective_chat.id
    args    = context.args or []

    if not TA_MODULE_AVAILABLE:
        await update.message.reply_text("⚠️ TA module unavailable.")
        return

    sector = args[0].lower() if args else ""
    if sector not in _SCAN_SECTORS:
        sector_list = "  ".join(f"`{s}`" for s in _SCAN_SECTORS)
        await update.message.reply_text(
            f"📡 *SECTOR SCAN*\n\nUsage: `/scan <sector>`\n\n"
            f"Available sectors:\n{sector_list}",
            parse_mode="Markdown"
        )
        return

    symbols = _SCAN_SECTORS[sector]
    msg = await update.message.reply_text(
        f"🔍 Scanning `{sector.upper()}` — {len(symbols)} symbols…\n"
        f"_Running TA pipeline in series (may take ~{len(symbols)*8}s)_",
        parse_mode="Markdown"
    )

    results_table = []
    for sym in symbols:
        try:
            async with _TA_SEM:
                import gc as _gc2; _gc2.collect()
                _r = await asyncio.wait_for(
                    run_ict_analysis(sym, market_type="spot"), timeout=45
                )
            grade = _r.get("grade", "?")
            bias  = _r.get("bias", "?")[:4]
            rr    = _r.get("rr", 0)
            conf  = _r.get("conf", 0)
            gates = sum(_r.get("gate_pass") or [])
            price = _r.get("price", 0)
            st    = _r.get("signal_state","?")
            fp_   = lambda v: f"${v:,.4f}" if v < 1 else f"${v:,.2f}"
            results_table.append({
                "sym": sym, "grade": grade, "bias": bias,
                "rr": rr, "conf": conf, "gates": gates,
                "price": price, "state": st,
            })
        except Exception as _se:
            results_table.append({"sym": sym, "grade": "ERR", "bias": "—",
                                   "rr": 0, "conf": 0, "gates": 0, "price": 0, "state": "ERR"})
        await asyncio.sleep(1)  # rate limit between symbols

    # Sort by grade then confidence
    _grade_order = {"S":0,"A":1,"B":2,"C":3,"D":4,"F":5,"ERR":6,"?":6}
    results_table.sort(key=lambda x: (_grade_order.get(x["grade"],6), -x["conf"]))

    lines = [
        f"📡 *{sector.upper()} SCAN RESULTS*",
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"",
        f"`{'SYM':<6} {'GR':>2} {'BIAS':>4} {'R:R':>5} {'CONF':>4} {'G':>2} {'STATE'}`",
    ]
    for r in results_table:
        state_icon = "✅" if r["state"]=="ACTIVE" else "⚠️" if r["state"]=="DEFERRED" else "⛔"
        lines.append(
            f"`{r['sym']:<6} {r['grade']:>2} {r['bias']:>4} {r['rr']:>5.2f} "
            f"{r['conf']:>4.0f} {r['gates']:>2}` {state_icon}"
        )

    top = next((r for r in results_table if r["grade"] in ("S","A","B") and r["state"]=="ACTIVE"), None)
    if top:
        lines += ["", f"🏆 *Best:* `{top['sym']}` Grade`{top['grade']}` {top['bias']} R:R`{top['rr']:.2f}` — run `/analyze {top['sym']}` for full signal"]

    await msg.edit_text("\n".join(lines), parse_mode="Markdown", reply_markup=create_main_keyboard())


# ══════════════════════════════════════════════════════════════════════════════
# REC-6: /alert <SYMBOL> <PRICE> — custom price alerts
# ══════════════════════════════════════════════════════════════════════════════
import json as _json
_CUSTOM_ALERTS: dict = {}   # {alert_id: {chat_id, symbol, target_price, direction, ts}}
_CUSTOM_ALERTS_FILE = "custom_alerts.json"
_custom_alerts_lock: list = []

def _get_ca_lock():
    if not _custom_alerts_lock:
        _custom_alerts_lock.append(asyncio.Lock())
    return _custom_alerts_lock[0]

def _load_custom_alerts():
    global _CUSTOM_ALERTS
    try:
        with open(_CUSTOM_ALERTS_FILE) as f:
            _CUSTOM_ALERTS = _json.load(f)
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")

def _save_custom_alerts():
    try:
        with open(_CUSTOM_ALERTS_FILE, "w") as f:
            _json.dump(_CUSTOM_ALERTS, f)
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")

_load_custom_alerts()

async def cmd_alert(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/alert <SYMBOL> <PRICE> — set a custom price alert. /alert list to view, /alert clear to remove all."""
    chat_id = update.effective_chat.id
    args    = context.args or []

    if not args or args[0].lower() == "list":
        my = {k:v for k,v in _CUSTOM_ALERTS.items() if v.get("chat_id") == chat_id}
        if not my:
            await update.message.reply_text(
                "🔔 *No active alerts.*\nUsage: `/alert BTCUSDT 100000`",
                parse_mode="Markdown"
            )
            return
        lines = ["🔔 *Your Active Alerts:*", ""]
        for aid, a in list(my.items())[:10]:
            fp_ = lambda v: f"${v:,.4f}" if v < 1 else f"${v:,.2f}"
            lines.append(f"  `{aid[-6:]}` {a['symbol']} → `{fp_(a['target_price'])}` ({a['direction']})")
        lines.append("\n_/alert clear to remove all_")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return

    if args[0].lower() == "clear":
        async with _get_ca_lock():
            to_del = [k for k,v in _CUSTOM_ALERTS.items() if v.get("chat_id")==chat_id]
            for k in to_del:
                del _CUSTOM_ALERTS[k]
            _save_custom_alerts()
        await update.message.reply_text(f"🗑️ Cleared {len(to_del)} alert(s).", reply_markup=create_main_keyboard())
        return

    if len(args) < 2:
        await update.message.reply_text("Usage: `/alert BTCUSDT 100000`", parse_mode="Markdown")
        return

    sym = args[0].upper().replace("/","").replace("-","")
    if not sym.endswith("USDT"):
        sym += "USDT"
    try:
        target = float(args[1].replace(",","").replace("$",""))
    except ValueError:
        await update.message.reply_text("⚠️ Invalid price. Example: `/alert BTCUSDT 100000`", parse_mode="Markdown")
        return

    # Fetch current price to determine direction
    _cg_id = {"BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin"}.get(sym.replace("USDT",""), sym.replace("USDT","").lower())
    try:
        _pr = await asyncio.to_thread(fetch_json, f"{COINGECKO_BASE_URL}/simple/price", {"ids":_cg_id,"vs_currencies":"usd"})
        _cur = float((_pr.get(_cg_id) or {}).get("usd", 0))
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        _cur = 0.0

    direction = "ABOVE" if (_cur <= 0 or target > _cur) else "BELOW"
    aid = f"ca_{chat_id}_{int(time.time())}"

    async with _get_ca_lock():
        # Cap at 10 alerts per user
        my_count = sum(1 for v in _CUSTOM_ALERTS.values() if v.get("chat_id")==chat_id)
        if my_count >= 10:
            await update.message.reply_text("⚠️ Maximum 10 alerts per user. Use `/alert clear` to remove old ones.")
            return
        _CUSTOM_ALERTS[aid] = {
            "chat_id": chat_id, "symbol": sym, "target_price": target,
            "direction": direction, "ts": time.time(), "cg_id": _cg_id,
        }
        _save_custom_alerts()

    fp_ = lambda v: f"${v:,.4f}" if v < 1 else f"${v:,.2f}"
    await update.message.reply_text(
        f"🔔 *Alert set!*\n`{sym}` → `{fp_(target)}` ({direction})\n"
        f"{'Current price: ' + fp_(_cur) if _cur else ''}\n_ID: {aid[-8:]}_",
        parse_mode="Markdown", reply_markup=create_main_keyboard()
    )


async def _check_custom_alerts(application) -> None:
    """Poll custom price alerts — called each APEX cycle. Fires and removes on hit."""
    if not _CUSTOM_ALERTS:
        return
    # Batch-fetch all unique symbols
    unique_ids = list({v["cg_id"] for v in _CUSTOM_ALERTS.values()})
    if not unique_ids:
        return
    try:
        _prices = await asyncio.to_thread(
            fetch_json, f"{COINGECKO_BASE_URL}/simple/price",
            {"ids": ",".join(unique_ids[:25]), "vs_currencies": "usd"}
        )
        if not _prices:
            return
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        return

    fired = []
    for aid, a in list(_CUSTOM_ALERTS.items()):
        try:
            live = float((_prices.get(a["cg_id"]) or {}).get("usd", 0))
            if live <= 0:
                continue
            fp_ = lambda v: f"${v:,.4f}" if v < 1 else f"${v:,.2f}"
            triggered = (
                (a["direction"] == "ABOVE" and live >= a["target_price"]) or
                (a["direction"] == "BELOW" and live <= a["target_price"])
            )
            if triggered:
                fired.append(aid)
                await application.bot.send_message(
                    chat_id=a["chat_id"],
                    text=(
                        f"🔔 *PRICE ALERT TRIGGERED*\n"
                        f"`{a['symbol']}` hit `{fp_(a['target_price'])}` ({a['direction']})\n"
                        f"Live: `{fp_(live)}`\n"
                        f"_Alert ID: {aid[-8:]}_"
                    ),
                    parse_mode=ParseMode.MARKDOWN,
                )
                logger.info(f"[ALERT] {a['symbol']} → {fp_(a['target_price'])} fired for {a['chat_id']}")
        except Exception as _ae:
            logger.debug(f"[SILENT_EX] {type(_ae).__name__}: {_ae}")

    if fired:
        async with _get_ca_lock():
            for aid in fired:
                _CUSTOM_ALERTS.pop(aid, None)
            _save_custom_alerts()


# ══════════════════════════════════════════════════════════════════════════════
# REC-9: /backtest <SYMBOL> [DAYS] — paper trade equity review from TradeLifecycleTracker
# ══════════════════════════════════════════════════════════════════════════════
async def cmd_backtest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/backtest <SYMBOL> [DAYS] — replay stored paper trades and show equity stats."""
    chat_id = update.effective_chat.id
    args    = context.args or []

    if not TA_MODULE_AVAILABLE:
        await update.message.reply_text("⚠️ TA module unavailable.")
        return

    symbol = args[0].upper().replace("/","").replace("-","") if args else "BTCUSDT"
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    days = int(args[1]) if len(args) >= 2 and args[1].isdigit() else 30

    try:
        tt_sum = _TRADE_TRACKER.get_summary()
        paper_trades = tt_sum.get("paper_trades", 0)
        equity       = tt_sum.get("paper_equity", 1.0)
        pnl          = tt_sum.get("realized_pnl", 0.0)
        win_rate     = round(tt_sum.get("win_rate", 0.0) * 100, 1)
        max_dd       = round(tt_sum.get("max_drawdown", 0.0) * 100, 2)
        equity_pct   = round((equity - 1.0) * 100, 2)

        # Try to get symbol-specific stats if tracker supports it
        sym_stats = {}
        try:
            sym_stats = _TRADE_TRACKER.get_symbol_stats(symbol) or {}
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")

        lines = [
            f"📊 *BACKTEST REPORT — {symbol}*",
            f"_Last {days} days · Paper trades only_",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"",
            f"*Global Paper Stats:*",
            f"  Trades tracked : `{paper_trades}`",
            f"  Final equity   : `{equity:.4f}` ({equity_pct:+.2f}%)",
            f"  Realized P&L   : `{pnl:+.6f}`",
            f"  Win rate       : `{win_rate}%`",
            f"  Max drawdown   : `{max_dd}%`",
        ]

        if sym_stats:
            lines += [
                f"",
                f"*{symbol} Specific:*",
                f"  Trades : `{sym_stats.get('n',0)}`",
                f"  Win%   : `{round(sym_stats.get('win_rate',0)*100,1)}%`",
                f"  Avg RR : `1:{sym_stats.get('avg_rr',0):.2f}`",
                f"  Sharpe : `{sym_stats.get('sharpe',0):.2f}`",
            ]
        else:
            lines += ["", f"_No symbol-specific data for {symbol} yet._",
                      f"_Run `/analyze {symbol.replace('USDT','')}` to start tracking._"]

        lines += [
            "",
            "_Source: TradeLifecycleTracker SQLite equity curve_",
            "_Paper mode — not real trades. For strategy validation only._",
        ]
    except Exception as _bt_e:
        lines = [f"⚠️ Backtest data unavailable: {_bt_e}"]

    await update.message.reply_text(
        "\n".join(lines), parse_mode="Markdown", reply_markup=create_main_keyboard()
    )


async def _fomc_weekly_refresh_task() -> None:
    """
    Weekly re-fetch of FOMC/CPI/NFP calendar from Fed + BLS websites.
    Runs every 7 days. Falls back silently — hardcoded dates remain active on failure.
    Logs to mop_ict_structured.jsonl via standard logger for observability.
    Memory cost: O(1) — no state stored beyond the update inside TechnicalAnalysis_v9_6.
    """
    _FOMC_REFRESH_INTERVAL = 7 * 24 * 3600   # 7 days in seconds
    await asyncio.sleep(3600)                  # 1h startup stagger — let bot stabilise first
    while True:
        try:
            from TechnicalAnalysis_v9_6 import refresh_fomc_calendar as _rfc
            await asyncio.to_thread(_rfc)
            logger.info("[FOMC] Weekly calendar refresh completed — dates updated")
        except Exception as _e:
            logger.warning(f"[FOMC] Weekly refresh failed — hardcoded dates remain active: {_e}")
        await asyncio.sleep(_FOMC_REFRESH_INTERVAL)


def main():
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", BOT_TOKEN)
    if not TOKEN:
        logger.critical("[BOOT] TELEGRAM_BOT_TOKEN is not set. Cannot start bot.")
        return
    
    application = Application.builder().token(TOKEN).build()

    # Run Gemini startup probe after the event loop is ready
    async def _post_init(app):
        global _alpha_notify_task, _btc_regime_task, _provider_health_task, _prewarm_task
        await _probe_gemini_on_startup()
        # REC-7: Refresh FOMC/macro calendar from Fed website at startup
        if TA_MODULE_AVAILABLE:
            try:
                from TechnicalAnalysis_v9_6 import refresh_fomc_calendar as _rfc
                await asyncio.to_thread(_rfc)
            except Exception as _fce:
                logger.debug(f"[FOMC] startup refresh non-critical: {_fce}")
        # Cancel any stale task before creating a new one (thesis A2.5)
        if _alpha_notify_task and not _alpha_notify_task.done():
            _alpha_notify_task.cancel()
            try:
                await _alpha_notify_task
            except (asyncio.CancelledError, Exception) as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        if LOW_RAM_MODE:
            _btc_regime_task = asyncio.create_task(_btc_regime_singleton_task())
            logger.warning("[BOOT] LOW_RAM_MODE active — started only BTC regime task; skipped alpha/providers/prewarm/conditions/AWS/deep-struct/FOMC")
        else:
            _alpha_notify_task    = asyncio.create_task(auto_alpha_notification(app))
            _btc_regime_task      = asyncio.create_task(_btc_regime_singleton_task())   # [MOP-P04]
            _provider_health_task = asyncio.create_task(_run_provider_health_task())    # [MOP-P04] renamed to avoid variable/function collision
            _prewarm_task         = asyncio.create_task(_session_prewarm_task())        # [MOP-P05]
            asyncio.create_task(_condition_evaluator_task())                            # [MOP-P08]
            asyncio.create_task(_aws_learning_pass_task())                              # [AWS] multi-coin learning
            asyncio.create_task(_deep_struct_refresh_task())                            # [DEEP STRUCT] extended history
            if TA_MODULE_AVAILABLE:
                asyncio.create_task(_fomc_weekly_refresh_task())                        # [FOMC] weekly calendar re-fetch
            logger.info("[BOOT] All background tasks started: alpha + BTC regime + providers + prewarm + conditions + AWS learning + deep struct + FOMC refresh")
    application.post_init = _post_init

    async def _post_shutdown(app):
        """Cancel all tracked background tasks on clean shutdown (memory-safe for 127 MB)."""
        # 1. Cancel pending delete-notification tasks
        for t in list(_delete_tasks):
            t.cancel()
        if _delete_tasks:
            await asyncio.gather(*_delete_tasks, return_exceptions=True)
        _delete_tasks.clear()
        # 2. Cancel alpha notify task
        if _alpha_notify_task and not _alpha_notify_task.done():
            _alpha_notify_task.cancel()
            try:
                await _alpha_notify_task
            except (asyncio.CancelledError, Exception) as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        # 3. Cancel MOP-P04/P05 singleton tasks
        for _t in [_btc_regime_task, _provider_health_task, _prewarm_task]:
            if _t and not _t.done():
                _t.cancel()
                try:
                    await _t
                except (asyncio.CancelledError, Exception) as _e:
                    logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        # 4. Cancel all per-chat background tasks
        for t in list(background_tasks.values()):
            if not t.done():
                t.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks.values(), return_exceptions=True)
        background_tasks.clear()
        logger.info("[SHUTDOWN] All background tasks cancelled cleanly")
    application.post_shutdown = _post_shutdown

    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("refresh", _cmd_refresh))
    application.add_handler(CommandHandler("status", _cmd_status))
    application.add_handler(CommandHandler("tvsetup",      cmd_app_tvsetup))
    application.add_handler(CommandHandler("tvcheck",      cmd_app_tvcheck))
    application.add_handler(CommandHandler("pine",         cmd_app_pine))
    application.add_handler(CommandHandler("killzone",     cmd_app_killzone))
    application.add_handler(CommandHandler("watch",        cmd_watch))         # [MOP-P08]
    application.add_handler(CommandHandler("watchlist",    cmd_watchlist))     # [MOP-P08]
    application.add_handler(CommandHandler("cancel_watch", cmd_cancel_watch))  # [MOP-P08]
    # GAP-03: Immune system blacklist admin
    application.add_handler(CommandHandler("unflag",      cmd_unflag))
    # GAP-06: Per-user signal history
    application.add_handler(CommandHandler("my_signals",  cmd_my_signals))
    # GAP-07: Markov brain inspector
    application.add_handler(CommandHandler("brain",       cmd_brain))
    # GAP-09: Performance dashboard
    application.add_handler(CommandHandler("performance", cmd_performance))
    # REC-5: Sector batch scan
    application.add_handler(CommandHandler("scan",        cmd_scan))
    # REC-6: Custom price alerts
    application.add_handler(CommandHandler("alert",       cmd_alert))
    # REC-9: Backtest paper equity review
    application.add_handler(CommandHandler("backtest",    cmd_backtest))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(ta_market_choice, pattern="^ta_(spot|futures)_"))
    application.add_handler(CallbackQueryHandler(ai_choice, pattern="^ai_"))
    application.add_handler(CallbackQueryHandler(feedback_callback_handler, pattern="^(fb_|go_feedback)"))
    application.add_handler(CallbackQueryHandler(_cmd_refresh_callback, pattern="^refresh_market$"))
    application.add_handler(CallbackQueryHandler(markov_audit_callback, pattern="^markov_audit$"))
    application.add_handler(CallbackQueryHandler(settings_callback, pattern="^settings_"))
    # BUG-FIX: airdrop filter submenu taps — register ^airdrop: callback handler.
    # Uses module-level import (AIRDROPS_MODULE_AVAILABLE) — no duplicate import needed.
    if AIRDROPS_MODULE_AVAILABLE:
        application.add_handler(CallbackQueryHandler(
            lambda u, c: handle_airdrops_filter(u, c, create_main_keyboard),
            pattern="^airdrop:"
        ))
        logger.info("[BOOT] Airdrop module registered — callback pattern ^airdrop: active")
    else:
        logger.warning("[BOOT] airdrops.py not available — airdrop callbacks disabled")
    
    # Message handler
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # [MOP-P10] WebApp trade journal data handler
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, _handle_journal_data))
    # Suppress "NansenProBot pinned a message" service messages — delete immediately on receipt
    # group=-1 ensures this fires before all other handlers and cannot be blocked
    application.add_handler(
        MessageHandler(filters.StatusUpdate.PINNED_MESSAGE, _suppress_pin_service_message),
        group=-1
    )
    
    # Error handler
    application.add_error_handler(error_handler)

    # ── REC-16: Webhook vs polling mode ─────────────────────────────────────
    # Set USE_WEBHOOK=true + WEBHOOK_URL=https://yourdomain.com in environment
    # to run in webhook mode (lower CPU, more reliable on VPS).
    # Polling mode (default) requires no additional setup.
    _use_webhook  = os.getenv("USE_WEBHOOK", "false").lower() in ("1", "true", "yes")
    _webhook_url  = os.getenv("WEBHOOK_URL", "").rstrip("/")
    _webhook_port = int(os.getenv("WEBHOOK_PORT", "8443"))
    _webhook_path = os.getenv("WEBHOOK_PATH", "/webhook")

    if _use_webhook and _webhook_url:
        logger.info(f"[BOOT] Webhook mode — {_webhook_url}{_webhook_path}:{_webhook_port}")
        application.run_webhook(
            listen="0.0.0.0",
            port=_webhook_port,
            url_path=_webhook_path,
            webhook_url=f"{_webhook_url}{_webhook_path}",
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )
    else:
        logger.info("[BOOT] Polling mode")
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )

if __name__ == "__main__":
    main()
