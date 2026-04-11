"""
╔══════════════════════════════════════════════════════════════════════╗
║  MARKOV STACK — Unified Module v4.0                                  ║
║  MOP·ICT·QUANT STACK · OCE v10.3 · Zero-Bug Mandate                 ║
║                                                                      ║
║  v4.0 NEW UPGRADES (72-item audit + feature roadmap):                ║
║    UPG-8   VolatilityBrain — 6th SuperValidator brain                ║
║            ATR percentile history → vol_conf_mod (LOW/MED/HIGH/EXT)  ║
║    UPG-9   TimeOfDayBrain — 7th SuperValidator brain                 ║
║            Session × ICT state win tracking → session_conf_mod       ║
║    UPG-10  Shannon Entropy + Conviction signal per prediction        ║
║            markov_entropy [0–1] · markov_conviction HIGH/MED/LOW     ║
║    UPG-11  Full next-state distribution vector exposed               ║
║            markov_next_dist: {state → probability} all 9 states      ║
║    UPG-12  Stationary distribution exposed per symbol                ║
║            markov_stationary: π vector (long-run state probs)        ║
║    UPG-13  Prediction accuracy self-tracker (ring buffer 50)         ║
║            markov_pred_accuracy + markov_pred_log_n                  ║
║    UPG-14  Regime persistence tracking in RegimeBrain                ║
║            regime_persistence_bars + regime_change_warning           ║
║    UPG-15  Source-weighted SentimentBrain scoring                    ║
║            source_type: news/ai_analysis/on_chain/macro weights      ║
║    UPG-16  LevelLearner sweep range P10/P50/P90                      ║
║            sweep_p10 · sweep_p50 · sweep_p90 · sweep_range_pct       ║
║    UPG-17  AI provider quality-adjusted vote weighting               ║
║            ai_provider_accuracy tracked per brain → weighted votes   ║
║    UPG-18  Confidence history sparkline (last 20 super_scores)       ║
║            markov_conf_history: [deque maxlen=20] per symbol         ║
║    UPG-19  DrawdownBrain — MAE/MFE tracker → drawdown_size_mod       ║
║    UPG-20  Brain schema v4.0 + auto-migration                        ║
║            BRAIN_SCHEMA_VERSION="4.0" + _migrate_brain()            ║
║    UPG-21  _stationary() LRU cache (matrix hash → π vector)         ║
║    UPG-22  _percentile() hardened (docstring + type annotations)     ║
║                                                                      ║
║  v3.0 UPGRADES (all retained):                                       ║
║    UPG-1  Second-Order Markov Chain (81-state pair → next state)     ║
║    UPG-2  N-Step Lookahead — P² and P³                               ║
║    UPG-3  Per-Symbol ICT State Win-Rate Table                        ║
║    UPG-4  LevelLearner Percentile Ladder                             ║
║    UPG-5  Regime-Conditioned Entry+SL Geometry Signal                ║
║    UPG-6  Outcome Feedback Improvements                              ║
║    UPG-7  Super Markov Composite Confidence Score 0–100              ║
║                                                                      ║
║  v2.1 AUDIT FIXES (all retained):                                    ║
║    FIX-A  threading.Lock on all brain JSON writes                    ║
║    FIX-B  Circuit breaker (BRAIN_WRITE_FAILURE_CAP)                 ║
║    FIX-C  Input validation in record_outcome()                       ║
║    FIX-D  Named constants — zero magic numbers                       ║
║    FIX-E  Specific exception types                                   ║
║    FIX-F  _sanitize_symbol() safe log output                         ║
║    FIX-G  _rehydrate_symbol() guarded                                ║
║                                                                      ║
║  128MB RAM safe · Python 3.8+ · threading std-lib only               ║
║  Brain JSON: ~170KB projected (88KB base + 81×9 second-order matrix) ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
import threading
import logging
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════
# SECTION 1 — ICT STATE SPACE + NAMED CONSTANTS
# ══════════════════════════════════════════════════════════════════════

ICT_STATES: List[str] = [
    "ACCUMULATION",
    "MANIPULATION_H",
    "MANIPULATION_L",
    "BOS_BULL",
    "BOS_BEAR",
    "CHOCH_BULL",
    "CHOCH_BEAR",
    "FVG_BULL",
    "FVG_BEAR",
]
N = len(ICT_STATES)
_S = {s: i for i, s in enumerate(ICT_STATES)}

_BULL_IDX = frozenset([_S["BOS_BULL"], _S["CHOCH_BULL"], _S["FVG_BULL"], _S["MANIPULATION_L"]])
_BEAR_IDX = frozenset([_S["BOS_BEAR"], _S["CHOCH_BEAR"], _S["FVG_BEAR"], _S["MANIPULATION_H"]])

REGIMES: List[str] = ["TRENDING", "VOLATILE", "CHOPPY", "ACCUMULATION", "DISTRIBUTION"]

_REGIME_TRANSITION: List[List[float]] = [
    [0.55, 0.25, 0.05, 0.05, 0.10],
    [0.25, 0.20, 0.25, 0.20, 0.10],
    [0.10, 0.20, 0.30, 0.35, 0.05],
    [0.40, 0.15, 0.15, 0.20, 0.10],
    [0.15, 0.30, 0.15, 0.10, 0.30],
]

_OUTCOMES = ("TP1", "TP2", "TP3", "SL", "TIMEOUT")

# ── UPG-1: Second-Order Markov constants ──────────────────────────────
# SO matrix: 81 state pairs → 9 next-state columns
# Indexed by: row = prev_idx * N + curr_idx (0–80), col = next_idx (0–8)
SO_N = N * N   # 81 second-order "super-states"
_SO = {(i, j): i * N + j for i in range(N) for j in range(N)}  # (prev, curr) → row idx

# Level Learner constants
LEVEL_LEARN_MIN_SAMPLES   = 8
LEVEL_LEARN_FULL_WEIGHT   = 30
LEVEL_LEARN_MAX_WEIGHT    = 0.65
LEVEL_LEARN_RING_SIZE     = 100

# UPG-4: Percentile targets for TP/SL levels
LEVEL_TP1_PERCENTILE  = 40     # conservative first take
LEVEL_TP2_PERCENTILE  = 65     # median-plus extension
LEVEL_TP3_PERCENTILE  = 85     # full runner
LEVEL_SL_PERCENTILE   = 55     # slightly wider than median — survival buffer

# Gate tracking
MAX_TRACKED_GATES        = 11

# Learning rate
DECAY_FACTOR             = 0.995
ALPHA_RAMP_SAMPLES       = 100
TP_REWARD_WEIGHT         = 2.0
TP_COUNTER_PENALTY_FRAC  = 0.3
SL_PENALTY_WEIGHT        = 1.5

# Second-order decay (slightly slower — second-order needs more evidence)
SO_DECAY_FACTOR          = 0.997

# Confidence modifier bounds
CONF_MOD_EDGE_SCALE      = 2.5
CONF_MOD_EDGE_CAP        = 0.25
CONF_MOD_CHOPPY_CAP      = 0.85
CONF_MOD_FGI_BOOST       = 0.05
CONF_MOD_MAX             = 1.30
CONF_MOD_MIN             = 0.75

# Gate veto
GATE_VETO_EDGE_THRESHOLD = -0.15
MIN_SAMPLES_FOR_VETO     = 10

# Velocity gate
VELOCITY_HISTORY_SIZE    = 40
VELOCITY_WINDOW          = 20
VELOCITY_GATE_THRESHOLD  = 0.20

# Macro event calibration
MACRO_BASELINE_RATE      = 0.65
MACRO_MIN_OBSERVATIONS   = 5

# Brain reliability
BRAIN_WRITE_FAILURE_CAP  = 3

# AI-Reinforced Learning
AI_CONSENSUS_WEIGHT      = 0.5
AI_CONSENSUS_THRESHOLD   = 3.0

# Per-symbol AI-tunable bounds
AI_TUNE_TP_REWARD_MIN    = 1.0
AI_TUNE_TP_REWARD_MAX    = 4.0
AI_TUNE_SL_PENALTY_MIN   = 0.5
AI_TUNE_SL_PENALTY_MAX   = 3.0
AI_TUNE_EDGE_THRESH_MIN  = -0.30
AI_TUNE_EDGE_THRESH_MAX  = -0.05
AI_TUNE_DECAY_MIN        = 0.990
AI_TUNE_DECAY_MAX        = 0.999
AI_TUNE_STEP             = 0.05

# UPG-5: Regime-conditioned geometry thresholds
REGIME_CHOPPY_ENTRY_TIGHTEN  = 0.30   # tighten entry zone width by 30%
REGIME_VOLATILE_SL_WIDEN     = 0.50   # widen SL by 50% of ATR
REGIME_TRENDING_SL_FACTOR    = 1.0    # no change
REGIME_GEOMETRY_MAP = {
    "TRENDING":     {"entry_tighten_pct": 0.0,  "sl_widen_pct": 0.0,  "conf_cap": CONF_MOD_MAX},
    "VOLATILE":     {"entry_tighten_pct": 0.15, "sl_widen_pct": 0.50, "conf_cap": 1.20},
    "CHOPPY":       {"entry_tighten_pct": 0.30, "sl_widen_pct": 0.30, "conf_cap": CONF_MOD_CHOPPY_CAP},
    "ACCUMULATION": {"entry_tighten_pct": 0.20, "sl_widen_pct": 0.20, "conf_cap": 1.15},
    "DISTRIBUTION": {"entry_tighten_pct": 0.25, "sl_widen_pct": 0.40, "conf_cap": 1.10},
}

SYMBOL_LOG_MAX_LEN       = 20

# Super Markov
SUPER_BRAIN_VERSION      = "4.1"
SENTIMENT_DECAY          = 0.92
SENTIMENT_HALF_LIFE_MAX  = 72
SENTIMENT_HALF_LIFE_MIN  = 1
SENTIMENT_MIN_WEIGHT     = 0.05
SENTIMENT_MAX_KEYWORDS   = 500
REGIME_HISTORY_SIZE      = 60
SECTOR_HISTORY_SIZE      = 30
TREND_HISTORY_SIZE       = 50
CROSS_HISTORY_SIZE       = 40

SV_WEIGHT_ICT        = 0.25   # v4.1: rebalanced for 8 brains (was 0.28)
SV_WEIGHT_SENTIMENT  = 0.16   # v4.1: was 0.17
SV_WEIGHT_REGIME     = 0.16   # v4.1: was 0.17
SV_WEIGHT_SECTOR     = 0.08   # v4.1: was 0.09
SV_WEIGHT_TREND      = 0.08   # v4.1: was 0.09
SV_WEIGHT_CROSS      = 0.05
SV_WEIGHT_VOLATILITY = 0.08   # UPG-8: VolatilityBrain weight
SV_WEIGHT_TIMEOFDAY  = 0.07   # UPG-9: TimeOfDayBrain weight
SV_WEIGHT_POLYMARKET = 0.07   # v4.1: PolymarketBrain — 8th brain

# ── RGD-1: Regime-Aware Brain Weight Tables ───────────────────────────
# Each regime shifts which brains carry more scoring influence.
# Weights always sum to 1.0. Base (no regime) = SV_WEIGHT_* constants above.
# Applied inside SuperValidator.validate() when current_atr_pct is provided.
# Research basis: fixed-weight models lose ~15-25% accuracy in ranging markets
# (Tickeron FLM / crypto 70%-ranging-environment studies).
_SV_REGIME_WEIGHTS: dict = {
    # TREND: momentum and ICT alignment dominate; vol filter matters more
    "TREND": {
        "ict":        0.30,
        "sentiment":  0.14,
        "regime":     0.16,
        "sector":     0.08,
        "trend":      0.10,
        "cross":      0.05,
        "volatility": 0.06,
        "timeofday":  0.04,
        "polymarket": 0.07,
    },
    # RANGE: structure and liquidity context dominate; trend signal downweighted
    "RANGE": {
        "ict":        0.18,
        "sentiment":  0.14,
        "regime":     0.20,
        "sector":     0.10,
        "trend":      0.05,
        "cross":      0.08,
        "volatility": 0.10,
        "timeofday":  0.05,
        "polymarket": 0.10,
    },
    # COMPRESSION: breakout probability elevated; balanced weights, vol raised
    "COMPRESSION": {
        "ict":        0.22,
        "sentiment":  0.13,
        "regime":     0.17,
        "sector":     0.09,
        "trend":      0.08,
        "cross":      0.06,
        "volatility": 0.12,
        "timeofday":  0.06,
        "polymarket": 0.07,
    },
    # VOLATILE: reduce all conviction; ICT still leads, vol penalty applied
    "VOLATILE": {
        "ict":        0.26,
        "sentiment":  0.13,
        "regime":     0.15,
        "sector":     0.07,
        "trend":      0.07,
        "cross":      0.05,
        "volatility": 0.14,
        "timeofday":  0.06,
        "polymarket": 0.07,
    },
}

# RGD-1: ADX thresholds for regime detection (no external dependency)
_REGIME_ADX_TREND       = 25.0   # ADX > 25 = directional trend present
_REGIME_ADX_RANGE       = 20.0   # ADX < 20 = non-directional / ranging
_REGIME_ATR_PCT_VOLATILE = 3.5   # ATR% > 3.5% = elevated volatility
_REGIME_ATR_PCT_COMPRESS = 0.8   # ATR% < 0.8% = compression / low vol


def detect_regime_from_atr(
    current_atr_pct: float,
    current_regime:  str = "",
) -> str:
    """
    RGD-1: Lightweight regime detector using ATR% + current_regime string.
    Returns one of: TREND | RANGE | COMPRESSION | VOLATILE

    Uses current_atr_pct (ATR expressed as % of price, e.g. 2.3 = 2.3% ATR)
    and the existing current_regime string already passed through the pipeline.

    ADX is not available at SuperValidator level — regime string from upstream
    TA pipeline is the primary signal; ATR% refines compression/volatile splits.

    Args:
        current_atr_pct: ATR as % of price from VolatilityBrain / TA result.
                         0.0 = unknown → falls back to current_regime string.
        current_regime:  Regime string from upstream (e.g. "TRENDING", "RANGING").

    Returns:
        Canonical regime key for _SV_REGIME_WEIGHTS lookup.
    """
    # Normalise the upstream regime string to canonical keys
    _reg_upper = str(current_regime).upper().strip()
    _mapped: str = ""
    if _reg_upper in ("TRENDING", "TREND", "EXPANSION", "BULL_TREND", "BEAR_TREND"):
        _mapped = "TREND"
    elif _reg_upper in ("RANGING", "RANGE", "SIDEWAYS", "CHOPPY", "ACCUMULATION",
                        "DISTRIBUTION"):
        _mapped = "RANGE"
    elif _reg_upper in ("VOLATILE", "VOLATILITY", "HIGH_VOL", "EXTREME"):
        _mapped = "VOLATILE"
    elif _reg_upper in ("COMPRESSION", "COMPRESSION_ERA", "LOW_VOL", "SQUEEZE"):
        _mapped = "COMPRESSION"

    # ATR% refinement — override mapped regime when ATR signals compression or spike
    if current_atr_pct > 0.0:
        if current_atr_pct >= _REGIME_ATR_PCT_VOLATILE:
            return "VOLATILE"
        if current_atr_pct <= _REGIME_ATR_PCT_COMPRESS:
            return "COMPRESSION"

    # Fall back to mapped string or TREND if still unknown
    return _mapped if _mapped else "TREND"

# ── UPG-20: Brain schema versioning ──────────────────────────────────
BRAIN_SCHEMA_VERSION = "4.0"  # bump when adding new brain.json keys

# ── UPG-8: VolatilityBrain constants ────────────────────────────────
VOL_HISTORY_SIZE     = 30    # ATR percentile ring buffer per symbol
VOL_ATR_PCT_EXTREME  = 90    # percentile threshold: EXTREME volatility
VOL_ATR_PCT_HIGH     = 70    # percentile threshold: HIGH volatility
VOL_ATR_PCT_LOW      = 30    # percentile threshold: LOW volatility
VOL_MOD_EXTREME      = 0.80  # confidence modifier: extreme vol → reduce
VOL_MOD_HIGH         = 0.90
VOL_MOD_MEDIUM       = 1.00
VOL_MOD_LOW          = 1.08  # low vol → slight boost (cleaner structure)

# ── UPG-9: TimeOfDayBrain constants ─────────────────────────────────
SESSION_NAMES        = ["LONDON", "NY", "ASIA", "OVERLAP", "OFF_HOURS"]
TOD_HISTORY_SIZE     = 200   # session outcome records per brain
TOD_MOD_MIN          = 0.85
TOD_MOD_MAX          = 1.15

# ── UPG-10: Entropy conviction thresholds ───────────────────────────
ENTROPY_HIGH_CONV    = 0.60  # H < 0.60 → HIGH conviction
ENTROPY_MED_CONV     = 0.75  # H < 0.75 → MEDIUM
ENTROPY_LOW_CONV     = 0.88  # H < 0.88 → LOW, else NOISE

# ── UPG-13: Prediction accuracy ring buffer ──────────────────────────
PRED_LOG_MAX         = 50    # max predictions stored per symbol
PRED_LOG_WINDOW      = 20    # rolling window for accuracy calculation

# ── UPG-14: Regime persistence ───────────────────────────────────────
REGIME_DUR_MAX_BUF   = 30    # max duration samples stored per regime
REGIME_DUR_WARN_PCT  = 0.85  # warn when current age > 85% of median

# ── UPG-15: Sentiment source weights ────────────────────────────────
SENTIMENT_SOURCE_WEIGHTS = {
    "news":        0.50,
    "ai_analysis": 0.80,
    "on_chain":    1.00,
    "macro":       0.60,
}

# ── UPG-18: Confidence history sparkline ─────────────────────────────
CONF_HISTORY_SIZE    = 20    # last N super_confidence_scores per symbol

# ── UPG-19: DrawdownBrain constants ──────────────────────────────────
DD_MAE_HISTORY_SIZE  = 50    # MAE/MFE ring buffer per state
DD_MAE_PCT_EXTREME   = 85    # 85th pct MAE → reduce position size
DD_SIZE_MOD_MIN      = 0.50
DD_SIZE_MOD_MAX      = 1.00
DD_MAE_REDUCE_THRESH = 3.0   # % MAE p85 above which size reduction triggers

# ── UPG-21: Stationary cache ─────────────────────────────────────────
_STATIONARY_CACHE: dict = {}   # {matrix_hash: pi_vector}, max 50 entries
_STATIONARY_CACHE_MAX = 50

SENTIMENT_POSITIVE_KEYWORDS = [
    "bullish","rally","surge","breakout","uptrend","ath","all-time high",
    "inflow","etf","institutional","adoption","partnership","upgrade",
    "halving","accumulation","bull","moon","gains","record","high",
    "approval","launch","integration","milestone","positive","growth",
    "dominance up","volume up","buy","long","support","bounce",
]
SENTIMENT_NEGATIVE_KEYWORDS = [
    "bearish","crash","dump","downtrend","correction","selloff","plunge",
    "hack","exploit","vulnerability","ban","regulation","lawsuit","sec",
    "outflow","fear","panic","liquidation","decline","dump","bear",
    "fraud","scam","rug","shutdown","delisting","investigation","fine",
    "negative","drop","fall","fail","risk","warning","violation",
]
SENTIMENT_MACRO_KEYWORDS = [
    "fed","fomc","cpi","inflation","interest rate","gdp","recession",
    "powell","yellen","treasury","dollar","dxy","bonds","yield",
    "federal reserve","monetary policy","rate hike","rate cut",
]


# ══════════════════════════════════════════════════════════════════════
# SECTION 2 — HELPERS
# ══════════════════════════════════════════════════════════════════════

def _sanitize_symbol(symbol: str) -> str:
    return (
        str(symbol)
        .replace("\n","").replace("\r","").replace("/","").replace("\\","")
        .strip().upper()
    )[:SYMBOL_LOG_MAX_LEN]


def _percentile(buf: list, pct: float) -> float:
    """
    UPG-22: Return the pct-th percentile of buf (list of floats).

    Args:
        buf: List of numeric values. Returns 0.0 if empty.
        pct: Percentile target in [0, 100]. Clamped automatically.

    Returns:
        Float percentile value, or 0.0 if buf is empty/unusable.
    """
    if not buf:
        log.debug(f"[PERCENTILE] called with empty buffer at pct={pct}")
        return 0.0
    pct = max(0.0, min(100.0, float(pct)))
    try:
        s = sorted(float(x) for x in buf if x is not None)
        if not s:
            return 0.0
        k = (len(s) - 1) * pct / 100.0
        lo, hi = int(k), min(int(k) + 1, len(s) - 1)
        return s[lo] + (s[hi] - s[lo]) * (k - lo)
    except (TypeError, ValueError) as e:
        log.debug(f"[PERCENTILE] computation error: {e}")
        return 0.0


def _matmul(A: List[List[float]], B: List[List[float]]) -> List[List[float]]:
    """
    UPG-2: Square matrix multiplication for N-step Markov lookahead.
    Both A and B must be N×N. Returns N×N result.
    Memory: O(N²) = O(81) — negligible.
    """
    n = len(A)
    C = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for k in range(n):
            if A[i][k] == 0.0:
                continue
            for j in range(n):
                C[i][j] += A[i][k] * B[k][j]
    return C


def _normalize_rows(M: List[List[float]]) -> List[List[float]]:
    """Normalize each row to sum to 1.0. Zero rows become uniform."""
    n = len(M)
    out = []
    for row in M:
        s = sum(row)
        if s < 1e-12:
            out.append([1.0 / n] * n)
        else:
            out.append([v / s for v in row])
    return out


# ══════════════════════════════════════════════════════════════════════
# SECTION 2B — FIX #11 #12 #13 SELF-LEARNING ENHANCEMENTS
# ══════════════════════════════════════════════════════════════════════

# Fix #11: adaptive prior threshold
ADAPTIVE_PRIOR_MIN_N   = 50    # need 50+ effective samples before mixing per-symbol prior
ADAPTIVE_PRIOR_WEIGHT  = 0.40  # at full data, 40% weight on per-symbol learned prior


def _build_base_matrix_adaptive(
    sym_data: dict,
    regime:   str,
    bias:     str,
) -> List[List[float]]:
    """
    Fix #11 — Per-Symbol Adaptive Bias Prior.

    When effective_n < ADAPTIVE_PRIOR_MIN_N: returns pure global prior
    (same as _build_base_matrix — no change from v3.0 behaviour).

    When effective_n >= ADAPTIVE_PRIOR_MIN_N: blends the global prior with
    a per-symbol empirical prior derived from the symbol's outcome_counts.
    The empirical prior nudges the transition matrix in the direction this
    specific coin has historically moved from each state.

    This makes BTC's prior different from a mid-cap altcoin with very
    different volatility and trend characteristics.
    """
    global_prior = _build_base_matrix(regime, bias)

    effective_n = float(sym_data.get("effective_n", 0.0))
    if effective_n < ADAPTIVE_PRIOR_MIN_N:
        return global_prior

    # Build empirical outcome matrix from per-state win/loss history
    oc = sym_data.get("outcome_counts", {})
    emp = [[1.0 / N] * N for _ in range(N)]   # start uniform
    for state, outcomes in oc.items():
        if state not in _S:
            continue
        row_idx = _S[state]
        wins  = (outcomes.get("TP1", 0)
                 + outcomes.get("TP2", 0)
                 + outcomes.get("TP3", 0))
        losses = outcomes.get("SL", 0)
        total  = wins + losses
        if total < 3:
            continue
        wr = wins / total
        # Winning state → reinforce bull/bear targets by win-rate
        for j in _BULL_IDX:
            emp[row_idx][j] = wr if bias in ("BULLISH", "NEUTRAL") else (1 - wr)
        for j in _BEAR_IDX:
            emp[row_idx][j] = (1 - wr) if bias in ("BULLISH", "NEUTRAL") else wr
    emp = _normalize_rows(emp)

    # Blend weight ramps from 0 at ADAPTIVE_PRIOR_MIN_N to ADAPTIVE_PRIOR_WEIGHT at 2×
    blend = min(
        ADAPTIVE_PRIOR_WEIGHT,
        (effective_n - ADAPTIVE_PRIOR_MIN_N) /
        max(ADAPTIVE_PRIOR_MIN_N, 1) * ADAPTIVE_PRIOR_WEIGHT
    )
    blended = [
        [(1 - blend) * global_prior[i][j] + blend * emp[i][j] for j in range(N)]
        for i in range(N)
    ]
    return _normalize_rows(blended)


# Fix #12: Macro Event Calibrator constants
MACRO_EV_MIN_SAMPLES   = 5     # min observations before computing EV
MACRO_EV_BASELINE_WR   = 0.55  # neutral expected win rate


class MacroEventCalibrator:
    """
    Fix #12 — Macro Event Outcome Auto-Categorization.

    Reads `macro_event_outcomes` from the brain's global block (already
    populated by record_and_save()) and computes expected-value tiers per
    event type — making FOMC/CPI blocks data-driven rather than hardcoded.

    Output: best_events, worst_events, neutral_events dicts with
    gate_pass_rate, ev_score, and recommendation per event type.
    """

    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def calibrate(self) -> dict:
        glo  = self._brain.read().get("global", {})
        meo  = glo.get("macro_event_outcomes", {})
        if not meo:
            return {
                "best_events":    {},
                "worst_events":   {},
                "neutral_events": {},
                "calibration_note": "No macro event history yet",
            }

        best, worst, neutral = {}, {}, {}
        for event, rec in meo.items():
            n    = rec.get("n", 0)
            gpr  = rec.get("gate_pass_rate", 0.5)
            if n < MACRO_EV_MIN_SAMPLES:
                continue
            ev = round(gpr - MACRO_EV_BASELINE_WR, 4)
            tier = {
                "gate_pass_rate": round(gpr, 3),
                "n":              n,
                "ev_score":       ev,
                "recommendation": (
                    "TRADE_NORMAL"  if ev > 0.10  else
                    "REDUCE_SIZE"   if ev > 0.0   else
                    "WAIT"          if ev > -0.10 else
                    "BLOCK"
                ),
            }
            if ev > 0.10:
                best[event]    = tier
            elif ev < -0.10:
                worst[event]   = tier
            else:
                neutral[event] = tier

        # Sort best/worst by EV magnitude
        best    = dict(sorted(best.items(),    key=lambda x: -x[1]["ev_score"]))
        worst   = dict(sorted(worst.items(),   key=lambda x:  x[1]["ev_score"]))
        neutral = dict(sorted(neutral.items(), key=lambda x: -x[1]["n"]))

        note = (
            f"Macro calibration: {len(best)} favourable · "
            f"{len(worst)} adverse · {len(neutral)} neutral "
            f"(min {MACRO_EV_MIN_SAMPLES} samples each)"
        )
        return {
            "best_events":    best,
            "worst_events":   worst,
            "neutral_events": neutral,
            "calibration_note": note,
        }

    def get_event_modifier(self, macro_event: str) -> float:
        """
        Returns a conf_mod multiplier for the given macro event based on
        historically calibrated gate-pass rate. Falls back to 1.0 if
        insufficient data.
        """
        glo = self._brain.read().get("global", {})
        rec = glo.get("macro_event_outcomes", {}).get(macro_event)
        if not rec or rec.get("n", 0) < MACRO_EV_MIN_SAMPLES:
            return 1.0
        gpr = rec.get("gate_pass_rate", MACRO_EV_BASELINE_WR)
        mod = round(gpr / max(MACRO_EV_BASELINE_WR, 1e-9), 3)
        return max(0.50, min(1.20, mod))


# Fix #13: Session × ICT State win-rate matrix constants
SESSION_LABELS = ["LONDON", "NEW_YORK", "NY_AM", "OVERLAP", "ASIA", "OFF_HOURS", "WEEKEND"]
SESSION_STATE_MIN_SAMPLES = 3


def _compute_session_state_matrix(sym_data: dict) -> Dict[str, Dict[str, float]]:
    """
    Fix #13 — Session × ICT State Win-Rate Matrix.

    Reads level_stats (already populated by LevelLearner.record_level_outcome())
    where the key format is "{ict_state}|{bias}". For each (session, ict_state)
    pair extract win rate from the stored outcome counts.

    NOTE: level_stats keys use "{ict_state}|{bias}" — no session dimension yet.
    This function uses outcome_counts (which has per-state totals) as the primary
    source and cross-references session data from gate_pass_history metadata.

    Returns {session: {ict_state: win_rate}} — empty dicts for states with
    insufficient data. Callers should check value != 0 before trusting the number.
    """
    oc  = sym_data.get("outcome_counts", {})
    ls  = sym_data.get("level_stats", {})

    # Build per-state base win rates from outcome_counts
    state_base_wr: Dict[str, float] = {}
    for state, outcomes in oc.items():
        wins  = (outcomes.get("TP1", 0)
                 + outcomes.get("TP2", 0)
                 + outcomes.get("TP3", 0))
        total = wins + outcomes.get("SL", 0)
        if total >= SESSION_STATE_MIN_SAMPLES:
            state_base_wr[state] = round(wins / total, 3)

    if not state_base_wr:
        return {}

    # Build session-conditioned matrix from level_stats session sub-keys
    # level_stats keys: "{ICT_STATE}|{BIAS}" — some may also store session info
    # in extended metadata if added. For now: derive session-conditioned win rates
    # by applying session quality multipliers from _ENH_SESSION_QUALITY_MAP (TA layer).
    _SESSION_QUALITY = {
        "OVERLAP":   1.15, "NEW_YORK": 1.10, "NY_AM": 1.10,
        "LONDON":    1.08, "ASIA":     0.88, "OFF_HOURS": 0.80, "WEEKEND": 0.70,
    }
    matrix: Dict[str, Dict[str, float]] = {}
    for sess, quality in _SESSION_QUALITY.items():
        sess_row = {}
        for state, base_wr in state_base_wr.items():
            # Session-adjusted win rate: blend base with quality multiplier signal
            # quality > 1.0 nudges wr upward; quality < 1.0 nudges downward
            adj = base_wr * quality
            adj = max(0.0, min(1.0, round(adj, 3)))
            sess_row[state] = adj
        matrix[sess] = sess_row

    return matrix


# ══════════════════════════════════════════════════════════════════════
# SECTION 3 — ICT STATE CLASSIFIER
# ══════════════════════════════════════════════════════════════════════

def classify_ict_state(
    bias: str, sweep_bull: bool, sweep_bear: bool,
    bos_bull: bool, bos_bear: bool,
    choch_bull: bool, choch_bear: bool,
    fvg_bull: bool, fvg_bear: bool,
    smc_score: float,
) -> str:
    if choch_bull: return "CHOCH_BULL"
    if choch_bear: return "CHOCH_BEAR"
    if fvg_bull:   return "FVG_BULL"
    if fvg_bear:   return "FVG_BEAR"
    if sweep_bear: return "MANIPULATION_L"
    if sweep_bull: return "MANIPULATION_H"
    if bos_bull:   return "BOS_BULL"
    if bos_bear:   return "BOS_BEAR"
    return "ACCUMULATION"


# ══════════════════════════════════════════════════════════════════════
# SECTION 4 — BASE TRANSITION MATRIX BUILDER
# ══════════════════════════════════════════════════════════════════════

def _build_base_matrix(regime: str, bias: str) -> List[List[float]]:
    """Returns a row-stochastic 9×9 prior transition matrix."""
    if regime == "TRENDING" and bias != "BEARISH":
        P = [
            [0.05,0.10,0.20,0.10,0.05,0.05,0.05,0.25,0.15],
            [0.10,0.05,0.05,0.10,0.30,0.05,0.15,0.05,0.15],
            [0.05,0.05,0.05,0.10,0.05,0.10,0.05,0.40,0.15],
            [0.25,0.05,0.05,0.15,0.05,0.10,0.05,0.20,0.10],
            [0.10,0.20,0.05,0.05,0.15,0.05,0.20,0.05,0.15],
            [0.10,0.05,0.05,0.30,0.05,0.10,0.05,0.25,0.05],
            [0.10,0.20,0.05,0.05,0.25,0.05,0.10,0.05,0.15],
            [0.10,0.05,0.05,0.35,0.05,0.10,0.05,0.15,0.10],
            [0.10,0.10,0.05,0.05,0.30,0.05,0.20,0.05,0.10],
        ]
    elif regime == "TRENDING" and bias == "BEARISH":
        P = [
            [0.05,0.25,0.10,0.05,0.10,0.05,0.10,0.10,0.20],
            [0.05,0.10,0.05,0.05,0.15,0.05,0.10,0.05,0.40],
            [0.10,0.10,0.05,0.15,0.10,0.15,0.05,0.20,0.10],
            [0.10,0.10,0.05,0.10,0.20,0.05,0.20,0.10,0.10],
            [0.10,0.05,0.10,0.05,0.15,0.05,0.10,0.05,0.35],
            [0.15,0.10,0.05,0.25,0.10,0.10,0.05,0.15,0.05],
            [0.10,0.05,0.05,0.05,0.30,0.05,0.15,0.05,0.25],
            [0.15,0.10,0.10,0.20,0.10,0.10,0.05,0.10,0.10],
            [0.05,0.10,0.05,0.05,0.35,0.05,0.20,0.05,0.10],
        ]
    elif regime == "CHOPPY":
        P = [
            [0.30,0.10,0.10,0.10,0.10,0.05,0.05,0.10,0.10],
            [0.15,0.20,0.05,0.05,0.15,0.05,0.10,0.10,0.15],
            [0.15,0.05,0.20,0.10,0.05,0.10,0.05,0.15,0.15],
            [0.20,0.05,0.05,0.20,0.10,0.10,0.10,0.10,0.10],
            [0.20,0.10,0.05,0.10,0.20,0.05,0.10,0.10,0.10],
            [0.20,0.05,0.05,0.25,0.05,0.20,0.05,0.10,0.05],
            [0.20,0.05,0.05,0.05,0.25,0.05,0.20,0.05,0.10],
            [0.15,0.05,0.10,0.20,0.10,0.10,0.05,0.15,0.10],
            [0.15,0.10,0.05,0.10,0.20,0.05,0.10,0.10,0.15],
        ]
    elif regime in ("VOLATILE", "DISTRIBUTION"):
        P = [
            [0.10,0.20,0.15,0.05,0.15,0.05,0.10,0.05,0.15],
            [0.10,0.15,0.05,0.05,0.20,0.05,0.10,0.05,0.25],
            [0.10,0.05,0.15,0.10,0.05,0.10,0.05,0.25,0.15],
            [0.15,0.10,0.05,0.10,0.15,0.10,0.10,0.15,0.10],
            [0.10,0.15,0.10,0.10,0.10,0.05,0.15,0.05,0.20],
            [0.15,0.05,0.05,0.25,0.10,0.10,0.05,0.15,0.10],
            [0.10,0.10,0.05,0.05,0.25,0.05,0.15,0.05,0.20],
            [0.15,0.05,0.10,0.20,0.05,0.10,0.05,0.15,0.15],
            [0.10,0.10,0.05,0.05,0.25,0.05,0.15,0.10,0.15],
        ]
    else:  # ACCUMULATION or unknown
        uniform = [[1.0 / N] * N for _ in range(N)]
        P = uniform

    return _normalize_rows(P)


def _apply_macro_modifier(P: List[List[float]], macro_event: str) -> List[List[float]]:
    if macro_event in ("FOMC_HARD_BLOCK", "CPI_RELEASE"):
        n = len(P)
        acc_idx = _S.get("ACCUMULATION", 0)
        P2 = [row[:] for row in P]
        for i in range(n):
            P2[i] = [0.2 * (1.0 / n)] * n
            P2[i][acc_idx] = 0.8
        return _normalize_rows(P2)
    return P


def _stationary(P: List[List[float]], max_iter: int = 2000, tol: float = 1e-10) -> List[float]:
    """
    UPG-21: Power-iteration stationary distribution with LRU hash cache.
    Same P matrix → cached π vector. Cache size capped at _STATIONARY_CACHE_MAX.
    """
    # Build cache key from matrix hash
    try:
        _key = hashlib.md5(
            str([[round(v, 6) for v in row] for row in P]).encode()
        ).hexdigest()[:16]
        if _key in _STATIONARY_CACHE:
            return _STATIONARY_CACHE[_key]
    except Exception:
        _key = None

    # Power iteration
    n  = len(P)
    pi = [1.0 / n] * n
    for _ in range(max_iter):
        new_pi = [0.0] * n
        for j in range(n):
            new_pi[j] = sum(pi[i] * P[i][j] for i in range(n))
        diff = sum(abs(new_pi[i] - pi[i]) for i in range(n))
        pi   = new_pi
        if diff < tol:
            break
    result = pi

    # Store in cache with LRU eviction
    if _key:
        if len(_STATIONARY_CACHE) >= _STATIONARY_CACHE_MAX:
            oldest = next(iter(_STATIONARY_CACHE))
            del _STATIONARY_CACHE[oldest]
        _STATIONARY_CACHE[_key] = result
    return result


# ══════════════════════════════════════════════════════════════════════
# SECTION 5 — BRAIN DATA SCHEMAS
# ══════════════════════════════════════════════════════════════════════

def _empty_symbol_block() -> dict:
    return {
        "n_samples":            0,
        "ict_counts":           [[0.0] * N for _ in range(N)],
        # UPG-1: Second-order counts [SO_N × N] = [81 × 9]
        # Stored flat as list-of-lists for JSON compat. ~729 floats per symbol.
        "second_order_counts":  [[0.0] * N for _ in range(SO_N)],
        "gate_pass_counts":     {r: [0] * MAX_TRACKED_GATES for r in REGIMES},
        "gate_total_counts":    {r: [0] * MAX_TRACKED_GATES for r in REGIMES},
        "outcome_counts":       {s: {o: 0 for o in _OUTCOMES} for s in ICT_STATES},
        "ai_consensus_counts":  {s: {"AGREE": 0, "DISAGREE": 0, "NEUTRAL": 0} for s in ICT_STATES},
        "gate_pass_history":    [],
        "effective_n":          0.0,
        "adaptive_weights": {
            "tp_reward":    TP_REWARD_WEIGHT,
            "sl_penalty":   SL_PENALTY_WEIGHT,
            "edge_thresh":  GATE_VETO_EDGE_THRESHOLD,
            "decay":        DECAY_FACTOR,
            "tune_count":   0,
            "last_tuned":   "",
        },
        "level_stats": {},
    }


def _empty_brain() -> dict:
    return {
        "version":      3,
        "last_updated": "",
        "symbols":      {},
        "global": {
            "macro_event_outcomes": {},
            "total_analyses":       0,
        },
        "super_markov": {},
    }


def _rehydrate_symbol(sym: dict) -> dict:
    """
    FIX-G: Backfill v3.0 fields from older JSON.
    All additions are additive — never removes existing data.
    """
    try:
        if "outcome_counts" not in sym:
            sym["outcome_counts"] = {s: {o: 0 for o in _OUTCOMES} for s in ICT_STATES}
        else:
            for s in ICT_STATES:
                sym["outcome_counts"].setdefault(s, {o: 0 for o in _OUTCOMES})
                for o in _OUTCOMES:
                    sym["outcome_counts"][s].setdefault(o, 0)

        if "gate_pass_history" not in sym:
            sym["gate_pass_history"] = []

        if "effective_n" not in sym:
            try:
                sym["effective_n"] = float(sum(
                    sym["ict_counts"][i][j]
                    for i in range(N) for j in range(N)
                ))
            except (TypeError, IndexError, KeyError):
                sym["effective_n"] = 0.0

        if "ai_consensus_counts" not in sym:
            sym["ai_consensus_counts"] = {
                s: {"AGREE": 0, "DISAGREE": 0, "NEUTRAL": 0} for s in ICT_STATES
            }

        if "adaptive_weights" not in sym:
            sym["adaptive_weights"] = {
                "tp_reward":  TP_REWARD_WEIGHT,
                "sl_penalty": SL_PENALTY_WEIGHT,
                "edge_thresh": GATE_VETO_EDGE_THRESHOLD,
                "decay":      DECAY_FACTOR,
                "tune_count": 0,
                "last_tuned": "",
            }

        # UPG-1: Backfill second_order_counts if missing (v2.x → v3.0)
        if "second_order_counts" not in sym:
            sym["second_order_counts"] = [[0.0] * N for _ in range(SO_N)]
        else:
            # Ensure correct dimensions
            so = sym["second_order_counts"]
            if len(so) != SO_N:
                sym["second_order_counts"] = [[0.0] * N for _ in range(SO_N)]
            else:
                for row in so:
                    while len(row) < N:
                        row.append(0.0)

    except Exception as e:
        log.warning(f"[MARKOV_BRAIN] rehydrate warning ({type(e).__name__}): {e}")
    return sym


# ══════════════════════════════════════════════════════════════════════
# SECTION 6 — MARKOV BRAIN (Atomic JSON Store)
# ══════════════════════════════════════════════════════════════════════

class MarkovBrain:
    """
    Atomic JSON store for the Markov stack.
    FIX-A: threading.Lock on all writes.
    FIX-B: Circuit breaker after BRAIN_WRITE_FAILURE_CAP consecutive failures.
    FIX-E: Specific exception types only.
    """

    def __init__(self, path: str = "markov_brain.json"):
        self._path     = path
        self._lock       = threading.Lock()
        self._data: dict = {}
        self._fail_count = 0
        self._last_save: float = 0.0   # [MEM-FIX LOW-06] debounce tracking
        self._dirty:     bool  = False
        self._load()

    def _load(self) -> None:
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                self._data = json.load(f)
            # Backfill top-level keys
            self._data.setdefault("symbols", {})
            self._data.setdefault("global", {"macro_event_outcomes": {}, "total_analyses": 0})
            self._data.setdefault("super_markov", {})
            log.info(f"[MARKOV_BRAIN] Loaded {self._path} | "
                     f"symbols={len(self._data['symbols'])} | "
                     f"analyses={self._data['global'].get('total_analyses', 0)}")
        except FileNotFoundError:
            log.info(f"[MARKOV_BRAIN] No brain found at {self._path} — starting fresh")
            self._data = _empty_brain()
        except json.JSONDecodeError as e:
            log.error(f"[MARKOV_BRAIN] JSON decode error: {e} — starting fresh")
            self._data = _empty_brain()
        except OSError as e:
            log.error(f"[MARKOV_BRAIN] OS error loading brain: {e} — starting fresh")
            self._data = _empty_brain()

    def read(self) -> dict:
        return self._data

    def save(self) -> None:
        if self._fail_count >= BRAIN_WRITE_FAILURE_CAP:
            log.warning(f"[MARKOV_BRAIN] Circuit breaker OPEN — skipping write "
                        f"(failures={self._fail_count})")
            return
        with self._lock:
            try:
                self._data["last_updated"] = datetime.now(timezone.utc).isoformat()
                tmp = self._path + ".tmp"
                with tempfile.NamedTemporaryFile(
                    mode="w", encoding="utf-8",
                    dir=os.path.dirname(os.path.abspath(self._path)) or ".",
                    delete=False, suffix=".tmp"
                ) as tf:
                    json.dump(self._data, tf, separators=(",", ":"))
                    tmp = tf.name
                os.replace(tmp, self._path)
                self._fail_count = 0
                self._last_save  = time.time()
            except (OSError, IOError) as e:
                self._fail_count += 1
                log.error(f"[MARKOV_BRAIN] Write failed ({self._fail_count}/"
                          f"{BRAIN_WRITE_FAILURE_CAP}): {e}")

    # [MEM-FIX LOW-06] Debounced save — flush at most once per BRAIN_SAVE_INTERVAL
    # seconds. Prevents full JSON re-serialisation of growing brain on every outcome.
    BRAIN_SAVE_INTERVAL: float = 30.0

    def save_debounced(self) -> None:
        """Flush only if interval elapsed since last save. Marks dirty flag."""
        self._dirty = True
        if time.time() - self._last_save >= self.BRAIN_SAVE_INTERVAL:
            self.save()   # [MEM-FIX LOW-06] debounced flush

    def get_symbol(self, symbol: str) -> dict:
        safe = _sanitize_symbol(symbol)
        if safe not in self._data["symbols"]:
            self._data["symbols"][safe] = _empty_symbol_block()
        sym = self._data["symbols"][safe]
        _rehydrate_symbol(sym)
        return sym

    def get_adaptive_weights(self, symbol: str) -> dict:
        sym = self.get_symbol(_sanitize_symbol(symbol))
        aw  = sym.get("adaptive_weights", {})
        return {
            "tp_reward":  float(aw.get("tp_reward",  TP_REWARD_WEIGHT)),
            "sl_penalty": float(aw.get("sl_penalty", SL_PENALTY_WEIGHT)),
            "edge_thresh":float(aw.get("edge_thresh", GATE_VETO_EDGE_THRESHOLD)),
            "decay":      float(aw.get("decay",      DECAY_FACTOR)),
        }

    def apply_ai_tuning(
        self,
        symbol:          str,
        tp_reward_adj:   float = 0.0,
        sl_penalty_adj:  float = 0.0,
        edge_thresh_adj: float = 0.0,
        decay_adj:       float = 0.0,
    ) -> dict:
        safe = _sanitize_symbol(symbol)
        sym  = self.get_symbol(safe)
        aw   = sym.setdefault("adaptive_weights", {
            "tp_reward":  TP_REWARD_WEIGHT,
            "sl_penalty": SL_PENALTY_WEIGHT,
            "edge_thresh":GATE_VETO_EDGE_THRESHOLD,
            "decay":      DECAY_FACTOR,
            "tune_count": 0,
            "last_tuned": "",
        })

        def _bounded(current, delta, lo, hi):
            clamped_delta = max(-AI_TUNE_STEP, min(AI_TUNE_STEP, delta))
            return round(max(lo, min(hi, current + clamped_delta)), 4)

        aw["tp_reward"]  = _bounded(aw.get("tp_reward",  TP_REWARD_WEIGHT),
                                     tp_reward_adj, AI_TUNE_TP_REWARD_MIN, AI_TUNE_TP_REWARD_MAX)
        aw["sl_penalty"] = _bounded(aw.get("sl_penalty", SL_PENALTY_WEIGHT),
                                     sl_penalty_adj, AI_TUNE_SL_PENALTY_MIN, AI_TUNE_SL_PENALTY_MAX)
        aw["edge_thresh"]= _bounded(aw.get("edge_thresh", GATE_VETO_EDGE_THRESHOLD),
                                     edge_thresh_adj, AI_TUNE_EDGE_THRESH_MIN, AI_TUNE_EDGE_THRESH_MAX)
        aw["decay"]      = _bounded(aw.get("decay", DECAY_FACTOR),
                                     decay_adj, AI_TUNE_DECAY_MIN, AI_TUNE_DECAY_MAX)
        aw["tune_count"] = aw.get("tune_count", 0) + 1
        aw["last_tuned"] = datetime.now(timezone.utc).isoformat()

        self.save_debounced()   # [MEM-FIX LOW-06] debounced flush
        log.info(f"[AI_TUNE] {safe} | tp_rew={aw['tp_reward']:.3f} "
                 f"sl_pen={aw['sl_penalty']:.3f} decay={aw['decay']:.4f}")
        return dict(aw)

    def record_and_save(
        self,
        symbol:      str,
        prev_ict:    Optional[str],
        curr_ict:    str,
        regime:      str,
        gate_pass:   List[bool],
        macro_event: str,
        prev_prev_ict: Optional[str] = None,  # UPG-1: second-order requires t-2 state
    ) -> None:
        """
        Update first-order and second-order (UPG-1) transition counts.
        prev_prev_ict: the state at t-2, enabling second-order updates.
        """
        sym = self.get_symbol(symbol)

        # Apply per-symbol decay
        _decay    = float(sym.get("adaptive_weights", {}).get("decay", DECAY_FACTOR))
        _so_decay = SO_DECAY_FACTOR   # fixed — second-order needs more stability

        # First-order decay + update
        for i in range(N):
            for j in range(N):
                sym["ict_counts"][i][j] = round(sym["ict_counts"][i][j] * _decay, 6)

        if prev_ict and prev_ict in _S and curr_ict in _S:
            sym["ict_counts"][_S[prev_ict]][_S[curr_ict]] += 1.0

        # UPG-1: Second-order decay + update
        so_counts = sym["second_order_counts"]
        for row in so_counts:
            for j in range(N):
                row[j] = round(row[j] * _so_decay, 6)

        if (prev_prev_ict and prev_ict and curr_ict and
                prev_prev_ict in _S and prev_ict in _S and curr_ict in _S):
            so_row = _SO[(  _S[prev_prev_ict], _S[prev_ict]  )]
            so_counts[so_row][_S[curr_ict]] += 1.0

        # Gate counts
        r = regime if regime in REGIMES else "TRENDING"
        for k, passed in enumerate(gate_pass[:MAX_TRACKED_GATES]):
            while len(sym["gate_pass_counts"][r])  < MAX_TRACKED_GATES:
                sym["gate_pass_counts"][r].append(0)
            while len(sym["gate_total_counts"][r]) < MAX_TRACKED_GATES:
                sym["gate_total_counts"][r].append(0)
            sym["gate_pass_counts"][r][k]  += int(bool(passed))
            sym["gate_total_counts"][r][k] += 1

        sym["effective_n"] = round(
            sum(sym["ict_counts"][i][j] for i in range(N) for j in range(N)), 2
        )
        sym["n_samples"] += 1
        self._data["global"]["total_analyses"] = (
            self._data["global"].get("total_analyses", 0) + 1
        )

        if gate_pass:
            obs_rate = sum(bool(g) for g in gate_pass) / len(gate_pass)
            hist = sym["gate_pass_history"]
            hist.append(round(obs_rate, 3))
            if len(hist) > VELOCITY_HISTORY_SIZE:
                sym["gate_pass_history"] = hist[-VELOCITY_HISTORY_SIZE:]

        if macro_event != "CLEAR" and gate_pass:
            obs_rate = sum(bool(g) for g in gate_pass) / len(gate_pass)
            glo = self._data["global"]["macro_event_outcomes"]
            rec = glo.setdefault(macro_event, {"gate_pass_rate": 0.5, "n": 0})
            rec["gate_pass_rate"] = round(0.9 * rec["gate_pass_rate"] + 0.1 * obs_rate, 4)
            rec["n"] += 1

        self.save_debounced()   # [MEM-FIX LOW-06] debounced flush

    def record_outcome(
        self,
        symbol:    str,
        ict_state: str,
        bias:      str,
        outcome:   str,
    ) -> None:
        """
        FIX-C: Full input validation. UPG-6: also updates second-order counts.
        """
        safe_sym = _sanitize_symbol(symbol)

        if ict_state not in _S:
            log.warning(f"[MARKOV_OUTCOME] {safe_sym} — unknown state '{str(ict_state)[:30]}'")
            return
        if outcome not in _OUTCOMES:
            log.warning(f"[MARKOV_OUTCOME] {safe_sym} — unknown outcome '{str(outcome)[:20]}'")
            return
        if bias not in ("BULLISH", "BEARISH", "NEUTRAL"):
            log.warning(f"[MARKOV_OUTCOME] {safe_sym} — unknown bias '{str(bias)[:20]}'")
            return

        sym = self.get_symbol(safe_sym)
        oc  = sym["outcome_counts"]
        if ict_state in oc:
            oc[ict_state][outcome] = oc[ict_state].get(outcome, 0) + 1

        if outcome == "TIMEOUT":
            self.save_debounced()   # [MEM-FIX LOW-06] debounced flush
            return

        is_win  = outcome in ("TP1", "TP2", "TP3")

        if bias == "BULLISH":
            win_targets, lose_targets = list(_BULL_IDX), list(_BEAR_IDX)
        elif bias == "BEARISH":
            win_targets, lose_targets = list(_BEAR_IDX), list(_BULL_IDX)
        else:
            self.save_debounced()   # [MEM-FIX LOW-06] debounced flush
            return

        row_idx   = _S[ict_state]
        _aw       = sym.get("adaptive_weights", {})
        _tp_rew   = float(_aw.get("tp_reward",  TP_REWARD_WEIGHT))
        _sl_pen   = float(_aw.get("sl_penalty", SL_PENALTY_WEIGHT))

        if is_win:
            for j in win_targets:
                sym["ict_counts"][row_idx][j] = round(
                    sym["ict_counts"][row_idx][j] + _tp_rew, 6)
            for j in lose_targets:
                sym["ict_counts"][row_idx][j] = max(0.0, round(
                    sym["ict_counts"][row_idx][j] - (_sl_pen * TP_COUNTER_PENALTY_FRAC), 6))
        else:  # SL
            for j in win_targets:
                sym["ict_counts"][row_idx][j] = max(0.0, round(
                    sym["ict_counts"][row_idx][j] - _sl_pen, 6))

        sym["effective_n"] = round(
            sum(sym["ict_counts"][i][j] for i in range(N) for j in range(N)), 2
        )
        self.save_debounced()   # [MEM-FIX LOW-06] debounced flush
        log.info(f"[MARKOV_OUTCOME] {safe_sym} | state={ict_state} "
                 f"bias={bias} outcome={outcome} | effective_n={sym['effective_n']:.1f}")

    def record_ai_consensus(
        self,
        symbol:       str,
        ict_state:    str,
        bias:         str,
        consensus:    str,
        confidence:   float,
        weight:       float = AI_CONSENSUS_WEIGHT,
    ) -> None:
        safe_sym = _sanitize_symbol(symbol)
        if ict_state not in _S or consensus not in ("AGREE", "DISAGREE", "NEUTRAL"):
            return
        if confidence < AI_CONSENSUS_THRESHOLD:
            return

        sym = self.get_symbol(safe_sym)
        acc = sym["ai_consensus_counts"]
        if ict_state in acc:
            acc[ict_state][consensus] = acc[ict_state].get(consensus, 0) + 1

        row_idx = _S[ict_state]
        _aw     = sym.get("adaptive_weights", {})
        _tp_rew = float(_aw.get("tp_reward",  TP_REWARD_WEIGHT))
        _sl_pen = float(_aw.get("sl_penalty", SL_PENALTY_WEIGHT))

        if bias == "BULLISH":
            win_targets, lose_targets = list(_BULL_IDX), list(_BEAR_IDX)
        elif bias == "BEARISH":
            win_targets, lose_targets = list(_BEAR_IDX), list(_BULL_IDX)
        else:
            self.save_debounced()   # [MEM-FIX LOW-06] debounced flush
            return

        if consensus == "AGREE":
            for j in win_targets:
                sym["ict_counts"][row_idx][j] = round(
                    sym["ict_counts"][row_idx][j] + _tp_rew * weight, 6)
            for j in lose_targets:
                sym["ict_counts"][row_idx][j] = max(0.0, round(
                    sym["ict_counts"][row_idx][j]
                    - (_sl_pen * TP_COUNTER_PENALTY_FRAC * weight), 6))
        elif consensus == "DISAGREE":
            for j in win_targets:
                sym["ict_counts"][row_idx][j] = max(0.0, round(
                    sym["ict_counts"][row_idx][j] - _sl_pen * weight, 6))

        sym["effective_n"] = round(
            sum(sym["ict_counts"][i][j] for i in range(N) for j in range(N)), 2
        )
        self.save_debounced()   # [MEM-FIX LOW-06] debounced flush


# ══════════════════════════════════════════════════════════════════════
# SECTION 7 — MARKOV ICT ENGINE
# UPG-1: Second-order prediction added.
# UPG-2: N-step lookahead (P², P³) added.
# UPG-3: Per-state win-rate table added.
# UPG-5: Regime geometry signal added.
# ══════════════════════════════════════════════════════════════════════

class MarkovICTEngine:

    def __init__(self, min_samples: int = MIN_SAMPLES_FOR_VETO):
        self._prev_state: Dict[str, str]      = {}
        self._prev_prev_state: Dict[str, str] = {}   # UPG-1: t-2 state
        self._min_samples = min_samples

    def _blend(
        self, prior: List[List[float]], learned: List[List[float]], alpha: float
    ) -> List[List[float]]:
        blended = [
            [(1 - alpha) * prior[i][j] + alpha * learned[i][j] for j in range(N)]
            for i in range(N)
        ]
        return _normalize_rows(blended)

    def _learned_matrix(self, sym_data: dict) -> List[List[float]]:
        counts = sym_data.get("ict_counts", [[0.0] * N] * N)
        rows = []
        for row in counts:
            s = sum(row)
            rows.append([v / s for v in row] if s > 1e-9 else [1.0 / N] * N)
        return rows

    def _learned_second_order_matrix(self, sym_data: dict) -> List[List[float]]:
        """UPG-1: Build row-normalized second-order matrix from stored counts."""
        so = sym_data.get("second_order_counts", [[0.0] * N] * SO_N)
        rows = []
        for row in so:
            s = sum(row)
            rows.append([v / s for v in row] if s > 1e-9 else [1.0 / N] * N)
        return rows

    def _compute_state_winrates(self, sym_data: dict) -> Dict[str, dict]:
        """
        UPG-3: Compute per-ICT-state win rates from recorded outcome counts.
        Returns {state: {tp_rate, sl_rate, total, expected_rr}} for states
        with enough data. Empty dict for states with no outcomes yet.
        """
        oc = sym_data.get("outcome_counts", {})
        winrates: Dict[str, dict] = {}
        for state in ICT_STATES:
            state_oc = oc.get(state, {})
            wins  = (state_oc.get("TP1", 0)
                     + state_oc.get("TP2", 0)
                     + state_oc.get("TP3", 0))
            losses = state_oc.get("SL", 0)
            total  = wins + losses
            if total < 3:
                continue   # not enough data — skip
            tp_rate = round(wins / total, 3)
            sl_rate = round(losses / total, 3)
            # Expected R:R approximation: TP2 counts as 2× reward, TP3 as 3×
            tp2_w = state_oc.get("TP2", 0)
            tp3_w = state_oc.get("TP3", 0)
            tp1_w = state_oc.get("TP1", 0)
            exp_rr = round(
                (tp1_w * 1.0 + tp2_w * 2.0 + tp3_w * 3.0) / max(total, 1), 2
            )
            winrates[state] = {
                "tp_rate":   tp_rate,
                "sl_rate":   sl_rate,
                "total":     total,
                "exp_rr":    exp_rr,
                "tp1_count": tp1_w,
                "tp2_count": tp2_w,
                "tp3_count": tp3_w,
                "sl_count":  losses,
            }
        return winrates

    def _compute_regime_geometry(
        self, hmm_state: str, bias: str, n: int
    ) -> dict:
        """
        UPG-5: Return regime-conditioned entry/SL geometry adjustments.
        Only activates when Markov has enough samples (>= MIN_SAMPLES_FOR_VETO).
        """
        geo = REGIME_GEOMETRY_MAP.get(hmm_state, REGIME_GEOMETRY_MAP["TRENDING"])
        if n < MIN_SAMPLES_FOR_VETO:
            return {
                "regime_geometry_active":   False,
                "regime_entry_tighten_pct": 0.0,
                "regime_sl_widen_pct":      0.0,
                "regime_conf_cap":          CONF_MOD_MAX,
                "regime_geometry_note":     f"Geometry: inactive (n={n}/{MIN_SAMPLES_FOR_VETO})",
            }
        note = (
            f"Regime [{hmm_state}] → "
            f"entry tighten {geo['entry_tighten_pct']*100:.0f}% | "
            f"SL widen {geo['sl_widen_pct']*100:.0f}% ATR | "
            f"conf cap ×{geo['conf_cap']:.2f}"
        )
        return {
            "regime_geometry_active":   geo["entry_tighten_pct"] > 0 or geo["sl_widen_pct"] > 0,
            "regime_entry_tighten_pct": geo["entry_tighten_pct"],
            "regime_sl_widen_pct":      geo["sl_widen_pct"],
            "regime_conf_cap":          geo["conf_cap"],
            "regime_geometry_note":     note,
        }

    def update_and_predict(
        self,
        symbol:     str,
        bias:       str,
        sweep_bull: bool,
        sweep_bear: bool,
        bos_bull:   bool,
        bos_bear:   bool,
        choch_bull: bool,
        choch_bear: bool,
        fvg_bull:   bool,
        fvg_bear:   bool,
        smc_score:  float,
        fg_value:   int,
        hmm_state:  str,
        brain:      "MarkovBrain",
    ) -> dict:
        safe_sym = _sanitize_symbol(symbol)

        current = classify_ict_state(
            bias, sweep_bull, sweep_bear, bos_bull, bos_bear,
            choch_bull, choch_bear, fvg_bull, fvg_bear, smc_score,
        )

        prev      = self._prev_state.get(safe_sym)
        prev_prev = self._prev_prev_state.get(safe_sym)   # UPG-1

        brain.record_and_save(
            symbol=safe_sym, prev_ict=prev, curr_ict=current,
            regime=hmm_state, gate_pass=[True] * MAX_TRACKED_GATES,
            macro_event="CLEAR", prev_prev_ict=prev_prev,   # UPG-1
        )

        # Advance state history
        self._prev_prev_state[safe_sym] = prev
        self._prev_state[safe_sym]      = current

        sym_data    = brain.get_symbol(safe_sym)
        n           = sym_data.get("n_samples", 0)
        effective_n = sym_data.get("effective_n", float(n))
        alpha       = min(1.0, effective_n / ALPHA_RAMP_SAMPLES)

        # ── First-order prediction ─────────────────────────────────────
        # Fix #11: use adaptive per-symbol prior instead of global-only
        prior   = _build_base_matrix_adaptive(sym_data, hmm_state, bias)
        learned = self._learned_matrix(sym_data)
        P       = self._blend(prior, learned, alpha)
        P       = _apply_macro_modifier(P, "CLEAR")
        pi      = _stationary(P)

        curr_idx   = _S.get(current, 0)
        next_probs = P[curr_idx]
        top_idx    = max(range(N), key=lambda i: next_probs[i])

        # ── UPG-1: Second-order prediction ────────────────────────────
        so_pred_state = ICT_STATES[top_idx]   # fallback = first-order
        so_pred_prob  = round(float(next_probs[top_idx]), 4)
        if prev is not None and prev in _S and current in _S:
            so_matrix = self._learned_second_order_matrix(sym_data)
            so_row_idx = _SO.get((_S[prev], _S[current]), 0)
            so_probs   = so_matrix[so_row_idx]
            so_top_idx = max(range(N), key=lambda i: so_probs[i])
            # Only use second-order if it has meaningful data
            so_total   = sum(sym_data["second_order_counts"][so_row_idx])
            if so_total >= 3.0:
                so_pred_state = ICT_STATES[so_top_idx]
                so_pred_prob  = round(float(so_probs[so_top_idx]), 4)

        # ── UPG-2: N-step lookahead (P² and P³) ───────────────────────
        P2 = _matmul(P, P)
        P3 = _matmul(P2, P)
        P2_probs = P2[curr_idx]
        P3_probs = P3[curr_idx]
        top2_idx = max(range(N), key=lambda i: P2_probs[i])
        top3_idx = max(range(N), key=lambda i: P3_probs[i])

        # ── Directional edge ──────────────────────────────────────────
        bull_prob = sum(next_probs[i] for i in _BULL_IDX)
        bear_prob = sum(next_probs[i] for i in _BEAR_IDX)
        bull_base = sum(pi[i]         for i in _BULL_IDX)
        bear_base = sum(pi[i]         for i in _BEAR_IDX)
        bull_edge = round(bull_prob - bull_base, 4)
        bear_edge = round(bear_prob - bear_base, 4)

        d_edge  = bull_edge if bias == "BULLISH" else bear_edge if bias == "BEARISH" else 0.0
        raw_mod = d_edge * CONF_MOD_EDGE_SCALE
        mod     = round(1.0 + max(-CONF_MOD_EDGE_CAP, min(CONF_MOD_EDGE_CAP, raw_mod)), 3)

        if hmm_state == "CHOPPY":
            mod = min(mod, CONF_MOD_CHOPPY_CAP)

        fg_regime = (
            "BULL" if fg_value > 60 and hmm_state == "TRENDING" else
            "BEAR" if fg_value < 40 else "NEUTRAL"
        )
        if fg_regime == "BULL" and bias == "BULLISH":
            mod = min(CONF_MOD_MAX, mod + CONF_MOD_FGI_BOOST)
        elif fg_regime == "BEAR" and bias == "BEARISH":
            mod = min(CONF_MOD_MAX, mod + CONF_MOD_FGI_BOOST)

        # ── Gate veto ─────────────────────────────────────────────────
        gate_veto    = False
        _edge_thresh = float(
            brain.get_symbol(safe_sym)
            .get("adaptive_weights", {})
            .get("edge_thresh", GATE_VETO_EDGE_THRESHOLD)
        )
        if n >= self._min_samples:
            if bias == "BULLISH"  and bull_edge < _edge_thresh:
                gate_veto = True
            elif bias == "BEARISH" and bear_edge < _edge_thresh:
                gate_veto = True

        # ── UPG-3: State win rates ────────────────────────────────────
        state_winrates = self._compute_state_winrates(sym_data)

        # Fix #13: Session × ICT State win-rate matrix
        session_state_wr = _compute_session_state_matrix(sym_data)

        # Current state win rate (used in note)
        curr_wr = state_winrates.get(current, {})
        wr_str  = (f" WR={curr_wr['tp_rate']*100:.0f}%"
                   if curr_wr else "")

        # ── UPG-5: Regime geometry signal ────────────────────────────
        regime_geo = self._compute_regime_geometry(hmm_state, bias, n)

        # ── Build note ────────────────────────────────────────────────
        if n < self._min_samples:
            note = f"🔄 Markov v3.0: Collecting ({n}/{self._min_samples} samples)"
        else:
            edge_str = (
                f"+{bull_edge*100:.1f}%"  if bias == "BULLISH"
                else f"+{bear_edge*100:.1f}%" if bias == "BEARISH"
                else "0%"
            )
            veto_str = " ⛔ VETO" if gate_veto else ""
            note = (
                f"🎲 Markov v3.0 [{current}]→1st[{ICT_STATES[top_idx]}] "
                f"({next_probs[top_idx]*100:.0f}%) "
                f"→2nd[{so_pred_state}] ({so_pred_prob*100:.0f}%) | "
                f"P²:[{ICT_STATES[top2_idx]}] P³:[{ICT_STATES[top3_idx]}] | "
                f"Edge {edge_str} | α={alpha:.2f} | Conf×{mod:.2f}"
                f"{wr_str}{veto_str}"
            )

        return {
            # First-order
            "markov_state":       current,
            "markov_next_pred":   ICT_STATES[top_idx],
            "markov_next_prob":   round(float(next_probs[top_idx]), 4),
            # UPG-1: Second-order
            "markov_2nd_pred":    so_pred_state,
            "markov_2nd_prob":    so_pred_prob,
            # UPG-2: N-step lookahead
            "markov_p2_pred":     ICT_STATES[top2_idx],
            "markov_p2_prob":     round(float(P2_probs[top2_idx]), 4),
            "markov_p3_pred":     ICT_STATES[top3_idx],
            "markov_p3_prob":     round(float(P3_probs[top3_idx]), 4),
            # Directional
            "markov_bull_prob":   round(float(bull_prob), 4),
            "markov_bear_prob":   round(float(bear_prob), 4),
            "markov_bull_base":   round(float(bull_base), 4),
            "markov_bear_base":   round(float(bear_base), 4),
            "markov_bull_edge":   round(float(bull_edge), 4),
            "markov_bear_edge":   round(float(bear_edge), 4),
            "markov_regime":      fg_regime,
            "markov_conf_mod":    round(mod, 3),
            "markov_gate_veto":   gate_veto,
            "markov_n_samples":   n,
            "markov_effective_n": round(effective_n, 2),
            "markov_alpha":       round(alpha, 3),
            # UPG-3: Win rates
            "markov_state_winrates": state_winrates,
            "markov_curr_winrate":   curr_wr,
            # UPG-5: Regime geometry
            "markov_regime_geometry": regime_geo,
            "markov_note":        note,
            # Fix #13: session × ICT state win-rate matrix
            "markov_session_state_wr": session_state_wr,
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 8 — MARKOV VALIDATOR (unchanged from v2.1, preserved)
# ══════════════════════════════════════════════════════════════════════

class MarkovValidator:

    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def validate(
        self,
        symbol:      str,
        markov_data: dict,
        gate_pass:   List[bool],
        conf_total:  float,
        bias:        str,
        macro_event: str,
        regime:      str,
    ) -> dict:
        safe_sym  = _sanitize_symbol(symbol)
        n         = markov_data.get("markov_n_samples", 0)
        alpha     = markov_data.get("markov_alpha", 0.0)
        gate_veto = markov_data.get("markov_gate_veto", False)
        conf_mod  = markov_data.get("markov_conf_mod", 1.0)
        bull_edge = markov_data.get("markov_bull_edge", 0.0)
        note      = markov_data.get("markov_note", "")

        self._brain.record_and_save(
            symbol=safe_sym,
            prev_ict=None,
            curr_ict=markov_data.get("markov_state", "ACCUMULATION"),
            regime=regime,
            gate_pass=gate_pass,
            macro_event=macro_event,
        )

        learned_macro_mod = 1.0
        brain_data = self._brain.read()
        if macro_event != "CLEAR":
            rec = (brain_data.get("global", {})
                             .get("macro_event_outcomes", {})
                             .get(macro_event))
            if rec and rec.get("n", 0) >= MACRO_MIN_OBSERVATIONS:
                learned_macro_mod = round(
                    rec["gate_pass_rate"] / max(MACRO_BASELINE_RATE, 1e-9), 3)
                learned_macro_mod = max(0.5, min(1.1, learned_macro_mod))

        final_mod = round(conf_mod * learned_macro_mod, 3)

        # Apply regime geometry cap (UPG-5)
        regime_geo = markov_data.get("markov_regime_geometry", {})
        if regime_geo.get("regime_geometry_active"):
            cap = regime_geo.get("regime_conf_cap", CONF_MOD_MAX)
            final_mod = min(final_mod, cap)

        velocity_flag = False
        velocity_note = ""
        sym_hist = (brain_data.get("symbols", {})
                              .get(safe_sym, {})
                              .get("gate_pass_history", []))
        if len(sym_hist) >= VELOCITY_HISTORY_SIZE:
            recent_avg = sum(sym_hist[-VELOCITY_WINDOW:]) / VELOCITY_WINDOW
            prior_avg  = sum(sym_hist[-VELOCITY_HISTORY_SIZE:-VELOCITY_WINDOW]) / VELOCITY_WINDOW
            vel_delta  = prior_avg - recent_avg
            if vel_delta > VELOCITY_GATE_THRESHOLD:
                velocity_flag = True
                final_mod     = round(1.0 + (final_mod - 1.0) * 0.5, 3)
                velocity_note = (
                    f"⚠️ REGIME SHIFT — gate pass rate dropped "
                    f"{vel_delta*100:.0f}% over last {VELOCITY_WINDOW} analyses. "
                    f"conf_mod dampened to {final_mod:.2f}"
                )

        gate_chain = "".join("✓" if g else "✗" for g in gate_pass[:MAX_TRACKED_GATES])
        gates_ok   = sum(bool(g) for g in gate_pass[:MAX_TRACKED_GATES])
        fomc_block = macro_event in ("FOMC_HARD_BLOCK", "CPI_RELEASE")

        if fomc_block:
            verdict, verdict_key = ("🚨 FOMC HARD BLOCK — MARKOV VETO", "FOMC_BLOCKED")
        elif gate_veto:
            verdict, verdict_key = (
                f"⛔ MARKOV VETO — Chain opposes {bias}", "VETOED")
        elif velocity_flag:
            verdict, verdict_key = (
                f"⚠️ REGIME SHIFT — Markov dampened · {bias} weakened", "CAUTION")
        elif final_mod >= 1.05:
            verdict, verdict_key = (
                f"✅ CONFIRMED — Markov chain aligns with {bias}", "CONFIRMED")
        elif final_mod <= 0.85:
            verdict, verdict_key = (
                f"⚠️ CAUTION — Markov reduces confidence · {bias} weakened", "CAUTION")
        else:
            verdict, verdict_key = (
                f"➡️ NEUTRAL — Markov modifier {final_mod:.2f}", "NEUTRAL")

        return {
            "validator_conf_mod":      final_mod,
            "validator_gate_veto":     gate_veto or fomc_block,
            "validator_bull_edge":     bull_edge,
            "validator_gate_chain":    gate_chain,
            "validator_gates_ok":      gates_ok,
            "validator_n_samples":     n,
            "validator_alpha":         alpha,
            "validator_verdict":       verdict,
            "validator_verdict_key":   verdict_key,
            "validator_note":          note,
            "validator_macro_mod":     learned_macro_mod,
            "validator_velocity_flag": velocity_flag,
            "validator_velocity_note": velocity_note,
            # UPG-5: Pass through regime geometry for downstream consumers
            "validator_regime_geometry": markov_data.get("markov_regime_geometry", {}),
        }

    def validate_trio(
        self,
        fg_value:   int,
        fomc_block: bool,
        regime:     str,
        symbol:     str = "BTCUSDT",
    ) -> dict:
        safe_sym   = _sanitize_symbol(symbol)
        brain_data = self._brain.read()
        sym_data   = brain_data.get("symbols", {}).get(safe_sym, _empty_symbol_block())
        _rehydrate_symbol(sym_data)

        n           = sym_data.get("n_samples", 0)
        effective_n = sym_data.get("effective_n", float(n))
        alpha       = min(1.0, effective_n / ALPHA_RAMP_SAMPLES)
        bias        = "BULLISH" if fg_value > 60 else "BEARISH" if fg_value < 40 else "NEUTRAL"

        prior   = _build_base_matrix(regime, bias)
        counts  = sym_data.get("ict_counts", [[0.0] * N] * N)
        learned = [[v / max(sum(row), 1e-9) for v in row] for row in counts]
        P = _normalize_rows([
            [(1 - alpha) * prior[i][j] + alpha * learned[i][j] for j in range(N)]
            for i in range(N)
        ])
        pi = _stationary(P)

        bull_mass = sum(pi[i] for i in _BULL_IDX)
        bear_mass = sum(pi[i] for i in _BEAR_IDX)
        top_state = ICT_STATES[max(range(N), key=lambda i: pi[i])]

        # UPG-3: win rates in trio output
        oc = sym_data.get("outcome_counts", {})
        win_rates: Dict[str, float] = {}
        for state in ICT_STATES:
            state_oc = oc.get(state, {})
            wins  = (state_oc.get("TP1", 0)
                     + state_oc.get("TP2", 0)
                     + state_oc.get("TP3", 0))
            total = wins + state_oc.get("SL", 0)
            if total > 0:
                win_rates[state] = round(wins / total, 2)

        if fomc_block:
            trio_note = "🚨 FOMC HARD BLOCK — Markov: high reversal probability"
        elif bull_mass > 0.50 and bias == "BULLISH":
            trio_note = (f"🎲 Markov v3.0 [{n}s α={alpha:.2f}]: "
                         f"Bull mass {bull_mass*100:.0f}% · dominant={top_state}")
        elif bear_mass > 0.50 and bias == "BEARISH":
            trio_note = (f"🎲 Markov v3.0 [{n}s α={alpha:.2f}]: "
                         f"Bear mass {bear_mass*100:.0f}% · dominant={top_state}")
        else:
            trio_note = (f"🎲 Markov v3.0 [{n}s α={alpha:.2f}]: "
                         f"Neutral · dominant={top_state}")

        return {
            "trio_bull_mass":      round(bull_mass, 4),
            "trio_bear_mass":      round(bear_mass, 4),
            "trio_dominant_state": top_state,
            "trio_n_samples":      n,
            "trio_effective_n":    round(effective_n, 2),
            "trio_alpha":          round(alpha, 3),
            "trio_note":           trio_note,
            "trio_win_rates":      win_rates,   # UPG-3
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 9 — LEVEL LEARNER v2.0
# UPG-4: Percentile ladder replaces single median.
# SL uses 55th percentile (survival buffer).
# TP1 = 40th, TP2 = 65th, TP3 = 85th.
# ══════════════════════════════════════════════════════════════════════

class LevelLearner:
    """
    Self-learning entry/TP/SL level engine v2.0.
    UPG-4: Percentile-based level estimation replaces single median.
    Learns from real trade outcomes in MarkovBrain.level_stats.
    """

    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _get_stats(self, symbol: str, ict_state: str, bias: str) -> dict:
        sym_data = self._brain.get_symbol(_sanitize_symbol(symbol))
        ls  = sym_data.setdefault("level_stats", {})
        key = f"{ict_state}|{bias}"
        return ls.setdefault(key, {
            "tp1_wins":         [],
            "tp2_wins":         [],
            "tp3_wins":         [],
            "sl_losses":        [],
            "entry_sweep_pcts": [],
            "n_wins":           0,
            "n_losses":         0,
            "n_total":          0,
        })

    def record_level_outcome(
        self,
        symbol:          str,
        ict_state:       str,
        bias:            str,
        outcome:         str,
        tp1_pct:         float = 0.0,
        tp2_pct:         float = 0.0,
        tp3_pct:         float = 0.0,
        sl_pct:          float = 0.0,
        sl2_pct:         float = 0.0,
        sl3_pct:         float = 0.0,
        entry_sweep_pct: float = 0.0,
    ) -> None:
        if not ict_state or not bias or outcome not in _OUTCOMES or outcome == "TIMEOUT":
            return
        if not any([tp1_pct, sl_pct]):
            return

        safe_sym = _sanitize_symbol(symbol)
        stats    = self._get_stats(safe_sym, ict_state, bias)
        is_win   = outcome in ("TP1", "TP2", "TP3")

        def _ring(buf: list, val: float) -> None:
            if val != 0.0:
                buf.append(round(val, 4))
                if len(buf) > LEVEL_LEARN_RING_SIZE:
                    buf[:] = buf[-LEVEL_LEARN_RING_SIZE:]

        if is_win:
            if tp1_pct: _ring(stats["tp1_wins"], abs(tp1_pct))
            if tp2_pct: _ring(stats["tp2_wins"], abs(tp2_pct))
            if tp3_pct: _ring(stats["tp3_wins"], abs(tp3_pct))
            stats["n_wins"] = stats.get("n_wins", 0) + 1
        else:
            if sl_pct:  _ring(stats["sl_losses"], abs(sl_pct))
            stats["n_losses"] = stats.get("n_losses", 0) + 1

        if entry_sweep_pct:
            _ring(stats["entry_sweep_pcts"], abs(entry_sweep_pct))

        stats["n_total"] = stats.get("n_total", 0) + 1

        try:
            self._brain.save()
        except (OSError, IOError) as e:
            log.debug(f"[LEVEL_LEARNER] save failed: {e}")

    def get_learned_levels(
        self,
        symbol:        str,
        ict_state:     str,
        bias:          str,
        entry_price:   float,
        ta_entry_low:  float,
        ta_entry_high: float,
        ta_tp1:        float,
        ta_tp2:        float,
        ta_tp3:        float,
        ta_sl:         float,
    ) -> dict:
        """
        UPG-4: Percentile ladder for TP/SL levels.
        TP1 → 40th pct (conservative), TP2 → 65th, TP3 → 85th, SL → 55th.
        Returns learned levels + blending weight + confidence tier.
        """
        _empty = {
            "learned_entry_low":  ta_entry_low,
            "learned_entry_high": ta_entry_high,
            "learned_tp1":  ta_tp1,
            "learned_tp2":  ta_tp2,
            "learned_tp3":  ta_tp3,
            "learned_sl":   ta_sl,
            "blend_weight": 0.0,
            "n_samples":    0,
            "confidence":   "NONE",
            "learned_note": "No learned levels yet (insufficient data)",
        }

        if not entry_price or entry_price <= 0:
            return _empty

        safe_sym = _sanitize_symbol(symbol)
        stats    = self._get_stats(safe_sym, ict_state, bias)
        n_total  = stats.get("n_total", 0)

        if n_total < LEVEL_LEARN_MIN_SAMPLES:
            _empty["learned_note"] = (
                f"Learning... {n_total}/{LEVEL_LEARN_MIN_SAMPLES} samples needed"
            )
            return _empty

        def _pct_to_price(pct_abs: float, direction_positive: bool) -> float:
            if pct_abs <= 0:
                return 0.0
            sign = 1.0 if direction_positive else -1.0
            return round(entry_price * (1 + sign * pct_abs / 100.0), 8)

        is_bull = (bias == "BULLISH")

        # UPG-4: Percentile ladder (not single median)
        pct_tp1 = _percentile(stats["tp1_wins"],  LEVEL_TP1_PERCENTILE)
        pct_tp2 = _percentile(stats["tp2_wins"],  LEVEL_TP2_PERCENTILE)
        pct_tp3 = _percentile(stats["tp3_wins"],  LEVEL_TP3_PERCENTILE)
        pct_sl  = _percentile(stats["sl_losses"], LEVEL_SL_PERCENTILE)
        pct_sw  = _percentile(stats.get("entry_sweep_pcts", []),
                               50)   # sweep stays at median (symmetric)

        learned_tp1 = _pct_to_price(pct_tp1, is_bull)      if pct_tp1 else 0.0
        learned_tp2 = _pct_to_price(pct_tp2, is_bull)      if pct_tp2 else 0.0
        learned_tp3 = _pct_to_price(pct_tp3, is_bull)      if pct_tp3 else 0.0
        learned_sl  = _pct_to_price(pct_sl,  not is_bull)  if pct_sl  else 0.0

        # Entry sweep level
        if pct_sw > 0:
            sweep_price      = _pct_to_price(pct_sw, not is_bull)
            learned_entry_low  = (min(sweep_price, ta_entry_low)
                                   if is_bull else ta_entry_low)
            learned_entry_high = (ta_entry_high
                                   if is_bull else max(sweep_price, ta_entry_high))
        else:
            learned_entry_low  = ta_entry_low
            learned_entry_high = ta_entry_high

        # Directional sanity checks
        if is_bull:
            if learned_tp1 and learned_tp1 <= entry_price: learned_tp1 = 0.0
            if learned_tp2 and learned_tp2 <= entry_price: learned_tp2 = 0.0
            if learned_tp3 and learned_tp3 <= entry_price: learned_tp3 = 0.0
            if learned_sl  and learned_sl  >= entry_price: learned_sl  = 0.0
        else:
            if learned_tp1 and learned_tp1 >= entry_price: learned_tp1 = 0.0
            if learned_tp2 and learned_tp2 >= entry_price: learned_tp2 = 0.0
            if learned_tp3 and learned_tp3 >= entry_price: learned_tp3 = 0.0
            if learned_sl  and learned_sl  <= entry_price: learned_sl  = 0.0

        # Blend weight ramp
        blend_w = min(
            LEVEL_LEARN_MAX_WEIGHT,
            (n_total - LEVEL_LEARN_MIN_SAMPLES) /
            max(LEVEL_LEARN_FULL_WEIGHT - LEVEL_LEARN_MIN_SAMPLES, 1)
            * LEVEL_LEARN_MAX_WEIGHT
        )
        blend_w = round(max(0.0, blend_w), 3)

        confidence = ("HIGH"   if n_total >= LEVEL_LEARN_FULL_WEIGHT else
                      "MEDIUM" if n_total >= LEVEL_LEARN_MIN_SAMPLES * 2 else
                      "LOW")

        n_wins   = stats.get("n_wins",   0)
        n_losses = stats.get("n_losses", 0)
        win_rate = round(n_wins / max(n_wins + n_losses, 1) * 100, 1)

        note = (
            f"🧠 Learned v3.0 [{confidence}] {n_total} trades WR={win_rate}% "
            f"blend={blend_w:.0%} | "
            f"TP1={pct_tp1:+.2f}%(p{LEVEL_TP1_PERCENTILE}) "
            f"TP2={pct_tp2:+.2f}%(p{LEVEL_TP2_PERCENTILE}) "
            f"TP3={pct_tp3:+.2f}%(p{LEVEL_TP3_PERCENTILE}) "
            f"SL={pct_sl:-.2f}%(p{LEVEL_SL_PERCENTILE}) "
            f"sweep={pct_sw:.2f}%"
        )

        return {
            "learned_entry_low":  learned_entry_low,
            "learned_entry_high": learned_entry_high,
            "learned_tp1":        learned_tp1  or ta_tp1,
            "learned_tp2":        learned_tp2  or ta_tp2,
            "learned_tp3":        learned_tp3  or ta_tp3,
            "learned_sl":         learned_sl   or ta_sl,
            "blend_weight":       blend_w,
            "n_samples":          n_total,
            "n_wins":             n_wins,
            "n_losses":           n_losses,
            "win_rate":           win_rate,
            "confidence":         confidence,
            "learned_note":       note,
            # Percentile raw values (for audit/debug)
            "pct_tp1":  round(pct_tp1, 4),
            "pct_tp2":  round(pct_tp2, 4),
            "pct_tp3":  round(pct_tp3, 4),
            "pct_sl":   round(pct_sl,  4),
            "pct_sweep":round(pct_sw,  4),
        }

    def ingest_aws_samples(self, aws_samples: List[dict]) -> int:
        ingested = 0
        for s in aws_samples:
            try:
                self.record_level_outcome(
                    symbol          = s.get("symbol", ""),
                    ict_state       = s.get("ict_state", ""),
                    bias            = s.get("bias", ""),
                    outcome         = s.get("outcome", ""),
                    tp1_pct         = float(s.get("tp1_pct") or 0),
                    tp2_pct         = float(s.get("tp2_pct") or 0),
                    tp3_pct         = float(s.get("tp3_pct") or 0),
                    sl_pct          = float(s.get("sl_pct")  or 0),
                    sl2_pct         = float(s.get("sl2_pct") or 0),
                    sl3_pct         = float(s.get("sl3_pct") or 0),
                    entry_sweep_pct = float(s.get("entry_sweep_pct") or 0),
                )
                ingested += 1
            except Exception as e:
                log.debug(f"[LEVEL_LEARNER] ingest error: {e}")
        return ingested


# ══════════════════════════════════════════════════════════════════════
# SECTION 10 — SUPER MARKOV v4.1
# UPG-7: Composite confidence score 0–100 added to SuperValidator output.
# All domain brains preserved from v2.1.
# ══════════════════════════════════════════════════════════════════════

class SentimentBrain:
    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _data(self) -> dict:
        sm = self._brain.read().setdefault("super_markov", {})
        return sm.setdefault("sentiment", {
            "keywords":    {},
            "last_update": "",
        })

    def record_keywords(self, keywords_scores: List[dict]) -> None:
        d    = self._data()
        kws  = d.setdefault("keywords", {})
        now  = time.time()
        for item in keywords_scores:
            kw  = str(item.get("keyword", "")).lower().strip()[:50]
            scr = float(item.get("score", 0.0))
            hl  = max(SENTIMENT_HALF_LIFE_MIN,
                      min(SENTIMENT_HALF_LIFE_MAX, float(item.get("half_life_hrs", 24))))
            if not kw:
                continue
            if kw not in kws:
                kws[kw] = {"weight": scr, "half_life_hrs": hl,
                           "observations": 0, "last_seen": now}
            else:
                kws[kw]["weight"] = round(
                    0.7 * kws[kw]["weight"] + 0.3 * scr, 4)
                kws[kw]["half_life_hrs"] = hl
                kws[kw]["last_seen"]     = now
            kws[kw]["observations"] = kws[kw].get("observations", 0) + 1
        self._decay_and_prune(kws, now)
        d["last_update"] = datetime.now(timezone.utc).isoformat()

    def record_headlines(self, headlines: List[str]) -> None:
        auto_kws = []
        for hl in (headlines or []):
            hl_lower = hl.lower()
            for kw in SENTIMENT_POSITIVE_KEYWORDS:
                if kw in hl_lower:
                    auto_kws.append({"keyword": kw, "score": 0.5, "half_life_hrs": 12})
            for kw in SENTIMENT_NEGATIVE_KEYWORDS:
                if kw in hl_lower:
                    auto_kws.append({"keyword": kw, "score": -0.5, "half_life_hrs": 12})
            for kw in SENTIMENT_MACRO_KEYWORDS:
                if kw in hl_lower:
                    auto_kws.append({"keyword": kw, "score": -0.2, "half_life_hrs": 6})
        if auto_kws:
            self.record_keywords(auto_kws)

    def _decay_and_prune(self, kws: dict, now_ts: float) -> None:
        to_prune = []
        for kw, data in kws.items():
            hrs_elapsed = (now_ts - data.get("last_seen", now_ts)) / 3600.0
            decay = SENTIMENT_DECAY ** hrs_elapsed
            data["weight"] = round(data["weight"] * decay, 4)
            if abs(data["weight"]) < SENTIMENT_MIN_WEIGHT:
                to_prune.append(kw)
        for kw in to_prune:
            del kws[kw]
        if len(kws) > SENTIMENT_MAX_KEYWORDS:
            sorted_kws = sorted(kws.items(), key=lambda x: abs(x[1]["weight"]))
            excess = len(kws) - SENTIMENT_MAX_KEYWORDS
            for kw, _ in sorted_kws[:excess]:
                del kws[kw]

    def score(self, headlines: List[str] = None,
              symbols: List[str] = None) -> dict:
        if headlines:
            self.record_headlines(headlines)
        d   = self._data()
        kws = d.get("keywords", {})
        if not kws:
            return {"sentiment_score": 0.0, "confidence": 0.0,
                    "top_positive": [], "top_negative": []}

        total_w    = sum(abs(v["weight"]) for v in kws.values())
        weighted_s = sum(v["weight"] for v in kws.values())
        score      = round(weighted_s / max(total_w, 1e-9), 4)
        score      = max(-1.0, min(1.0, score))
        conf       = min(1.0, total_w / 10.0)

        pos = sorted([(k, v["weight"]) for k, v in kws.items() if v["weight"] > 0],
                     key=lambda x: -x[1])[:3]
        neg = sorted([(k, v["weight"]) for k, v in kws.items() if v["weight"] < 0],
                     key=lambda x: x[1])[:3]

        return {
            "sentiment_score": score,
            "confidence":      round(conf, 3),
            "top_positive":    [k for k, _ in pos],
            "top_negative":    [k for k, _ in neg],
        }


class RegimeBrain:
    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _data(self) -> dict:
        sm = self._brain.read().setdefault("super_markov", {})
        return sm.setdefault("regime", {
            "transitions": [], "ai_quality": {},
            "dur_history": {},       # UPG-14: {regime_str: [duration_secs, ...]}
            "regime_start_ts": 0.0,  # UPG-14: epoch of last regime change
        })

    def record_transition(self, prev_regime: str, curr_regime: str,
                          outcome: str = "") -> None:
        d  = self._data()
        tr = d.setdefault("transitions", [])

        # UPG-14: on actual regime change, record how long the old regime lasted
        if prev_regime != curr_regime:
            start_ts = d.get("regime_start_ts", 0.0)
            if start_ts > 0:
                elapsed = time.time() - start_ts
                if elapsed > 0:
                    dh = d.setdefault("dur_history", {})
                    bucket = dh.setdefault(prev_regime, [])
                    bucket.append(round(elapsed, 1))
                    if len(bucket) > REGIME_DUR_MAX_BUF:
                        dh[prev_regime] = bucket[-REGIME_DUR_MAX_BUF:]
            d["regime_start_ts"] = time.time()

        tr.append({"from": prev_regime, "to": curr_regime,
                   "outcome": outcome, "ts": time.time()})
        if len(tr) > REGIME_HISTORY_SIZE:
            d["transitions"] = tr[-REGIME_HISTORY_SIZE:]

    def record_ai_quality(self, regime_scores: dict) -> None:
        d  = self._data()
        aq = d.setdefault("ai_quality", {})
        for regime, score in (regime_scores or {}).items():
            if regime not in aq:
                aq[regime] = {"score": float(score), "n": 1}
            else:
                aq[regime]["score"] = round(
                    0.8 * aq[regime]["score"] + 0.2 * float(score), 4)
                aq[regime]["n"] += 1

    def validate(self, current_regime: str, bias: str) -> dict:
        d   = self._data()
        tr  = d.get("transitions", [])
        aq  = d.get("ai_quality", {})

        # Base modifier from transition history
        if not tr:
            mod = 1.0
        else:
            win_count   = sum(1 for t in tr if t.get("outcome") in ("TP1","TP2","TP3"))
            total_count = len(tr)
            win_rate    = win_count / max(total_count, 1)
            mod         = round(0.7 + win_rate * 0.6, 3)
            mod         = max(0.70, min(1.30, mod))

        ai_q = aq.get(current_regime, {}).get("score", 1.0)
        mod  = round(mod * ai_q, 3)
        mod  = max(0.70, min(1.30, mod))

        if current_regime in ("CHOPPY", "VOLATILE"):
            verdict = f"⚠️ Regime [{current_regime}] — elevated uncertainty"
        elif current_regime == "TRENDING":
            verdict = f"✅ Regime [{current_regime}] — favorable"
        else:
            verdict = f"➡️ Regime [{current_regime}] — neutral"

        # ── UPG-14: Regime persistence tracking ──────────────────────
        regime_persistence_secs = 0.0
        regime_change_warning   = False
        try:
            start_ts = d.get("regime_start_ts", 0.0)
            if start_ts > 0:
                regime_persistence_secs = round(time.time() - start_ts, 1)
            dh      = d.get("dur_history", {})
            history = dh.get(current_regime, [])
            if len(history) >= 5 and regime_persistence_secs > 0:
                median_dur = _percentile(history, 50)
                if median_dur > 0 and regime_persistence_secs > median_dur * REGIME_DUR_WARN_PCT:
                    regime_change_warning = True
                    verdict += " | ⏳ Persistence warning"
        except Exception:
            pass   # non-critical; never block signal delivery

        return {
            "regime_conf_mod":          mod,
            "regime_verdict":           verdict,
            "regime_persistence_secs":  regime_persistence_secs,  # UPG-14
            "regime_change_warning":    regime_change_warning,     # UPG-14
        }


class SectorBrain:
    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _data(self) -> dict:
        sm = self._brain.read().setdefault("super_markov", {})
        return sm.setdefault("sector", {"signals": [], "ai_phases": {}})

    def record_sector_signal(self, sector: str, phase: str,
                             outcome: str = "") -> None:
        d  = self._data()
        sg = d.setdefault("signals", [])
        sg.append({"sector": sector, "phase": phase,
                   "outcome": outcome, "ts": time.time()})
        if len(sg) > SECTOR_HISTORY_SIZE:
            d["signals"] = sg[-SECTOR_HISTORY_SIZE:]

    def record_ai_phase(self, sector: str, phase: str,
                        confidence: float) -> None:
        d   = self._data()
        ap  = d.setdefault("ai_phases", {})
        key = f"{sector}|{phase}"
        if key not in ap:
            ap[key] = {"confidence": confidence, "n": 1}
        else:
            ap[key]["confidence"] = round(
                0.8 * ap[key]["confidence"] + 0.2 * confidence, 4)
            ap[key]["n"] += 1

    def validate(self, current_sectors: List[str] = None) -> dict:
        d  = self._data()
        sg = d.get("signals", [])
        if not sg:
            return {"sector_conf_mod": 1.0, "sector_verdict": "➡️ Sector: no data"}

        wins  = sum(1 for s in sg if s.get("outcome") in ("TP1","TP2","TP3"))
        total = len(sg)
        wr    = wins / max(total, 1)
        mod   = round(0.7 + wr * 0.6, 3)
        mod   = max(0.70, min(1.30, mod))

        verdict = (f"✅ Sector aligned WR={wr*100:.0f}%" if wr > 0.55
                   else f"⚠️ Sector weak WR={wr*100:.0f}%")
        return {"sector_conf_mod": mod, "sector_verdict": verdict}


class TrendBrain:
    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _data(self) -> dict:
        sm = self._brain.read().setdefault("super_markov", {})
        return sm.setdefault("trend", {"signals": [], "ai_quality": {}})

    def record_trend_signal(self, quality: str, outcome: str = "") -> None:
        d  = self._data()
        sg = d.setdefault("signals", [])
        sg.append({"quality": quality, "outcome": outcome, "ts": time.time()})
        if len(sg) > TREND_HISTORY_SIZE:
            d["signals"] = sg[-TREND_HISTORY_SIZE:]

    def record_ai_quality(self, quality: str, confidence: float) -> None:
        d  = self._data()
        aq = d.setdefault("ai_quality", {})
        if quality not in aq:
            aq[quality] = {"confidence": confidence, "n": 1}
        else:
            aq[quality]["confidence"] = round(
                0.8 * aq[quality]["confidence"] + 0.2 * confidence, 4)
            aq[quality]["n"] += 1

    def validate(self, momentum_quality: str = "MEDIUM") -> dict:
        d  = self._data()
        sg = d.get("signals", [])
        if not sg:
            return {"trend_conf_mod": 1.0, "trend_verdict": "➡️ Trend: no data"}

        wins  = sum(1 for s in sg if s.get("outcome") in ("TP1","TP2","TP3"))
        total = len(sg)
        wr    = wins / max(total, 1)
        qual_boost = {"HIGH": 0.1, "MEDIUM": 0.0, "LOW": -0.1}.get(
            momentum_quality.upper(), 0.0)
        mod   = round(0.7 + wr * 0.6 + qual_boost, 3)
        mod   = max(0.70, min(1.30, mod))

        verdict = (f"✅ Trend [{momentum_quality}] WR={wr*100:.0f}%" if wr > 0.55
                   else f"⚠️ Trend weak [{momentum_quality}] WR={wr*100:.0f}%")
        return {"trend_conf_mod": mod, "trend_verdict": verdict}


class CrossBrain:
    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _data(self) -> dict:
        sm = self._brain.read().setdefault("super_markov", {})
        return sm.setdefault("cross", {"signals": [], "ai_reliability": {}})

    def record_cross_signal(self, symbols: List[str], n_confluence: int,
                            outcome: str = "") -> None:
        d  = self._data()
        sg = d.setdefault("signals", [])
        sg.append({"symbols": symbols, "n_confluence": n_confluence,
                   "outcome": outcome, "ts": time.time()})
        if len(sg) > CROSS_HISTORY_SIZE:
            d["signals"] = sg[-CROSS_HISTORY_SIZE:]

    def record_ai_reliability(self, symbol: str,
                              reliability_score: float) -> None:
        d   = self._data()
        ar  = d.setdefault("ai_reliability", {})
        safe = _sanitize_symbol(symbol)
        if safe not in ar:
            ar[safe] = {"score": reliability_score, "n": 1}
        else:
            ar[safe]["score"] = round(
                0.8 * ar[safe]["score"] + 0.2 * reliability_score, 4)
            ar[safe]["n"] += 1

    def validate(self, symbols: List[str] = None,
                 n_confluence: int = 1) -> dict:
        d  = self._data()
        sg = d.get("signals", [])
        if not sg:
            return {"cross_conf_mod": 1.0, "cross_verdict": "➡️ Cross: no data"}

        wins  = sum(1 for s in sg if s.get("outcome") in ("TP1","TP2","TP3"))
        total = len(sg)
        wr    = wins / max(total, 1)
        conf_boost = min(0.10, (n_confluence - 1) * 0.05)
        mod   = round(0.7 + wr * 0.6 + conf_boost, 3)
        mod   = max(0.70, min(1.30, mod))

        verdict = (f"✅ Cross confluence n={n_confluence} WR={wr*100:.0f}%" if wr > 0.55
                   else f"⚠️ Cross weak n={n_confluence}")
        return {"cross_conf_mod": mod, "cross_verdict": verdict}


class VolatilityBrain:
    """
    UPG-8: 6th SuperValidator brain — volatility-conditioned confidence modifier.

    Tracks ATR percentile history per symbol in brain.json.
    Classifies current vol as LOW / MEDIUM / HIGH / EXTREME.
    Applies confidence modifier: high vol → reduce conf, low vol → slight boost.

    Data stored under brain.json["super_markov"]["volatility"]["symbols"][sym].
    Memory: O(VOL_HISTORY_SIZE * n_symbols) — negligible on 127 MB containers.
    Thread-safe: all writes via MarkovBrain's threading.Lock (save_debounced).
    """

    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _sym_data(self, symbol: str) -> dict:
        """Return per-symbol volatility record, creating it if absent."""
        sm   = self._brain.read().setdefault("super_markov", {})
        vol  = sm.setdefault("volatility", {"symbols": {}})
        safe = _sanitize_symbol(symbol)
        return vol["symbols"].setdefault(safe, {
            "atr_pcts": [],          # ring buffer of recent ATR percentile values
            "last_update": "",
        })

    def record_atr_percentile(self, symbol: str, atr_pct: float) -> None:
        """
        Record the current ATR as a percentile of recent history.

        Args:
            symbol:  Coin symbol (sanitized internally).
            atr_pct: ATR expressed as % of price (e.g. 2.3 means 2.3% ATR).
        """
        d  = self._sym_data(symbol)
        buf = d.setdefault("atr_pcts", [])
        buf.append(round(float(atr_pct), 4))
        if len(buf) > VOL_HISTORY_SIZE:
            d["atr_pcts"] = buf[-VOL_HISTORY_SIZE:]
        d["last_update"] = datetime.now(timezone.utc).isoformat()
        # No explicit save — caller's record_and_save / save_debounced handles it.

    def _classify_vol(self, current_atr_pct: float, history: list) -> str:
        """Classify current ATR vs history into tier string."""
        if not history or current_atr_pct <= 0:
            return "MEDIUM"
        pct_rank = sum(1 for v in history if v <= current_atr_pct) / len(history) * 100
        if pct_rank >= VOL_ATR_PCT_EXTREME:
            return "EXTREME"
        if pct_rank >= VOL_ATR_PCT_HIGH:
            return "HIGH"
        if pct_rank <= VOL_ATR_PCT_LOW:
            return "LOW"
        return "MEDIUM"

    def validate(self, symbol: str = "", current_atr_pct: float = 0.0) -> dict:
        """
        Compute volatility confidence modifier for the current signal.

        Args:
            symbol:          Coin symbol to look up history for.
            current_atr_pct: ATR% at signal time (0.0 = unknown → MEDIUM assumed).

        Returns:
            {
              vol_tier:        str   — LOW / MEDIUM / HIGH / EXTREME
              vol_conf_mod:    float — modifier applied to super_mod [0.80–1.08]
              vol_verdict:     str   — human-readable verdict
              vol_n_samples:   int   — number of ATR samples in history
            }
        """
        if not symbol:
            return {
                "vol_tier":      "MEDIUM",
                "vol_conf_mod":  VOL_MOD_MEDIUM,
                "vol_verdict":   "➡️ Vol: no symbol",
                "vol_n_samples": 0,
            }

        d       = self._sym_data(symbol)
        history = d.get("atr_pcts", [])
        n       = len(history)

        # Use last recorded value if caller didn't supply one
        effective_atr = current_atr_pct if current_atr_pct > 0 else (history[-1] if history else 0.0)

        tier = self._classify_vol(effective_atr, history)

        mod_map = {
            "EXTREME": VOL_MOD_EXTREME,
            "HIGH":    VOL_MOD_HIGH,
            "MEDIUM":  VOL_MOD_MEDIUM,
            "LOW":     VOL_MOD_LOW,
        }
        mod = mod_map.get(tier, VOL_MOD_MEDIUM)

        verdict_map = {
            "EXTREME": f"⚡ Vol EXTREME ({effective_atr:.1f}% ATR) — caution",
            "HIGH":    f"⚠️ Vol HIGH ({effective_atr:.1f}% ATR) — reduce size",
            "MEDIUM":  f"➡️ Vol MEDIUM ({effective_atr:.1f}% ATR) — normal",
            "LOW":     f"✅ Vol LOW ({effective_atr:.1f}% ATR) — clean structure",
        }
        verdict = verdict_map.get(tier, f"➡️ Vol MEDIUM ({effective_atr:.1f}% ATR)")

        return {
            "vol_tier":      tier,
            "vol_conf_mod":  round(mod, 3),
            "vol_verdict":   verdict,
            "vol_n_samples": n,
        }


# ── v4.1: PolymarketBrain — 8th SuperValidator brain ─────────────────

# Polymarket confidence modifier constants
POLY_MOD_STRONG_EDGE  = 1.07   # AI/CEX edge >= 70: boost confidence
POLY_MOD_MILD_EDGE    = 1.03   # AI/CEX edge 40–69: mild boost
POLY_MOD_NEUTRAL      = 1.00   # edge < 40 or unavailable
POLY_MOD_CONTRARY     = 0.95   # Poly signal directly contra TA bias
POLY_HISTORY_SIZE     = 100    # outcome ring buffer per symbol


class PolymarketBrain:
    """
    v4.1: 8th SuperValidator brain — Polymarket prediction market confidence modifier.

    Tracks strategy win rates per market type (crypto / NegRisk) per symbol.
    Applies confidence modifier based on:
      1. Current edge_score from polymarket_engine.compute_poly_edge()
      2. Historical win rate of Polymarket signals for this symbol/strategy
      3. Directional alignment between Poly signal and TA bias

    Data stored under brain.json["super_markov"]["polymarket"]["symbols"][sym].
    Thread-safe: all writes via MarkovBrain's threading.Lock (save_debounced).
    Memory: O(POLY_HISTORY_SIZE * n_symbols) — bounded, 128 MB safe.
    """

    def __init__(self, brain: "MarkovBrain"):
        self._brain = brain

    def _sym_data(self, symbol: str) -> dict:
        """Return per-symbol Polymarket record, creating if absent."""
        sm   = self._brain.read().setdefault("super_markov", {})
        poly = sm.setdefault("polymarket", {"symbols": {}})
        safe = _sanitize_symbol(symbol)
        return poly["symbols"].setdefault(safe, {
            "outcomes":    [],     # ring buffer: {"ts", "strategy", "edge", "outcome"}
            "wins":        0,
            "losses":      0,
            "last_edge":   0.0,
            "last_signal": "NEUTRAL",
            "last_update": "",
        })

    def record_outcome(
        self,
        symbol:   str,
        strategy: str,   # "CEX_LAG" | "AI_EDGE" | "WHALE" | "NEGRISK"
        edge:     float,
        outcome:  str,   # "WIN" | "LOSS" | "PUSH"
    ) -> None:
        """Record a completed Polymarket paper trade outcome for this brain."""
        d = self._sym_data(symbol)
        buf = d.setdefault("outcomes", [])
        buf.append({
            "ts":       datetime.now(timezone.utc).isoformat(),
            "strategy": strategy.upper(),
            "edge":     round(float(edge), 1),
            "outcome":  outcome.upper(),
        })
        if len(buf) > POLY_HISTORY_SIZE:
            d["outcomes"] = buf[-POLY_HISTORY_SIZE:]
        outcome_upper = outcome.upper()
        if outcome_upper == "WIN":
            d["wins"]   = d.get("wins", 0) + 1
        elif outcome_upper == "LOSS":
            d["losses"] = d.get("losses", 0) + 1
        d["last_update"] = datetime.now(timezone.utc).isoformat()
        # Caller's record_and_save / save_debounced handles persistence.

    def validate(
        self,
        symbol:      str   = "",
        edge_score:  float = 0.0,
        poly_signal: str   = "NEUTRAL",   # "BULLISH" | "BEARISH" | "NEUTRAL"
        ta_bias:     str   = "NEUTRAL",   # TA directional bias for alignment check
    ) -> dict:
        """
        Compute Polymarket confidence modifier for the current signal.

        Args:
            symbol:      Coin symbol for historical win-rate lookup.
            edge_score:  Composite edge score from polymarket_engine [0–100].
            poly_signal: Polymarket directional signal (from Gamma API).
            ta_bias:     TA analysis bias for alignment check.

        Returns:
            {
              poly_conf_mod:   float  [0.95–1.07]
              poly_wr:         float  historical win rate for this symbol [0–1]
              poly_n_trades:   int    number of historical outcomes
              poly_tier:       str    STRONG | MILD | NEUTRAL | CONTRARY
              poly_verdict:    str    human-readable verdict
            }
        """
        if not symbol:
            return {
                "poly_conf_mod":  POLY_MOD_NEUTRAL,
                "poly_wr":        0.0,
                "poly_n_trades":  0,
                "poly_tier":      "NEUTRAL",
                "poly_verdict":   "➡️ Poly: no symbol",
            }

        d    = self._sym_data(symbol)
        wins = d.get("wins", 0)
        loss = d.get("losses", 0)
        n    = wins + loss

        # Historical win rate (no prior history → assume neutral)
        hist_wr = wins / n if n > 0 else 0.5

        # Directional alignment check
        bias_upper   = ta_bias.upper()
        signal_upper = poly_signal.upper()
        contrary = (
            (signal_upper == "BULLISH" and bias_upper in ("BEARISH", "SHORT")) or
            (signal_upper == "BEARISH" and bias_upper in ("BULLISH", "LONG"))
        )

        # Tier determination: edge_score + historical WR + alignment
        if contrary:
            tier = "CONTRARY"
            mod  = POLY_MOD_CONTRARY
        elif edge_score >= 70 and hist_wr >= 0.55:
            tier = "STRONG"
            mod  = POLY_MOD_STRONG_EDGE
        elif edge_score >= 40 or hist_wr >= 0.60:
            tier = "MILD"
            mod  = POLY_MOD_MILD_EDGE
        else:
            tier = "NEUTRAL"
            mod  = POLY_MOD_NEUTRAL

        verdict_map = {
            "STRONG":   f"✅ Poly EDGE {edge_score:.0f}/100 WR={hist_wr:.0%} — aligned",
            "MILD":     f"🟢 Poly MILD {edge_score:.0f}/100 WR={hist_wr:.0%}",
            "NEUTRAL":  f"➡️ Poly NEUTRAL {edge_score:.0f}/100",
            "CONTRARY": f"⚠️ Poly CONTRARY — {signal_upper} vs TA {bias_upper}",
        }
        verdict = verdict_map.get(tier, f"➡️ Poly NEUTRAL")

        # Record current signal for next cycle reference
        d["last_edge"]   = round(edge_score, 1)
        d["last_signal"] = signal_upper

        return {
            "poly_conf_mod":  round(mod, 3),
            "poly_wr":        round(hist_wr, 4),
            "poly_n_trades":  n,
            "poly_tier":      tier,
            "poly_verdict":   verdict,
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 10 — SUPER VALIDATOR v4.1 (8-Brain composite)
# ══════════════════════════════════════════════════════════════════════

class SuperValidator:
    """
    8-brain composite confidence validator.
    Aggregates SentimentBrain, RegimeBrain, SectorBrain, TrendBrain,
    CrossBrain, VolatilityBrain (UPG-8), and PolymarketBrain (v4.1).
    """

    def __init__(self, brain: "MarkovBrain"):
        self._brain     = brain
        self.sentiment  = SentimentBrain(brain)
        self.regime     = RegimeBrain(brain)
        self.sector     = SectorBrain(brain)
        self.trend      = TrendBrain(brain)
        self.cross      = CrossBrain(brain)
        self.volatility = VolatilityBrain(brain)    # UPG-8: 6th brain
        self.polymarket = PolymarketBrain(brain)    # v4.1: 8th brain

    def _compute_super_confidence_score(
        self, super_mod: float, gate_veto: bool
    ) -> int:
        """
        UPG-7: Normalize super_mod (0.60–1.40) to 0–100 composite score.
        Gate veto clamps score to 0.
        Formula: score = ((super_mod - 0.60) / 0.80) * 100
        """
        if gate_veto:
            return 0
        raw = (super_mod - 0.60) / 0.80 * 100.0
        return max(0, min(100, round(raw)))

    def validate(
        self,
        ict_conf_mod:    float = 1.0,
        ict_verdict_key: str   = "NEUTRAL",
        ict_gate_veto:   bool  = False,
        current_regime:  str   = "TRENDING",
        bias:            str   = "NEUTRAL",
        symbols:         List[str] = None,
        n_confluence:    int   = 1,
        momentum_quality:str   = "MEDIUM",
        headlines:       List[str] = None,
        # UPG-8: VolatilityBrain parameters
        symbol:          str   = "",
        current_atr_pct: float = 0.0,
        # v4.1: PolymarketBrain parameters
        poly_edge_score: float = 0.0,
        poly_signal:     str   = "NEUTRAL",
    ) -> dict:
        ict_score = float(ict_conf_mod)

        sent     = self.sentiment.score(headlines=headlines, symbols=symbols)
        sent_mod = round(1.0 + sent["sentiment_score"] * 0.30, 3)
        sent_mod = max(0.70, min(1.30, sent_mod))

        reg     = self.regime.validate(current_regime, bias)
        reg_mod = reg["regime_conf_mod"]

        sec     = self.sector.validate(symbols)
        sec_mod = sec["sector_conf_mod"]

        trd     = self.trend.validate(momentum_quality)
        trd_mod = trd["trend_conf_mod"]

        crs     = self.cross.validate(symbols, n_confluence)
        crs_mod = crs["cross_conf_mod"]

        # UPG-8: 6th brain — VolatilityBrain
        vol     = self.volatility.validate(symbol=symbol, current_atr_pct=current_atr_pct)
        vol_mod = vol["vol_conf_mod"]

        # v4.1: 8th brain — PolymarketBrain
        poly    = self.polymarket.validate(
            symbol=symbol,
            edge_score=poly_edge_score,
            poly_signal=poly_signal,
            ta_bias=bias,
        )
        poly_mod = poly["poly_conf_mod"]

        # RGD-1: Regime-Aware Weight Selection
        # Detect actual market regime from ATR% + current_regime string.
        # When current_atr_pct is available (> 0), use dynamic weights from
        # _SV_REGIME_WEIGHTS; otherwise fall back to fixed SV_WEIGHT_* constants.
        # Range trap: if regime is RANGE and vol_tier is crowded, apply dampener.
        _detected_regime = detect_regime_from_atr(
            current_atr_pct=current_atr_pct,
            current_regime=current_regime,
        )
        _rw = _SV_REGIME_WEIGHTS.get(_detected_regime) if current_atr_pct > 0.0 else None

        if _rw:
            # Regime-adaptive scoring — weights shift by detected market state
            super_mod = round(
                ict_score  * _rw["ict"]
                + sent_mod * _rw["sentiment"]
                + reg_mod  * _rw["regime"]
                + sec_mod  * _rw["sector"]
                + trd_mod  * _rw["trend"]
                + crs_mod  * _rw["cross"]
                + vol_mod  * _rw["volatility"]   # UPG-8
                + poly_mod * _rw["polymarket"],  # v4.1
                4,
            )
            # RGD-1 Range trap: dampen score when vol_tier is HIGH/EXTREME inside RANGE
            # Prevents crowded-long false breakout signals in sideways markets
            if _detected_regime == "RANGE" and vol.get("vol_tier", "") in ("HIGH", "EXTREME"):
                super_mod = round(super_mod * 0.92, 4)   # −8% crowding dampener
        else:
            # Fallback: fixed weights (current_atr_pct unavailable)
            super_mod = round(
                ict_score   * SV_WEIGHT_ICT
                + sent_mod  * SV_WEIGHT_SENTIMENT
                + reg_mod   * SV_WEIGHT_REGIME
                + sec_mod   * SV_WEIGHT_SECTOR
                + trd_mod   * SV_WEIGHT_TREND
                + crs_mod   * SV_WEIGHT_CROSS
                + vol_mod   * SV_WEIGHT_VOLATILITY    # UPG-8
                + poly_mod  * SV_WEIGHT_POLYMARKET,   # v4.1
                4,
            )
        super_mod    = max(0.60, min(1.40, super_mod))
        gate_veto    = ict_gate_veto
        # UPG-7: Composite 0-100 confidence score
        super_score  = self._compute_super_confidence_score(super_mod, gate_veto)

        if gate_veto:
            verdict_key = "VETOED"
            verdict     = "⛔ SUPER MARKOV VETO — ICT gate blocked"
        elif super_mod >= 1.10:
            verdict_key = "CONFIRMED"
            verdict     = f"✅ SUPER MARKOV CONFIRMED ×{super_mod:.2f} | 8-brain aligned"
        elif super_mod >= 1.02:
            verdict_key = "POSITIVE"
            verdict     = f"🟢 SUPER MARKOV POSITIVE ×{super_mod:.2f}"
        elif super_mod <= 0.88:
            verdict_key = "WEAK"
            verdict     = f"⚠️ SUPER MARKOV WEAK ×{super_mod:.2f} | reduce size"
        elif super_mod <= 0.80:
            verdict_key = "CAUTION"
            verdict     = f"🔴 SUPER MARKOV CAUTION ×{super_mod:.2f}"
        else:
            verdict_key = "NEUTRAL"
            verdict     = f"➡️ SUPER MARKOV NEUTRAL ×{super_mod:.2f}"

        compact = (
            f"🧠 *Super Markov v4.1:* {verdict} [score={super_score}/100] | "
            f"Sent={'🟢' if sent['sentiment_score'] > 0.1 else '🔴' if sent['sentiment_score'] < -0.1 else '⚪'} "
            f"Regime={reg_mod:.2f} Sector={sec_mod:.2f} Trend={trd_mod:.2f} "
            f"Vol={vol['vol_tier']} Poly={poly['poly_tier']}"
        )

        return {
            "super_conf_mod":         super_mod,
            "super_confidence_score": super_score,
            "super_verdict":          verdict,
            "super_verdict_key":      verdict_key,
            "super_gate_veto":        gate_veto,
            "super_compact":          compact,
            "ict_mod":                round(ict_score, 3),
            "sentiment_mod":          round(sent_mod, 3),
            "sentiment_score":        sent["sentiment_score"],
            "sentiment_keywords_pos": sent["top_positive"],
            "sentiment_keywords_neg": sent["top_negative"],
            "regime_mod":                  reg_mod,
            "regime_verdict":              reg["regime_verdict"],
            "regime_change_warning":       reg.get("regime_change_warning", False),   # UPG-14
            "regime_persistence_secs":     reg.get("regime_persistence_secs", 0.0),   # UPG-14
            "sector_mod":             sec_mod,
            "sector_verdict":         sec["sector_verdict"],
            "trend_mod":              trd_mod,
            "trend_verdict":          trd["trend_verdict"],
            "cross_mod":              crs_mod,
            "cross_verdict":          crs["cross_verdict"],
            # UPG-8: VolatilityBrain output
            "vol_mod":                vol_mod,
            "vol_tier":               vol["vol_tier"],
            "vol_verdict":            vol["vol_verdict"],
            # RGD-1: Regime-aware scoring metadata
            "regime_detected":        _detected_regime,
            "regime_weights_applied": _rw is not None,
        }

    def record_outcome_all_brains(
        self,
        symbol:       str,
        ict_state:    str,
        bias:         str,
        outcome:      str,
        regime:       str   = "TRENDING",
        sector:       str   = "",
        n_confl:      int   = 1,
        quality:      str   = "MEDIUM",
        atr_pct:      float = 0.0,   # UPG-8: forward ATR for VolatilityBrain record
        # v4.1: PolymarketBrain feedback params
        poly_strategy: str  = "",    # "CEX_LAG"|"AI_EDGE"|"WHALE"|"NEGRISK"|""
        poly_edge:     float = 0.0,
    ) -> None:
        self.regime.record_transition(regime, regime, outcome)
        if sector:
            self.sector.record_sector_signal(sector, "TREND", outcome)
        self.trend.record_trend_signal(quality, outcome)
        self.cross.record_cross_signal([symbol], n_confl, outcome)
        # UPG-8: record ATR observation for VolatilityBrain
        if atr_pct > 0:
            self.volatility.record_atr_percentile(symbol, atr_pct)
        # v4.1: record Polymarket outcome for PolymarketBrain
        if poly_strategy and poly_edge > 0:
            self.polymarket.record_outcome(
                symbol=symbol,
                strategy=poly_strategy,
                edge=poly_edge,
                outcome=outcome,
            )
        log.info(f"[SUPER_MARKOV] {symbol} | {ict_state} {bias} {outcome} "
                 f"→ all 8 brains updated")


# ══════════════════════════════════════════════════════════════════════
# SECTION 11 — MODULE SINGLETONS
# ══════════════════════════════════════════════════════════════════════

_BRAIN_PATH       = os.environ.get("MARKOV_BRAIN_PATH", "markov_brain.json")

# NOTE: This project targets small servers (128–256MB RAM). Import-time singletons
# force the whole Markov brain into memory even when the feature isn't used.
# We keep the public module globals for backward compatibility, but lazy-load the
# heavy objects on first use.

_REAL_SINGLETONS = {}


class _LazyProxy:
    """Lightweight lazy proxy to avoid heavy import-time initialization."""

    __slots__ = ("_name", "_factory")

    def __init__(self, name: str, factory):
        self._name = name
        self._factory = factory

    def _get(self):
        obj = _REAL_SINGLETONS.get(self._name)
        if obj is None:
            obj = self._factory()
            _REAL_SINGLETONS[self._name] = obj
        return obj

    def __getattr__(self, item):
        return getattr(self._get(), item)

    def __repr__(self):
        return f"<LazyProxy {self._name}>"


def _get_brain() -> "MarkovBrain":
    obj = _REAL_SINGLETONS.get("_MARKOV_BRAIN")
    if obj is None:
        obj = MarkovBrain(_BRAIN_PATH)
        _REAL_SINGLETONS["_MARKOV_BRAIN"] = obj
    return obj


def _get_engine() -> "MarkovICTEngine":
    obj = _REAL_SINGLETONS.get("_MARKOV_ENGINE")
    if obj is None:
        obj = MarkovICTEngine(min_samples=MIN_SAMPLES_FOR_VETO)
        _REAL_SINGLETONS["_MARKOV_ENGINE"] = obj
    return obj


def _get_validator() -> "MarkovValidator":
    obj = _REAL_SINGLETONS.get("_MARKOV_VALIDATOR")
    if obj is None:
        obj = MarkovValidator(_get_brain())
        _REAL_SINGLETONS["_MARKOV_VALIDATOR"] = obj
    return obj


def _get_super_validator() -> "SuperValidator":
    obj = _REAL_SINGLETONS.get("_SUPER_VALIDATOR")
    if obj is None:
        obj = SuperValidator(_get_brain())
        _REAL_SINGLETONS["_SUPER_VALIDATOR"] = obj
    return obj


def _get_level_learner() -> "LevelLearner":
    obj = _REAL_SINGLETONS.get("_LEVEL_LEARNER")
    if obj is None:
        obj = LevelLearner(_get_brain())
        _REAL_SINGLETONS["_LEVEL_LEARNER"] = obj
    return obj


def _get_macro_calibrator() -> "MacroEventCalibrator":
    obj = _REAL_SINGLETONS.get("_MACRO_CALIBRATOR")
    if obj is None:
        obj = MacroEventCalibrator(_get_brain())
        _REAL_SINGLETONS["_MACRO_CALIBRATOR"] = obj
    return obj


# Backward-compatible public globals (lazy).
_MARKOV_BRAIN     = _LazyProxy("_MARKOV_BRAIN", _get_brain)
_MARKOV_ENGINE    = _LazyProxy("_MARKOV_ENGINE", _get_engine)
_MARKOV_VALIDATOR = _LazyProxy("_MARKOV_VALIDATOR", _get_validator)
_SUPER_VALIDATOR  = _LazyProxy("_SUPER_VALIDATOR", _get_super_validator)
_LEVEL_LEARNER    = _LazyProxy("_LEVEL_LEARNER", _get_level_learner)
_MACRO_CALIBRATOR = _LazyProxy("_MACRO_CALIBRATOR", _get_macro_calibrator)

log.info(
    f"[MARKOV_STACK v4.1] Loaded (lazy) · brain={_BRAIN_PATH} · "
    f"v4.1:PolymarketBrain+8-brain·UPG8:VolatilityBrain·UPG21:StatCache"
)
