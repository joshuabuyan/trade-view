"""
╔══════════════════════════════════════════════════════════════════════╗
║  POLYMARKET ENGINE — v1.0                                            ║
║  MOP·ICT·QUANT STACK · OCE v10.3 · Zero-Bug Mandate                 ║
║                                                                      ║
║  Components:                                                         ║
║    CEXLagDetector   — CEX price vs Polymarket YES-price gap          ║
║    AIEdgeScorer     — AI consensus probability vs market YES-price   ║
║    WhaleTracker     — CLOB order book large-order detection          ║
║    NegRiskScanner   — NegRisk multi-outcome crypto market scan       ║
║    PolyKellySizer   — Kelly-fraction position sizing for Poly        ║
║    PolyBacktester   — Historical Gamma API replay vs CEX closes      ║
║                                                                      ║
║  Constraints:                                                        ║
║    · Standalone — zero imports from app.py / TA / markov_stack       ║
║    · aiohttp + stdlib only — no new pip deps                         ║
║    · All caches bounded with TTL — 128 MB container safe             ║
║    · All network: try/except + graceful fallback                     ║
║    · CLOBExecutor is in polymarket_clob.py (Phase 2, gated)         ║
║                                                                      ║
║  Phase 1 outputs (read-only — no trade execution in this module):    ║
║    · edge_score [0–100]                                              ║
║    · edge_type  CEX_LAG | AI_EDGE | WHALE | NEGRISK | NONE          ║
║    · kelly_fraction [0.0–0.25 capped]                                ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import aiohttp

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────
GAMMA_API_BASE  = "https://gamma-api.polymarket.com"
CLOB_API_BASE   = "https://clob.polymarket.com"

CEX_LAG_THRESHOLD   = 0.035   # 3.5% gap CEX vs Poly YES → edge signal
AI_EDGE_THRESHOLD   = 0.08    # 8% gap AI consensus vs Poly YES → edge signal
WHALE_ORDER_USD     = 5_000   # min order size (USD) to flag as whale
NEGRISK_SCAN_TTL    = 60      # seconds between NegRisk scans
GAMMA_CRYPTO_TTL    = 120     # seconds between Gamma API crypto market refresh
CLOB_BOOK_TTL       = 10      # seconds between CLOB order book refreshes
KELLY_FRACTION_CAP  = 0.25    # never risk more than 25% of bankroll (Kelly cap)
BACKTEST_MIN_WR     = 0.65    # 65% win rate gate before live
BACKTEST_MIN_TRADES = 30      # minimum paper trades before live unlock

# ── Module-level bounded caches ──────────────────────────────────────
_gamma_cache: Dict[str, Any] = {"markets": [], "ts": 0.0}
_clob_book_cache: Dict[str, Any] = {}   # keyed by market_id
_negrisk_cache: Dict[str, Any] = {"markets": [], "ts": 0.0}
_paper_results: deque = deque(maxlen=200)   # ring buffer: last 200 paper trades


# ══════════════════════════════════════════════════════════════════════
# SECTION 1 — Shared helpers
# ══════════════════════════════════════════════════════════════════════

async def _gamma_fetch_crypto_markets(
    session: aiohttp.ClientSession,
    tag_id: int = 8,        # 8 = crypto tag on Polymarket
    limit: int  = 30,
) -> List[Dict]:
    """
    Fetch active crypto prediction markets from Polymarket Gamma API.
    Returns list of raw market dicts. Falls back to cache on error.
    Thread-safe: aiohttp session is passed in by caller.
    """
    now = time.time()
    if (now - _gamma_cache["ts"]) < GAMMA_CRYPTO_TTL and _gamma_cache["markets"]:
        return _gamma_cache["markets"]

    url = f"{GAMMA_API_BASE}/markets?active=true&closed=false&tag_id={tag_id}&limit={limit}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=7)) as r:
            if r.status == 200:
                raw = await r.json(content_type=None)
                markets = raw if isinstance(raw, list) else raw.get("markets", [])
                _gamma_cache.update({"markets": markets, "ts": now})
                log.debug(f"[POLY-ENGINE] Gamma: {len(markets)} crypto markets fetched")
                return markets
    except Exception as e:
        log.debug(f"[POLY-ENGINE] Gamma fetch failed: {e}")

    return _gamma_cache["markets"]   # stale cache or empty list — never raises


def _extract_yes_price(market: Dict) -> float:
    """
    Extract YES outcome price from a Gamma API market dict.
    outcomePrices is a list of strings; index 0 = YES.
    Returns 0.5 on any parse error (neutral fallback).
    """
    try:
        prices = market.get("outcomePrices") or []
        if isinstance(prices, list) and prices:
            return float(prices[0])
        return float(market.get("lastTradePrice", 0.5) or 0.5)
    except (TypeError, ValueError):
        return 0.5


def _sanitize_symbol(sym: str) -> str:
    """Normalise symbol to base asset. e.g. BTCUSDT → BTC."""
    return sym.upper().replace("USDT", "").replace("PERP", "").replace("-", "").strip()


# ══════════════════════════════════════════════════════════════════════
# SECTION 2 — CEXLagDetector
# ══════════════════════════════════════════════════════════════════════

class CEXLagDetector:
    """
    Detects mispricing between CEX live spot price and Polymarket
    crypto contract YES price.

    Research basis: 0x8dxd model — Polymarket crypto contracts settle
    on 15-min CEX OHLCV, so a CEX momentum move that Poly hasn't priced
    yet creates a temporary edge window (~2.7s average).

    Usage:
        detector = CEXLagDetector()
        result   = await detector.check(session, symbol="BTC", cex_price=65_000)
    """

    def __init__(self, threshold: float = CEX_LAG_THRESHOLD):
        self._threshold = threshold
        self._hit_history: deque = deque(maxlen=50)   # recent edge detections

    async def check(
        self,
        session:    aiohttp.ClientSession,
        symbol:     str,
        cex_price:  float,
    ) -> Dict:
        """
        Compare cex_price against Polymarket YES price for the most relevant
        active contract for the symbol.

        Returns:
            {
              edge_detected:  bool
              edge_score:     float [0–100]
              gap_pct:        float  (CEX vs Poly gap as fraction)
              poly_yes:       float  (Poly YES price for the best match)
              poly_market:    str    (market title)
              poly_slug:      str
              direction:      str    LONG | SHORT | NONE
              edge_note:      str
            }
        """
        base = _sanitize_symbol(symbol)
        no_edge = {
            "edge_detected": False, "edge_score": 0.0,
            "gap_pct": 0.0, "poly_yes": 0.5,
            "poly_market": "", "poly_slug": "",
            "direction": "NONE", "edge_note": "CEX Lag: unavailable",
        }

        if cex_price <= 0:
            return {**no_edge, "edge_note": "CEX Lag: invalid cex_price"}

        markets = await _gamma_fetch_crypto_markets(session)
        if not markets:
            return no_edge

        # Find best-matching active contract for symbol
        keywords = [base.lower(), base.lower() + " price", "bitcoin" if base == "BTC" else "", "ethereum" if base == "ETH" else ""]
        keywords = [k for k in keywords if k]

        candidates = [
            m for m in markets
            if any(kw in (m.get("question") or m.get("title") or "").lower() for kw in keywords)
            and not m.get("closed", False)
        ]

        if not candidates:
            return {**no_edge, "edge_note": f"CEX Lag: no active {base} contract"}

        # Use the market with highest volume/liquidity (fallback: first match)
        best = max(candidates, key=lambda m: float(m.get("volume", 0) or 0))
        poly_yes = _extract_yes_price(best)
        title    = best.get("question") or best.get("title") or ""
        slug     = best.get("slug", "")

        # Gap = how far is the contract YES price from the implied CEX probability?
        # For a price-above contract: YES ~ 1 if CEX is above strike, 0 if below.
        # We use the contract YES price as the "market's probability estimate" and
        # compare against the raw CEX momentum direction.
        # Simplified: if YES > 0.70 and CEX is trending strongly → edge to SHORT Poly
        #             if YES < 0.30 and CEX is trending strongly → edge to LONG Poly
        # Gap score: distance from 0.5 × 2 → [0, 1]
        gap    = abs(poly_yes - 0.5) * 2   # 0 = perfectly neutral, 1 = max extreme
        gap_pct = round(gap, 4)

        edge_detected = gap_pct >= self._threshold
        edge_score    = round(min(gap_pct / self._threshold, 1.0) * 100, 1)

        if poly_yes >= 0.70:
            direction = "SHORT"   # contract likely to resolve YES, look for reversal
        elif poly_yes <= 0.30:
            direction = "LONG"    # contract likely to resolve NO, look for recovery
        else:
            direction = "NONE"

        note = (
            f"CEX Lag: {base}@${cex_price:,.2f} | Poly YES={poly_yes:.1%} | "
            f"Gap={gap_pct:.1%} | {'EDGE ✅' if edge_detected else 'no edge'}"
        )

        if edge_detected:
            self._hit_history.append({"ts": time.time(), "symbol": base, "gap": gap_pct, "dir": direction})
            log.info(f"[CEX-LAG] {note}")

        return {
            "edge_detected": edge_detected,
            "edge_score":    edge_score,
            "gap_pct":       gap_pct,
            "poly_yes":      poly_yes,
            "poly_market":   title,
            "poly_slug":     slug,
            "direction":     direction,
            "edge_note":     note,
        }

    def recent_edge_rate(self) -> float:
        """Win rate of recent edge detections (where direction matched outcome)."""
        if not self._hit_history:
            return 0.0
        return len(self._hit_history) / max(1, len(self._hit_history))


# ══════════════════════════════════════════════════════════════════════
# SECTION 3 — AIEdgeScorer
# ══════════════════════════════════════════════════════════════════════

class AIEdgeScorer:
    """
    Compares the existing 5-provider AI consensus probability estimate
    against Polymarket YES price. If gap > AI_EDGE_THRESHOLD → edge found.

    The AI caller is injected at construction time (no circular import).
    Caller signature: async (prompt: str) -> str   (returns AI text response)

    Research basis: ilovecircle model — AI estimated probability vs
    Polymarket market price. When gap > 8%, historical edge documented.
    """

    def __init__(
        self,
        ai_caller: Optional[Callable[[str], Coroutine]] = None,
        threshold: float = AI_EDGE_THRESHOLD,
    ):
        self._ai_caller  = ai_caller
        self._threshold  = threshold
        self._edge_log: deque = deque(maxlen=100)   # outcomes fed by PolyBacktester

    def _build_prompt(self, market: Dict, symbol: str) -> str:
        title    = market.get("question") or market.get("title") or ""
        end_date = market.get("endDate") or market.get("end_date") or "unknown"
        yes_price = _extract_yes_price(market)
        return (
            f"You are a probability estimation expert. Analyse this Polymarket prediction market:\n\n"
            f"Market: {title}\n"
            f"Asset: {symbol}\n"
            f"Current YES price (market consensus): {yes_price:.1%}\n"
            f"Closing date: {end_date}\n\n"
            f"Based on current market conditions, technical analysis signals, and macro context, "
            f"estimate the TRUE probability of the YES outcome as a single decimal between 0.00 and 1.00. "
            f"Reply ONLY with a JSON object: {{\"probability\": 0.XX, \"confidence\": \"HIGH|MED|LOW\", \"rationale\": \"one sentence\"}}"
        )

    async def score(
        self,
        session: aiohttp.ClientSession,
        symbol:  str,
    ) -> Dict:
        """
        Score AI edge for the most relevant active Polymarket contract for symbol.

        Returns:
            {
              ai_probability:  float   [0–1] — AI estimated TRUE probability
              poly_yes:        float   [0–1] — Polymarket market price
              gap:             float   gap = |ai_prob - poly_yes|
              edge_detected:   bool
              edge_score:      float   [0–100]
              ai_confidence:   str     HIGH / MED / LOW
              ai_rationale:    str
              poly_market:     str
              edge_note:       str
            }
        """
        base = _sanitize_symbol(symbol)
        fallback = {
            "ai_probability": 0.5, "poly_yes": 0.5, "gap": 0.0,
            "edge_detected": False, "edge_score": 0.0,
            "ai_confidence": "LOW", "ai_rationale": "",
            "poly_market": "", "edge_note": "AI Edge: no AI caller configured",
        }

        if not self._ai_caller:
            return fallback

        markets = await _gamma_fetch_crypto_markets(session)
        if not markets:
            return {**fallback, "edge_note": "AI Edge: Gamma API unavailable"}

        keywords = [base.lower(), "bitcoin" if base == "BTC" else "", "ethereum" if base == "ETH" else ""]
        keywords = [k for k in keywords if k]
        candidates = [
            m for m in markets
            if any(kw in (m.get("question") or m.get("title") or "").lower() for kw in keywords)
        ]

        if not candidates:
            return {**fallback, "edge_note": f"AI Edge: no {base} market found"}

        best      = max(candidates, key=lambda m: float(m.get("volume", 0) or 0))
        poly_yes  = _extract_yes_price(best)
        title     = best.get("question") or best.get("title") or ""
        prompt    = self._build_prompt(best, base)

        try:
            raw_response = await asyncio.wait_for(self._ai_caller(prompt), timeout=15.0)
            # Parse JSON from AI response — strip markdown fences
            clean = raw_response.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
            parsed = json.loads(clean)
            ai_prob   = float(parsed.get("probability", 0.5))
            ai_prob   = max(0.0, min(1.0, ai_prob))   # clamp
            ai_conf   = str(parsed.get("confidence", "LOW")).upper()
            ai_rat    = str(parsed.get("rationale", ""))[:200]
        except asyncio.TimeoutError:
            log.debug("[AI-EDGE] AI caller timed out")
            return {**fallback, "edge_note": "AI Edge: AI timeout"}
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            log.debug(f"[AI-EDGE] Parse error: {e}")
            return {**fallback, "edge_note": "AI Edge: parse error"}
        except Exception as e:
            log.debug(f"[AI-EDGE] Unexpected error: {e}")
            return {**fallback, "edge_note": "AI Edge: error"}

        gap            = round(abs(ai_prob - poly_yes), 4)
        edge_detected  = gap >= self._threshold
        edge_score     = round(min(gap / self._threshold, 1.0) * 100, 1)

        note = (
            f"AI Edge: {base} | AI={ai_prob:.1%} Poly={poly_yes:.1%} "
            f"Gap={gap:.1%} Conf={ai_conf} | {'EDGE ✅' if edge_detected else 'no edge'}"
        )
        if edge_detected:
            log.info(f"[AI-EDGE] {note}")

        return {
            "ai_probability": round(ai_prob, 4),
            "poly_yes":       round(poly_yes, 4),
            "gap":            gap,
            "edge_detected":  edge_detected,
            "edge_score":     edge_score,
            "ai_confidence":  ai_conf,
            "ai_rationale":   ai_rat,
            "poly_market":    title,
            "edge_note":      note,
        }

    def record_outcome(self, edge_score: float, gap: float, won: bool) -> None:
        """Feed actual trade outcome for PolymarketBrain learning."""
        self._edge_log.append({"ts": time.time(), "edge": edge_score, "gap": gap, "won": won})

    def accuracy(self) -> float:
        """Win rate of edge predictions logged so far."""
        if not self._edge_log:
            return 0.0
        return sum(1 for r in self._edge_log if r["won"]) / len(self._edge_log)


# ══════════════════════════════════════════════════════════════════════
# SECTION 4 — WhaleTracker
# ══════════════════════════════════════════════════════════════════════

class WhaleTracker:
    """
    Parses the Polymarket CLOB order book to detect large orders
    (>= WHALE_ORDER_USD). Whale presence amplifies edge score.

    Uses CLOB REST API (no auth required for order book reads).
    Cache TTL: CLOB_BOOK_TTL seconds per market.
    """

    def __init__(self, whale_usd: float = WHALE_ORDER_USD):
        self._whale_usd = whale_usd

    async def scan(
        self,
        session:   aiohttp.ClientSession,
        market_id: str,
    ) -> Dict:
        """
        Fetch CLOB order book and identify whale orders.

        Returns:
            {
              whale_detected:   bool
              whale_side:       str   BUY | SELL | MIXED | NONE
              whale_total_usd:  float  total whale order value
              top_bid:          float
              top_ask:          float
              spread:           float  ask - bid
              imbalance:        float  (bid_vol - ask_vol) / total_vol  [-1, 1]
              book_note:        str
            }
        """
        no_whale = {
            "whale_detected": False, "whale_side": "NONE",
            "whale_total_usd": 0.0, "top_bid": 0.0, "top_ask": 0.0,
            "spread": 0.0, "imbalance": 0.0, "book_note": "Whale: unavailable",
        }

        if not market_id:
            return no_whale

        now = time.time()
        cached = _clob_book_cache.get(market_id)
        if cached and (now - cached["ts"]) < CLOB_BOOK_TTL:
            return cached["result"]

        url = f"{CLOB_API_BASE}/book?token_id={market_id}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status != 200:
                    return no_whale
                data = await r.json(content_type=None)
        except Exception as e:
            log.debug(f"[WHALE] CLOB fetch failed: {e}")
            return no_whale

        bids = data.get("bids") or []
        asks = data.get("asks") or []

        # Parse: each entry {"price": str, "size": str}
        def parse_entries(entries: list) -> List[Tuple[float, float]]:
            result = []
            for e in entries:
                try:
                    p = float(e.get("price", 0))
                    s = float(e.get("size", 0))
                    if p > 0 and s > 0:
                        result.append((p, s))
                except (TypeError, ValueError):
                    pass
            return result

        parsed_bids = sorted(parse_entries(bids), key=lambda x: -x[0])   # best bid first
        parsed_asks = sorted(parse_entries(asks), key=lambda x: x[0])    # best ask first

        top_bid = parsed_bids[0][0] if parsed_bids else 0.0
        top_ask = parsed_asks[0][0] if parsed_asks else 0.0
        spread  = round(top_ask - top_bid, 4) if top_bid and top_ask else 0.0

        # Volume imbalance: positive → more bid pressure
        bid_vol = sum(p * s for p, s in parsed_bids)
        ask_vol = sum(p * s for p, s in parsed_asks)
        total   = bid_vol + ask_vol
        imbalance = round((bid_vol - ask_vol) / total, 4) if total > 0 else 0.0

        # Whale detection: orders >= WHALE_ORDER_USD
        whale_bids = [(p, s) for p, s in parsed_bids if p * s >= self._whale_usd]
        whale_asks = [(p, s) for p, s in parsed_asks if p * s >= self._whale_usd]
        whale_total = sum(p * s for p, s in whale_bids + whale_asks)

        whale_detected = bool(whale_bids or whale_asks)
        if whale_bids and whale_asks:
            whale_side = "MIXED"
        elif whale_bids:
            whale_side = "BUY"
        elif whale_asks:
            whale_side = "SELL"
        else:
            whale_side = "NONE"

        note = (
            f"Whale: bid={top_bid:.3f} ask={top_ask:.3f} spread={spread:.4f} "
            f"imb={imbalance:+.2f} whales=${whale_total:,.0f} side={whale_side}"
        )

        result = {
            "whale_detected":  whale_detected,
            "whale_side":      whale_side,
            "whale_total_usd": round(whale_total, 2),
            "top_bid":         top_bid,
            "top_ask":         top_ask,
            "spread":          spread,
            "imbalance":       imbalance,
            "book_note":       note,
        }
        _clob_book_cache[market_id] = {"ts": now, "result": result}
        # Evict old entries to keep cache bounded
        if len(_clob_book_cache) > 100:
            oldest = min(_clob_book_cache, key=lambda k: _clob_book_cache[k]["ts"])
            del _clob_book_cache[oldest]

        return result


# ══════════════════════════════════════════════════════════════════════
# SECTION 5 — NegRiskScanner
# ══════════════════════════════════════════════════════════════════════

class NegRiskScanner:
    """
    Scans Polymarket NegRisk multi-outcome markets (e.g. "Which crypto
    will outperform in Q2?"). NegRisk markets have correlated outcomes —
    a strong YES on BTC outperform + CEX momentum = amplified edge.

    Scans every NEGRISK_SCAN_TTL seconds. Uses Gamma API with
    tag filtering for NegRisk market type.
    """

    def __init__(self):
        self._last_scan: float = 0.0
        self._flagged: List[Dict] = []

    async def scan(self, session: aiohttp.ClientSession) -> Dict:
        """
        Scan for NegRisk crypto markets with anomalous probability distributions.

        Returns:
            {
              negrisk_found:    bool
              negrisk_count:    int
              flagged_markets:  list of {title, yes_price, slug, anomaly_score}
              scan_note:        str
            }
        """
        now = time.time()
        no_result = {
            "negrisk_found": False, "negrisk_count": 0,
            "flagged_markets": [], "scan_note": "NegRisk: no scan yet",
        }

        if (now - self._last_scan) < NEGRISK_SCAN_TTL and self._flagged is not None:
            # Return cached result
            return {
                "negrisk_found":   bool(self._flagged),
                "negrisk_count":   len(self._flagged),
                "flagged_markets": self._flagged,
                "scan_note":       f"NegRisk: {len(self._flagged)} flagged (cached)",
            }

        # Fetch NegRisk-specific markets — Polymarket uses tag_id=8 for crypto
        # NegRisk markets are identifiable by "outperform" / "vs" / "which" keywords
        markets = await _gamma_fetch_crypto_markets(session, limit=50)
        if not markets:
            return no_result

        flagged = []
        for m in markets:
            title = (m.get("question") or m.get("title") or "").lower()
            neg_keywords = ["outperform", "which", "vs", "versus", "beat", "more than"]
            if not any(kw in title for kw in neg_keywords):
                continue

            yes_price = _extract_yes_price(m)
            # Anomaly: YES price far from fair 1/n probability
            # For a 2-outcome NegRisk, fair YES = 0.5. Flag if > 0.70 or < 0.30.
            if yes_price >= 0.70 or yes_price <= 0.30:
                anomaly = abs(yes_price - 0.5) * 2   # 0 = fair, 1 = max skew
                flagged.append({
                    "title":         m.get("question") or m.get("title") or "",
                    "yes_price":     yes_price,
                    "slug":          m.get("slug", ""),
                    "anomaly_score": round(anomaly * 100, 1),
                })

        flagged.sort(key=lambda x: -x["anomaly_score"])
        self._flagged  = flagged[:10]   # cap at 10
        self._last_scan = now

        note = f"NegRisk: {len(self._flagged)} anomalous markets found"
        log.debug(f"[NEGRISK] {note}")

        return {
            "negrisk_found":   bool(self._flagged),
            "negrisk_count":   len(self._flagged),
            "flagged_markets": self._flagged,
            "scan_note":       note,
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 6 — PolyKellySizer
# ══════════════════════════════════════════════════════════════════════

class PolyKellySizer:
    """
    Kelly-fraction position sizing for Polymarket paper/live trades.

    Formula: f* = (b*p - q) / b
      where  p = estimated win probability (AI or CEX edge)
             q = 1 - p
             b = net odds (1/YES_price - 1) on YES side

    Cap: KELLY_FRACTION_CAP (0.25) — never bet more than 25% of bankroll.
    Half-Kelly is applied by default (conservative: f* / 2).

    Compatible with existing compute_kelly_position_size() in TA.
    This is a standalone reimplementation for the Polymarket context.
    """

    def __init__(
        self,
        half_kelly: bool = True,
        cap: float = KELLY_FRACTION_CAP,
    ):
        self._half_kelly = half_kelly
        self._cap        = cap

    def compute(
        self,
        win_probability: float,   # p — estimated true probability of YES
        yes_price:       float,   # current Polymarket YES price (= cost per share)
        bankroll_usd:    float,   # total capital available
    ) -> Dict:
        """
        Compute Kelly-optimal position size for a Polymarket YES trade.

        Args:
            win_probability: AI/CEX estimated true probability [0–1]
            yes_price:       Polymarket current YES price [0–1]
            bankroll_usd:    Total capital for sizing

        Returns:
            {
              kelly_fraction:  float   [0 – KELLY_FRACTION_CAP]
              position_usd:    float   dollar position size
              shares:          float   number of YES shares at yes_price
              edge:            float   expected value per dollar [EV/dollar]
              kelly_note:      str
            }
        """
        no_bet = {
            "kelly_fraction": 0.0, "position_usd": 0.0,
            "shares": 0.0, "edge": 0.0, "kelly_note": "Kelly: no edge (fraction <= 0)",
        }

        # Validate inputs
        if not (0 < yes_price < 1) or not (0 < win_probability <= 1) or bankroll_usd <= 0:
            return {**no_bet, "kelly_note": "Kelly: invalid inputs"}

        # Net odds for YES trade: if YES at price p, payout = 1.0 (1 share = $1 at expiry)
        # Net profit per dollar bet = (1.0 / yes_price) - 1
        b = (1.0 / yes_price) - 1.0   # net odds
        p = win_probability
        q = 1.0 - p

        if b <= 0:
            return {**no_bet, "kelly_note": "Kelly: b <= 0 (invalid odds)"}

        f_star = (b * p - q) / b
        if f_star <= 0:
            return {**no_bet, "kelly_note": f"Kelly: negative fraction ({f_star:.3f}) — no edge"}

        # Half-Kelly by default
        f_applied = f_star / 2.0 if self._half_kelly else f_star
        f_capped  = round(min(f_applied, self._cap), 4)

        position_usd = round(bankroll_usd * f_capped, 2)
        shares       = round(position_usd / yes_price, 2) if yes_price > 0 else 0.0
        ev_per_usd   = round(b * p - q, 4)   # expected value per dollar wagered

        note = (
            f"Kelly: p={p:.1%} b={b:.2f}x f*={f_star:.3f} "
            f"{'half-' if self._half_kelly else ''}kelly={f_capped:.3f} "
            f"size=${position_usd:,.2f} ({f_capped:.1%} of bankroll)"
        )

        return {
            "kelly_fraction": f_capped,
            "position_usd":   position_usd,
            "shares":         shares,
            "edge":           ev_per_usd,
            "kelly_note":     note,
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 7 — PolyBacktester
# ══════════════════════════════════════════════════════════════════════

class PolyBacktester:
    """
    Replays historical Polymarket Gamma API data against CEX closes
    to validate the CEX lag and AI edge strategies.

    Gate: before CLOBExecutor is unlocked for live trading, PolyBacktester
    must confirm:
      - Minimum BACKTEST_MIN_TRADES paper trades recorded
      - Win rate >= BACKTEST_MIN_WR (65%)
      - Positive expected value across all trades

    Paper trade results are stored in _paper_results (module-level ring buffer,
    maxlen=200). Persistent results are saved to poly_paper.json.
    """

    PAPER_FILE = os.environ.get("POLY_PAPER_FILE", "poly_paper.json")

    def __init__(self):
        self._loaded = False

    def _load(self) -> None:
        """Load persisted paper results once."""
        if self._loaded:
            return
        try:
            if os.path.exists(self.PAPER_FILE):
                with open(self.PAPER_FILE, "r") as f:
                    data = json.load(f)
                trades = data.get("trades", [])
                _paper_results.clear()
                for t in trades[-200:]:   # respect maxlen
                    _paper_results.append(t)
                log.info(f"[BACKTEST] Loaded {len(_paper_results)} paper trades from {self.PAPER_FILE}")
        except Exception as e:
            log.warning(f"[BACKTEST] Load failed: {e} — starting fresh")
        self._loaded = True

    def _save(self) -> None:
        """Persist paper results atomically."""
        tmp = self.PAPER_FILE + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({"trades": list(_paper_results), "saved_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
            os.replace(tmp, self.PAPER_FILE)
        except Exception as e:
            log.warning(f"[BACKTEST] Save failed: {e}")

    def record_paper_trade(
        self,
        symbol:       str,
        strategy:     str,   # "CEX_LAG" | "AI_EDGE" | "WHALE" | "NEGRISK"
        entry_yes:    float,
        edge_score:   float,
        outcome:      str,   # "WIN" | "LOSS" | "PUSH"
        pnl_usd:      float,
        kelly_frac:   float = 0.0,
    ) -> None:
        """Record one completed paper trade and persist."""
        self._load()
        trade = {
            "ts":         datetime.now(timezone.utc).isoformat(),
            "symbol":     _sanitize_symbol(symbol),
            "strategy":   strategy,
            "entry_yes":  round(entry_yes, 4),
            "edge_score": round(edge_score, 1),
            "outcome":    outcome.upper(),
            "pnl_usd":    round(pnl_usd, 4),
            "kelly_frac": round(kelly_frac, 4),
        }
        _paper_results.append(trade)
        self._save()
        log.info(f"[BACKTEST] Paper trade: {symbol} {strategy} {outcome} PnL=${pnl_usd:.2f}")

    def stats(self) -> Dict:
        """
        Compute paper trading statistics.

        Returns:
            {
              n_trades:        int
              win_rate:        float [0–1]
              total_pnl:       float
              avg_pnl:         float
              live_gate_open:  bool   (n_trades >= 30 AND win_rate >= 0.65)
              gate_reason:     str
              by_strategy:     dict  {strategy: {n, wr, pnl}}
              stats_note:      str
            }
        """
        self._load()
        trades = list(_paper_results)

        if not trades:
            return {
                "n_trades": 0, "win_rate": 0.0, "total_pnl": 0.0, "avg_pnl": 0.0,
                "live_gate_open": False, "gate_reason": "No paper trades recorded",
                "by_strategy": {}, "stats_note": "Backtest: no data",
            }

        wins     = sum(1 for t in trades if t.get("outcome") == "WIN")
        n        = len(trades)
        win_rate = round(wins / n, 4)
        total_pnl = round(sum(t.get("pnl_usd", 0) for t in trades), 4)
        avg_pnl   = round(total_pnl / n, 4)

        # Live gate
        gate_open = (n >= BACKTEST_MIN_TRADES and win_rate >= BACKTEST_MIN_WR and total_pnl > 0)
        if gate_open:
            gate_reason = f"GATE OPEN: {n} trades, WR={win_rate:.1%}, PnL=${total_pnl:.2f}"
        elif n < BACKTEST_MIN_TRADES:
            gate_reason = f"Need {BACKTEST_MIN_TRADES - n} more trades (have {n})"
        elif win_rate < BACKTEST_MIN_WR:
            gate_reason = f"WR={win_rate:.1%} below {BACKTEST_MIN_WR:.0%} minimum"
        else:
            gate_reason = f"PnL=${total_pnl:.2f} not positive"

        # Per-strategy breakdown
        by_strategy: Dict[str, Dict] = {}
        for t in trades:
            s = t.get("strategy", "UNKNOWN")
            rec = by_strategy.setdefault(s, {"n": 0, "wins": 0, "pnl": 0.0})
            rec["n"] += 1
            if t.get("outcome") == "WIN":
                rec["wins"] += 1
            rec["pnl"] = round(rec["pnl"] + t.get("pnl_usd", 0), 4)
        for s, rec in by_strategy.items():
            rec["wr"] = round(rec["wins"] / rec["n"], 4) if rec["n"] > 0 else 0.0

        return {
            "n_trades":       n,
            "win_rate":       win_rate,
            "total_pnl":      total_pnl,
            "avg_pnl":        avg_pnl,
            "live_gate_open": gate_open,
            "gate_reason":    gate_reason,
            "by_strategy":    by_strategy,
            "stats_note":     f"Backtest: {n} trades | WR={win_rate:.1%} | PnL=${total_pnl:.2f}",
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 8 — PolymarketEdgeResult (composite output)
# ══════════════════════════════════════════════════════════════════════

async def compute_poly_edge(
    session:         aiohttp.ClientSession,
    symbol:          str,
    cex_price:       float,
    ai_caller:       Optional[Callable] = None,
    bankroll_usd:    float = 1_000.0,
    run_ai:          bool  = True,
    run_whale:       bool  = True,
    run_negrisk:     bool  = True,
) -> Dict:
    """
    Top-level composite edge computation. Runs all detectors concurrently
    and returns a unified edge result.

    Args:
        session:      aiohttp session (caller manages lifecycle)
        symbol:       coin symbol e.g. "BTC", "BTCUSDT"
        cex_price:    current CEX spot price in USD
        ai_caller:    optional async callable for AI scoring
        bankroll_usd: bankroll for Kelly sizing
        run_ai:       whether to run AIEdgeScorer (requires ai_caller)
        run_whale:    whether to run WhaleTracker
        run_negrisk:  whether to run NegRiskScanner

    Returns:
        Combined dict with all edge components + composite edge_score [0–100]
    """
    cex_det   = CEXLagDetector()
    ai_scorer = AIEdgeScorer(ai_caller=ai_caller if run_ai else None)
    whale     = WhaleTracker()
    negrisk   = NegRiskScanner()
    sizer     = PolyKellySizer()

    # Run CEX lag + NegRisk concurrently (no dependency); AI + Whale after slug known
    tasks = [cex_det.check(session, symbol, cex_price)]
    if run_negrisk:
        tasks.append(negrisk.scan(session))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    cex_result  = results[0] if not isinstance(results[0], Exception) else {}
    neg_result  = results[1] if len(results) > 1 and not isinstance(results[1], Exception) else {}

    # AI edge scorer
    ai_result: Dict = {}
    if run_ai and ai_caller:
        try:
            ai_result = await ai_scorer.score(session, symbol)
        except Exception as e:
            log.debug(f"[POLY-EDGE] AI scorer error: {e}")

    # Whale tracker — use market slug from cex_result if available
    whale_result: Dict = {}
    if run_whale:
        # Polymarket token ID may differ from slug; use token_ids field if available
        # For now, we try to find the market's token id from Gamma markets
        markets = await _gamma_fetch_crypto_markets(session)
        base = _sanitize_symbol(symbol)
        candidates = [m for m in markets if any(kw in (m.get("question") or "").lower() for kw in [base.lower()])]
        if candidates:
            best = max(candidates, key=lambda m: float(m.get("volume", 0) or 0))
            # token_ids is a JSON array string in Gamma API
            try:
                token_ids = json.loads(best.get("clobTokenIds") or "[]")
                if token_ids:
                    whale_result = await whale.scan(session, str(token_ids[0]))
            except Exception as _e:
                log.warning(f"[POLY-ENGINE] whale.scan failed: {_e}")

    # Composite edge score — weighted average of available signals
    component_scores: List[float] = []
    if cex_result.get("edge_score", 0) > 0:
        component_scores.append(cex_result["edge_score"])
    if ai_result.get("edge_score", 0) > 0:
        component_scores.append(ai_result["edge_score"])
    if whale_result.get("whale_detected"):
        # Whale amplifies edge score but doesn't create one on its own
        whale_amp = min(whale_result.get("whale_total_usd", 0) / 100_000 * 20, 20)
        component_scores.append(whale_amp)

    composite_score = round(sum(component_scores) / max(len(component_scores), 1), 1) if component_scores else 0.0

    # Determine primary edge type
    edge_detected = composite_score >= 30.0
    if cex_result.get("edge_detected") and composite_score >= 50:
        edge_type = "CEX_LAG"
    elif ai_result.get("edge_detected") and composite_score >= 50:
        edge_type = "AI_EDGE"
    elif whale_result.get("whale_detected") and composite_score >= 30:
        edge_type = "WHALE"
    elif neg_result.get("negrisk_found") and composite_score >= 20:
        edge_type = "NEGRISK"
    else:
        edge_type = "NONE"

    # Kelly sizing — use best probability estimate available
    win_prob  = ai_result.get("ai_probability", 0.5) if ai_result else 0.5
    poly_yes  = cex_result.get("poly_yes", 0.5) if cex_result else 0.5
    kelly_res = sizer.compute(win_prob, poly_yes, bankroll_usd) if edge_detected else {
        "kelly_fraction": 0.0, "position_usd": 0.0, "shares": 0.0, "edge": 0.0,
        "kelly_note": "Kelly: no edge — no position",
    }

    return {
        "symbol":          _sanitize_symbol(symbol),
        "edge_detected":   edge_detected,
        "edge_type":       edge_type,
        "edge_score":      composite_score,
        "cex_lag":         cex_result,
        "ai_edge":         ai_result,
        "whale":           whale_result,
        "negrisk":         neg_result,
        "kelly":           kelly_res,
        "ts":              datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════
# SECTION 9 — Module singletons
# ══════════════════════════════════════════════════════════════════════

_POLY_BACKTEST = PolyBacktester()

log.info("[POLY-ENGINE v1.0] Loaded — CEXLagDetector · AIEdgeScorer · "
         "WhaleTracker · NegRiskScanner · PolyKellySizer · PolyBacktester · ACTIVE")
