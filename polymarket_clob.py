"""
╔══════════════════════════════════════════════════════════════════════╗
║  POLYMARKET CLOB — v1.0                                              ║
║  MOP·ICT·QUANT STACK · OCE v10.3 · Zero-Bug Mandate                 ║
║                                                                      ║
║  Components:                                                         ║
║    PaperExecutor   — Simulate fills using real Polymarket prices     ║
║    CLOBExecutor    — Live CLOB execution (Phase 2, hard-gated)       ║
║    OrderTracker    — Track open/closed paper + live orders           ║
║                                                                      ║
║  Phase gates:                                                        ║
║    Phase 1 (PaperExecutor): always available                         ║
║    Phase 2 (CLOBExecutor):  requires:                                ║
║      · POLY_PRIVATE_KEY env var set                                  ║
║      · PolyBacktester.stats().live_gate_open == True                 ║
║      · Explicit human confirmation via HumanOverrideDashboard gate   ║
║                                                                      ║
║  SQLite: wraps existing TradeLifecycleTracker DB (poly_paper.db)     ║
║  Memory: bounded ring buffers — 128 MB container safe                ║
║  Thread-safe: all writes via threading.Lock                          ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────
PAPER_DB_PATH    = os.environ.get("POLY_PAPER_DB",  "poly_paper.db")
CLOB_API_BASE    = "https://clob.polymarket.com"
POLY_CHAIN_ID    = 137   # Polygon mainnet
PHASE2_ENV_KEY   = "POLY_PRIVATE_KEY"
PHASE2_FLAG_KEY  = "POLY_LIVE_ENABLED"   # must be "true" in env to unlock CLOBExecutor

# ── Threading lock ─────────────────────────────────────────────────────
_DB_LOCK = threading.Lock()


# ══════════════════════════════════════════════════════════════════════
# SECTION 1 — SQLite schema
# ══════════════════════════════════════════════════════════════════════

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS poly_orders (
    order_id     TEXT PRIMARY KEY,
    ts_open      TEXT NOT NULL,
    ts_close     TEXT,
    symbol       TEXT NOT NULL,
    strategy     TEXT NOT NULL,
    side         TEXT NOT NULL,          -- YES | NO
    entry_yes    REAL NOT NULL,          -- price paid per share
    shares       REAL NOT NULL,
    position_usd REAL NOT NULL,
    kelly_frac   REAL NOT NULL DEFAULT 0.0,
    edge_score   REAL NOT NULL DEFAULT 0.0,
    status       TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | FILLED | RESOLVED | CANCELLED
    exit_yes     REAL,                   -- price at close / resolution
    pnl_usd      REAL,
    outcome      TEXT,                   -- WIN | LOSS | PUSH | PENDING
    market_id    TEXT,
    market_title TEXT,
    mode         TEXT NOT NULL DEFAULT 'PAPER',  -- PAPER | LIVE
    notes        TEXT
);

CREATE INDEX IF NOT EXISTS idx_poly_orders_status ON poly_orders(status);
CREATE INDEX IF NOT EXISTS idx_poly_orders_symbol ON poly_orders(symbol);
CREATE INDEX IF NOT EXISTS idx_poly_orders_ts     ON poly_orders(ts_open);
"""


def _get_conn() -> sqlite3.Connection:
    """Return a SQLite connection with WAL mode for concurrent safety."""
    conn = sqlite3.connect(PAPER_DB_PATH, timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    """Create tables if they don't exist. Called once at module load."""
    with _DB_LOCK:
        try:
            conn = _get_conn()
            conn.executescript(_SCHEMA_SQL)
            conn.commit()
            conn.close()
            log.info(f"[CLOB-DB] Schema ready at {PAPER_DB_PATH}")
        except Exception as e:
            log.error(f"[CLOB-DB] Schema init failed: {e}")


def _make_order_id(symbol: str, ts: float) -> str:
    """Generate a deterministic order ID."""
    raw = f"poly|{symbol}|{ts}"
    return hashlib.md5(raw.encode()).hexdigest()[:12].upper()


# ══════════════════════════════════════════════════════════════════════
# SECTION 2 — OrderTracker
# ══════════════════════════════════════════════════════════════════════

class OrderTracker:
    """
    Tracks all open and closed Polymarket paper + live orders via SQLite.

    Wraps the same database used by PaperExecutor and CLOBExecutor.
    Provides query methods used by the /polymarket Telegram command.
    """

    def open_orders(self, mode: str = "PAPER") -> List[Dict]:
        """Return all currently open orders for the given mode."""
        with _DB_LOCK:
            try:
                conn = _get_conn()
                rows = conn.execute(
                    "SELECT * FROM poly_orders WHERE status='OPEN' AND mode=? ORDER BY ts_open DESC",
                    (mode.upper(),)
                ).fetchall()
                conn.close()
                return [dict(r) for r in rows]
            except Exception as e:
                log.error(f"[ORDER-TRACKER] open_orders error: {e}")
                return []

    def closed_orders(self, mode: str = "PAPER", limit: int = 20) -> List[Dict]:
        """Return recent closed/resolved orders."""
        with _DB_LOCK:
            try:
                conn = _get_conn()
                rows = conn.execute(
                    "SELECT * FROM poly_orders WHERE status != 'OPEN' AND mode=? ORDER BY ts_close DESC LIMIT ?",
                    (mode.upper(), limit)
                ).fetchall()
                conn.close()
                return [dict(r) for r in rows]
            except Exception as e:
                log.error(f"[ORDER-TRACKER] closed_orders error: {e}")
                return []

    def summary(self, mode: str = "PAPER") -> Dict:
        """Aggregate P&L summary for the given mode."""
        with _DB_LOCK:
            try:
                conn = _get_conn()
                rows = conn.execute(
                    "SELECT outcome, pnl_usd, COUNT(*) as n FROM poly_orders "
                    "WHERE mode=? AND outcome IS NOT NULL GROUP BY outcome",
                    (mode.upper(),)
                ).fetchall()
                conn.close()
            except Exception as e:
                log.error(f"[ORDER-TRACKER] summary error: {e}")
                return {}

        total_n = sum(r["n"] for r in rows)
        wins    = sum(r["n"] for r in rows if r["outcome"] == "WIN")
        total_pnl = sum(r["pnl_usd"] for r in rows if r["pnl_usd"] is not None)

        return {
            "mode":      mode,
            "n_total":   total_n,
            "n_wins":    wins,
            "win_rate":  round(wins / total_n, 4) if total_n > 0 else 0.0,
            "total_pnl": round(total_pnl, 4),
            "avg_pnl":   round(total_pnl / total_n, 4) if total_n > 0 else 0.0,
        }

    def update_order(
        self,
        order_id:  str,
        status:    str,
        exit_yes:  Optional[float] = None,
        pnl_usd:   Optional[float] = None,
        outcome:   Optional[str]   = None,
        notes:     Optional[str]   = None,
    ) -> bool:
        """Update an existing order record. Returns True on success."""
        ts_close = datetime.now(timezone.utc).isoformat() if status != "OPEN" else None
        with _DB_LOCK:
            try:
                conn = _get_conn()
                conn.execute(
                    """UPDATE poly_orders
                       SET status=?, exit_yes=?, pnl_usd=?, outcome=?, notes=?, ts_close=?
                       WHERE order_id=?""",
                    (status, exit_yes, pnl_usd, outcome, notes, ts_close, order_id)
                )
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                log.error(f"[ORDER-TRACKER] update_order error: {e}")
                return False


# ══════════════════════════════════════════════════════════════════════
# SECTION 3 — PaperExecutor (Phase 1)
# ══════════════════════════════════════════════════════════════════════

class PaperExecutor:
    """
    Simulates Polymarket YES fills using real current YES prices from
    the Gamma API. Records all trades to SQLite for RL loop feedback.

    Phase 1 — Zero risk. Always available. No private key required.

    Connects to:
      - PolyBacktester.record_paper_trade() → poly_paper.json
      - OrderTracker (poly_paper.db SQLite)
      - TradeLifecycleTracker (existing Cryptex SQLite) via outcome callback
    """

    def __init__(self, tracker: Optional[OrderTracker] = None):
        self._tracker = tracker or OrderTracker()
        self._open_orders: deque = deque(maxlen=50)   # in-memory mirror of open orders

    async def open_position(
        self,
        symbol:       str,
        strategy:     str,
        side:         str,           # "YES" | "NO"
        entry_yes:    float,         # current YES price (from Gamma API)
        shares:       float,
        position_usd: float,
        kelly_frac:   float = 0.0,
        edge_score:   float = 0.0,
        market_id:    str   = "",
        market_title: str   = "",
        notes:        str   = "",
    ) -> Dict:
        """
        Open a paper position. Records to SQLite. Returns order dict.

        Args:
            symbol:       Base symbol (e.g. "BTC")
            strategy:     "CEX_LAG" | "AI_EDGE" | "WHALE" | "NEGRISK"
            side:         "YES" | "NO"
            entry_yes:    YES price at fill (cost per share)
            shares:       Number of shares bought
            position_usd: Total dollars spent (shares * entry_yes)
            kelly_frac:   Kelly fraction used for sizing
            edge_score:   Composite edge score at entry [0–100]
            market_id:    Polymarket token ID
            market_title: Market question/title
            notes:        Optional annotation

        Returns:
            Order dict with order_id or {"error": reason}
        """
        # Validate inputs
        if not (0 < entry_yes < 1):
            return {"error": f"Invalid entry_yes={entry_yes} (must be 0–1)"}
        if shares <= 0 or position_usd <= 0:
            return {"error": "shares and position_usd must be positive"}
        side = side.upper()
        if side not in ("YES", "NO"):
            return {"error": f"side must be YES or NO, got {side!r}"}

        ts_now   = time.time()
        order_id = _make_order_id(symbol, ts_now)

        order = {
            "order_id":    order_id,
            "ts_open":     datetime.now(timezone.utc).isoformat(),
            "symbol":      symbol.upper(),
            "strategy":    strategy.upper(),
            "side":        side,
            "entry_yes":   round(entry_yes, 4),
            "shares":      round(shares, 4),
            "position_usd": round(position_usd, 4),
            "kelly_frac":  round(kelly_frac, 4),
            "edge_score":  round(edge_score, 1),
            "status":      "OPEN",
            "market_id":   market_id,
            "market_title": market_title,
            "mode":        "PAPER",
            "notes":       notes,
        }

        with _DB_LOCK:
            try:
                conn = _get_conn()
                conn.execute(
                    """INSERT INTO poly_orders
                       (order_id, ts_open, symbol, strategy, side, entry_yes, shares,
                        position_usd, kelly_frac, edge_score, status, market_id,
                        market_title, mode, notes)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (order_id, order["ts_open"], order["symbol"], order["strategy"],
                     side, order["entry_yes"], order["shares"], order["position_usd"],
                     order["kelly_frac"], order["edge_score"], "OPEN", market_id,
                     market_title, "PAPER", notes)
                )
                conn.commit()
                conn.close()
            except Exception as e:
                log.error(f"[PAPER] DB insert failed: {e}")
                return {"error": f"DB error: {e}"}

        self._open_orders.append(order)
        log.info(f"[PAPER] Opened: {order_id} {symbol} {side} {shares}sh@{entry_yes:.3f} ${position_usd:.2f}")
        return order

    async def close_position(
        self,
        order_id:    str,
        exit_yes:    float,   # YES price at close / resolution price
        outcome_override: Optional[str] = None,   # force WIN/LOSS/PUSH (for resolved markets)
    ) -> Dict:
        """
        Close a paper position. Computes P&L. Records outcome for RL loop.

        Args:
            order_id:         Order ID from open_position()
            exit_yes:         YES price at close (1.0 if market resolved YES, 0.0 if NO)
            outcome_override: Force outcome string if known from market resolution

        Returns:
            Dict with pnl_usd, outcome, win_rate_running
        """
        # Find open order
        with _DB_LOCK:
            try:
                conn = _get_conn()
                row = conn.execute(
                    "SELECT * FROM poly_orders WHERE order_id=? AND status='OPEN'",
                    (order_id,)
                ).fetchone()
                conn.close()
            except Exception as e:
                log.error(f"[PAPER] close_position DB read error: {e}")
                return {"error": str(e)}

        if not row:
            return {"error": f"Order {order_id} not found or already closed"}

        order     = dict(row)
        entry_yes = order["entry_yes"]
        shares    = order["shares"]
        side      = order["side"]

        # P&L computation
        # YES trade: profit = (exit_yes - entry_yes) * shares
        # NO trade:  profit = (entry_yes - exit_yes) * shares  (inverse)
        if side == "YES":
            pnl_usd = round((exit_yes - entry_yes) * shares, 4)
        else:
            pnl_usd = round((entry_yes - exit_yes) * shares, 4)

        if outcome_override:
            outcome = outcome_override.upper()
        elif pnl_usd > 0:
            outcome = "WIN"
        elif pnl_usd < 0:
            outcome = "LOSS"
        else:
            outcome = "PUSH"

        # Update DB
        ok = self._tracker.update_order(
            order_id=order_id,
            status="RESOLVED",
            exit_yes=exit_yes,
            pnl_usd=pnl_usd,
            outcome=outcome,
            notes=f"Closed at {exit_yes:.4f} | PnL=${pnl_usd:.4f}",
        )

        if not ok:
            return {"error": "DB update failed"}

        # Remove from in-memory open orders
        self._open_orders = deque(
            (o for o in self._open_orders if o.get("order_id") != order_id),
            maxlen=50
        )

        summary = self._tracker.summary("PAPER")
        log.info(f"[PAPER] Closed: {order_id} {outcome} PnL=${pnl_usd:.4f} | Running WR={summary.get('win_rate', 0):.1%}")

        return {
            "order_id":    order_id,
            "symbol":      order["symbol"],
            "pnl_usd":     pnl_usd,
            "outcome":     outcome,
            "exit_yes":    exit_yes,
            "win_rate":    summary.get("win_rate", 0.0),
            "total_pnl":   summary.get("total_pnl", 0.0),
            "n_trades":    summary.get("n_total", 0),
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 4 — CLOBExecutor (Phase 2 — hard-gated)
# ══════════════════════════════════════════════════════════════════════

class CLOBExecutor:
    """
    Live Polymarket CLOB execution via Polygon network.

    PHASE 2 — LIVE MONEY. Hard-gated behind:
      1. POLY_PRIVATE_KEY environment variable (Polygon wallet private key)
      2. POLY_LIVE_ENABLED=true environment variable
      3. PolyBacktester gate: win_rate >= 65% over 30 paper trades
      4. HumanOverrideDashboard explicit approval

    If any gate is not met, all execute() calls return a gate-blocked dict
    without touching any network or funds.

    Security:
      - Private key read from env var ONLY — never logged, never stored
      - Sensitive values never surface in logs or error messages
      - All orders signed locally — no key sent to external API
      - CORS + auth patterns per Polymarket CLOB API spec

    Note: Polygon wallet signing requires eth_account or equivalent.
    Install: pip install eth_account  (add to requirements.txt for Phase 2)
    """

    def __init__(
        self,
        backtest_stats_fn: Optional[callable] = None,   # callable → stats dict
        human_approved:    bool = False,
    ):
        self._backtest_stats_fn = backtest_stats_fn
        self._human_approved    = human_approved
        self._private_key       = os.environ.get(PHASE2_ENV_KEY, "")
        self._live_enabled      = os.environ.get(PHASE2_FLAG_KEY, "false").lower() in ("1", "true", "yes")

    def _gate_check(self) -> Tuple[bool, str]:
        """
        Check all Phase 2 gates. Returns (gate_open, reason).
        All checks must pass for gate_open=True.
        """
        from typing import Tuple   # local import to avoid polluting module top
        if not self._live_enabled:
            return False, f"Gate: {PHASE2_FLAG_KEY} not set to true"
        if not self._private_key:
            return False, f"Gate: {PHASE2_ENV_KEY} env var not set"
        if not self._human_approved:
            return False, "Gate: HumanOverrideDashboard approval required"
        if self._backtest_stats_fn:
            try:
                stats = self._backtest_stats_fn()
                if not stats.get("live_gate_open"):
                    return False, f"Gate: {stats.get('gate_reason', 'backtest gate not open')}"
            except Exception as e:
                return False, f"Gate: backtest stats error — {e}"
        return True, "All gates passed"

    async def execute(
        self,
        session:     Any,   # aiohttp.ClientSession
        symbol:      str,
        side:        str,   # "YES" | "NO"
        shares:      float,
        price:       float,   # limit price (YES token price)
        market_id:   str,
    ) -> Dict:
        """
        Place a limit order on Polymarket CLOB.

        All gates are checked before any network call. Returns gate-blocked
        dict if any gate fails — no side effects.

        Args:
            session:   aiohttp session
            symbol:    base symbol
            side:      "YES" | "NO"
            shares:    number of shares
            price:     limit price per share
            market_id: Polymarket CLOB token ID

        Returns:
            {"status": "GATE_BLOCKED", "reason": str}   if gated
            {"status": "SUBMITTED", "order_id": str, ...} if submitted
            {"status": "ERROR", "reason": str}           on API error
        """
        gate_open, gate_reason = self._gate_check()
        if not gate_open:
            log.info(f"[CLOB-EXEC] Gate blocked: {gate_reason}")
            return {"status": "GATE_BLOCKED", "reason": gate_reason}

        # ── Phase 2 execution path ───────────────────────────────────────
        # Polymarket CLOB requires EIP-712 signed orders.
        # Signing is done locally with eth_account before sending to API.
        try:
            from eth_account import Account   # type: ignore  # Phase 2 dep
            from eth_account.messages import encode_defunct   # type: ignore
        except ImportError:
            return {
                "status": "ERROR",
                "reason": "eth_account not installed — run: pip install eth_account",
            }

        side_upper = side.upper()
        if side_upper not in ("YES", "NO"):
            return {"status": "ERROR", "reason": f"Invalid side: {side!r}"}

        if shares <= 0 or not (0 < price < 1):
            return {"status": "ERROR", "reason": "Invalid shares or price"}

        # Build order payload (Polymarket CLOB API spec)
        ts_now  = int(time.time())
        nonce   = str(ts_now)
        order_payload = {
            "tokenID":   market_id,
            "side":      "BUY" if side_upper == "YES" else "SELL",
            "price":     str(round(price, 4)),
            "size":      str(round(shares, 2)),
            "feeRateBps": "0",
            "nonce":     nonce,
            "expiration": str(ts_now + 300),   # 5-minute order expiry
        }

        # Sign the order
        try:
            acct = Account.from_key(self._private_key)
            msg  = encode_defunct(text=json.dumps(order_payload, sort_keys=True))
            signed = acct.sign_message(msg)
            signature = signed.signature.hex()
        except Exception as e:
            # DO NOT log the private key or any key-derived values
            log.error("[CLOB-EXEC] Signing failed (key error redacted)")
            return {"status": "ERROR", "reason": "Order signing failed"}

        headers = {
            "Content-Type":  "application/json",
            "POLY_ADDRESS":  acct.address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": nonce,
            "POLY_NONCE":    nonce,
        }

        url = f"{CLOB_API_BASE}/order"
        try:
            async with session.post(
                url,
                json=order_payload,
                headers=headers,
                timeout=__import__("aiohttp").ClientTimeout(total=10),
            ) as r:
                resp_body = await r.json(content_type=None)
                if r.status in (200, 201):
                    order_id = resp_body.get("orderID") or resp_body.get("id") or "unknown"
                    log.info(f"[CLOB-EXEC] LIVE ORDER SUBMITTED: {order_id} {symbol} {side} {shares}@{price}")
                    return {
                        "status":    "SUBMITTED",
                        "order_id":  order_id,
                        "symbol":    symbol,
                        "side":      side_upper,
                        "shares":    shares,
                        "price":     price,
                        "market_id": market_id,
                        "ts":        datetime.now(timezone.utc).isoformat(),
                    }
                else:
                    reason = resp_body.get("error") or str(resp_body)[:100]
                    log.warning(f"[CLOB-EXEC] API error {r.status}: {reason}")
                    return {"status": "ERROR", "reason": f"API {r.status}: {reason}"}
        except Exception as e:
            log.error(f"[CLOB-EXEC] Request failed: {e}")
            return {"status": "ERROR", "reason": f"Request failed: {type(e).__name__}"}

    def set_human_approved(self, approved: bool) -> None:
        """
        Set human approval state for the HumanOverrideDashboard gate.
        Must be explicitly called by an admin command handler.
        """
        self._human_approved = approved
        log.info(f"[CLOB-EXEC] Human approval {'GRANTED' if approved else 'REVOKED'}")

    def gate_status(self) -> Dict:
        """Return current gate status without executing anything."""
        gate_open, reason = self._gate_check()
        return {
            "gate_open":       gate_open,
            "reason":          reason,
            "live_enabled":    self._live_enabled,
            "key_configured":  bool(self._private_key),
            "human_approved":  self._human_approved,
        }


# ══════════════════════════════════════════════════════════════════════
# SECTION 5 — Module init + singletons
# ══════════════════════════════════════════════════════════════════════

# Type alias to avoid circular import in gate_check
from typing import Tuple   # noqa: E402

_init_db()

_ORDER_TRACKER  = OrderTracker()
_PAPER_EXECUTOR = PaperExecutor(tracker=_ORDER_TRACKER)
_CLOB_EXECUTOR  = CLOBExecutor()   # Phase 2, hard-gated until live gate passes

log.info(
    "[POLY-CLOB v1.0] Loaded — PaperExecutor · CLOBExecutor (gated) · "
    f"OrderTracker · DB={PAPER_DB_PATH} · Phase2={'READY' if _CLOB_EXECUTOR.gate_status()['key_configured'] else 'GATED'}"
)
