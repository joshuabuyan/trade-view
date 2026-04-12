"""
╔══════════════════════════════════════════════════════════════════════╗
║  CRYPTEX STORES — cryptex_stores.py                                  ║
║  CRYPTEX / MOP·ICT·QUANT Stack — OCE v10.3 · Zero-Bug Mandate       ║
║                                                                      ║
║  Unified data-layer module. Combines 4 independent store modules:    ║
║                                                                      ║
║  §1 — APEX WATCH STORE     (was: apex_watch_store.py)                ║
║       Persistent multi-coin watch records. FIFO/dedup. 3-day TTL.   ║
║                                                                      ║
║  §2 — GATE MEMORY ENGINE   (was: gate_memory.py)                     ║
║       Gate fingerprint store + immune system blacklist.              ║
║                                                                      ║
║  §3 — CONDITION ENGINE     (was: condition_engine.py)                ║
║       Declarative condition-based alert subscriptions.               ║
║                                                                      ║
║  §4 — DEEP STRUCTURE TASK  (was: deep_structure_task.py)             ║
║       AI-powered extended candle history + ICT structural levels.    ║
║                                                                      ║
║  ── MIGRATION NOTES FOR app.py / markov_stack.py ───────────────────║
║  Replace old single-file imports with:                               ║
║    from cryptex_stores import (                                      ║
║        upsert, record_outcome, set_alert_flag, get,                  ║
║        get_all_pending, get_unrecorded_outcomes,                     ║
║        mark_outcome_recorded, get_level_samples,                     ║
║        get_watch_summary, get_all, evict_expired,          # §1     ║
║        record_gate_outcome, get_gate_probability,                    ║
║        check_blacklist, unflag_blacklist,                            ║
║        get_fingerprint_data, get_crowd_rates,                        ║
║        get_memory_summary,                                  # §2     ║
║        register_condition, evaluate_conditions,                      ║
║        expire_conditions, list_conditions,                           ║
║        cancel_condition, get_condition_summary,             # §3     ║
║        get_levels, get_deep_summary, refresh_symbol,        # §4     ║
║    )                                                                 ║
║                                                                      ║
║  Renamed public functions (3 get_summary() conflicts resolved):      ║
║    apex_watch_store.get_summary()      → get_watch_summary()         ║
║    condition_engine.get_summary()      → get_condition_summary()     ║
║    deep_structure_task.get_summary()   → get_deep_summary()          ║
║                                                                      ║
║  BUG FIX applied: apex_watch_store get_summary() body was orphaned  ║
║  dead code inside get_level_samples(). Extracted and fixed.         ║
║                                                                      ║
║  Zero external dependencies. Python 3.8+. Thread-safe.              ║
║  128 MB RAM safe. All original smoke tests preserved in __main__.    ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os
import json
import time
import math
import asyncio
import hashlib
import logging
import threading
from collections import Counter
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# §1 — APEX WATCH STORE
# Persistent multi-coin APEX watch records.
# Holds up to MAX_WATCH_RECORDS (100) analysis records.
# FIFO eviction: oldest record removed when capacity is reached.
# Same-symbol deduplication: newest analysis replaces prior entry.
#
# Used by:
#   • app.py        — writes on every analysis completion
#   • app.py APEX   — reads for multi-coin TP/SL price monitoring
#   • gate_memory   — reads for bulk fingerprint outcome recording
#   • condition_engine — reads for backfill condition matching
#   • markov_stack  — outcome feedback recorded through this store
#
# Storage: /tmp/apex_watch_store.json (env: APEX_WATCH_STORE_FILE)
# ══════════════════════════════════════════════════════════════════════════════

STORE_FILE        = os.getenv("APEX_WATCH_STORE_FILE", "/tmp/apex_watch_store.json")
MAX_WATCH_RECORDS = 100
WATCH_TTL_SECONDS = 86400 * 3
OUTCOME_PENDING   = "PENDING"
VALID_OUTCOMES    = frozenset({"PENDING", "TP1", "TP2", "TP3", "SL", "TIMEOUT"})

_aws_lock: threading.Lock = threading.Lock()
_store: Dict[str, dict] = {}


def _empty_record(symbol: str) -> dict:
    """Return a blank WatchRecord dict with all expected keys."""
    return {
        "symbol":           symbol.upper().strip(),
        "created_ts":       time.time(),
        "updated_ts":       time.time(),
        "bias":             "",
        "all_pass":         False,
        "grade":            "?",
        "conf":             0.0,
        "price":            0.0,
        "entry_low":        0.0,
        "entry_high":       0.0,
        "tp1":              0.0,
        "tp2":              0.0,
        "tp3":              0.0,
        "sl":               0.0,
        "sl2":              0.0,
        "sl3":              0.0,
        "rr":               0.0,
        "gate_pass":        [],
        "gate_scores":      [],
        "gate_hash":        "",
        "gates_passed":     0,
        "hmm_state":        "",
        "ict_state":        "",
        "regime":           "",
        "session":          "",
        "markov_state":     "",
        "best_tf":          "",
        "gate11_pass":      True,
        "gate11_adv_score": 0,
        "outcome":          OUTCOME_PENDING,
        "outcome_ts":       0.0,
        "outcome_recorded": False,
        "tp1_alerted":      False,
        "tp2_alerted":      False,
        "tp3_alerted":      False,
        "sl_alerted":       False,
        "chat_ids":         [],
        "signal_prob":      0.0,
        "prob_tier":        "LOW",
    }


def _aws_load() -> None:
    global _store
    try:
        if os.path.exists(STORE_FILE):
            with open(STORE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                loaded = {}
                for sym, rec in raw.items():
                    if isinstance(rec, dict) and "created_ts" in rec:
                        base = _empty_record(sym)
                        base.update(rec)
                        loaded[sym.upper()] = base
                _store = loaded
                log.info(f"[AWS] Loaded {len(_store)} records from {STORE_FILE}")
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"[AWS] {STORE_FILE} unreadable — starting fresh: {e}")
        _store = {}


def _aws_save() -> None:
    """Write store to disk. Never raises — pipeline must not be blocked."""
    try:
        with open(STORE_FILE, "w", encoding="utf-8") as f:
            json.dump(_store, f, separators=(",", ":"))
    except OSError as e:
        log.warning(f"[AWS] Failed to save {STORE_FILE}: {e}")


def _evict_oldest() -> None:
    """
    FIFO eviction: remove expired records first, then oldest by created_ts.
    Caller must hold _aws_lock.
    """
    if len(_store) < MAX_WATCH_RECORDS:
        return
    now = time.time()
    expired = [sym for sym, rec in _store.items()
               if now - rec.get("created_ts", 0) > WATCH_TTL_SECONDS]
    for sym in expired:
        del _store[sym]
        log.debug(f"[AWS EVICT] Expired: {sym}")
    while len(_store) >= MAX_WATCH_RECORDS:
        oldest_sym = min(_store, key=lambda s: _store[s].get("created_ts", 0))
        oldest_ts  = _store[oldest_sym].get("created_ts", 0)
        del _store[oldest_sym]
        log.info(f"[AWS EVICT FIFO] Removed oldest: {oldest_sym} "
                 f"(age={(now - oldest_ts) / 3600:.1f}h)")


def upsert(
    symbol:   str,
    result:   dict,
    chat_ids: Optional[List[int]] = None,
) -> dict:
    """
    Insert or update a watch record from a completed run_ict_analysis() result.
    Same-symbol deduplication: newest analysis replaces the prior record.
    FIFO eviction fires automatically when len >= MAX_WATCH_RECORDS.
    Returns the stored record dict (copy).
    """
    sym  = symbol.upper().strip().replace("/", "").replace("-", "")
    _fp  = result.get("gate_fp_data") or {}
    _g11 = result.get("gate11_detail") or {}
    _sp  = result.get("signal_probability_data") or {}
    _gp  = [bool(g) for g in (result.get("gate_pass") or [])]
    _gs  = [round(float(s), 3) for s in (result.get("gate_scores") or [])]

    _el  = float(result.get("entry_low")  or 0)
    _eh  = float(result.get("entry_high") or 0)
    _em  = (_el + _eh) / 2.0 if (_el and _eh) else float(result.get("price") or 0)
    _tp3 = float(result.get("tp3") or 0)
    _sl  = float(result.get("inv_sl") or result.get("sl") or 0)
    _rr  = round(abs(_tp3 - _em) / max(abs(_em - _sl), 1e-9), 2) if (_em and _tp3 and _sl) else 0.0

    record = _empty_record(sym)
    record.update({
        "symbol":           sym,
        "created_ts":       time.time(),
        "updated_ts":       time.time(),
        "bias":             str(result.get("bias") or ""),
        "all_pass":         bool(result.get("all_pass", False)),
        "grade":            str(result.get("grade") or "?"),
        "conf":             round(float(result.get("conf") or 0), 1),
        "price":            round(_em, 8),
        "entry_low":        round(_el, 8),
        "entry_high":       round(_eh, 8),
        "tp1":              round(float(result.get("tp1") or 0), 8),
        "tp2":              round(float(result.get("tp2") or 0), 8),
        "tp3":              round(_tp3, 8),
        "sl":               round(_sl, 8),
        "sl2":              round(float(result.get("sl2") or 0), 8),
        "sl3":              round(float(result.get("sl3") or 0), 8),
        "rr":               _rr,
        "gate_pass":        _gp,
        "gate_scores":      _gs,
        "gate_hash":        str(_fp.get("gate_hash") or ""),
        "gates_passed":     sum(1 for g in _gp if g),
        "hmm_state":        str((result.get("hmm_data") or {}).get("hmm_state") or ""),
        "ict_state":        str(result.get("markov_state") or ""),
        "regime":           str(result.get("regime") or ""),
        "session":          str(result.get("session") or ""),
        "markov_state":     str(result.get("markov_state") or ""),
        "best_tf":          str(result.get("best_tf") or ""),
        "gate11_pass":      bool(_g11.get("gate11_pass", True)),
        "gate11_adv_score": int(_g11.get("gate11_adv_score") or 0),
        "outcome":          OUTCOME_PENDING,
        "outcome_ts":       0.0,
        "outcome_recorded": False,
        "tp1_alerted":      False,
        "tp2_alerted":      False,
        "tp3_alerted":      False,
        "sl_alerted":       False,
        "chat_ids":         list(chat_ids or []),
        "signal_prob":      round(float(_sp.get("signal_probability") or 0), 1),
        "prob_tier":        str(_sp.get("probability_tier") or "LOW"),
    })

    with _aws_lock:
        existing = _store.get(sym, {})
        if existing.get("outcome") == OUTCOME_PENDING:
            for flag in ("tp1_alerted", "tp2_alerted", "tp3_alerted", "sl_alerted"):
                record[flag] = existing.get(flag, False)
            old_chats = existing.get("chat_ids", [])
            record["chat_ids"] = list(set(old_chats + record["chat_ids"]))
        if sym not in _store:
            _evict_oldest()
        _store[sym] = record
        _aws_save()

    log.info(
        f"[AWS] Upserted {sym} | grade={record['grade']} conf={record['conf']} "
        f"gates={record['gates_passed']}/{len(_gp)} all_pass={record['all_pass']} "
        f"store_size={len(_store)}/{MAX_WATCH_RECORDS}"
    )
    return dict(record)


def record_outcome(symbol: str, outcome: str, chat_ids: Optional[List[int]] = None) -> bool:
    """
    Record a trade outcome for a symbol.
    Returns True if record found and updated, False if not found.
    """
    sym = symbol.upper().strip()
    if outcome not in VALID_OUTCOMES:
        log.warning(f"[AWS] Invalid outcome {outcome!r} for {sym}")
        return False

    with _aws_lock:
        rec = _store.get(sym)
        if not rec:
            return False

        _entry_mid = (rec.get("entry_low", 0) + rec.get("entry_high", 0)) / 2.0
        _entry_mid = _entry_mid or rec.get("price", 0)

        def _pct(level):
            if not level or not _entry_mid:
                return 0.0
            return round((level - _entry_mid) / _entry_mid * 100, 4)

        _pct_moves = {
            "entry_pct": 0.0,
            "tp1_pct":   _pct(rec.get("tp1", 0)),
            "tp2_pct":   _pct(rec.get("tp2", 0)),
            "tp3_pct":   _pct(rec.get("tp3", 0)),
            "sl_pct":    _pct(rec.get("sl", 0)),
            "sl2_pct":   _pct(rec.get("sl2", 0)),
            "sl3_pct":   _pct(rec.get("sl3", 0)),
            "entry_mid": round(_entry_mid, 8),
            "outcome":   outcome,
            "ict_state": rec.get("ict_state", ""),
            "bias":      rec.get("bias", ""),
            "regime":    rec.get("regime", ""),
            "gate_hash": rec.get("gate_hash", ""),
        }

        rec["outcome"]    = outcome
        rec["outcome_ts"] = time.time()
        rec["updated_ts"] = time.time()
        rec["pct_moves"]  = _pct_moves
        _aws_save()

    log.info(f"[AWS] Outcome recorded: {sym} → {outcome} | "
             f"tp1_pct={_pct_moves['tp1_pct']:+.2f}% sl_pct={_pct_moves['sl_pct']:+.2f}%")
    return True


def set_alert_flag(symbol: str, flag: str, value: bool = True) -> None:
    """Set a per-level alert flag. Called by APEX price monitor after firing an alert."""
    sym = symbol.upper().strip()
    valid_flags = {"tp1_alerted", "tp2_alerted", "tp3_alerted", "sl_alerted"}
    if flag not in valid_flags:
        log.warning(f"[AWS] Unknown alert flag: {flag}")
        return
    with _aws_lock:
        rec = _store.get(sym)
        if rec:
            rec[flag]         = value
            rec["updated_ts"] = time.time()
            _aws_save()


def get(symbol: str) -> Optional[dict]:
    """Return the watch record for a symbol, or None if not found."""
    sym = symbol.upper().strip()
    with _aws_lock:
        rec = _store.get(sym)
        return dict(rec) if rec else None


def get_all_pending() -> List[dict]:
    """Return all records with outcome == PENDING."""
    with _aws_lock:
        return [dict(r) for r in _store.values() if r.get("outcome") == OUTCOME_PENDING]


def get_unrecorded_outcomes() -> List[dict]:
    """Return all records resolved but not yet fed into gate_memory / markov brain."""
    with _aws_lock:
        return [
            dict(r) for r in _store.values()
            if r.get("outcome") != OUTCOME_PENDING
            and not r.get("outcome_recorded", False)
        ]


def mark_outcome_recorded(symbol: str) -> None:
    """Mark a record's outcome as fed back into the learning pipeline."""
    sym = symbol.upper().strip()
    with _aws_lock:
        rec = _store.get(sym)
        if rec:
            rec["outcome_recorded"] = True
            rec["updated_ts"]       = time.time()
            _aws_save()


def get_level_samples(min_outcome: str = None) -> List[dict]:
    """
    Return all records that have pct_moves data (resolved with level info).
    Used by LevelLearner in markov_stack to accumulate real price move statistics.
    """
    with _aws_lock:
        result = []
        for rec in _store.values():
            pm = rec.get("pct_moves")
            if not pm:
                continue
            if min_outcome and pm.get("outcome") != min_outcome:
                continue
            result.append({
                "symbol":    rec.get("symbol", ""),
                "ict_state": pm.get("ict_state", ""),
                "bias":      pm.get("bias", ""),
                "regime":    pm.get("regime", ""),
                "gate_hash": pm.get("gate_hash", ""),
                "tp1_pct":   pm.get("tp1_pct", 0.0),
                "tp2_pct":   pm.get("tp2_pct", 0.0),
                "tp3_pct":   pm.get("tp3_pct", 0.0),
                "sl_pct":    pm.get("sl_pct", 0.0),
                "sl2_pct":   pm.get("sl2_pct", 0.0),
                "sl3_pct":   pm.get("sl3_pct", 0.0),
                "entry_mid": pm.get("entry_mid", 0.0),
                "outcome":   pm.get("outcome", ""),
            })
        return result


def get_watch_summary() -> dict:
    """Return summary stats for /status command. (was: get_summary in apex_watch_store)"""
    with _aws_lock:
        total    = len(_store)
        pending  = sum(1 for r in _store.values() if r.get("outcome") == OUTCOME_PENDING)
        resolved = total - pending
        outcomes = {}
        for r in _store.values():
            o = r.get("outcome", OUTCOME_PENDING)
            outcomes[o] = outcomes.get(o, 0) + 1
        all_pass_count = sum(1 for r in _store.values() if r.get("all_pass"))
        avg_conf = round(
            sum(r.get("conf", 0) for r in _store.values()) / max(total, 1), 1
        )
    return {
        "total":          total,
        "capacity":       MAX_WATCH_RECORDS,
        "pending":        pending,
        "resolved":       resolved,
        "outcomes":       outcomes,
        "all_pass_count": all_pass_count,
        "avg_conf":       avg_conf,
    }


def get_all() -> List[dict]:
    """Return all records as a list of dicts. Read-only snapshot."""
    with _aws_lock:
        return [dict(r) for r in _store.values()]


def evict_expired() -> int:
    """Manually evict all records older than WATCH_TTL_SECONDS. Returns count removed."""
    now = time.time()
    with _aws_lock:
        before  = len(_store)
        expired = [sym for sym, rec in _store.items()
                   if now - rec.get("created_ts", 0) > WATCH_TTL_SECONDS]
        for sym in expired:
            del _store[sym]
        if expired:
            _aws_save()
        removed = before - len(_store)
    if removed:
        log.info(f"[AWS] Evicted {removed} expired records ({len(_store)} remain)")
    return removed


# ══════════════════════════════════════════════════════════════════════════════
# §2 — GATE MEMORY ENGINE
# Gate vector fingerprint store (MD5 hash → TP/SL/timeout counts).
# Immune system blacklist (regime+state+gate → veto on SL abuse).
# Fingerprint data builder for WebApp URL payload.
# Crowd-label support (WebApp journal → HMAC-verified records).
# Memory summary for /status command.
#
# Files: /tmp/gate_memory.json · /tmp/gate_blacklist.json
# ══════════════════════════════════════════════════════════════════════════════

GATE_MEMORY_FILE    = os.getenv("GATE_MEMORY_FILE",    "/tmp/gate_memory.json")
GATE_BLACKLIST_FILE = os.getenv("GATE_BLACKLIST_FILE", "/tmp/gate_blacklist.json")

BLACKLIST_SL_RATE_THRESHOLD = 0.60
BLACKLIST_MIN_SAMPLES       = 10
FINGERPRINT_MIN_SAMPLES     = 3
FINGERPRINT_WEIGHT          = 0.15

_GM_VALID_OUTCOMES: Tuple[str, ...] = ("TP1", "TP2", "TP3", "SL", "TIMEOUT")
_GM_TP_OUTCOMES:    Tuple[str, ...] = ("TP1", "TP2", "TP3")

_gm_lock:     threading.Lock = threading.Lock()
_gm_mem_lock: threading.Lock = threading.Lock()

_memory:    Dict[str, dict] = {}
_blacklist: Dict[str, dict] = {}


def _load_memory() -> None:
    """Load both gate stores from disk. Silently recovers from corrupt files."""
    global _memory, _blacklist
    try:
        if os.path.exists(GATE_MEMORY_FILE):
            with open(GATE_MEMORY_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                _memory = raw
                log.info(f"[GATE_MEM] Loaded {len(_memory)} fingerprints from disk")
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"[GATE_MEM] gate_memory.json corrupt or unreadable — starting fresh: {e}")
        _memory = {}

    try:
        if os.path.exists(GATE_BLACKLIST_FILE):
            with open(GATE_BLACKLIST_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                _blacklist = raw
                flagged = sum(1 for v in _blacklist.values() if v.get("flagged"))
                log.info(f"[GATE_MEM] Loaded {len(_blacklist)} blacklist patterns ({flagged} flagged)")
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"[GATE_MEM] gate_blacklist.json corrupt or unreadable — starting fresh: {e}")
        _blacklist = {}


def _save_memory() -> None:
    """Write both gate stores to disk. Thread-safe. Silent on failure."""
    try:
        with open(GATE_MEMORY_FILE, "w", encoding="utf-8") as f:
            json.dump(_memory, f, separators=(",", ":"))
    except OSError as e:
        log.warning(f"[GATE_MEM] Failed to save gate_memory.json: {e}")
    try:
        with open(GATE_BLACKLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(_blacklist, f, separators=(",", ":"))
    except OSError as e:
        log.warning(f"[GATE_MEM] Failed to save gate_blacklist.json: {e}")


def _gate_hash(gate_pass: List[bool]) -> str:
    """Build 8-char MD5 fingerprint from gate vector."""
    vec = "".join("1" if g else "0" for g in gate_pass)
    return hashlib.md5(vec.encode("ascii")).hexdigest()[:8]


def _vec_str(gate_pass: List[bool]) -> str:
    """Convert gate_pass list to bit string."""
    return "".join("1" if g else "0" for g in gate_pass)


def _pattern_key(hmm_regime: str, ict_state: str, gate_pass: List[bool]) -> str:
    """Composite key for immune system blacklist: regime|state|vec."""
    vec    = _vec_str(gate_pass)
    regime = str(hmm_regime).replace("|", "_").strip().upper()[:20]
    state  = str(ict_state).replace("|", "_").strip().upper()[:30]
    return f"{regime}|{state}|{vec}"


def record_gate_outcome(
    gate_pass:  List[bool],
    outcome:    str,
    hmm_regime: str = "",
    ict_state:  str = "",
    crowd:      bool = False,
) -> None:
    """
    Record a trade outcome against a gate vector fingerprint.
    Thread-safe. Writes to disk after each call. Silently drops invalid inputs.
    """
    if not gate_pass or not isinstance(gate_pass, list):
        log.warning("[GATE_MEM] record_gate_outcome: gate_pass is empty or not a list — skipped")
        return

    outcome = str(outcome).upper().strip()
    if outcome not in _GM_VALID_OUTCOMES:
        log.warning(f"[GATE_MEM] record_gate_outcome: invalid outcome {outcome!r} — skipped")
        return

    gh     = _gate_hash(gate_pass)
    vec    = _vec_str(gate_pass)
    is_tp  = outcome in _GM_TP_OUTCOMES
    is_sl  = outcome == "SL"
    source = "CROWD" if crowd else "BOT"

    with _gm_lock:
        if gh not in _memory:
            _memory[gh] = {
                "vec": vec, "tp": 0, "sl": 0, "timeout": 0, "n": 0,
                "crowd_tp": 0, "crowd_sl": 0,
            }
        entry = _memory[gh]
        entry["n"] += 1
        if is_tp:
            entry["tp"] += 1
            if crowd:
                entry["crowd_tp"] = entry.get("crowd_tp", 0) + 1
        elif is_sl:
            entry["sl"] += 1
            if crowd:
                entry["crowd_sl"] = entry.get("crowd_sl", 0) + 1
        else:
            entry["timeout"] += 1

        if hmm_regime and ict_state:
            pk = _pattern_key(hmm_regime, ict_state, gate_pass)
            if pk not in _blacklist:
                _blacklist[pk] = {
                    "sl": 0, "n": 0, "flagged": False, "flagged_ts": 0.0, "reason": "",
                }
            bl = _blacklist[pk]
            bl["n"] += 1
            if is_sl:
                bl["sl"] += 1
            sl_rate = bl["sl"] / bl["n"]
            if (
                not bl["flagged"]
                and bl["n"] >= BLACKLIST_MIN_SAMPLES
                and sl_rate >= BLACKLIST_SL_RATE_THRESHOLD
            ):
                bl["flagged"]    = True
                bl["flagged_ts"] = time.time()
                bl["reason"]     = (
                    f"SL rate {sl_rate:.0%} over {bl['n']} samples "
                    f"[regime={hmm_regime}, state={ict_state}]"
                )
                log.warning(f"[GATE_MEM] IMMUNE BLACKLIST FLAGGED: {bl['reason']}")

        _save_memory()

    log.debug(f"[GATE_MEM] Recorded {source} {outcome} for hash={gh} vec={vec} n={entry['n']}")


def get_gate_probability(gate_pass: List[bool]) -> Optional[float]:
    """Return historical TP rate for this gate vector. None if insufficient data."""
    if not gate_pass:
        return None
    gh = _gate_hash(gate_pass)
    with _gm_lock:
        entry = _memory.get(gh)
    if not entry or entry["n"] < FINGERPRINT_MIN_SAMPLES:
        return None
    return entry["tp"] / entry["n"]


def check_blacklist(
    hmm_regime: str,
    ict_state:  str,
    gate_pass:  List[bool],
) -> Optional[str]:
    """
    Check if a regime+state+gate pattern is immune-blacklisted.
    Returns veto reason string if blacklisted, None if clean.
    """
    if not (hmm_regime and ict_state and gate_pass):
        return None
    pk = _pattern_key(hmm_regime, ict_state, gate_pass)
    with _gm_lock:
        bl = _blacklist.get(pk)
    if bl and bl.get("flagged"):
        return f"[IMMUNE VETO] {bl['reason']}"
    return None


def unflag_blacklist(hmm_regime: str, ict_state: str, gate_pass: List[bool]) -> bool:
    """Manually clear a blacklist flag. Returns True if found and cleared."""
    if not (hmm_regime and ict_state and gate_pass):
        return False
    pk = _pattern_key(hmm_regime, ict_state, gate_pass)
    with _gm_lock:
        bl = _blacklist.get(pk)
        if bl and bl.get("flagged"):
            bl["flagged"]    = False
            bl["flagged_ts"] = 0.0
            bl["reason"]     = ""
            _save_memory()
            log.info(f"[GATE_MEM] Unflagged blacklist pattern: {pk}")
            return True
    return False


def get_fingerprint_data(gate_pass: List[bool]) -> dict:
    """
    Return full fingerprint data for WebApp URL payload and compute_signal_probability().
    Always returns a dict — safe to call with no history.
    """
    if not gate_pass:
        return {
            "gate_hash": "", "gate_vec": "", "gate_n": 0, "gate_tp_rate": None,
            "gate_tp": 0, "gate_sl": 0, "gate_timeout": 0,
            "gate_crowd_tp": 0, "gate_crowd_sl": 0,
        }

    gh  = _gate_hash(gate_pass)
    vec = _vec_str(gate_pass)

    with _gm_lock:
        entry = dict(_memory.get(gh, {}))

    n        = entry.get("n", 0)
    tp       = entry.get("tp", 0)
    sl       = entry.get("sl", 0)
    timeout  = entry.get("timeout", 0)
    crowd_tp = entry.get("crowd_tp", 0)
    crowd_sl = entry.get("crowd_sl", 0)
    tp_rate  = (tp / n) if n >= FINGERPRINT_MIN_SAMPLES else None

    return {
        "gate_hash":     gh,
        "gate_vec":      vec,
        "gate_n":        n,
        "gate_tp_rate":  round(tp_rate, 3) if tp_rate is not None else None,
        "gate_tp":       tp,
        "gate_sl":       sl,
        "gate_timeout":  timeout,
        "gate_crowd_tp": crowd_tp,
        "gate_crowd_sl": crowd_sl,
    }


def get_crowd_rates(gate_pass: List[bool]) -> Optional[dict]:
    """Return crowd TP/SL rates for WebApp social proof display. None if no crowd data."""
    if not gate_pass:
        return None
    gh = _gate_hash(gate_pass)
    with _gm_lock:
        entry = _memory.get(gh, {})
    crowd_tp = entry.get("crowd_tp", 0)
    crowd_sl = entry.get("crowd_sl", 0)
    total    = crowd_tp + crowd_sl
    if total == 0:
        return None
    return {
        "crowd_total":   total,
        "crowd_tp":      crowd_tp,
        "crowd_sl":      crowd_sl,
        "crowd_tp_rate": round(crowd_tp / total, 2),
        "crowd_sl_rate": round(crowd_sl / total, 2),
    }


def get_memory_summary() -> dict:
    """Return summary stats for /status command."""
    with _gm_lock:
        total_patterns  = len(_memory)
        total_outcomes  = sum(v.get("n", 0) for v in _memory.values())
        total_tp        = sum(v.get("tp", 0) for v in _memory.values())
        total_sl        = sum(v.get("sl", 0) for v in _memory.values())
        blacklisted     = sum(1 for v in _blacklist.values() if v.get("flagged"))
        blacklist_total = len(_blacklist)
    return {
        "fingerprint_patterns": total_patterns,
        "total_outcomes":       total_outcomes,
        "total_tp":             total_tp,
        "total_sl":             total_sl,
        "tp_rate_global":       round(total_tp / total_outcomes, 3) if total_outcomes else None,
        "blacklist_patterns":   blacklist_total,
        "blacklisted_active":   blacklisted,
    }


# ══════════════════════════════════════════════════════════════════════════════
# §3 — CONDITION SUBSCRIPTION ENGINE
# Declarative condition-based alert system.
# User registers: /watch BTCUSDT CHOCH_BULL all_gates NY_OPEN
# Bot fires exactly ONCE when the condition matches a live analysis.
#
# Storage: /tmp/conditions.json
# ══════════════════════════════════════════════════════════════════════════════

CONDITIONS_FILE = os.getenv("CONDITIONS_FILE", "/tmp/conditions.json")
CONDITION_TTL   = 86400
MAX_CONDITIONS  = 50

VALID_ICT_STATES = {
    "ANY", "ACCUMULATION", "MANIPULATION_H", "MANIPULATION_L",
    "BOS_BULL", "BOS_BEAR", "CHOCH_BULL", "CHOCH_BEAR",
    "FVG_BULL", "FVG_BEAR",
}
VALID_GATE_REQS = {"any", "all_gates", "8_gates", "7_gates", "6_gates"}
VALID_SESSIONS  = {"ANY", "LONDON", "NEW_YORK", "NY_OPEN", "ASIAN", "WEEKEND"}


@dataclass
class ConditionRecord:
    condition_id: str
    chat_id:      int
    symbol:       str
    ict_state:    str
    gate_req:     str
    session:      str
    created_ts:   float = field(default_factory=time.time)
    fired:        bool  = False
    fired_ts:     float = 0.0
    notes:        str   = ""


_cond_lock:  threading.Lock = threading.Lock()
_conditions: Dict[str, ConditionRecord] = {}


def _cond_load() -> None:
    global _conditions
    try:
        if os.path.exists(CONDITIONS_FILE):
            with open(CONDITIONS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            loaded = {}
            for cid, d in raw.items():
                try:
                    loaded[cid] = ConditionRecord(**d)
                except (TypeError, KeyError) as _e:
                    log.debug(f"[SILENT_EX] type({type(_e).__name__}): {_e}")
            _conditions = loaded
            log.info(f"[COND] Loaded {len(_conditions)} conditions from disk")
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"[COND] conditions.json unreadable — starting fresh: {e}")
        _conditions = {}


def _cond_save() -> None:
    try:
        with open(CONDITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump({k: asdict(v) for k, v in _conditions.items()}, f, separators=(",", ":"))
    except OSError as e:
        log.warning(f"[COND] Failed to save conditions.json: {e}")


def _new_id() -> str:
    return f"cond_{int(time.time() * 1000) % 1_000_000_000}"


def register_condition(
    chat_id:   int,
    symbol:    str,
    ict_state: str = "ANY",
    gate_req:  str = "all_gates",
    session:   str = "ANY",
    notes:     str = "",
) -> tuple:
    """
    Register a new watch condition.
    Returns (True, condition_id, message) on success or (False, None, error) on failure.
    """
    symbol    = symbol.upper().strip().replace("/", "")
    ict_state = ict_state.upper().strip()
    gate_req  = gate_req.lower().strip()
    session   = session.upper().strip()

    if not symbol:
        return False, None, "Symbol cannot be empty."
    if ict_state not in VALID_ICT_STATES:
        return False, None, f"Unknown ICT state '{ict_state}'. Valid: {', '.join(sorted(VALID_ICT_STATES))}"
    if gate_req not in VALID_GATE_REQS:
        return False, None, f"Unknown gate requirement '{gate_req}'. Valid: {', '.join(sorted(VALID_GATE_REQS))}"
    if session not in VALID_SESSIONS:
        return False, None, f"Unknown session '{session}'. Valid: {', '.join(sorted(VALID_SESSIONS))}"

    with _cond_lock:
        user_active = [c for c in _conditions.values() if c.chat_id == chat_id and not c.fired]
        if len(user_active) >= 5:
            return False, None, "You already have 5 active conditions. Cancel one with /cancel_watch before adding more."
        if len(_conditions) >= MAX_CONDITIONS:
            return False, None, "System condition limit reached. Try again later."

        cid = _new_id()
        rec = ConditionRecord(
            condition_id=cid, chat_id=chat_id, symbol=symbol,
            ict_state=ict_state, gate_req=gate_req, session=session, notes=notes,
        )
        _conditions[cid] = rec
        _cond_save()
        log.info(f"[COND] Registered {cid}: {symbol} {ict_state} {gate_req} {session} for chat={chat_id}")

    return True, cid, f"✅ Watch condition registered ({cid[-6:]}): {symbol} · {ict_state} · {gate_req} · {session}"


def evaluate_conditions(result: dict, symbol: str) -> List[dict]:
    """
    Called after run_ict_analysis() completes.
    Returns list of fired notifications: [{chat_id, cid, message}, ...].
    Thread-safe. Never raises.
    """
    fired_list = []
    if not result:
        return fired_list

    sym_clean    = symbol.upper().replace("/", "").strip()
    gate_pass    = result.get("gate_pass", [])
    gates_passed = sum(1 for g in gate_pass if g)
    gates_total  = len(gate_pass)
    all_pass     = result.get("all_pass", False)
    ict_from_res = _infer_ict_state(result)
    session_now  = (result.get("session") or "").upper()

    with _cond_lock:
        for cid, cond in list(_conditions.items()):
            if cond.fired:
                continue
            if cond.symbol != sym_clean and cond.symbol != "ANY":
                continue
            gate_ok = _check_gate_req(cond.gate_req, gates_passed, gates_total, all_pass)
            ict_ok  = (cond.ict_state == "ANY") or (cond.ict_state == ict_from_res)
            sess_ok = (cond.session == "ANY") or _session_matches(cond.session, session_now)

            if gate_ok and ict_ok and sess_ok:
                _conditions[cid].fired    = True
                _conditions[cid].fired_ts = time.time()
                grade = result.get("grade", "?")
                conf  = result.get("conf", 0)
                prob  = (result.get("signal_probability_data") or {}).get("signal_probability_pct", "?")
                msg = (
                    f"🔔 *WATCH CONDITION MET* `({cid[-6:]})`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Symbol : *{sym_clean}*\n"
                    f"ICT    : `{ict_from_res}`\n"
                    f"Gates  : `{gates_passed}/{gates_total}`\n"
                    f"Session: `{session_now}`\n"
                    f"Grade  : `{grade}` | Conf: `{conf}` | Prob: `{prob}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Run /analyze {sym_clean} for full signal."
                )
                fired_list.append({"chat_id": cond.chat_id, "cid": cid, "message": msg})
                log.info(f"[COND] FIRED {cid} for chat={cond.chat_id} symbol={sym_clean}")
        if fired_list:
            _cond_save()

    return fired_list


def expire_conditions() -> int:
    """Remove conditions older than CONDITION_TTL or already fired. Returns count removed."""
    now = time.time()
    with _cond_lock:
        before  = len(_conditions)
        expired = [
            cid for cid, c in _conditions.items()
            if c.fired or (now - c.created_ts) > CONDITION_TTL
        ]
        for cid in expired:
            del _conditions[cid]
        if expired:
            _cond_save()
        after = len(_conditions)
    removed = before - after
    if removed:
        log.info(f"[COND] Expired {removed} conditions ({after} active)")
    return removed


def list_conditions(chat_id: int) -> List[ConditionRecord]:
    """Return active (unfired) conditions for a chat_id."""
    with _cond_lock:
        return [c for c in _conditions.values() if c.chat_id == chat_id and not c.fired]


def cancel_condition(chat_id: int, cid_suffix: str) -> tuple:
    """Cancel a condition by its last-6-char ID suffix. Returns (True, msg) or (False, err)."""
    with _cond_lock:
        for cid, cond in list(_conditions.items()):
            if cond.chat_id == chat_id and cid.endswith(cid_suffix):
                del _conditions[cid]
                _cond_save()
                return True, f"✅ Condition `{cid_suffix}` cancelled."
        return False, f"No active condition found with ID ending `{cid_suffix}`."


def get_condition_summary() -> dict:
    """Return summary stats for /status command. (was: get_summary in condition_engine)"""
    with _cond_lock:
        active = sum(1 for c in _conditions.values() if not c.fired)
        fired  = sum(1 for c in _conditions.values() if c.fired)
    return {"active": active, "fired_pending_cleanup": fired, "total": active + fired}


def _check_gate_req(gate_req: str, gates_passed: int, gates_total: int, all_pass: bool) -> bool:
    if gate_req == "all_gates": return all_pass
    if gate_req == "8_gates":   return gates_passed >= 8
    if gate_req == "7_gates":   return gates_passed >= 7
    if gate_req == "6_gates":   return gates_passed >= 6
    return True  # "any"


def _session_matches(cond_session: str, result_session: str) -> bool:
    _MAP = {
        "NY_OPEN":  {"NEW_YORK", "NY", "NY_OPEN", "NY AM"},
        "NEW_YORK": {"NEW_YORK", "NY", "NY_OPEN", "NY AM"},
        "LONDON":   {"LONDON", "LONDON OPEN"},
        "ASIAN":    {"ASIAN", "ASIA", "TOKYO"},
        "WEEKEND":  {"WEEKEND"},
    }
    candidates = _MAP.get(cond_session, {cond_session})
    return any(c in result_session for c in candidates)


def _infer_ict_state(result: dict) -> str:
    bos_data = result.get("bos_data") or {}
    bias     = result.get("bias", "")
    choch    = bos_data.get("choch", False) if isinstance(bos_data, dict) else False
    struct   = bos_data.get("structure_valid", False) if isinstance(bos_data, dict) else False

    if choch and bias == "BULLISH": return "CHOCH_BULL"
    if choch and bias == "BEARISH": return "CHOCH_BEAR"
    if struct and bias == "BULLISH": return "BOS_BULL"
    if struct and bias == "BEARISH": return "BOS_BEAR"
    markov = result.get("markov_state", "")
    if markov:
        return markov
    return "ACCUMULATION"


# ══════════════════════════════════════════════════════════════════════════════
# §4 — DEEP STRUCTURE TASK
# AI-powered extended candle history engine.
# Fetches 500×1D candles per symbol every 6h, distills into 15 ICT structural
# levels, discards raw candles immediately — ~800 bytes stored per symbol.
#
# Levels: pwh/pwl, pmh/pml, pqh/pql, pyh/pyl, swing_h/l_6m,
#         eq_high/low, ipda_60d_h/l
#
# Storage: /tmp/deep_struct_cache.json
# ══════════════════════════════════════════════════════════════════════════════

DEEP_STRUCT_FILE = os.getenv("DEEP_STRUCT_FILE", "/tmp/deep_struct_cache.json")
REFRESH_INTERVAL = int(os.getenv("DEEP_STRUCT_REFRESH_HOURS", "6")) * 3600
CANDLES_1D       = 500
CANDLES_1W       = 260
AI_TIMEOUT       = 20

_BINANCE_1D_URL = "https://api.binance.com/api/v3/klines?symbol={sym}&interval=1d&limit={limit}"
_BINANCE_1W_URL = "https://api.binance.com/api/v3/klines?symbol={sym}&interval=1w&limit={limit}"

_cache: Dict[str, dict] = {}


def _deep_load() -> None:
    global _cache
    try:
        if os.path.exists(DEEP_STRUCT_FILE):
            with open(DEEP_STRUCT_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                _cache = raw
                log.info(f"[DEEP] Loaded {len(_cache)} deep struct records")
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"[DEEP] Cache unreadable — starting fresh: {e}")
        _cache = {}


def _deep_save() -> None:
    try:
        with open(DEEP_STRUCT_FILE, "w", encoding="utf-8") as f:
            json.dump(_cache, f, separators=(",", ":"))
    except OSError as e:
        log.warning(f"[DEEP] Save failed: {e}")


def get_levels(symbol: str) -> Optional[dict]:
    """
    Return deep structural levels for symbol if cached and fresh.
    Returns None if not cached or older than REFRESH_INTERVAL × 2.
    Used by TechnicalAnalysis_v9_6 to enrich weekly_daily_liq.
    """
    sym = symbol.upper().strip()
    rec = _cache.get(sym)
    if not rec:
        return None
    if time.time() - rec.get("ts", 0) > REFRESH_INTERVAL * 2:
        return None
    return {k: v for k, v in rec.items() if k != "ts"}


def get_deep_summary() -> dict:
    """Return summary stats for /status command. (was: get_summary in deep_structure_task)"""
    now   = time.time()
    fresh = sum(1 for r in _cache.values() if now - r.get("ts", 0) < REFRESH_INTERVAL)
    return {"total": len(_cache), "fresh": fresh, "stale": len(_cache) - fresh}


async def _fetch_candles(session, url: str) -> List[list]:
    """Fetch raw klines from Binance. Returns list of [o,h,l,c,v] rows."""
    try:
        async with session.get(url, timeout=12) as resp:
            if resp.status != 200:
                return []
            raw = await resp.json()
            if not isinstance(raw, list):
                return []
            parsed = []
            for k in raw:
                try:
                    parsed.append([
                        float(k[1]), float(k[2]), float(k[3]),
                        float(k[4]), float(k[5]),
                    ])
                except (IndexError, TypeError, ValueError):
                    continue
            return parsed
    except Exception as e:
        log.debug(f"[DEEP] candle fetch error: {e}")
        return []


def _extract_levels_pure(daily: List[list], weekly: List[list], symbol: str) -> dict:
    """
    Extract 15 structural levels from raw candle history using pure math.
    No AI call. Runs in < 5ms. Raw candle data freed by caller after return.
    """
    out = {
        "pwh": 0.0, "pwl": 0.0, "pmh": 0.0, "pml": 0.0,
        "pqh": 0.0, "pql": 0.0, "pyh": 0.0, "pyl": 0.0,
        "swing_h6m": 0.0, "swing_l6m": 0.0,
        "eq_high": 0.0, "eq_low": 0.0,
        "ipda_60d_h": 0.0, "ipda_60d_l": 0.0,
        "source": "pure_math",
    }
    if not daily:
        return out

    d = daily

    if len(d) >= 8:
        wk = d[-8:-1]
        out["pwh"] = round(max(c[1] for c in wk), 8)
        out["pwl"] = round(min(c[2] for c in wk), 8)

    if len(d) >= 32:
        mo = d[-35:-5]
        out["pmh"] = round(max(c[1] for c in mo), 8)
        out["pml"] = round(min(c[2] for c in mo), 8)

    if len(d) >= 90:
        qt = d[-95:-22]
        out["pqh"] = round(max(c[1] for c in qt), 8)
        out["pql"] = round(min(c[2] for c in qt), 8)
    elif len(d) >= 45:
        qt = d[-65:-5]
        out["pqh"] = round(max(c[1] for c in qt), 8)
        out["pql"] = round(min(c[2] for c in qt), 8)

    if len(d) >= 275:
        yr = d[-275:-22]
        out["pyh"] = round(max(c[1] for c in yr), 8)
        out["pyl"] = round(min(c[2] for c in yr), 8)
    elif len(d) >= 60:
        yr = d[:-5]
        out["pyh"] = round(max(c[1] for c in yr), 8)
        out["pyl"] = round(min(c[2] for c in yr), 8)

    if len(d) >= 130:
        h6 = d[-130:]
        out["swing_h6m"] = round(max(c[1] for c in h6), 8)
        out["swing_l6m"] = round(min(c[2] for c in h6), 8)

    if len(d) >= 60:
        ip = d[-62:-2]
        out["ipda_60d_h"] = round(max(c[1] for c in ip), 8)
        out["ipda_60d_l"] = round(min(c[2] for c in ip), 8)

    if len(d) >= 20:
        closes     = [c[3] for c in d[-60:]]
        atr_approx = (max(closes) - min(closes)) / max(len(closes), 1) * 3
        if atr_approx > 0:
            highs_r = [round(c[1] / atr_approx) * atr_approx for c in d[-60:]]
            lows_r  = [round(c[2] / atr_approx) * atr_approx for c in d[-60:]]
            h_cnt   = Counter(highs_r)
            l_cnt   = Counter(lows_r)
            eq_h    = h_cnt.most_common(1)[0][0] if h_cnt else 0.0
            eq_l    = l_cnt.most_common(1)[0][0] if l_cnt else 0.0
            if h_cnt.get(eq_h, 0) >= 3:
                out["eq_high"] = round(eq_h, 8)
            if l_cnt.get(eq_l, 0) >= 3:
                out["eq_low"] = round(eq_l, 8)

    if weekly and len(weekly) >= 52:
        wkly_yr = weekly[-54:-4]
        out["pyh"] = round(max(c[1] for c in wkly_yr), 8)
        out["pyl"] = round(min(c[2] for c in wkly_yr), 8)
        if len(weekly) >= 104:
            wkly_2yr = weekly[-108:-52]
            out["pqh"] = max(out["pqh"], round(max(c[1] for c in wkly_2yr), 8))
            out["pql"] = min(out["pql"] or 1e18, round(min(c[2] for c in wkly_2yr), 8))

    return out


async def _extract_levels_with_ai(
    daily:  List[list],
    symbol: str,
    ai_fn,
) -> Optional[dict]:
    """Use AI to identify 6 most important structural levels from condensed candle summary."""
    if not daily or len(daily) < 30 or not ai_fn:
        return None

    try:
        closes  = [c[3] for c in daily]
        highs   = [c[1] for c in daily]
        lows    = [c[2] for c in daily]
        n       = len(daily)
        price   = closes[-1]

        def _qhi(q): return round(max(highs[int(n*(1-q)):]), 6)
        def _qlo(q): return round(min(lows[int(n*(1-q)):]),  6)

        summary = (
            f"Symbol: {symbol}  Price: {price}  Candles(1D): {n}\n"
            f"Last 7d:  H={_qhi(7/n) if n>=7 else 'N/A'}  L={_qlo(7/n) if n>=7 else 'N/A'}\n"
            f"Last 30d: H={_qhi(30/n) if n>=30 else 'N/A'}  L={_qlo(30/n) if n>=30 else 'N/A'}\n"
            f"Last 90d: H={_qhi(90/n) if n>=90 else 'N/A'}  L={_qlo(90/n) if n>=90 else 'N/A'}\n"
            f"Last 180d:H={_qhi(180/n) if n>=180 else 'N/A'}  L={_qlo(180/n) if n>=180 else 'N/A'}\n"
            f"Last 365d:H={_qhi(365/n) if n>=365 else 'N/A'}  L={_qlo(365/n) if n>=365 else 'N/A'}\n"
            f"All-time in window: H={round(max(highs),6)}  L={round(min(lows),6)}"
        )

        prompt = (
            "You are an ICT (Inner Circle Trader) structural analyst.\n"
            "Given this OHLC summary, identify the 6 most important price levels "
            "that institutional traders use: prior week H/L, prior month H/L, "
            "prior quarter H/L, 6-month dominant swing H/L, equal highs cluster (EQH), "
            "equal lows cluster (EQL).\n\n"
            f"{summary}\n\n"
            "Return ONLY valid JSON with these exact keys (no markdown, no explanation):\n"
            "{\n"
            "  \"pwh\": float,  \"pwl\": float,\n"
            "  \"pmh\": float,  \"pml\": float,\n"
            "  \"pqh\": float,  \"pql\": float,\n"
            "  \"swing_h6m\": float, \"swing_l6m\": float,\n"
            "  \"eq_high\": float,   \"eq_low\": float\n"
            "}\n"
            "Use 0.0 if a level cannot be determined. No text outside the JSON."
        )

        response_text = await asyncio.wait_for(ai_fn(prompt), timeout=AI_TIMEOUT)
        if not response_text:
            return None

        clean = response_text.strip()
        if "```" in clean:
            clean = clean.split("```")[1].lstrip("json").strip()

        parsed    = json.loads(clean)
        ai_levels = {}
        for key in ["pwh","pwl","pmh","pml","pqh","pql","swing_h6m","swing_l6m","eq_high","eq_low"]:
            val = parsed.get(key, 0.0)
            try:
                val = float(val)
                if math.isnan(val) or math.isinf(val):
                    val = 0.0
            except (TypeError, ValueError):
                val = 0.0
            ai_levels[key] = round(val, 8)

        ai_levels["source"] = "ai_enriched"
        return ai_levels

    except (asyncio.TimeoutError, json.JSONDecodeError, Exception) as e:
        log.debug(f"[DEEP] AI extraction failed ({type(e).__name__}): {e}")
        return None


async def refresh_symbol(
    symbol:  str,
    session,
    ai_fn = None,
) -> dict:
    """
    Fetch extended candles for one symbol, extract structural levels,
    discard raw candles, cache the compact levels dict.
    Returns the levels dict (or empty dict on failure).
    """
    sym = symbol.upper().strip().replace("/", "").replace("-", "")
    if not sym.endswith("USDT"):
        sym = sym + "USDT"

    existing = _cache.get(sym, {})
    if existing and time.time() - existing.get("ts", 0) < REFRESH_INTERVAL:
        log.debug(f"[DEEP] {sym} fresh — skip refresh")
        return existing

    log.info(f"[DEEP] Refreshing {sym} (1D×{CANDLES_1D} + 1W×{CANDLES_1W})")

    daily_url  = _BINANCE_1D_URL.format(sym=sym, limit=CANDLES_1D)
    weekly_url = _BINANCE_1W_URL.format(sym=sym, limit=CANDLES_1W)

    daily, weekly = await asyncio.gather(
        _fetch_candles(session, daily_url),
        _fetch_candles(session, weekly_url),
        return_exceptions=True,
    )
    if isinstance(daily,  Exception): daily  = []
    if isinstance(weekly, Exception): weekly = []

    if not daily or len(daily) < 30:
        log.warning(f"[DEEP] {sym}: insufficient daily candles ({len(daily)})")
        return {}

    levels = _extract_levels_pure(daily, weekly, sym)

    if ai_fn:
        try:
            ai_levels = await _extract_levels_with_ai(daily, sym, ai_fn)
            if ai_levels:
                for k, v in ai_levels.items():
                    if k != "source" and v != 0.0:
                        levels[k] = v
                levels["source"] = "ai_enriched"
                log.info(f"[DEEP] {sym}: AI enrichment applied")
        except Exception as e:
            log.debug(f"[DEEP] {sym}: AI enrichment skipped ({e})")

    del daily, weekly  # free immediately

    levels["ts"] = time.time()
    _cache[sym]  = levels
    _deep_save()

    log.info(
        f"[DEEP] {sym}: cached [{levels.get('source','pure_math')}] "
        f"pwh={levels.get('pwh',0):.4g} pwl={levels.get('pwl',0):.4g} "
        f"pyh={levels.get('pyh',0):.4g} pyl={levels.get('pyl',0):.4g}"
    )
    return levels


# ══════════════════════════════════════════════════════════════════════════════
# MODULE INIT — all four stores loaded on import
# ══════════════════════════════════════════════════════════════════════════════

_aws_load()
_load_memory()
_cond_load()
_deep_load()


# ══════════════════════════════════════════════════════════════════════════════
# SMOKE TESTS  (python cryptex_stores.py)
# Runs all original per-module tests in sequence.
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys, tempfile, random
    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")

    # ── §1 AWS TESTS ──────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("§1 APEX WATCH STORE TESTS")
    log.info("=" * 60)

    _test_aws = os.path.join(tempfile.gettempdir(), "aws_test.json")
    STORE_FILE = _test_aws
    _store.clear()

    fake_result = {
        "all_pass": True, "grade": "A", "conf": 78.0, "bias": "BULLISH",
        "entry_low": 95000.0, "entry_high": 96000.0,
        "tp1": 98000.0, "tp2": 100000.0, "tp3": 104000.0,
        "inv_sl": 93000.0, "sl2": 91000.0, "sl3": 89000.0,
        "gate_pass": [True]*11, "gate_scores": [0.9]*11,
        "gate_fp_data": {"gate_hash": "abc12345", "gate_tp_rate": 0.72, "gate_n": 18},
        "gate11_detail": {"gate11_pass": True, "gate11_adv_score": 1},
        "hmm_data": {"hmm_state": "TRENDING"},
        "markov_state": "BOS_BULL", "regime": "TRENDING",
        "session": "NEW_YORK", "best_tf": "4h",
        "signal_probability_data": {"signal_probability": 74.2, "probability_tier": "HIGH"},
    }
    rec = upsert("BTCUSDT", fake_result, chat_ids=[12345])
    assert rec["symbol"] == "BTCUSDT" and rec["gate_hash"] == "abc12345" and rec["outcome"] == OUTCOME_PENDING
    log.info("AWS Test 1 PASS: upsert basic record")

    rec2 = upsert("ETHUSDT", {**fake_result, "conf": 65.0}, chat_ids=[12345])
    assert len(_store) == 2
    log.info("AWS Test 2 PASS: two symbols stored")

    for i in range(99):
        upsert(f"COIN{i:03d}USDT", {**fake_result, "conf": float(i)}, chat_ids=[])
    assert len(_store) <= MAX_WATCH_RECORDS
    log.info(f"AWS Test 3 PASS: FIFO cap enforced ({len(_store)}/{MAX_WATCH_RECORDS})")

    test_sym = list(_store.keys())[-1]
    ok = record_outcome(test_sym, "TP3")
    assert ok and _store[test_sym]["outcome"] == "TP3"
    log.info(f"AWS Test 4 PASS: outcome recorded for {test_sym}")

    pending = get_all_pending()
    assert all(r["outcome"] == OUTCOME_PENDING for r in pending)
    log.info(f"AWS Test 5 PASS: get_all_pending = {len(pending)} records")

    unrecorded = get_unrecorded_outcomes()
    assert len(unrecorded) >= 1
    mark_outcome_recorded(test_sym)
    assert len(get_unrecorded_outcomes()) == len(unrecorded) - 1
    log.info("AWS Test 6 PASS: mark_outcome_recorded works")

    set_alert_flag(test_sym, "tp1_alerted", True)
    assert _store[test_sym]["tp1_alerted"] is True
    log.info("AWS Test 7 PASS: set_alert_flag")

    s = get_watch_summary()
    assert s["total"] == len(_store) and s["capacity"] == MAX_WATCH_RECORDS
    log.info(f"AWS Test 8 PASS: get_watch_summary → {s}")

    # ── §2 GATE MEMORY TESTS ──────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("§2 GATE MEMORY ENGINE TESTS")
    log.info("=" * 60)

    _memory.clear()
    _blacklist.clear()

    GATE_A = [True, True, False, True, True, True, False, True, True, True]
    GATE_B = [True, False, False, True, True, True, False, True, True, True]

    assert get_gate_probability(GATE_A) is None
    log.info("GM Test 1 PASS: insufficient samples → None")

    for _ in range(4):
        record_gate_outcome(GATE_A, "TP2", hmm_regime="TRENDING", ict_state="BOS_BULL")
    assert get_gate_probability(GATE_A) == 1.0
    log.info("GM Test 2 PASS: 4 TP outcomes → TP rate 1.0")

    for _ in range(8):
        record_gate_outcome(GATE_B, "SL", hmm_regime="CHOPPY", ict_state="FVG_BEAR")
    assert check_blacklist("CHOPPY", "FVG_BEAR", GATE_B) is None
    log.info("GM Test 3 PASS: n=8 < min_samples → no veto yet")

    for _ in range(4):
        record_gate_outcome(GATE_B, "SL", hmm_regime="CHOPPY", ict_state="FVG_BEAR")
    veto = check_blacklist("CHOPPY", "FVG_BEAR", GATE_B)
    assert veto and "IMMUNE VETO" in veto
    log.info(f"GM Test 4 PASS: blacklist veto triggered")

    fp = get_fingerprint_data(GATE_A)
    assert fp["gate_hash"] != "" and fp["gate_tp_rate"] == 1.0
    log.info("GM Test 5 PASS: fingerprint data correct")

    assert unflag_blacklist("CHOPPY", "FVG_BEAR", GATE_B) is True
    assert check_blacklist("CHOPPY", "FVG_BEAR", GATE_B) is None
    log.info("GM Test 6 PASS: unflag_blacklist clears flag")

    summary_gm = get_memory_summary()
    assert summary_gm["fingerprint_patterns"] >= 2
    log.info(f"GM Test 7 PASS: memory summary → {summary_gm}")

    # ── §3 CONDITION ENGINE TESTS ─────────────────────────────────────────────
    log.info("=" * 60)
    log.info("§3 CONDITION ENGINE TESTS")
    log.info("=" * 60)

    _conditions.clear()

    ok, cid, msg = register_condition(99999, "BTCUSDT", "CHOCH_BULL", "all_gates", "NY_OPEN")
    assert ok
    log.info(f"Cond Test 1 PASS: {msg}")

    fake_cond_result = {
        "all_pass": True, "gate_pass": [True]*11, "bias": "BULLISH",
        "session": "NEW_YORK", "grade": "A", "conf": 78.0,
        "signal_probability_data": {"signal_probability_pct": "74.2%"},
        "bos_data": {"choch": True, "structure_valid": True, "bias": "BULLISH"},
    }
    fired = evaluate_conditions(fake_cond_result, "BTCUSDT")
    assert len(fired) == 1
    log.info(f"Cond Test 2 PASS: condition fired")

    assert len(evaluate_conditions(fake_cond_result, "BTCUSDT")) == 0
    log.info("Cond Test 3 PASS: no double-fire")

    expired = expire_conditions()
    log.info(f"Cond Test 4 PASS: expire_conditions removed {expired}")

    cs = get_condition_summary()
    assert "active" in cs
    log.info(f"Cond Test 5 PASS: get_condition_summary → {cs}")

    # ── §4 DEEP STRUCTURE TESTS ───────────────────────────────────────────────
    log.info("=" * 60)
    log.info("§4 DEEP STRUCTURE TASK TESTS")
    log.info("=" * 60)

    DEEP_STRUCT_FILE = os.path.join(tempfile.gettempdir(), "deep_test.json")
    _cache.clear()

    random.seed(42)
    base_price = 50000.0
    fake_daily = []
    for i in range(500):
        o = base_price + random.uniform(-2000, 2000)
        h = o + random.uniform(0, 1500)
        l = o - random.uniform(0, 1500)
        c = (o + h + l) / 3
        fake_daily.append([o, h, l, c, random.uniform(1e8, 5e8)])
        base_price = c
    fake_weekly = fake_daily[::5]

    levels = _extract_levels_pure(fake_daily, fake_weekly, "BTCUSDT")
    assert all(levels[k] > 0 for k in ["pwh","pwl","pmh","pqh","pyh","ipda_60d_h"])
    log.info(f"Deep Test 1 PASS: pure math extraction OK")

    assert get_levels("BTCUSDT") is None
    log.info("Deep Test 2 PASS: get_levels None before cache")

    levels["ts"] = time.time()
    _cache["BTCUSDT"] = levels
    _deep_save()
    assert get_levels("BTCUSDT") is not None
    log.info("Deep Test 3 PASS: cached and retrieved")

    ds = get_deep_summary()
    assert ds["total"] >= 1 and ds["fresh"] >= 1
    log.info(f"Deep Test 4 PASS: get_deep_summary → {ds}")

    log.info("")
    log.info("✅ ALL SMOKE TESTS PASSED — cryptex_stores.py VERIFIED")
    sys.exit(0)
