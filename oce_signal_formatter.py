"""
╔══════════════════════════════════════════════════════════════════════╗
║  OCE SIGNAL FORMATTER v10.3                                          ║
║  GAP-10 FIX: Missing module — referenced in app.py but never built  ║
║                                                                      ║
║  Applies the OCE v10.3 pipeline to every TA signal result:          ║
║                                                                      ║
║  Stage 4.8 — Evaluability Gate                                      ║
║    oce_eval.eval_gate:       PASS / WARN / FAIL                     ║
║    oce_eval.eval_score:      0–100                                  ║
║    oce_eval.epistemic_label: VERIFIED / PLAUSIBLE / SPECULATIVE     ║
║                                                                      ║
║  Stage 4.9 — Pro Code Audit (signal-level)                         ║
║    oce_audit.audit_result:  PASS / PASS WITH NOTES / FAIL          ║
║    oce_audit.d1_functional: Functional completeness check           ║
║    oce_audit.d2_ux:         Level/display quality check            ║
║    oce_audit.d3_qa:         Edge-case / sanity check               ║
║                                                                      ║
║  BRVF Loop — Build-Run-Verify-Fix                                   ║
║    oce_brvf.brvf_status: PASS / FIX_APPLIED / ERROR                ║
║    oce_brvf.brvf_fixes:  list of auto-applied corrections          ║
║                                                                      ║
║  PCA Points — 2D gate-vector clustering for scatter plot            ║
║    result.pca_points: [{x, y, outcome, current}]                   ║
║                                                                      ║
║  Chart annotation — draws OCE metadata onto a Matplotlib chart      ║
║    (optional: requires matplotlib + Pillow; core pipeline is stdlib)║
║                                                                      ║
║  Core: zero external deps. Python 3.8+. Thread-safe (no state).     ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import io
import math
import logging
from typing import Optional

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# STAGE 4.8 — EVALUABILITY GATE
# Does the signal meet minimum quality thresholds to be usable?
# ══════════════════════════════════════════════════════════════════════

_EVAL_WEIGHTS = {
    "has_entry":        15,   # entry zone defined
    "has_tp_stack":     15,   # TP1/TP2/TP3 all present and ordered
    "has_sl":           15,   # SL defined and below entry (LONG) or above (SHORT)
    "rr_ok":            15,   # R:R >= 1.5
    "gate_count_ok":    10,   # at least 6/11 gates passed
    "bias_locked":      10,   # bias not empty
    "conf_ok":          10,   # confidence score >= 30
    "no_price_anomaly":  5,   # no flash-crash warning
    "has_markov":        5,   # Markov state present
}

def _run_eval_gate(result: dict) -> dict:
    """
    Stage 4.8: Evaluability Gate.
    Checks 9 signal quality dimensions and produces a composite score.
    Returns oce_eval dict with eval_gate, eval_score, epistemic_label.
    """
    bias       = (result.get("bias") or "").upper()
    entry_low  = float(result.get("entry_low") or 0)
    entry_high = float(result.get("entry_high") or 0)
    tp1        = float(result.get("tp1") or 0)
    tp2        = float(result.get("tp2") or 0)
    tp3        = float(result.get("tp3") or 0)
    sl         = float(result.get("inv_sl") or result.get("sl") or 0)
    rr         = float(result.get("rr") or 0)
    gate_pass  = result.get("gate_pass") or []
    conf       = float(result.get("conf") or 0)
    price      = float(result.get("price") or 0)
    flash_warn = bool(result.get("flash_warning", False))
    mk_state   = (result.get("markov_state") or
                  (result.get("markov_data") or {}).get("markov_state") or "")

    entry_mid  = (entry_low + entry_high) / 2.0 if (entry_low and entry_high) else price

    checks = {}
    fails  = []

    # 1. Entry zone
    checks["has_entry"] = bool(entry_low > 0 and entry_high > 0 and entry_low < entry_high)
    if not checks["has_entry"]:
        fails.append("Entry zone missing or inverted")

    # 2. TP stack: all three present and in ascending order for LONG, descending for SHORT
    checks["has_tp_stack"] = False
    if tp1 and tp2 and tp3:
        if bias == "BULLISH":
            checks["has_tp_stack"] = (entry_mid < tp1 < tp2 < tp3)
        elif bias == "BEARISH":
            checks["has_tp_stack"] = (entry_mid > tp1 > tp2 > tp3)
        else:
            checks["has_tp_stack"] = (tp1 > 0 and tp2 > 0 and tp3 > 0)
    if not checks["has_tp_stack"]:
        fails.append("TP stack missing or directionally invalid")

    # 3. SL defined and directionally valid
    checks["has_sl"] = False
    if sl > 0 and entry_mid > 0:
        checks["has_sl"] = (sl < entry_mid if bias == "BULLISH"
                            else sl > entry_mid if bias == "BEARISH"
                            else sl > 0)
    if not checks["has_sl"]:
        fails.append("SL missing or on wrong side of entry")

    # 4. R:R
    checks["rr_ok"] = (rr >= 1.5)
    if not checks["rr_ok"]:
        fails.append(f"R:R {rr:.2f} below 1.5 floor")

    # 5. Gates
    gates_passed = sum(1 for g in gate_pass if g)
    checks["gate_count_ok"] = (gates_passed >= 6)
    if not checks["gate_count_ok"]:
        fails.append(f"Only {gates_passed}/{len(gate_pass)} gates passed (min 6)")

    # 6. Bias
    checks["bias_locked"] = bool(bias in ("BULLISH", "BEARISH"))
    if not checks["bias_locked"]:
        fails.append("Bias not determined")

    # 7. Confidence
    checks["conf_ok"] = (conf >= 30)
    if not checks["conf_ok"]:
        fails.append(f"Confidence {conf:.0f} below 30 floor")

    # 8. No flash crash (warning doesn't fail but reduces score)
    checks["no_price_anomaly"] = (not flash_warn)
    if flash_warn:
        fails.append("Flash-crash warning active — elevated volatility")

    # 9. Markov state
    checks["has_markov"] = bool(mk_state)

    # Score
    score = sum(_EVAL_WEIGHTS[k] for k, v in checks.items() if v)
    score = min(100, max(0, score))

    # Gate: PASS ≥ 75, WARN ≥ 50, FAIL < 50
    if score >= 75:
        eval_gate = "PASS"
    elif score >= 50:
        eval_gate = "WARN"
    else:
        eval_gate = "FAIL"

    # Epistemic label
    critical_ok = all([
        checks["has_entry"], checks["has_tp_stack"],
        checks["has_sl"], checks["bias_locked"],
    ])
    if critical_ok and score >= 80:
        epistemic_label = "VERIFIED"
    elif critical_ok and score >= 55:
        epistemic_label = "PLAUSIBLE"
    else:
        epistemic_label = "SPECULATIVE"

    return {
        "eval_gate":      eval_gate,
        "eval_score":     score,
        "epistemic_label": epistemic_label,
        "checks":         checks,
        "failures":       fails,
        "gates_passed":   gates_passed,
    }


# ══════════════════════════════════════════════════════════════════════
# STAGE 4.9 — PRO CODE AUDIT (signal-level)
# Checks 3 audit domains on the signal output itself.
# ══════════════════════════════════════════════════════════════════════

def _run_signal_audit(result: dict, eval_result: dict) -> dict:
    """
    Stage 4.9: Pro Code Audit applied to the signal result dict.
    Domain 1 — Functional: all required keys present and non-zero.
    Domain 2 — UX: levels are human-meaningful (no $0.00, no reversed stacks).
    Domain 3 — QA: edge-case sanity (SL < 0.1% from entry would liquidate, etc.)
    """
    d1_issues = []
    d2_issues = []
    d3_issues = []

    required_keys = ["price", "bias", "grade", "conf", "entry_low", "entry_high",
                     "tp1", "tp2", "tp3", "inv_sl", "gate_pass", "rr"]
    for k in required_keys:
        v = result.get(k)
        if v is None or v == "" or v == [] or v == 0:
            if k not in ("rr",):   # rr can be 0 when blocked
                d1_issues.append(f"Missing/zero: {k}")

    # D2: UX quality
    price     = float(result.get("price") or 0)
    tp1       = float(result.get("tp1") or 0)
    tp3       = float(result.get("tp3") or 0)
    inv_sl    = float(result.get("inv_sl") or 0)
    entry_mid = (float(result.get("entry_low") or 0) +
                 float(result.get("entry_high") or 0)) / 2.0 or price
    bias      = (result.get("bias") or "").upper()

    if price > 0 and tp3 > 0:
        tp3_move_pct = abs(tp3 - price) / price * 100
        if tp3_move_pct < 0.5:
            d2_issues.append(f"TP3 too close to price ({tp3_move_pct:.2f}% move)")
        if tp3_move_pct > 200:
            d2_issues.append(f"TP3 implausibly far ({tp3_move_pct:.0f}% move)")

    if inv_sl > 0 and price > 0:
        sl_pct = abs(inv_sl - price) / price * 100
        if sl_pct < 0.05:
            d2_issues.append(f"SL {sl_pct:.3f}% from entry — would cause immediate stop-out")
        if sl_pct > 50:
            d2_issues.append(f"SL {sl_pct:.0f}% from entry — impractical for retail sizing")

    # D3: QA edge cases
    rr = float(result.get("rr") or 0)
    if result.get("all_pass") and rr < 1.5:
        d3_issues.append(f"Signal marked all_pass but R:R={rr:.2f} < 1.5")

    session = (result.get("session") or "").upper()
    if session == "WEEKEND":
        d3_issues.append("Weekend session — low liquidity, higher spread risk")

    if result.get("flash_warning"):
        d3_issues.append(f"Flash-crash warning: {result.get('flash_warning_pct',0):+.2f}%")

    # Determine audit result
    if not d1_issues and not d2_issues and not d3_issues:
        audit_result = "PASS"
    elif d1_issues:
        audit_result = "FAIL"
    else:
        audit_result = "PASS WITH NOTES"

    d1_str = "; ".join(d1_issues) if d1_issues else "✓ All fields present"
    d2_str = "; ".join(d2_issues) if d2_issues else "✓ Levels within range"
    d3_str = "; ".join(d3_issues) if d3_issues else "✓ No edge-case flags"

    return {
        "audit_result":   audit_result,
        "d1_functional":  d1_str,
        "d2_ux":          d2_str,
        "d3_qa":          d3_str,
        "d1_issues":      d1_issues,
        "d2_issues":      d2_issues,
        "d3_issues":      d3_issues,
    }


# ══════════════════════════════════════════════════════════════════════
# BRVF LOOP — Build-Run-Verify-Fix
# Applies auto-corrections to obvious signal defects.
# ══════════════════════════════════════════════════════════════════════

def _run_brvf(result: dict, audit: dict) -> tuple:
    """
    BRVF: Apply minimal targeted fixes to signal defects identified in audit.
    Returns (fixed_result, brvf_dict).
    Only fixes issues that are safe to auto-correct without re-running the pipeline.
    """
    fixes   = []
    r       = dict(result)   # shallow copy — we only mutate top-level scalars

    bias       = (r.get("bias") or "").upper()
    entry_low  = float(r.get("entry_low") or 0)
    entry_high = float(r.get("entry_high") or 0)
    tp1        = float(r.get("tp1") or 0)
    tp2        = float(r.get("tp2") or 0)
    tp3        = float(r.get("tp3") or 0)
    inv_sl     = float(r.get("inv_sl") or 0)
    price      = float(r.get("price") or 0)
    atr_pct    = float(r.get("atr_pct") or 1.0)
    atr_est    = price * atr_pct / 100

    # Fix 1: TP2 out of order
    if bias == "BULLISH" and tp1 > 0 and tp2 > 0 and tp3 > 0:
        if not (tp1 < tp2 < tp3):
            tp_sorted = sorted([tp1, tp2, tp3])
            r["tp1"], r["tp2"], r["tp3"] = tp_sorted
            fixes.append("TP stack reordered (BULLISH ascending)")
    elif bias == "BEARISH" and tp1 > 0 and tp2 > 0 and tp3 > 0:
        if not (tp1 > tp2 > tp3):
            tp_sorted = sorted([tp1, tp2, tp3], reverse=True)
            r["tp1"], r["tp2"], r["tp3"] = tp_sorted
            fixes.append("TP stack reordered (BEARISH descending)")

    # Fix 2: Entry zone inverted
    if entry_low > 0 and entry_high > 0 and entry_low > entry_high:
        r["entry_low"], r["entry_high"] = entry_high, entry_low
        fixes.append("Entry zone low/high swapped (was inverted)")

    # Fix 3: SL on wrong side — shift by 1×ATR
    if inv_sl > 0 and price > 0 and atr_est > 0:
        if bias == "BULLISH" and inv_sl >= price:
            r["inv_sl"] = round(price - atr_est * 1.5, 8)
            fixes.append(f"SL shifted below price by 1.5×ATR (was above for LONG)")
        elif bias == "BEARISH" and inv_sl <= price:
            r["inv_sl"] = round(price + atr_est * 1.5, 8)
            fixes.append(f"SL shifted above price by 1.5×ATR (was below for SHORT)")

    # Fix 4: Recalculate R:R if levels changed
    if fixes:
        _em   = (float(r.get("entry_low") or 0) + float(r.get("entry_high") or 0)) / 2 or price
        _sl   = float(r.get("inv_sl") or 0)
        _tp3  = float(r.get("tp3") or 0)
        if _em and _sl and _tp3 and abs(_em - _sl) > 1e-9:
            r["rr"] = round(abs(_tp3 - _em) / abs(_em - _sl), 2)
            fixes.append(f"R:R recalculated to {r['rr']}")

    brvf_status = "FIX_APPLIED" if fixes else "PASS"

    return r, {
        "brvf_status": brvf_status,
        "brvf_fixes":  fixes,
        "brvf_pass":   (brvf_status == "PASS"),
    }


# ══════════════════════════════════════════════════════════════════════
# PCA SCATTER — 2D gate vector projection for trade_view.html
# Reduces 11-dimensional gate_pass vector to 2D using manual projection.
# No sklearn needed — pure math, fast, deterministic.
# ══════════════════════════════════════════════════════════════════════

# Fixed 2D projection matrix (11 gates → 2 axes)
# PC1 captures "overall gate quality" (all gates equal weight)
# PC2 captures "structural vs execution" split
_PC1 = [0.302, 0.302, 0.302, 0.302, 0.302, 0.302, 0.302, 0.302, 0.302, 0.302, 0.302]
_PC2 = [0.5, 0.5, -0.5, 0.3, 0.3, -0.3, -0.3, 0.2, -0.2, 0.0, 0.5]   # gate specialisation axis


def _project_gate_vector(gate_pass: list) -> tuple:
    """Project 11-dimensional gate vector to (x, y) 2D space."""
    vec = [1.0 if g else 0.0 for g in (gate_pass or [])]
    # Pad or truncate to 11
    while len(vec) < 11:
        vec.append(0.0)
    vec = vec[:11]
    # Add jitter so overlapping points are distinguishable
    import random
    rng = random.Random(hash(tuple(vec)))
    x = sum(v * w for v, w in zip(vec, _PC1)) + rng.uniform(-0.05, 0.05)
    y = sum(v * w for v, w in zip(vec, _PC2)) + rng.uniform(-0.05, 0.05)
    return round(x, 3), round(y, 3)


def _build_pca_points(result: dict) -> list:
    """
    Build PCA scatter points from gate_memory fingerprint history.
    Each point = one historical signal with the same gate hash pattern.
    Current signal is marked current=True.
    """
    try:
        from cryptex_stores import get_all as _aws_get_all
        all_records = _aws_get_all()
    except ImportError:
        return []

    current_gp   = result.get("gate_pass") or []
    current_hash = (result.get("gate_fp_data") or {}).get("gate_hash", "")
    points       = []

    for rec in all_records[-60:]:   # last 60 records only — keep payload small
        gp      = rec.get("gate_pass") or []
        outcome = rec.get("outcome", "PENDING")
        if outcome == "PENDING" or not gp:
            continue
        x, y = _project_gate_vector(gp)
        points.append({
            "x":       x,
            "y":       y,
            "outcome": outcome,
            "current": False,
        })

    # Add current signal point
    if current_gp:
        cx, cy = _project_gate_vector(current_gp)
        points.append({
            "x":       cx,
            "y":       cy,
            "outcome": "PENDING",
            "current": True,
        })

    return points[:50]   # hard cap — URL size budget


# ══════════════════════════════════════════════════════════════════════
# CHART ANNOTATION — draw OCE metadata onto the Matplotlib chart buffer
# ══════════════════════════════════════════════════════════════════════

def oce_annotate_chart(
    chart_buf: Optional[io.BytesIO],
    result:    dict,
    eval_res:  dict,
    grade:     str,
) -> Optional[io.BytesIO]:
    """
    Annotate the trade chart with OCE eval gate + epistemic label.

    Optional dependencies:
      - matplotlib (rendering)
      - Pillow/PIL (image decode)

    Returns the original buffer (may be annotated in-place or returned unchanged
    if optional deps are missing or annotation fails).
    """
    if not chart_buf:
        return chart_buf
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from PIL import Image

        chart_buf.seek(0)
        img = Image.open(chart_buf)
        fig, ax = plt.subplots(figsize=(img.width / 100, img.height / 100), dpi=100)
        ax.imshow(img)
        ax.axis("off")

        eval_gate     = eval_res.get("eval_gate", "—")
        epistemic_lbl = eval_res.get("epistemic_label", "—")
        eval_score    = eval_res.get("eval_score", 0)
        gates_passed  = eval_res.get("gates_passed", 0)
        gate_total    = len(result.get("gate_pass") or [])
        flash_warn    = result.get("flash_warning", False)

        # Colour coding
        gate_colour = {"PASS": "#00a651", "WARN": "#f5a623", "FAIL": "#d0021b"}.get(eval_gate, "#999")

        # Bottom annotation bar
        bar_y  = img.height - 18
        bar_txt = (
            f"OCE v10.3 | Eval: {eval_gate} ({eval_score}/100) | "
            f"{epistemic_lbl} | Gates {gates_passed}/{gate_total} | Grade {grade}"
        )
        if flash_warn:
            bar_txt += f" | ⚠ HIGH VOL {result.get('flash_warning_pct',0):+.1f}%"

        # UX: keep overlays readable and avoid overly-long captions on small charts.
        if len(bar_txt) > 140:
            bar_txt = bar_txt[:137] + "…"

        ax.text(
            img.width / 2, bar_y, bar_txt,
            ha="center", va="center", fontsize=6, color="white",
            bbox=dict(boxstyle="round,pad=0.2", facecolor=gate_colour, alpha=0.85),
        )

        out_buf = io.BytesIO()
        plt.savefig(out_buf, format="png", bbox_inches="tight", dpi=100, pad_inches=0)
        plt.close(fig)
        out_buf.seek(0)
        return out_buf
    except Exception as _ann_e:
        log.debug(f"[OCE_CHART] annotation failed: {_ann_e}")
        if chart_buf:
            chart_buf.seek(0)
        return chart_buf


# ══════════════════════════════════════════════════════════════════════
# PUBLIC ENTRY POINT — oce_enhance_result()
# Called by app.py after run_ict_analysis() returns.
# ══════════════════════════════════════════════════════════════════════

def oce_enhance_result(result: dict, symbol: str, bias: str) -> dict:
    """
    Apply OCE v10.3 post-processing pipeline to a TA signal result.

    Stages applied:
      4.8 — Evaluability Gate   → result["oce_eval"]
      4.9 — Pro Code Audit      → result["oce_audit"]
      BRVF Loop                 → result["oce_brvf"] + fixes applied in-place
      PCA Points                → result["pca_points"]

    Returns the (possibly corrected) result dict with all OCE keys populated.
    This function never raises — all failures are caught and logged.
    The original result is returned unchanged on any error.
    """
    if not result or not isinstance(result, dict):
        return result or {}

    try:
        # ── Stage 4.8: Evaluability Gate ──────────────────────────────
        eval_res     = _run_eval_gate(result)
        result["oce_eval"] = {
            "eval_gate":       eval_res["eval_gate"],
            "eval_score":      eval_res["eval_score"],
            "epistemic_label": eval_res["epistemic_label"],
            "gates_passed":    eval_res["gates_passed"],
            "failures":        eval_res["failures"][:3],   # top 3 failures only
        }
        log.info(
            f"[OCE 4.8] {symbol} | {eval_res['eval_gate']} "
            f"score={eval_res['eval_score']} label={eval_res['epistemic_label']}"
        )
    except Exception as _e48:
        log.debug(f"[OCE 4.8] eval gate error: {_e48}")
        result.setdefault("oce_eval", {
            "eval_gate": "—", "eval_score": 0,
            "epistemic_label": "UNVERIFIED", "gates_passed": 0, "failures": [],
        })

    try:
        # ── Stage 4.9: Pro Code Audit ──────────────────────────────────
        audit_res = _run_signal_audit(result, result.get("oce_eval", {}))
        result["oce_audit"] = {
            "audit_result":   audit_res["audit_result"],
            "d1_functional":  audit_res["d1_functional"],
            "d2_ux":          audit_res["d2_ux"],
            "d3_qa":          audit_res["d3_qa"],
        }
        _d1ok = '✓' if not audit_res['d1_issues'] else f"{len(audit_res['d1_issues'])}issues"
        _d2ok = '✓' if not audit_res['d2_issues'] else f"{len(audit_res['d2_issues'])}issues"
        _d3ok = '✓' if not audit_res['d3_issues'] else f"{len(audit_res['d3_issues'])}issues"
        log.info(
            f"[OCE 4.9] {symbol} | audit={audit_res['audit_result']} "
            f"d1={_d1ok} d2={_d2ok} d3={_d3ok}"
        )
    except Exception as _e49:
        log.debug(f"[OCE 4.9] audit error: {_e49}")
        result.setdefault("oce_audit", {
            "audit_result": "—", "d1_functional": "—", "d2_ux": "—", "d3_qa": "—",
        })

    try:
        # ── BRVF Loop ──────────────────────────────────────────────────
        result, brvf_res = _run_brvf(result, result.get("oce_audit", {}))
        result["oce_brvf"] = brvf_res
        if brvf_res["brvf_fixes"]:
            log.info(f"[OCE BRVF] {symbol} | {len(brvf_res['brvf_fixes'])} fix(es): "
                     f"{'; '.join(brvf_res['brvf_fixes'][:2])}")
        else:
            log.debug(f"[OCE BRVF] {symbol} | PASS — no fixes needed")
    except Exception as _ebrvf:
        log.debug(f"[OCE BRVF] error: {_ebrvf}")
        result.setdefault("oce_brvf", {"brvf_status": "ERROR", "brvf_fixes": [], "brvf_pass": False})

    try:
        # ── PCA Points ─────────────────────────────────────────────────
        pca_pts = _build_pca_points(result)
        result["pca_points"] = pca_pts
        log.debug(f"[OCE PCA] {symbol} | {len(pca_pts)} scatter points")
    except Exception as _epca:
        log.debug(f"[OCE PCA] error: {_epca}")
        result.setdefault("pca_points", [])

    return result
