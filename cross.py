# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# cross.py — 📐 Cross Analysis button handler
# Extracted from app.py (no logic changes).
# Connection: app.py does ↓
#   from cross import cross_analysis as _cross_raw
#   cross_analysis = safe_menu_handler(_cross_raw)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import asyncio
import logging
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)


async def cross_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Cross Analysis button - runs AI Cross Engine scan"""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    clear_market_messages        = _app.clear_market_messages
    create_main_keyboard         = _app.create_main_keyboard
    run_cross_scan               = _app.run_cross_scan
    build_cross_message          = _app.build_cross_message
    MARKOV_AVAILABLE             = _app.MARKOV_AVAILABLE
    _SUPER_VALIDATOR             = _app._SUPER_VALIDATOR
    _btc_regime                  = _app._btc_regime
    _markov_ai_consensus         = _app._markov_ai_consensus
    capture_output               = _app.capture_output
    get_feedback_inline_keyboard = _app.get_feedback_inline_keyboard
    logger                       = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    chat_id = update.effective_chat.id
    await clear_market_messages(chat_id, context)

    msg = await update.message.reply_text(
        "📐 *AI CROSS ENGINE*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⏳ Running live multi-exchange scan…\n"
        "_Fetching top coins from 5 exchanges across 3 timeframes…_\n\n"
        "🔄 Binance · Bybit · OKX · KuCoin · MEXC",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=create_main_keyboard()
    )
    try:
        # Wrap the whole scan in 60s — if exchanges are down, fail fast
        try:
            scores = await asyncio.wait_for(run_cross_scan(), timeout=60.0)
        except asyncio.TimeoutError:
            await msg.edit_text(
                "⏱ Scan timed out — exchanges are responding slowly.\n"
                "Please try again in 30 seconds.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
            return

        if not scores:
            await msg.edit_text(
                "⚠️ No cross signals found right now.\n"
                "Market may be in a neutral/choppy state. Try again later.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
            return

        text = build_cross_message(scores)

        # ── Super Markov: CrossBrain validation ───────────────────────
        if MARKOV_AVAILABLE and scores:
            try:
                _cross_syms = [getattr(s, 'symbol', str(s))[:10] for s in scores[:5]]
                # CROSS-BUG-1 FIX: was hardcoded "TRENDING" — now uses live _btc_regime
                # CROSS-BUG-2 FIX: derive bias from dominant signal direction, not missing param
                _dominant_type = scores[0].signal_type if scores else "neutral"
                _cross_bias = (
                    "BULLISH" if "golden" in _dominant_type
                    else "BEARISH" if "death" in _dominant_type
                    else "NEUTRAL"
                )
                _sv_cross = _SUPER_VALIDATOR.validate(
                    current_regime   = _btc_regime if _btc_regime != "unknown" else "TRENDING",
                    momentum_quality = "HIGH" if len(scores) >= 5 else "MEDIUM",
                    symbols          = _cross_syms,
                    n_confluence     = len(scores),
                    bias             = _cross_bias,
                    ict_conf_mod     = 1.0,
                    ict_verdict_key  = "NEUTRAL",
                )
                text += f"\n\n{_sv_cross['super_compact']}"
                if _sv_cross.get("cross_verdict"):
                    text += f"\n  ↳ {_sv_cross['cross_verdict']}"
                # Teach CrossBrain about current confluence
                await asyncio.to_thread(
                    _SUPER_VALIDATOR.cross.record_cross_signal,
                    _cross_syms, len(scores), ""
                )
                # CROSS-BUG-3 FIX: was hardcoded BOS_BULL/BULLISH/conf=65 regardless of signal
                # Now reflects the actual dominant cross direction
                _cross_ict = "BOS_BULL" if _cross_bias == "BULLISH" else "BOS_BEAR" if _cross_bias == "BEARISH" else "ACCUMULATION"
                _cross_conf = min(95, round(scores[0].confidence)) if scores else 65
                asyncio.create_task(
                    _markov_ai_consensus(
                        symbol    = _cross_syms[0] if _cross_syms else "MARKET",
                        ict_state = _cross_ict,
                        bias      = _cross_bias,
                        conf      = _cross_conf,
                        smc       = 60,
                        gates     = 7,
                        n_samples = 0,
                    ),
                    name="markov_ai_cross"
                )
            except Exception as _sv_cx_e:
                logger.debug(f"[SUPER_MARKOV] cross: {_sv_cx_e}")

        # Capture for Feedback engine
        capture_output(chat_id, text, "cross")

        # Telegram message limit is 4096 chars; split if needed
        feedback_kb = get_feedback_inline_keyboard()
        if len(text) <= 4096:
            try:
                await msg.edit_text(
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=feedback_kb
                )
            except BadRequest:
                await update.message.reply_text(
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=create_main_keyboard()
                )
        else:
            try:
                await msg.delete()
            except BadRequest as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
            for i, chunk in enumerate(chunks):
                kb = feedback_kb if i == len(chunks) - 1 else create_main_keyboard()
                await update.message.reply_text(
                    chunk,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb
                )

    except Exception as e:
        logger.error(f"Cross scan error: {e}", exc_info=True)
        try:
            await msg.edit_text(
                "❌ Scan failed. Check your internet connection and try again.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
        except BadRequest:
            await update.message.reply_text(
                "❌ Scan failed. Check your internet connection and try again.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
