# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# sector_rotation.py — 🌊 Sector Rotation button handler
# Extracted from app.py (no logic changes).
# Connection: app.py does ↓
#   from sector_rotation import sector_rotation as _sector_raw
#   sector_rotation = safe_menu_handler(_sector_raw)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import asyncio
import logging
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


async def sector_rotation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Sector Rotation button — v8.2 upgraded"""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    clear_market_messages         = _app.clear_market_messages
    create_main_keyboard          = _app.create_main_keyboard
    analyze_sector_rotation_async = _app.analyze_sector_rotation_async
    get_sector_explanation        = _app.get_sector_explanation
    _btc_regime                   = _app._btc_regime
    _btc_rsi                      = _app._btc_rsi
    MARKOV_AVAILABLE              = _app.MARKOV_AVAILABLE
    _SUPER_VALIDATOR              = _app._SUPER_VALIDATOR
    _markov_ai_consensus          = _app._markov_ai_consensus
    capture_output                = _app.capture_output
    logger                        = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    chat_id = update.effective_chat.id
    await clear_market_messages(chat_id, context)

    progress_msg = await update.message.reply_text(
        "🌊 *SECTOR ROTATION ENGINE*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⏳ Fetching DeFi TVL + CoinGecko data…\n"
        "_Analyzing institutional capital flows across sectors…_",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )

    sectors = []
    for attempt in range(3):
        try:
            sectors = await analyze_sector_rotation_async()
            if sectors:
                break
            if attempt < 2:
                logger.warning(f"[SECTOR] Empty result attempt {attempt+1}, retrying…")
                await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"[SECTOR] Attempt {attempt+1} exception: {e}", exc_info=True)
            if attempt < 2:
                await asyncio.sleep(2)

    if not sectors:
        try:
            await progress_msg.edit_text(
                "⚠️ Sector data temporarily unavailable — APIs may be slow.\n"
                "Please try again in 30 seconds.",
                reply_markup=create_main_keyboard()
            )
        except Exception:
            await update.message.reply_text(
                "⚠️ Sector data temporarily unavailable.",
                reply_markup=create_main_keyboard()
            )
        return

    try:
        await progress_msg.delete()
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    # ── [UPGRADE] BTC regime context header ──────────────────────────────
    _btc_reg_icons = {
        "bull_expansion": "🟢 Bull Expansion",
        "bull_pullback":  "🟡 Bull Pullback",
        "chop":           "🟡 Chop / Consolidation",
        "bear":           "🔴 Bear Market",
        "unknown":        "⚪ Unknown",
    }
    _btc_reg_label = _btc_reg_icons.get(_btc_regime, "⚪ Unknown")
    _reg_guidance  = {
        "bull_expansion": "Rotate into ACCUM → TREND sectors. Size up.",
        "bull_pullback":  "Focus ACCUM only. Wait for BTC confirmation.",
        "chop":           "Reduce position size. Favour NEUTRAL / ACCUM.",
        "bear":           "Capital preservation mode. DIST exits only.",
    }.get(_btc_regime, "Monitor all sectors.")

    reg_header = (
        f"🌊 *SECTOR ROTATION — v8.2*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"*BTC Macro Regime:* {_btc_reg_label}  RSI `{_btc_rsi:.0f}`\n"
        f"💡 _{_reg_guidance}_\n\n"
        f"*{len(sectors)} Sectors Analyzed* · DeFiLlama + CoinGecko\n"
        f"⏰ `{datetime.now(timezone.utc).strftime('%b %d %H:%M UTC')}`"
    )
    await update.message.reply_text(
        reg_header, parse_mode="Markdown", reply_markup=create_main_keyboard()
    )

    # ── Phase dividers with actionable guidance ───────────────────────────
    _PHASE_DIVIDERS = {
        "ACCUM":   "🧠 ACCUMULATION — Smart money loading quietly  →  WATCH FOR ENTRY",
        "TREND":   "📈 TRENDING — Confirmed move, still room  →  RIDE WITH STOPS",
        "DIST":    "📉 DISTRIBUTION — Smart money exiting  →  REDUCE OR AVOID",
        "NEUTRAL": "➖ NEUTRAL — No clear signal  →  MONITOR ONLY",
    }
    last_phase     = None
    all_sector_text = []

    for i, sector in enumerate(sectors, 1):
        current_phase = sector.get("dominant_phase", "NEUTRAL")

        if current_phase != last_phase:
            divider = _PHASE_DIVIDERS.get(current_phase, "")
            if divider:
                try:
                    await update.message.reply_text(
                        f"*━━━ {divider} ━━━*",
                        parse_mode="Markdown", reply_markup=create_main_keyboard()
                    )
                except Exception as _e:
                    logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            last_phase = current_phase

        tvl_b       = sector['tvl'] / 1e9
        tvl_chg     = sector.get('tvl_change_7d', 0)
        tvl_chg_str = f"+{tvl_chg:.1f}%" if tvl_chg >= 0 else f"{tvl_chg:.1f}%"
        tvl_chg_1d  = sector.get('tvl_change_1d', 0)
        tvl_1d_str  = f"+{tvl_chg_1d:.1f}%" if tvl_chg_1d >= 0 else f"{tvl_chg_1d:.1f}%"

        vote_bar    = sector.get("ai_vote_bar", "⬜⬜⬜⬜⬜")
        consensus   = sector.get("ai_consensus", "")
        ai_top_coin = sector.get("ai_top_coin", "")
        ai_top_note = f"  🤖 AI Pick: `{ai_top_coin}`" if ai_top_coin else ""

        # ── [UPGRADE] Phase-aware action instruction ───────────────────
        _phase_actions = {
            "ACCUM":   "✅ Entry zone — confirm with TA before sizing in",
            "TREND":   "⚡ Ride with trailing stop — do not chase breakouts",
            "DIST":    "⚠️ Reduce exposure — watch for breakdown signals",
            "NEUTRAL": "⏳ No clear edge — wait for phase shift",
        }
        action_note = _phase_actions.get(current_phase, "")

        header = (
            f"{sector['flow_status']} *{i}. {sector['category']}*\n"
            f"TVL: `${tvl_b:.2f}B`  7d: `{tvl_chg_str}`  1d: `{tvl_1d_str}`\n"
            f"AvgRCI: `{sector['avg_rci']}`  Avg%: `{sector.get('avg_change',0):+.1f}%`\n"
            f"🧠 *AI CONSENSUS* {vote_bar}{ai_top_note}\n"
            f"_{consensus}_\n"
            f"💡 _{action_note}_\n"
            f"{get_sector_explanation(sector['category'])}\n\n"
        )

        table = (
            "#  | Coin    | %      | RCI  | Smart | State\n"
            "---------------------------------------------------\n"
        )
        for j, token in enumerate(sector['tokens'], 1):
            table += (
                f"{j:<2} | {token['symbol']:<7} | {token['change']:+.1f}% | "
                f"{token['rci']:<4.1f} | {token['smart_money']:<5.1f} | {token['state']}\n"
            )

        legend = ""
        if i == len(sectors):
            legend = (
                "\n*Phase Legend:*\n"
                "🧠 ACCUM / ACCUM+ → smart money loading quietly\n"
                "📈 TREND / TREND+ → confirmed move, room to run\n"
                "📉 EARLY DIST → extended, watch for reversal\n"
                "📉 DIST / DUMP → smart money exiting\n"
                "➖ NEUTRAL → no clear signal\n\n"
                "⚠️ _Not financial advice. DYOR._"
            )

        message = f"{header}```\n{table}```{legend}"
        all_sector_text.append(message)

        try:
            await update.message.reply_text(
                message, parse_mode="Markdown", reply_markup=create_main_keyboard()
            )
        except Exception as e:
            logger.error(f"[SECTOR] Failed to send sector {i} ({sector['category']}): {e}")

    # ── [UPGRADE] Capture for QA validation ──────────────────────────────
    try:
        combined = reg_header + "\n\n" + "\n\n".join(all_sector_text)
        capture_output(chat_id, combined, "sector_rotation")
    except Exception as _e:
        logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    # ── Super Markov: SectorBrain validation + learning ───────────────
    if MARKOV_AVAILABLE and sectors:
        try:
            _top_sector = sectors[0].get("category", "") if sectors else ""
            _top_phase  = sectors[0].get("dominant_phase", "NEUTRAL") if sectors else "NEUTRAL"
            # Teach SectorBrain about each sector observed
            for _sec in sectors[:5]:
                await asyncio.to_thread(
                    _SUPER_VALIDATOR.sector.record_sector_signal,
                    _sec.get("category", ""),
                    _sec.get("dominant_phase", "NEUTRAL"),
                    "",
                    _sec.get("ai_top_coin", ""),
                )
            # SuperValidator compact verdict
            _sv_sec = _SUPER_VALIDATOR.validate(
                current_regime   = _btc_regime.upper() if _btc_regime else "TRENDING",
                momentum_quality = "HIGH" if _btc_rsi > 60 else "LOW" if _btc_rsi < 40 else "MEDIUM",
                symbols          = [_top_sector],
            )
            try:
                await update.message.reply_text(
                    f"{_sv_sec['super_compact']}\n"
                    f"  ↳ {_sv_sec.get('sector_verdict','')}\n"
                    f"⚠️ _Not financial advice. DYOR._",
                    parse_mode="Markdown",
                    reply_markup=create_main_keyboard()
                )
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            asyncio.create_task(
                _markov_ai_consensus(
                    symbol    = _top_sector[:20] if _top_sector else "MARKET",
                    ict_state = "ACCUMULATION",
                    bias      = "BULLISH" if _top_phase == "TREND" else "BEARISH" if _top_phase == "DIST" else "NEUTRAL",
                    conf      = 60,
                    smc       = 55,
                    gates     = 6,
                    n_samples = 0,
                ),
                name="markov_ai_sector"
            )
        except Exception as _sv_se_e:
            logger.debug(f"[SUPER_MARKOV] sector: {_sv_se_e}")
