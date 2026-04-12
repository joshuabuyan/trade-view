# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# alpha_signals.py — 💎 Alpha Signals button handler
# Extracted from app.py (no logic changes).
# Connection: app.py does ↓
#   from alpha_signals import alpha_signals as _alpha_raw
#   alpha_signals = safe_menu_handler(_alpha_raw)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import asyncio
import logging
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


async def alpha_signals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Alpha Signals v8.2 — Institutional capital flow detection.
    Upgrades: 100 coins, 1H price change, all 5 RCI phases (ACCUM/TREND/DIST/MOMENTUM/NEUTRAL),
    PHP price display, AI consensus vote, BTC regime context, corrected distribution threshold,
    APEX sensor score integration.
    """
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    clear_market_messages        = _app.clear_market_messages
    create_main_keyboard         = _app.create_main_keyboard
    fetch_alpha_coin_data        = _app.fetch_alpha_coin_data
    calculate_rci_metrics        = _app.calculate_rci_metrics
    _get_peso_rate               = _app._get_peso_rate
    GROQ_API_KEY                 = _app.GROQ_API_KEY
    GEMINI_API_KEY               = _app.GEMINI_API_KEY
    MISTRAL_API_KEY              = _app.MISTRAL_API_KEY
    CEREBRAS_API_KEY             = _app.CEREBRAS_API_KEY
    GITHUB_TOKEN                 = _app.GITHUB_TOKEN
    ai_query                     = _app.ai_query
    _btc_regime                  = _app._btc_regime
    _btc_rsi                     = _app._btc_rsi
    _apex_state                  = _app._apex_state
    MARKOV_AVAILABLE             = _app.MARKOV_AVAILABLE
    _SUPER_VALIDATOR             = _app._SUPER_VALIDATOR
    capture_output               = _app.capture_output
    get_feedback_inline_keyboard = _app.get_feedback_inline_keyboard
    logger                       = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    chat_id = update.effective_chat.id
    await clear_market_messages(chat_id, context)

    progress_msg = await update.message.reply_text(
        "💎 *ALPHA SIGNALS — v8.2*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⏳ Scanning 100 coins for stealth capital flows…\n"
        "_RCI · Smart Money · Efficiency · Phase Detection_",
        parse_mode="Markdown",
        reply_markup=create_main_keyboard()
    )

    try:
        # ── [FIX] resilient CoinGecko fetch with graceful fallback ───────
        top_coins = await asyncio.to_thread(fetch_alpha_coin_data)

        if not top_coins:
            try:
                await progress_msg.edit_text(
                    "❌ Failed to fetch coin data after retries. Please try again in a moment.",
                    reply_markup=create_main_keyboard()
                )
            except Exception:
                await update.message.reply_text(
                    "❌ Failed to fetch coin data after retries.",
                    reply_markup=create_main_keyboard()
                )
            return

        # ── Phase buckets ─────────────────────────────────────────────────
        accumulation = []   # high RCI, low price move — stealth load
        trending     = []   # RCI + aligned price move — ride signal
        momentum     = []   # explosive move + very high RCI — FOMO/blow-off alert
        distribution = []   # price ran, smart money fading — avoid/exit
        stablecoin_excluded = []

        peso_rate = _get_peso_rate()
        _stablecoin_symbols = {"USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "PYUSD", "USDD", "FRAX", "BUSD"}

        for coin in top_coins:
            vol    = float(coin.get("total_volume") or 0)
            change = float(coin.get("price_change_percentage_24h") or 0)
            # UPGRADE-A3: track whether 1h data was actually present or null-filled
            _chg1h_raw = coin.get("price_change_percentage_1h_in_currency")
            chg1h      = float(_chg1h_raw or 0)
            _chg1h_str = f"{chg1h:+.2f}%" if _chg1h_raw is not None else "N/A"
            mcap   = float(coin.get("market_cap") or 0)
            price  = float(coin.get("current_price") or 0)
            sym    = (coin.get("symbol") or "").upper()

            if vol == 0 or mcap == 0:
                continue
            if sym in _stablecoin_symbols:
                stablecoin_excluded.append(sym)
                continue

            metrics = calculate_rci_metrics(coin, vol, mcap, price, change)
            rci     = metrics['rci']
            smart   = metrics['smart_money']
            eff     = metrics['efficiency']
            state   = metrics['state']
            liq     = metrics['liquidity']

            fp = lambda v: f"${v:,.4f}" if 0 < v < 1 else f"${v:,.2f}"

            base = {
                "symbol":      sym,
                "rci":         rci,
                "change":      change,
                "chg1h":       chg1h,
                "chg1h_str":   _chg1h_str,   # UPGRADE-A3: N/A when API returns null
                "price":       price,
                "price_str":   fp(price),
                "peso":        f"₱{price * peso_rate:,.2f}",
                "smart_money": smart,
                "efficiency":  eff,
                "liquidity":   liq,
                "state":       state,
                "name":        coin.get("name", sym),
            }

            # ── [UPGRADE] Phase detection with calibrated thresholds ──────
            # MOMENTUM: explosive move + extremely high RCI = FOMO / blow-off
            if rci >= 75 and abs(change) > 15:
                momentum.append(base)

            # ACCUMULATION: high RCI, price mostly flat = smart money loading
            elif rci > 70 and abs(change) < 3:
                accumulation.append(base)

            # TREND: RCI above baseline, aligned price movement
            elif rci >= 55 and 3 <= abs(change) <= 12 and change > 0:
                trending.append(base)

            # DISTRIBUTION: [FIX] efficiency > 40 (not 10 — 10 was far too loose,
            # nearly everything qualified). Also require smart_money < 40 to confirm
            # institutions are exiting while retail is buying the move.
            elif change > 5 and eff > 40 and smart < 40:
                distribution.append(base)

        # Sort by RCI desc within each bucket
        for bucket in (accumulation, trending, momentum, distribution):
            bucket.sort(key=lambda x: x['rci'], reverse=True)

        # ── [UPGRADE] AI consensus vote on top accumulation coin ─────────
        # UPGRADE-A2 FIX: was gated on `if accumulation` only — when accum empty but
        # trending has signals, users got no AI pick at all. Now falls back to trending.
        ai_pick_text = ""
        _vote_bucket = accumulation if accumulation else trending
        _vote_label  = "stealth accumulation" if accumulation else "trending"
        if _vote_bucket:
            top5_text = "\n".join([
                f"{i+1}. {c['symbol']} RCI:{c['rci']:.1f} 1h:{c['chg1h_str']} Smart:{c['smart_money']:.0f}"
                for i, c in enumerate(_vote_bucket[:5])
            ])
            vote_prompt = (
                "You are a crypto institutional analyst operating under OCE v13.0 execution discipline.\n"
                f"Top {_vote_label} candidates (quantitative RCI + price signal):\n\n"
                + top5_text +
                "\n\n=== SELECTION CRITERIA ===\n"
                "  1. Highest RCI = strongest institutional activity\n"
                "  2. 1h change flat or mildly negative = smart money absorbing quietly\n"
                "  3. Smart money score > 60 preferred\n"
                "Grounding rule: select from the list above only.\n\n"
                "Reply ONLY:\nSYMBOL: [symbol]\nREASON: [one sentence]"
            )
            votes: dict[str, int] = {}
            ai_reason = ""
            available_providers = [
                p for p in ["groq","gemini","mistral","cerebras","github"]
                if {"groq":GROQ_API_KEY,"gemini":GEMINI_API_KEY,"mistral":MISTRAL_API_KEY,
                    "cerebras":CEREBRAS_API_KEY,"github":GITHUB_TOKEN}.get(p,"").strip()
            ]
            for provider in available_providers[:3]:
                try:
                    resp = await asyncio.wait_for(ai_query(vote_prompt, {}, provider), timeout=10.0)
                    for line in resp.splitlines():
                        if line.upper().startswith("SYMBOL:"):
                            voted = line.split(":",1)[1].strip().upper()
                            votes[voted] = votes.get(voted, 0) + 1
                        if line.upper().startswith("REASON:") and not ai_reason:
                            ai_reason = line.split(":",1)[1].strip()
                except Exception as _e:
                    logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            if votes:
                top_pick = max(votes, key=votes.get)
                vote_bar = "🟢" * votes.get(top_pick, 0) + "⬜" * max(0, 3 - votes.get(top_pick, 0))
                ai_pick_text = (
                    f"\n🤖 *AI CONSENSUS* {vote_bar}\n"
                    f"Top Pick: *{top_pick}* ({votes.get(top_pick,0)}/3 agree)\n"
                    f"_{ai_reason}_\n"
                )

        # ── [UPGRADE] BTC regime context ─────────────────────────────────
        _reg_map = {
            "bull_expansion": "🟢 Bull Expansion — alts favour upside",
            "bull_pullback":  "🟡 Bull Pullback — accumulate on dips",
            "chop":           "🟡 Chop — reduce size, favour ACCUM only",
            "bear":           "🔴 Bear — capital preservation priority",
        }
        btc_reg_note = _reg_map.get(_btc_regime, "⚪ Unknown regime")
        btc_risk_note = ""
        if _btc_rsi >= 80:
            btc_risk_note = "\n⚠️ *Risk Flag:* BTC RSI is overbought — do not treat regime strength as a standalone entry trigger."

        # ── [UPGRADE] APEX sensor integration ────────────────────────────
        apex_urgency = _apex_state.get("urgency", 0.0)
        apex_note    = ""
        if apex_urgency >= 70:
            apex_note = f"\n⚡ *APEX ENGINE* 🔴 URGENT `{apex_urgency:.0f}/100` — event-driven signal active"
        elif apex_urgency >= 50:
            apex_note = f"\n⚡ *APEX ENGINE* 🟡 HIGH `{apex_urgency:.0f}/100` — elevated scan cadence"

        # ── Build output ──────────────────────────────────────────────────
        _pht_h24 = (datetime.now(timezone.utc).hour + 8) % 24
        _pht_min = datetime.now(timezone.utc).minute
        _ampm    = "AM" if _pht_h24 < 12 else "PM"
        _pht_h12 = _pht_h24 % 12 or 12
        _pht_str = f"{_pht_h12}:{_pht_min:02d} {_ampm} PHT"

        text = (
            f"💎 *ALPHA SIGNALS v8.2* — Institutional Capital Flow\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"*BTC Regime:* {btc_reg_note}  RSI `{_btc_rsi:.0f}`{apex_note}\n\n"
        )

        # ── ACCUMULATION section ──────────────────────────────────────────
        text += "🧠 *STEALTH ACCUMULATION* _(High RCI + Low Price Move)_\n"
        text += "_Smart money absorbing supply quietly — best entry signals_\n"
        if ai_pick_text:
            text += ai_pick_text
        text += "\n"
        if accumulation:
            for i, c in enumerate(accumulation[:6], 1):
                arrow = "📈" if c['change'] >= 0 else "📉"
                text += (
                    f"{i}. *{c['symbol']}*  {arrow} `{c['change']:+.2f}%`  1h: `{c['chg1h_str']}`\n"
                    f"   💰 {c['price_str']}  {c['peso']}\n"
                    f"   RCI: `{c['rci']:.1f}` · Smart: `{c['smart_money']:.0f}` · "
                    f"Liq: `{c['liquidity']:.2f}` · {c['state']}\n\n"
                )
        else:
            text += "No stealth accumulation detected right now.\n\n"

        text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        # ── TRENDING section ──────────────────────────────────────────────
        if trending:
            text += "📈 *TRENDING SIGNALS* _(RCI + Aligned Price Move)_\n"
            text += "_Confirmed institutional trend — ride with stops_\n\n"
            for i, c in enumerate(trending[:4], 1):
                text += (
                    f"{i}. *{c['symbol']}*  📈 `{c['change']:+.2f}%`\n"
                    f"   💰 {c['price_str']}  {c['peso']}\n"
                    f"   RCI: `{c['rci']:.1f}` · Smart: `{c['smart_money']:.0f}` · {c['state']}\n\n"
                )
            text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        # ── MOMENTUM / blow-off section ───────────────────────────────────
        if momentum:
            text += "🚀 *MOMENTUM ALERTS* _(Explosive Move + Very High RCI)_\n"
            text += "_Possible FOMO / institutional blow-off — risk both ways_\n\n"
            for i, c in enumerate(momentum[:3], 1):
                text += (
                    f"{i}. *{c['symbol']}*  🚀 `{c['change']:+.2f}%`\n"
                    f"   💰 {c['price_str']}  {c['peso']}\n"
                    f"   RCI: `{c['rci']:.1f}` · {c['state']} · ⚠️ Volatile\n\n"
                )
            text += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        # ── DISTRIBUTION section ──────────────────────────────────────────
        if distribution:
            text += "⚠️ *DISTRIBUTION WARNINGS* _(Smart Money Exiting)_\n"
            text += "_High price + fading efficiency = retail chase = danger_\n\n"
            for i, c in enumerate(distribution[:4], 1):
                text += (
                    f"{i}. *{c['symbol']}*  📈 `{c['change']:+.2f}%`  "
                    f"Eff: `{c['efficiency']:.0f}` Smart: `{c['smart_money']:.0f}`\n"
                    f"   💰 {c['price_str']}  {c['peso']}  · Avoid chasing\n\n"
                )
        else:
            text += "⚠️ *DISTRIBUTION:* None detected\n\n"

        text += (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Scanned: `{len(top_coins)}` coins · "
            f"ACCUM: `{len(accumulation)}` · "
            f"TREND: `{len(trending)}` · "
            f"DIST: `{len(distribution)}`\n"
            f"⏰ `{datetime.now(timezone.utc).strftime('%b %d %H:%M UTC')}` · _{_pht_str}_\n"
            f"⚠️ _RCI = Relative Capital Index · Not financial advice · DYOR_"
        )

        if stablecoin_excluded:
            text += (
                f"\n🛡️ *Compliance Filter:* excluded stablecoins from alpha ranking — "
                f"`{', '.join(stablecoin_excluded[:5])}`"
                + (" …" if len(stablecoin_excluded) > 5 else "")
                + "\n"
            )

        # ── UPGRADE-A1: SuperValidator Markov enrichment ─────────────────
        # Alpha had no Markov validation — Cross had it, Alpha didn't. Now consistent.
        if MARKOV_AVAILABLE and (accumulation or trending):
            try:
                _alpha_syms = [
                    c["symbol"] for c in (accumulation or trending)[:5]
                ]
                _alpha_bias = (
                    "BULLISH" if len(accumulation) >= len(distribution)
                    else "BEARISH"
                )
                _sv_alpha = _SUPER_VALIDATOR.validate(
                    current_regime   = _btc_regime if _btc_regime != "unknown" else "TRENDING",
                    momentum_quality = "HIGH" if _btc_rsi > 60 else "LOW" if _btc_rsi < 40 else "MEDIUM",
                    symbols          = _alpha_syms,
                    bias             = _alpha_bias,
                    ict_conf_mod     = 1.0,
                    ict_verdict_key  = "NEUTRAL",
                )
                text += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{_sv_alpha['super_compact']}\n"
                if _sv_alpha.get("super_gate_veto"):
                    text += "⛔ *MARKOV VETO — High-risk conditions detected.*\n"
            except Exception as _sv_a_e:
                logger.debug(f"[SUPER_MARKOV] alpha: {_sv_a_e}")

        capture_output(chat_id, text, "alpha")

        # ALPHA-BUG-1 FIX: was create_main_keyboard() — feedback validate button never appeared
        feedback_kb = get_feedback_inline_keyboard()
        try:
            await progress_msg.edit_text(
                text, parse_mode="Markdown", reply_markup=feedback_kb
            )
        except Exception:
            await update.message.reply_text(
                text, parse_mode="Markdown", reply_markup=feedback_kb
            )

    except Exception as e:
        logger.error(f"[ALPHA] Error in alpha_signals: {e}", exc_info=True)
        try:
            await progress_msg.edit_text(
                "❌ Error detecting alpha signals. Please try again.",
                reply_markup=create_main_keyboard()
            )
        except Exception:
            await update.message.reply_text(
                "❌ Error detecting alpha signals.", reply_markup=create_main_keyboard()
            )
