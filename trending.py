# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# trending.py — 🔥 Trending Coins button handler
# Extracted from app.py (no logic changes).
# Connection: app.py does ↓
#   from trending import trending_coins as _trending_raw
#   trending_coins = safe_menu_handler(_trending_raw)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import asyncio
import logging
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest

logger = logging.getLogger(__name__)


async def trending_coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Trending Coins button - optimized with async"""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    clear_market_messages = _app.clear_market_messages
    create_main_keyboard  = _app.create_main_keyboard
    fetch_json            = _app.fetch_json
    COINGECKO_BASE_URL    = _app.COINGECKO_BASE_URL
    capture_output        = _app.capture_output
    MARKOV_AVAILABLE      = _app.MARKOV_AVAILABLE
    _SUPER_VALIDATOR      = _app._SUPER_VALIDATOR
    _markov_ai_consensus  = _app._markov_ai_consensus
    logger                = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    chat_id = update.effective_chat.id
    await clear_market_messages(chat_id, context)

    progress_msg = await update.message.reply_text(
        "🔍 Fetching trending coins...",
        reply_markup=create_main_keyboard()
    )

    try:
        # Fetch trending data asynchronously
        trending = await asyncio.to_thread(
            fetch_json,
            f"{COINGECKO_BASE_URL}/search/trending"
        )

        if not trending or 'coins' not in trending:
            try:
                await progress_msg.edit_text(
                    "❌ Failed to fetch trending data",
                    reply_markup=create_main_keyboard()
                )
            except BadRequest:
                await update.message.reply_text(
                    "❌ Failed to fetch trending data",
                    reply_markup=create_main_keyboard()
                )
            return

        text = "🔥 *TRENDING COINS*\n\n"

        # Batch-fetch live prices for trending coins
        coin_ids_for_prices = [
            item['item'].get('id', '') for item in trending['coins'][:10]
            if item['item'].get('id')
        ]
        live_prices = {}
        if coin_ids_for_prices:
            try:
                _pr = await asyncio.to_thread(
                    fetch_json,
                    f"{COINGECKO_BASE_URL}/simple/price",
                    {"ids": ",".join(coin_ids_for_prices),
                     "vs_currencies": "usd",
                     "include_24hr_change": "true",
                     "include_market_cap": "true"}
                )
                if _pr:
                    live_prices = _pr
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
        for i, item in enumerate(trending['coins'][:10], 1):
            coin = item['item']
            rank = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            cid  = coin.get('id', '')
            pdata = live_prices.get(cid, {})
            price = pdata.get('usd')
            chg   = pdata.get('usd_24h_change')
            mcap  = pdata.get('usd_market_cap')

            price_str = f"${price:,.4f}" if price and price < 1 else (f"${price:,.2f}" if price else "—")
            chg_str   = (f"{'📈' if chg >= 0 else '📉'} {chg:+.2f}%" if chg is not None else "")
            mcap_str  = (f"${mcap/1e9:.2f}B" if mcap and mcap >= 1e9 else
                         (f"${mcap/1e6:.1f}M" if mcap else ""))

            text += (
                f"{rank} *{coin['symbol'].upper()}* — {coin['name']}\n"
                f"   💰 {price_str}  {chg_str}\n"
                f"   📊 Rank: #{coin.get('market_cap_rank', 'N/A')}  MCap: {mcap_str}\n"
                f"   🔥 Trend Score: {coin.get('score', 0) + 1}\n\n"
            )

        text += f"⏰ _Updated: {datetime.now().strftime('%b %d, %Y %I:%M %p UTC')}_\n"
        text += "⚠️ _Trending ≠ buy signal. DYOR._\n"

        # ── Super Markov: TrendBrain validation ───────────────────────
        if MARKOV_AVAILABLE:
            try:
                _sv_trend = _SUPER_VALIDATOR.validate(
                    current_regime   = "TRENDING",
                    momentum_quality = "HIGH",
                    symbols          = [item['item'].get('symbol','').upper()
                                        for item in trending.get('coins', [])[:5]],
                )
                text += f"\n{_sv_trend['super_compact']}\n"
                asyncio.create_task(
                    _markov_ai_consensus(
                        symbol    = "MARKET",
                        ict_state = "BOS_BULL",
                        bias      = "BULLISH",
                        conf      = 60,
                        smc       = 50,
                        gates     = 6,
                        n_samples = 0,
                    ),
                    name="markov_ai_trending"
                )
            except Exception as _sv_tr_e:
                logger.debug(f"[SUPER_MARKOV] trending: {_sv_tr_e}")

        # Capture for Feedback engine
        capture_output(chat_id, text, "trending")

        try:
            await progress_msg.edit_text(
                text,
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )
        except BadRequest:
            await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=create_main_keyboard()
            )

    except Exception as e:
        logger.error(f"Error fetching trending: {e}", exc_info=True)
        try:
            await progress_msg.edit_text(
                "❌ Error fetching trending coins",
                reply_markup=create_main_keyboard()
            )
        except BadRequest:
            await update.message.reply_text(
                "❌ Error fetching trending coins",
                reply_markup=create_main_keyboard()
            )
