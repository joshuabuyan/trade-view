# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ai_assistant.py — 🤖 AI Assistant button handler + ai_choice callback
# Extracted from app.py (no logic changes).
# Connection: app.py does ↓
#   from ai_assistant import ai_assistant, ai_choice as _ai_choice_raw
#   ai_choice = safe_menu_handler(_ai_choice_raw)
#   # ai_assistant has no @safe_menu_handler — imported directly
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import logging
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


async def ai_assistant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle AI Assistant button — shows only providers with configured API keys."""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    create_main_keyboard = _app.create_main_keyboard
    GROQ_API_KEY         = _app.GROQ_API_KEY
    CEREBRAS_API_KEY     = _app.CEREBRAS_API_KEY
    GEMINI_API_KEY       = _app.GEMINI_API_KEY
    MISTRAL_API_KEY      = _app.MISTRAL_API_KEY
    GITHUB_TOKEN         = _app.GITHUB_TOKEN
    # ─────────────────────────────────────────────────────────────────────────

    _provider_configs = [
        ("Groq",     "groq",     GROQ_API_KEY,     "ai_groq"),
        ("Cerebras", "cerebras", CEREBRAS_API_KEY,  "ai_cerebras"),
        ("Gemini",   "gemini",   GEMINI_API_KEY,    "ai_gemini"),
        ("Mistral",  "mistral",  MISTRAL_API_KEY,   "ai_mistral"),
        ("GitHub",   "github",   GITHUB_TOKEN,      "ai_github"),
    ]
    available = [(name, cb) for name, ptype, key, cb in _provider_configs if key and key.strip()]

    if not available:
        await update.message.reply_text(
            "🤖 *AI ASSISTANT*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ *No AI providers configured.*\n\n"
            "Add at least one API key to your `.env` file:\n"
            "```\n"
            "GROQ_API_KEY=...\n"
            "CEREBRAS_API_KEY=...\n"
            "GEMINI_API_KEY=...\n"
            "MISTRAL_API_KEY=...\n"
            "GITHUB_TOKEN=...\n"
            "```\n\n"
            "All providers offer free tiers. Get keys at:\n"
            "• console.groq.com\n"
            "• inference.cerebras.ai\n"
            "• aistudio.google.com\n"
            "• console.mistral.ai",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
        return

    # Build rows of up to 2 buttons each
    rows = []
    for i in range(0, len(available), 2):
        row = [InlineKeyboardButton(name, callback_data=cb) for name, cb in available[i:i+2]]
        rows.append(row)
    keyboard = InlineKeyboardMarkup(rows)

    provider_list = " · ".join(name for name, _ in available)
    await update.message.reply_text(
        f"🤖 *AI ASSISTANT*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Ask anything about crypto markets.\n"
        f"Each provider uses its smartest free model.\n\n"
        f"*Available:* {provider_list}\n\n"
        f"Choose your AI provider:",
        parse_mode="Markdown",
        reply_markup=keyboard
    )


async def ai_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle AI provider selection — supports groq, cerebras, gemini, mistral."""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    user_ai_preference  = _app.user_ai_preference
    GROQ_API_KEY        = _app.GROQ_API_KEY
    CEREBRAS_API_KEY    = _app.CEREBRAS_API_KEY
    GEMINI_API_KEY      = _app.GEMINI_API_KEY
    MISTRAL_API_KEY     = _app.MISTRAL_API_KEY
    GITHUB_TOKEN        = _app.GITHUB_TOKEN
    _fb_discover_models = _app._fb_discover_models
    logger              = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    query   = update.callback_query
    chat_id = query.message.chat_id

    _cb_to_provider = {
        "ai_groq":     "groq",
        "ai_cerebras": "cerebras",
        "ai_gemini":   "gemini",
        "ai_mistral":  "mistral",
        "ai_github":   "github",
    }
    _provider_labels = {
        "groq":     "Groq",
        "cerebras": "Cerebras",
        "gemini":   "Gemini",
        "mistral":  "Mistral",
        "github":   "GitHub",
    }

    ai_provider = _cb_to_provider.get(query.data, "groq")
    user_ai_preference[chat_id] = ai_provider
    provider_name = _provider_labels.get(ai_provider, ai_provider.capitalize())

    await query.answer()

    # Auto-detect the model that will be used
    _AQ_KEYS = {
        "groq":     GROQ_API_KEY,
        "cerebras": CEREBRAS_API_KEY,
        "gemini":   GEMINI_API_KEY,
        "mistral":  MISTRAL_API_KEY,
        "github":   GITHUB_TOKEN,
    }
    api_key = _AQ_KEYS.get(ai_provider, "").strip()
    models  = await _fb_discover_models(ai_provider, api_key) if api_key else []
    model_info = models[0] if models else "auto"

    await query.edit_message_text(
        f"🤖 *AI ASSISTANT — {provider_name}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Model: `{model_info}`\n\n"
        f"💬 *Ask me anything about crypto:*\n"
        f"• Current market analysis\n"
        f"• Specific coin outlook\n"
        f"• TA/ICT/SMC concepts\n"
        f"• DeFi & on-chain insights\n\n"
        f"_Type your question now ↓_",
        parse_mode="Markdown"
    )

    context.user_data["ai_mode"] = True
