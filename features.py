# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# features.py — 🚀 Features button handler
# Extracted from app.py (no logic changes).
# Connection: app.py does ↓
#   from features import features_button_handler as _features_raw
#   from features import _FEATURES_TARGET_LABELS, _COMPLEXITY_EMOJI, _PRIORITY_EMOJI
#   features_button_handler = safe_menu_handler(_features_raw)
# NOTE: _FEATURES_TARGET_LABELS/_COMPLEXITY_EMOJI/_PRIORITY_EMOJI are also
#       imported by feedback.py — define here, import there.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import logging
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# ── Shared constants (also imported by feedback.py for fb_feat_detail) ────────
_FEATURES_TARGET_LABELS = {
    "trio":            "📡 Trio Output",
    "cross":           "⚔️ Cross Analysis",
    "sector_rotation": "🌊 Sector Rotation",
    "trending":        "🔥 Trending",
    "alpha":           "💎 Alpha Signals",
    "ta":              "📊 Technical Analysis",
    "dex":             "🦎 DEX Scanner",
    "ai_assistant":    "🤖 AI Assistant",
    "new":             "🆕 New Button",
}
_COMPLEXITY_EMOJI = {"EASY": "🟢", "MEDIUM": "🟡", "HARD": "🔴"}
_PRIORITY_EMOJI   = {"HIGH": "🔥", "MEDIUM": "⚡", "LOW": "💡"}


async def features_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle 🚀 Features menu button — show AI-sourced upgrade suggestions."""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    feedback_store       = _app.feedback_store
    create_main_keyboard = _app.create_main_keyboard
    _md_escape           = _app._md_escape
    _fb_split_message    = _app._fb_split_message
    logger               = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    chat_id = update.effective_chat.id
    store   = feedback_store.get(chat_id, {})
    report  = store.get("last_report")

    if not report:
        # No report yet — explain how it works
        msg = (
            "🚀 *CRYPTEX FEATURES ENGINE*\n\n"
            "Each AI validator analyses your bot outputs and suggests specific new features, "
            "improvements to existing buttons, and new Telegram menu buttons.\n\n"
            "📋 *How to use:*\n"
            "1. Press any button (Trio, Cross, DEX…)\n"
            "2. Press *📋 Feedback* → *🔍 Validate Last Output*\n"
            "3. Come back here — each validator's suggestions appear\n\n"
            "The validators (Groq, Cerebras, Gemini, Mistral, GitHub) each bring different "
            "expertise: one might focus on new data sources, another on UI improvements, "
            "another on algorithm upgrades.\n\n"
            "_Run a validation first to see suggestions._"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 Go to Feedback", callback_data="go_feedback"),
        ]])
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)
        return

    suggestions = report.get("feature_suggestions", [])
    validators_ok = report.get("validators_ok", 0)

    if not suggestions:
        await update.message.reply_text(
            "🚀 *FEATURES ENGINE*\n\n"
            "No feature suggestions in the last report.\n"
            "Try running a *Feedback → Validate* again — the AI validators will generate suggestions.",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
        return

    # ── Group by target menu ──────────────────────────────────────────────────
    by_target: dict[str, list[dict]] = {}
    for fs in suggestions:
        t = fs.get("target_menu", "new")
        by_target.setdefault(t, []).append(fs)

    total = len(suggestions)
    output_label = report.get("output_type", "Last Output")

    header = (
        f"🚀 *FEATURES ENGINE — AI UPGRADE SUGGESTIONS*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Source: _{_md_escape(output_label)}_ validation\n"
        f"Validators: *{validators_ok}* responded · *{total}* suggestions\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(header, parse_mode="Markdown", reply_markup=create_main_keyboard())

    # ── Send one message per target group ────────────────────────────────────
    for target, items in sorted(by_target.items(), key=lambda x: {"trio":0,"cross":1,"ta":2,"dex":3,"new":99}.get(x[0],5)):
        # Sort within group: HIGH priority + EASY complexity first
        items.sort(key=lambda x: (
            {"HIGH":0,"MEDIUM":1,"LOW":2}.get(x.get("priority","LOW"),2),
            {"EASY":0,"MEDIUM":1,"HARD":2}.get(x.get("implementation_complexity","MEDIUM"),1)
        ))
        target_label = _FEATURES_TARGET_LABELS.get(target, f"🆕 {target.replace('_',' ').title()}")
        lines = [f"*{_md_escape(target_label)} SUGGESTIONS ({len(items)}):*", ""]

        for i, fs in enumerate(items, 1):
            pri   = fs.get("priority", "MEDIUM")
            cplx  = fs.get("implementation_complexity", "MEDIUM")
            vname = fs.get("_validator", "AI")
            title = fs.get("title", "Untitled")
            desc  = fs.get("description", "")[:300]
            btn   = fs.get("button_label", "")
            srcs  = fs.get("data_sources", [])

            lines += [
                f"{_PRIORITY_EMOJI.get(pri,'💡')} *{i}. {_md_escape(title)}*",
                f"  └ Suggested by: _{_md_escape(vname)}_ · {_COMPLEXITY_EMOJI.get(cplx,'🟡')} {cplx}",
            ]
            if btn:
                lines.append(f"  🔘 Button label: `{_md_escape(btn)}`")
            lines.append(f"  {_md_escape(desc)}")
            if srcs:
                lines.append(f"  📡 Data: {_md_escape(', '.join(srcs[:3]))}")
            lines.append("")

        # Inline buttons: Detail + (if HIGH priority) a "request implementation" option
        high_items = [fs for fs in items if fs.get("priority") == "HIGH"]
        kb_rows = []
        for fs in high_items[:3]:
            idx = suggestions.index(fs)
            kb_rows.append([InlineKeyboardButton(
                f"🔍 Deep-dive: {fs.get('title','?')[:30]}",
                callback_data=f"fb_feat_detail:{idx}"
            )])
        if kb_rows:
            kb_rows.append([InlineKeyboardButton("📥 Export All Suggestions", callback_data="fb_feat_export")])
            kb = InlineKeyboardMarkup(kb_rows)
        else:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("📥 Export All Suggestions", callback_data="fb_feat_export")]])

        msg_text = "\n".join(lines)
        # U3 FIX: use _fb_split_message (line-boundary aware) instead of raw byte slice
        for i, chunk in enumerate(_fb_split_message(msg_text, limit=4000)):
            _kb = kb if i == 0 else None   # keyboard on first chunk only
            await update.message.reply_text(chunk, parse_mode="Markdown", reply_markup=_kb)

    # ── Summary footer ────────────────────────────────────────────────────────
    hi  = sum(1 for fs in suggestions if fs.get("priority") == "HIGH")
    mid = sum(1 for fs in suggestions if fs.get("priority") == "MEDIUM")
    lo  = sum(1 for fs in suggestions if fs.get("priority") == "LOW")
    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔥 HIGH: *{hi}*  ⚡ MEDIUM: *{mid}*  💡 LOW: *{lo}*\n"
        f"\nRun *Feedback → Validate* again after any bot update to refresh suggestions."
    )
    await update.message.reply_text(footer, parse_mode="Markdown", reply_markup=create_main_keyboard())
