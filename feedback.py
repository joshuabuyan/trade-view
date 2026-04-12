# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# feedback.py — 📋 Feedback button + callback + evidence handlers
# Extracted from app.py (no logic changes).
# Connection: app.py does ↓
#   from feedback import (
#       feedback_button_handler   as _feedback_btn_raw,
#       feedback_callback_handler as _feedback_cb_raw,
#       feedback_evidence_handler,
#       get_feedback_inline_keyboard,
#   )
#   feedback_button_handler   = safe_menu_handler(_feedback_btn_raw)
#   feedback_callback_handler = safe_menu_handler(_feedback_cb_raw)
#   # feedback_evidence_handler has no decorator — used directly in handle_message
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

# Constants shared with features.py (defined there, imported here)
from features import _FEATURES_TARGET_LABELS, _COMPLEXITY_EMOJI, _PRIORITY_EMOJI

logger = logging.getLogger(__name__)


def get_feedback_inline_keyboard():
    """Inline keyboard appended to bot outputs for quick validation."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📋 Validate This Output", callback_data="fb_validate"),
        InlineKeyboardButton("📥 Download Report",      callback_data="fb_download"),
    ]])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# feedback_button_handler — menu button (📋 Feedback)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def feedback_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle 📋 Feedback menu button."""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    feedback_store       = _app.feedback_store
    create_main_keyboard = _app.create_main_keyboard
    _fb_rank_providers   = _app._fb_rank_providers
    _FB_OUTPUT_TYPES     = _app._FB_OUTPUT_TYPES
    logger               = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    chat_id   = update.effective_chat.id

    # ── Block feedback while TA is running — prevents partial output validation ─
    if context.user_data.get("ta_running"):
        await update.message.reply_text(
            "⏳ *Analysis still running.*\n\nPlease wait for the TA to finish before opening Feedback.",
            parse_mode="Markdown",
            reply_markup=create_main_keyboard()
        )
        return

    store     = feedback_store.get(chat_id, {})
    last_type = store.get("last_output_type", "unknown")
    # Use cached provider ranking — avoids live API call on every menu button press
    providers = store.get("_cached_providers") or await _fb_rank_providers()
    store["_cached_providers"] = providers
    provider_names = ", ".join(p["name"] for p in providers) if providers else "None configured"

    TRIO_LABELS = {"pin": "Pin Message", "market_overview": "Market Overview", "news": "News Output"}
    trio_outputs = store.get("trio_outputs", {})
    capture_ts   = store.get("capture_ts", {})

    # ── Build "last captured output" display ─────────────────────────────────
    if last_type == "trio":
        # Show each trio member with ✅/⏳ status
        trio_parts = []
        for key in ["pin", "market_overview", "news"]:
            mark = "✅" if (key in trio_outputs and trio_outputs[key]) else "⏳"
            trio_parts.append(f"{mark} {TRIO_LABELS[key]}")
        captured_display = "Trio — " + " · ".join(trio_parts)
    elif last_type == "unknown" or not store.get("last_output"):
        captured_display = "None yet — use any feature first"
    else:
        # Non-trio: show human-readable label + capture time
        label = _FB_OUTPUT_TYPES.get(last_type, last_type.replace("_", " ").title())
        ts    = capture_ts.get(last_type)
        ts_str = ts.strftime("%H:%M:%S UTC") if ts else ""
        captured_display = f"{label}" + (f"  _(captured {ts_str})_" if ts_str else "")

    msg = (
        "📋 *CRYPTEX-QA VALIDATION ENGINE v5.0*\n\n"
        f"Active AI Validators: *{provider_names}*\n"
        f"Last captured output: *{captured_display}*\n\n"
        "📌 *Trio Rule:* Pin Message · Market Overview · News are always validated together.\n\n"
        "*Pipeline:* RAG → Synthesis → 80/20 Trim → Probability Modeling → Recursive Validation\n"
        "*Dimensions:* Accuracy · Completeness · Consistency · Timeliness · Reliability\n"
        "*Intelligence:* ICT/SMC · Wyckoff · Elliott Wave · On-Chain · Fundamentals · Quant Stats\n\n"
        "What would you like to do?"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Validate Last Output",  callback_data="fb_validate")],
        [InlineKeyboardButton("📥 Download Last Report",  callback_data="fb_download")],
    ])
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# feedback_callback_handler — inline button callbacks (fb_validate, fb_download, etc.)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def feedback_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all fb_* callback buttons."""
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    feedback_store           = _app.feedback_store
    create_main_keyboard     = _app.create_main_keyboard
    _fb_rank_providers       = _app._fb_rank_providers
    _fb_discover_models      = _app._fb_discover_models
    _fb_run_validation       = _app._fb_run_validation
    _fb_format_telegram      = _app._fb_format_telegram
    _fb_send_report          = _app._fb_send_report
    _fb_send_thumbnails      = _app._fb_send_thumbnails
    _fb_build_txt            = _app._fb_build_txt
    _fb_build_gemini_log     = _app._fb_build_gemini_log
    _FB_OUTPUT_TYPES         = _app._FB_OUTPUT_TYPES
    _md_escape               = _app._md_escape
    logger                   = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    query   = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data    = query.data

    store = feedback_store.setdefault(chat_id, {
        "last_output": "", "last_output_type": "unknown",
        "last_report": None, "awaiting_evidence": False,
    })

    # ── Validate ────────────────────────────────────────────────────────────
    if data == "fb_validate":
        output_text = store.get("last_output", "")
        output_type = store.get("last_output_type", "unknown")
        if not output_text:
            await query.edit_message_text(
                "⚠️ No output captured yet.\nUse any bot feature first, then press Feedback.",
                parse_mode="Markdown"
            )
            return

        # ── TRIO ENFORCEMENT ────────────────────────────────────────────────
        TRIO_LABELS = {"pin": "Pin Message", "market_overview": "Market Overview", "news": "News Output"}
        if output_type == "trio":
            trio_outputs = store.get("trio_outputs", {})
            captured = [k for k in ["pin", "market_overview", "news"] if k in trio_outputs and trio_outputs[k]]
            missing  = [TRIO_LABELS[k] for k in ["pin", "market_overview", "news"] if k not in trio_outputs or not trio_outputs[k]]
            if missing and len(captured) == 0:
                # Nothing captured at all
                await query.edit_message_text(
                    "⚠️ *No trio outputs captured yet.*\n\n"
                    "Press /start or wait for the market overview to load first.",
                    parse_mode="Markdown"
                )
                return
            elif missing:
                # Partially captured — warn but proceed with what we have
                await query.edit_message_text(
                    f"⚠️ *Trio partially ready* ({len(captured)}/3 captured)\n\n"
                    f"Still waiting for: " + ", ".join(missing) + "\n\n"
                    f"Validating with available outputs ({len(captured)} member(s))...",
                    parse_mode="Markdown"
                )
            type_label = "Pin Message · Market Overview · News Output (Trio)"
        else:
            type_label = _FB_OUTPUT_TYPES.get(output_type, "Output")
        # ────────────────────────────────────────────────────────────────────

        # Use cached provider list — avoids redundant live API ranking call
        providers     = store.get("_cached_providers") or await _fb_rank_providers()
        store["_cached_providers"] = providers
        # Discover models for all providers in parallel — not sequential
        model_lists   = await asyncio.gather(*[_fb_discover_models(p["type"], p["key"]) for p in providers])
        provider_info = ""
        for p, models in zip(providers, model_lists):
            provider_info += f"\n  • {p['name']} → {models[0] if models else 'unknown'}"

        await query.edit_message_text(
            f"🔍 *Validating {type_label}...*\n\n"
            f"AI Validators:{provider_info}\n\n"
            f"*Pipeline:* RAG → Synthesis → 80/20 → Probability Modeling → Recursive Validation\n"
            f"*Intelligence:* ICT/SMC · Wyckoff · Elliott Wave · On-Chain · Quant Stats\n"
            f"🔎 Auto-searching YouTube & web for ground-truth evidence...\n"
            f"Please wait ~20s...",
            parse_mode="Markdown"
        )
        try:
            report = await _fb_run_validation(output_text, output_type, chat_id=chat_id)
            store["last_report"] = report
            summary = _fb_format_telegram(report)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Download Full Report", callback_data="fb_download")],
            ])
            await _fb_send_report(query.message, summary, kb)
            await _fb_send_thumbnails(query.message, report)
        except Exception as e:
            logger.exception(f"Feedback validation error: {e}")
            await query.message.reply_text(
                f"❌ Validation failed: {str(e)[:200]}\nPlease try again.",
                reply_markup=create_main_keyboard()
            )

    # ── Download ─────────────────────────────────────────────────────────────
    elif data == "fb_download":
        report = store.get("last_report")
        if not report:
            await query.message.reply_text("⚠️ No report available. Run validation first.", reply_markup=create_main_keyboard())
            return
        try:
            txt_content = _fb_build_txt(report)
            ts          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename    = f"qa_report_{report['output_type_key']}_{ts}.txt"
            filepath    = f"/tmp/{filename}"
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(txt_content)
            with open(filepath, "rb") as f:
                file_bytes = f.read()

            # Delete BEFORE sending — we already have the bytes in memory
            try:
                os.remove(filepath)
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            def _plain(val):
                return re.sub(r'[*_`\[\]()~>#+=|{}.!\-\\]', '', str(val))

            caption = (
                f"QA Validation Report\n"
                f"Type: {_plain(report['output_type'])}\n"
                f"Verdict: {_plain(report['verdict'])}\n"
                f"Score: {_plain(report['composite_score'])}/100"
            )
            await query.message.reply_document(
                document=file_bytes,
                filename=filename,
                caption=caption,
                parse_mode=None,
            )
        except Exception as e:
            logger.exception(f"Report download error: {e}")
            await query.message.reply_text(f"❌ Download failed: {str(e)[:200]}", reply_markup=create_main_keyboard())

    # ── Features: deep-dive on a single suggestion ───────────────────────────
    elif data.startswith("fb_feat_detail:"):
        report = store.get("last_report")
        if not report:
            await query.message.reply_text("⚠️ No report. Run Feedback → Validate first.")
            return
        try:
            idx = int(data.split(":")[1])
            suggestions = report.get("feature_suggestions", [])
            if idx >= len(suggestions):
                await query.message.reply_text("⚠️ Feature not found in report.")
                return
            fs    = suggestions[idx]
            pri   = fs.get("priority", "MEDIUM")
            cplx  = fs.get("implementation_complexity", "MEDIUM")
            vname = fs.get("_validator", "AI")
            title = fs.get("title", "Untitled")
            desc  = fs.get("description", "")
            btn   = fs.get("button_label", "")
            srcs  = fs.get("data_sources", [])
            target = _FEATURES_TARGET_LABELS.get(fs.get("target_menu","new"), "New Feature")

            lines = [
                f"🔍 *FEATURE DEEP-DIVE*",
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                f"*{_md_escape(title)}*",
                f"",
                f"📌 *Target:* {_md_escape(target)}",
                f"🎯 *Priority:* {_PRIORITY_EMOJI.get(pri,'💡')} {pri}",
                f"🔧 *Complexity:* {_COMPLEXITY_EMOJI.get(cplx,'🟡')} {cplx}",
                f"🤖 *Suggested by:* {_md_escape(vname)}",
            ]
            if btn:
                lines += [f"", f"🔘 *Button label:* `{_md_escape(btn)}`"]
            lines += [
                f"",
                f"📝 *Description:*",
                f"{_md_escape(desc)}",
            ]
            if srcs:
                lines += [f"", f"📡 *Data Sources Required:*"]
                for s in srcs[:5]:
                    lines.append(f"  • {_md_escape(s)}")

            lines += [
                f"",
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                f"_To implement: share this with your developer or ask AI Assistant_"
            ]
            await query.message.reply_text(
                "\n".join(lines),
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📥 Export All Suggestions", callback_data="fb_feat_export")
                ]])
            )
        except Exception as e:
            logger.exception(f"feat_detail error: {e}")
            await query.message.reply_text(f"❌ Error loading feature detail: {str(e)[:200]}")

    # ── Features: export all suggestions as text file ────────────────────────
    elif data == "fb_feat_export":
        report = store.get("last_report")
        if not report:
            await query.message.reply_text("⚠️ No report. Run Feedback → Validate first.")
            return
        try:
            suggestions = report.get("feature_suggestions", [])
            if not suggestions:
                await query.message.reply_text("⚠️ No feature suggestions in the last report.")
                return
            lines = [
                "CRYPTEX FEATURES ENGINE — AI UPGRADE SUGGESTIONS",
                f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
                f"Source: {report.get('output_type','Unknown')} validation",
                f"Validators: {report.get('validators_ok',0)} responded",
                "=" * 60, "",
            ]
            for i, fs in enumerate(suggestions, 1):
                lines += [
                    f"[{i}] {fs.get('title','Untitled')}",
                    f"  Priority:   {fs.get('priority','?')}",
                    f"  Complexity: {fs.get('implementation_complexity','?')}",
                    f"  Target:     {fs.get('target_menu','?')}",
                    f"  Button:     {fs.get('button_label','')}",
                    f"  Validator:  {fs.get('_validator','AI')}",
                    f"  Description:",
                    f"    {fs.get('description','')}",
                    f"  Data Sources: {', '.join(fs.get('data_sources',[]))}",
                    "",
                ]
            txt = "\n".join(lines)
            ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            fname = f"features_{ts}.txt"
            fpath = f"/tmp/{fname}"
            with open(fpath, "w", encoding="utf-8") as f_out:
                f_out.write(txt)
            with open(fpath, "rb") as f_in:
                fbytes = f_in.read()
            try:
                os.remove(fpath)
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            await query.message.reply_document(
                document=fbytes,
                filename=fname,
                caption=f"CRYPTEX Features Suggestions — {len(suggestions)} items from AI validators",
                parse_mode=None,
            )
        except Exception as e:
            logger.exception(f"feat_export error: {e}")
            await query.message.reply_text(f"❌ Export failed: {str(e)[:200]}")

    # ── go_feedback shortcut (from Features page) ─────────────────────────────
    elif data == "go_feedback":
        await query.message.reply_text(
            "📋 Press *📋 Feedback* on the keyboard below to start validation.",
            parse_mode="MarkdownV2",
            reply_markup=create_main_keyboard()
        )

    elif data == "fb_gemini_log":
        report = store.get("last_report")
        if not report:
            await query.message.reply_text("⚠️ No report available. Run validation first.", reply_markup=create_main_keyboard())
            return
        try:
            txt_content = _fb_build_gemini_log(report)
            ts          = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename    = f"gemini_log_{report['output_type_key']}_{ts}.txt"
            filepath    = f"/tmp/{filename}"
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(txt_content)
            with open(filepath, "rb") as f:
                file_bytes = f.read()
            try:
                os.remove(filepath)
            except Exception as _e:
                logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
            def _plain(val):
                return re.sub(r'[*_`\[\]()~>#+=|{}.!\-\\]', '', str(val))

            caption = (
                f"Gemini Validator Log\n"
                f"Type: {_plain(report['output_type'])}\n"
                f"Generated: {report['generated_at'][:19].replace('T',' ')} UTC"
            )
            await query.message.reply_document(
                document=file_bytes,
                filename=filename,
                caption=caption,
                parse_mode=None,
            )
        except Exception as e:
            logger.exception(f"Gemini log download error: {e}")
            await query.message.reply_text(f"❌ Gemini log download failed: {str(e)[:200]}", reply_markup=create_main_keyboard())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# feedback_evidence_handler — intercepts user message when awaiting_evidence=True
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def feedback_evidence_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Called from handle_message. Returns True if message consumed by feedback engine.
    Intercepts user messages when awaiting_evidence=True.
    """
    # ── Lazy app imports (avoids circular import at module load) ──────────────
    import app as _app
    feedback_store      = _app.feedback_store
    _fb_extract_urls    = _app._fb_extract_urls
    _fb_fetch_url       = _app._fb_fetch_url
    _fb_rank_providers  = _app._fb_rank_providers
    _fb_run_validation  = _app._fb_run_validation
    _fb_send_report     = _app._fb_send_report
    _fb_format_telegram = _app._fb_format_telegram
    _fb_send_thumbnails = _app._fb_send_thumbnails
    logger              = _app.logger
    # ─────────────────────────────────────────────────────────────────────────

    chat_id = update.effective_chat.id
    store   = feedback_store.get(chat_id, {})
    if not store.get("awaiting_evidence"):
        return False

    text        = update.message.text or ""
    output_text = store.get("last_output", "")
    output_type = store.get("last_output_type", "unknown")

    # Fetch any URLs in the message
    urls    = _fb_extract_urls(text)
    parts   = []
    if text:
        parts.append(f"User written input:\n{text}")
    if urls:
        fetch_msg = await update.message.reply_text(
            f"📎 Fetching {len(urls)} URL(s)...", parse_mode="Markdown"
        )
        for url in urls[:5]:
            parts.append(await _fb_fetch_url(url))
        try:
            await fetch_msg.delete()
        except Exception as _e:
            logger.debug(f"[SILENT_EX] {type(_e).__name__}: {_e}")
    evidence = "\n\n".join(parts)
    providers = await _fb_rank_providers()
    pnames    = ", ".join(p["name"] for p in providers) if providers else "None"

    await update.message.reply_text(
        f"🔍 *Re-validating with your evidence...*\n"
        f"Validators: {pnames}\nPlease wait ~15s...",
        parse_mode="Markdown"
    )
    try:
        report = await _fb_run_validation(output_text, output_type, evidence, chat_id=chat_id)
        store["last_report"]      = report
        store["awaiting_evidence"] = False
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📥 Download Full Report", callback_data="fb_download")
        ]])
        await _fb_send_report(update.message, _fb_format_telegram(report), kb)
        await _fb_send_thumbnails(update.message, report)
    except Exception as e:
        logger.exception(f"Evidence re-validation error: {e}")
        store["awaiting_evidence"] = False
        await update.message.reply_text(f"❌ Re-validation failed: {str(e)[:200]}")

    return True  # message consumed, do not process further
