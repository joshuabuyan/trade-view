import asyncio
import html
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, quote as _url_quote

import feedparser
import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

logger = logging.getLogger(__name__)

AIRDROP_TOP_N = int(os.getenv("AIRDROP_TOP_N", "10"))
AIRDROP_TIMEOUT = int(os.getenv("AIRDROP_TIMEOUT", "12"))
AIRDROP_X_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN") or os.getenv("X_BEARER_TOKEN") or ""
COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
REQUEST_HEADERS = {"User-Agent": "CRYPTEX-AIRDROP/2.0"}

FILTER_TOP = "top"
FILTER_AIRDROPS = "airdrops"
FILTER_EVENTS = "events"
FILTER_SOON = "soon"
FILTER_LOW_CROWD = "low_crowd"
FILTER_NOT_LISTED = "not_listed"

DEFAULT_HTML_SOURCES = [
    ("airdropsio", "https://airdrops.io/latest/", "airdrop"),
    ("cmc_guide", "https://coinmarketcap.com/academy/categories/airdrop-guide", "airdrop"),
]
DEFAULT_EVENT_HTML_SOURCES = [
    ("cmc_calendar", "https://coinmarketcap.com/academy/", "event"),
]
DEFAULT_RSS_URLS = [u.strip() for u in os.getenv("AIRDROP_RSS_URLS", "").split(",") if u.strip()]
DEFAULT_EVENT_RSS_URLS = [u.strip() for u in os.getenv("AIRDROP_EVENT_RSS_URLS", "").split(",") if u.strip()]

# BUG-FIX: removed \-.*$ branch — it destroyed hyphenated names like "Layer3-Protocol".
# Trailing hyphens/pipes are already stripped by the `strip(' -:|')` call in _clean_name.
NAME_CLEAN_RE = re.compile(r"\s+|\|.*$")
URL_RE = re.compile(r'https?://[^\s)\]">]+')
X_RE = re.compile(r'https?://(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]+)/?')
MONTH_DATE_RE = re.compile(r'(?i)\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,?\s+\d{4})?\b')
ISO_DATE_RE = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')
Q_DATE_RE = re.compile(r'\bQ[1-4]\b', re.I)

EVENT_TERMS = {
    "token sale": "token_sale",
    "token release": "token_release",
    "mainnet release": "mainnet_release",
    "mainnet": "mainnet_release",
    "listing": "listing",
    "unlock": "unlock",
    "snapshot": "snapshot",
    "claim": "airdrop",
    "airdrop": "airdrop",
    "quest": "airdrop",
}
EVENT_LABELS = {
    "airdrop": "Airdrop",
    "token_sale": "Token Sale",
    "token_release": "Token Release",
    "mainnet_release": "Mainnet Release",
    "listing": "Listing",
    "unlock": "Unlock",
    "snapshot": "Snapshot",
    "unknown": "Opportunity",
}


@dataclass
class OpportunityCandidate:
    name: str
    source: str
    source_url: str
    kind: str = "airdrop"
    official_url: str = ""
    tasks_url: str = ""
    x_url: str = ""
    status: str = "🟡 POTENTIAL"
    validity: int = 50
    early_opportunity: int = 50
    actionability: int = 45
    event_clarity: int = 30
    crowdedness: str = "🟡 MEDIUM"
    crowdedness_penalty: int = 0
    social_mentions: int = 0
    unique_accounts: int = 0
    hype_level: str = "Low"
    start_date: str = "TBA"
    claim_date: str = "TBA"
    snapshot_date: str = "TBA"
    deadline: str = "TBA"
    listing_status: str = "Unknown"
    cex_status: str = "Unknown"
    dex_status: str = "Unknown"
    token_live: str = "Unknown"
    listed: bool = False
    ai_consensus: str = "N/A"
    markov_confidence: str = "N/A"
    reasons: list[str] = field(default_factory=list)
    what_to_do: list[str] = field(default_factory=list)
    score: int = 0


def _clean_name(raw: str) -> str:
    raw = html.unescape(raw or "").strip()
    raw = re.sub(r'<[^>]+>', ' ', raw)
    raw = NAME_CLEAN_RE.sub(' ', raw).strip(' -:|')
    raw = re.sub(r'\(.*?\)', ' ', raw)
    raw = re.sub(r'\s{2,}', ' ', raw)
    return raw[:80]


# BUG-FIX: pre-strip <script>, <style>, <noscript>, <template>, <svg> blocks
# from raw HTML before any regex scanning. These blocks contain JSON-LD
# structured data (e.g. {"@context":"https://schema.org"...}) whose plain text
# leaks into anchor-tag captures when the re.S DOTALL flag is active.
_NOISE_BLOCK_RE = re.compile(
    r'<(script|style|noscript|template|svg)[^>]*>.*?</\1>',
    re.I | re.S,
)


def _strip_noise_blocks(html_text: str) -> str:
    """Remove script/style/noscript/template/svg blocks to prevent JSON-LD pollution."""
    return _NOISE_BLOCK_RE.sub('', html_text or '')


# BUG-FIX: reject strings that are clearly not human-readable project names.
# Catches JSON-LD, bare URLs, schema.org fragments, and garbage that slips
# through _clean_name when the HTML parser is bypassed.
_JUNK_NAME_RE = re.compile(r'[{}\[\]@]|https?://', re.I)


def _is_valid_name(name: str) -> bool:
    """Return False if name looks like JSON-LD, a URL, or structured-data noise."""
    if not name or len(name) < 2 or len(name) > 80:
        return False
    if _JUNK_NAME_RE.search(name):
        return False
    # Must contain at least one regular ASCII letter
    if not re.search(r'[A-Za-z]', name):
        return False
    return True


def _safe_get(url: str) -> str:
    try:
        r = requests.get(url, timeout=AIRDROP_TIMEOUT, headers=REQUEST_HEADERS)
        if r.ok:
            return r.text
    except Exception as e:
        logger.debug("[AIRDROP] GET failed %s: %s", url, e)
    return ""


def _extract_links(text: str) -> list[str]:
    return list(dict.fromkeys(URL_RE.findall(text or "")))


def _extract_dates(text: str) -> dict[str, str]:
    out = {"start": "TBA", "claim": "TBA", "snapshot": "TBA", "deadline": "TBA"}
    lower = (text or "").lower()
    dates = MONTH_DATE_RE.findall(text or "") + ISO_DATE_RE.findall(text or "") + Q_DATE_RE.findall(text or "")
    first = dates[0] if dates else "TBA"
    out["start"] = first
    if "claim" in lower and len(dates) > 1:
        out["claim"] = dates[1]
    if "snapshot" in lower:
        out["snapshot"] = first if out["snapshot"] == "TBA" else out["snapshot"]
    if "deadline" in lower and dates:
        out["deadline"] = dates[-1]
    return out


def _detect_kind(text: str, fallback: str = "airdrop") -> str:
    lower = (text or "").lower()
    for term, kind in EVENT_TERMS.items():
        if term in lower:
            return kind
    return fallback


def _scrape_airdropsio() -> list[OpportunityCandidate]:
    html_text = _safe_get("https://airdrops.io/latest/")
    if not html_text:
        return []
    # BUG-FIX: strip script/style/JSON-LD blocks before anchor scanning to
    # prevent schema.org structured-data text polluting candidate names.
    html_text = _strip_noise_blocks(html_text)
    seen = set()
    items: list[OpportunityCandidate] = []
    for href, title in re.findall(r'href="(https://airdrops\.io/[^"#?]+/)"[^>]*>(.*?)</a>', html_text, flags=re.I | re.S):
        name = _clean_name(title)
        if not _is_valid_name(name):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(OpportunityCandidate(name=name, source="airdropsio", source_url=href, kind="airdrop"))
        if len(items) >= 25:
            break
    return items


def _scrape_cmc_guides() -> list[OpportunityCandidate]:
    html_text = _safe_get("https://coinmarketcap.com/academy/categories/airdrop-guide")
    if not html_text:
        return []
    # BUG-FIX: strip script/style/JSON-LD blocks before anchor scanning.
    html_text = _strip_noise_blocks(html_text)
    items: list[OpportunityCandidate] = []
    seen = set()
    for href, title in re.findall(r'href="(/academy/article/[^"]+)"[^>]*>(.*?)</a>', html_text, flags=re.I | re.S):
        title_clean = _clean_name(title)
        if not _is_valid_name(title_clean):
            continue
        kind = _detect_kind(title_clean, "airdrop")
        if kind != "airdrop" and "airdrop" not in title_clean.lower():
            continue
        name = title_clean.replace('Airdrop Guide', '').replace('Everything You Need to Know', '').strip(' :-')
        name = _clean_name(name)
        if not _is_valid_name(name):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(OpportunityCandidate(name=name, source="cmc_guide", source_url=f"https://coinmarketcap.com{href}", kind="airdrop"))
        if len(items) >= 20:
            break
    return items


def _scrape_event_like_html() -> list[OpportunityCandidate]:
    items: list[OpportunityCandidate] = []
    # Lightweight generic scrape from configured HTML pages; intentionally conservative.
    for source_name, url, fallback_kind in DEFAULT_EVENT_HTML_SOURCES:
        html_text = _safe_get(url)
        if not html_text:
            continue
        # BUG-FIX: strip script/style/JSON-LD blocks before anchor scanning.
        html_text = _strip_noise_blocks(html_text)
        seen = set()
        _source_limit_hit = False
        for href, title in re.findall(r'href="([^"]+)"[^>]*>(.*?)</a>', html_text, flags=re.I | re.S):
            title_clean = _clean_name(title)
            if not _is_valid_name(title_clean):
                continue
            kind = _detect_kind(title_clean, fallback_kind)
            if kind == "airdrop":
                continue
            if not any(term in title_clean.lower() for term in ["token", "mainnet", "listing", "unlock", "release", "sale"]):
                continue
            name = title_clean
            key = f"{kind}:{name.lower()}"
            if key in seen:
                continue
            seen.add(key)
            full_url = href if href.startswith("http") else url.rstrip("/") + "/" + href.lstrip("/")
            items.append(OpportunityCandidate(name=name, source=source_name, source_url=full_url, kind=kind))
            if len(items) >= 15:
                _source_limit_hit = True
                break
        if _source_limit_hit:
            break
    return items


def _parse_rss(urls: list[str], fallback_kind: str) -> list[OpportunityCandidate]:
    items: list[OpportunityCandidate] = []
    seen = set()
    for url in urls:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:
                title = _clean_name(getattr(entry, 'title', ''))
                summary = getattr(entry, 'summary', '') or ''
                if not _is_valid_name(title):
                    continue
                kind = _detect_kind(f"{title} {summary}", fallback_kind)
                key = f"{kind}:{re.sub(r'[^a-z0-9]+', '', title.lower())}"
                if key in seen:
                    continue
                seen.add(key)
                items.append(OpportunityCandidate(name=title, source="rss", source_url=getattr(entry, 'link', url), kind=kind))
        except Exception as e:
            logger.debug("[AIRDROP] RSS failed %s: %s", url, e)
    return items


def _dedupe(items: list[OpportunityCandidate]) -> list[OpportunityCandidate]:
    merged: dict[str, OpportunityCandidate] = {}
    for item in items:
        key = f"{item.kind}:{re.sub(r'[^a-z0-9]+', '', item.name.lower())}"
        if key not in merged:
            merged[key] = item
        else:
            keep = merged[key]
            if keep.source != 'airdropsio' and item.source == 'airdropsio':
                # airdropsio wins, but carry over any official_url already enriched on keep
                prior_official = keep.official_url
                merged[key] = item
                if not item.official_url and prior_official:
                    merged[key].official_url = prior_official
            elif not keep.official_url and item.official_url:
                merged[key].official_url = item.official_url
    return list(merged.values())


def _enrich_detail(item: OpportunityCandidate) -> OpportunityCandidate:
    html_text = _safe_get(item.source_url)
    if not html_text:
        item.reasons.append("Source page unavailable during scan")
        return item

    links = _extract_links(html_text)
    x_links = [u for u in links if 'x.com/' in u or 'twitter.com/' in u]
    if x_links and not item.x_url:
        item.x_url = x_links[0].replace('twitter.com/', 'x.com/')

    external = []
    src_domain = urlparse(item.source_url).netloc.lower()
    for link in links:
        domain = urlparse(link).netloc.lower()
        if not domain or src_domain in domain:
            continue
        if 'x.com' in domain or 'twitter.com' in domain:
            continue
        external.append(link)
    if external:
        item.official_url = external[0]
        item.tasks_url = external[1] if len(external) > 1 else external[0]

    dates = _extract_dates(html_text)
    item.start_date = dates['start']
    item.claim_date = dates['claim']
    item.snapshot_date = dates['snapshot']
    item.deadline = dates['deadline']

    kind = _detect_kind(html_text, item.kind)
    item.kind = kind

    item.validity = 56
    if item.official_url:
        item.validity += 20
        item.status = '✅ CONFIRMED'
    if item.x_url:
        item.validity += 6
    if item.source == 'airdropsio':
        item.validity += 10
    elif item.source == 'cmc_guide':
        item.validity += 8
    elif item.source == 'rss':
        item.validity += 4
    item.validity = min(item.validity, 98)

    item.event_clarity = 25
    if item.start_date != 'TBA':
        item.event_clarity += 20
    if item.kind != 'unknown':
        item.event_clarity += 20
    if item.official_url:
        item.event_clarity += 15
    item.event_clarity = min(item.event_clarity, 95)
    return item


def _coingecko_listing_check(item: OpportunityCandidate) -> None:
    q = _url_quote(item.name)
    try:
        sr = requests.get(f"{COINGECKO_BASE_URL}/search?query={q}", timeout=10, headers=REQUEST_HEADERS)
        if not sr.ok:
            return
        data = sr.json() or {}
        coins = data.get('coins') or []
        if not coins:
            item.listing_status = 'Not found on CoinGecko'
            item.cex_status = 'Not listed'
            item.dex_status = 'No public token yet'
            item.token_live = 'NO'
            return
        first = coins[0]
        coin_id = first.get('id')
        if not coin_id:
            return
        item.listed = True
        item.token_live = 'YES'
        # BUG-FIX: short sleep between sequential CoinGecko calls avoids free-tier rate limit
        import time as _time; _time.sleep(1.2)
        detail = requests.get(
            f"{COINGECKO_BASE_URL}/coins/{coin_id}",
            timeout=10,
            headers=REQUEST_HEADERS,
            params={"tickers": "true", "market_data": "false", "community_data": "false", "developer_data": "false", "sparkline": "false"},
        )
        if detail.ok:
            dd = detail.json() or {}
            tickers = dd.get('tickers') or []
            has_cex = any((t.get('market') or {}).get('identifier') and not t.get('is_stale') and not t.get('is_anomaly') and str((t.get('market') or {}).get('identifier', '')).lower() not in {'uniswap', 'pancakeswap-new-token', 'raydium', 'orca'} for t in tickers)
            has_dex = any(str((t.get('market') or {}).get('identifier', '')).lower() in {'uniswap', 'pancakeswap-new-token', 'raydium', 'orca', 'camelot'} for t in tickers)
            item.cex_status = 'Listed' if has_cex else 'Not listed'
            item.dex_status = 'Listed' if has_dex else 'Not listed'
            if has_cex and has_dex:
                item.listing_status = 'CEX + DEX listed'
            elif has_cex:
                item.listing_status = 'CEX listed'
            elif has_dex:
                item.listing_status = 'DEX only'
            else:
                item.listing_status = 'Tracked token'
        else:
            item.listing_status = 'Tracked token'
    except Exception as e:
        logger.debug("[AIRDROP] CoinGecko check failed for %s: %s", item.name, e)


def _x_search(item: OpportunityCandidate) -> None:
    if not AIRDROP_X_BEARER_TOKEN:
        return
    query_name = item.name.replace(' ', ' OR ')
    if item.x_url:
        m = X_RE.search(item.x_url)
        if m:
            query_name = f'"{item.name}" OR from:{m.group(1)}'
    headers = {"Authorization": f"Bearer {AIRDROP_X_BEARER_TOKEN}", "User-Agent": "CRYPTEX-AIRDROP/2.0"}
    params = {
        "query": f'{query_name} -is:retweet -is:reply lang:en',
        "max_results": 50,
        "tweet.fields": "author_id,created_at,public_metrics",
    }
    try:
        # BUG-FIX: api.x.com DNS is unstable; canonical v2 endpoint is api.twitter.com
        r = requests.get("https://api.twitter.com/2/tweets/search/recent", params=params, headers=headers, timeout=12)
        if not r.ok:
            return
        data = r.json() or {}
        posts = data.get('data') or []
        item.social_mentions = len(posts)
        item.unique_accounts = len({p.get('author_id') for p in posts if p.get('author_id')})
    except Exception as e:
        logger.debug("[AIRDROP] X search failed for %s: %s", item.name, e)


def _rank_item(item: OpportunityCandidate) -> None:
    early = 52
    if item.start_date != 'TBA' or item.claim_date != 'TBA' or item.snapshot_date != 'TBA':
        early += 12
    if item.cex_status == 'Not listed':
        early += 12
    if item.dex_status == 'No public token yet':
        early += 8
    if item.listing_status in {'CEX listed', 'CEX + DEX listed'}:
        early -= 18
    if item.kind in {'token_release', 'mainnet_release', 'token_sale'} and item.start_date != 'TBA':
        early += 8
    item.early_opportunity = max(0, min(99, early))

    mentions = item.social_mentions
    uniq = item.unique_accounts
    if mentions == 0 and uniq == 0:
        item.crowdedness = '🟡 MEDIUM'
        item.hype_level = 'Low'
        crowd_penalty = 4
    elif mentions < 150 and uniq < 80:
        item.crowdedness = '🟢 LOW'
        item.hype_level = 'Low'
        crowd_penalty = -8
    elif mentions < 600 and uniq < 250:
        item.crowdedness = '🟢 LOW'
        item.hype_level = 'Medium'
        crowd_penalty = -2
    elif mentions < 1500 and uniq < 600:
        item.crowdedness = '🟡 MEDIUM'
        item.hype_level = 'Medium-High'
        crowd_penalty = 6
    else:
        item.crowdedness = '🔴 HIGH'
        item.hype_level = 'High'
        crowd_penalty = 16
    item.crowdedness_penalty = crowd_penalty

    actionability = 35
    if item.official_url:
        actionability += 18
    if item.tasks_url:
        actionability += 14
    if item.start_date != 'TBA' or item.claim_date != 'TBA':
        actionability += 10
    item.actionability = min(actionability, 95)

    reasons = []
    if item.official_url:
        reasons.append('Official link available')
    if item.cex_status == 'Not listed':
        reasons.append('No major CEX listing yet')
    if item.crowdedness.startswith('🟢'):
        reasons.append('Still early and not overcrowded')
    elif item.crowdedness.startswith('🔴'):
        reasons.append('Already getting crowded')
    if item.kind != 'airdrop':
        reasons.append(f"Catalyst type: {EVENT_LABELS.get(item.kind, 'Opportunity')}")
    if item.start_date != 'TBA' or item.claim_date != 'TBA':
        reasons.append('Clear timing signal detected')
    if not reasons:
        reasons.append('Early watchlist candidate with incomplete public data')
    item.reasons = reasons[:4]

    item.what_to_do = [
        'Open official page',
        'Complete tasks / register' if item.kind == 'airdrop' else 'Monitor launch / event timing',
        'Track snapshot / claim / listing updates',
    ]

    base_social = min(15, mentions // 100) + min(10, uniq // 50)
    social_quality = min(100, 35 + base_social * 2)
    crowdedness_advantage = max(0, 100 - max(0, crowd_penalty * 4 + 25))
    item.score = max(
        1,
        min(
            99,
            round(
                item.validity * 0.25
                + item.early_opportunity * 0.25
                + crowdedness_advantage * 0.20
                + item.actionability * 0.15
                + item.event_clarity * 0.10
                + social_quality * 0.05
            ),
        ),
    )


def _filter_items(items: list[OpportunityCandidate], mode: str) -> list[OpportunityCandidate]:
    if mode == FILTER_AIRDROPS:
        return [i for i in items if i.kind == 'airdrop']
    if mode == FILTER_EVENTS:
        return [i for i in items if i.kind != 'airdrop']
    if mode == FILTER_SOON:
        return [i for i in items if i.start_date != 'TBA' or i.claim_date != 'TBA']
    if mode == FILTER_LOW_CROWD:
        return [i for i in items if i.crowdedness.startswith('🟢')]
    if mode == FILTER_NOT_LISTED:
        return [i for i in items if i.cex_status == 'Not listed']
    return items


def _build_card(item: OpportunityCandidate, rank: int) -> str:
    best_stage = '🚀 FARM NOW' if item.kind == 'airdrop' and item.crowdedness.startswith('🟢') and item.status.startswith('✅') else '🚀 PREP NOW' if item.kind != 'airdrop' and item.crowdedness.startswith('🟢') else '👀 WATCH / FARM' if item.crowdedness.startswith('🟢') else '⚠️ PICK CAREFULLY'
    risk = '🟡 MEDIUM' if item.status.startswith('✅') else '🟠 ELEVATED'
    return (
        f"╔════════════════════════╗\n"
        f"🎁 #{rank} {item.name.upper()}\n"
        f"╚════════════════════════╝\n\n"
        f"Type: {EVENT_LABELS.get(item.kind, 'Opportunity')}\n"
        f"Status: {item.status}\n"
        f"Early Score: {item.score}/100\n"
        f"Crowdedness: {item.crowdedness}\n"
        f"Best Stage: {best_stage}\n"
        f"Risk: {risk}\n\n"
        f"📅 Timeline\n"
        f"• Start: {item.start_date}\n"
        f"• Claim: {item.claim_date}\n"
        f"• Snapshot: {item.snapshot_date}\n"
        f"• Deadline: {item.deadline}\n\n"
        f"🏦 Listing Status\n"
        f"• CEX: {item.cex_status}\n"
        f"• DEX: {item.dex_status}\n"
        f"• Token Live: {item.token_live}\n\n"
        f"📣 Social Proof\n"
        f"• Mentions: {item.social_mentions or 'N/A'}\n"
        f"• Unique Accounts: {item.unique_accounts or 'N/A'}\n"
        f"• Hype: {item.hype_level}\n"
        f"• Crowd Penalty: {item.crowdedness_penalty:+d}\n\n"
        f"🧠 Validation\n"
        f"• Official Source: {'YES' if item.official_url else 'NO'}\n"
        f"• Source: {item.source}\n"
        f"• AI Consensus: {item.ai_consensus}\n"
        f"• Markov Confidence: {item.markov_confidence}\n\n"
        f"📝 Why Ranked\n"
        + ''.join(f"• {r}\n" for r in item.reasons[:4]) + "\n"
        f"🔗 Start Here\n"
        f"• Official: {item.official_url or item.source_url}\n"
        f"• Tasks: {item.tasks_url or item.source_url}\n"
        f"• X: {item.x_url or 'N/A'}\n\n"
        f"⚡ What To Do\n"
        + ' • '.join(item.what_to_do[:3]) + "\n\n"
        f"⚠️ Use official links only. Watchlist only."
    )


def _build_buttons(item: OpportunityCandidate) -> InlineKeyboardMarkup:
    row1 = [InlineKeyboardButton("🌐 Official", url=item.official_url or item.source_url)]
    row1.append(InlineKeyboardButton("📋 Tasks", url=item.tasks_url or item.source_url))
    if item.x_url:
        row1.append(InlineKeyboardButton("🐦 X", url=item.x_url))
    row2 = [InlineKeyboardButton("📎 Source", url=item.source_url)]
    return InlineKeyboardMarkup([row1, row2])


def get_airdrop_submenu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏆 Top Early Opportunities", callback_data="airdrop:top")],
        [InlineKeyboardButton("🎁 Airdrops Only", callback_data="airdrop:airdrops"), InlineKeyboardButton("📅 Token Events", callback_data="airdrop:events")],
        [InlineKeyboardButton("⏰ Starting Soon", callback_data="airdrop:soon"), InlineKeyboardButton("🟢 Low Crowdedness", callback_data="airdrop:low_crowd")],
        [InlineKeyboardButton("🏦 Not Yet Listed", callback_data="airdrop:not_listed")],
    ])


def generate_airdrop_cards_sync(limit: int = AIRDROP_TOP_N, mode: str = FILTER_TOP) -> tuple[str, list[tuple[str, InlineKeyboardMarkup]]]:
    items = _dedupe(
        _scrape_airdropsio()
        + _scrape_cmc_guides()
        + _scrape_event_like_html()
        + _parse_rss(DEFAULT_RSS_URLS, 'airdrop')
        + _parse_rss(DEFAULT_EVENT_RSS_URLS, 'event')
    )
    if not items:
        header = (
            "🎁 *AIRDROP INTEL — EARLY RANKED OPPORTUNITIES*\n\n"
            "⚠️ No candidates were found from the configured free sources right now.\n"
            "Add more RSS URLs via `AIRDROP_RSS_URLS` or `AIRDROP_EVENT_RSS_URLS` and try again later."
        )
        return header, []

    ranked: list[OpportunityCandidate] = []
    for item in items[:35]:
        try:
            _enrich_detail(item)
            _coingecko_listing_check(item)
            _x_search(item)
            _rank_item(item)
            ranked.append(item)
        except Exception as e:
            logger.warning("[AIRDROP] candidate failed %s: %s", item.name, e)
    ranked.sort(key=lambda x: (x.score, x.validity, x.early_opportunity, x.event_clarity), reverse=True)
    ranked = _filter_items(ranked, mode)[:limit]

    mode_title = {
        FILTER_TOP: 'EARLY RANKED OPPORTUNITIES',
        FILTER_AIRDROPS: 'AIRDROPS ONLY',
        FILTER_EVENTS: 'TOKEN EVENTS',
        FILTER_SOON: 'STARTING SOON',
        FILTER_LOW_CROWD: 'LOW CROWDEDNESS',
        FILTER_NOT_LISTED: 'NOT YET LISTED',
    }.get(mode, 'EARLY RANKED OPPORTUNITIES')

    header = (
        f"🎁 *AIRDROP INTEL — {mode_title}*\n"
        f"Updated: `{datetime.now(timezone.utc).strftime('%b %d %Y · %H:%M UTC')}`\n\n"
        "Goal: find *VALID* opportunities early before they become too crowded.\n"
        "Ranking: validity + early edge + lower crowdedness + actionability + event clarity.\n\n"
        "⚠️ Watchlist only — verify only from official links.\n"
        f"Now loading top `{len(ranked)}` cards..."
    )
    cards = [(_build_card(item, i), _build_buttons(item)) for i, item in enumerate(ranked, start=1)]
    return header, cards


async def _send_cards(message, create_main_keyboard, mode: str) -> None:
    progress = await message.reply_text(
        "🎁 *AIRDROP INTEL*\n\n⏳ Scanning free sources and validating opportunities...",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=create_main_keyboard(),
    )
    header, cards = await asyncio.to_thread(generate_airdrop_cards_sync, AIRDROP_TOP_N, mode)
    try:
        await progress.edit_text(header, parse_mode=ParseMode.MARKDOWN, reply_markup=create_main_keyboard())
    except Exception:
        await message.reply_text(header, parse_mode=ParseMode.MARKDOWN, reply_markup=create_main_keyboard())
    for text, kb in cards:
        # BUG-FIX: explicit parse_mode=None — card text is plain ASCII/emoji.
        # Without it, project names with _ or ` can trigger Telegram's Markdown parser.
        await message.reply_text(text, reply_markup=kb, parse_mode=None, disable_web_page_preview=True)
        await asyncio.sleep(0.25)


async def handle_airdrops_button(update, context, create_main_keyboard):
    message = update.message or (update.callback_query.message if update.callback_query else None)
    if message is None:
        return
    await message.reply_text(
        "🎁 *AIRDROP INTEL*\n\nChoose a view:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_airdrop_submenu_keyboard(),
    )


async def handle_airdrops_filter(update, context, create_main_keyboard):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    mode = FILTER_TOP
    if ':' in (query.data or ''):
        mode = (query.data or '').split(':', 1)[1]
    await _send_cards(query.message, create_main_keyboard, mode)


async def handle_airdrops_command(update, context, create_main_keyboard, mode: Optional[str] = None):
    message = update.message or (update.callback_query.message if update.callback_query else None)
    if message is None:
        return
    await _send_cards(message, create_main_keyboard, mode or FILTER_TOP)
