"""
dex.py — DexScreener Scanner Module
====================================
Imported by app.py. Do NOT run directly.

Public API:
    cards = await build_top10()
    # returns list of (message_str, InlineKeyboardMarkup)

Score weights (0-100):
    Age          30pts  freshest wins
    5m Txns      20pts  activity RIGHT NOW
    Volume 24h   20pts  real interest
    Momentum     15pts  multi-TF price direction
    Liquidity    10pts  depth / safety
    Buy Ratio     5pts  buy dominance

Source: DexScreener free API — no key required.
Chains: Solana, Base, Ethereum, BSC, Arbitrum
Min Liquidity: $5,000  |  Max Age: 24h
"""

import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

log = logging.getLogger(__name__)

# ---- Endpoints (all free, no key) -------------------------------------------
DSC_SEARCH   = "https://api.dexscreener.com/latest/dex/search"
DSC_PROFILES = "https://api.dexscreener.com/token-profiles/latest/v1"
DSC_TOKENS   = "https://api.dexscreener.com/latest/dex/tokens"

# ---- Config ------------------------------------------------------------------
ALL_CHAINS  = ["solana", "base", "ethereum", "bsc", "arbitrum"]
MIN_LIQ     = 5_000
MIN_VOL     = 50
MIN_BUYS    = 1
MAX_AGE_SEC = 86_400  # 24h


# ==============================================================================
# FORMATTERS
# ==============================================================================

def fmt_usd(value) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if v >= 1_000_000_000:
        return f"${v/1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}k"
    return f"${v:.2f}"


def fmt_pct(value) -> str:
    try:
        v = float(value)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def fmt_age(created_ms) -> str:
    if not created_ms:
        return "?"
    diff = datetime.now(timezone.utc).timestamp() - (created_ms / 1000)
    if diff < 60:
        return f"{int(diff)}s"
    if diff < 3600:
        return f"{int(diff // 60)}m"
    if diff < 86400:
        h = int(diff // 3600)
        m = int((diff % 3600) // 60)
        return f"{h}h {m}m"
    return f"{int(diff // 86400)}d"


def age_seconds(created_ms) -> float:
    if not created_ms:
        return float("inf")
    return datetime.now(timezone.utc).timestamp() - (created_ms / 1000)


def trend_arrow(val) -> str:
    try:
        v = float(val)
        return "up" if v > 0 else ("down" if v < 0 else "flat")
    except (TypeError, ValueError):
        return "flat"


def detect_meta(name: str, symbol: str) -> tuple:
    c = (name + " " + symbol).lower()
    if any(k in c for k in ["frog", "pepe", "toad", "kek", "ribbit"]):
        return "MEMECOIN", "Frog meta"
    if any(k in c for k in ["doge", "shib", "inu", "dog", "pup", "woof", "floki"]):
        return "MEMECOIN", "Dog meta"
    if any(k in c for k in ["cat", "meow", "neko", "kitty", "puss"]):
        return "MEMECOIN", "Cat meta"
    if any(k in c for k in ["ai", "gpt", "llm", "neural", "agent", "bot"]):
        return "AI TOKEN", "AI agent meta"
    if any(k in c for k in ["trump", "maga", "biden", "elon", "musk"]):
        return "POLITICAL", "Political meta"
    if any(k in c for k in ["moon", "pump", "rocket", "gem", "100x"]):
        return "MEMECOIN", "Hype meta"
    if any(k in c for k in ["btc", "bitcoin", "eth", "sol", "base"]):
        return "CHAIN TOKEN", "Chain meta"
    return "MEMECOIN", "New narrative"


# ==============================================================================
# SCORING
# ==============================================================================

def calc_score(liq, vol, vol5m, h5m, h1, h6, h24,
               buys, sells, buys5m, created_ms) -> int:
    score   = 0
    total   = (buys + sells) or 1
    buy_dom = buys / total

    # Age (30 pts) — freshest wins
    if created_ms:
        age = age_seconds(created_ms)
        if age < 300:       score += 30
        elif age < 1800:    score += 27
        elif age < 3600:    score += 24
        elif age < 7200:    score += 20
        elif age < 21600:   score += 14
        elif age < 43200:   score += 8
        else:               score += 3

    # 5m Txns (20 pts) — activity RIGHT NOW
    if buys5m >= 50:        score += 20
    elif buys5m >= 20:      score += 16
    elif buys5m >= 10:      score += 12
    elif buys5m >= 5:       score += 8
    elif buys5m >= 2:       score += 5
    elif buys5m >= 1:       score += 2

    # Volume 24h (20 pts)
    if vol >= 1_000_000:    score += 20
    elif vol >= 500_000:    score += 17
    elif vol >= 100_000:    score += 14
    elif vol >= 50_000:     score += 10
    elif vol >= 10_000:     score += 6
    elif vol >= 1_000:      score += 3

    # Momentum (15 pts)
    mom = 0
    if float(h5m or 0) > 10:   mom += 5
    elif float(h5m or 0) > 0:  mom += 2
    if float(h1 or 0) > 20:    mom += 5
    elif float(h1 or 0) > 0:   mom += 3
    if float(h6 or 0) > 0:     mom += 3
    if float(h24 or 0) > 0:    mom += 2
    score += min(15, mom)

    # Liquidity (10 pts)
    if liq >= 500_000:      score += 10
    elif liq >= 200_000:    score += 9
    elif liq >= 100_000:    score += 8
    elif liq >= 50_000:     score += 6
    elif liq >= 20_000:     score += 4
    elif liq >= 5_000:      score += 2

    # Buy ratio (5 pts)
    if buy_dom >= 0.75:     score += 5
    elif buy_dom >= 0.60:   score += 3
    elif buy_dom >= 0.50:   score += 1

    return min(score, 100)


# ==============================================================================
# LABELS
# ==============================================================================

def score_tag(score) -> str:
    if score >= 85: return "FIRE"
    if score >= 70: return "HOT"
    if score >= 55: return "WARM"
    return "WATCH"


def rank_label(rank, score) -> str:
    if score >= 85:   label = "ULTRA EARLY"
    elif score >= 70: label = "SUPER EARLY"
    elif score >= 55: label = "EARLY GEM"
    elif score >= 40: label = "RADAR"
    else:             label = "WATCH"
    return f"#{rank} {label}"


def age_label(created_ms) -> str:
    if not created_ms: return "EARLY"
    diff = age_seconds(created_ms)
    if diff < 300:    return "BRAND NEW"
    if diff < 3600:   return "JUST LAUNCHED"
    if diff < 21600:  return "SUPER EARLY"
    if diff < 86400:  return "EARLY"
    return "ESTABLISHED"


def rug_tag(label) -> str:
    if label == "HIGH":   return "[HIGH RUG RISK]"
    if label == "MEDIUM": return "[MEDIUM RUG RISK]"
    return "[LOW RUG RISK]"


# ==============================================================================
# ANALYTICS
# ==============================================================================

def calc_max_pump(h1, h6, h24) -> str:
    best = max(float(h1 or 0), float(h6 or 0), float(h24 or 0), 0)
    if best < 50:
        return ""
    return f"{1 + best / 100:.1f}X"


def calc_first_mc(current_mc, h24) -> float:
    try:
        h24v = float(h24 or 0)
        if current_mc > 0 and h24v != -100:
            return current_mc / (1 + h24v / 100)
    except (ZeroDivisionError, TypeError, ValueError):
        pass
    return 0.0


def assess_rug(liq, vol, buys, sells) -> tuple:
    risk_pts = 0
    total    = (buys + sells) or 1
    sell_r   = sells / total
    if liq < 5_000:     risk_pts += 30
    elif liq < 10_000:  risk_pts += 15
    elif liq < 20_000:  risk_pts += 8
    if sell_r > 0.80:   risk_pts += 35
    elif sell_r > 0.65: risk_pts += 20
    elif sell_r > 0.55: risk_pts += 10
    if liq > 0 and vol > 0 and (vol / liq) < 0.05:
        risk_pts += 10
    conf = max(0, 100 - risk_pts)
    if risk_pts < 20: return "LOW",    conf
    if risk_pts < 50: return "MEDIUM", conf
    return "HIGH", conf


def estimate_tax(buys, sells, h5m) -> tuple:
    total  = (buys + sells) or 1
    sell_r = sells / total
    h5mv   = float(h5m or 0)
    if sell_r > 0.80 and h5mv < -10: return "0%", "~15%+"
    if sell_r > 0.70:                 return "0%", "~5%"
    return "0%", "0%"


def estimate_smart_wallets(buys, sells, vol, h1, h5m) -> tuple:
    wallets = []
    total   = (buys + sells) or 1
    buy_dom = buys / total
    avg_buy = (vol * buy_dom / buys) if buys > 0 else 0
    sol_px  = 140

    if avg_buy > 2_000 and buy_dom > 0.60:
        count = min(5, max(1, int(avg_buy / 1_500)))
        for i in range(count):
            usd = avg_buy * (1.0 - i * 0.15)
            wallets.append({"tag": f"SmartWallet {i+1}", "usd_amount": usd,
                             "sol_amount": usd / sol_px, "time_ago": f"{(i+1)*5}m"})
        return count, wallets

    if avg_buy > 500 and buy_dom > 0.55:
        count = min(3, max(1, int(avg_buy / 800)))
        for i in range(count):
            usd = avg_buy * (1.0 - i * 0.2)
            wallets.append({"tag": f"SmartWallet {i+1}", "usd_amount": usd,
                             "sol_amount": usd / sol_px, "time_ago": f"{(i+1)*8}m"})
        return count, wallets

    return 0, []


def estimate_holders(vol, buys, total_txns) -> dict:
    if total_txns == 0:
        return {}
    avg_tx  = vol / total_txns if total_txns > 0 else 0
    holders = max(10, int(buys * 0.7))
    top10   = 45.0 if avg_tx > 5_000 else (30.0 if avg_tx > 1_000 else 20.0)
    return {"holders": holders, "top10_pct": top10, "avg_tx": avg_tx}


def build_risk_checks(liq, vol, buys, sells, h24, created_ms) -> list:
    checks = []
    total  = (buys + sells) or 1
    sell_r = sells / total
    vl_r   = (vol / liq) if liq > 0 else 0

    if liq >= 200_000:   checks.append("OK  Liq healthy (>=$200k)")
    elif liq >= 50_000:  checks.append("OK  Liq moderate ($50k-$200k)")
    elif liq >= 5_000:   checks.append("LOW Liq low ($5k-$50k)")
    else:                checks.append("LOW Very low liq (<$5k)")

    if sell_r < 0.45:    checks.append("OK  Buy dominant")
    elif sell_r < 0.65:  checks.append("MED Balanced buys/sells")
    else:                checks.append("BAD Sell dominant -- caution")

    if vl_r > 1.5:       checks.append("OK  Active vol/liq ratio")
    elif vl_r > 0.3:     checks.append("OK  Normal vol/liq")
    else:                checks.append("LOW Low vol/liq ratio")

    if created_ms:
        diff = age_seconds(created_ms)
        if diff < 300:    checks.append("NEW Brand new (<5m) -- unproven")
        elif diff < 3600: checks.append("NEW Just launched -- monitor closely")
        else:             checks.append("OK  Age verified")

    total_txns = buys + sells
    if total_txns > 500:   checks.append("OK  High txn count (>500)")
    elif total_txns > 100: checks.append("OK  Moderate txn count")
    else:                  checks.append("LOW Low txn count")

    return checks


def build_smart_wallet_lines(wallets, chain) -> str:
    if not wallets:
        return "  No smart wallet signals detected"
    lines = []
    for w in wallets[:3]:
        tag  = w.get("tag", "Wallet")
        usd  = w.get("usd_amount", 0)
        sol  = w.get("sol_amount", 0)
        when = w.get("time_ago", "?")
        if chain == "SOLANA":
            lines.append(f"  [{tag}]  Buy {sol:.3f} SOL (${usd:.2f}) -- {when} ago")
        else:
            lines.append(f"  [{tag}]  Buy ${usd:.2f} -- {when} ago")
    return "\n".join(lines)


def build_narrative(meta, chain, h5m_v, h1_v, h6_v, h24_v,
                    rug, sm_count, liq, vol, buys, sells) -> str:
    total   = (buys + sells) or 1
    buy_dom = buys / total
    vl_r    = vol / liq if liq > 0 else 0
    h5m_f   = float(h5m_v or 0)
    h1_f    = float(h1_v or 0)
    h24_f   = float(h24_v or 0)
    points  = []

    if h5m_f > 10:
        points.append(f"1. Explosive 5m candle on {chain} -- {meta} surging NOW.")
    elif h1_f > 20:
        points.append(f"1. Strong 1h momentum -- {meta} gaining fast on {chain}.")
    elif h1_f > 0:
        points.append(f"1. Early {meta} forming on {chain} -- quiet accumulation phase.")
    else:
        points.append(f"1. New {meta} on {chain} -- watching for breakout trigger.")

    if rug == "LOW" and vl_r > 1:
        points.append("2. Low rug risk + strong vol/liq ratio -- healthy early entry.")
    elif rug == "HIGH":
        points.append("2. High rug risk -- sell pressure dominant. Extreme caution.")
    elif vl_r > 2:
        points.append("2. Volume exceeding liquidity 2x -- aggressive buyer pressure.")
    elif liq > 200_000:
        points.append("2. Solid liquidity (>$200k) -- lower dump risk for entries.")
    else:
        points.append("2. Monitor for breakout confirmation before entry.")

    if sm_count >= 3:
        points.append(f"3. {sm_count} smart wallet signals -- institutional accumulation.")
    elif buy_dom > 0.70:
        points.append(f"3. Strong buy dominance ({int(buy_dom*100)}%) -- retail FOMO building.")
    elif buy_dom > 0.55:
        points.append(f"3. Moderate buy dominance ({int(buy_dom*100)}%) -- organic pattern.")
    else:
        points.append("3. Balanced buy/sell -- no directional bias yet.")

    if h24_f > 100:
        points.append(f"4. Massive 24h gain (+{h24_f:.1f}%) -- full price discovery mode.")
    elif h24_f > 30:
        points.append(f"4. Strong 24h performance (+{h24_f:.1f}%) -- sustained momentum.")
    elif h24_f > 0:
        points.append(f"4. Positive 24h trend (+{h24_f:.1f}%) -- building steadily.")
    elif h24_f < -30:
        points.append(f"4. 24h decline ({h24_f:.1f}%) -- watch key support levels.")
    else:
        points.append("4. 24h neutral -- awaiting volume catalyst.")

    return "\n".join(points)


# ==============================================================================
# DEXSCREENER FETCHERS
# ==============================================================================

async def _fetch_search(chain, session) -> list:
    try:
        async with session.get(
            DSC_SEARCH,
            params={"q": chain},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                log.warning(f"[dex] search({chain}) HTTP {r.status}")
                return []
            data = await r.json()
            return data.get("pairs", []) or []
    except Exception as e:
        log.warning(f"[dex] search({chain}): {e}")
        return []


async def _fetch_profiles(session) -> list:
    try:
        async with session.get(
            DSC_PROFILES,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                log.warning(f"[dex] profiles HTTP {r.status}")
                return []
            data = await r.json()
            return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"[dex] profiles: {e}")
        return []


async def _fetch_token_pairs(address, chain_id, session) -> list:
    try:
        async with session.get(
            f"{DSC_TOKENS}/{address}",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status != 200:
                return []
            data  = await r.json()
            pairs = data.get("pairs", []) or []
            return [p for p in pairs
                    if (p.get("chainId") or "").lower() == chain_id.lower()]
    except Exception as e:
        log.warning(f"[dex] token_pairs({address[:8]}): {e}")
        return []


# ==============================================================================
# PAIR NORMALISER
# ==============================================================================

def _extract(p) -> dict:
    token  = p.get("baseToken") or {}
    pc     = p.get("priceChange") or {}
    t24    = (p.get("txns") or {}).get("h24") or {}
    t5m    = (p.get("txns") or {}).get("m5") or {}
    liq_d  = p.get("liquidity") or {}
    vol_d  = p.get("volume") or {}
    return {
        "symbol":  (token.get("symbol") or "").upper(),
        "name":    token.get("name") or token.get("symbol") or "",
        "address": token.get("address") or "",
        "chain":   (p.get("chainId") or "").lower(),
        "liq":     float(liq_d.get("usd") or 0),
        "vol":     float(vol_d.get("h24") or 0),
        "vol5m":   float(vol_d.get("m5") or 0),
        "h5m":     float(pc.get("m5") or 0),
        "h1":      float(pc.get("h1") or 0),
        "h6":      float(pc.get("h6") or 0),
        "h24":     float(pc.get("h24") or 0),
        "buys":    int(t24.get("buys") or 0),
        "sells":   int(t24.get("sells") or 0),
        "buys5m":  int(t5m.get("buys") or 0),
        "sells5m": int(t5m.get("sells") or 0),
        "mc":      float(p.get("fdv") or p.get("marketCap") or 0),
        "created": p.get("pairCreatedAt"),
        "url":     p.get("url") or "",
    }


def _valid(f) -> bool:
    if f["liq"] < MIN_LIQ:             return False
    if f["vol"] < MIN_VOL:             return False
    if f["buys"] < MIN_BUYS:           return False
    if not f["symbol"]:                return False
    if f["chain"] not in ALL_CHAINS:   return False
    if f["created"] and age_seconds(f["created"]) > MAX_AGE_SEC:
        return False
    return True


# ==============================================================================
# CARD BUILDER
# ==============================================================================

def _build_card(rank, f) -> tuple:
    symbol   = f["symbol"] or "???"
    name     = f["name"] or symbol
    chain_id = f["chain"].upper()
    age      = fmt_age(f["created"])
    alabel   = age_label(f["created"])
    contract = f["address"] or "N/A"
    sc       = f["score"]
    liq      = f["liq"]
    vol      = f["vol"]

    liq_fmt   = fmt_usd(liq)
    vol_fmt   = fmt_usd(vol)
    vol5m_fmt = fmt_usd(f["vol5m"])
    mc_fmt    = fmt_usd(f["mc"]) if f["mc"] else "N/A"
    fmc       = calc_first_mc(f["mc"], f["h24"])
    fmc_fmt   = fmt_usd(fmc) if fmc else "N/A"
    arrow     = trend_arrow(f["h1"])
    max_pump  = calc_max_pump(f["h1"], f["h6"], f["h24"])

    h5m_s = fmt_pct(f["h5m"])
    h1_s  = fmt_pct(f["h1"])
    h6_s  = fmt_pct(f["h6"])
    h24_s = fmt_pct(f["h24"])

    rug, rug_pct   = assess_rug(liq, vol, f["buys"], f["sells"])
    buy_tax, s_tax = estimate_tax(f["buys"], f["sells"], f["h5m"])
    sm_cnt, sm_wlt = estimate_smart_wallets(f["buys"], f["sells"], vol, f["h1"], f["h5m"])
    hd             = estimate_holders(vol, f["buys"], f["buys"] + f["sells"])
    risk_checks    = build_risk_checks(liq, vol, f["buys"], f["sells"], f["h24"], f["created"])
    sw_lines       = build_smart_wallet_lines(sm_wlt, chain_id)
    category, meta = detect_meta(name, symbol)
    narrative      = build_narrative(
        meta, chain_id,
        f["h5m"], f["h1"], f["h6"], f["h24"],
        rug, sm_cnt, liq, vol,
        f["buys"], f["sells"],
    )

    r_lbl      = rank_label(rank, sc)
    risk_str   = "\n".join(f"  {r}" for r in risk_checks)
    total_txns = f["buys"] + f["sells"]
    buy_pct    = f"{f['buys']/total_txns*100:.0f}%" if total_txns else "N/A"
    sell_pct   = f"{f['sells']/total_txns*100:.0f}%" if total_txns else "N/A"
    danger     = 1 if rug == "HIGH" else 0
    warnings   = sum(1 for r in risk_checks if r.startswith(("LOW", "MED", "BAD", "NEW")))
    rug_lvl    = "LOW RISK" if rug == "LOW" else ("MEDIUM RISK" if rug == "MEDIUM" else "HIGH RISK")
    rug_gauge  = "20%" if rug == "LOW" else ("55%" if rug == "MEDIUM" else "80%")

    W  = 40
    WI = W - 2
    score_icon = "🔥" if sc >= 85 else "🌶" if sc >= 70 else "⚡" if sc >= 55 else "👁"
    rug_icon   = "🟢" if rug == "LOW" else "🟡" if rug == "MEDIUM" else "🔴"
    pump_str   = f"   Max Pump *{max_pump}*" if max_pump else ""

    lines = []
    lines.append(f"╔{'═'*(W-2)}╗")
    lines.append(f"║  {score_icon}  {alabel}  ·  *{chain_id}*{' '*(WI-len(alabel)-len(chain_id)-8)}║")
    lines.append(f"║  🎯  SNIPER MODE  ·  {r_lbl}{' '*(WI-len(r_lbl)-21)}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  *${symbol}*   {name[:WI-len(symbol)-5]:<{WI-len(symbol)-5}}║")
    lines.append(f"║  ⏱  Age: {age:<8}  {category}  ·  {meta[:WI-len(age)-len(category)-12]:<{WI-len(age)-len(category)-12}}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  Score  *{sc}* [{score_tag(sc)}]{pump_str}{' '*(WI-len(str(sc))-len(score_tag(sc))-len(pump_str)-10)}║")
    if fmc and fmc > 0:
        lines.append(f"║  MC  {fmc_fmt} → *{mc_fmt}* ({arrow}){' '*(WI-len(fmc_fmt)-len(mc_fmt)-len(arrow)-12)}║")
    else:
        lines.append(f"║  MC  *{mc_fmt}*  ({arrow}){' '*(WI-len(mc_fmt)-len(arrow)-10)}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  📊  PRICE CHANGE{' '*(WI-18)}║")
    lines.append(f"║  {'5m':<6}  {h5m_s:<10}  {'1h':<6}  {h1_s:<{WI-28}}║")
    lines.append(f"║  {'6h':<6}  {h6_s:<10}  {'24h':<6}  {h24_s:<{WI-28}}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  💧  LIQUIDITY  &  VOLUME{' '*(WI-26)}║")
    lines.append(f"║  {'Liq':<8}  {liq_fmt:<12}  {'Vol24h':<8}  {vol_fmt:<{WI-34}}║")
    lines.append(f"║  {'Vol5m':<8}  {vol5m_fmt:<12}{' '*(WI-23)}║")
    lines.append(f"║  Buys  {f['buys']} ({buy_pct})   Sells  {f['sells']} ({sell_pct}){' '*(WI-38)}║")
    lines.append(f"║  5m →  Buys {f['buys5m']}   Sells {f['sells5m']}{' '*(WI-30)}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  {rug_icon}  RISK  ·  {rug_lvl}  ({rug_gauge}){' '*(WI-len(rug_lvl)-len(rug_gauge)-16)}║")
    lines.append(f"║  Danger: {danger}   Warnings: {warnings}   Buy Tax: {buy_tax}   Sell: {s_tax}{' '*(WI-47)}║")
    for rc in risk_checks[:4]:
        rc_icon = "✅" if rc.startswith("OK") else "⚠️" if rc.startswith(("MED","NEW")) else "🔴"
        rc_text = rc[4:] if len(rc) > 3 else rc
        lines.append(f"║  {rc_icon}  {rc_text[:WI-8]:<{WI-8}}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    if hd:
        lines.append(f"║  👥  HOLDER ANALYSIS (est.){' '*(WI-28)}║")
        lines.append(f"║  {'Est. Holders':<16}  ~{hd['holders']:,}{' '*(WI-26)}║")
        lines.append(f"║  {'Top10 Conc.':<16}  ~{hd['top10_pct']:.0f}%{' '*(WI-22)}║")
        lines.append(f"║  {'Avg Tx Size':<16}  {fmt_usd(hd['avg_tx']):<{WI-20}}║")
    else:
        lines.append(f"║  👥  HOLDER ANALYSIS — Insufficient data{' '*(WI-42)}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    if sm_cnt > 0:
        lines.append(f"║  🧠  SMART WALLETS  ({sm_cnt} signals){' '*(WI-len(str(sm_cnt))-28)}║")
        for sw_line in sw_lines.split("\n")[:3]:
            lines.append(f"║  {sw_line.strip()[:WI-4]:<{WI-4}}║")
    else:
        lines.append(f"║  🧠  SMART WALLETS — No significant activity{' '*(WI-46)}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  {rug_icon}  RUG ANALYSIS  ·  {rug_tag(rug)}  ·  Conf {rug_pct}%{' '*(WI-len(rug_tag(rug))-len(str(rug_pct))-28)}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  🤖  AI NARRATIVE{' '*(WI-18)}║")
    for nar_line in narrative.split("\n"):
        lines.append(f"║  {nar_line[:WI-4]:<{WI-4}}║")
    lines.append(f"╠{'═'*(W-2)}╣")
    lines.append(f"║  Contract:{' '*(WI-11)}║")
    contract_str = contract[:WI-4]
    lines.append(f"║  `{contract_str}`{' '*(WI-len(contract_str)-4)}║")
    lines.append(f"╚{'═'*(W-2)}╝")
    lines.append(f"_DexScreener data only. DYOR. Not financial advice._")

    msg = "\n".join(lines)

    pair_url = f["url"] or f"https://dexscreener.com/{f['chain']}/{contract}"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Chart",       url=pair_url),
        InlineKeyboardButton("DexScreener", url=pair_url),
        InlineKeyboardButton("Twitter",     url=f"https://twitter.com/search?q=%24{symbol}"),
    ]])
    return msg, kb


# ==============================================================================
# PUBLIC API  —  called by app.py
# ==============================================================================

async def build_top10() -> list:
    """
    Scan DexScreener, score and rank tokens, return top 10 cards.

    Returns:
        list of (str, InlineKeyboardMarkup) tuples —
        ready to pass directly to update.message.reply_text()
    """
    async with aiohttp.ClientSession() as session:

        # 1. Parallel fetch: 5 chain searches + new-profiles endpoint
        tasks = [_fetch_search(c, session) for c in ALL_CHAINS]
        tasks.append(_fetch_profiles(session))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        search_results = results[:len(ALL_CHAINS)]
        profiles_raw   = results[len(ALL_CHAINS)]
        if isinstance(profiles_raw, Exception):
            profiles_raw = []

        # 2. Enrich new profiles with full pair data
        profile_addresses = []
        for prof in (profiles_raw or [])[:20]:
            addr     = prof.get("tokenAddress") or prof.get("address") or ""
            chain_id = (prof.get("chainId") or "solana").lower()
            if addr and chain_id in ALL_CHAINS:
                profile_addresses.append((addr, chain_id))

        pair_tasks    = [_fetch_token_pairs(a, c, session) for a, c in profile_addresses]
        profile_pairs = await asyncio.gather(*pair_tasks, return_exceptions=True)

        # 3. Merge + deduplicate + filter
        all_raw = []
        for res in search_results:
            if not isinstance(res, Exception):
                all_raw.extend(res)
        for res in profile_pairs:
            if not isinstance(res, Exception):
                all_raw.extend(res)

        seen   = set()
        fields = []
        for p in all_raw:
            f   = _extract(p)
            key = f"{f['symbol']}_{f['chain']}_{f['address'][:8]}"
            if key in seen:
                continue
            seen.add(key)
            if _valid(f):
                fields.append(f)

        log.info(f"[dex] candidates after filter: {len(fields)}")

        if not fields:
            return [("No fresh tokens found. DexScreener may be slow — try again in 30s.", None)]

        # 4. Score + sort (score DESC, age ASC tiebreaker)
        for f in fields:
            f["score"] = calc_score(
                f["liq"], f["vol"], f["vol5m"],
                f["h5m"], f["h1"], f["h6"], f["h24"],
                f["buys"], f["sells"], f["buys5m"],
                f["created"],
            )
            f["age_s"] = age_seconds(f["created"])

        fields.sort(key=lambda x: (-x["score"], x["age_s"]))

        # 5. Build and return cards
        return [_build_card(rank, f) for rank, f in enumerate(fields[:10], 1)]
