"""
admin_stats.py — drop-in admin statistics module for funnel bots.

Identical across all funnel bot projects. Auto-detects v2 vs legacy onboarding
schema at runtime, so the same file works on every bot.

Purpose: give admins ACCURATE lead numbers (per day, per period) so Meta Ads
lead counts / cost-per-lead can be audited against the real database.

Usage (2 lines in bot.py, just before `if __name__ == "__main__":`):

    from admin_stats import register_stats_handlers
    register_stats_handlers(app)

Read-only: this module never writes to the database.
"""

import logging
from datetime import datetime, timedelta

import pytz
from pyrogram import filters, StopPropagation
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from configs import cfg
from database import onboarding, users

logger = logging.getLogger(__name__)

TZ = pytz.timezone("Africa/Lagos")
UTC = pytz.UTC

# Stages where the lead exists but the welcome DM was never confirmed delivered
INITIAL_STAGES = ["welcome_sent", "awaiting_channel_join"]
# Stages that prove the user pressed /start (or got further)
STARTED_STAGES = ["start_clicked", "verified", "completed", "setup_reminder_sent"]

# v2 funnel timestamp fields (absent on legacy schemas — harmless there)
V2_FUNNEL_FIELDS = (
    ("broker_link_tapped_at", "🔗 Broker link tapped"),
    ("account_created_at", "🏦 Account created"),
    ("funded_at", "💰 Funded"),
    ("support_contacted_at", "📞 Contacted support"),
)

# Aggregation condition: "this user demonstrably started the bot"
_STARTED_COND = {
    "$or": [
        {"$in": [{"$ifNull": ["$onboarding_stage", ""]}, STARTED_STAGES]},
        {"$eq": ["$setup_completed", True]},
        {"$eq": ["$account_verified", True]},
        {"$gt": [{"$ifNull": ["$chosen_mode", None]}, None]},
        {"$gt": [{"$ifNull": ["$broker_link_tapped_at", None]}, None]},
        {"$gt": [{"$ifNull": ["$funded_at", None]}, None]},
    ]
}

# "welcome DM reached this user" — stage moved past initial, OR they provably
# started (some records progress without the stage field being updated)
_DELIVERED_COND = {
    "$or": [
        {
            "$not": [
                {"$in": [{"$ifNull": ["$onboarding_stage", ""]}, INITIAL_STAGES]}
            ]
        },
        _STARTED_COND,
    ]
}

PERIODS = (
    ("today", "Today"),
    ("yday", "Yesterday"),
    ("7", "7d"),
    ("14", "14d"),
    ("30", "30d"),
    ("all", "All-time"),
)
PERIOD_LABELS = dict(PERIODS)
DEFAULT_PERIOD = "7"


def _now_local():
    return datetime.now(TZ)


def _period_bounds(period):
    """Return (start_utc, end_utc, prev_start_utc, prev_end_utc) — None means unbounded."""
    now = _now_local()
    today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period == "today":
        start, end = today0, now
        prev_start, prev_end = today0 - timedelta(days=1), today0
    elif period == "yday":
        start, end = today0 - timedelta(days=1), today0
        prev_start, prev_end = today0 - timedelta(days=2), today0 - timedelta(days=1)
    elif period == "all":
        return None, None, None, None
    else:
        days = int(period)
        start, end = now - timedelta(days=days), now
        prev_start, prev_end = start - timedelta(days=days), start

    to_utc = lambda dt: dt.astimezone(UTC)
    return to_utc(start), to_utc(end), to_utc(prev_start), to_utc(prev_end)


def _is_v2_schema():
    """True if this bot's onboarding collection carries v2 funnel timestamps."""
    try:
        return onboarding.count_documents(
            {"broker_link_tapped_at": {"$exists": True}}, limit=1
        ) > 0
    except Exception as e:
        logger.error(f"admin_stats schema detect failed: {e}")
        return False


def _gather(period):
    """Run the (read-only) aggregation and return a stats dict."""
    start, end, prev_start, prev_end = _period_bounds(period)

    match = {}
    if start is not None:
        match = {"created_at": {"$gte": start, "$lt": end}}

    funnel_group = {
        "_id": None,
        "total": {"$sum": 1},
        "delivered": {"$sum": {"$cond": [_DELIVERED_COND, 1, 0]}},
        "started": {"$sum": {"$cond": [_STARTED_COND, 1, 0]}},
        "setup_completed": {
            "$sum": {"$cond": [{"$eq": ["$setup_completed", True]}, 1, 0]}
        },
        "account_verified": {
            "$sum": {"$cond": [{"$eq": ["$account_verified", True]}, 1, 0]}
        },
        "chose_mode": {
            "$sum": {"$cond": [{"$gt": [{"$ifNull": ["$chosen_mode", None]}, None]}, 1, 0]}
        },
        "mode_signals": {
            "$sum": {"$cond": [{"$eq": ["$chosen_mode", "SIGNALS"]}, 1, 0]}
        },
        "mode_auto": {
            "$sum": {
                "$cond": [
                    {
                        "$and": [
                            {"$gt": [{"$ifNull": ["$chosen_mode", None]}, None]},
                            {"$ne": ["$chosen_mode", "SIGNALS"]},
                        ]
                    },
                    1,
                    0,
                ]
            }
        },
    }
    for field, _label in V2_FUNNEL_FIELDS:
        funnel_group[field] = {
            "$sum": {"$cond": [{"$gt": [{"$ifNull": [f"${field}", None]}, None]}, 1, 0]}
        }

    pipeline = [
        {"$match": match},
        {
            "$facet": {
                "funnel": [{"$group": funnel_group}],
                "daily": [
                    {
                        "$group": {
                            "_id": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$created_at",
                                    "timezone": "Africa/Lagos",
                                }
                            },
                            "joins": {"$sum": 1},
                            "started": {"$sum": {"$cond": [_STARTED_COND, 1, 0]}},
                        }
                    },
                    {"$sort": {"_id": -1}},
                    {"$limit": 31},
                ],
            }
        },
    ]

    result = list(onboarding.aggregate(pipeline))[0]
    funnel = result["funnel"][0] if result["funnel"] else {}
    daily = result["daily"]

    prev_total = None
    if prev_start is not None:
        prev_total = onboarding.count_documents(
            {"created_at": {"$gte": prev_start, "$lt": prev_end}}
        )

    return {
        "funnel": funnel,
        "daily": daily,
        "prev_total": prev_total,
        "start": start,
        "end": end,
        "all_users": users.estimated_document_count(),
        "all_onboarding": onboarding.estimated_document_count(),
    }


def _pct(part, whole):
    return f"{(part / whole * 100):.1f}%" if whole else "0.0%"


def _bar(value, max_value, width=8):
    if max_value <= 0:
        return ""
    filled = round(value / max_value * width)
    return "█" * filled + "▏" * (1 if filled == 0 and value > 0 else 0)


def _trend(current, previous):
    if previous is None:
        return ""
    if previous == 0:
        return " (prev: 0)" if current == 0 else " (prev: 0 · 🆕)"
    delta = (current - previous) / previous * 100
    arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "▶")
    return f" (prev: {previous} · {arrow} {delta:+.0f}%)"


def _build_report(period):
    data = _gather(period)
    f = data["funnel"]
    is_v2 = _is_v2_schema()

    total = f.get("total", 0)
    delivered = f.get("delivered", 0)
    started = f.get("started", 0)
    ghosts = total - delivered

    bot_name = getattr(cfg, "BOT_NAME", None) or "Bot"
    label = PERIOD_LABELS.get(period, period)

    if data["start"] is not None:
        s = data["start"].astimezone(TZ).strftime("%b %d")
        e = data["end"].astimezone(TZ).strftime("%b %d")
        window = f"{s} → {e}" if s != e else s
    else:
        window = "entire history"

    lines = [
        f"📊 **{bot_name} — Statistics**",
        f"🗓 **{label}** ({window}) · days in Lagos time",
        "",
        "━━━ **LEADS** (ad-audit) ━━━",
        f"🆕 New leads: `{total}`{_trend(total, data['prev_total'])}",
        f"📬 Welcome delivered: `{delivered}` ({_pct(delivered, total)})",
        f"🚀 Started bot: `{started}` ({_pct(started, total)})",
        f"👻 Ghost joins: `{ghosts}` ({_pct(ghosts, total)})",
        "   ↳ counted as leads but never reachable",
    ]

    daily = data["daily"]
    if daily and period != "all":
        lines += ["", "━━━ **DAILY** (joins · started) ━━━"]
        max_joins = max(d["joins"] for d in daily)
        for d in daily:
            try:
                day = datetime.strptime(d["_id"], "%Y-%m-%d").strftime("%b %d")
            except (ValueError, TypeError):
                day = str(d["_id"])
            bar = _bar(d["joins"], max_joins)
            lines.append(f"`{day} {bar} {d['joins']} · {d['started']}`")

    lines += ["", "━━━ **FUNNEL** ━━━"]
    funnel_rows = [
        ("👋 Lead (joined)", total),
        ("📬 DM delivered", delivered),
        ("🚀 Started bot", started),
    ]
    if is_v2:
        chose = f.get("chose_mode", 0)
        funnel_rows.append(
            (f"🎯 Chose mode (SIG {f.get('mode_signals', 0)} · AUTO {f.get('mode_auto', 0)})", chose)
        )
        for field, flabel in V2_FUNNEL_FIELDS:
            funnel_rows.append((flabel, f.get(field, 0)))
    else:
        funnel_rows.append(("✅ Setup completed", f.get("setup_completed", 0)))
        funnel_rows.append(("🎯 Account verified", f.get("account_verified", 0)))

    for flabel, count in funnel_rows:
        lines.append(f"{flabel}: `{count}` ({_pct(count, total)})")

    lines += [
        "",
        "━━━ **ALL-TIME** ━━━",
        f"👤 Users in DB: `{data['all_users']}` · Onboarding: `{data['all_onboarding']}`",
    ]

    return "\n".join(lines)


def _keyboard(current):
    def btn(key):
        label = PERIOD_LABELS[key]
        if key == current:
            label = f"• {label} •"
        return InlineKeyboardButton(label, callback_data=f"stx:{key}")

    return InlineKeyboardMarkup(
        [
            [btn("today"), btn("yday"), btn("7")],
            [btn("14"), btn("30"), btn("all")],
            [InlineKeyboardButton("🔄 Refresh", callback_data=f"stx:{current}")],
        ]
    )


async def _stats_command(_, m):
    try:
        text = _build_report(DEFAULT_PERIOD)
        await m.reply_text(text, reply_markup=_keyboard(DEFAULT_PERIOD))
    except Exception as e:
        logger.error(f"/stats failed: {e}")
        try:
            await m.reply_text(f"⚠️ Stats error: `{e}`")
        except Exception:
            pass
    # Never let the funnel's catch-all private handler also reply to /stats
    raise StopPropagation


async def _stats_callback(_, cb):
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        raise StopPropagation
    period = cb.data.split(":", 1)[1]
    if period not in PERIOD_LABELS:
        period = DEFAULT_PERIOD
    try:
        text = _build_report(period)
        await cb.edit_message_text(text, reply_markup=_keyboard(period))
        await cb.answer()
    except Exception as e:
        # MessageNotModified (refresh with identical data) lands here too — fine
        if "MESSAGE_NOT_MODIFIED" in str(e).upper().replace(" ", "_"):
            try:
                await cb.answer("Already up to date ✅")
            except Exception:
                pass
        else:
            logger.error(f"stats callback failed: {e}")
            try:
                await cb.answer("⚠️ Stats error — check logs", show_alert=True)
            except Exception:
                pass
    raise StopPropagation


def register_stats_handlers(app):
    """Attach /stats + filter buttons to the bot. Registered in group -1 so the
    funnel's catch-all private handler can never swallow or double-reply."""
    app.add_handler(
        MessageHandler(
            _stats_command,
            filters.private & filters.command("stats") & filters.user(cfg.SUDO),
        ),
        group=-1,
    )
    app.add_handler(
        CallbackQueryHandler(_stats_callback, filters.regex(r"^stx:")),
        group=-1,
    )
    logger.info("admin_stats: /stats handlers registered")
