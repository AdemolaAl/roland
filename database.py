from pymongo import MongoClient
from configs import cfg
from datetime import datetime, timedelta
import pytz

client = MongoClient(cfg.MONGO_URI)

users = client[cfg.DB_NAME]['users']
groups = client[cfg.DB_NAME]['groups']
onboarding = client[cfg.DB_NAME]['onboarding']

def already_db(user_id):
        user = users.find_one({"user_id" : str(user_id)})
        if not user:
            return False
        return True

def already_dbg(chat_id):
        group = groups.find_one({"chat_id" : str(chat_id)})
        if not group:
            return False
        return True

def add_user(user_id):
    in_db = already_db(user_id)
    if in_db:
        return
    return users.insert_one({"user_id": str(user_id)}) 

def remove_user(user_id):
    in_db = already_db(user_id)
    if not in_db:
        return 
    return users.delete_one({"user_id": str(user_id)})
    
def add_group(chat_id):
    in_db = already_dbg(chat_id)
    if in_db:
        return
    return groups.insert_one({"chat_id": str(chat_id)})

def all_users():
    user = users.find({})
    usrs = len(list(user))
    return usrs

def all_groups():
    group = groups.find({})
    grps = len(list(group))
    return grps

# Roland v2 — new per-message sent flags
V2_SENT_FLAGS = (
    "stuck_step_1_sent",
    "stuck_step_2_sent",
    "stuck_step_3_sent",
    "m5_sent",
    "m6_sent",
    "f1_sent",
    "f2_sent",
    "f3_sent",
    "f4_sent",
    "f5_sent",
    "f6_sent",
    "f7_sent",
)

V2_TIMESTAMP_FIELDS = (
    "broker_link_tapped_at",
    "account_created_at",
    "funded_at",
    "support_contacted_at",
    "do_later_at",  # set when user taps "I'LL DO IT LATER" on M6 to fast-track F1
)


# Onboarding functions
def add_onboarding_user(user_id, first_name):
    """Add user to onboarding tracking with their first name"""
    utc = pytz.UTC
    now = datetime.now(utc)

    doc = {
        "user_id": str(user_id),
        "first_name": first_name,
        "onboarding_stage": "welcome_sent",
        "created_at": now,
        # legacy follow-up flags (kept so legacy users still work)
        "follow_up_30min_sent": False,
        "follow_up_30min_sent_at": None,
        "follow_up_1h_sent": False,
        "follow_up_1h_sent_at": None,
        "follow_up_3h_sent": False,
        "follow_up_3h_sent_at": None,
        "setup_completed": False,
        "account_verified": False,
        # Roland v2 fields — new records are v2-eligible by default
        "chosen_mode": None,
        "v2_eligible": True,
    }
    for f in V2_TIMESTAMP_FIELDS:
        doc[f] = None
    for f in V2_SENT_FLAGS:
        doc[f] = False
    return onboarding.insert_one(doc)

def get_onboarding_user(user_id):
    """Get onboarding data for a user"""
    return onboarding.find_one({"user_id": str(user_id)})

def update_onboarding_stage(user_id, stage):
    """Update user's onboarding stage"""
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"onboarding_stage": stage}}
    )

def mark_follow_up_sent(user_id, follow_up_type):
    """Mark a follow-up as sent with timestamp"""
    utc = pytz.UTC
    now = datetime.now(utc)
    
    field = f"follow_up_{follow_up_type}_sent"
    timestamp_field = f"follow_up_{follow_up_type}_sent_at"
    
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {
            field: True,
            timestamp_field: now
        }}
    )

def mark_setup_completed(user_id, completed=True):
    """Mark user's setup as completed"""
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"setup_completed": completed}}
    )

def mark_account_verified(user_id, verified=True):
    """Mark user's account as verified"""
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"account_verified": verified}}
    )

def get_users_for_follow_up(follow_up_type, minutes_ago):
    """Get users who need follow-up messages with timestamp-based anti-spam filtering"""
    utc = pytz.UTC
    cutoff_time = datetime.now(utc) - timedelta(minutes=minutes_ago)
    
    field = f"follow_up_{follow_up_type}_sent"
    timestamp_field = f"follow_up_{follow_up_type}_sent_at"
    
    # Base query with timestamp-based duplicate prevention
    query = {
        "created_at": {"$lte": cutoff_time},
        field: {"$ne": True},  # Has not been sent
        timestamp_field: {"$eq": None},  # No timestamp recorded
        "setup_completed": {"$ne": True}  # Setup not completed
    }
    
    # Additional anti-spam: prevent sending same follow-up within 24 hours even if marked false
    recent_cutoff = datetime.now(utc) - timedelta(hours=24)
    query[timestamp_field] = {"$or": [
        {"$eq": None},  # Never sent
        {"$lt": recent_cutoff}  # Sent more than 24 hours ago
    ]}
    
    # Sequential follow-up enforcement
    if follow_up_type == "30min":
        # Only send 30min follow-up if no other follow-ups sent yet
        query.update({
            "follow_up_1h_sent": {"$ne": True},
            "follow_up_3h_sent": {"$ne": True}
        })
    elif follow_up_type == "1h":
        # Only send 1h follow-up if 30min was already sent
        query.update({
            "follow_up_30min_sent": True,
            "follow_up_30min_sent_at": {"$ne": None}  # Must have timestamp
        })
    elif follow_up_type == "3h":
        # Only send 3h follow-up if 1h was already sent
        query.update({
            "follow_up_1h_sent": True,
            "follow_up_1h_sent_at": {"$ne": None}  # Must have timestamp
        })
    
    # Additional time protection - don't send if created too recently
    min_creation_time = datetime.now(utc) - timedelta(minutes=minutes_ago + 10)
    query["created_at"]["$lte"] = min_creation_time
    
    return onboarding.find(query).limit(2)  # Very strict limit

def migrate_existing_users():
    """Add timestamp fields to existing users who don't have them"""
    try:
        # Find users missing timestamp fields
        users_to_update = onboarding.find({
            "$or": [
                {"follow_up_30min_sent_at": {"$exists": False}},
                {"follow_up_1h_sent_at": {"$exists": False}},
                {"follow_up_3h_sent_at": {"$exists": False}}
            ]
        })
        
        updated_count = 0
        for user in users_to_update:
            update_fields = {}
            
            # Add missing timestamp fields as None
            if "follow_up_30min_sent_at" not in user:
                update_fields["follow_up_30min_sent_at"] = None
            if "follow_up_1h_sent_at" not in user:
                update_fields["follow_up_1h_sent_at"] = None
            if "follow_up_3h_sent_at" not in user:
                update_fields["follow_up_3h_sent_at"] = None
            
            if update_fields:
                onboarding.update_one(
                    {"_id": user["_id"]},
                    {"$set": update_fields}
                )
                updated_count += 1
        
        return updated_count
    except Exception as e:
        print(f"Error migrating users: {e}")
        return 0

def is_user_in_onboarding(user_id):
    """Check if user is in onboarding process"""
    user = onboarding.find_one({"user_id": str(user_id)})
    return user is not None

def already_onboarding(user_id):
    """Check if user already has onboarding record"""
    return onboarding.find_one({"user_id": str(user_id)}) is not None

def reset_onboarding(user_id):
    """Reset onboarding for a user (delete existing record)"""
    return onboarding.delete_one({"user_id": str(user_id)})


# ─────────────────────────────────────────────────────────────────────────────
# Roland v2 helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(pytz.UTC)


def set_chosen_mode(user_id, mode):
    """mode: 'SIGNALS' or 'AUTO-TRADE'. Also stamps broker_link_tapped_at."""
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"chosen_mode": mode, "broker_link_tapped_at": _now()}},
    )


def set_account_created(user_id):
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"account_created_at": _now()}},
    )


def set_funded(user_id):
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"funded_at": _now()}},
    )


def set_support_contacted(user_id):
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"support_contacted_at": _now()}},
    )


def set_do_later(user_id):
    """User tapped 'I'LL DO IT LATER' on M6 — fast-tracks F1."""
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"do_later_at": _now()}},
    )


def mark_v2_message_sent(user_id, msg_type):
    """msg_type: one of V2_SENT_FLAGS without the _sent suffix (e.g. 'stuck_step_1', 'm5', 'f3')."""
    flag = f"{msg_type}_sent"
    if flag not in V2_SENT_FLAGS:
        raise ValueError(f"unknown msg_type: {msg_type}")
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {flag: True, f"{flag}_at": _now()}},
    )


def admin_activate(user_id):
    """SUDO override: set funded_at + support_contacted_at to kill all pending jobs."""
    now = _now()
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"funded_at": now, "support_contacted_at": now}},
    )


def set_v2_eligible(user_id, eligible=True):
    """Flip v2 scheduler eligibility for a user.

    Called when a legacy user re-enters the v2 flow (e.g. taps /start and gets
    routed through route_v2_entry), so the scheduler picks them up properly.
    """
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"v2_eligible": bool(eligible)}},
    )


def set_unreachable(user_id, unreachable=True):
    """Flag a user the bot can't message (never started this bot, blocked it,
    or deleted their account) so the v2 scheduler stops retrying them.
    Cleared again when the user taps /start.
    """
    return onboarding.update_one(
        {"user_id": str(user_id)},
        {"$set": {"unreachable": bool(unreachable)}},
    )


def get_active_v2_users():
    """Return users who still need v2 scheduled messages.

    Hard-gated on v2_eligible=True so pre-v2 legacy users are NEVER pinged with
    new-flow follow-ups (token 89V8001, "your batch", held-slot recovery, etc).
    Also filtered to records created in the last 14 days (3-day buffer past
    the day-10 F7 trigger). Funded users are still included so Stuck-Step-3
    can fire after funding — the predicates handle per-message gating.
    """
    cutoff = _now() - timedelta(days=14)
    return onboarding.find({
        "v2_eligible": True,
        "created_at": {"$gte": cutoff},
        "unreachable": {"$ne": True},
        "$or": [
            {"support_contacted_at": None},
            {"support_contacted_at": {"$exists": False}},
        ],
    })


def migrate_to_v2_schema():
    """Backfill v2 fields onto pre-existing onboarding records.

    Idempotent — uses $exists checks so it only touches records missing fields.
    Critically, records that EXISTED before v2 was deployed are marked
    v2_eligible=False so the new scheduler never targets them with token /
    "your batch" messaging they never opted into. New records created after
    deploy get v2_eligible=True via add_onboarding_user.
    """
    updated = 0
    try:
        # Backfill timestamp fields
        for field in V2_TIMESTAMP_FIELDS + ("chosen_mode",):
            res = onboarding.update_many(
                {field: {"$exists": False}},
                {"$set": {field: None}},
            )
            updated += res.modified_count

        # Backfill sent flags
        for flag in V2_SENT_FLAGS:
            res = onboarding.update_many(
                {flag: {"$exists": False}},
                {"$set": {flag: False}},
            )
            updated += res.modified_count

        # Mark all pre-existing records as v2-ineligible (one-shot).
        # New records created post-deploy already get v2_eligible=True from
        # add_onboarding_user, so this never touches them.
        res = onboarding.update_many(
            {"v2_eligible": {"$exists": False}},
            {"$set": {"v2_eligible": False}},
        )
        updated += res.modified_count

        return updated
    except Exception as e:
        print(f"Error migrating to v2 schema: {e}")
        return updated
