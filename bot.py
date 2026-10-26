import logging
import asyncio
import random
import os
import time
from datetime import datetime, timedelta
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from pyrogram import filters, Client, errors, enums
from pyrogram.errors import UserNotParticipant, FloodWait
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import pytz

from pacer import SendPacer

from database import (
    add_user, add_group, all_users, all_groups, users, remove_user,
    add_onboarding_user, get_onboarding_user, update_onboarding_stage,
    mark_follow_up_sent, mark_setup_completed, mark_account_verified,
    get_users_for_follow_up, is_user_in_onboarding, already_onboarding,
    reset_onboarding,
    # Roland v2
    set_chosen_mode, set_account_created, set_funded, set_support_contacted,
    set_do_later, mark_v2_message_sent, admin_activate, get_active_v2_users,
    migrate_to_v2_schema, set_v2_eligible, set_unreachable,
)
from configs import cfg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Create unique session name for different environments
session_name = f"graceboy_bot_{os.getenv('RAILWAY_ENVIRONMENT', 'local')}_{hash(cfg.BOT_TOKEN) % 10000}"

# Ensure sessions directory exists and is writable
import os
sessions_dir = "/tmp/sessions"
os.makedirs(sessions_dir, exist_ok=True)

# Initialize bot client with improved connection settings
app = Client(
    session_name,
    api_id=cfg.API_ID,
    api_hash=cfg.API_HASH,
    bot_token=cfg.BOT_TOKEN,
    workers=25,  # Reduced workers to prevent overwhelming connections
    sleep_threshold=20,  # Floods longer than this raise FloodWait to our
                         # backoff instead of silently parking a worker for
                         # up to 60s inside a single send.
    workdir=sessions_dir,  # Store session files in /tmp
    in_memory=False,  # Use persistent sessions
    max_concurrent_transmissions=10  # Limit concurrent transmissions
)

# Initialize scheduler with proper job settings
scheduler = AsyncIOScheduler()

# Global outbound pacing. A single shared pacer spaces EVERY send in the
# process (onboarding + v2 scheduler), enforced atomically across all 25
# dispatcher workers so concurrent bursts can't trip Telegram's flood limits.
# ~5 sends/sec aggregate leaves comfortable headroom under Telegram's ceiling.
SEND_MIN_INTERVAL = 0.2
send_pacer = SendPacer(SEND_MIN_INTERVAL)

async def rate_limited_send(func, *args, **kwargs):
    """Apply global rate limiting to API calls with exponential backoff."""
    await send_pacer.wait()

    max_retries = 5
    base_delay = 2

    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except (errors.FloodWait, ConnectionError, OSError) as e:
            if attempt == max_retries - 1:
                raise
            
            if isinstance(e, errors.FloodWait):
                delay = e.value
            else:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            
            logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay:.2f}s")
            await asyncio.sleep(delay)
    
    raise Exception(f"Failed after {max_retries} attempts")


async def broadcast_send_with_retry(action_factory, userid, *, max_conn_retries=4, max_flood_retries=3):
    """Run a single broadcast send, retrying transient failures.

    `action_factory` must be a zero-arg callable returning a *fresh* awaitable
    each time it is called (a coroutine can only be awaited once).

    "Connection lost" (ConnectionError/OSError) is what tears down in-flight
    sends when Telegram resets the socket mid-broadcast — the connection
    self-heals, so we back off and retry instead of dropping the recipient.
    FloodWait is honoured. Blocked / deactivated / invalid users are permanent.

    Returns one of: "success", "deactivated", "blocked", "failed".
    """
    conn_attempts = 0
    flood_attempts = 0
    while True:
        try:
            await action_factory()
            return "success"
        except FloodWait as ex:
            flood_attempts += 1
            if flood_attempts > max_flood_retries:
                logger.error(f"Broadcast giving up on {userid} after {max_flood_retries} flood waits")
                return "failed"
            await asyncio.sleep(ex.value + 1)
        except (ConnectionError, OSError) as e:
            conn_attempts += 1
            if conn_attempts > max_conn_retries:
                logger.error(f"Broadcast giving up on {userid} after {max_conn_retries} connection retries: {e}")
                return "failed"
            delay = 2 * (2 ** (conn_attempts - 1)) + random.uniform(0, 1)
            logger.warning(f"Connection lost sending to {userid} (retry {conn_attempts}/{max_conn_retries}) in {delay:.1f}s")
            await asyncio.sleep(delay)
        except errors.InputUserDeactivated:
            remove_user(userid)
            return "deactivated"
        except errors.UserIsBlocked:
            remove_user(userid)
            return "blocked"
        except errors.PeerIdInvalid:
            return "failed"
        except Exception as e:
            logger.error(f"Broadcast error for {userid}: {str(e)}")
            return "failed"

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Admin Menu ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_admin_menu_keyboard():
    """Get the admin control panel keyboard"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Statistics", callback_data="adm_stats"),
            InlineKeyboardButton("📢 Broadcast Guide", callback_data="adm_bcast_help")
        ],
        [
            InlineKeyboardButton("👥 Pending Requests", callback_data="adm_pending"),
            InlineKeyboardButton("⚙️ Bot Config", callback_data="adm_config")
        ],
        [
            InlineKeyboardButton("🔄 Reset Follow-ups", callback_data="adm_reset_fu"),
            InlineKeyboardButton("🧹 Cleanup Help", callback_data="adm_cleanup")
        ],
        [
            InlineKeyboardButton("📋 All Commands", callback_data="adm_commands")
        ]
    ])

@app.on_message(filters.command("menu") & filters.user(cfg.SUDO))
async def admin_menu_command(_, m: Message):
    """Show admin control panel"""
    menu_text = (
        "🤖 **Admin Control Panel**\n\n"
        "Use the buttons below to manage the bot:"
    )
    await m.reply_text(menu_text, reply_markup=get_admin_menu_keyboard())

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Debug Handler (Priority) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("test") & filters.private)
async def test_admin_priority(_, m: Message):
    """Test admin permissions and message functionality"""
    user_id = m.from_user.id
    first_name = m.from_user.first_name or "Friend"
    
    logger.info(f"🔍 PRIORITY: /test command from user {user_id}")
    logger.info(f"🔍 PRIORITY: cfg.SUDO contains: {cfg.SUDO}")
    logger.info(f"🔍 PRIORITY: Is {user_id} in SUDO? {user_id in cfg.SUDO}")
    
    try:
        if user_id in cfg.SUDO:
            await m.reply_text(f"✅ You are an admin! Your ID: {user_id}")
            
            # Test welcome message functionality
            try:
                logger.info(f"🔍 DEBUG: Testing welcome message for admin {user_id}")
                await send_welcome_message(user_id, first_name)
                await m.reply_text("✅ Welcome message test: SUCCESS")
            except Exception as e:
                logger.error(f"🔍 DEBUG: Welcome message test failed: {e}")
                await m.reply_text(f"❌ Welcome message test: FAILED - {e}")
                
        else:
            await m.reply_text(f"❌ You are not an admin. Your ID: {user_id}\nAdmin IDs: {cfg.SUDO}")
            
            # Test welcome message for regular user
            try:
                logger.info(f"🔍 DEBUG: Testing welcome message for user {user_id}")
                await send_welcome_message(user_id, first_name)
                await m.reply_text("✅ Welcome message test: SUCCESS")
            except Exception as e:
                logger.error(f"🔍 DEBUG: Welcome message test failed: {e}")
                await m.reply_text(f"❌ Welcome message test: FAILED - {e}")
                
    except Exception as e:
        logger.error(f"Error in test command: {e}")
        await m.reply_text(f"Error: {e}")

@app.on_message(filters.command("resetonboarding") & filters.user(cfg.SUDO))
async def reset_onboarding_command(_, m: Message):
    """Reset onboarding for a user (admin only)"""
    user_id = m.from_user.id
    
    try:
        # Check if there's a user ID in the command
        parts = m.text.split()
        if len(parts) > 1:
            target_user_id = int(parts[1])
        else:
            target_user_id = user_id
            
        if already_onboarding(target_user_id):
            reset_onboarding(target_user_id)
            await m.reply_text(f"✅ Reset onboarding for user {target_user_id}")
            logger.info(f"Admin {user_id} reset onboarding for user {target_user_id}")
        else:
            await m.reply_text(f"❌ User {target_user_id} has no onboarding record")
            
    except ValueError:
        await m.reply_text("❌ Invalid user ID. Usage: /resetonboarding [user_id]")
    except Exception as e:
        logger.error(f"Error in reset onboarding command: {e}")
        await m.reply_text(f"Error: {e}")

@app.on_message(filters.command("activate") & filters.user(cfg.SUDO))
async def activate_user_command(_, m: Message):
    """SUDO override: mark a user as funded+support-contacted, killing all pending v2 messages."""
    try:
        parts = m.text.split()
        if len(parts) < 2:
            await m.reply_text("Usage: /activate <user_id>")
            return
        target = int(parts[1])
        if not already_onboarding(target):
            await m.reply_text(f"❌ User {target} has no onboarding record")
            return
        admin_activate(target)
        mark_setup_completed(target, True)
        mark_account_verified(target, True)
        await m.reply_text(
            f"✅ Activated user {target} — all pending v2 messages cancelled."
        )
        logger.info(f"Admin {m.from_user.id} activated user {target}")
    except ValueError:
        await m.reply_text("❌ Invalid user ID. Usage: /activate <user_id>")
    except Exception as e:
        logger.error(f"activate_user_command failed: {e}")
        await m.reply_text(f"Error: {e}")


@app.on_message(filters.command("approvepending") & filters.user(cfg.SUDO))
async def approve_pending_requests(_, m: Message):
    """Approve all pending join requests (admin only)"""
    user_id = m.from_user.id
    
    try:
        status_msg = await m.reply_text("🔄 Processing pending requests...")
        
        approved = 0
        failed = 0
        
        # Debug and format channel ID properly
        logger.info(f"Raw CHID from config: {cfg.CHID}")
        
        # Ensure channel ID is properly formatted with negative sign
        channel_id = int(cfg.CHID)
        if channel_id > 0:
            channel_id = -channel_id  # Add negative sign for supergroup IDs
            
        logger.info(f"Processing pending requests for channel ID: {channel_id}")
        
        # Get all pending requests from the channel
        async for request in app.get_chat_join_requests(channel_id):
            try:
                await app.approve_chat_join_request(channel_id, request.user.id)
                
                # Add to database and start onboarding
                add_user(request.user.id)
                add_group(channel_id)
                
                # Start onboarding if not admin
                if request.user.id not in cfg.SUDO:
                    first_name = request.user.first_name or "Friend"
                    
                    # Reset existing onboarding for fresh start
                    if already_onboarding(request.user.id):
                        reset_onboarding(request.user.id)
                    
                    add_onboarding_user(request.user.id, first_name)
                    
                    # Try to send welcome message
                    try:
                        await send_welcome_message(request.user.id, first_name)
                        await asyncio.sleep(2)
                        await send_immediate_follow_up(request.user.id)
                        update_onboarding_stage(request.user.id, "welcome_actually_sent")
                    except errors.PeerIdInvalid:
                        logger.info(f"User {request.user.id} hasn't started bot - welcome will be sent when they message")
                    except Exception as e:
                        logger.warning(f"Could not send welcome to {request.user.id}: {e}")
                
                approved += 1
                
                # Update status every 10 approvals
                if approved % 10 == 0:
                    await status_msg.edit_text(f"🔄 Approved {approved} requests so far...")
                
                # Rate limit to avoid flooding
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Failed to approve request from {request.user.id}: {e}")
                failed += 1
        
        result_text = f"✅ Bulk approval completed!\n\n📊 **Results:**\n• Approved: {approved}\n• Failed: {failed}"
        await status_msg.edit_text(result_text)
        logger.info(f"Admin {user_id} bulk approved {approved} pending requests")
        
    except Exception as e:
        logger.error(f"Error in bulk approve command: {e}")
        await m.reply_text(f"❌ Error: {e}")

@app.on_message(filters.command("checkpending") & filters.user(cfg.SUDO))
async def check_pending_requests(_, m: Message):
    """Check how many pending join requests exist (admin only)"""
    user_id = m.from_user.id
    
    try:
        status_msg = await m.reply_text("🔍 Checking pending requests...")
        
        pending_count = 0
        pending_users = []
        
        # Debug channel ID formatting
        logger.info(f"Raw CHID from config: {cfg.CHID}")
        logger.info(f"CHID type: {type(cfg.CHID)}")
        
        # Ensure channel ID is properly formatted with negative sign
        channel_id = int(cfg.CHID)
        if channel_id > 0:
            channel_id = -channel_id  # Add negative sign for supergroup IDs
        
        logger.info(f"Final channel ID for requests: {channel_id}")
        
        # Count all pending requests
        async for request in app.get_chat_join_requests(channel_id):
            pending_count += 1
            pending_users.append(f"• {request.user.first_name or 'Unknown'} ({request.user.id})")
            
            # Limit display to first 20 users
            if pending_count >= 20:
                break
        
        if pending_count == 0:
            result_text = "✅ No pending join requests!"
        else:
            user_list = "\n".join(pending_users[:10])  # Show first 10
            if pending_count > 10:
                user_list += f"\n... and {pending_count - 10} more"
            
            result_text = f"📊 **Pending Join Requests: {pending_count}**\n\n👥 **Recent Users:**\n{user_list}\n\n💡 Use `/approvepending` to approve all at once"
        
        await status_msg.edit_text(result_text)
        logger.info(f"Admin {user_id} checked pending requests: {pending_count} found")
        
    except Exception as e:
        logger.error(f"Error checking pending requests: {e}")
        await m.reply_text(f"❌ Error: {e}")

@app.on_message(filters.command("checkconfig") & filters.user(cfg.SUDO))
async def check_config(_, m: Message):
    """Check current bot configuration (admin only)"""
    user_id = m.from_user.id
    
    try:
        config_info = f"""📋 **Bot Configuration:**

🆔 **Current CHID:** `{cfg.CHID}`
🆔 **Formatted CHID:** `{-int(cfg.CHID) if int(cfg.CHID) > 0 else int(cfg.CHID)}`

🤖 **Bot Token:** `{cfg.BOT_TOKEN[:10]}...` (truncated)
🔑 **API ID:** `{cfg.API_ID}`
👤 **Admin IDs:** `{cfg.SUDO}`

💡 **Instructions:**
• Check recent logs for "Join request received from user X in chat Y"
• The chat Y should match your CHID
• If different, update your CHID environment variable

📝 **Recent join request channel IDs from logs:**
• Check your deployment logs for the actual channel ID"""
        
        await m.reply_text(config_info)
        logger.info(f"Admin {user_id} checked bot configuration")
        
    except Exception as e:
        logger.error(f"Error checking config: {e}")
        await m.reply_text(f"❌ Error: {e}")

@app.on_message(filters.command("testbuttons") & filters.user(cfg.SUDO))
async def test_buttons_command(_, m: Message):
    """Test button functionality (admin only)"""
    user_id = m.from_user.id
    
    try:
        # Create test buttons
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Test Yes", callback_data="setup_yes"),
                InlineKeyboardButton("❌ Test No", callback_data="setup_no")
            ],
            [
                InlineKeyboardButton("🔄 Test Check", callback_data="chk")
            ]
        ])
        
        test_message = """
🧪 **Button Test for Admin**

Click the buttons below to test the callback functionality:

• **Test Yes** - Should show setup completion message
• **Test No** - Should show setup reminder message  
• **Test Check** - Should test subscription verification

This is for debugging purposes only.
        """
        
        await m.reply_text(test_message, reply_markup=keyboard)
        logger.info(f"Admin {user_id} initiated button test")
        
    except Exception as e:
        logger.error(f"Error in test buttons command: {e}")
        await m.reply_text(f"Error: {e}")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Onboarding Flow ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def send_welcome_message(user_id: int, first_name: str):
    """Send the initial welcome message"""
    welcome_text = cfg.WELCOME_MESSAGE.format(first_name=first_name)
    
    try:
        await rate_limited_send(app.send_message, user_id, welcome_text, disable_web_page_preview=True)
        logger.info(f"Welcome message sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error sending welcome message to {user_id}: {e}")

async def send_immediate_follow_up(user_id: int):
    """Send immediate follow-up message"""
    follow_up_text = cfg.IMMEDIATE_FOLLOW_UP
    
    try:
        await rate_limited_send(app.send_message, user_id, follow_up_text, disable_web_page_preview=True)
        logger.info(f"Immediate follow-up sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error sending immediate follow-up to {user_id}: {e}")

async def send_setup_instructions(user_id: int, first_name: str):
    """Send setup instructions after /start command"""
    setup_text = cfg.SETUP_INSTRUCTIONS.format(
        first_name=first_name,
        deposit_guide_link=cfg.DEPOSIT_GUIDE_LINK,
        results_channel_link=cfg.RESULTS_CHANNEL_LINK,
        support_username=cfg.SUPPORT_USERNAME
    )
    
    try:
        await rate_limited_send(app.send_message, user_id, setup_text, disable_web_page_preview=True)
        logger.info(f"Setup instructions sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error sending setup instructions to {user_id}: {e}")

async def send_support_message(user_id: int):
    """Send support message"""
    support_text = cfg.SUPPORT_MESSAGE.format(
        support_username=cfg.SUPPORT_USERNAME,
        deposit_guide_link=cfg.DEPOSIT_GUIDE_LINK,
        results_channel_link=cfg.RESULTS_CHANNEL_LINK
    )
    
    try:
        await rate_limited_send(app.send_message, user_id, support_text, disable_web_page_preview=True)
        logger.info(f"Support message sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error sending support message to {user_id}: {e}")

async def send_30min_follow_up(user_id: int, first_name: str):
    """Send 30-minute follow-up for funding reminder"""
    # Mark as sent FIRST to prevent duplicate sends
    mark_follow_up_sent(user_id, "30min")
    
    follow_up_text = cfg.FOLLOW_UP_30MIN.format(first_name=first_name)
    
    try:
        await rate_limited_send(app.send_message, user_id, follow_up_text, disable_web_page_preview=True)
        logger.info(f"30-minute follow-up sent to user {user_id}")
    except errors.PeerIdInvalid:
        logger.warning(f"Cannot send 30min follow-up to {user_id}: User hasn't started the bot yet")
    except errors.UserIsBlocked:
        logger.warning(f"User {user_id} has blocked the bot")
    except Exception as e:
        logger.error(f"Error sending 30-minute follow-up to {user_id}: {e}")

async def send_1hour_follow_up(user_id: int):
    """Send 1-hour follow-up with Yes/No buttons"""
    # Mark as sent FIRST to prevent duplicate sends
    mark_follow_up_sent(user_id, "1h")
    
    follow_up_text = cfg.FOLLOW_UP_1H
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, I have", callback_data="setup_yes"),
            InlineKeyboardButton("❌ No, not yet", callback_data="setup_no")
        ]
    ])
    
    try:
        await rate_limited_send(app.send_message, user_id, follow_up_text, reply_markup=keyboard, disable_web_page_preview=True)
        logger.info(f"1-hour follow-up sent to user {user_id}")
    except errors.PeerIdInvalid:
        logger.warning(f"Cannot send follow-up to {user_id}: User hasn't started the bot yet")
    except errors.UserIsBlocked:
        logger.warning(f"User {user_id} has blocked the bot")
    except Exception as e:
        logger.error(f"Error sending 1-hour follow-up to {user_id}: {e}")

async def send_3hour_follow_up(user_id: int, first_name: str):
    """Send 3-hour follow-up"""
    # Mark as sent FIRST to prevent duplicate sends
    mark_follow_up_sent(user_id, "3h")
    
    follow_up_text = cfg.FOLLOW_UP_3H.format(
        first_name=first_name,
        direct_contact_username=cfg.DIRECT_CONTACT_USERNAME,
        deposit_guide_link=cfg.DEPOSIT_GUIDE_LINK
    )
    
    try:
        await rate_limited_send(app.send_message, user_id, follow_up_text, disable_web_page_preview=True)
        logger.info(f"3-hour follow-up sent to user {user_id}")
    except errors.PeerIdInvalid:
        logger.warning(f"Cannot send 3h follow-up to {user_id}: User hasn't started the bot yet")
    except errors.UserIsBlocked:
        logger.warning(f"User {user_id} has blocked the bot")
    except Exception as e:
        logger.error(f"Error sending 3-hour follow-up to {user_id}: {e}")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Roland v2 — Keyboards ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def kb_mode_picker():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 SIGNALS — $20+ trading capital", callback_data="mode_signals")],
        [InlineKeyboardButton("🔵 AUTO-TRADE — $50+ trading capital", callback_data="mode_autotrade")],
        [InlineKeyboardButton("❓ WHICH ONE FITS ME?", callback_data="which_one")],
    ])


def kb_m3_difference():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 I PICK SIGNALS", callback_data="mode_signals")],
        [InlineKeyboardButton("🔵 I PICK AUTO-TRADE ⚡", callback_data="mode_autotrade")],
    ])


def kb_create_account(mode):
    url = cfg.SIGNALS_BROKER_URL if mode == "SIGNALS" else cfg.AUTOTRADE_BROKER_URL
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 CREATE MY TRADING ACCOUNT", url=url)],
    ])


def kb_account_created():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ACCOUNT CREATED", callback_data="account_created")],
    ])


def kb_funded():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I'VE FUNDED MY ACCOUNT", callback_data="funded")],
    ])


def kb_dm_support():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"💬 DM @{cfg.SUPPORT_USERNAME}", url=cfg.SUPPORT_URL)],
    ])


def kb_m5_proof():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 SHOW ME THE WINS", url=cfg.WINS_CHANNEL_LINK)],
        [InlineKeyboardButton("🚀 I'M FUNDED — TAKE ME TO SUPPORT", url=cfg.SUPPORT_URL)],
    ])


def kb_m6_checkin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ I'M SETTING UP NOW", callback_data="setting_up")],
        [InlineKeyboardButton("💬 I HAVE A QUESTION", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("⏸️ I'LL DO IT LATER", callback_data="do_later")],
        [InlineKeyboardButton("📊 I NEED MORE PROOF", url=cfg.WINS_CHANNEL_LINK)],
    ])


def kb_stuck_step_1():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 SHOW ME HOW TO REGISTER", url=cfg.GUIDE_CHANNEL_LINK)],
        [InlineKeyboardButton("💬 SUPPORT CAN HELP", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("✅ I'VE CREATED IT", callback_data="account_created")],
    ])


def kb_stuck_step_2():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 HOW TO FUND", url=cfg.GUIDE_CHANNEL_LINK)],
        [InlineKeyboardButton("💬 SUPPORT CAN HELP", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("✅ I'VE FUNDED IT", callback_data="funded")],
    ])


def kb_stuck_step_3():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 DM SUPPORT NOW", url=cfg.SUPPORT_URL)],
    ])


def kb_f1():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 ALMOST DONE — FUNDING NOW", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("💬 I HIT A PROBLEM", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("📘 SHOW ME THE STEPS", url=cfg.GUIDE_CHANNEL_LINK)],
    ])


def kb_f2():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚡ ACTIVATE TOKEN {cfg.PROMO_TOKEN}", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("📘 ONE MORE LOOK AT STEPS", url=cfg.GUIDE_CHANNEL_LINK)],
    ])


def kb_f3():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ ACTIVATE MY TOKEN NOW", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("📘 STEPS ONE MORE TIME", url=cfg.GUIDE_CHANNEL_LINK)],
    ])


def kb_f4():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 ACTIVATE ME NOW", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("💬 QUICK QUESTION FIRST", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("📊 LAST CHECK ON PROOF", url=cfg.WINS_CHANNEL_LINK)],
    ])


def kb_f5():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔥 ACTIVATE {cfg.PROMO_TOKEN} NOW", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("📊 ONE LAST LOOK", url=cfg.WINS_CHANNEL_LINK)],
    ])


def kb_f6():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 REOPEN MY TOKEN", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("📘 REMIND ME OF STEPS", url=cfg.GUIDE_CHANNEL_LINK)],
    ])


def kb_f7():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 ISSUE MY TOKEN — SIGNALS", url=cfg.SUPPORT_URL)],
        [InlineKeyboardButton("🔵 ISSUE MY TOKEN — AUTO ⚡", url=cfg.SUPPORT_URL)],
    ])


def kb_returning_supported():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 WINS CHANNEL", url=cfg.WINS_CHANNEL_LINK)],
        [InlineKeyboardButton(f"💬 DM @{cfg.SUPPORT_USERNAME}", url=cfg.SUPPORT_URL)],
    ])


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Roland v2 — Send helpers ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fmt(template, **kwargs):
    """Format a message template with config defaults + caller-supplied kwargs."""
    defaults = {
        "promo_token": cfg.PROMO_TOKEN,
        "promo_code": cfg.PROMO_CODE,
        "support_username": cfg.SUPPORT_USERNAME,
        "direct_contact_username": cfg.DIRECT_CONTACT_USERNAME,
        "deposit_guide_link": cfg.DEPOSIT_GUIDE_LINK,
        "guide_channel_link": cfg.GUIDE_CHANNEL_LINK,
        "wins_channel_link": cfg.WINS_CHANNEL_LINK,
        "results_channel_link": cfg.RESULTS_CHANNEL_LINK,
    }
    defaults.update(kwargs)
    try:
        return template.format(**defaults)
    except KeyError as e:
        logger.warning(f"Missing template var {e} — sending raw")
        return template


async def _send(user_id, text, reply_markup=None):
    """Returns True if the message was delivered."""
    try:
        await rate_limited_send(
            app.send_message, user_id, text,
            reply_markup=reply_markup, disable_web_page_preview=True,
        )
        return True
    except errors.PeerIdInvalid:
        logger.warning(f"Cannot send to {user_id}: user hasn't started bot")
        set_unreachable(user_id)
    except errors.UserIsBlocked:
        logger.warning(f"User {user_id} blocked the bot")
        set_unreachable(user_id)
    except errors.InputUserDeactivated:
        logger.warning(f"User {user_id} deleted their account")
        set_unreachable(user_id)
    except Exception as e:
        logger.error(f"Send to {user_id} failed: {e}")
    return False


async def _send_photo(user_id, photo_path, caption=None, reply_markup=None):
    """Send a photo with optional caption + inline keyboard. Falls back to a
    text-only message if the image file is missing so the flow never silently
    dies on a bad path."""
    if not photo_path or not os.path.exists(photo_path):
        logger.warning(f"Photo missing at {photo_path!r}, falling back to text-only")
        if caption:
            return await _send(user_id, caption, reply_markup=reply_markup)
        return False
    try:
        await rate_limited_send(
            app.send_photo, user_id, photo=photo_path,
            caption=caption, reply_markup=reply_markup,
        )
        return True
    except errors.PeerIdInvalid:
        logger.warning(f"Cannot send photo to {user_id}: user hasn't started bot")
        set_unreachable(user_id)
    except errors.UserIsBlocked:
        logger.warning(f"User {user_id} blocked the bot")
        set_unreachable(user_id)
    except errors.InputUserDeactivated:
        logger.warning(f"User {user_id} deleted their account")
        set_unreachable(user_id)
    except Exception as e:
        logger.error(f"Send photo to {user_id} failed: {e}")
        # Best-effort fallback so the flow doesn't dead-end
        if caption:
            return await _send(user_id, caption, reply_markup=reply_markup)
    return False


async def send_m2_reveal(user_id, first_name):
    await _send(user_id, _fmt(cfg.M2_REVEAL, first_name=first_name), kb_mode_picker())
    # Nudge 2s after M2 so the user notices the mode-picker buttons and taps one.
    if cfg.M2_FOLLOWUP:
        await asyncio.sleep(2)
        await _send(user_id, _fmt(cfg.M2_FOLLOWUP))


async def send_m3_difference(user_id):
    await _send(user_id, _fmt(cfg.M3_DIFFERENCE), kb_m3_difference())


async def send_4_bridge(user_id, first_name, mode):
    """Send 4a-i or 4b-i with the broker-create button, then 4a-ii / 4b-ii after a beat."""
    bridge = cfg.M4A_I_BRIDGE if mode == "SIGNALS" else cfg.M4B_I_BRIDGE
    confirm = cfg.M4A_II_CONFIRM if mode == "SIGNALS" else cfg.M4B_II_CONFIRM
    await _send(user_id, _fmt(bridge, first_name=first_name), kb_create_account(mode))
    await asyncio.sleep(8)
    await _send(user_id, _fmt(confirm), kb_account_created())


async def send_4_funding(user_id, first_name, mode):
    """Send 4a-iii or 4b-iii."""
    funding = cfg.M4A_III_FUNDING if mode == "SIGNALS" else cfg.M4B_III_FUNDING
    await _send(user_id, _fmt(funding, first_name=first_name), kb_funded())


async def send_4_support(user_id, mode):
    """Send 4a-iv or 4b-iv, then 4a-v / 4b-v after a beat."""
    support = cfg.M4A_IV_SUPPORT if mode == "SIGNALS" else cfg.M4B_IV_SUPPORT
    nextmsg = cfg.M4A_V_NEXT if mode == "SIGNALS" else cfg.M4B_V_NEXT
    await _send(user_id, _fmt(support), kb_dm_support())
    await asyncio.sleep(8)
    await _send(user_id, _fmt(nextmsg))


def _within_seconds(ts, seconds):
    """True if `ts` was set within the last N seconds. Used to dedupe rapid
    double-taps and Telegram callback retries that fire the same handler twice."""
    if ts is None:
        return False
    if ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    return (datetime.now(pytz.UTC) - ts).total_seconds() < seconds


async def route_v2_entry(user_id, first_name, *, force_welcome=False):
    """Resolve a user's current state and send the right v2 message.

    Behavior:
    - No onboarding record: create one, send M1 then M2 (mode picker).
    - Has record, no chosen_mode: send M2 (mode picker).
    - Has record, mode set, no account_created: re-send the 4-bridge (4a-i/4b-i).
    - Has record, account created, no funded_at: re-send funding step (4a-iii/4b-iii).
    - Has record, funded but no support_contacted: re-send support step (4a-iv/4b-iv).
    - Has record, support contacted (or admin-activated): send "you're in" message.

    Set `force_welcome=True` to force M1 first even on a returning user (used by
    the channel auto-approve flow so the welcome opener doesn't get skipped).
    """
    if not already_onboarding(user_id):
        # First-time entry (channel join, cold DM, or cold /start).
        # Per spec: send M1 + immediate-follow-up only. M2 (the reveal) waits
        # for the user to explicitly tap /start.
        add_onboarding_user(user_id, first_name)
        add_user(user_id)
        await _send_photo(
            user_id, cfg.WELCOME_IMAGE,
            caption=_fmt(cfg.WELCOME_MESSAGE, first_name=first_name),
        )
        await asyncio.sleep(2)
        await _send(user_id, _fmt(cfg.IMMEDIATE_FOLLOW_UP, first_name=first_name))
        update_onboarding_stage(user_id, "welcome_actually_sent")
        return

    u = get_onboarding_user(user_id) or {}

    # If a legacy (pre-v2) user is being routed through v2 because they
    # interacted with the bot fresh, opt them into the v2 scheduler from now on.
    if not u.get("v2_eligible"):
        set_v2_eligible(user_id, True)

    if force_welcome:
        # Re-send M1 + IMMEDIATE_FOLLOW_UP, then STOP. M2 must wait for the
        # user to explicitly tap /start, per spec. Without this return, an
        # existing-record user being routed with force_welcome=True (e.g.
        # cold DM after a failed welcome delivery) would get M1 + M2 in one
        # burst, skipping the /start gate.
        await _send_photo(
            user_id, cfg.WELCOME_IMAGE,
            caption=_fmt(cfg.WELCOME_MESSAGE, first_name=first_name),
        )
        await asyncio.sleep(2)
        await _send(user_id, _fmt(cfg.IMMEDIATE_FOLLOW_UP, first_name=first_name))
        return

    if u.get("support_contacted_at"):
        await _send(user_id, _fmt(cfg.RETURNING_SUPPORTED, first_name=first_name),
                    kb_returning_supported())
        return

    if u.get("funded_at"):
        await _send(user_id, _fmt(cfg.RETURNING_FUNDED, first_name=first_name),
                    kb_dm_support())
        return

    mode = u.get("chosen_mode")

    if not mode:
        # User has a record (so they saw M1) and is now back — send M2 with a
        # 3-second beat so it doesn't feel like an instant pop.
        await asyncio.sleep(3)
        await send_m2_reveal(user_id, first_name)
        update_onboarding_stage(user_id, "welcome_actually_sent")
        return

    if not u.get("account_created_at"):
        await send_4_bridge(user_id, first_name, mode)
        return

    # account created but not funded
    await send_4_funding(user_id, first_name, mode)


# Hold references to in-flight background onboarding tasks so they aren't
# garbage-collected mid-run (asyncio only keeps weak refs to bare tasks).
_onboarding_tasks = set()


def spawn_onboarding(user_id, first_name, *, force_welcome=True):
    """Run route_v2_entry in the background so the caller (e.g. the join-request
    handler) returns immediately and frees its dispatcher worker. Under a flood,
    the sends inside route_v2_entry can block for seconds; running them inline
    backs up the update queue and freezes new approvals."""
    async def _runner():
        try:
            await route_v2_entry(user_id, first_name, force_welcome=force_welcome)
            logger.info(f"Started v2 onboarding for user {user_id} after auto-approval")
        except errors.PeerIdInvalid:
            logger.warning(
                f"🔍 DEBUG: PeerIdInvalid - User {user_id} hasn't started the bot "
                f"- will send welcome when they message us"
            )
        except Exception as e:
            logger.error(f"🔍 DEBUG: Error sending welcome message to {user_id}: {e}")
            logger.exception("Full exception details:")

    task = asyncio.create_task(_runner())
    _onboarding_tasks.add(task)
    task.add_done_callback(_onboarding_tasks.discard)
    return task


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Roland v2 — Scheduler predicates ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _utcnow():
    return datetime.now(pytz.UTC)


def _minutes_since(ts, now):
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    return (now - ts).total_seconds() / 60.0


def _days_since(ts, now):
    m = _minutes_since(ts, now)
    return None if m is None else m / 1440.0


def should_send_stuck_step_1(u, now):
    if u.get("stuck_step_1_sent"):
        return False
    if u.get("account_created_at"):
        return False
    if u.get("funded_at"):
        return False
    m = _minutes_since(u.get("broker_link_tapped_at"), now)
    return m is not None and m >= cfg.STUCK_STEP_MINUTES


def should_send_stuck_step_2(u, now):
    if u.get("stuck_step_2_sent"):
        return False
    if u.get("funded_at"):
        return False
    m = _minutes_since(u.get("account_created_at"), now)
    return m is not None and m >= cfg.STUCK_STEP_MINUTES


def should_send_stuck_step_3(u, now):
    if u.get("stuck_step_3_sent"):
        return False
    if u.get("support_contacted_at"):
        return False
    m = _minutes_since(u.get("funded_at"), now)
    return m is not None and m >= cfg.STUCK_STEP_MINUTES


def should_send_m5(u, now):
    """1 hour after broker link tap, only if user hasn't funded."""
    if u.get("m5_sent"):
        return False
    if u.get("funded_at"):
        return False
    m = _minutes_since(u.get("broker_link_tapped_at"), now)
    return m is not None and m >= cfg.M5_AFTER_BROKER_MINUTES


def should_send_m6(u, now):
    if u.get("m6_sent"):
        return False
    if u.get("funded_at"):
        return False
    # M6 fires 2h after M5
    if not u.get("m5_sent"):
        return False
    m = _minutes_since(u.get("m5_sent_at"), now)
    return m is not None and m >= cfg.M6_AFTER_M5_MINUTES


def should_send_f1(u, now):
    if u.get("f1_sent"):
        return False
    if u.get("funded_at"):
        return False
    # Fast-track if user tapped "I'LL DO IT LATER" on M6
    if u.get("do_later_at"):
        return True
    if not u.get("m6_sent"):
        return False
    m = _minutes_since(u.get("m6_sent_at"), now)
    return m is not None and m >= cfg.F1_AFTER_M6_MINUTES


def should_send_f2(u, now):
    if u.get("f2_sent") or u.get("funded_at") or not u.get("f1_sent"):
        return False
    m = _minutes_since(u.get("f1_sent_at"), now)
    return m is not None and m >= cfg.F2_AFTER_F1_MINUTES


def should_send_f3(u, now):
    if u.get("f3_sent") or u.get("funded_at") or not u.get("f2_sent"):
        return False
    m = _minutes_since(u.get("f2_sent_at"), now)
    return m is not None and m >= cfg.F3_AFTER_F2_MINUTES


def _local_tz():
    try:
        return pytz.timezone(cfg.USER_TIMEZONE)
    except Exception:
        return pytz.UTC


def _local_now():
    return datetime.now(_local_tz())


def _local_date_of(ts):
    """Return the local-TZ calendar date of a UTC datetime."""
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    return ts.astimezone(_local_tz()).date()


def should_send_f4(u, now):
    """Next morning local time (8-10 AM window) — strictly after entry's local calendar day."""
    if u.get("f4_sent") or u.get("funded_at"):
        return False
    local = _local_now()
    if not (cfg.F4_LOCAL_HOUR <= local.hour < cfg.F4_LOCAL_HOUR + 2):
        return False
    entry_date = _local_date_of(u.get("created_at"))
    return entry_date is not None and local.date() > entry_date


def should_send_f5(u, now):
    """Evening day 2 local time (7-9 PM window) — strictly after entry's local calendar day."""
    if u.get("f5_sent") or u.get("funded_at"):
        return False
    local = _local_now()
    if not (cfg.F5_LOCAL_HOUR <= local.hour < cfg.F5_LOCAL_HOUR + 2):
        return False
    entry_date = _local_date_of(u.get("created_at"))
    return entry_date is not None and local.date() > entry_date


def should_send_f6(u, now):
    if u.get("f6_sent") or u.get("funded_at"):
        return False
    days = _days_since(u.get("created_at"), now)
    return days is not None and days >= cfg.F6_AFTER_ENTRY_DAYS


def should_send_f7(u, now):
    if u.get("f7_sent") or u.get("funded_at"):
        return False
    days = _days_since(u.get("created_at"), now)
    return days is not None and days >= cfg.F7_AFTER_ENTRY_DAYS


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Roland v2 — Per-message senders ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _send_v2_and_mark(user_id, msg_type, text, keyboard):
    """Mark sent first to avoid duplicates if send is slow; send second."""
    mark_v2_message_sent(user_id, msg_type)
    return await _send(user_id, text, keyboard)


async def send_stuck_step_1_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "stuck_step_1",
        _fmt(cfg.STUCK_STEP_1, first_name=u.get("first_name") or "Friend"),
        kb_stuck_step_1(),
    )


async def send_stuck_step_2_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "stuck_step_2",
        _fmt(cfg.STUCK_STEP_2, first_name=u.get("first_name") or "Friend"),
        kb_stuck_step_2(),
    )


async def send_stuck_step_3_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "stuck_step_3",
        _fmt(cfg.STUCK_STEP_3, first_name=u.get("first_name") or "Friend"),
        kb_stuck_step_3(),
    )


async def send_m5_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "m5",
        _fmt(cfg.M5_PROOF_NUDGE),
        kb_m5_proof(),
    )


async def send_m6_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "m6",
        _fmt(cfg.M6_SOFT_CHECKIN, first_name=u.get("first_name") or "Friend"),
        kb_m6_checkin(),
    )


async def send_f1_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "f1",
        _fmt(cfg.F1_CHECK, first_name=u.get("first_name") or "Friend"),
        kb_f1(),
    )


async def send_f2_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "f2",
        _fmt(cfg.F2_URGENCY, first_name=u.get("first_name") or "Friend"),
        kb_f2(),
    )


async def send_f3_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "f3",
        _fmt(cfg.F3_RAISING_STAKES, first_name=u.get("first_name") or "Friend"),
        kb_f3(),
    )


async def send_f4_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "f4",
        _fmt(cfg.F4_URGENCY_SPIKE, first_name=u.get("first_name") or "Friend"),
        kb_f4(),
    )


async def send_f5_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "f5",
        _fmt(cfg.F5_HARD_CLOSE, first_name=u.get("first_name") or "Friend"),
        kb_f5(),
    )


async def send_f6_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "f6",
        _fmt(cfg.F6_HELD_SLOT, first_name=u.get("first_name") or "Friend"),
        kb_f6(),
    )


async def send_f7_msg(u):
    return await _send_v2_and_mark(
        int(u["user_id"]), "f7",
        _fmt(cfg.F7_NEW_BATCH, first_name=u.get("first_name") or "Friend"),
        kb_f7(),
    )


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Roland v2 — Unified scheduler tick ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# (predicate, sender) pairs, evaluated in order for each user every tick
V2_RULES = [
    ("stuck_step_1", should_send_stuck_step_1, send_stuck_step_1_msg),
    ("stuck_step_2", should_send_stuck_step_2, send_stuck_step_2_msg),
    ("stuck_step_3", should_send_stuck_step_3, send_stuck_step_3_msg),
    ("m5",           should_send_m5,           send_m5_msg),
    ("m6",           should_send_m6,           send_m6_msg),
    ("f1",           should_send_f1,           send_f1_msg),
    ("f2",           should_send_f2,           send_f2_msg),
    ("f3",           should_send_f3,           send_f3_msg),
    ("f4",           should_send_f4,           send_f4_msg),
    ("f5",           should_send_f5,           send_f5_msg),
    ("f6",           should_send_f6,           send_f6_msg),
    ("f7",           should_send_f7,           send_f7_msg),
]


async def process_v2_scheduled_messages():
    """Single-pass scheduler tick for Roland v2 funnel.

    Funded users still get evaluated (so Stuck-Step-3 can fire), but every
    F1–F7 predicate checks `funded_at` and short-circuits. No active job
    cancellation needed — the kill switch is implicit in the predicates.
    """
    try:
        sent_this_cycle = 0
        # Cap sends per tick so the cycle reliably finishes within its 1-minute
        # interval while sharing the global pacer with live onboarding. At ~5
        # sends/sec, 25 messages clear in a few seconds even with some flood
        # backoff, leaving the rest of the budget for real-time traffic.
        max_per_cycle = 25
        now = _utcnow()
        users_list = list(get_active_v2_users())
        logger.info(f"[v2 sched] evaluating {len(users_list)} active users")

        for u in users_list:
            if sent_this_cycle >= max_per_cycle:
                break
            for name, pred, sender in V2_RULES:
                try:
                    if pred(u, now):
                        if await sender(u):
                            sent_this_cycle += 1
                            await asyncio.sleep(0.2)  # gentle pacing within a user
                        break  # one v2 message per user per tick
                except Exception as e:
                    logger.error(f"[v2 sched] error evaluating {name} for {u.get('user_id')}: {e}")

        if sent_this_cycle:
            logger.info(f"[v2 sched] sent {sent_this_cycle} messages this cycle")
    except Exception as e:
        logger.error(f"[v2 sched] tick failed: {e}")


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Legacy Scheduler Functions ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def process_follow_ups():
    """Process scheduled follow-ups with database timestamp-based duplicate prevention"""
    try:
        total_sent = 0
        max_per_cycle = 3  # Very conservative limit
        
        # Process 30-minute follow-ups (configurable)
        users_30m = list(get_users_for_follow_up("30min", cfg.FOLLOW_UP_30_MINUTES))
        logger.info(f"Found {len(users_30m)} users for 30-minute follow-up")
        
        for user in users_30m[:1]:  # Very strict - only 1 per cycle
            if total_sent >= max_per_cycle:
                break
            user_id = int(user["user_id"])
            
            await send_30min_follow_up(user_id, user["first_name"])
            total_sent += 1
            await asyncio.sleep(5)  # 5 second delay between messages
        
        # Process 1-hour follow-ups (configurable)
        users_1h = list(get_users_for_follow_up("1h", cfg.FOLLOW_UP_1_MINUTES))
        logger.info(f"Found {len(users_1h)} users for 1-hour follow-up")
        
        for user in users_1h[:1]:  # Very strict - only 1 per cycle
            if total_sent >= max_per_cycle:
                break
            user_id = int(user["user_id"])
            
            await send_1hour_follow_up(user_id)
            total_sent += 1
            await asyncio.sleep(5)  # 5 second delay between messages
        
        # Process 3-hour follow-ups (configurable)
        users_3h = list(get_users_for_follow_up("3h", cfg.FOLLOW_UP_3_MINUTES))
        logger.info(f"Found {len(users_3h)} users for 3-hour follow-up")
        
        for user in users_3h[:1]:  # Very strict - only 1 per cycle
            if total_sent >= max_per_cycle:
                break
            user_id = int(user["user_id"])
            
            await send_3hour_follow_up(user_id, user["first_name"])
            total_sent += 1
            await asyncio.sleep(5)  # 5 second delay between messages
            
        logger.info(f"Follow-up cycle completed. Sent {total_sent} messages.")
            
    except Exception as e:
        logger.error(f"Error processing follow-ups: {e}")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Main process ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_chat_join_request(filters.group | filters.channel)
async def approve(_, m: Message):
    """Auto-approve chat join requests and start onboarding flow"""
    op = m.chat
    kk = m.from_user
    logger.info(f"🔍 DEBUG: Join request received from user {kk.id} ({kk.first_name}) in chat {op.id} ({op.title})")
    
    try:
        add_group(m.chat.id)
        logger.info(f"🔍 DEBUG: Added group {op.id} to database")

        # Approve the join request — but tolerate "already handled" races.
        # Telegram returns HIDE_REQUESTER_MISSING when the request was already
        # approved (by another bot instance, a fast user re-tap, manual admin
        # approval, or the user withdrawing the request). The user is still a
        # valid candidate for onboarding, so we log a warning and continue
        # rather than aborting the whole flow.
        try:
            await app.approve_chat_join_request(op.id, kk.id)
            logger.info(f"🔍 DEBUG: Approved join request for user {kk.id}")
        except errors.HideRequesterMissing:
            logger.warning(
                f"Join request for user {kk.id} was already handled "
                f"(HIDE_REQUESTER_MISSING) — continuing with onboarding anyway"
            )

        add_user(kk.id)
        logger.info(f"🔍 DEBUG: Added user {kk.id} to database")
        
        # Start onboarding flow for new users (skip if admin)
        user_id = kk.id
        first_name = kk.first_name or "Friend"
        
        # Skip onboarding for admins
        if user_id in cfg.SUDO:
            logger.info(f"🔍 DEBUG: Skipping onboarding for admin user {user_id}")
            logger.info(f"Approved join request for admin user {kk.id} in chat {op.id}")
            return
        
        logger.info(f"🔍 DEBUG: Starting onboarding process for user {user_id}")
        
        # Always reset onboarding for fresh start (handles rejoin scenarios)
        if already_onboarding(user_id):
            logger.info(f"🔍 DEBUG: User {user_id} has existing onboarding record - resetting for fresh start")
            # Remove existing onboarding record
            reset_onboarding(user_id)
            logger.info(f"🔍 DEBUG: Removed existing onboarding record for user {user_id}")
        
        # Create fresh onboarding record for all users
        logger.info(f"🔍 DEBUG: Creating fresh onboarding record for user {user_id}")
        add_onboarding_user(user_id, first_name)
        logger.info(f"🔍 DEBUG: Added user {user_id} to onboarding database")
        
        # Send welcome + mode picker in the background (Roland v2 flow). We do
        # NOT await it here: under a flood the sends can block for seconds, and
        # awaiting inline holds this dispatcher worker, backing up the join-
        # request queue and freezing new approvals. Fire-and-forget instead so
        # the approval itself completes immediately.
        logger.info(f"🔍 DEBUG: Routing user {user_id} into v2 onboarding flow")
        spawn_onboarding(user_id, first_name, force_welcome=True)

        logger.info(f"🔍 DEBUG: Approval process completed for user {kk.id} in chat {op.id}")
        
    except Exception as err:
        logger.error(f"🔍 DEBUG: Error in approval process: {str(err)}")
        logger.exception("Full exception details:")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ New User Detection ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.private & filters.incoming & ~filters.command(["start", "users", "bcast", "fcast", "test", "menu"]))
async def handle_new_user(_, m: Message):
    """Handle new users and trigger onboarding"""
    user_id = m.from_user.id
    first_name = m.from_user.first_name or "Friend"
    
    # Skip if it's a command (additional safety check)
    if m.text and m.text.startswith('/'):
        logger.info(f"🔍 DEBUG: Skipping command in handle_new_user: {m.text}")
        return
    
    # Skip onboarding for admins
    if user_id in cfg.SUDO:
        logger.info(f"Skipping onboarding for admin user {user_id} in private message")
        return
    
    # Roland v2: route user to wherever they are in the funnel
    if already_onboarding(user_id):
        user_data = get_onboarding_user(user_id)
        if user_data and user_data.get("onboarding_stage") == "welcome_sent":
            # User was auto-approved but welcome wasn't deliverable until now (cold DM).
            await route_v2_entry(user_id, first_name, force_welcome=True)
            logger.info(f"Sent delayed v2 welcome to user {user_id}")
        # Already greeted: stay quiet to avoid spam.
    else:
        # Cold DM, no prior record — kick off v2 onboarding
        await route_v2_entry(user_id, first_name, force_welcome=True)
        logger.info(f"Started v2 onboarding for cold-DM user {user_id}")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Start Command ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.private & filters.command("start"))
async def start_command(_, m: Message):
    """Handle /start command"""
    user_id = m.from_user.id
    first_name = m.from_user.first_name or "Friend"
    
    # Show admin menu
    if user_id in cfg.SUDO:
        menu_text = (
            "🤖 **Admin Control Panel**\n\n"
            "Use the buttons below to manage the bot:"
        )
        await m.reply_text(menu_text, reply_markup=get_admin_menu_keyboard())
        logger.info(f"Admin {user_id} used /start command")
        return
    
    # Check if user is member of required channel
    try:
        print("🔍 DEBUG: User is member of required channel")
        # await send_setup_instructions(user_id, first_name)
        # await app.get_chat_member(cfg.CHID, user_id)
    except:
        try:
            invite_link = await app.create_chat_invite_link(int(cfg.CHID))
        except Exception as e:
            logger.error(f"Error creating invite link: {str(e)}")
            # await m.reply("**Make Sure I Am Admin In Your Channel**")
            await m.reply("**Welcome**")
            return
        
        key = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🍿 Join Update Channel 🍿", url=invite_link.invite_link),
                InlineKeyboardButton("🍀 Check Again 🍀", callback_data="chk")
            ]
        ])
        
        await m.reply_text(
            "**⚠️ Access Denied! ⚠️\n\n"
            "Please Join My Update Channel To Use Me. "
            "If You Joined The Channel Then Click On Check Again Button To Confirm.**",
            reply_markup=key
        )
        return
    
    # Roland v2: route the user based on their current funnel state.
    # First-time /start → M1 + M2; returning users get the next step they're on.
    force_welcome = not already_onboarding(user_id)
    set_unreachable(user_id, False)  # they just messaged us, so we can reach them again
    await route_v2_entry(user_id, first_name, force_welcome=force_welcome)
    update_onboarding_stage(user_id, "start_clicked")
    logger.info(f"User {user_id} routed via /start")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Callback Handlers ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_callback_query(filters.regex("chk"))
async def check_subscription(_, cb: CallbackQuery):
    """Handle check subscription callback"""
    user_id = cb.from_user.id
    first_name = cb.from_user.first_name or "Friend"
    
    logger.info(f"🔍 DEBUG: User {user_id} clicked 'Check Again' button")
    
    try:
        await app.get_chat_member(cfg.CHID, user_id)
        logger.info(f"🔍 DEBUG: User {user_id} is verified as channel member")
    except Exception as e:
        logger.info(f"🔍 DEBUG: User {user_id} is not a channel member: {e}")
        await cb.answer(
            "🙅‍♂️ You are not joined my channel first join channel then check again. 🙅‍♂️",
            show_alert=True
        )
        return
    
    # Skip onboarding for admins
    if user_id in cfg.SUDO:
        await cb.edit_message_text("👋 Welcome back, Admin!")
        logger.info(f"Admin {user_id} verified subscription")
        return
    
    # Roland v2: route based on current funnel state
    force_welcome = not already_onboarding(user_id)
    await route_v2_entry(user_id, first_name, force_welcome=force_welcome)
    update_onboarding_stage(user_id, "verified")
    
    try:
        await cb.edit_message_text("✅ Welcome! Check your messages for setup instructions.")
        logger.info(f"🔍 DEBUG: Successfully edited subscription verification message for user {user_id}")
    except Exception as e:
        logger.error(f"🔍 DEBUG: Error editing subscription verification message for user {user_id}: {e}")
        # Try to answer callback if editing failed
        try:
            await cb.answer("✅ Welcome! Check your private messages for setup instructions.", show_alert=True)
        except:
            pass
    
    logger.info(f"User {user_id} verified subscription")

@app.on_callback_query(filters.regex("setup_yes"))
async def setup_yes_callback(_, cb: CallbackQuery):
    """Handle 'Yes, I have' callback"""
    user_id = cb.from_user.id
    
    try:
        logger.info(f"🔍 DEBUG: User {user_id} clicked 'Yes, I have' button")
        
        response_text = cfg.SETUP_COMPLETED_MSG
        
        # Try to edit the message
        await cb.edit_message_text(response_text)
        logger.info(f"🔍 DEBUG: Successfully edited message for user {user_id}")
        
        # Mark as completed
        mark_setup_completed(user_id, True)
        mark_account_verified(user_id, True)
        update_onboarding_stage(user_id, "completed")
        
        logger.info(f"User {user_id} confirmed setup completion")
        
        # Send confirmation callback answer
        await cb.answer("✅ Great! Setup completed successfully!", show_alert=False)
        
    except Exception as e:
        logger.error(f"🔍 DEBUG: Error in setup_yes_callback for user {user_id}: {e}")
        logger.exception("Full exception details:")
        # Try to answer the callback even if editing failed
        try:
            await cb.answer("❌ Error processing your response. Please try again.", show_alert=True)
        except:
            pass

@app.on_callback_query(filters.regex("setup_no"))
async def setup_no_callback(_, cb: CallbackQuery):
    """Handle 'No, not yet' callback"""
    user_id = cb.from_user.id
    
    try:
        logger.info(f"🔍 DEBUG: User {user_id} clicked 'No, not yet' button")
        
        response_text = cfg.SETUP_REMINDER_MSG.format(
            deposit_guide_link=cfg.DEPOSIT_GUIDE_LINK,
            promo_code=cfg.PROMO_CODE,
            support_username=cfg.SUPPORT_USERNAME
        )
        
        # Try to edit the message
        await cb.edit_message_text(response_text)
        logger.info(f"🔍 DEBUG: Successfully edited message for user {user_id}")
        
        # Update stage but don't mark as completed
        update_onboarding_stage(user_id, "setup_reminder_sent")
        
        logger.info(f"User {user_id} needs setup reminder")
        
        # Send confirmation callback answer
        await cb.answer("📝 No problem! Here's a reminder to help you get started.", show_alert=False)
        
    except Exception as e:
        logger.error(f"🔍 DEBUG: Error in setup_no_callback for user {user_id}: {e}")
        logger.exception("Full exception details:")
        # Try to answer the callback even if editing failed
        try:
            await cb.answer("❌ Error processing your response. Please try again.", show_alert=True)
        except:
            pass

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Roland v2 — Callback Handlers ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_callback_query(filters.regex("^which_one$"))
async def which_one_cb(_, cb: CallbackQuery):
    user_id = cb.from_user.id
    try:
        await cb.answer()
        await send_m3_difference(user_id)
    except Exception as e:
        logger.error(f"which_one_cb error for {user_id}: {e}")


async def _pick_mode(cb: CallbackQuery, mode: str):
    user_id = cb.from_user.id
    first_name = cb.from_user.first_name or "Friend"
    try:
        await cb.answer()
        if not already_onboarding(user_id):
            add_onboarding_user(user_id, first_name)
            add_user(user_id)
        u = get_onboarding_user(user_id) or {}
        # Dedupe: if the user already picked this mode within the last 30s,
        # this is a double-tap or Telegram callback retry — skip re-sending
        # the 4-bridge sequence (which would otherwise duplicate 4a-ii/4b-ii).
        if u.get("chosen_mode") == mode and _within_seconds(u.get("broker_link_tapped_at"), 30):
            logger.info(f"User {user_id} re-tapped {mode} within 30s — dedupe")
            return
        set_chosen_mode(user_id, mode)  # also stamps broker_link_tapped_at
        await send_4_bridge(user_id, first_name, mode)
        logger.info(f"User {user_id} picked mode {mode}")
    except Exception as e:
        logger.error(f"pick_mode {mode} error for {user_id}: {e}")


@app.on_callback_query(filters.regex("^mode_signals$"))
async def mode_signals_cb(_, cb: CallbackQuery):
    await _pick_mode(cb, "SIGNALS")


@app.on_callback_query(filters.regex("^mode_autotrade$"))
async def mode_autotrade_cb(_, cb: CallbackQuery):
    await _pick_mode(cb, "AUTO-TRADE")


@app.on_callback_query(filters.regex("^account_created$"))
async def account_created_cb(_, cb: CallbackQuery):
    user_id = cb.from_user.id
    first_name = cb.from_user.first_name or "Friend"
    try:
        await cb.answer("Nice work — funding step next.")
        if not already_onboarding(user_id):
            # Edge case: user hit the button without entering through normal flow
            add_onboarding_user(user_id, first_name)
            add_user(user_id)
        u = get_onboarding_user(user_id) or {}
        # Dedupe rapid double-taps so 4a-iii / 4b-iii doesn't duplicate.
        if _within_seconds(u.get("account_created_at"), 30):
            logger.info(f"User {user_id} double-tapped ACCOUNT CREATED — dedupe")
            return
        set_account_created(user_id)
        mode = u.get("chosen_mode") or "SIGNALS"  # fallback so flow doesn't dead-end
        await send_4_funding(user_id, first_name, mode)
        logger.info(f"User {user_id} confirmed account created (mode={mode})")
    except Exception as e:
        logger.error(f"account_created_cb error for {user_id}: {e}")


@app.on_callback_query(filters.regex("^funded$"))
async def funded_cb(_, cb: CallbackQuery):
    user_id = cb.from_user.id
    first_name = cb.from_user.first_name or "Friend"
    try:
        await cb.answer("Almost done — DM support next.")
        if not already_onboarding(user_id):
            add_onboarding_user(user_id, first_name)
            add_user(user_id)
        u = get_onboarding_user(user_id) or {}
        # Dedupe rapid double-taps so 4a-iv / 4b-iv doesn't duplicate.
        if _within_seconds(u.get("funded_at"), 30):
            logger.info(f"User {user_id} double-tapped FUNDED — dedupe")
            return
        set_funded(user_id)  # kill switch for F1–F7
        # Also flag legacy fields so old admin stats see the user as completed
        mark_setup_completed(user_id, True)
        mode = u.get("chosen_mode") or "SIGNALS"
        await send_4_support(user_id, mode)
        logger.info(f"User {user_id} confirmed funded (mode={mode}) — F1-F7 killed")
    except Exception as e:
        logger.error(f"funded_cb error for {user_id}: {e}")


@app.on_callback_query(filters.regex("^setting_up$"))
async def setting_up_cb(_, cb: CallbackQuery):
    user_id = cb.from_user.id
    try:
        await cb.answer()
        await _send(user_id, _fmt(cfg.M6_REPLY_SETTING_UP))
    except Exception as e:
        logger.error(f"setting_up_cb error for {user_id}: {e}")


@app.on_callback_query(filters.regex("^do_later$"))
async def do_later_cb(_, cb: CallbackQuery):
    user_id = cb.from_user.id
    try:
        await cb.answer()
        set_do_later(user_id)  # fast-tracks F1 in the scheduler
        await _send(user_id, _fmt(cfg.M6_REPLY_LATER))
        logger.info(f"User {user_id} chose 'do it later' — F1 fast-tracked")
    except Exception as e:
        logger.error(f"do_later_cb error for {user_id}: {e}")


#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Admin Menu Callbacks ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_callback_query(filters.regex("^adm_stats$"))
async def admin_stats_cb(_, cb: CallbackQuery):
    """Show detailed statistics"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    from database import onboarding

    total_users = all_users()
    total_groups = all_groups()

    total_onboarding = onboarding.count_documents({})
    completed = onboarding.count_documents({"setup_completed": True})
    verified = onboarding.count_documents({"account_verified": True})
    pending_welcome = onboarding.count_documents({"onboarding_stage": "welcome_sent"})
    started = onboarding.count_documents({"onboarding_stage": "start_clicked"})
    welcome_sent = onboarding.count_documents({"onboarding_stage": "welcome_actually_sent"})

    got_30min = onboarding.count_documents({"follow_up_30min_sent": True})
    got_1h = onboarding.count_documents({"follow_up_1h_sent": True})
    got_3h = onboarding.count_documents({"follow_up_3h_sent": True})

    completion_rate = (completed / total_onboarding * 100) if total_onboarding > 0 else 0

    stats_text = (
        "📊 **Detailed Bot Statistics**\n\n"
        "━━━ **Overview** ━━━\n"
        f"👤 Total Users in DB: `{total_users}`\n"
        f"👥 Groups/Channels: `{total_groups}`\n\n"
        "━━━ **Onboarding Funnel** ━━━\n"
        f"📥 Total Onboarded: `{total_onboarding}`\n"
        f"⏳ Awaiting Bot Start: `{pending_welcome}`\n"
        f"   ↳ Approved but haven't messaged bot yet\n"
        f"👋 Welcome Sent: `{welcome_sent}`\n"
        f"   ↳ Bot sent welcome message successfully\n"
        f"🚀 Clicked /start: `{started}`\n"
        f"   ↳ Got setup instructions\n"
        f"✅ Setup Completed: `{completed}`\n"
        f"   ↳ Confirmed they completed setup\n"
        f"🎯 Account Verified: `{verified}`\n\n"
        "━━━ **Follow-up Messages** ━━━\n"
        f"📨 30-min reminder: `{got_30min}` users\n"
        f"📨 1-hour reminder: `{got_1h}` users\n"
        f"📨 3-hour reminder: `{got_3h}` users\n\n"
        "━━━ **Conversion Rate** ━━━\n"
        f"📈 Completion: `{completion_rate:.1f}%` ({completed}/{total_onboarding})\n"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="adm_stats")],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
    ])

    await cb.edit_message_text(stats_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_bcast_help$"))
async def admin_bcast_help_cb(_, cb: CallbackQuery):
    """Show broadcast guide"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    help_text = (
        "📢 **Broadcast Guide**\n\n"
        "**Copy Broadcast** (`/bcast`)\n"
        "Reply to any message and use `/bcast` to copy it to all users.\n"
        "• Supports text, photos, videos, documents\n"
        "• Use `[firstname]` in text to personalize\n\n"
        "**Forward Broadcast** (`/fcast`)\n"
        "Reply to a message and use `/fcast` to forward it.\n"
        "• Shows 'Forwarded from' tag\n"
        "• `[firstname]` sends as copy instead\n\n"
        "**Progress Tracking**\n"
        "• Shows total users, progress bar, speed & ETA\n"
        "• Updates live every 25 messages\n"
        "• Auto-removes blocked/deactivated users\n\n"
        "**How to use:**\n"
        "1. Type your message in the chat\n"
        "2. Reply to it with `/bcast`\n"
        "3. Watch the progress update live"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
    ])

    await cb.edit_message_text(help_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_pending$"))
async def admin_pending_cb(_, cb: CallbackQuery):
    """Check pending requests via menu"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    await cb.edit_message_text("🔍 Checking pending requests...")

    try:
        pending_count = 0
        pending_users = []

        channel_id = int(cfg.CHID)
        if channel_id > 0:
            channel_id = -channel_id

        async for request in app.get_chat_join_requests(channel_id):
            pending_count += 1
            pending_users.append(f"• {request.user.first_name or 'Unknown'} (`{request.user.id}`)")
            if pending_count >= 20:
                break

        if pending_count == 0:
            result_text = "✅ **No pending join requests!**\n\nAll requests have been processed."
        else:
            user_list = "\n".join(pending_users[:10])
            if pending_count > 10:
                user_list += f"\n... and {pending_count - 10} more"
            result_text = (
                f"📊 **Pending Join Requests: {pending_count}**\n\n"
                f"👥 **Recent Users:**\n{user_list}\n\n"
                f"💡 Use `/approvepending` to approve all"
            )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="adm_pending")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
        ])

        await cb.edit_message_text(result_text, reply_markup=keyboard)
    except Exception as e:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
        ])
        await cb.edit_message_text(f"❌ Error: {e}", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_config$"))
async def admin_config_cb(_, cb: CallbackQuery):
    """Show bot configuration"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    channel_id = int(cfg.CHID)
    formatted_chid = -channel_id if channel_id > 0 else channel_id

    config_text = (
        "⚙️ **Bot Configuration**\n\n"
        f"🆔 Channel ID: `{formatted_chid}`\n"
        f"🤖 Bot Token: `{cfg.BOT_TOKEN[:10]}...`\n"
        f"🔑 API ID: `{cfg.API_ID}`\n"
        f"👤 Admin IDs: `{cfg.SUDO}`\n\n"
        "💡 Update env variables and redeploy to change settings."
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
    ])

    await cb.edit_message_text(config_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_reset_fu$"))
async def admin_reset_fu_cb(_, cb: CallbackQuery):
    """Reset follow-ups confirmation"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    confirm_text = (
        "🔄 **Reset All Follow-ups?**\n\n"
        "This will stop ALL pending follow-up messages.\n\n"
        "⚠️ New users will still get follow-ups normally.\n\n"
        "Are you sure?"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Reset", callback_data="adm_reset_confirm"),
            InlineKeyboardButton("❌ Cancel", callback_data="adm_menu")
        ]
    ])

    await cb.edit_message_text(confirm_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_reset_confirm$"))
async def admin_reset_confirm_cb(_, cb: CallbackQuery):
    """Execute follow-ups reset"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    from database import onboarding as onboarding_col

    result = onboarding_col.update_many(
        {},
        {"$set": {
            "follow_up_30min_sent": True,
            "follow_up_30min_sent_at": datetime.now(pytz.UTC),
            "follow_up_1h_sent": True,
            "follow_up_1h_sent_at": datetime.now(pytz.UTC),
            "follow_up_3h_sent": True,
            "follow_up_3h_sent_at": datetime.now(pytz.UTC)
        }}
    )

    result_text = (
        "✅ **Follow-up Reset Complete**\n\n"
        f"🛑 Stopped all pending follow-ups\n"
        f"📊 Updated `{result.modified_count}` user records\n\n"
        "New users will still get follow-ups normally."
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
    ])

    await cb.edit_message_text(result_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_cleanup$"))
async def admin_cleanup_cb(_, cb: CallbackQuery):
    """Show cleanup help"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    help_text = (
        "🧹 **Spam Cleanup Guide**\n\n"
        "Use these commands to clean up duplicate messages:\n\n"
        "• `/cleanup_spam all` — Clean all users (last 2h)\n"
        "• `/cleanup_spam <user_id>` — Clean specific user\n"
        "• `/cleanup_spam recent 50` — Clean last 50 users\n\n"
        "This deletes the bot's own spam messages from user chats."
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
    ])

    await cb.edit_message_text(help_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_commands$"))
async def admin_commands_cb(_, cb: CallbackQuery):
    """Show all admin commands"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    commands_text = (
        "📋 **All Admin Commands**\n\n"
        "━━━ **Menu** ━━━\n"
        "• `/start` or `/menu` — Admin control panel\n\n"
        "━━━ **Broadcasting** ━━━\n"
        "• `/bcast` — Copy-broadcast (reply to msg)\n"
        "• `/fcast` — Forward-broadcast (reply to msg)\n\n"
        "━━━ **User Management** ━━━\n"
        "• `/users` — Quick stats\n"
        "• `/checkpending` — Check pending requests\n"
        "• `/approvepending` — Approve all pending\n\n"
        "━━━ **Onboarding** ━━━\n"
        "• `/resetonboarding [id]` — Reset user onboarding\n"
        "• `/reset_followups` — Stop all follow-ups\n\n"
        "━━━ **Maintenance** ━━━\n"
        "• `/cleanup_spam` — Clean bot spam\n"
        "• `/checkconfig` — View config\n"
        "• `/test` — Test permissions\n"
        "• `/testbuttons` — Test callbacks"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="adm_menu")]
    ])

    await cb.edit_message_text(commands_text, reply_markup=keyboard)

@app.on_callback_query(filters.regex("^adm_menu$"))
async def admin_menu_back_cb(_, cb: CallbackQuery):
    """Go back to admin menu"""
    if cb.from_user.id not in cfg.SUDO:
        await cb.answer("⛔ Not authorized", show_alert=True)
        return

    menu_text = (
        "🤖 **Admin Control Panel**\n\n"
        "Use the buttons below to manage the bot:"
    )
    await cb.edit_message_text(menu_text, reply_markup=get_admin_menu_keyboard())

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Admin Commands ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("users") & filters.user(cfg.SUDO))
async def get_stats(_, m: Message):
    """Get bot statistics (admin only)"""
    try:
        from database import onboarding

        total_users = all_users()
        total_groups = all_groups()

        total_onboarding = onboarding.count_documents({})
        completed = onboarding.count_documents({"setup_completed": True})
        verified = onboarding.count_documents({"account_verified": True})

        completion_rate = (completed / total_onboarding * 100) if total_onboarding > 0 else 0

        stats_text = (
            "📊 **Bot Statistics**\n\n"
            f"👤 Users: `{total_users}`\n"
            f"👥 Groups: `{total_groups}`\n"
            f"✅ Setup Completed: `{completed}`\n"
            f"🎯 Verified: `{verified}`\n"
            f"📈 Completion Rate: `{completion_rate:.1f}%`\n\n"
            "💡 Use /menu → Statistics for full breakdown"
        )

        await m.reply_text(text=stats_text)
        logger.info(f"Stats requested by admin {m.from_user.id}")

    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        await m.reply_text("Error getting statistics.")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Broadcast ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("bcast") & filters.user(cfg.SUDO))
async def broadcast(_, m: Message):
    """Broadcast message to all users with concurrent sending"""
    logger.info(f"Broadcast command triggered by user {m.from_user.id}")
    if not m.reply_to_message:
        await m.reply_text("Reply to a message with `/bcast` to broadcast it to all users.\n\n💡 Use `[firstname]` in text to personalize.")
        return

    # ── Guard: never broadcast a command message itself ──
    # If the admin replied to a bot command (e.g. tapped reply on the
    # /bcast command instead of the content), copying it would blast the
    # literal "/bcast" text to every user. Refuse and explain.
    _rt_text = (m.reply_to_message.text or m.reply_to_message.caption or "").strip()
    if _rt_text.startswith("/"):
        await m.reply_text(
            "⚠️ **That's a command, not your broadcast.**\n\n"
            "You replied to a message starting with `/` — sending it would "
            "blast the command text to all users.\n\n"
            "👉 Reply to your actual broadcast *content* message, then send the command."
        )
        return

    # Pre-fetch all user IDs
    all_user_ids = [u["user_id"] for u in users.find()]
    total_count = len(all_user_ids)

    if total_count == 0:
        await m.reply_text("No users in database.")
        return

    # Check if personalization needed and pre-fetch names in bulk
    message_content = m.reply_to_message.text or m.reply_to_message.caption
    needs_personalization = message_content and "[firstname]" in message_content

    name_map = {}
    if needs_personalization:
        from database import onboarding
        for doc in onboarding.find({}, {"user_id": 1, "first_name": 1}):
            name_map[doc["user_id"]] = doc.get("first_name", "Friend")

    lel = await m.reply_text(
        f"📢 **Broadcasting to {total_count} users...**\n\n"
        f"▱▱▱▱▱▱▱▱▱▱ 0%\n"
        f"⚡ Concurrent mode (20 parallel)"
    )

    success = 0
    failed = 0
    deactivated = 0
    blocked = 0
    start_time = time.time()
    send_semaphore = asyncio.Semaphore(20)

    async def send_to_user(userid):
        async with send_semaphore:
            if needs_personalization:
                first_name = name_map.get(str(userid), "Friend")
                personalized_text = message_content.replace("[firstname]", first_name)
                action = lambda: app.send_message(
                    int(userid), personalized_text,
                    reply_markup=m.reply_to_message.reply_markup,
                    disable_web_page_preview=True
                )
            else:
                action = lambda: m.reply_to_message.copy(int(userid))
            status = await broadcast_send_with_retry(action, userid)
            # gentle pacing so 20 concurrent sends don't hammer Telegram
            await asyncio.sleep(0.05)
            return status

    # Process in batches of 50 with concurrent sending
    BATCH_SIZE = 50
    for i in range(0, total_count, BATCH_SIZE):
        batch = all_user_ids[i:i + BATCH_SIZE]
        results = await asyncio.gather(*[send_to_user(uid) for uid in batch])

        for status in results:
            if status == "success":
                success += 1
            elif status == "deactivated":
                deactivated += 1
            elif status == "blocked":
                blocked += 1
            else:
                failed += 1

        # Progress update after each batch
        processed = success + failed + deactivated + blocked
        elapsed = time.time() - start_time
        speed = processed / elapsed if elapsed > 0 else 0
        remaining = total_count - processed
        eta = remaining / speed if speed > 0 else 0
        pct = min(processed / total_count * 100, 100) if total_count > 0 else 100
        filled = int(pct / 10)
        bar = "▰" * filled + "▱" * (10 - filled)
        try:
            await lel.edit(
                f"📢 **Broadcasting...**\n\n"
                f"{bar} {pct:.0f}%\n"
                f"✅ Sent: `{success}` | ❌ Failed: `{failed + blocked + deactivated}`\n"
                f"📨 {processed}/{total_count} processed\n"
                f"⚡ Speed: `{speed:.1f}` msg/sec\n"
                f"⏱ ETA: `{int(eta)}s` remaining"
            )
        except Exception:
            pass

    elapsed = time.time() - start_time
    speed = (success + failed + deactivated + blocked) / elapsed if elapsed > 0 else 0

    result_text = (
        f"✅ **Broadcast Complete!**\n\n"
        f"━━━ **Results** ━━━\n"
        f"📨 Sent successfully: `{success}`\n"
        f"❌ Failed (not started bot): `{failed}`\n"
        f"🚫 Blocked the bot: `{blocked}`\n"
        f"👻 Deactivated accounts: `{deactivated}`\n\n"
        f"━━━ **Performance** ━━━\n"
        f"⏱ Time taken: `{int(elapsed)}s`\n"
        f"⚡ Speed: `{speed:.1f}` msg/sec\n\n"
        f"💡 Blocked & deactivated users were removed from DB."
    )

    await lel.edit(result_text)
    logger.info(f"Broadcast completed by admin {m.from_user.id}: {success} successful, {failed} failed")

#━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ Broadcast Forward ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("fcast") & filters.user(cfg.SUDO))
async def forward_broadcast(_, m: Message):
    """Forward message to all users with concurrent sending"""
    logger.info(f"Forward broadcast command triggered by user {m.from_user.id}")
    if not m.reply_to_message:
        await m.reply_text("Reply to a message with `/fcast` to forward it to all users.\n\n💡 Use `[firstname]` in text to personalize.")
        return

    # ── Guard: never broadcast a command message itself ──
    # If the admin replied to a bot command (e.g. tapped reply on the
    # /bcast command instead of the content), copying it would blast the
    # literal "/bcast" text to every user. Refuse and explain.
    _rt_text = (m.reply_to_message.text or m.reply_to_message.caption or "").strip()
    if _rt_text.startswith("/"):
        await m.reply_text(
            "⚠️ **That's a command, not your broadcast.**\n\n"
            "You replied to a message starting with `/` — sending it would "
            "blast the command text to all users.\n\n"
            "👉 Reply to your actual broadcast *content* message, then send the command."
        )
        return

    # Pre-fetch all user IDs
    all_user_ids = [u["user_id"] for u in users.find()]
    total_count = len(all_user_ids)

    if total_count == 0:
        await m.reply_text("No users in database.")
        return

    # Check if personalization needed and pre-fetch names in bulk
    message_content = m.reply_to_message.text or m.reply_to_message.caption
    needs_personalization = message_content and "[firstname]" in message_content

    name_map = {}
    if needs_personalization:
        from database import onboarding
        for doc in onboarding.find({}, {"user_id": 1, "first_name": 1}):
            name_map[doc["user_id"]] = doc.get("first_name", "Friend")

    lel = await m.reply_text(
        f"📢 **Forwarding to {total_count} users...**\n\n"
        f"▱▱▱▱▱▱▱▱▱▱ 0%\n"
        f"⚡ Concurrent mode (20 parallel)"
    )

    success = 0
    failed = 0
    deactivated = 0
    blocked = 0
    start_time = time.time()
    send_semaphore = asyncio.Semaphore(20)

    async def send_to_user(userid):
        async with send_semaphore:
            if needs_personalization:
                first_name = name_map.get(str(userid), "Friend")
                personalized_text = message_content.replace("[firstname]", first_name)
                action = lambda: app.send_message(
                    int(userid), personalized_text,
                    reply_markup=m.reply_to_message.reply_markup,
                    disable_web_page_preview=True
                )
            else:
                action = lambda: m.reply_to_message.forward(int(userid))
            status = await broadcast_send_with_retry(action, userid)
            # gentle pacing so 20 concurrent sends don't hammer Telegram
            await asyncio.sleep(0.05)
            return status

    # Process in batches of 50 with concurrent sending
    BATCH_SIZE = 50
    for i in range(0, total_count, BATCH_SIZE):
        batch = all_user_ids[i:i + BATCH_SIZE]
        results = await asyncio.gather(*[send_to_user(uid) for uid in batch])

        for status in results:
            if status == "success":
                success += 1
            elif status == "deactivated":
                deactivated += 1
            elif status == "blocked":
                blocked += 1
            else:
                failed += 1

        # Progress update after each batch
        processed = success + failed + deactivated + blocked
        elapsed = time.time() - start_time
        speed = processed / elapsed if elapsed > 0 else 0
        remaining = total_count - processed
        eta = remaining / speed if speed > 0 else 0
        pct = min(processed / total_count * 100, 100) if total_count > 0 else 100
        filled = int(pct / 10)
        bar = "▰" * filled + "▱" * (10 - filled)
        try:
            await lel.edit(
                f"📢 **Forwarding...**\n\n"
                f"{bar} {pct:.0f}%\n"
                f"✅ Sent: `{success}` | ❌ Failed: `{failed + blocked + deactivated}`\n"
                f"📨 {processed}/{total_count} processed\n"
                f"⚡ Speed: `{speed:.1f}` msg/sec\n"
                f"⏱ ETA: `{int(eta)}s` remaining"
            )
        except Exception:
            pass

    elapsed = time.time() - start_time
    speed = (success + failed + deactivated + blocked) / elapsed if elapsed > 0 else 0

    result_text = (
        f"✅ **Forward Broadcast Complete!**\n\n"
        f"━━━ **Results** ━━━\n"
        f"📨 Sent successfully: `{success}`\n"
        f"❌ Failed (not started bot): `{failed}`\n"
        f"🚫 Blocked the bot: `{blocked}`\n"
        f"👻 Deactivated accounts: `{deactivated}`\n\n"
        f"━━━ **Performance** ━━━\n"
        f"⏱ Time taken: `{int(elapsed)}s`\n"
        f"⚡ Speed: `{speed:.1f}` msg/sec\n\n"
        f"💡 Blocked & deactivated users were removed from DB."
    )

    await lel.edit(result_text)
    logger.info(f"Forward broadcast completed by admin {m.from_user.id}: {success} successful, {failed} failed")

@app.on_message(filters.command("cleanup_spam") & filters.user(cfg.SUDO))
async def cleanup_spam_command(_, m: Message):
    """Admin command to cleanup spam messages from recent follow-ups"""
    logger.info(f"Cleanup spam command triggered by admin {m.from_user.id}")
    
    try:
        # Parse command arguments
        args = m.text.split()
        
        if len(args) < 2:
            help_text = """
🧹 **Spam Cleanup Command**

**Usage:**
• `/cleanup_spam all` - Clean spam for all users (last 2 hours)
• `/cleanup_spam <user_id>` - Clean spam for specific user
• `/cleanup_spam recent 50` - Clean spam for last 50 users with follow-ups

**Examples:**
• `/cleanup_spam all`
• `/cleanup_spam 123456789`
• `/cleanup_spam recent 20`
            """
            await m.reply_text(help_text)
            return
        
        status_msg = await m.reply_text("🔄 Starting spam cleanup...")
        
        cleaned_users = 0
        deleted_messages = 0
        failed_users = 0
        
        if args[1] == "all":
            # Clean spam for all users who received follow-ups in last 2 hours
            from database import onboarding
            from datetime import datetime, timedelta
            import pytz
            
            utc = pytz.UTC
            two_hours_ago = datetime.now(utc) - timedelta(hours=2)
            
            # Find users who got follow-ups recently
            recent_users = onboarding.find({
                "$or": [
                    {"follow_up_30min_sent": True},
                    {"follow_up_1h_sent": True}, 
                    {"follow_up_3h_sent": True}
                ],
                "created_at": {"$gte": two_hours_ago}
            }).limit(100)  # Limit to prevent overload
            
            for user_data in recent_users:
                user_id = int(user_data["user_id"])
                deleted = await cleanup_user_spam(user_id)
                if deleted > 0:
                    cleaned_users += 1
                    deleted_messages += deleted
                elif deleted == -1:
                    failed_users += 1
                
                # Update status every 10 users
                if cleaned_users % 10 == 0:
                    await status_msg.edit_text(f"🔄 Cleaning... {cleaned_users} users processed")
                    
                await asyncio.sleep(0.5)  # Rate limiting
        
        elif args[1] == "recent":
            # Clean spam for recent N users
            limit = int(args[2]) if len(args) > 2 else 20
            
            from database import onboarding
            from datetime import datetime, timedelta
            import pytz
            
            utc = pytz.UTC
            recent_users = onboarding.find({}).sort("created_at", -1).limit(limit)
            
            for user_data in recent_users:
                user_id = int(user_data["user_id"])
                deleted = await cleanup_user_spam(user_id)
                if deleted > 0:
                    cleaned_users += 1
                    deleted_messages += deleted
                elif deleted == -1:
                    failed_users += 1
                    
                await asyncio.sleep(0.5)  # Rate limiting
        
        else:
            # Clean spam for specific user ID
            try:
                user_id = int(args[1])
                deleted = await cleanup_user_spam(user_id)
                if deleted > 0:
                    cleaned_users = 1
                    deleted_messages = deleted
                elif deleted == -1:
                    failed_users = 1
            except ValueError:
                await status_msg.edit_text("❌ Invalid user ID. Please provide a valid number.")
                return
        
        # Final results
        result_text = f"""
✅ **Spam Cleanup Complete**

👥 Users processed: `{cleaned_users}`
🗑️ Messages deleted: `{deleted_messages}`
❌ Failed users: `{failed_users}`

Cleanup completed successfully!
        """
        
        await status_msg.edit_text(result_text)
        logger.info(f"Spam cleanup completed by admin {m.from_user.id}: {cleaned_users} users, {deleted_messages} messages deleted")
        
    except Exception as e:
        logger.error(f"Error in cleanup spam command: {e}")
        await m.reply_text(f"❌ Error during cleanup: {str(e)}")

@app.on_message(filters.command("reset_followups") & filters.user(cfg.SUDO))
async def reset_followups_command(_, m: Message):
    """Admin command to reset follow-up timestamps to stop spam"""
    logger.info(f"Reset follow-ups command triggered by admin {m.from_user.id}")
    
    try:
        from database import onboarding
        from datetime import datetime
        import pytz
        
        # Reset all follow-up flags and timestamps to stop ongoing spam
        result = onboarding.update_many(
            {},  # All users
            {"$set": {
                "follow_up_30min_sent": True,
                "follow_up_30min_sent_at": datetime.now(pytz.UTC),
                "follow_up_1h_sent": True, 
                "follow_up_1h_sent_at": datetime.now(pytz.UTC),
                "follow_up_3h_sent": True,
                "follow_up_3h_sent_at": datetime.now(pytz.UTC)
            }}
        )
        
        response_text = f"""
✅ **Follow-up Reset Complete**

🛑 Stopped all ongoing follow-ups
📊 Updated {result.modified_count} user records
⏰ All follow-ups marked as sent with current timestamp

This will prevent any duplicate or spam follow-ups from being sent.
New users will still get proper follow-ups based on their join time.
        """
        
        await m.reply_text(response_text)
        logger.info(f"Reset follow-ups completed by admin {m.from_user.id}: {result.modified_count} users updated")
        
    except Exception as e:
        logger.error(f"Error in reset follow-ups command: {e}")
        await m.reply_text(f"❌ Error during reset: {str(e)}")

async def cleanup_user_spam(user_id: int, hours_back: int = 2) -> int:
    """
    Clean up spam messages for a specific user
    Returns: number of messages deleted, or -1 if failed
    """
    try:
        deleted_count = 0
        spam_keywords = [
            "follow-up", "LENTROM", "Lentrom", "checking in", 
            "don't skip this", "bootcamp", "Polycarp", 
            "funded account", "IQ Broker", "@policeesupport"
        ]
        
        # Get recent messages from the chat (limit to last 50 messages)
        async for message in app.get_chat_history(user_id, limit=50):
            # Only process bot's own messages
            if message.from_user and message.from_user.is_self:
                # Check if message was sent in the last X hours
                from datetime import datetime, timedelta
                import pytz
                
                utc = pytz.UTC
                cutoff_time = datetime.now(utc) - timedelta(hours=hours_back)
                
                if message.date and message.date > cutoff_time:
                    # Check if it contains spam keywords
                    if message.text and any(keyword.lower() in message.text.lower() for keyword in spam_keywords):
                        try:
                            await app.delete_messages(user_id, message.id)
                            deleted_count += 1
                            logger.info(f"Deleted spam message {message.id} for user {user_id}")
                            await asyncio.sleep(0.2)  # Small delay between deletions
                        except Exception as delete_error:
                            logger.warning(f"Failed to delete message {message.id} for user {user_id}: {delete_error}")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error cleaning up spam for user {user_id}: {e}")
        return -1

# Start the bot
# Admin statistics module (/stats) — registered in group -1, read-only
from admin_stats import register_stats_handlers
register_stats_handlers(app)

if __name__ == "__main__":
    logger.info(f"Starting {cfg.BOT_NAME}...")
    print(f"🤖 {cfg.BOT_NAME} is starting...")
    
    try:
        # Debug environment variables
        logger.info(f"MONGO_URI set: {'Yes' if cfg.MONGO_URI else 'No'}")
        logger.info(f"DB_NAME: {cfg.DB_NAME}")
        
        # Test database connection
        from database import client, migrate_existing_users
        client.admin.command('ping')
        logger.info("✅ Database connection successful")
        
        # Migrate existing users to add timestamp fields (legacy)
        migrated_count = migrate_existing_users()
        if migrated_count > 0:
            logger.info(f"✅ Migrated {migrated_count} existing users to legacy timestamp format")
        else:
            logger.info("✅ All users already have legacy timestamp fields")

        # Roland v2 schema migration
        v2_migrated = migrate_to_v2_schema()
        if v2_migrated > 0:
            logger.info(f"✅ Backfilled v2 fields on {v2_migrated} onboarding documents")
        else:
            logger.info("✅ All onboarding records already on v2 schema")

        # Legacy follow-up processor (kept so users still in the old flow still finish)
        scheduler.add_job(
            process_follow_ups,
            IntervalTrigger(minutes=10),
            id='follow_up_processor',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120
        )

        # Roland v2 unified scheduler — 1-minute interval
        scheduler.add_job(
            process_v2_scheduled_messages,
            IntervalTrigger(minutes=1),
            id='v2_scheduled_messages',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=30
        )
        scheduler.start()
        logger.info("Scheduler started (legacy + v2 jobs)")

        # Schedule deployment notification (10s delay so bot is fully connected)
        async def notify_admins_on_deploy():
            """Send deployment notification to all admins"""
            for admin_id in cfg.SUDO:
                try:
                    total_users_count = all_users()
                    deploy_time = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
                    await app.send_message(
                        admin_id,
                        f"🟢 **{cfg.BOT_NAME} Deployed!**\n\n"
                        f"✅ Bot is live and running\n"
                        f"📊 Database connected — `{total_users_count}` users\n"
                        f"⏰ {deploy_time} UTC\n\n"
                        f"Use /menu to open the admin panel.",
                        disable_web_page_preview=True
                    )
                    logger.info(f"Deploy notification sent to admin {admin_id}")
                except Exception as e:
                    logger.warning(f"Could not notify admin {admin_id}: {e}")

        scheduler.add_job(
            notify_admins_on_deploy,
            'date',
            run_date=datetime.now() + timedelta(seconds=10),
            id='deploy_notification',
            replace_existing=True
        )

        # Start bot
        app.run()
    except Exception as e:
        logger.error(f"Error starting bot: {str(e)}")
        logger.exception("Full exception details:")
        print(f"❌ Error starting bot: {str(e)}")
    finally:
        scheduler.shutdown()
        logger.info("Bot stopped.")
        print("🛑 Bot stopped.")
