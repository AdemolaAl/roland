import os
import json
from typing import List
from pathlib import Path

class Config:
    def __init__(self):
        # Load JSON configuration first
        self._load_json_config()
        
        # Load environment variables (required)
        self._load_env_config()
    
    def _load_json_config(self):
        """Load configuration from JSON file"""
        config_file = Path("config.json")
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    self.json_config = json.load(f)
                print("✅ JSON configuration loaded successfully")
            except Exception as e:
                print(f"⚠️ Error loading JSON config: {e}")
                self.json_config = {}
        else:
            print("⚠️ No config.json found, using environment variables and defaults")
            self.json_config = {}
    
    def _load_env_config(self):
        """Load environment variables"""
        # Required environment variables
        self.API_ID: int = int(os.getenv("API_ID", "0"))
        self.API_HASH: str = os.getenv("API_HASH", "")
        self.BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
        
        # Force Subscribe Channel ID
        self.CHID: int = int(os.getenv("CHID", "0"))
        
        # Admin/Owner User IDs (can be comma-separated)
        self.SUDO: List[int] = []
        if os.getenv("SUDO"):
            self.SUDO = [int(x.strip()) for x in os.getenv("SUDO").split(",")]
        
        # Database Configuration
        self.MONGO_URI: str = os.getenv("MONGO_URI", "")
        self.DB_NAME: str = os.getenv("DB_NAME", "main")
        
        # Bot Owner Configuration
        self.BOT_OWNER: str = os.getenv("BOT_OWNER", "graceboy")
        
        # Load configuration from JSON using bot owner namespace
        owner_config = self.json_config.get(self.BOT_OWNER, {})
        bot_config = owner_config.get("bot_config", {})
        timing_config = owner_config.get("timing", {})
        messages_config = owner_config.get("messages", {})
        
        if not owner_config:
            print(f"⚠️  Warning: No configuration found for BOT_OWNER='{self.BOT_OWNER}'")
            print(f"Available owners: {list(self.json_config.keys())}")
            print("Using fallback configuration...")
        
        # Bot Configuration
        self.BOT_NAME: str = bot_config.get("bot_name", os.getenv("BOT_NAME", "Graceboy Trading Bot"))
        
        # Optional URLs
        self.CHANNEL_URL: str = bot_config.get("channel_url", os.getenv("CHANNEL_URL", "https://t.me/xxxx"))
        self.SUPPORT_URL: str = bot_config.get("support_url", os.getenv("SUPPORT_URL", "https://t.me/xxxx"))
        
        # Trading Links
        self.DEPOSIT_GUIDE_LINK: str = bot_config.get("deposit_guide_link", os.getenv("DEPOSIT_GUIDE_LINK", "https://t.me/graceboydeposit/3"))
        self.GUIDE_CHANNEL_LINK: str = bot_config.get("guide_channel_link", self.DEPOSIT_GUIDE_LINK)
        self.RESULTS_CHANNEL_LINK: str = bot_config.get("results_channel_link", os.getenv("RESULTS_CHANNEL_LINK", "https://t.me/Graceboytrading"))
        self.WINS_CHANNEL_LINK: str = bot_config.get("wins_channel_link", self.RESULTS_CHANNEL_LINK)
        self.SUPPORT_USERNAME: str = bot_config.get("support_username", os.getenv("SUPPORT_USERNAME", "graceboysupport"))
        self.DIRECT_CONTACT_USERNAME: str = bot_config.get("direct_contact_username", os.getenv("DIRECT_CONTACT_USERNAME", "graceboylive"))
        self.PROMO_CODE: str = bot_config.get("promo_code", os.getenv("PROMO_CODE", "GRACEBOY100"))
        self.PROMO_TOKEN: str = bot_config.get("promo_token", os.getenv("PROMO_TOKEN", "89V8001"))

        # Roland v2 — product paths
        self.SIGNALS_BROKER_URL: str = bot_config.get("signals_broker_url", os.getenv("SIGNALS_BROKER_URL", ""))
        self.AUTOTRADE_BROKER_URL: str = bot_config.get("autotrade_broker_url", os.getenv("AUTOTRADE_BROKER_URL", ""))
        self.USER_TIMEZONE: str = bot_config.get("user_timezone", os.getenv("USER_TIMEZONE", "Africa/Lagos"))
        self.F4_LOCAL_HOUR: int = int(bot_config.get("f4_local_hour", os.getenv("F4_LOCAL_HOUR", "9")))
        self.F5_LOCAL_HOUR: int = int(bot_config.get("f5_local_hour", os.getenv("F5_LOCAL_HOUR", "20")))

        # Image attachments — relative paths from the bot's working directory
        self.WELCOME_IMAGE: str = bot_config.get("welcome_image", "")
        self.M2_IMAGE: str = bot_config.get("m2_image", "")
        self.M3_IMAGE: str = bot_config.get("m3_image", "")
        self.M4_CREATE_ACCOUNT_IMAGE: str = bot_config.get("m4_create_account_image", "")
        self.M4_FUND_ACCOUNT_IMAGE: str = bot_config.get("m4_fund_account_image", "")

        # Timing Configuration
        self.FOLLOW_UP_30_MINUTES: int = timing_config.get("follow_up_30_minutes", int(os.getenv("FOLLOW_UP_30_MINUTES", "30")))
        self.FOLLOW_UP_1_MINUTES: int = timing_config.get("follow_up_1_minutes", int(os.getenv("FOLLOW_UP_1_MINUTES", "60")))
        self.FOLLOW_UP_3_MINUTES: int = timing_config.get("follow_up_3_minutes", int(os.getenv("FOLLOW_UP_3_MINUTES", "180")))

        # Roland v2 — flow timing (minutes / days)
        self.STUCK_STEP_MINUTES: int = timing_config.get("stuck_step_minutes", 15)
        self.M5_AFTER_BROKER_MINUTES: int = timing_config.get("m5_after_broker_minutes", 60)
        self.M6_AFTER_M5_MINUTES: int = timing_config.get("m6_after_m5_minutes", 120)
        self.F1_AFTER_M6_MINUTES: int = timing_config.get("f1_after_m6_minutes", 120)
        self.F2_AFTER_F1_MINUTES: int = timing_config.get("f2_after_f1_minutes", 240)
        self.F3_AFTER_F2_MINUTES: int = timing_config.get("f3_after_f2_minutes", 480)
        self.F6_AFTER_ENTRY_DAYS: int = timing_config.get("f6_after_entry_days", 3)
        self.F7_AFTER_ENTRY_DAYS: int = timing_config.get("f7_after_entry_days", 10)
        
        # Custom Messages
        self.WELCOME_MESSAGE: str = messages_config.get("welcome_message", os.getenv("WELCOME_MESSAGE", """Welcome — I'm Polycarp.

Over 15,000 traders are already inside my private community on Lentrom — copying my trades, using my signals, and growing daily.

In April 2025, I made over $300,000 live in this exact system.

And today, I'm giving you the chance to join — for free.

✅ Copy my trades automatically
✅ Use my daily signals — even if you've never traded before
✅ Access my personal bot + mentorship dashboard

🎓 Normally $899 — but you're getting access free today."""))
        
        self.IMMEDIATE_FOLLOW_UP: str = messages_config.get("immediate_follow_up", os.getenv("IMMEDIATE_FOLLOW_UP", """Just click 👉 /start and I'll send your registration link to unlock the dashboard.

Access closes shortly — don't miss this window."""))
        
        self.SETUP_INSTRUCTIONS: str = messages_config.get("setup_instructions", os.getenv("SETUP_INSTRUCTIONS", """🔥 Congrats, {first_name}!

You're seconds away from joining the Lentrom bootcamp — currently LIVE.

This is where thousands are copying my trades daily.

🟩 Register now using the link below:
🌐 Lentrom.com

(It takes less than 2 minutes.)

🎁 You'll get 4 months of premium access once you're in — but only if you register before this window closes.

⏳ Only a few free spots left for today.

Once the system closes, no new accounts will be accepted."""))
        
        self.SUPPORT_MESSAGE: str = messages_config.get("support_message", os.getenv("SUPPORT_MESSAGE", """📩 After registering, send your name and email to @policeesupport to unlock your dashboard.

Let's build results — not excuses 👊"""))
        
        self.FOLLOW_UP_30MIN: str = messages_config.get("follow_up_30min", os.getenv("FOLLOW_UP_30MIN", """Hi {first_name}, don't skip this!

🎯 The LENTROM bootcamp is 100% FREE — but strictly for those who are serious.

Access shuts down in a few hours!

To qualify:
✅ Create your trading account
✅ Fund it with $30 or more (this filters out the time-wasters)

👉 Use this exact link 👇
https://iqbroker.com/lp/regframe-01-light-nosocials/en/?aff=743768&aff_model=revenue&afftrack=policee

Once you've funded, send a DM to @policeesupport to unlock access to our private community — where you'll learn, copy real trades, and grow faster.

⚠️ No funded account = No access.

This is your shot at something real. Don't miss it."""))
        
        self.FOLLOW_UP_1H: str = messages_config.get("follow_up_1h", os.getenv("FOLLOW_UP_1H", """🕐 First Follow-Up

Hey 👋

Just checking in — have you created your account and sent proof to support yet?"""))
        
        self.FOLLOW_UP_3H: str = messages_config.get("follow_up_3h", os.getenv("FOLLOW_UP_3H", """🕒 Final Follow-Up

Hey again, {first_name}!

Just checking one last time.

If you got stuck or have questions, reach out to me directly here @{direct_contact_username}

Let's not waste time — this bot is literally printing results every day.

You either plug in or watch others eat.

You need the Step by step guide to setting up your account? Here 👇

{deposit_guide_link}"""))
        
        self.SETUP_COMPLETED_MSG: str = messages_config.get("setup_completed_msg", os.getenv("SETUP_COMPLETED_MSG", """Perfect.

You're officially inside. Stay locked in — signals are already flowing, and we're entering new trades soon.

You're good now.

Just follow what the bot tells you."""))
        
        self.SETUP_REMINDER_MSG: str = messages_config.get("setup_reminder_msg", os.getenv("SETUP_REMINDER_MSG", """Let's wrap it up right now ✅

Here's the Step by Step setup guide to creating your trading account to get you ready👇

👉 {deposit_guide_link}

Don't forget the promo code: {promo_code} for up to 150% bonus.

After funding, send proof to @{support_username} so we activate the bot for you."""))

        # Roland v2 message templates — pulled from messages_config with empty fallback
        self.M2_REVEAL: str = messages_config.get("m2_reveal", "")
        self.M2_FOLLOWUP: str = messages_config.get("m2_followup", "")
        self.M3_DIFFERENCE: str = messages_config.get("m3_difference", "")
        self.M4A_I_BRIDGE: str = messages_config.get("m4a_i_bridge", "")
        self.M4A_II_CONFIRM: str = messages_config.get("m4a_ii_confirm", "")
        self.M4A_III_FUNDING: str = messages_config.get("m4a_iii_funding", "")
        self.M4A_IV_SUPPORT: str = messages_config.get("m4a_iv_support", "")
        self.M4A_V_NEXT: str = messages_config.get("m4a_v_next", "")
        self.M4B_I_BRIDGE: str = messages_config.get("m4b_i_bridge", "")
        self.M4B_II_CONFIRM: str = messages_config.get("m4b_ii_confirm", "")
        self.M4B_III_FUNDING: str = messages_config.get("m4b_iii_funding", "")
        self.M4B_IV_SUPPORT: str = messages_config.get("m4b_iv_support", "")
        self.M4B_V_NEXT: str = messages_config.get("m4b_v_next", "")
        self.M5_PROOF_NUDGE: str = messages_config.get("m5_proof_nudge", "")
        self.M6_SOFT_CHECKIN: str = messages_config.get("m6_soft_checkin", "")
        self.M6_REPLY_SETTING_UP: str = messages_config.get("m6_reply_setting_up", "")
        self.M6_REPLY_LATER: str = messages_config.get("m6_reply_later", "")
        self.STUCK_STEP_1: str = messages_config.get("stuck_step_1", "")
        self.STUCK_STEP_2: str = messages_config.get("stuck_step_2", "")
        self.STUCK_STEP_3: str = messages_config.get("stuck_step_3", "")
        self.F1_CHECK: str = messages_config.get("f1_check", "")
        self.F2_URGENCY: str = messages_config.get("f2_urgency", "")
        self.F3_RAISING_STAKES: str = messages_config.get("f3_raising_stakes", "")
        self.F4_URGENCY_SPIKE: str = messages_config.get("f4_urgency_spike", "")
        self.F5_HARD_CLOSE: str = messages_config.get("f5_hard_close", "")
        self.F6_HELD_SLOT: str = messages_config.get("f6_held_slot", "")
        self.F7_NEW_BATCH: str = messages_config.get("f7_new_batch", "")
        self.RETURNING_FUNDED: str = messages_config.get("returning_funded", "")
        self.RETURNING_IN_FLOW_NO_MODE: str = messages_config.get("returning_in_flow_no_mode", "")
        self.RETURNING_SUPPORTED: str = messages_config.get("returning_supported", "")

    def validate(self):
        """Validate that all required environment variables are set"""
        errors = []
        
        if not self.API_ID or self.API_ID == 0:
            errors.append("API_ID is required")
        
        if not self.API_HASH:
            errors.append("API_HASH is required")
        
        if not self.BOT_TOKEN:
            errors.append("BOT_TOKEN is required")
        
        if not self.CHID or self.CHID == 0:
            errors.append("CHID is required")
        
        if not self.SUDO:
            errors.append("SUDO is required")
        
        if not self.MONGO_URI:
            errors.append("MONGO_URI is required")
        
        if errors:
            raise ValueError(f"Missing required environment variables: {', '.join(errors)}")
        
        return True

# Create and validate configuration
cfg = Config()
cfg.validate()


