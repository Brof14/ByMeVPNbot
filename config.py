"""
Configuration Module for ByMeVPN Bot

This module loads all configuration from environment variables using python-dotenv.
All sensitive credentials should be stored in the .env file (not committed to git).

Environment Variables (.env file):
    BOT_TOKEN: Telegram bot token from BotFather
    SUPPORT_USERNAME: Support bot username (default: @ByMeVPN_support_bot)
    MENU_PHOTO: URL or file ID for menu photo
    DB_FILE: Database file path (default: vpnbot.db)
    ADMIN_IDS: Comma-separated list of admin Telegram user IDs
    XUI_HOST: 3x-UI panel host/domain
    XUI_USERNAME: 3x-UI panel username
    XUI_PASSWORD: 3x-UI panel password
    INBOUND_ID: 3x-UI inbound ID for VPN (default: 5)
    REALITY_HOST: Reality server host/domain
    REALITY_PORT: Reality server port (default: 443)
    REALITY_SNI: Reality SNI (default: www.microsoft.com)
    REALITY_FP: Reality fingerprint (default: chrome)
    REALITY_PBK: Reality public key
    REALITY_SID: Reality short ID
    YOOKASSA_SHOP_ID: YooKassa payment shop ID
    YOOKASSA_SECRET_KEY: YooKassa payment secret key
    SMTP_HOST: SMTP server host for email auth
    SMTP_PORT: SMTP server port (default: 465)
    SMTP_USER: SMTP username
    SMTP_PASSWORD: SMTP password
    WEBHOOK_HOST: Webhook server host (default: 0.0.0.0)
    WEBHOOK_PORT: Webhook server port (default: 8080)
    REF_BONUS_DAYS: Referral bonus days (default: 3)
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================================
# Telegram Bot Configuration
# ============================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@ByMeVPN_support_bot")
MENU_PHOTO = os.getenv("MENU_PHOTO", "")
DB_FILE = os.getenv("DB_FILE", "vpnbot.db")

# Admin configuration - supports multiple admins via comma-separated list
ADMIN_IDS = [
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", os.getenv("ADMIN_ID", "0")).split(",")
    if x.strip()
]
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0  # Legacy single admin support

# ============================================================================
# 3x-UI Panel Configuration (VPN Management)
# ============================================================================
XUI_HOST = os.getenv("XUI_HOST", "")
XUI_USERNAME = os.getenv("XUI_USERNAME", "")
XUI_PASSWORD = os.getenv("XUI_PASSWORD", "")
INBOUND_ID = int(os.getenv("INBOUND_ID", "5"))

# ============================================================================
# Reality Protocol Configuration
# ============================================================================
REALITY_HOST = os.getenv("REALITY_HOST", "")
REALITY_PORT = int(os.getenv("REALITY_PORT", "443"))
REALITY_SNI = os.getenv("REALITY_SNI", "www.microsoft.com")
REALITY_FP = os.getenv("REALITY_FP", "chrome")
REALITY_PBK = os.getenv("REALITY_PBK", "")
REALITY_SID = os.getenv("REALITY_SID", "")

# ============================================================================
# YooKassa Payment Configuration
# ============================================================================
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID", "")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY", "")

# ============================================================================
# Email Authentication Configuration
# ============================================================================
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")

# ============================================================================
# Webhook Server Configuration
# ============================================================================
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") or "0.0.0.0"
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT") or "8080")

# ============================================================================
# Trial and Referral Configuration
# ============================================================================
TRIAL_DAYS = 3
TRIAL_PRICE = 0
REF_BONUS_DAYS = int(os.getenv("REF_BONUS_DAYS", "3"))

# ============================================================================
# Pricing Configuration (Legacy - see constants.py for current prices)
# ============================================================================
PRICE_1_MONTH = 99
PRICE_3_MONTHS = 237
PRICE_6_MONTHS = 414
PRICE_12_MONTHS = 708

DAYS_1M = 30
DAYS_3M = 120
DAYS_6M = 240
DAYS_12M = 450

# ============================================================================
# Logging
# ============================================================================
import logging as _logging
_logging.getLogger(__name__).info(
    "3x-UI config → host=%s user=%s inbound_id=%d",
    XUI_HOST, XUI_USERNAME, INBOUND_ID
)
