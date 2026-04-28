"""
Background expiry notification scheduler.

This module handles automated notifications for subscription expiry reminders
and automatic promo code generation for renewals.
"""
import asyncio
import logging
import random
import string
import time
from datetime import datetime

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database import get_keys_nearing_expiry, create_promo_code, validate_promo_code

logger = logging.getLogger(__name__)


def get_day_word(days: int) -> str:
    """Return correct form of 'day' word in Russian."""
    if days % 10 == 1 and days % 100 != 11:
        return "день"
    elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
        return "дня"
    else:
        return "дней"


def generate_promo_code(length: int = 8) -> str:
    """Generate a random promo code."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


async def get_or_create_renewal_promo(user_id: int) -> str:
    """
    Get existing renewal promo code for user or create a new one.

    Creates a 30% discount promo code valid for 7 days.
    """
    # Try to find existing active promo code for this user
    # We'll use a naming convention: RENEW{user_id}
    code = f"RENEW{user_id}"

    existing = await validate_promo_code(code)
    if existing and existing["is_active"]:
        return code

    # Create new promo code: 30% discount, 7 days validity, 1 use
    success = await create_promo_code(
        code=code,
        promo_type="percent",
        discount_value=30,
        max_uses=1,
        valid_days=7
    )

    if success:
        logger.info("Created renewal promo code %s for user %d", code, user_id)
        return code
    else:
        # Fallback to random code if naming convention fails
        random_code = generate_promo_code()
        await create_promo_code(
            code=random_code,
            promo_type="percent",
            discount_value=30,
            max_uses=1,
            valid_days=7
        )
        return random_code


async def _send_urgent_notification(bot: Bot, item: dict) -> None:
    """Send urgent notification for keys expiring in 1-3 days."""
    date_str = datetime.fromtimestamp(item["expiry"]).strftime("%d.%m.%Y")
    days_left = max(1, int((item["expiry"] - int(time.time())) / 86400))

    promo_code = await get_or_create_renewal_promo(item["user_id"])

    text = (
        f"🚨 <b>СРОЧНО! Ваша подписка истекает!</b>\n\n"
        f"📅 Дата окончания: <b>{date_str}</b>\n"
        f"🔔 Осталось: <b>{days_left} {get_day_word(days_left)}</b>\n\n"
        f"⚠️ <b>ВНИМАНИЕ:</b> После истечения срока вы потеряете доступ к:\n"
        f"• YouTube и все видео\n"
        f"• Telegram и мессенджеры\n"
        f"• Социальные сети\n"
        f"• Все заблокированные сайты\n\n"
        f"🎁 <b>СПЕЦИАЛЬНОЕ ПРЕДЛОЖЕНИЕ:</b>\n"
        f"Используйте промокод <code>{promo_code}</code> для получения <b>30% СКИДКИ</b> на продление!\n"
        f"Промокод действителен 7 дней.\n\n"
        f"💰 <b>Экономия:</b>\n"
        f"• 1 месяц: сэкономите ~30 ₽\n"
        f"• 3 месяца: сэкономите ~70 ₽\n"
        f"• 6 месяцев: сэкономите ~120 ₽\n"
        f"• 12 месяцев: сэкономите ~210 ₽\n\n"
        f"⏰ <b>Не откладывайте!</b> Продлите прямо сейчас, чтобы сохранить доступ."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Продлить сейчас", callback_data="buy_vpn")],
        [InlineKeyboardButton(text="🎁 Пригласить друга и получить +5 дней", callback_data="partner")]
    ])

    await bot.send_message(item["user_id"], text, parse_mode="HTML", reply_markup=kb)


async def _send_warning_notification(bot: Bot, item: dict) -> None:
    """Send warning notification for keys expiring in 7-14 days."""
    date_str = datetime.fromtimestamp(item["expiry"]).strftime("%d.%m.%Y")
    days_left = max(1, int((item["expiry"] - int(time.time())) / 86400))

    promo_code = await get_or_create_renewal_promo(item["user_id"])

    text = (
        f"⏳ <b>Напоминание о продлении подписки</b>\n\n"
        f"📅 Дата окончания: <b>{date_str}</b>\n"
        f"🔔 Осталось: <b>{days_left} {get_day_word(days_left)}</b>\n\n"
        f"🎁 <b>Ваш эксклюзивный промокод:</b>\n"
        f"<code>{promo_code}</code> — <b>30% СКИДКА</b> на продление!\n"
        f"Действителен 7 дней.\n\n"
        f"💡 <b>Почему стоит продлить сейчас?</b>\n"
        f"• Гарантированный доступ без перерывов\n"
        f"• Стабильная скорость работы\n"
        f"• Поддержка всех устройств\n"
        f"• Сэкономьте с промокодом!\n\n"
        f"� <b>Партнёрская программа:</b> Приглашайте друзей и получайте +5 дней за каждого!"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Продлить подписку", callback_data="buy_vpn")],
        [InlineKeyboardButton(text="🎁 Пригласить друга", callback_data="partner")]
    ])

    await bot.send_message(item["user_id"], text, parse_mode="HTML", reply_markup=kb)


async def _send_early_notification(bot: Bot, item: dict) -> None:
    """Send early notification for keys expiring in 21-30 days."""
    date_str = datetime.fromtimestamp(item["expiry"]).strftime("%d.%m.%Y")
    days_left = max(1, int((item["expiry"] - int(time.time())) / 86400))

    text = (
        f"📢 <b>Информация о вашей подписке</b>\n\n"
        f"📅 Дата окончания: <b>{date_str}</b>\n"
        f"🔔 Осталось: <b>{days_left} {get_day_word(days_left)}</b>\n\n"
        f"✅ <b>Ваша подписка активна!</b>\n"
        f"Продлите заранее, чтобы избежать перерывов в работе.\n\n"
        f"💡 <b>Совет:</b> Чем дольше срок подписки, тем меньше цена за месяц!\n"
        f"• 12 месяцев: всего 59 ₽/мес\n"
        f"• 6 месяцев: всего 69 ₽/мес\n"
        f"• 3 месяца: всего 79 ₽/мес\n\n"
        f"🎁 <b>Скоро:</b> Приближается дата продления — мы пришлём вам промокод на скидку!"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Продлить подписку", callback_data="buy_vpn")],
        [InlineKeyboardButton(text="🎁 Пригласить друга", callback_data="partner")]
    ])

    await bot.send_message(item["user_id"], text, parse_mode="HTML", reply_markup=kb)


async def _send_expiry_notifications(bot: Bot) -> None:
    """Send all expiry notifications based on days remaining."""
    # Urgent: 1-3 days remaining
    urgent_keys = await get_keys_nearing_expiry(days_min=1, days_max=3)
    for item in urgent_keys:
        try:
            await _send_urgent_notification(bot, item)
            logger.info("Sent urgent notification to user %d", item["user_id"])
        except Exception as e:
            logger.debug("Urgent notification error for user %d: %s", item["user_id"], e)

    # Warning: 7-14 days remaining
    warning_keys = await get_keys_nearing_expiry(days_min=7, days_max=14)
    for item in warning_keys:
        try:
            await _send_warning_notification(bot, item)
            logger.info("Sent warning notification to user %d", item["user_id"])
        except Exception as e:
            logger.debug("Warning notification error for user %d: %s", item["user_id"], e)

    # Early: 21-30 days remaining
    early_keys = await get_keys_nearing_expiry(days_min=21, days_max=30)
    for item in early_keys:
        try:
            await _send_early_notification(bot, item)
            logger.info("Sent early notification to user %d", item["user_id"])
        except Exception as e:
            logger.debug("Early notification error for user %d: %s", item["user_id"], e)


async def start_notification_scheduler(bot: Bot) -> None:
    """Run expiry notifications once per day at ~10:00."""
    logger.info("Notification scheduler started")
    while True:
        try:
            await _send_expiry_notifications(bot)
            logger.info("Daily notifications completed")
        except Exception as e:
            logger.error("Scheduler error: %s", e)
        # Wait 24 hours using asyncio.sleep (non-blocking)
        await asyncio.sleep(86400)
