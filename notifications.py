"""
Модуль для обработки уведомлений о стирках.
Отправляет уведомления пользователям и админам о предстоящих и завершенных стирках.
"""
import asyncio
import datetime
import logging
from typing import Optional
from datetime import datetime, timedelta, timezone

from vkbottle.bot import Bot

from cache import get_cache
from config import ADMIN_IDS, NOTIFY_AFTER_MIN, NOTIFY_BEFORE_MIN, WASH_DURATION_MIN
from google_sheets import STATUS_CONFIRMED, check_sheet_changes, complete_booking, get_bookings

logger = logging.getLogger(__name__)


async def _send(bot: Bot, peer_id: int, message: str) -> None:
    try:
        await bot.api.messages.send(peer_id=peer_id, message=message, random_id=0)
    except Exception as exc:
        logger.exception("Не удалось отправить сообщение %s: %s", peer_id, exc)


async def _notify_admins(bot: Bot, message: str) -> None:
    for admin_id in ADMIN_IDS:
        await _send(bot, admin_id, message)


async def notification_loop(bot: Bot):
    """
    Асинхронный цикл уведомлений:
    - За NOTIFY_BEFORE_MIN минут до начала стирки уведомляет пользователя и админов
    - Через WASH_DURATION_MIN + NOTIFY_AFTER_MIN минут после начала уведомляет об окончании
    """
    while True:
        offset = timedelta(hours=3)
        dt = timezone(offset, name='МСК')
        now = datetime.now(dt)

        today_bookings = get_bookings(
            date=now.date(),
            statuses={STATUS_CONFIRMED},
        )

        for booking in today_bookings:
            booking_time_str = f"{booking['Дата']} {booking['Время']}"
            try:
                booking_start = datetime.strptime(
                    booking_time_str, "%Y-%m-%d %H:%M"
                )
                booking_start = booking_start.replace(tzinfo=dt)
            except ValueError:
                logger.warning("Неверный формат даты/времени в записи: %s", booking)
                continue

            user_peer: Optional[int] = None
            user_id_str = booking.get("Пользователь_ID")
            if user_id_str:
                try:
                    user_peer = int(user_id_str)
                except (TypeError, ValueError):
                    logger.warning("Невалидный ID пользователя: %s", booking)

            diff_before = (booking_start - now).total_seconds()
            notify_before_target = NOTIFY_BEFORE_MIN * 60
            notify_before_window_start = max(0, notify_before_target - 60)
            if notify_before_window_start <= diff_before <= notify_before_target:
                message_user = (
                    f"⚠️ Через {NOTIFY_BEFORE_MIN} минут начнётся ваша стирка.\n"
                    f"Дата: {booking['Дата']} {booking['Время']}\n"
                    f"Опции: {booking.get('Опция стирки') or 'Без добавок'}"
                )
                message_admin = (
                    f"⚠️ Стирка пользователя {booking['Пользователь']} "
                    f"начнётся через {NOTIFY_BEFORE_MIN} минут "
                    f"({booking['Дата']} {booking['Время']})."
                )
                if user_peer is not None:
                    await _send(bot, user_peer, message_user)
                await _notify_admins(bot, message_admin)

            notify_after_time = booking_start + datetime.timedelta(
                minutes=WASH_DURATION_MIN + NOTIFY_AFTER_MIN
            )
            diff_after = (now - notify_after_time).total_seconds()
            if 0 <= diff_after < 60:
                message_user = (
                    f"✅ Ваша стирка завершена.\n"
                    f"Дата: {booking['Дата']} {booking['Время']}"
                )
                message_admin = (
                    f"✅ Стирка пользователя {booking['Пользователь']} завершена "
                    f"({booking['Дата']} {booking['Время']})."
                )
                if user_peer is not None:
                    await _send(bot, user_peer, message_user)
                await _notify_admins(bot, message_admin)
                
                # Удаляем запись после уведомления
                try:
                    complete_booking(booking)
                except Exception as exc:
                    logger.warning("Не удалось удалить завершенную запись %s: %s", booking, exc)
        
        # Удаляем прошедшие записи (которые уже прошли более чем на WASH_DURATION_MIN + NOTIFY_AFTER_MIN минут)
        # Проверяем все подтвержденные записи, не только за сегодня
        all_confirmed = get_bookings(statuses={STATUS_CONFIRMED})
        processed_today_ids = {booking.get("_row") for booking in today_bookings}
        
        for booking in all_confirmed:
            # Пропускаем записи за сегодня, они уже обработаны выше
            if booking.get("_row") in processed_today_ids:
                continue
                
            booking_time_str = f"{booking['Дата']} {booking['Время']}"
            try:
                booking_start = datetime.strptime(
                    booking_time_str, "%Y-%m-%d %H:%M"
                )
            except ValueError:
                continue
            
            # Удаляем записи, которые прошли более чем на WASH_DURATION_MIN + NOTIFY_AFTER_MIN минут
            booking_end_time = booking_start + datetime.timedelta(
                minutes=WASH_DURATION_MIN + NOTIFY_AFTER_MIN
            )
            if now > booking_end_time:
                try:
                    complete_booking(booking)
                    logger.info(
                        "Автоматически удалена прошедшая запись: %s %s",
                        booking['Дата'],
                        booking['Время']
                    )
                except Exception as exc:
                    logger.warning(
                        "Не удалось автоматически удалить прошедшую запись %s: %s",
                        booking,
                        exc
                    )

        # Периодически очищаем истекшие записи из кеша
        cache = get_cache()
        cache.clear_expired()
        
        # Проверяем изменения в Google Sheets и обновляем кеш при необходимости
        try:
            check_sheet_changes()
        except Exception as exc:
            logger.warning(f"Ошибка при проверке изменений в Google Sheets: {exc}")

        await asyncio.sleep(60)
