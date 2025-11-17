"""
Модуль обработчиков команд для администраторов.
Обрабатывает подтверждение/отклонение заявок, блокировку слотов, управление черным списком и т.д.
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone 
from typing import Any, Dict, List

# Настройка Pydantic для работы с vkbottle
try:
    from pydantic import BaseConfig
    BaseConfig.arbitrary_types_allowed = True
except ImportError:
    pass

from vkbottle.bot import Bot, Message

from config import (
    ADMIN_IDS,
    DATE_FORMAT,
    SLOT_INTERVAL_MIN,
    TIME_FORMAT,
)
from google_sheets import (
    STATUS_BLOCKED,
    STATUS_CONFIRMED,
    add_blacklist,
    add_booking,
    complete_booking,
    delete_booking,
    get_admin_blockings,
    get_blacklist,
    get_bookings,
    get_pending_bookings,
    is_time_free,
    remove_blacklist,
    set_booking_confirmed,
    set_booking_rejected,
    time_of_begining,
    time_of_end,
)
from keyboards import (
    booking_list_keyboard,
    main_menu,
    paginate_buttons,
    pending_decision_keyboard,
    unblock_keyboard,
)

logger = logging.getLogger(__name__)



# Контекст для хранения состояния диалога администраторов
admin_context: Dict[int, Dict[str, Any]] = defaultdict(dict)


def extract_payload(message: Message) -> Dict[str, Any]:
    payload = message.payload
    if not payload:
        return {}
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def booking_window_dates() -> List[datetime.date]:
    today = datetime.today().date()
    start_of_week = today - timedelta(days=today.weekday())
    return [
        date for date in (start_of_week + timedelta(days=i) for i in range(14))
        if date >= today
    ]


def date_keyboard(page: int = 0):
    formatted = [date.strftime(DATE_FORMAT) for date in booking_window_dates()]
    return paginate_buttons(
        formatted,
        target="admin_date",
        page=page,
        buttons_per_row=3,
        rows_per_page=4,
    )


def _all_time_slots() -> List[str]:
    """Генерирует все возможные временные слоты."""
    return [
        f"{minutes // 60:02d}:{minutes % 60:02d}"
        for minutes in range(0, 24 * 60, SLOT_INTERVAL_MIN)
    ]


def time_slots_keyboard(date: datetime.date, page: int = 0):
    """
    Генерирует клавиатуру с временными слотами для админа.
    Учитывает расписание работы (как при записи пользователя).
    """
    # Определяем время начала и конца в зависимости от дня недели
    # 0 = понедельник, 6 = воскресенье
    current_weekday = date.weekday()
    start_hour = time_of_begining(current_weekday)
    end_hour = time_of_end(current_weekday)
    
    # Фильтруем слоты по расписанию работы
    all_slots = _all_time_slots()
    times = []
    
    offset = timedelta(hours=3)
    dt = timezone(offset, name='МСК')
    now = datetime.now(dt)
    
    for time_slot in all_slots:
        slot_hour = int(time_slot[:2])
        
        # Пропускаем слоты до начала рабочего времени
        if slot_hour < start_hour:
            continue
        
        # Пропускаем слоты после окончания рабочего времени
        if slot_hour >= end_hour:
            continue
        
        # Для текущей даты пропускаем прошедшие слоты
        if date == now.date():
            slot_minutes = int(time_slot[:2]) * 60 + int(time_slot[3:])
            current_minutes = now.hour * 60 + now.minute
            if slot_minutes <= current_minutes:
                continue
        
        times.append(time_slot)
    
    keyboard = paginate_buttons(
        times,
        target="admin_time",
        page=page,
        buttons_per_row=4,
        rows_per_page=5,
    )
    return times, keyboard


def format_booking(record: Dict[str, str]) -> str:
    return (
        f"{record['Дата']} {record['Время']} — {record['Пользователь']} "
        f"({record['Ссылка']}) [{record['Статус']}] | Опции: {record.get('Опция стирки') or 'Без добавок'}"
    )


def register(bot: Bot):
    def is_admin(message: Message) -> bool:
        return message.from_id in ADMIN_IDS

    async def send_user_notification(user_id: str, text: str) -> None:
        """
        Отправляет уведомление пользователю.
        
        Args:
            user_id: ID пользователя VK (строка)
            text: Текст уведомления
        """
        if not user_id:
            return
        try:
            await bot.api.messages.send(
                peer_id=int(user_id),
                message=text,
                random_id=0,
            )
        except Exception as exc:
            logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {exc}")

    async def finalize_rejection(
        message: Message,
        record: Dict[str, str],
        reason: str,
        persist_context: bool = True,
    ) -> None:
        admin_info = (await message.ctx_api.users.get(message.from_id))[0]
        admin_name = f"{admin_info.first_name} {admin_info.last_name}"
        updated = set_booking_rejected(record, admin_name, reason) #todo удалить фичу внесения отказов в таблицу
        if persist_context:
            admin_context.pop(message.from_id, None)
        if updated is None:
            await message.answer("Запись уже была удалена.")
            return

        display_reason = reason if reason else "не указана"
        await message.answer(
            f"❌ Заявка отклонена. Причина: {display_reason}",
            keyboard=main_menu(is_admin),
        )

        await send_user_notification(
            updated.get("Пользователь_ID"),
            "❌ Ваша запись отклонена.\n"
            f"Дата: {updated['Дата']} {updated['Время']}\n"
            f"Причина: {display_reason}",
        )

        # Удаляем запись
        delete_booking(record)
        admin_context.pop(message.from_id, None)

    @bot.on.private_message(text=["Админ меню"])
    async def show_admin_menu(message: Message):
        if not is_admin(message):
            return
        await message.answer(
            "Админ меню:",
            keyboard=main_menu(is_admin),
        )

    @bot.on.private_message(text=["Вернуться"])
    async def back_to_main(message: Message):
        if not is_admin(message):
            return
        admin_context.pop(message.from_id, None)
        await message.answer(
            "Админ меню:",
            keyboard=main_menu(is_admin),
        )
    
    @bot.on.private_message(
        func=lambda m: admin_context.get(m.from_id, {}).get("step") == "booking_list"
    )
    async def handle_booking_list_selection(message: Message, page: int = 0):
        if not is_admin(message):
            return
        
        payload = extract_payload(message)
        action = payload.get("action")
        
        # Обработка пагинации
        if action == "booking_list_page":
            await handle_booking_list_page(message, payload.get("page", 0))
            return
        
        if action == "back_to_menu":
            admin_context.pop(message.from_id, None)
            await message.answer(
                "Админ меню:",
                keyboard=main_menu(is_admin),
            )
            return
        
        # Получаем ВСЕ записи для контекста
        all_bookings = get_bookings()  # Твоя функция получения всех записей
        
        if action != "admin_complete_booking":
            # Сохраняем все записи в контекст для последующего использования
            context = admin_context.get(message.from_id, {})
            context["all_bookings"] = all_bookings
            admin_context[message.from_id] = context
            
            # Показываем первую страницу
            await show_booking_page(message, all_bookings, page)
            return
        
        # Обработка завершения записи
        row_key = str(payload.get("row"))
        
        # Ищем запись во всех бронированиях
        target_booking = None
        for booking in all_bookings:
            if str(booking.get("_row")) == row_key:
                target_booking = booking
                break
        
        if not target_booking:
            admin_context.pop(message.from_id, None)
            await message.answer("Не удалось найти запись. Попробуйте снова.")
            return
        
        # Отправляем уведомление клиенту
        user_id = target_booking.get("Пользователь_ID")
        if user_id:
            try:
                await send_user_notification(
                    user_id,
                    f"✅ Ваша стирка завершена!\n"
                    f"Дата: {target_booking['Дата']} {target_booking['Время']}\n"
                    f"Спасибо, что воспользовались нашими услугами!",
                )
            except Exception as exc:
                logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {exc}")
        
        # Удаляем запись
        complete_booking(target_booking)
        admin_context.pop(message.from_id, None)
        
        await message.answer(
            f"✅ Запись завершена и удалена из таблицы.\n"
            f"Клиент {target_booking.get('Пользователь', 'неизвестен')} уведомлен.",
            keyboard=main_menu(is_admin),
        )

    async def handle_booking_list_page(message: Message, page: int):
        """Обработчик смены страницы"""
        if not is_admin(message):
            return
        
        # Получаем все записи из контекста или заново
        context = admin_context.get(message.from_id, {})
        all_bookings = context.get("all_bookings", get_bookings())
        
        await show_booking_page(message, all_bookings, page)

    async def show_booking_page(message: Message, all_bookings: list, page: int):
        """Показывает страницу с записями"""
        keyboard = booking_list_keyboard(all_bookings, page=page)
        total_pages = max(1, (len(all_bookings) + 7) // 8)  # Округление вверх
        
        text = f"Список записей (страница {page + 1} из {total_pages}):\nИспользуйте кнопки для выбора записи."
        
        await message.answer(text, keyboard=keyboard)
        
        # Обновляем контекст
        context = admin_context.get(message.from_id, {})
        context["all_bookings"] = all_bookings
        context["current_page"] = page
        admin_context[message.from_id] = context

    @bot.on.private_message(text=["Неподтвержденные"])
    async def pending_list(message: Message):
        if not is_admin(message):
            return
        records = get_pending_bookings()
        if not records:
            await message.answer("📭 Нет заявок, ожидающих подтверждения.")
            return
        for record in records:
            details = (
                f"Заявка №{record['_row']}:\n"
                f"Дата: {record['Дата']} {record['Время']}\n"
                f"Пользователь: {record['Пользователь']} ({record['Ссылка']})\n"
                f"Опции: {record.get('Опция стирки') or 'Без добавок'}"
            )
            await message.answer(
                details,
                keyboard=pending_decision_keyboard(record["_row"]),
            )

    @bot.on.private_message(text=["Список записей"])
    async def show_bookings(message: Message):
        if not is_admin(message):
            return
        # Показываем только подтвержденные записи для завершения
        records = sorted(
            get_bookings(statuses={STATUS_CONFIRMED}),
            key=lambda r: (r["Дата"], r["Время"]),
        )
        if not records:
            await message.answer(
                "Список подтвержденных записей пуст.",
                keyboard=main_menu(is_admin),
            )
            return
        
        admin_context[message.from_id] = {
            "step": "booking_list",
            "bookings": {str(record["_row"]): record for record in records},
        }
        
        chunks: List[str] = []
        current_chunk: List[str] = []
        for record in records:
            entry = format_booking(record)
            current_chunk.append(entry)
            if len("\n".join(current_chunk)) > 3500:
                chunks.append("\n".join(current_chunk))
                current_chunk = []
        if current_chunk:
            chunks.append("\n".join(current_chunk))
        
        for i, chunk in enumerate(chunks):
            if i == len(chunks) - 1:
                # Последний чанк с клавиатурой
                await message.answer(
                    f"📋 Подтвержденные записи (выберите для завершения):\n{chunk}",
                    keyboard=booking_list_keyboard(records),
                )
            else:
                await message.answer(f"📋 Записи:\n{chunk}")

    @bot.on.private_message(text=["Блокировать слот"])
    async def start_block_slot(message: Message):
        if not is_admin(message):
            return
        admin_context[message.from_id] = {"step": "block_date"}
        await message.answer(
            "Выберите дату для блокировки:",
            keyboard=date_keyboard(),
        )

    @bot.on.private_message(
        func=lambda m: admin_context.get(m.from_id, {}).get("step") == "block_date"
    )
    async def handle_block_date(message: Message):
        if not is_admin(message):
            return
        payload = extract_payload(message)
        if payload.get("action") == "paginate" and payload.get("target") == "admin_date":
            page = payload.get("page", 0)
            await message.answer(
                "Выберите дату для блокировки:",
                keyboard=date_keyboard(page),
            )
            return

        if payload.get("action") == "select" and payload.get("target") == "admin_date":
            date_text = payload.get("value")
        else:
            date_text = message.text.strip()

        try:
            selected_date = datetime.strptime(date_text, DATE_FORMAT).date()
        except ValueError:
            await message.answer(
                "❌ Неверный формат даты. Используйте YYYY-MM-DD.",
                keyboard=date_keyboard(),
            )
            return

        admin_context[message.from_id]["date"] = selected_date
        admin_context[message.from_id]["step"] = "block_time"
        _, keyboard = time_slots_keyboard(selected_date)
        await message.answer(
            f"Дата {selected_date} выбрана. Теперь выберите время:",
            keyboard=keyboard,
        )

    @bot.on.private_message(
        func=lambda m: admin_context.get(m.from_id, {}).get("step") == "block_time"
    )
    async def handle_block_time(message: Message):
        if not is_admin(message):
            return
        context = admin_context.get(message.from_id)
        payload = extract_payload(message)
        selected_date = context.get("date")
        if not selected_date:
            admin_context.pop(message.from_id, None)
            await message.answer("Сессия прервана. Начните заново.")
            return

        if payload.get("action") == "paginate" and payload.get("target") == "admin_time":
            page = payload.get("page", 0)
            _, keyboard = time_slots_keyboard(selected_date, page)
            await message.answer(
                "Выберите время для блокировки:",
                keyboard=keyboard,
            )
            return

        if payload.get("action") == "select" and payload.get("target") == "admin_time":
            time_text = payload.get("value")
        else:
            time_text = message.text.strip()

        try:
            datetime.strptime(time_text, TIME_FORMAT)
        except ValueError:
            _, keyboard = time_slots_keyboard(selected_date)
            await message.answer(
                "❌ Неверный формат времени. Используйте HH:MM.",
                keyboard=keyboard,
            )
            return

        if not is_time_free(selected_date, time_text):
            _, keyboard = time_slots_keyboard(selected_date)
            await message.answer(
                "❌ Слот уже занят или забронирован.",
                keyboard=keyboard,
            )
            return

        add_booking(
            user_name="Блокировка администратора",
            user_link="admin_blocked",
            date=selected_date,
            time_slot=time_text,
            user_id=None,
            status=STATUS_BLOCKED,
            wash_option="Блокировка",
            confirmed_by="Администратор",
            confirmed_at=datetime.utcnow().isoformat(),
        )
        admin_context.pop(message.from_id, None)
        await message.answer(
            f"✅ Слот {selected_date} {time_text} заблокирован.",
            keyboard=main_menu(is_admin),
        )

    @bot.on.private_message(text=["Разблокировать слот"])
    async def start_unblock(message: Message):
        if not is_admin(message):
            return
        blockings = get_admin_blockings()
        if not blockings:
            await message.answer("Нет заблокированных слотов.")
            return
        admin_context[message.from_id] = {
            "step": "unblock_select",
            "blockings": {str(record["_row"]): record for record in blockings},
        }
        await message.answer(
            "Выберите слот, который нужно разблокировать:",
            keyboard=unblock_keyboard(blockings),
        )

    @bot.on.private_message(
        func=lambda m: admin_context.get(m.from_id, {}).get("step") == "unblock_select"
    )
    async def handle_unblock_selection(message: Message):
        if not is_admin(message):
            return
        payload = extract_payload(message)
        action = payload.get("action")
        context = admin_context.get(message.from_id, {})

        if action == "admin_unblock_cancel":
            admin_context.pop(message.from_id, None)
            await message.answer(
                "Разблокировка отменена.",
                keyboard=main_menu(is_admin),
            )
            return

        if action != "admin_unblock":
            blockings = list(context.get("blockings", {}).values())
            await message.answer(
                "Используйте кнопки клавиатуры, чтобы выбрать слот.",
                keyboard=unblock_keyboard(blockings),
            )
            return

        row_key = str(payload.get("row"))
        record = context.get("blockings", {}).get(row_key)
        if not record:
            admin_context.pop(message.from_id, None)
            await message.answer("Не удалось найти слот. Попробуйте снова.")
            return

        delete_booking(record)
        admin_context.pop(message.from_id, None)
        await message.answer(
            "✅ Слот разблокирован.",
            keyboard=main_menu(is_admin),
        )


    @bot.on.private_message(text=["Черный список"])
    async def request_blacklist(message: Message):
        if not is_admin(message):
            return
        blacklist = await get_blacklist(bot.api)
        if blacklist:
            for user in blacklist:
                await message.answer(f"https://vk.com/id{user}")
        else:
            await message.answer("Черный список пуст")

        

    @bot.on.private_message(text=["+ в черный список"])
    async def request_blacklist_add(message: Message):
        if not is_admin(message):
            return
        admin_context[message.from_id] = {"step": "blacklist_add"}
        await message.answer("Отправьте ссылку пользователя для добавления в черный список.")

    @bot.on.private_message(text=["- из черного списка"])
    async def request_blacklist_remove(message: Message):
        if not is_admin(message):
            return
        admin_context[message.from_id] = {"step": "blacklist_remove"}
        await message.answer("Отправьте ссылку пользователя для удаления из черного списка.")

    @bot.on.private_message(
        func=lambda m: admin_context.get(m.from_id, {}).get("step")
        in {"blacklist_add", "blacklist_remove"}
    )
    async def handle_blacklist_input(message: Message):
        if not is_admin(message):
            return
        context = admin_context.get(message.from_id, {})
        step = context.get("step")
        link = message.text
        if step == "blacklist_add":
            await add_blacklist(bot.api, link)
            admin_context.pop(message.from_id, None)
            await message.answer(f"✅ Пользователь {link} добавлен в черный список.")
        elif step == "blacklist_remove":
            removed = remove_blacklist(link)
            admin_context.pop(message.from_id, None)
            if removed:
                await message.answer(f"✅ Пользователь {link} удален из черного списка.")
            else:
                await message.answer("❌ Пользователь не найден в черном списке.")
        else:
            await message.answer("Сессия истекла. Начните заново.")

    @bot.on.private_message(
        func=lambda m: admin_context.get(m.from_id, {}).get("step") == "reject_reason"
    )
    async def handle_reject_reason(message: Message):
        if not is_admin(message):
            return
        context = admin_context.get(message.from_id, {})
        record = context.get("record")
        if not record:
            admin_context.pop(message.from_id, None)
            await message.answer("Сессия истекла. Отклонение не выполнено.")
            return
        if extract_payload(message):
            await message.answer("Пожалуйста, отправьте причину отказа текстом.")
            return
        reason = (message.text or "").strip()
        if not reason:
            await message.answer(
                "Пожалуйста, укажите причину отказа или повторно нажмите «Отклонить» для отказа без комментария."
            )
            return
        await finalize_rejection(message, record, reason)

    @bot.on.private_message(func=lambda m: m.from_id in ADMIN_IDS)
    async def handle_admin_payloads(message: Message):
        payload = extract_payload(message)
        action = payload.get("action")
        if not action:
            return

        context = admin_context.get(message.from_id)

        if action != "admin_reject" and context and context.get("step") == "reject_reason":
            await message.answer("Сначала укажите причину отказа.")
            return

        if action == "admin_confirm":
            row = str(payload.get("row"))
            record = next(
                (r for r in get_pending_bookings() if str(r["_row"]) == row),
                None,
            )
            if not record:
                await message.answer("❌ Заявка уже обработана или не найдена.")
                return

            admin_info = (await message.ctx_api.users.get(message.from_id))[0]
            admin_name = f"{admin_info.first_name} {admin_info.last_name}"
            updated = set_booking_confirmed(record, admin_name)
            await message.answer(
                f"✅ Заявка подтверждена.\n{format_booking(updated)}",
                keyboard=main_menu(is_admin),
            )
            await send_user_notification(
                updated.get("Пользователь_ID"),
                "✅ Ваша запись подтверждена!\n"
                f"Дата: {updated['Дата']} {updated['Время']}\n"
                f"Опции: {updated.get('Опция стирки') or 'Без добавок'}",
            )
            return

        if action == "admin_reject":
            context = admin_context.get(message.from_id)
            if context and context.get("step") == "reject_reason" and context.get("record"):
                record = context["record"]
                await finalize_rejection(
                    message, record, "", persist_context=False
                )
                admin_context.pop(message.from_id, None)
                return

            row = str(payload.get("row"))
            record = next(
                (r for r in get_pending_bookings() if str(r["_row"]) == row),
                None,
            )
            if not record:
                await message.answer("❌ Заявка уже обработана или не найдена.")
                return

            admin_context[message.from_id] = {
                "step": "reject_reason",
                "record": record,
            }
            await message.answer(
                "Укажите причину отказа для пользователя "
                f"{record['Пользователь']} ({record['Дата']} {record['Время']}):"
            )
            return

        if action in {"admin_unblock", "admin_unblock_cancel"}:
            # Эти действия обрабатываются в другом обработчике со стейтом.
            return
        
        if action == "back_to_menu":
            admin_context.pop(message.from_id, None)
            await message.answer(
                "Админ меню:",
                keyboard=main_menu(is_admin),
            )
            return

    @bot.on.private_message(
        func=lambda m: m.from_id in ADMIN_IDS
        and not admin_context.get(m.from_id, {}).get("step")
        and not extract_payload(m)
    )
    async def admin_fallback(message: Message):
        await message.answer("Админ меню:", keyboard=main_menu(is_admin))
