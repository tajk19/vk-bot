"""
Модуль обработчиков команд для обычных пользователей.
Обрабатывает запись на стирку, просмотр записей, отмену и другие действия пользователей.
"""
import json
import logging
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

# Настройка Pydantic для работы с vkbottle
try:
    from pydantic import BaseConfig
    BaseConfig.arbitrary_types_allowed = True
except ImportError:
    pass

from vkbottle.bot import Bot, Message

from config import (
    ADMIN_CONTACT_URL,
    ADMIN_IDS,
    DATE_FORMAT,
    MAX_SLOTS_PER_DAY,
    SLOT_INTERVAL_MIN,
    TIME_FORMAT,
)
from google_sheets import (
    ACTIVE_STATUSES,
    STATUS_PENDING,
    add_booking,
    delete_booking,
    time_of_begining,
    time_of_end,
    get_blacklist_sync,
    get_bookings,
    get_user_active_bookings,
    is_time_free,
)
from keyboards import (
    admin_menu,
    cancellation_keyboard,
    main_menu,
    paginate_buttons,
    wash_options_keyboard,
)

WASH_OPTIONS = ["Без добавок", "Отбеливатель", "Порошок", "Кондиционер"]

HELP_TEXT = (
    "Доступные команды:\n"
    "• «Записаться» — выбрать дату, время и опции\n"
    "• «Мои записи» — посмотреть активные брони\n"
    "• «Отмена» — отменить конкретную запись\n"
    "• «Связаться с админом» — получить контакт\n"
    "• «Привет» или «Начать» или «Старт»  — показать меню заново"
)


logger = logging.getLogger(__name__)

# Типы для контекста пользователя
Context = Dict[str, Any]
user_context: Dict[int, Context] = defaultdict(dict)

# Храним ID последних сообщений бота для каждого пользователя
last_bot_messages: Dict[int, int] = {}


def reset_context(user_id: int) -> None:
    user_context.pop(user_id, None)
    # Не удаляем last_bot_messages при сбросе контекста, чтобы можно было удалить последнее сообщение


# async def answer_and_delete_previous(message: Message, text: str, **kwargs) -> None:
#     """
#     Отправляет сообщение и удаляет предыдущее сообщение бота для этого пользователя.
#     """
#     user_id = message.from_id
#     peer_id = message.peer_id
    
#     # Удаляем предыдущее сообщение бота, если оно есть
#     if user_id in last_bot_messages:
#         try:
#             result = await message.ctx_api.messages.delete(
#                 message_ids=[last_bot_messages[user_id]],
#                 delete_for_all=True
#             )
#             logger.info(f"Удалено сообщение {last_bot_messages[user_id]}, результат: {result}")
#         except Exception as exc:
#             # Игнорируем ошибки удаления (сообщение могло быть уже удалено или недоступно)
#             logger.warning(f"Не удалось удалить предыдущее сообщение {last_bot_messages[user_id]}: {exc}")
    
#     # Извлекаем keyboard из kwargs, если есть
#     keyboard = kwargs.get('keyboard')
    
#     # Отправляем новое сообщение через API напрямую, чтобы получить message_id
#     try:
#         random_id = random.randint(0, 2**31 - 1)
        
#         # Подготавливаем параметры для отправки
#         send_params = {
#             "peer_id": peer_id,
#             "message": text,
#             "random_id": random_id,
#         }
        
#         # Добавляем keyboard, если есть
#         if keyboard:
#             # В vkbottle Keyboard имеет метод get_json() для получения JSON строки
#             if hasattr(keyboard, 'get_json'):
#                 send_params["keyboard"] = keyboard.get_json()
#             elif hasattr(keyboard, 'json'):
#                 send_params["keyboard"] = keyboard.json
#             else:
#                 # Если keyboard - это строка, используем её напрямую
#                 send_params["keyboard"] = str(keyboard)
        
#         # Отправляем сообщение
#         result = await message.ctx_api.messages.send(**send_params)
        
#         # Сохраняем message_id из ответа
#         # В VK API messages.send возвращает message_id как int
#         message_id = None
#         if isinstance(result, int):
#             message_id = result
#         elif hasattr(result, 'message_id'):
#             message_id = result.message_id
#         elif isinstance(result, dict):
#             if 'message_id' in result:
#                 message_id = result['message_id']
#             elif 'response' in result:
#                 response = result['response']
#                 if isinstance(response, int):
#                     message_id = response
#                 elif isinstance(response, dict) and 'message_id' in response:
#                     message_id = response['message_id']
        
#         if message_id:
#             last_bot_messages[user_id] = message_id
#             logger.info(f"Отправлено сообщение пользователю {user_id}, message_id: {message_id}")
#         else:
#             logger.warning(f"Не удалось получить message_id из ответа: {result}")
        
#     except Exception as exc:
#         logger.error(f"Ошибка при отправке сообщения: {exc}", exc_info=True)
#         # Fallback: используем обычный message.answer
#         await message.answer(text, **kwargs)


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
    dates = [start_of_week + timedelta(days=i) for i in range(14)]
    return [date for date in dates if date >= today]


def date_keyboard(
    page: int = 0,
    active_bookings: Optional[List[Dict[str, str]]] = None,
):
    dates = booking_window_dates()
    if active_bookings is not None:
        dates = [
            date
            for date in dates
            if free_times_for_date(date, active_bookings)
        ]
    formatted = [date.strftime(DATE_FORMAT) for date in dates]
    return paginate_buttons(
        formatted,
        target="date",
        page=page,
        buttons_per_row=3,
        rows_per_page=4,
    )


def _all_time_slots() -> List[str]:
    return [
        f"{minutes // 60:02d}:{minutes % 60:02d}"
        for minutes in range(0, 24 * 60, SLOT_INTERVAL_MIN)
    ]


def free_times_for_date(
    selected_date: datetime.date,
    active_bookings: Optional[List[Dict[str, str]]] = None,
) -> List[str]:
    if active_bookings is None:
        bookings = get_bookings(date=selected_date, statuses=ACTIVE_STATUSES)
    else:
        date_str = selected_date.strftime(DATE_FORMAT)
        bookings = [
            booking for booking in active_bookings if booking.get("Дата") == date_str
        ]
    existing = {booking["Время"] for booking in bookings}

    offset = timedelta(hours=3)
    dt = timezone(offset, name='МСК')
    now = datetime.now(dt)
    
    slots: List[str] = []
    
    # Определяем время начала в зависимости от дня недели
    # 0 = понедельник, 6 = воскресенье
    current_weekday = selected_date.weekday()

    # Начальное время
    start_hour = time_of_begining(current_weekday)

    # Конечное время
    end_hour = time_of_end(current_weekday) 
    
    
    
    for time_slot in _all_time_slots():
        # Пропускаем слоты до начала рабочего времени
        slot_hour = int(time_slot[:2])
        if slot_hour < start_hour:
            continue
        
        # Пропускаем слоты после окончания рабочего времени
        if slot_hour >= end_hour:
            continue
            
        if time_slot in existing:
            continue
            
        # Проверяем, не прошел ли уже временной слот для текущей даты
        if selected_date == now.date():
            slot_minutes = int(time_slot[:2]) * 60 + int(time_slot[3:])
            current_minutes = now.hour * 60 + now.minute
            if slot_minutes <= current_minutes:
                continue
                
        slots.append(time_slot)
        
    return slots


def time_keyboard(
    selected_date: datetime.date,
    active_bookings: Optional[List[Dict[str, str]]] = None,
    page: int = 0,
):
    free_times = free_times_for_date(selected_date, active_bookings)
    keyboard = paginate_buttons(
        free_times,
        target="time",
        page=page,
        buttons_per_row=4,
        rows_per_page=5,
    )
    return free_times, keyboard


def available_dates(active_bookings: List[Dict[str, str]]) -> List[datetime.date]:
    dates = []
    for date in booking_window_dates():
        if free_times_for_date(date, active_bookings):
            dates.append(date)
    return dates


def format_booking(record: Dict[str, str]) -> str:
    option = record.get("Опция стирки") or "Без добавок"
    status = record.get("Статус", "")
    return f"{record['Дата']} {record['Время']} — {status} ({option})"


def register(bot: Bot):
    def is_admin(user_id: int) -> bool:
        return user_id in ADMIN_IDS

    def normalize(text: Optional[str]) -> str:
        return (text or "").strip().lower()

    user_commands = {
        "привет",
        "начать",
        "старт",
        "записаться",
        "отмена",
        "мои записи",
        "связаться с админом",
        "связаться с администратором",
    }

    admin_commands = {
        "админ меню",
        "неподтвержденные",
        "список записей",
        "блокировать слот",
        "разблокировать слот",
        "черный список +",
        "черный список -",
        "связаться с пользователем",
        "вернуться",
    }

    @bot.on.private_message(text=["привет", "начать", "старт"])
    async def greet_user(message: Message):
        if is_admin(message.from_id):
            await message.answer(
                "Привет! Вы в режиме администратора.",
                keyboard=admin_menu(),
            )
        else:
            await message.answer(
                "Привет! Я бот для записи на стирку вещей.\n"
                "Спасибо, что выбираешь постираться у меня! 🥺\n"
                "Вот такие расценки:\n"
                "90 рублей - стирка со своим порошком🤌\n"
                "Допы: с моим порошком +15 руб, кондиционер или отбеливатель +20 руб 💥\n"
                "+79842878451 альфа банк 💸\n"
                "11 этаж 297 комната 😶‍🌫️\n"
                "Приноси заранее за 5-10 минут, оставляй на пороге(внутри), стучаться не надо❗❗❗\n\n\n"
                f"{HELP_TEXT}\nВыберите действие:",
                keyboard=main_menu(),
            )

    @bot.on.private_message(
        text=["связаться с админом", "Связаться с админом", "связаться с администратором"]
    )
    async def contact_admin(message: Message):
        await message.answer(
            f"Связаться с администратором: {ADMIN_CONTACT_URL}",
            keyboard=main_menu(is_admin=is_admin(message.from_id)),
        )

    @bot.on.private_message(text=["записаться"])
    async def start_booking(message: Message):
        # Быстрая проверка черного списка (синхронная, без await)
        user_link = f"https://vk.com/id{message.from_id}"
        blacklist = get_blacklist_sync()
        if user_link in blacklist:
            await message.answer("❌ Вы в черном списке и не можете записываться.")
            return

        # Отправляем быстрый ответ пользователю
        await message.answer("⏳ Загрузка доступных дат...")
        
        # Получаем активные записи
        active_bookings = get_bookings(statuses=ACTIVE_STATUSES)
        
        # Быстрая проверка доступности дат
        if not available_dates(active_bookings):
            await message.answer(
                "❌ Сейчас нет свободных слотов для записи.\n"
                f"Свяжитесь с администратором: {ADMIN_CONTACT_URL}",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        # Сохраняем контекст и отправляем клавиатуру с датами
        reset_context(message.from_id)
        user_context[message.from_id] = {
            "step": "choose_date",
            "active_bookings": active_bookings,
        }
        await message.answer(
            "Выберите дату для записи:",
            keyboard=date_keyboard(active_bookings=active_bookings),
        )

    @bot.on.private_message(
        func=lambda m: user_context.get(m.from_id, {}).get("step") == "choose_date"
    )
    async def handle_date(message: Message):
        payload = extract_payload(message)
        active_bookings = user_context[message.from_id].get("active_bookings")
        if active_bookings is None:
            active_bookings = get_bookings(statuses=ACTIVE_STATUSES)
            user_context[message.from_id]["active_bookings"] = active_bookings

        if payload.get("action") == "back_to_menu":
            reset_context(message.from_id)
            await message.answer(
                "Главное меню:",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        if payload.get("action") == "paginate" and payload.get("target") == "date":
            page = payload.get("page", 0)
            await message.answer(
                "Выберите дату для записи:",
                keyboard=date_keyboard(page, active_bookings=active_bookings),
            )
            return

        if payload.get("action") == "select" and payload.get("target") == "date":
            date_text = payload.get("value")
        else:
            date_text = message.text.strip()

        try:
            selected_date = datetime.strptime(date_text, DATE_FORMAT).date()
        except ValueError:
            await message.answer(
                "❌ Пожалуйста, выберите дату с клавиатуры.",
                keyboard=date_keyboard(active_bookings=active_bookings),
            )
            return

        if selected_date not in booking_window_dates():
            await message.answer(
                "❌ Эту дату выбрать нельзя. Попробуйте другую:",
                keyboard=date_keyboard(active_bookings=active_bookings),
            )
            return

        free_times, keyboard = time_keyboard(selected_date, active_bookings=active_bookings)
        if not free_times:
            await message.answer(
                "❌ На выбранную дату нет свободных слотов.\n"
                "Выберите другую дату или свяжитесь с администратором.",
                keyboard=date_keyboard(active_bookings=active_bookings),
            )
            return

        user_context[message.from_id]["date"] = selected_date
        user_context[message.from_id]["step"] = "choose_time"
        await message.answer(
            f"Дата *{selected_date.strftime(DATE_FORMAT)}* выбрана. Теперь выберите время:",
            keyboard=keyboard,
        )

    @bot.on.private_message(
        func=lambda m: user_context.get(m.from_id, {}).get("step") == "choose_time"
    )
    async def handle_time(message: Message):
        context = user_context.get(message.from_id)
        if not context or "date" not in context:
            reset_context(message.from_id)
            await message.answer (
                "Сессия бронирования сброшена. Начните заново командой «Записаться».",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        selected_date = context["date"]
        payload = extract_payload(message)
        active_bookings = context.get("active_bookings")
        if active_bookings is None:
            active_bookings = get_bookings(statuses=ACTIVE_STATUSES)
            context["active_bookings"] = active_bookings

        if payload.get("action") == "back_to_menu":
            reset_context(message.from_id)
            await message.answer(
                "Главное меню:",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        if payload.get("action") == "paginate" and payload.get("target") == "time":
            page = payload.get("page", 0)
            _, keyboard = time_keyboard(selected_date, active_bookings, page)
            await message.answer(
                "Выберите время:",
                keyboard=keyboard,
            )
            return

        if payload.get("action") == "select" and payload.get("target") == "time":
            time_text = payload.get("value")
        else:
            time_text = message.text.strip()

        try:
            datetime.strptime(time_text, TIME_FORMAT)
        except ValueError:
            _, keyboard = time_keyboard(selected_date, active_bookings)
            await message.answer(
                "❌ Пожалуйста, выберите время с клавиатуры.",
                keyboard=keyboard,
            )
            return

        if not is_time_free(selected_date, time_text):
            _, keyboard = time_keyboard(selected_date, active_bookings)
            await message.answer(
                "❌ Слот уже занят. Выберите другое время:",
                keyboard=keyboard,
            )
            return

        bookings_same_day = [
            record
            for record in get_user_active_bookings(message.from_id)
            if record.get("Дата") == selected_date.strftime(DATE_FORMAT)
        ]
        if len(bookings_same_day) >= MAX_SLOTS_PER_DAY:
            reset_context(message.from_id)
            await message.answer(
                "❌ Вы достигли лимита бронирований на выбранную дату.",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        context["time"] = time_text
        context["step"] = "choose_options"
        context["options"] = []
        await message.answer(
            "Выберите дополнительные опции (по желанию):",
            keyboard=wash_options_keyboard(WASH_OPTIONS, []),
        )

    @bot.on.private_message(
        func=lambda m: user_context.get(m.from_id, {}).get("step") == "choose_options"
    )
    async def handle_options(message: Message):
        context = user_context.get(message.from_id)
        if not context or "date" not in context or "time" not in context:
            reset_context(message.from_id)
            await message.answer(
                "Сессия бронирования сброшена. Начните заново командой «Записаться».",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        payload = extract_payload(message)
        selected_options: List[str] = context.get("options", [])

        action = payload.get("action")
        
        if action == "back_to_menu":
            reset_context(message.from_id)
            await message.answer(
                "Главное меню:",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return
        
        if action == "toggle_option":
            option_value = payload.get("value")
            if option_value == WASH_OPTIONS[0]:
                selected_options.clear()
            elif option_value in WASH_OPTIONS[1:]:
                if option_value in selected_options:
                    selected_options.remove(option_value)
                else:
                    selected_options.append(option_value)
            context["options"] = selected_options
            await message.answer(
                "Обновлённые опции:",
                keyboard=wash_options_keyboard(WASH_OPTIONS, selected_options),
            )
            return

        if action == "options_reset":
            selected_options.clear()
            await message.answer(
                "Опции сброшены.",
                keyboard=wash_options_keyboard(WASH_OPTIONS, selected_options),
            )
            return

        if action == "options_cancel":
            reset_context(message.from_id)
            await message.answer(
                "Выбор отменён.",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        if action != "options_done":
            await message.answer(
                "Используйте кнопки, чтобы выбрать опции, или нажмите «Готово».",
                keyboard=wash_options_keyboard(WASH_OPTIONS, selected_options),
            )
            return

        selected_date: datetime.date = context["date"]
        time_text: str = context["time"]
        if not is_time_free(selected_date, time_text):
            reset_context(message.from_id)
            await message.answer(
                "❌ Пока вы выбирали опции, слот заняли. Попробуйте снова.",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        vk_user = (await message.ctx_api.users.get(message.from_id))[0]
        full_name = f"{vk_user.first_name} {vk_user.last_name}"
        user_link = f"https://vk.com/id{message.from_id}"

        wash_option = ", ".join(selected_options) if selected_options else "Без добавок"

        add_booking(
            user_name=full_name,
            user_link=user_link,
            date=selected_date,
            time_slot=time_text,
            user_id=message.from_id,
            status=STATUS_PENDING,
            wash_option=wash_option,
        )

        admin_message = (
            "🆕 Новая заявка на стирку\n"
            f"Пользователь: {full_name} ({user_link})\n"
            f"Дата и время: {selected_date.strftime(DATE_FORMAT)} {time_text}\n"
            f"Опции: {wash_option}\n"
            f"ID пользователя: {message.from_id}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.api.messages.send(
                    peer_id=admin_id,
                    message=admin_message,
                    random_id=0,
                )
            except Exception as exc:  # pragma: no cover - уведомление админа
                logger.warning(
                    "Не удалось уведомить администратора %s: %s", admin_id, exc
                )

        reset_context(message.from_id)
        await message.answer(
            "✅ Заявка отправлена на подтверждение администратору.\n"
            "Вносите оплату по номеру - +79842878451 (альфа банк) и ждите подтверждения\n"
            "Мы уведомим вас после принятия решения.",
            keyboard=main_menu(is_admin=is_admin(message.from_id)),
        )

    @bot.on.private_message(text=["отмена"])
    async def cancel_booking(message: Message):
        reset_context(message.from_id)
        bookings = get_user_active_bookings(message.from_id)
        if not bookings:
            await message.answer(
                "❌ У вас нет активных записей.",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        context = {
            "step": "cancel_select",
            "bookings": {str(record["_row"]): record for record in bookings},
        }
        user_context[message.from_id] = context

        details = "\n".join(format_booking(record) for record in bookings)
        await message.answer(
            "Выберите запись для отмены:\n"
            f"{details}",
            keyboard=cancellation_keyboard(bookings),
        )

    @bot.on.private_message(
        func=lambda m: user_context.get(m.from_id, {}).get("step") == "cancel_select"
    )
    async def handle_cancel_selection(message: Message):
        context = user_context.get(message.from_id, {})
        payload = extract_payload(message)
        action = payload.get("action")

        if action == "back_to_menu":
            reset_context(message.from_id)
            await message.answer(
                "Главное меню:",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        if action == "cancel_abort":
            reset_context(message.from_id)
            await message.answer(
                "Отмена бронирования прервана.",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        if action != "cancel_booking":
            await message.answer(
                "Выберите запись кнопкой на клавиатуре или нажмите «Отмена».",
                keyboard=cancellation_keyboard(
                    list(context.get("bookings", {}).values())
                ),
            )
            return

        row_key = str(payload.get("row"))
        bookings_map: Dict[str, Dict[str, str]] = context.get("bookings", {})
        record = bookings_map.get(row_key)
        if not record:
            reset_context(message.from_id)
            await message.answer(
                "Не удалось найти запись. Попробуйте снова.",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        delete_booking(record)
        reset_context(message.from_id)
        await message.answer(
            "✅ Запись отменена.",
            keyboard=main_menu(is_admin=is_admin(message.from_id)),
        )

    @bot.on.private_message(text=["мои записи"])
    async def my_bookings(message: Message):
        records = sorted(
            get_user_active_bookings(message.from_id),
            key=lambda r: (r["Дата"], r["Время"]),
        )
        if not records:
            await message.answer(
                "❌ У вас нет активных записей.",
                keyboard=main_menu(is_admin=is_admin(message.from_id)),
            )
            return

        lines = ["📋 Ваши записи:"]
        for record in records:
            lines.append(format_booking(record))
        await message.answer(
            "\n".join(lines),
            keyboard=main_menu(is_admin=is_admin(message.from_id)),
        )

    @bot.on.private_message(
        func=lambda m: m.from_id not in ADMIN_IDS
        and not user_context.get(m.from_id, {}).get("step")
        and not extract_payload(m)
        and normalize(m.text) not in user_commands
    )
    async def fallback(message: Message):
        await message.answer(
            f"{HELP_TEXT}\n\nВыберите действие:",
            keyboard=main_menu(),
        )
