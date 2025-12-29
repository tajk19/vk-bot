"""
Модуль обработчиков команд для обычных пользователей.
Обрабатывает запись на стирку, просмотр записей, отмену и другие действия пользователей.
"""
from handlers.role import Role
from datetime import datetime
from typing import Dict, List

# Настройка Pydantic для работы с vkbottle
try:
    from pydantic import BaseConfig
    BaseConfig.arbitrary_types_allowed = True
except ImportError:
    pass

from vkbottle.bot import Bot, Message, BotLabeler

from config import (
    ADMIN_CONTACT_URL,
    ADMIN_IDS,
    DATE_FORMAT,
    MAX_SLOTS_PER_DAY,
    TIME_FORMAT,
    WASH_OPTIONS,
    WASH_PRICES,
    format_date_with_weekday,
    convert_from_format_with_weekday,
)
from google_sheets import (
    ACTIVE_STATUSES,
    STATUS_PENDING,
    add_booking,
    delete_booking,
    get_blacklist,
    get_bookings,
    get_user_active_bookings,
    is_time_free,
)
from keyboards import (
    paginate_buttons,
    user_menu,
    wash_options_keyboard,
)

class User(Role):
    
    def __init__(self, bot: Bot):
        self.labeler = BotLabeler()
        self.labeler.vbml_ignore_case = True
        self.register(bot)
        bot.labeler.load(self.labeler)
    
    def format_booking(self, record: Dict[str, str]) -> str:
        option = record.get("Опция стирки") or "Без добавок"
        return f"{format_date_with_weekday(datetime.strptime(record['Дата'], DATE_FORMAT))} {record['Время']} — ({option})"

    
    HELP_TEXT = (
        "Доступные команды:\n"
        "• «Записаться» — выбрать дату, время и опции\n"
        "• «Мои записи» — посмотреть активные брони\n"
        "• «Отменить запись» — отменить конкретную запись\n"
        "• «Связаться с админом» — получить контакт\n"
        "• «Привет» или «Начать» или «Старт»  — показать меню заново"
    )

    # Храним ID последних сообщений бота для каждого пользователя
    # last_bot_messages: Dict[int, int] = {}

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
    #             self.logger.info(f"Удалено сообщение {last_bot_messages[user_id]}, результат: {result}")
    #         except Exception as exc:
    #             # Игнорируем ошибки удаления (сообщение могло быть уже удалено или недоступно)
    #             self.logger.warning(f"Не удалось удалить предыдущее сообщение {last_bot_messages[user_id]}: {exc}")
        
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
    #             self.logger.info(f"Отправлено сообщение пользователю {user_id}, message_id: {message_id}")
    #         else:
    #             self.logger.warning(f"Не удалось получить message_id из ответа: {result}")
            
    #     except Exception as exc:
    #         self.logger.error(f"Ошибка при отправке сообщения: {exc}", exc_info=True)
    #         # Fallback: используем обычный message.answer
    #         await message.answer(text, **kwargs)


    def register(self, bot: Bot):

        user_commands = {
            "записаться",
            "отменить запись",
            "мои записи",
            "связаться с админом",
            "связаться с администратором",
        }

        @self.labeler.private_message(
            text=["связаться с админом", "связаться с администратором"]
        )
        async def contact_admin(message: Message):
            await message.answer(
                f"Связаться с администратором: {ADMIN_CONTACT_URL}",
                keyboard=user_menu(),
            )

        @self.labeler.private_message(text=["записаться"], func=self.is_user)
        async def start_booking(message: Message):
            # Быстрая проверка черного списка (синхронная, без await)
            user_link = f"https://vk.com/id{message.from_id}"
            blacklist = await get_blacklist()
            if user_link in blacklist:
                await message.answer("❌ Вы в черном списке и не можете записываться.")
                return

            # Отправляем быстрый ответ пользователю
            await message.answer("⏳ Загрузка доступных дат...")
            
            # Получаем активные записи
            active_bookings = get_bookings(statuses=ACTIVE_STATUSES)
            
            # Быстрая проверка доступности дат
            if not self.available_dates(active_bookings):
                await message.answer(
                    "❌ Сейчас нет свободных слотов для записи.\n"
                    f"Свяжитесь с администратором: {ADMIN_CONTACT_URL}",
                    keyboard=user_menu(),
                )
                return

            # Сначала отчищаем, потом сохраняем контекст и отправляем клавиатуру с датами
            self.reset_context(message.from_id)
            self.context[message.from_id] = {
                "step": "choose_date",
                "active_bookings": active_bookings,
            }
            await message.answer(
                "Выберите дату для записи:",
                keyboard=self.date_keyboard(active_bookings=active_bookings),
            )

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "choose_date"
            and self.is_user(m)
        )
        async def handle_date(message: Message):
            payload = self.extract_payload(message)
            active_bookings = self.context[message.from_id].get("active_bookings")
            action = payload.get("action")
            if active_bookings is None:
                active_bookings = get_bookings(statuses=ACTIVE_STATUSES)
                self.context[message.from_id]["active_bookings"] = active_bookings

            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Главное меню:",
                    keyboard=user_menu(),
                )
                return

            if action == "paginate" and payload.get("target") == "date":
                page = payload.get("page", 0)
                await message.answer(
                    "Выберите дату для записи:",
                    keyboard=self.date_keyboard(page=page, active_bookings=active_bookings),
                )
                return

            if action == "select" and payload.get("target") == "date":
                date_text = payload.get("value")
            else:
                date_text = message.text.strip()

            try:
                selected_date = convert_from_format_with_weekday(date_text)
            except ValueError:
                await message.answer(
                    "❌ Пожалуйста, выберите дату с клавиатуры.",
                    keyboard=self.date_keyboard(active_bookings=active_bookings),
                )
                return

            if selected_date not in self.booking_window_dates():
                await message.answer(
                    "❌ Эту дату выбрать нельзя. Попробуйте другую:",
                    keyboard=self.date_keyboard(active_bookings=active_bookings),
                )
                return

            free_times, keyboard = self.time_keyboard(selected_date=selected_date)
            if not free_times:
                await message.answer(
                    "❌ На выбранную дату нет свободных слотов.\n"
                    "Выберите другую дату или свяжитесь с администратором.",
                    keyboard=self.date_keyboard(active_bookings=active_bookings),
                )
                return

            self.context[message.from_id]["date"] = selected_date
            self.context[message.from_id]["step"] = "choose_time"
            await message.answer(
                f"Дата *{format_date_with_weekday(selected_date)}* выбрана. Теперь выберите время:",
                keyboard=keyboard,
            )

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "choose_time"
            and self.is_user(m)
        )
        async def handle_time(message: Message):
            context = self.context.get(message.from_id)
            if not context or "date" not in context:
                self.reset_context(message.from_id)
                await message.answer (
                    "Сессия бронирования сброшена. Начните заново командой «Записаться».",
                    keyboard=user_menu(),
                )
                return

            selected_date: datetime.date = context["date"]
            payload = self.extract_payload(message)
            action = payload.get("action")
            active_bookings = context.get("active_bookings")
            if active_bookings is None:
                active_bookings = get_bookings(statuses=ACTIVE_STATUSES)
                context["active_bookings"] = active_bookings

            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Главное меню:",
                    keyboard=user_menu(),
                )
                return
            
            if action == "one_step_back" and context["step"] == "choose_time":
                self.context[message.from_id].pop("date")
                self.context[message.from_id]["step"] = "choose_date"
                page = 0
                await message.answer(
                    "Выберите дату для записи:",
                    keyboard=self.date_keyboard(page=page, active_bookings=active_bookings),
                )
                return
                
            if action == "paginate" and payload.get("target") == "time":
                page = payload.get("page", 0)
                _, keyboard = self.time_keyboard(selected_date=selected_date, page=page)
                await message.answer(
                    "Выберите время:",
                    keyboard=keyboard,
                )
                return

            if action == "select" and payload.get("target") == "time":
                time_text = payload.get("value")
            else:
                time_text = message.text.strip()

            try:
                datetime.strptime(time_text, TIME_FORMAT)
            except ValueError:
                _, keyboard = self.time_keyboard(selected_date=selected_date)
                await message.answer(
                    "❌ Пожалуйста, выберите время с клавиатуры.",
                    keyboard=keyboard,
                )
                return

            if not is_time_free(selected_date, time_text):
                _, keyboard = self.time_keyboard(selected_date=selected_date)
                await message.answer(
                    "❌ Слот уже занят. Выберите другое время:",
                    keyboard=keyboard,
                )
                return

            bookings_same_day = [
                record
                for record in get_user_active_bookings(message.from_id)
                if record.get("Дата") == selected_date
            ]
            if len(bookings_same_day) >= MAX_SLOTS_PER_DAY:
                self.reset_context(message.from_id)
                await message.answer(
                    "❌ Вы достигли лимита бронирований на выбранную дату.",
                    keyboard=user_menu(),
                )
                return

            context["time"] = time_text
            context["step"] = "choose_options"
            context["options"] = []
            context["price"] = WASH_PRICES["Без добавок"]
            await message.answer(
                "Выберите дополнительные опции (по желанию):",
                keyboard=wash_options_keyboard(WASH_OPTIONS, context["price"], []),
            )

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "choose_options"
            and self.is_user(m)
        )
        async def handle_options(message: Message):
            context = self.context.get(message.from_id)
            if not context or "date" not in context or "time" not in context:
                self.reset_context(message.from_id)
                await message.answer(
                    "Сессия бронирования сброшена. Начните заново командой «Записаться».",
                    keyboard=user_menu(),
                )
                return

            payload = self.extract_payload(message)
            selected_options: List[str] = context.get("options", [])
            selected_date: datetime.date = context["date"]   
            action = payload.get("action")
            
            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Главное меню:",
                    keyboard=user_menu(),
                )
                return
            
            if action == "one_step_back":
                self.context[message.from_id].pop("time")
                self.context[message.from_id]["step"] = "choose_time"
                self.context[message.from_id].pop("options") 
                self.context[message.from_id].pop("price")

                page = 0
                await message.answer(
                    "Выберите дату для записи:",
                    keyboard=self.time_keyboard(page=page, selected_date=selected_date),
                )
                return
            
            if action == "toggle_option":
                option_value = payload.get("value")

                if option_value == WASH_PRICES["Без добавок"]:
                    selected_options.clear()
                    context["price"] = WASH_PRICES["Без добавок"]
                elif option_value in WASH_OPTIONS[1:]:
                    if option_value in selected_options:
                        selected_options.remove(option_value)
                        context["price"] -= WASH_PRICES[option_value]
                    else:
                        selected_options.append(option_value)
                        context["price"] += WASH_PRICES[option_value]
                context["options"] = selected_options
                await message.answer(
                    "Обновлённые опции:",
                    keyboard=wash_options_keyboard(WASH_OPTIONS, context["price"], selected_options),
                )
                return

            if action == "options_reset":
                selected_options.clear()
                context["price"] = WASH_PRICES["Без добавок"]
                await message.answer(
                    "Опции сброшены.",
                    keyboard=wash_options_keyboard(WASH_OPTIONS, context["price"], selected_options),
                )
                return

            if action != "options_done":
                await message.answer(
                    "Используйте кнопки, чтобы выбрать опции, или нажмите «Готово».",
                    keyboard=wash_options_keyboard(WASH_OPTIONS, selected_options),
                )
                return

            time_text: str = context["time"]
            if not is_time_free(selected_date, time_text):
                self.reset_context(message.from_id)
                await message.answer(
                    "❌ Пока вы выбирали опции, слот заняли. Попробуйте снова.",
                    keyboard=user_menu(),
                )
                return

            vk_user = (await message.ctx_api.users.get(message.from_id))[0]
            full_name = f"{vk_user.first_name} {vk_user.last_name}"
            user_link = f"https://vk.com/id{message.from_id}"

            wash_option = ", ".join(selected_options) if selected_options else "Без добавок"
            price = context["price"]

            add_booking(
                user_name=full_name,
                user_link=user_link,
                date=datetime.strftime(selected_date, DATE_FORMAT),
                time_slot=time_text,
                user_id=message.from_id,
                status=STATUS_PENDING,
                wash_option=wash_option,
            )

            admin_message = (
                "🆕 Новая заявка на стирку\n"
                f"Пользователь: {full_name} ({user_link})\n"
                f"Дата и время: {format_date_with_weekday(selected_date)} {time_text}\n"
                f"Опции: {wash_option}\n"
                f"Итоговая стоимость: {price}\n" 
                f"ID пользователя: {message.from_id}"
            )
            for admin_id in ADMIN_IDS:
                try:
                    await bot.api.messages.send(
                        peer_id=admin_id,
                        message=admin_message,
                        random_id=0,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Не удалось уведомить администратора %s: %s", admin_id, exc
                    )

            self.reset_context(message.from_id)
            await message.answer(
                "✅ Заявка отправлена на подтверждение администратору.\n"
                "Вносите оплату по номеру - +79842878451 (альфа банк) и ждите подтверждения\n"
                "Мы уведомим вас после принятия решения.",
                keyboard=user_menu(),
            )

        @self.labeler.private_message(text=["отменить запись"], func=self.is_user)
        async def cancel_booking(message: Message):
            self.reset_context(message.from_id)
            bookings = get_user_active_bookings(message.from_id)
            if not bookings:
                await message.answer(
                    "❌ У вас нет активных записей.",
                    keyboard=user_menu(),
                )
                return

            context = {
                "step": "cancel_select",
                "bookings": {str(record["_row"]): record for record in bookings},
            }
            self.context[message.from_id] = context

            details = "\n".join(self.format_booking(record) for record in bookings)
            await message.answer(
                "Выберите запись для отмены:\n"
                f"{details}",
                keyboard=paginate_buttons(bookings, target="record", buttons_per_row=1, rows_per_page=8),
            )

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "cancel_select"
            and self.is_user(m)
        )
        async def handle_cancel_selection(message: Message, page: int = 0):
            context = self.context.get(message.from_id, {})
            payload = self.extract_payload(message)
            action = payload.get("action")

            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Главное меню:",
                    keyboard=user_menu(),
                )
                return

            # if action == "booking_list_page":
            #     await handle_cancel_list_page(message, payload.get("page", 0))
            #     return
        
            bookings = get_user_active_bookings(message.from_id)
            
            if action == "paginate" and payload.get("target") == "record":
                context = self.context.get(message.from_id, {})
                context["bookings"] = bookings
                self.context[message.from_id] = context
                
                await show_cancel_page(message, bookings, payload.get("page", 0))
                return
            
            if action != "select":
                await message.answer(
                    "Выберите запись кнопкой на клавиатуре или нажмите «Вернуться в главное».",
                    keyboard=paginate_buttons(
                        list(context.get("bookings", {}).values()),
                        target="record",
                        buttons_per_row=1,
                    ),
                )
                return

            row_key = str(payload.get("row"))
            record = context.get("bookings", {}).get(row_key)
            if not record:
                self.reset_context(message.from_id)
                await message.answer(
                    "Не удалось найти запись. Попробуйте снова.",
                    keyboard=user_menu(),
                )
                return

            vk_user = (await message.ctx_api.users.get(message.from_id))[0]
            full_name = f"{vk_user.first_name} {vk_user.last_name}"
            user_link = f"https://vk.com/id{message.from_id}"
            
            admin_message = f"Пользователь {full_name} - {user_link} отменил запись {str(record['Дата'])} {str(record['Время'])}",

            for admin_id in ADMIN_IDS:
                try:
                    await bot.api.messages.send(
                        peer_id=admin_id,
                        message=admin_message,
                        random_id=0,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Не удалось уведомить администратора %s: %s", admin_id, exc
                    )

            delete_booking(record)
            self.reset_context(message.from_id)
            await message.answer(
                "✅ Запись отменена.",
                keyboard=user_menu(),
            )
            
        # async def handle_cancel_list_page(message: Message, page: int):
        #     """Обработчик смены страницы"""
            
        #     # Получаем все записи из контекста или заново
        #     context = self.context.get(message.from_id, {})
        #     bookings = context.get("bookings", get_user_active_bookings(message.from_id))
            
        #     await show_cancel_page(message, bookings, page)

        async def show_cancel_page(message: Message, bookings: list, page: int):
            """Показывает страницу с записями"""
            rows_per_page = 8
            keyboard = paginate_buttons(bookings, page=page, target="record", buttons_per_row=1,rows_per_page=rows_per_page)
            total_pages = max(1, (len(bookings) + rows_per_page-1) // rows_per_page)  # Округление вверх
            
            text = f"Список записей (страница {page + 1} из {total_pages}):\nИспользуйте кнопки для выбора записи."
            
            await message.answer(text, keyboard=keyboard)
            
            # Обновляем контекст
            context = self.context.get(message.from_id, {})
            context["bookings"] = bookings
            context["page"] = page
            self.context[message.from_id] = context


        @self.labeler.private_message(text=["мои записи"], func=self.is_user)
        async def my_bookings(message: Message):
            records = sorted(
                get_user_active_bookings(message.from_id),
                key=lambda r: (r["Дата"], r["Время"]),
            )
            if not records:
                await message.answer(
                    "❌ У вас нет активных записей.",
                    keyboard=user_menu(),
                )
                return

            lines = ["📋 Ваши записи:"]
            for record in records:
                lines.append(self.format_booking(record))
            await message.answer(
                "\n".join(lines),
                keyboard=user_menu(),
            )

        @self.labeler.private_message(
            func=lambda m: self.is_user(m)
            and not self.context.get(m.from_id, {}).get("step")
            and not self.extract_payload(m)
            and self.normalize(m.text) not in user_commands
        )
        async def fallback(message: Message):
            await message.answer(
                f"Всем привет! "
                "Это бот для записи на стирку.\n"
                "❗ Чтобы записаться, напишите в ЛС боту ❗\n\n"
                "Расценки:\n"
                "• 90 рублей — стирка со своим порошком 🤌\n"
                "Дополнительно:\n"
                "• Мой порошок — +15 руб.\n"
                "• Мой гель, кондиционер или отбеливатель — +20 руб. 💥\n\n"
                "Оплата:\n"
                "📱 +7 984 287-84-51\n"
                "💳 Альфа-Банк\n\n"
                "Где:\n"
                "🏢2 корпус, 11 этаж, 297 комната 😶‍\n\n"
                "Важно:\n"
                "• Приносите вещи за 5–10 минут заранее\n"
                "• Оставляйте на пороге (внутри)\n"
                "• Стучаться не нужно ❗❗❗\n"
                "💬 Можете писать отзывы и предложения — что добавить или убрать. Будем развиваться 🤗"
            )
            await message.answer(
                f"{self.HELP_TEXT}\nВыберите действие:",
                keyboard=user_menu())

