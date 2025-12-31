"""
Модуль обработчиков команд для администраторов.
Обрабатывает подтверждение/отклонение заявок, блокировку слотов, управление черным списком и т.д.
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
    DATE_FORMAT,
    TIME_FORMAT,
    format_date_with_weekday,
    convert_from_format_with_weekday,
)
from google_sheets import (
    STATUS_BLOCKED,
    STATUS_CONFIRMED,
    add_booking,
    complete_booking,
    delete_booking,
    set_booking_confirmed,
    get_admin_blockings,
    get_blacklist,
    get_bookings,
    get_pending_bookings,
    is_time_free,
    add_blacklist,
    remove_blacklist,
)
from keyboards import (
    admin_menu,
    pending_decision_keyboard,
    back_to_menu_keyboard,
    paginate_buttons,
)

class Admin(Role):
    def __init__(self, bot: Bot):
        self.labeler = BotLabeler()
        self.labeler.vbml_ignore_case = True
        self.register(bot)
        bot.labeler.load(self.labeler)
    
    def format_booking(self, record: Dict[str, str]) -> str:
        option = record.get("Опция стирки") or "Без добавок"
        return f"{record['Дата']} {record['Время']} — {record['Пользователь']}/{record['Ссылка']} ({option})"

    
    def register(self, bot: Bot):
        admin_commands = {
            "неподтвержденные",
            "список записей",
            "блокировать слот",
            "разблокировать слот",
            "черный список",
            "+ в черный список",
            "- из черного списка",
            "назад",
        }
        
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
                self.logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {exc}")

        async def finalize_rejection(
            message: Message,
            record: Dict[str, str],
            reason: str,
        ) -> None:
            # Удаляем запись
            updated = delete_booking(record)
            if updated:
                display_reason = reason if reason else "не указана"
                await message.answer(
                    f"❌ Заявка отклонена. Причина: {display_reason}",
                    keyboard=admin_menu(),
                )

                await send_user_notification(
                    record.get("Пользователь_ID"),
                    "❌ Ваша запись отклонена.\n"
                    f"Дата: {record['Дата']} {record['Время']}\n"
                    f"Причина: {display_reason}",
                )

                self.reset_context(message.from_id)
            else:
                await message.answer(
                    f"❗Ошибка отклонения заявки.\n Возможно она не существует, проверьте наличие записи в таблице и попробуйте еще раз",
                    keyboard=admin_menu(),
                )

        # @self.labeler.private_message(text=["Админ меню"], func=self.is_admin)
        # async def show_admin_menu(message: Message):
        #     await message.answer(
        #         "Админ меню:",
        #         keyboard=admin_menu(),
        #     )

        @self.labeler.private_message(text=["неподтвержденные"], func=self.is_admin)
        async def pending_list(message: Message):
            records = get_pending_bookings()
            if not records:
                await message.answer("📭 Нет заявок, ожидающих подтверждения.")
                return
            
            await message.answer(
                'Если хотите вернуться, нажмите на кнопку "Вернуться в главное меню"',
                keyboard=back_to_menu_keyboard())
            
            for record in records:
                self.context[message.from_id] = {"step": "confirm_records"}
                date = format_date_with_weekday(datetime.strptime(record['Дата'], DATE_FORMAT).date())
                details = (
                    f"Заявка №{record['_row']}:\n"
                    f"Дата: {date} {record['Время']}\n"
                    f"Пользователь: {record['Пользователь']} ({record['Ссылка']})\n"
                    f"Опции: {record.get('Опция стирки') or 'Без добавок'}"
                )
                await message.answer(
                    details,
                    keyboard=pending_decision_keyboard(record["_row"]),
                )
                
        
        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "confirm_records"
            and self.is_admin(m)
        )
        async def handle_confirm_records(message: Message):
            payload = self.extract_payload(message)
            action = payload.get("action")
            if not action:
                return
            
            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Админ меню:",
                    keyboard=admin_menu(),
                )
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
                    f"✅ Заявка подтверждена.\n{self.format_booking(updated)}",
                    keyboard=back_to_menu_keyboard(),
                )
                await send_user_notification(
                    updated.get("Пользователь_ID"),
                    "✅ Ваша запись подтверждена!\n"
                    f"Дата: {updated['Дата']} {updated['Время']}\n"
                    f"Опции: {updated.get('Опция стирки') or 'Без добавок'}",
                )
                return

            if action == "admin_reject":
                row = str(payload.get("row"))
                record = next(
                    (r for r in get_pending_bookings() if str(r["_row"]) == row),
                    None,
                )
                if not record:
                    await message.answer("❌ Заявка уже обработана или не найдена.")
                    return

                self.context[message.from_id] = {
                    "step": "reject_reason",
                    "record": record,
                }
                await message.answer(
                    "Укажите причину отказа для пользователя "
                    f"{record['Пользователь']} ({record['Дата']} {record['Время']}):"
                )
                return
            
        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "reject_reason"
            and self.is_admin(m)
        )
        async def reject_record(message: Message):
            payload = self.extract_payload(message)
            action = payload.get("action")
            
            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Админ меню:",
                    keyboard=admin_menu(),
                )
                return
            
            context = self.context.get(message.from_id)
            
            if context and context.get("step") == "reject_reason" and context.get("record"):
                record = context["record"]
                await finalize_rejection(
                    message, record, reason=message.text
                )
                return     

        @self.labeler.private_message(text=["список записей"], func=self.is_admin)
        async def show_bookings(message: Message):
            # Показываем только подтвержденные записи для завершения
            bookings = get_bookings(statuses={STATUS_CONFIRMED})

            if not bookings:
                await message.answer(
                    "Список подтвержденных записей пуст.",
                    keyboard=admin_menu(),
                )
                return
            
            self.context[message.from_id] = {
                "step": "booking_list",
                "bookings": {str(record["_row"]): record for record in bookings},
            }
            
            chunks: List[str] = []
            current_chunk: List[str] = []
            for record in bookings:
                entry = self.format_booking(record)
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
                        keyboard=paginate_buttons(bookings, target="record", buttons_per_row=1, rows_per_page=8),
                    )
                else:
                    await message.answer(f"📋 Записи:\n{chunk}")
        
        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "booking_list"
            and self.is_admin(m)
        )
        async def handle_booking_list_selection(message: Message, page: int = 0):            
            payload = self.extract_payload(message)
            action = payload.get("action")
            
            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Админ меню:",
                    keyboard=admin_menu(),
                )
                return
            
            # # Обработка пагинации
            # if action == "booking_list_page":
            #     await handle_booking_list_page(message, payload.get("page", 0))
            #     return
        
            
            # Получаем ВСЕ записи для контекста
            bookings = get_bookings(statuses={STATUS_CONFIRMED})
            
            if action != "admin_complete_booking":
                # Сохраняем все записи в контекст для последующего использования
                context = self.context.get(message.from_id, {})
                context["bookings"] = bookings
                self.context[message.from_id] = context
                
                # Показываем первую страницу
                await show_booking_page(message, bookings, payload.get("page", 0))
                return
            
            # Обработка завершения записи
            row_key = str(payload.get("row"))
            
            # Ищем запись во всех бронированиях
            target_booking = None
            for booking in bookings:
                if str(booking.get("_row")) == row_key:
                    target_booking = booking
                    break
            
            if not target_booking:
                self.reset_context(message.from_id)
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
                    self.logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {exc}")
            
            # Удаляем запись
            complete_booking(target_booking)
            self.reset_context(message.from_id)
            
            await message.answer(
                f"✅ Запись завершена и удалена из таблицы.\n"
                f"Клиент {target_booking.get('Пользователь', 'неизвестен')} уведомлен.",
                keyboard=admin_menu(),
            )

        # async def handle_booking_list_page(message: Message, page: int):
        #     """Обработчик смены страницы"""
            
        #     # Получаем все записи из контекста или заново
        #     context = self.context.get(message.from_id, {})
        #     all_bookings = context.get("bookings", get_bookings(statuses={STATUS_CONFIRMED}))
            
        #     await show_booking_page(message, all_bookings, page)

        async def show_booking_page(message: Message, all_bookings: list, page: int):
            """Показывает страницу с записями"""
            rows_per_page = 8
            keyboard = paginate_buttons(all_bookings, target="record", page=page,buttons_per_row=1, rows_per_page=rows_per_page)
            total_pages = max(1, (len(all_bookings) + rows_per_page-1) // rows_per_page)  # Округление вверх
            
            text = f"Список записей (страница {page + 1} из {total_pages}):\nИспользуйте кнопки для выбора записи."
            
            await message.answer(text, keyboard=keyboard)
            
            # Обновляем контекст
            context = self.context.get(message.from_id, {})
            context["bookings"] = all_bookings
            context["page"] = page
            self.context[message.from_id] = context

        @self.labeler.private_message(text=["Блокировать слот"], func=self.is_admin)
        async def start_block_slot(message: Message):
            self.context[message.from_id] = {"step": "block_date"}
            await message.answer(
                "Выберите дату для блокировки:",
                keyboard=self.date_keyboard(),
            )

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "block_date"
        )
        async def handle_block_date(message: Message):
            payload = self.extract_payload(message)
            action = payload.get("action")
            
            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Главное меню:",
                    keyboard=admin_menu(),
                )
                return
            
            context = self.context.get(message.from_id)
            active_bookings = context.get("active_bookings")
            if action == "paginate" and payload.get("target") == "date":
                page = payload.get("page", 0)
                await message.answer(
                    "Выберите дату для блокировки:",
                    keyboard=self.date_keyboard(active_bookings=active_bookings, page=page),
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
                    "❌ Неверный формат даты. Используйте YYYY-MM-DD.",
                    keyboard=self.date_keyboard(),
                )
                return

            self.context[message.from_id]["date"] = selected_date
            self.context[message.from_id]["step"] = "block_time"
            _, keyboard = self.time_keyboard(selected_date=selected_date)
            await message.answer(
                f"Дата {format_date_with_weekday(selected_date)} выбрана. Теперь выберите время:",
                keyboard=keyboard,
            )

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "block_time"
            and self.is_admin(m)
        )
        async def handle_block_time(message: Message):
            context = self.context.get(message.from_id)
            payload = self.extract_payload(message)
            action = payload.get("action")
            
            selected_date: datetime.date = context["date"] 
            if not selected_date:
                self.reset_context(message.from_id)
                await message.answer("Сессия прервана. Начните заново.")
                return
            
            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Главное меню:",
                    keyboard=admin_menu(),
                )
                return
            
            active_bookings = context.get("active_bookings")
            if action == "one_step_back":
                self.context[message.from_id].pop("date")
                self.context[message.from_id]["step"] = "block_date"
                page = 0
                await message.answer(
                    "Выберите дату для записи:",
                    keyboard=self.date_keyboard(page=page, active_bookings=active_bookings),
                )
                return

            if action == "paginate" and payload.get("target") == "time":
                page = payload.get("page", 0)
                _, keyboard = self.time_keyboard(selected_date=selected_date, active_bookings=active_bookings, page=page)
                await message.answer(
                    "Выберите время для блокировки:",
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
                    "❌ Неверный формат времени. Используйте HH:MM.",
                    keyboard=keyboard,
                )
                return

            if not is_time_free(selected_date, time_text):
                _, keyboard = self.time_keyboard(selected_date=selected_date)
                await message.answer(
                    "❌ Слот уже занят или забронирован.",
                    keyboard=keyboard,
                )
                return

            add_booking(
                user_name="Блокировка администратора",
                user_link="admin_blocked",
                date=datetime.strftime(selected_date, DATE_FORMAT),
                time_slot=time_text,
                user_id=None,
                status=STATUS_BLOCKED,
                wash_option="Блокировка",
                confirmed_by="Администратор",
                confirmed_at=datetime.utcnow().isoformat(),
            )
            self.reset_context(message.from_id)
            await message.answer(
                f"✅ Слот {format_date_with_weekday(selected_date)} {time_text} заблокирован.",
                keyboard=admin_menu(),
            )

        @self.labeler.private_message(text=["Разблокировать слот"], func=self.is_admin)
        async def start_unblock(message: Message):
            bookings = get_admin_blockings()
            if not bookings:
                await message.answer("Нет заблокированных слотов.")
                return
            self.context[message.from_id] = {
                "step": "unblock_select",
                "bookings": {str(record["_row"]) : record for record in bookings},
            }
            await message.answer(
                "Выберите слот, который нужно разблокировать:",
                keyboard=paginate_buttons(bookings, target="record", buttons_per_row=1),
            )

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step") == "unblock_select"
            and self.is_admin(m)
        )
        async def handle_unblock_selection(message: Message, page: int = 0):
            payload = self.extract_payload(message)
            action = payload.get("action")

            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Разблокировка отменена.",
                    keyboard=admin_menu(),
                )
                return
            
            # if action == "booking_list_page":
            #     await handle_blockings_list_page(message, payload.get("page", 0))
            #     return
        
            bookings = get_admin_blockings()
            
            if action == "paginate" and payload.get("target") == "record":
                context = self.context.get(message.from_id, {})
                context["bookings"] = bookings
                self.context[message.from_id] = context
                
                await show_booking_page(message, bookings, payload.get("page", 0))
                return
            
            context = self.context.get(message.from_id, {})
            if action != "select":
                blockings = bookings
                await message.answer(
                    "Используйте кнопки клавиатуры, чтобы выбрать слот.",
                    keyboard=paginate_buttons(blockings),
                )
                return

            row_key = str(payload.get("row"))
            record = context.get("bookings", {}).get(row_key)
            if not record:
                self.reset_context(message.from_id)
                await message.answer("Не удалось найти слот. Попробуйте снова.")
                return
            
            self.reset_context(message.from_id)
            delete_booking(record)
            await message.answer(
                "✅ Слот разблокирован.",
                keyboard=admin_menu(),
            )
        
        # async def handle_blockings_list_page(message: Message, page: int):
        #     """Обработчик смены страницы"""
            
        #     # Получаем все записи из контекста или заново
        #     context = self.context.get(message.from_id, {})
        #     bookings = context.get("bookings", get_admin_blockings())
            
        #     await show_blockings_page(message, bookings, page)

        async def show_blockings_page(message: Message, all_bookings: list, page: int):
            """Показывает страницу с записями"""
            rows_per_page = 5
            keyboard = paginate_buttons(all_bookings, page=page, rows_per_page=rows_per_page)
            total_pages = max(1, (len(all_bookings) + rows_per_page-1) // rows_per_page)  # Округление вверх
            
            text = f"Список записей (страница {page + 1} из {total_pages}):\nИспользуйте кнопки для выбора записи."
            
            await message.answer(text, keyboard=keyboard)
            
            # Обновляем контекст
            context = self.context.get(message.from_id, {})
            context["bookings"] = all_bookings
            context["page"] = page
            self.context[message.from_id] = context


        @self.labeler.private_message(text=["черный список"], func=self.is_admin)
        async def request_blacklist(message: Message):
            blacklist = await get_blacklist()
            if blacklist:
                for user in blacklist:
                    await message.answer(user)
            else:
                await message.answer("Черный список пуст")

            

        @self.labeler.private_message(text=["+ в черный список"], func=self.is_admin)
        async def request_blacklist_add(message: Message):
            self.context[message.from_id] = {"step": "blacklist_add"}
            await message.answer("Отправьте ссылку пользователя для добавления в черный список.")

        @self.labeler.private_message(text=["- из черного списка"], func=self.is_admin)
        async def request_blacklist_remove(message: Message):
            self.context[message.from_id] = {"step": "blacklist_remove"}
            await message.answer("Отправьте ссылку пользователя для удаления из черного списка.")

        @self.labeler.private_message(
            func=lambda m: self.context.get(m.from_id, {}).get("step")
            in {"blacklist_add", "blacklist_remove"}
            and self.is_admin(m)
        )
        async def handle_blacklist_input(message: Message):
            payload = self.extract_payload(message)
            action = payload.get("action")
            
            if action == "back_to_menu":
                self.reset_context(message.from_id)
                await message.answer(
                    "Разблокировка отменена.",
                    keyboard=admin_menu(),
                )
                return
            
            step = self.context.get(message.from_id, {}).get("step")
            link = message.text
            if step == "blacklist_add":
                if await add_blacklist(bot.api, link):
                    self.reset_context(message.from_id)
                    await message.answer(
                        f"✅ Пользователь {link} добавлен в черный список.",
                        keyboard=admin_menu(),
                    )
                else:
                    await message.answer(
                        f"❓Пользователь с таким id - {link} не существует",
                        keyboard=admin_menu(),
                    )
            elif step == "blacklist_remove":
                removed = remove_blacklist(link)
                self.reset_context(message.from_id)
                if removed:
                    await message.answer(
                        f"✅ Пользователь {link} удален из черного списка.",
                        keyboard=admin_menu(),
                    )
                else:
                    await message.answer(
                        "❌ Пользователь не найден в черном списке.",
                        keyboard=admin_menu(),
                    )
            else:
                await message.answer(
                    "Сессия истекла. Начните заново.",
                    keyboard=admin_menu(),
                )
        @self.labeler.private_message(
            func=lambda m: self.is_admin(m)
            and not self.context.get(m.from_id, {}).get("step")
            and not self.extract_payload(m)
            and self.normalize(m.text) not in admin_commands
        )
        async def admin_fallback(message: Message):
            await message.answer("Админ меню:", keyboard=admin_menu())

        # @self.labeler.private_message(func=self.is_admin)
        # async def handle_admin_payloads(message: Message):

