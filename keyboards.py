"""
Модуль для генерации клавиатур VK Bot.
Содержит функции для создания различных типов клавиатур: главное меню, опции стирки, пагинация и т.д.
"""
from typing import Iterable, Sequence

from vkbottle import Keyboard, Text

from config import WASH_PRICES


def user_menu() -> Keyboard:
    """
    Создает главное меню для пользователя или администратора.
        
    Returns:
        Keyboard объект с кнопками юзер-меню
    """

    keyboard = Keyboard(one_time=False, inline=False)
    keyboard.add(Text("Записаться"))
    keyboard.add(Text("Мои записи"))
    keyboard.row()
    keyboard.add(Text("Отменить запись"))
    keyboard.add(Text("Связаться с админом"))
    return keyboard


def admin_menu() -> Keyboard:
    """
    Создает меню администратора с опциями управления записями.
    
    Returns:
        Keyboard объект с кнопками админ-меню
    """
    keyboard = Keyboard(one_time=False, inline=False)
    keyboard.add(Text("Неподтвержденные"))
    keyboard.add(Text("Список записей"))
    keyboard.row()
    keyboard.add(Text("Блокировать слот"))
    keyboard.add(Text("Разблокировать слот"))
    keyboard.row()
    keyboard.add(Text("Черный список"))
    keyboard.row()
    keyboard.add(Text("+ в черный список"))
    keyboard.add(Text("- из черного списка"))
    return keyboard


def wash_options_keyboard(
    options: Sequence[str],
    price: int,
    selected: Iterable[str],
) -> Keyboard:
    """
    Создает клавиатуру для выбора опций стирки.
    
    Args:
        options: Список доступных опций
        selected: Список выбранных опций
        
    Returns:
        Keyboard объект с кнопками опций
    """
    selected_set = set(selected)
    keyboard = Keyboard(one_time=True, inline=False)
    count = 0
    for option in options:
        is_selected = (
            not selected_set if option.lower().startswith("без") else option in selected_set
        )
        
        if option == "Без добавок":
            label = f"✅ {option}" if is_selected else option
        else:
            label = f"✅ {option} -{WASH_PRICES[option]} руб." if is_selected else f"{option} +{WASH_PRICES[option]} руб."

        keyboard.add(
            Text(
                label,
                payload={"action": "toggle_option", "value": option},
            )
        )
        if count % 2 == 0:
            keyboard.row()
        count += 1

    keyboard.add(Text(f"Готово - {price} руб.", payload={"action": "options_done"}))
    keyboard.add(Text("Сбросить", payload={"action": "options_reset"}))
    keyboard.row()
    keyboard.add(Text("Назад", payload={"action": "one_step_back"}))
    keyboard.add(Text("Вернуться в главное меню", payload={"action": "back_to_menu"}))
    return keyboard


def cancellation_keyboard(bookings: Sequence[dict]) -> Keyboard:
    """
    Создает клавиатуру для выбора записи для отмены.
    
    Args:
        bookings: Список записей для отмены
        
    Returns:
        Keyboard объект с кнопками записей
    """
    keyboard = Keyboard(inline=False)
    for booking in bookings:
        label = f"{booking['Дата']} {booking['Время']}"
        keyboard.add(
            Text(
                label,
                payload={
                    "action": "cancel_booking",
                    "row": booking["_row"],
                },
            )
        )
        keyboard.row()
    keyboard.add(Text("Вернуться в главное меню", payload={"action": "back_to_menu"}))
    return keyboard


def pending_decision_keyboard(row: int) -> Keyboard:
    """
    Создает клавиатуру для подтверждения/отклонения заявки администратором.
    
    Args:
        row: Номер строки записи в таблице
        
    Returns:
        Keyboard объект с кнопками подтверждения/отклонения
    """
    keyboard = Keyboard(inline=True)
    keyboard.add(
        Text(
            "✅ Подтвердить",
            payload={"action": "admin_confirm", "row": row},
        )
    )
    keyboard.add(
        Text(
            "❌ Отклонить",
            payload={"action": "admin_reject", "row": row},
        )
    )
    return keyboard


def unblock_keyboard(blockings: Sequence[dict]) -> Keyboard:
    """
    Создает клавиатуру для выбора заблокированного слота для разблокировки.
    
    Args:
        blockings: Список заблокированных слотов
        
    Returns:
        Keyboard объект с кнопками слотов
    """
    keyboard = Keyboard(one_time=False, inline=False)
    for record in blockings:
        label = f"{record['Дата']} {record['Время']}"
        keyboard.add(
            Text(
                label,
                payload={"action": "admin_unblock", "row": record["_row"]},
            )
        )
        keyboard.row()
    keyboard.add(Text("Вернуться в главное меню", payload={"action": "back_to_menu"}))
    return keyboard



def booking_list_keyboard(
    bookings: Sequence[dict],
    page: int = 0,
    buttons_per_row: int = 1,
    rows_per_page: int = 5,
) -> Keyboard:
    """
    Клавиатура для списка записей с пагинацией.
    """
    start_idx = page * buttons_per_row * rows_per_page
    end_idx = start_idx + buttons_per_row * rows_per_page
    page_items = bookings[start_idx:end_idx]
    
    keyboard = Keyboard(one_time=True, inline=False)
    for idx, item in enumerate(page_items):
        label = f"{item['Дата']} {item['Время']}\n{item['Пользователь']}"
        keyboard.add(
            Text(
                label,
                payload={"action": "admin_complete_booking", "row": item["_row"]},
            )
        )
        keyboard.row()
        
    
    has_prev = start_idx > 0
    has_next = end_idx < len(bookings)

    if has_prev or has_next:
        if has_prev:
            keyboard.add(
                Text(
                    "← Предыдущие",
                    payload={"action": "booking_list_page", "page": page - 1},
                )
            )
        if has_next:
            keyboard.add(
                Text(
                    "Следующие →",
                    payload={"action": "booking_list_page", "page": page + 1},
                )
            )
        keyboard.row()

    keyboard.add(Text("Вернуться в главное меню", payload={"action": "back_to_menu"}))
         
    return keyboard



def paginate_buttons(
    items,
    target: str,
    page: int = 0,
    buttons_per_row: int = 4,
    rows_per_page: int = 10,
) -> Keyboard:
    """
    Генерирует клавиатуру VK с пагинацией.

    :param items: Список элементов, которые нужно отобразить
    :param target: Идентификатор типа элементов (дата/время и т.д.)
    :param page: Номер страницы (с нуля)
    :param buttons_per_row: Количество кнопок в строке
    :param rows_per_page: Количество строк на странице
    """
    start_idx = page * buttons_per_row * rows_per_page
    end_idx = start_idx + buttons_per_row * rows_per_page
    page_items = items[start_idx:end_idx]

    keyboard = Keyboard(one_time=True, inline=False)
    for idx, item in enumerate(page_items):
        payload = {"action": "select", "target": target, "value": item}
        keyboard.add(Text(item, payload=payload))
        is_row_end = (idx + 1) % buttons_per_row == 0
        has_more_buttons = idx + 1 < len(page_items)
        if is_row_end and has_more_buttons:
            keyboard.row()

    has_prev = start_idx > 0
    has_next = end_idx < len(items)

    if has_prev or has_next:
        keyboard.row()
        if has_prev:
            keyboard.add(
                Text(
                    "← Предыдущие",
                    payload={"action": "paginate", "target": target, "page": page - 1},
                )
            )
        if has_next:
            keyboard.add(
                Text(
                    "Следующие →",
                    payload={"action": "paginate", "target": target, "page": page + 1},
                )
            )
    keyboard.row()
    if target == "time":
        keyboard.add(Text("Назад", payload={"action": "one_step_back"}))
    keyboard.add(Text("Вернуться в главное меню", payload={"action": "back_to_menu"}))
    return keyboard