"""
Модуль для генерации клавиатур VK Bot.
Содержит функции для создания различных типов клавиатур: главное меню, опции стирки, пагинация и т.д.
"""
from typing import Iterable, Sequence

from vkbottle import Keyboard, Text


def main_menu(is_admin: bool = False) -> Keyboard:
    """
    Создает главное меню для пользователя или администратора.
    
    Args:
        is_admin: Если True, возвращает админ-меню, иначе пользовательское меню
        
    Returns:
        Keyboard объект с кнопками главного меню
    """
    if is_admin:
        return admin_menu()
    keyboard = Keyboard(one_time=True, inline=False)
    keyboard.add(Text("Записаться"))
    keyboard.add(Text("Мои записи"))
    keyboard.row()
    keyboard.add(Text("Отмена"))
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
    keyboard.row()
    keyboard.add(Text("Вернуться"))
    return keyboard


def wash_options_keyboard(
    options: Sequence[str],
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
    keyboard = Keyboard(inline=True)
    for option in options:
        is_selected = (
            not selected_set if option.lower().startswith("без") else option in selected_set
        )
        label = f"✅ {option}" if is_selected else option
        keyboard.add(
            Text(
                label,
                payload={"action": "toggle_option", "value": option},
            )
        )
        keyboard.row()
    keyboard.add(Text("Готово", payload={"action": "options_done"}))
    keyboard.add(Text("Сбросить", payload={"action": "options_reset"}))
    keyboard.row()
    keyboard.add(Text("Отмена", payload={"action": "options_cancel"}))
    keyboard.add(Text("Вернуться", payload={"action": "back_to_menu"}))
    return keyboard


def cancellation_keyboard(bookings: Sequence[dict]) -> Keyboard:
    """
    Создает клавиатуру для выбора записи для отмены.
    
    Args:
        bookings: Список записей для отмены
        
    Returns:
        Keyboard объект с кнопками записей
    """
    keyboard = Keyboard(inline=True)
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
    keyboard.add(Text("Вернуться", payload={"action": "back_to_menu"}))
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
    keyboard.row()
    keyboard.add(Text("Вернуться", payload={"action": "back_to_menu"}))
    return keyboard


def unblock_keyboard(blockings: Sequence[dict]) -> Keyboard:
    """
    Создает клавиатуру для выбора заблокированного слота для разблокировки.
    
    Args:
        blockings: Список заблокированных слотов
        
    Returns:
        Keyboard объект с кнопками слотов
    """
    keyboard = Keyboard(inline=True)
    for record in blockings:
        label = f"{record['Дата']} {record['Время']}"
        keyboard.add(
            Text(
                label,
                payload={"action": "admin_unblock", "row": record["_row"]},
            )
        )
        keyboard.row()
    keyboard.add(Text("Вернуться", payload={"action": "back_to_menu"}))
    return keyboard


def booking_list_keyboard(bookings: Sequence[dict], page: int = 0, page_size: int = 6) -> Keyboard:
    """
    Клавиатура для списка записей с пагинацией.
    """
    keyboard = Keyboard(inline=False)
    
    # Рассчитываем элементы для текущей страницы
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_bookings = bookings[start_idx:end_idx]
    
    # Добавляем кнопки записей текущей страницы
    for booking in page_bookings:
        label = f"{booking['Дата']} {booking['Время']} - {booking['Пользователь']}"
        keyboard.add(
            Text(
                label,
                payload={"action": "admin_complete_booking", "row": booking["_row"]}
            )
        )
        keyboard.row()
    
    # Добавляем кнопки пагинации
    has_prev = page > 0
    has_next = end_idx < len(bookings)
    
    if has_prev or has_next:
        if has_prev:
            keyboard.add(
                Text("← Назад", payload={"action": "booking_list_page", "page": page - 1})
            )
        
        if has_next:
            keyboard.add(
                Text("Вперед →", payload={"action": "booking_list_page", "page": page + 1})
            )
        
        keyboard.row()
    
    # Кнопка возврата
    keyboard.add(Text("Вернуться", payload={"action": "back_to_menu"}))
    
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
        payload = {"action": "select", "target": target, "value": str(item)}
        keyboard.add(Text(str(item), payload=payload))
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
                    "← Назад",
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
    keyboard.add(Text("Вернуться", payload={"action": "back_to_menu"}))
    return keyboard