"""
Модуль для работы с Google Sheets API.
Обеспечивает взаимодействие с таблицами для хранения записей, расписания и черного списка.
Использует кеширование для оптимизации производительности.
"""
#https://docs.google.com/spreadsheets/d/1s9zB97Qxnp1YpoJMlB9wbRAk_b51D-9cDfBcsQvQu9g/edit?gid=0#gid=0
import asyncio
from datetime import datetime, timedelta, timezone
import time
import logging
import re
from typing import Dict, Iterable, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from vkbottle.api import API

from cache import (
    get_cached_blacklist,
    get_cached_bookings,
    get_cached_schedule,
    invalidate_blacklist_cache,
    invalidate_bookings_cache,
    invalidate_schedule_cache,
)
from config import (
    SPREADSHEET_NAME,
    DATE_FORMAT,
    DATETIME_FORMAT
)

# Путь к JSON-файлу сервисного аккаунта Google
SERVICE_ACCOUNT_FILE = "credentials.json"

# Области доступа Google Sheets и Drive
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Константы для листов и заголовков
LIST_SHEET_NAME = "List"
BLACKLIST_SHEET_NAME = "Blacklist"
SCHEDULE_SHEET_NAME = "Schedule"

BOOKING_HEADER = [
    "Пользователь",
    "Ссылка",
    "Дата",
    "Время",
    "Статус",
    "Пользователь_ID",
    "Создано",
    "Опция стирки",
    "Подтвердил",
    "Подтверждено в",
    "Причина отказа",
]

BLACKLIST_HEADER = ["Ссылка"]

SCHEDULE_HEADER = [
    "День недели",
    "Начало",
    "Конец",
]

STATUS_PENDING = "На подтверждении"
STATUS_CONFIRMED = "Подтвержден"
STATUS_REJECTED = "Отказан"
STATUS_BLOCKED = "Заблокировано администратором"

ACTIVE_STATUSES = {STATUS_PENDING, STATUS_CONFIRMED, STATUS_BLOCKED}

logger = logging.getLogger(__name__)

# Глобальные переменные для lazy инициализации
_credentials = None
_gc = None
_drive_service = None
_spreadsheet = None
_list_sheet = None
_blacklist_sheet = None
_schedule_sheet = None
_initialized = False
_init_lock = asyncio.Lock()


def _ensure_header(ws: gspread.Worksheet, expected_header: List[str]) -> None:
    """
    Проверяет и обновляет заголовок листа, если он не соответствует ожидаемому.

    Args:
        ws: Рабочий лист Google Sheets
        expected_header: Ожидаемый список заголовков
    """
    try:
        current_header = ws.row_values(1)
        if current_header != expected_header:
            end_column = chr(ord("A") + len(expected_header) - 1)
            ws.update(f"A1:{end_column}1", [expected_header])
    except Exception as e:
        logger.warning(f"Не удалось проверить/обновить заголовок листа: {e}")


async def _init_google_sheets() -> bool:
    """
    Инициализирует подключение к Google Sheets (lazy initialization).
    Вызывается автоматически при первом обращении к данным.

    Returns:
        True если инициализация успешна, False в случае ошибки
    """
    global _credentials, _gc, _drive_service, _spreadsheet
    global _list_sheet, _blacklist_sheet, _schedule_sheet, _initialized

    async with _init_lock:
        # Если уже инициализировано, просто возвращаем True
        if _initialized:
            return True

        try:
            logger.info("Инициализация подключения к Google Sheets...")

            # Выполняем инициализацию в executor, чтобы не блокировать event loop
            def init():
                global _credentials, _gc, _drive_service, _spreadsheet
                global _list_sheet, _blacklist_sheet, _schedule_sheet

                # Авторизация
                _credentials = Credentials.from_service_account_file(
                    SERVICE_ACCOUNT_FILE,
                    scopes=SCOPES,
                )

                _gc = gspread.authorize(_credentials)
                _drive_service = build("drive", "v3", credentials=_credentials)

                # Открываем таблицу и листы
                _spreadsheet = _gc.open(SPREADSHEET_NAME)
                _list_sheet = _spreadsheet.worksheet(LIST_SHEET_NAME)
                _blacklist_sheet = _spreadsheet.worksheet(BLACKLIST_SHEET_NAME)
                _schedule_sheet = _spreadsheet.worksheet(SCHEDULE_SHEET_NAME)

                # Проверяем заголовки
                _ensure_header(_list_sheet, BOOKING_HEADER)
                _ensure_header(_blacklist_sheet, BLACKLIST_HEADER)
                _ensure_header(_schedule_sheet, SCHEDULE_HEADER)

            # Выполняем в отдельном потоке
            await asyncio.get_event_loop().run_in_executor(None, init)

            _initialized = True
            logger.info("✅ Google Sheets успешно инициализирован")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Google Sheets: {e}")
            logger.error(
                "Проверьте наличие файла credentials.json и подключение к интернету"
            )
            return False


def _get_sheets():
    """
    Возвращает объекты листов. Используется внутренними функциями.
    Предполагается, что инициализация уже выполнена.
    """
    if not _initialized:
        raise RuntimeError(
            "Google Sheets не инициализирован. "
            "Вызовите await _init_google_sheets() перед использованием."
        )
    return _list_sheet, _blacklist_sheet, _schedule_sheet, _spreadsheet, _drive_service


def with_retries(loader, retries=5, base_delay=0.5):
    def wrapped():
        delay = base_delay
        for attempt in range(retries):
            try:
                return loader()
            except Exception as e:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)
                delay *= 2
        return loader()
    return wrapped



# -------------------------
# Проверка изменений в Google Sheets
# -------------------------

def _get_sheet_modified_time() -> Optional[datetime]:
    """
    Получает время последнего изменения таблицы через Drive API.

    Returns:
        Время последнего изменения или None при ошибке
    """
    if not _initialized:
        return None

    try:
        _, _, _, spreadsheet, drive_service = _get_sheets()
        # Получаем ID файла таблицы
        file_id = spreadsheet.id

        # Получаем информацию о файле
        file_metadata = drive_service.files().get(
            fileId=file_id,
            fields="modifiedTime"
        ).execute()

        modified_time_str = file_metadata.get("modifiedTime")
        if modified_time_str:
            # Парсим время в формате ISO 8601
            # Убираем 'Z' и добавляем '+00:00' для правильного парсинга
            modified_time_str = modified_time_str.replace("Z", "+00:00")
            return datetime.fromisoformat(modified_time_str)
    except Exception as e:
        logger.warning(f"Не удалось получить время изменения таблицы: {e}")
    return None


# Храним время последнего изменения таблицы
_last_modified_time: Optional[datetime] = None


async def check_sheet_changes() -> None:
    """
    Проверяет изменения в Google Sheets и инвалидирует кеш при необходимости.
    Вызывается периодически для автоматического обновления кеша при ручных изменениях в таблице.
    """
    global _last_modified_time

    current_time = _get_sheet_modified_time()

    if current_time is None:
        # Если не удалось получить время, пропускаем проверку
        return

    # Если это первая проверка, просто сохраняем время
    if _last_modified_time is None:
        _last_modified_time = current_time
        return

    # Если время изменилось, значит были изменения в таблице
    if current_time > _last_modified_time:
        logger.info(
            f"Обнаружены изменения в Google Sheets "
            f"(было: {_last_modified_time}, стало: {current_time}), "
            f"инвалидируем весь кеш"
        )
        # Инвалидируем все кеши, так как мы не знаем, какой именно лист изменился
        await invalidate_bookings_cache()
        await invalidate_blacklist_cache()
        await invalidate_schedule_cache()
        _last_modified_time = current_time


def _fetch_records(sheet) -> List[Dict[str, str]]:
    """
    Извлекает все записи из листа Google Sheets.
    
    Args:
        sheet: Рабочий лист Google Sheets
        
    Returns:
        Список словарей с данными записей, каждая запись содержит поле "_row" с номером строки
    """
    values = sheet.get_all_values()
    if not values:
        return []
    header = values[0]
    records: List[Dict[str, str]] = []
    for idx, row in enumerate(values[1:], start=2):
        if not any(cell.strip() for cell in row):
            continue
        record = {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}
        record["_row"] = idx  # техническое поле для обновлений
        records.append(record)
    return records


def _row_range(row_number: int) -> str:
    end_column = chr(ord("A") + len(BOOKING_HEADER) - 1)
    return f"A{row_number}:{end_column}{row_number}"


def _values_from_record(record: Dict[str, str]) -> List[str]:
    return [record.get(column, "") for column in BOOKING_HEADER]


def _filter_records(
    records: Iterable[Dict[str, str]],
    *,
    date: Optional[datetime.date] = None,
    user_id: Optional[int] = None,
    statuses: Optional[Iterable[str]] = None,
) -> List[Dict[str, str]]:
    filtered = []
    date_str = str(date) if date else None
    user_id_str = str(user_id) if user_id is not None else None
    statuses_set = set(statuses) if statuses else None
    for record in records:
        if date_str and record.get("Дата") != date_str:
            continue
        if user_id_str and record.get("Пользователь_ID") != user_id_str:
            continue
        if statuses_set and record.get("Статус") not in statuses_set:
            continue
        filtered.append(record)
    return filtered


# -------------------------
# Работа с бронированиями
# -------------------------
async def get_bookings(
    *,
    date: Optional[datetime.date] = None,
    user_id: Optional[int] = None,
    statuses: Optional[Iterable[str]] = None,
) -> List[Dict[str, str]]:
    """
    Получает список бронирований с возможностью фильтрации.
    Использует кеширование для оптимизации производительности.

    Args:
        date: Фильтр по дате (опционально)
        user_id: Фильтр по ID пользователя (опционально)
        statuses: Фильтр по статусам (опционально)

    Returns:
        Список записей, соответствующих фильтрам
    """
    # Инициализируем подключение при первом использовании
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return []

    list_sheet, _, _, _, _ = _get_sheets()

    # Загружаем все бронирования из кеша или из таблицы
    def loader():
        return _fetch_records(list_sheet)

    if date:
        date_str = str(datetime.strftime(date, DATE_FORMAT))
    else:
        date_str = None

    if statuses is None:
        statuses_tuple = None
    elif isinstance(statuses, str):
        statuses_tuple = (statuses,)
    else:
        statuses_tuple = tuple(statuses)


    all_records = await get_cached_bookings(
        loader=with_retries(loader),
        date=date_str,
        user_id=user_id,
        statuses=statuses_tuple,
    )

    return all_records


async def add_booking(
    user_name: str,
    user_link: str,
    date: datetime.date,
    time_slot: str,
    user_id: Optional[int],
    status: str,
    wash_option: str = "",
    confirmed_by: str = "",
    confirmed_at: str = "",
    decline_reason: str = "",
) -> None:
    """
    Добавляет новое бронирование в таблицу.

    Args:
        user_name: Имя пользователя
        user_link: Ссылка на профиль пользователя VK
        date: Дата бронирования
        time_slot: Временной слот (формат HH:MM)
        user_id: ID пользователя VK (опционально)
        status: Статус бронирования
        wash_option: Опции стирки (по умолчанию пустая строка)
        confirmed_by: Имя администратора, подтвердившего запись (по умолчанию пустая строка)
        confirmed_at: Время подтверждения (по умолчанию пустая строка)
        decline_reason: Причина отказа (по умолчанию пустая строка)
    """
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return None

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    date_str = str(date)
    record = {
        "Пользователь": user_name,
        "Ссылка": user_link,
        "Дата": date_str,
        "Время": time_slot,
        "Статус": status,
        "Пользователь_ID": str(user_id) if user_id is not None else "",
        "Создано": date_str,
        "Опция стирки": wash_option,
        "Подтвердил": confirmed_by,
        "Подтверждено в": confirmed_at,
        "Причина отказа": decline_reason,
    }
    list_sheet.append_row(_values_from_record(record))
    # Инвалидируем кеш бронирований после добавления
    await invalidate_bookings_cache()


async def update_booking(record: Dict[str, str], updates: Dict[str, str]) -> Dict[str, str]:
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return {}

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    updated = {**record, **updates}
    list_sheet.update(_row_range(record["_row"]), [_values_from_record(updated)])
    updated["_row"] = record["_row"]
    # Инвалидируем кеш бронирований после обновления
    await invalidate_bookings_cache()
    return updated


async def delete_booking(record: Dict[str, str]) -> bool:
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return False

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    try:
        list_sheet.delete_rows(record["_row"])
        await invalidate_bookings_cache()
        return True
    except Exception:
        return False



async def is_time_free(date: datetime.date, time_slot: str) -> bool:
    records = await get_bookings(date=date)
    for record in records:
        if record.get("Время") == time_slot and record.get("Статус") in ACTIVE_STATUSES:
            return False
    return True


async def get_user_active_bookings(user_id: int) -> List[Dict[str, str]]:
    return await get_bookings(
        user_id=user_id,
        statuses=ACTIVE_STATUSES,
    )


async def get_pending_bookings() -> List[Dict[str, str]]:
    return await get_bookings(statuses={STATUS_PENDING})


async def get_admin_blockings() -> List[Dict[str, str]]:
    return await get_bookings(statuses={STATUS_BLOCKED})


async def set_booking_confirmed(record: Dict[str, str], admin_name: str) -> Dict[str, str]:
    offset = timedelta(hours=3)
    moscow_tz = timezone(offset, name='МСК')
    now = datetime.now(moscow_tz)

    return await update_booking(
        record,
        {
            "Статус": STATUS_CONFIRMED,
            "Подтвердил": admin_name,
            "Подтверждено в": datetime.strftime(now, DATETIME_FORMAT),
            "Причина отказа": "",
        },
    )


async def set_booking_rejected(
    record: Dict[str, str],
    admin_name: str,
    reason: str,
    *,
    keep_record: bool = True,
) -> Optional[Dict[str, str]]:
    if keep_record:
        offset = timedelta(hours=3)
        moscow_tz = timezone(offset, name='МСК')
        now = datetime.now(moscow_tz)

        return await update_booking(
            record,
            {
                "Статус": STATUS_REJECTED,
                "Подтвердил": admin_name,
                "Подтверждено в": now,
                "Причина отказа": reason,
            },
        )
    await delete_booking(record)
    return None


async def complete_booking(record: Dict[str, str]) -> None:
    """
    Завершает запись - удаляет её из таблицы.
    Используется когда стирка завершена.

    Args:
        record: Словарь с данными записи, включая поле "_row"
    """
    await delete_booking(record)



# -------------------------
# Расписание работы
# -------------------------

async def time_of_begining(idx: int) -> Optional[int]:
    """
    Возвращает час начала работы для указанного дня недели.
    Использует кеширование для оптимизации производительности.

    Args:
        idx: Индекс дня недели (0=понедельник, 6=воскресенье)

    Returns:
        Час начала работы или None, если день не найден
    """
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return None

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    def loader():
        return _fetch_records(schedule_sheet)

    records = await get_cached_schedule(with_retries(loader))
    dict_weekdays = {
        0: "понедельник",
        1: "вторник",
        2: "среда",
        3: "четверг",
        4: "пятница",
        5: "суббота",
        6: "воскресенье",
    }
    target_day = dict_weekdays[idx]
    for record in records:
        if record.get("День недели").lower() == target_day:
            return int(record.get("Начало"))
    return None


async def time_of_end(idx: int) -> Optional[int]:
    """
    Возвращает час окончания работы для указанного дня недели.
    Использует кеширование для оптимизации производительности.

    Args:
        idx: Индекс дня недели (0=понедельник, 6=воскресенье)

    Returns:
        Час окончания работы или None, если день не найден
    """
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return None

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    def loader():
        return _fetch_records(schedule_sheet)

    records = await get_cached_schedule(with_retries(loader))
    dict_weekdays = {
        0: "понедельник",
        1: "вторник",
        2: "среда",
        3: "четверг",
        4: "пятница",
        5: "суббота",
        6: "воскресенье",
    }
    target_day = dict_weekdays[idx]
    for record in records:
        if record.get("День недели").lower() == target_day:
            return int(record.get("Конец"))
    return None



# -------------------------
# Чёрный список
# -------------------------

def extract_screen_name_from_url(url: str) -> Optional[str]:
    """
    Извлекает screen_name из URL VK
    Поддерживает форматы:
    
    - https://vk.com/nickname228
    - http://vk.com/nickname228
    - vk.com/nickname228
    - https://vk.ru/nickname228
    - http://vk.ru/nickname228
    - vk.ru/nickname228
    - https://m.vk.com/nickname228
    - http://m.vk.com/nickname228
    - m.vk.com/nickname228
    - https://m.vk.ru/nickname228
    - http://m.vk.ru/nickname228
    - m.vk.ru/nickname228
    - @nickname228
    """
    # Убираем пробелы и приводим к нижнему регистру
    clean_url = url.strip().lower()
    
    # Регулярное выражение для извлечения screen_name
    patterns = [
        r'(?:https?://)?(?:m\.)?vk\.(?:ru|com)/([^/?&]+)',  
        r'^@([a-zA-Z0-9_.]+)$',
        r'^([a-zA-Z0-9_.]+)$',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, clean_url)
        if match:
            screen_name = match.group(1)
            # Проверяем, что screen_name не пустой и не содержит запрещенных символов
            if screen_name and not re.search(r'[^\w\.]', screen_name):
                return screen_name
    
    return None

async def url_to_user_id(url: str, api: API) -> Optional[int]:
    """
    Преобразует URL VK в ID пользователя
    Возвращает (user_id, error_message)
    """
    # Извлекаем screen_name из URL
    screen_name = extract_screen_name_from_url(url)
    
    if not screen_name:
        return None
    
    # Преобразуем screen_name в ID через API
    response = await api.utils.resolve_screen_name(screen_name=screen_name)
    
    if not response:
        return None
    
    if getattr(response.type, 'value', None) == 'user':
        return response.object_id
    return None


async def get_blacklist() -> List[str]:
    """
    Получает черный список из кеша или из Google Sheets.
    Использует кеширование для оптимизации производительности.
    """
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return []

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    def loader():
        values = blacklist_sheet.get_all_values()
        return [row[0] for row in values[1:] if row and row[0].strip()]

    return await get_cached_blacklist(with_retries(loader))


async def add_blacklist(api: API, user_link: str) -> bool:
    """
    Добавляет пользователя в черный список.

    Args:
        api: API объект VK для преобразования URL в ID
        user_link: Ссылка на пользователя VK
    """
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return False

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    match = await url_to_user_id(user_link, api)
    if not match:
        return False
    vk_link = f"https://vk.com/id{match}"
    blacklist = await get_blacklist()
    if vk_link not in blacklist:
        blacklist_sheet.append_row([vk_link])
        # Инвалидируем кеш черного списка после добавления
        await invalidate_blacklist_cache()
    return True


async def remove_blacklist(user_link: str) -> bool:
    if not await _init_google_sheets():
        logger.error("Не удалось подключиться к Google Sheets")
        return False

    # Get sheet references
    list_sheet, blacklist_sheet, schedule_sheet, spreadsheet, drive_service = _get_sheets()

    values = blacklist_sheet.get_all_values()
    for idx, row in enumerate(values, start=1):
        if row and row[0] == user_link:
            blacklist_sheet.delete_rows(idx)
            # Инвалидируем кеш черного списка после удаления
            await invalidate_blacklist_cache()
            return True
    return False
