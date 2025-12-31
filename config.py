"""
Модуль конфигурации приложения.
Загружает настройки из переменных окружения и предоставляет константы для использования в других модулях.
"""
import os
from datetime import date, datetime
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "Не установлена зависимость python-dotenv. "
        "Установите зависимости из requirements.txt."
    ) from exc

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

VK_TOKEN = os.getenv("VK_TOKEN")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME", "test_vk_bot")

ADMIN_IDS = [
    int(admin_id)
    for admin_id in os.getenv("ADMIN_IDS").split(",")
    if admin_id.strip()
]

_admin_contact_env = os.getenv("ADMIN_CONTACT_URL", "").strip()
if _admin_contact_env:
    ADMIN_CONTACT_URL = _admin_contact_env
elif ADMIN_IDS:
    ADMIN_CONTACT_URL = f"https://vk.com/id{ADMIN_IDS[0]}"
else:
    ADMIN_CONTACT_URL = "https://vk.com"

MAX_SLOTS_PER_DAY = int(os.getenv("MAX_SLOTS_PER_DAY", "3"))
SLOT_INTERVAL_MIN = int(os.getenv("SLOT_INTERVAL_MIN", "30"))
WASH_DURATION_MIN = int(os.getenv("WASH_DURATION_MIN", "60"))

NOTIFY_BEFORE_MIN = int(os.getenv("NOTIFY_BEFORE_MIN", "10"))
NOTIFY_AFTER_MIN = int(os.getenv("NOTIFY_AFTER_MIN", "60"))


WEEKDAYS_SHORT_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
DATE_FORMAT = "%d.%m.%y"
TIME_FORMAT = "%H:%M"
DATETIME_FORMAT = "%d.%m.%y %H:%M"


WASH_OPTIONS = ["Без добавок", "Отбеливатель", "Порошок", "Кондиционер", "Гель"]
WASH_PRICES = {"Без добавок": 90, "Отбеливатель": 20, "Порошок": 15, "Кондиционер": 20, "Гель": 20} 

# Настройки кеширования (в секундах)
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  # 5 минут по умолчанию
# Интервал проверки изменений в Google Sheets (в секундах)
SHEET_CHECK_INTERVAL = int(os.getenv("SHEET_CHECK_INTERVAL", "60"))  # 1 минута по умолчанию


def format_date_with_weekday(d: date) -> str:
    return f"{WEEKDAYS_SHORT_RU[d.weekday()]} - {d.strftime(DATE_FORMAT)}"

def convert_from_format_with_weekday(value: str) -> Optional[date]:
    try:
        date_part = value.split("-")[1].strip()

        return datetime.strptime(
            date_part,
            DATE_FORMAT,
        ).date()
    except IndexError:
        return None