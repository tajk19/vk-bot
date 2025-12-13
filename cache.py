"""
Модуль кеширования данных из Google Sheets.
Обеспечивает thread-safe кеширование с автоматической инвалидацией при изменениях.
Использует threading.Lock для синхронных операций с Google Sheets API.
"""
import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class CacheEntry:
    """Запись в кеше с данными и временем создания."""

    def __init__(self, data: Any, ttl: Optional[float] = None):
        """
        Инициализирует запись кеша.

        Args:
            data: Данные для кеширования
            ttl: Время жизни кеша в секундах (None = без ограничения)
        """
        self.data = data
        self.created_at = time.time()
        self.ttl = ttl

    def is_expired(self) -> bool:
        """
        Проверяет, истек ли срок действия кеша.

        Returns:
            True, если кеш истек, иначе False
        """
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl


class Cache:

    """
    Thread-safe кеш для данных из Google Sheets.
    Использует threading.Lock для обеспечения безопасности при одновременном доступе.
    """

    def __init__(self, default_ttl: Optional[float] = 300):
        """
        Инициализирует кеш.

        Args:
            default_ttl: Время жизни кеша по умолчанию в секундах (5 минут)
        """
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self.default_ttl = default_ttl

    def get(
        self,
        key: str,
        loader: Optional[Callable[[], Any]] = None,
        ttl: Optional[float] = None,
    ) -> Any:
        
        with self._lock:
            entry = self._cache.get(key)
            if entry and not entry.is_expired():
                return entry.data

        if loader is None:
            return entry.data if entry else None

        # Выполняем loader вне lock
        try:
            data = loader()
        except Exception as e:
            if entry:
                return entry.data  # fallback на старые данные
            raise

        # Сохраняем результат в кеш под lock
        with self._lock:
            ttl_to_use = ttl if ttl is not None else self.default_ttl
            self._cache[key] = CacheEntry(data, ttl_to_use)

        return data

    def invalidate(self, key: Optional[str] = None) -> None:
        """
        Инвалидирует кеш (удаляет запись или все записи).

        Args:
            key: Ключ для удаления (None = удалить все)
        """
        with self._lock:
            if key is None:
                self._cache.clear()
                logger.debug("Весь кеш очищен")
            else:
                if key in self._cache:
                    del self._cache[key]
                    logger.debug(f"Кеш инвалидирован для ключа: {key}")

    def invalidate_pattern(self, pattern: str) -> None:
        """
        Инвалидирует все ключи, начинающиеся с указанного паттерна.

        Args:
            pattern: Паттерн для поиска ключей
        """
        with self._lock:
            keys_to_remove = [key for key in self._cache.keys() if key.startswith(pattern)]
            for key in keys_to_remove:
                del self._cache[key]
            if keys_to_remove:
                logger.debug(f"Инвалидировано {len(keys_to_remove)} ключей с паттерном: {pattern}")

    def clear_expired(self) -> None:
        """Удаляет все истекшие записи из кеша."""
        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items() if entry.is_expired()
            ]
            for key in expired_keys:
                del self._cache[key]
            if expired_keys:
                logger.debug(f"Удалено {len(expired_keys)} истекших записей из кеша")

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику кеша.

        Returns:
            Словарь со статистикой кеша
        """
        with self._lock:
            total = len(self._cache)
            expired = sum(1 for entry in self._cache.values() if entry.is_expired())
            return {
                "total_entries": total,
                "expired_entries": expired,
                "valid_entries": total - expired,
            }


# Глобальный экземпляр кеша
# TTL будет установлен при импорте config
_cache: Optional[Cache] = None


def init_cache(default_ttl: float = 300) -> None:
    """
    Инициализирует глобальный экземпляр кеша.
    
    Args:
        default_ttl: Время жизни кеша по умолчанию в секундах
    """
    global _cache
    _cache = Cache(default_ttl=default_ttl)
    logger.info(f"Кеш инициализирован с TTL={default_ttl} секунд")


def get_cache() -> Cache:
    """
    Возвращает глобальный экземпляр кеша.
    Инициализирует его, если он еще не создан.
    
    Returns:
        Экземпляр кеша
    """
    global _cache
    if _cache is None:
        init_cache()
    return _cache


# Ключи кеша
CACHE_KEY_BOOKINGS = "bookings:all"
CACHE_KEY_BLACKLIST = "blacklist:all"
CACHE_KEY_SCHEDULE = "schedule:all"


def get_cache_key_bookings(
    date: Optional[str] = None,
    user_id: Optional[int] = None,
    statuses: Optional[Tuple[str, ...]] = None,
) -> str:
    """
    Генерирует ключ кеша для бронирований с учетом фильтров.

    Args:
        date: Дата в формате строки
        user_id: ID пользователя
        statuses: Кортеж статусов

    Returns:
        Ключ кеша
    """
    parts = ["bookings"]
    if date:
        parts.append(f"date:{date}")
    if user_id is not None:
        parts.append(f"user:{user_id}")
    if statuses:
        parts.append(f"statuses:{','.join(sorted(statuses))}")
    return ":".join(parts)


def get_cached_bookings(
    loader: Callable[[], List[Dict[str, str]]],
    date: Optional[str] = None,
    user_id: Optional[int] = None,
    statuses: Optional[Tuple[str, ...]] = None,
) -> List[Dict[str, str]]:
    """
    Получает бронирования из кеша или загружает их.

    Args:
        loader: Функция для загрузки всех бронирований
        date: Дата для фильтрации
        user_id: ID пользователя для фильтрации
        statuses: Статусы для фильтрации

    Returns:
        Список бронирований
    """
    # Сначала получаем все бронирования из кеша
    cache = get_cache()
    all_bookings = cache.get(CACHE_KEY_BOOKINGS, loader)

    if all_bookings is None:
        return []

    # Применяем фильтры
    filtered = all_bookings
    if date:
        filtered = [b for b in filtered if b.get("Дата") == date]
    if user_id is not None:
        user_id_str = str(user_id)
        filtered = [b for b in filtered if b.get("Пользователь_ID") == user_id_str]
    if statuses:
        statuses_set = set(statuses)
        filtered = [b for b in filtered if b.get("Статус") in statuses_set]

    filtered.sort(
        key=lambda r: datetime.strptime(
            r.get("Дата").strip() + " " + r.get("Время").strip(),
            "%d.%m.%y %H:%M"
        )
    )

    return filtered


def invalidate_bookings_cache() -> None:
    """Инвалидирует кеш бронирований."""
    cache = get_cache()
    cache.invalidate(CACHE_KEY_BOOKINGS)
    cache.invalidate_pattern("bookings:")


def get_cached_blacklist(loader: Callable[[], List[str]]) -> List[str]:
    """
    Получает черный список из кеша или загружает его.

    Args:
        loader: Функция для загрузки черного списка

    Returns:
        Список ссылок из черного списка
    """
    cache = get_cache()
    return cache.get(CACHE_KEY_BLACKLIST, loader) or []


def invalidate_blacklist_cache() -> None:
    """Инвалидирует кеш черного списка."""
    cache = get_cache()
    cache.invalidate(CACHE_KEY_BLACKLIST)


def get_cached_schedule(loader: Callable[[], List[Dict[str, str]]]) -> List[Dict[str, str]]:
    """
    Получает расписание из кеша или загружает его.

    Args:
        loader: Функция для загрузки расписания

    Returns:
        Список записей расписания
    """
    cache = get_cache()
    return cache.get(CACHE_KEY_SCHEDULE, loader) or []


def invalidate_schedule_cache() -> None:
    """Инвалидирует кеш расписания."""
    cache = get_cache()
    cache.invalidate(CACHE_KEY_SCHEDULE)

