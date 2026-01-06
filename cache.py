"""
Модуль кеширования данных из Google Sheets.
Обеспечивает async-safe кеширование с автоматической инвалидацией при изменениях.
Использует asyncio.Lock для асинхронных операций с Google Sheets API.
Реализует single-flight pattern для предотвращения cache stampede.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from config import DATETIME_FORMAT

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
    Async-safe кеш для данных из Google Sheets.
    Использует asyncio.Lock для обеспечения безопасности при одновременном доступе.
    Реализует single-flight pattern: если несколько корутин запрашивают один ключ,
    только одна загружает данные, остальные ждут результата.
    """

    def __init__(self, default_ttl: Optional[float] = 300):
        """
        Инициализирует кеш.

        Args:
            default_ttl: Время жизни кеша по умолчанию в секундах (5 минут)
        """
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        # Словарь для отслеживания текущих загрузок (single-flight pattern)
        self._pending: Dict[str, asyncio.Task] = {}
        self.default_ttl = default_ttl

    async def get(
        self,
        key: str,
        loader: Optional[Callable[[], Any]] = None,
        ttl: Optional[float] = None,
    ) -> Any:
        """
        Получает данные из кеша или загружает их через loader.
        Реализует double-checked locking для предотвращения cache stampede.

        Args:
            key: Ключ кеша
            loader: Функция для загрузки данных (может быть sync или async)
            ttl: Время жизни кеша (если не указано, используется default_ttl)

        Returns:
            Закешированные или загруженные данные
        """
        # Первая проверка (без блокировки, быстрая)
        entry = self._cache.get(key)
        if entry and not entry.is_expired():
            return entry.data

        # Если loader не передан, возвращаем что есть
        if loader is None:
            return entry.data if entry else None

        # Проверяем, не загружается ли уже этот ключ другой корутиной
        async with self._lock:
            # Вторая проверка (под блокировкой)
            entry = self._cache.get(key)
            if entry and not entry.is_expired():
                return entry.data

            # Проверяем pending загрузки
            if key in self._pending:
                # Другая корутина уже загружает данные, ждем результата
                pending_task = self._pending[key]
                # Освобождаем lock, чтобы не блокировать других
                pass  # lock освободится при выходе из блока

        # Если есть pending задача, ждем ее завершения
        if key in self._pending:
            try:
                await self._pending[key]
            except Exception:
                pass  # Ошибка будет обработана в загружающей корутине
            # После завершения pending задачи данные должны быть в кеше
            entry = self._cache.get(key)
            if entry and not entry.is_expired():
                return entry.data
            # Если данных все еще нет, пробуем загрузить сами
            if entry:
                return entry.data

        # Создаем задачу загрузки
        async with self._lock:
            # Тройная проверка - возможно, пока мы ждали lock, кто-то уже загрузил
            entry = self._cache.get(key)
            if entry and not entry.is_expired():
                return entry.data

            # Создаем задачу загрузки и добавляем в pending
            load_task = asyncio.create_task(self._load_and_cache(key, loader, ttl))
            self._pending[key] = load_task

        # Выполняем загрузку
        try:
            return await load_task
        finally:
            # Удаляем из pending после завершения
            async with self._lock:
                self._pending.pop(key, None)

    async def _load_and_cache(
        self,
        key: str,
        loader: Callable[[], Any],
        ttl: Optional[float],
    ) -> Any:
        """
        Внутренний метод для загрузки и кеширования данных.

        Args:
            key: Ключ кеша
            loader: Функция загрузки (sync или async)
            ttl: Время жизни кеша

        Returns:
            Загруженные данные
        """
        # Выполняем loader (может быть sync или async)
        try:
            # Проверяем, является ли loader корутиной
            if asyncio.iscoroutinefunction(loader):
                data = await loader()
            else:
                # Sync функция - выполняем в executor, чтобы не блокировать event loop
                data = await asyncio.get_event_loop().run_in_executor(None, loader)
        except Exception as e:
            logger.error(f"Ошибка загрузки данных для ключа {key}: {e}")
            # Пробуем вернуть старые данные как fallback
            entry = self._cache.get(key)
            if entry:
                logger.warning(f"Используем устаревшие данные для ключа {key}")
                return entry.data
            raise

        # Сохраняем результат в кеш под lock
        async with self._lock:
            ttl_to_use = ttl if ttl is not None else self.default_ttl
            self._cache[key] = CacheEntry(data, ttl_to_use)
            logger.debug(f"Данные закешированы для ключа {key} с TTL={ttl_to_use}с")

        return data

    async def invalidate(self, key: Optional[str] = None) -> None:
        """
        Инвалидирует кеш (удаляет запись или все записи).

        Args:
            key: Ключ для удаления (None = удалить все)
        """
        async with self._lock:
            if key is None:
                self._cache.clear()
                logger.debug("Весь кеш очищен")
            else:
                if key in self._cache:
                    del self._cache[key]
                    logger.debug(f"Кеш инвалидирован для ключа: {key}")

    async def invalidate_pattern(self, pattern: str) -> None:
        """
        Инвалидирует все ключи, начинающиеся с указанного паттерна.

        Args:
            pattern: Паттерн для поиска ключей
        """
        async with self._lock:
            keys_to_remove = [key for key in self._cache.keys() if key.startswith(pattern)]
            for key in keys_to_remove:
                del self._cache[key]
            if keys_to_remove:
                logger.debug(f"Инвалидировано {len(keys_to_remove)} ключей с паттерном: {pattern}")

    async def clear_expired(self) -> None:
        """Удаляет все истекшие записи из кеша."""
        async with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items() if entry.is_expired()
            ]
            for key in expired_keys:
                del self._cache[key]
            if expired_keys:
                logger.debug(f"Удалено {len(expired_keys)} истекших записей из кеша")

    async def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику кеша.

        Returns:
            Словарь со статистикой кеша
        """
        async with self._lock:
            total = len(self._cache)
            expired = sum(1 for entry in self._cache.values() if entry.is_expired())
            pending = len(self._pending)
            return {
                "total_entries": total,
                "expired_entries": expired,
                "valid_entries": total - expired,
                "pending_loads": pending,
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


async def get_cached_bookings(
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
    all_bookings = await cache.get(CACHE_KEY_BOOKINGS, loader)

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

    # Сортируем по дате и времени
    filtered.sort(
        key=lambda r: datetime.strptime(
            r.get("Дата").strip() + " " + r.get("Время").strip(),
            DATETIME_FORMAT,
        )
    )

    return filtered


async def invalidate_bookings_cache() -> None:
    """Инвалидирует кеш бронирований."""
    cache = get_cache()
    await cache.invalidate(CACHE_KEY_BOOKINGS)
    await cache.invalidate_pattern("bookings:")


async def get_cached_blacklist(loader: Callable[[], List[str]]) -> List[str]:
    """
    Получает черный список из кеша или загружает его.

    Args:
        loader: Функция для загрузки черного списка

    Returns:
        Список ссылок из черного списка
    """
    cache = get_cache()
    result = await cache.get(CACHE_KEY_BLACKLIST, loader)
    return result or []


async def invalidate_blacklist_cache() -> None:
    """Инвалидирует кеш черного списка."""
    cache = get_cache()
    await cache.invalidate(CACHE_KEY_BLACKLIST)


async def get_cached_schedule(loader: Callable[[], List[Dict[str, str]]]) -> List[Dict[str, str]]:
    """
    Получает расписание из кеша или загружает его.

    Args:
        loader: Функция для загрузки расписания

    Returns:
        Список записей расписания
    """
    cache = get_cache()
    result = await cache.get(CACHE_KEY_SCHEDULE, loader)
    return result or []


async def invalidate_schedule_cache() -> None:
    """Инвалидирует кеш расписания."""
    cache = get_cache()
    await cache.invalidate(CACHE_KEY_SCHEDULE)
