"""
Главный модуль бота VK.
Инициализирует бота, регистрирует обработчики и запускает основной цикл работы.
"""
import asyncio
import logging

# Настройка Pydantic для работы с vkbottle
try:
    from pydantic import BaseConfig
    BaseConfig.arbitrary_types_allowed = True
except ImportError:
    # Для Pydantic v2 используем другой подход
    try:
        from pydantic import ConfigDict
        # В Pydantic v2 это настраивается по-другому
        pass
    except ImportError:
        pass

from vkbottle.bot import Bot

from cache import init_cache
from config import CACHE_TTL, VK_TOKEN
from handlers.admin import Admin
from handlers.user import User
from notifications import notification_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Проверка токена
if not VK_TOKEN:
    raise ValueError("VK_TOKEN не задан в .env")

# Инициализируем кеш
init_cache(default_ttl=CACHE_TTL)

# Создаем объект бота
bot = Bot(token=VK_TOKEN)
#bot.labeler.vbml_ignore_case = True

# Регистрируем обработчики команд


user = User(bot)
admin = Admin(bot)


async def main():
    """
    Главная функция для запуска бота.
    Запускает параллельно обработку сообщений и цикл уведомлений.
    """
    await asyncio.gather(bot.run_polling(), notification_loop(bot))


if __name__ == "__main__":
    asyncio.run(main())
