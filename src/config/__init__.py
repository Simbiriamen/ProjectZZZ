"""
Конфигурация приложения.

Использует pydantic-settings для типизированной конфигурации
с поддержкой environment variables и .env файлов.
"""

from .settings import Settings, DatabaseSettings, MLSettings, AppSettings

__all__ = [
    "Settings",
    "DatabaseSettings",
    "MLSettings",
    "AppSettings",
]
