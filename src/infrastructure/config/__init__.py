"""
Конфигурация и Dependency Injection.

Модуль предоставляет:
- Settings: типизированная конфигурация приложения
- Container: DI контейнер для управления зависимостями
- settings: глобальный экземпляр настроек (для обратной совместимости)
- container: глобальный экземпляр DI контейнера
"""

from .settings import Settings, settings, DatabaseSettings, MLSettings, AppSettings
from .container import Container, container

__all__ = [
    "Settings",
    "settings",
    "DatabaseSettings",
    "MLSettings",
    "AppSettings",
    "Container",
    "container",
]
