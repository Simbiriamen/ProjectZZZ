"""
ProjectZZZ - Система рекомендаций SKU на базе ML.

Модульная архитектура с разделением ответственности:
- domain: бизнес-сущности и логика
- application: команды и обработчики (CQRS)
- infrastructure: реализации репозиториев, ML, БД
- interfaces: CLI, API, GUI
- config: конфигурация приложения
- shared: общие утилиты и DI контейнер
"""

__version__ = "2.0.0"
__author__ = "ProjectZZZ Team"
