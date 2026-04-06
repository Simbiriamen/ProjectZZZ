"""
Dependency Injection контейнер на базе dependency-injector.

Обеспечивает централизованное управление зависимостями и их жизненным циклом.
"""

from dependency_injector import containers, providers

from .settings import Settings


class Container(containers.DeclarativeContainer):
    """DI контейнер приложения."""

    # Конфигурация
    config = providers.Singleton(Settings)

    # Ресурсы (будут добавлены в следующих итерациях)
    # database_engine = providers.Resource(...)
    # session_factory = providers.Factory(..., engine=database_engine)
    
    # Репозитории (будут добавлены после создания протоколов)
    # client_repository = providers.Factory(
    #     SqlAlchemyClientRepository,
    #     session_factory=session_factory,
    # )
    
    # Сервисы
    # recommendation_service = providers.Factory(
    #     RecommendationService,
    #     client_repo=client_repository,
    #     model_repository=...,
    # )

    wiring_config = containers.WiringConfiguration(
        modules=[
            "src.application.handlers",
            "src.interfaces.api",
        ],
        packages=[
            "src.domain",
            "src.infrastructure",
        ],
    )


# Глобальный экземпляр контейнера
container = Container()

__all__ = ["Container", "container"]
