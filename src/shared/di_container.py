# -*- coding: utf-8 -*-
"""
Dependency Injection контейнер.

Использует dependency-injector для управления зависимостями.
"""

import logging
from dependency_injector import containers, providers

from ..config.settings import Settings
from ..domain.services.model_service import ModelService
from ..domain.services.candidate_service import CandidateService
from ..domain.services.recommendation_service import RecommendationService

logger = logging.getLogger(__name__)


class Container(containers.DeclarativeContainer):
    """
    DI контейнер приложения.
    
    Определяет все зависимости и их жизненные циклы.
    
    Использование:
        container = Container()
        container.config.from_yaml("config.yaml")
        
        recommendation_service = container.recommendation_service()
    """
    
    # Конфигурация
    config = providers.Configuration()
    settings = providers.Singleton(Settings)
    
    # Domain Services
    model_service = providers.Factory(
        ModelService,
        models_dir=settings.ml.models_dir
    )
    
    candidate_service = providers.Factory(
        CandidateService
    )
    
    recommendation_service = providers.Factory(
        RecommendationService,
        model_service=model_service,
        candidate_service=candidate_service,
        min_score_threshold=config.min_score_threshold.as_float()(0.1),
        max_recommendations=config.max_recommendations.as_int()(10)
    )
    
    @classmethod
    def init_from_env(cls) -> "Container":
        """Создаёт контейнер с настройками из environment."""
        container = cls()
        logger.info("✅ DI контейнер инициализирован из environment")
        return container
