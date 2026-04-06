# -*- coding: utf-8 -*-
"""
Сервис генерации рекомендаций.

Инкапсулирует основную бизнес-логику системы рекомендаций.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import date

import pandas as pd
import numpy as np

from ..entities.recommendation import Recommendation, RecommendationSource
from ..protocols.repositories import (
    ClientRepositoryProtocol,
    PurchaseRepositoryProtocol,
    CandidateRepositoryProtocol,
)
from .model_service import ModelService
from .candidate_service import CandidateService

logger = logging.getLogger(__name__)


class RecommendationService:
    """
    Доменный сервис для генерации рекомендаций.
    
    Ответственность:
    - Генерация рекомендаций для клиентов
    - Комбинирование ML и fallback стратегий
    - Формирование итогового списка рекомендаций
    
    Зависимости:
    - ModelService
    - CandidateService
    - ClientRepositoryProtocol
    - PurchaseRepositoryProtocol
    """
    
    def __init__(
        self,
        model_service: ModelService,
        candidate_service: CandidateService,
        client_repo: Optional[ClientRepositoryProtocol] = None,
        purchase_repo: Optional[PurchaseRepositoryProtocol] = None,
        min_score_threshold: float = 0.1,
        max_recommendations: int = 10
    ):
        """
        Args:
            model_service: Сервис работы с моделями
            candidate_service: Сервис работы с кандидатами
            client_repo: Репозиторий клиентов
            purchase_repo: Репозиторий покупок
            min_score_threshold: Минимальный порог score
            max_recommendations: Максимум рекомендаций на клиента
        """
        self._model_service = model_service
        self._candidate_service = candidate_service
        self._client_repo = client_repo
        self._purchase_repo = purchase_repo
        self._min_score = min_score_threshold
        self._max_recs = max_recommendations
    
    def generate_for_clients(
        self,
        client_ids: List[str],
        reference_date: Optional[date] = None,
        include_fallback: bool = True
    ) -> List[Recommendation]:
        """
        Генерирует рекомендации для группы клиентов.
        
        Args:
            client_ids: Список идентификаторов клиентов
            reference_date: Дата среза данных
            include_fallback: Использовать fallback стратегию
        
        Returns:
            Список рекомендаций
        """
        if not client_ids:
            return []
        
        reference_date = reference_date or date.today()
        logger.info(f"🎯 Генерация рекомендаций для {len(client_ids)} клиентов...")
        
        all_recommendations = []
        
        # Получение кандидатов
        candidates = self._candidate_service.get_candidates_for_clients(client_ids)
        
        if candidates.empty:
            logger.warning("Кандидаты не получены, используем fallback")
            if include_fallback:
                return self._generate_fallback(client_ids)
            return []
        
        # Загрузка модели
        try:
            model_components = self._model_service.load_active_model()
        except Exception as e:
            logger.error(f"Ошибка загрузки модели: {e}")
            if include_fallback:
                return self._generate_fallback(client_ids)
            return []
        
        # Генерация предсказаний для каждого клиента
        for client_id in client_ids:
            client_candidates = candidates[candidates['client_id'] == client_id]
            
            if client_candidates.empty:
                continue
            
            # Фильтрация купленных товаров
            client_candidates = self._candidate_service.filter_purchased_skus(
                client_candidates,
                client_id
            )
            
            if client_candidates.empty:
                continue
            
            # Получение признаков и предсказание
            recommendations = self._predict_for_client(
                client_id=client_id,
                candidates=client_candidates,
                model_components=model_components,
                reference_date=reference_date
            )
            
            all_recommendations.extend(recommendations)
        
        logger.info(f"   ✅ Сгенерировано {len(all_recommendations)} рекомендаций")
        return all_recommendations
    
    def _predict_for_client(
        self,
        client_id: str,
        candidates: pd.DataFrame,
        model_components: Dict[str, Any],
        reference_date: date
    ) -> List[Recommendation]:
        """Генерирует предсказания для одного клиента."""
        feature_cols = model_components['feature_cols']
        
        # Подготовка признаков (упрощённо)
        features_df = candidates.copy()
        for col in feature_cols:
            if col not in features_df.columns:
                features_df[col] = 0
        
        # Предсказание
        scores = self._model_service.predict(
            features_df=features_df[feature_cols],
            model_components=model_components,
            apply_calibration=True
        )
        
        # Формирование рекомендаций
        recommendations = []
        for idx, (_, row) in enumerate(features_df.iterrows()):
            score = scores[idx]
            
            if score < self._min_score:
                continue
            
            rec = Recommendation(
                client_id=client_id,
                sku_id=row['sku_id'],
                score=float(score),
                rank=idx + 1,
                source=RecommendationSource.ML_MODEL,
                generated_at=reference_date
            )
            recommendations.append(rec)
        
        # Сортировка и ограничение
        recommendations.sort(key=lambda r: r.score, reverse=True)
        return recommendations[:self._max_recs]
    
    def _generate_fallback(self, client_ids: List[str]) -> List[Recommendation]:
        """Генерирует fallback рекомендации (популярные товары)."""
        logger.info("🔄 Использование fallback стратегии...")
        
        if not self._purchase_repo:
            return []
        
        try:
            popular = self._purchase_repo.get_popular_skus(min_purchases=5)
        except Exception as e:
            logger.error(f"Ошибка получения популярных товаров: {e}")
            return []
        
        recommendations = []
        reference_date = date.today()
        
        for client_id in client_ids:
            for rank, (sku_id, _) in enumerate(popular.head(self._max_recs).items()):
                rec = Recommendation(
                    client_id=client_id,
                    sku_id=sku_id,
                    score=0.5 - rank * 0.05,  # Убывающий score
                    rank=rank + 1,
                    source=RecommendationSource.POPULAR_ITEMS,
                    generated_at=reference_date
                )
                recommendations.append(rec)
        
        logger.info(f"   ✅ Fallback: {len(recommendations)} рекомендаций")
        return recommendations
    
    def get_recommendations_as_df(
        self,
        recommendations: List[Recommendation]
    ) -> pd.DataFrame:
        """Конвертирует список рекомендаций в DataFrame."""
        if not recommendations:
            return pd.DataFrame()
        
        data = [rec.to_dict() for rec in recommendations]
        df = pd.DataFrame(data)
        
        # Преобразование source из Enum в string
        if 'source' in df.columns:
            df['source'] = df['source'].apply(lambda x: x.value if hasattr(x, 'value') else x)
        
        return df
