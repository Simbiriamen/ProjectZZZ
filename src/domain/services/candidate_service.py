# -*- coding: utf-8 -*-
"""
Сервис работы с кандидатами на рекомендацию.

Инкапсулирует логику отбора и фильтрации кандидатов.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import date

import pandas as pd

from ..protocols.repositories import (
    CandidateRepositoryProtocol,
    PurchaseRepositoryProtocol,
)

logger = logging.getLogger(__name__)


class CandidateService:
    """
    Доменный сервис для работы с кандидатами.
    
    Ответственность:
    - Получение кандидатов для клиентов
    - Фильтрация уже купленных товаров
    - Ранжирование кандидатов
    
    Зависимости:
    - CandidateRepositoryProtocol
    - PurchaseRepositoryProtocol
    """
    
    def __init__(
        self,
        candidate_repo: Optional[CandidateRepositoryProtocol] = None,
        purchase_repo: Optional[PurchaseRepositoryProtocol] = None
    ):
        """
        Args:
            candidate_repo: Репозиторий кандидатов
            purchase_repo: Репозиторий покупок
        """
        self._candidate_repo = candidate_repo
        self._purchase_repo = purchase_repo
    
    def get_candidates_for_clients(
        self,
        client_ids: List[str],
        batch_size: int = 200,
        days: int = 90,
        top_n: int = 200
    ) -> pd.DataFrame:
        """
        Получает кандидатов для группы клиентов.
        
        Args:
            client_ids: Список идентификаторов клиентов
            batch_size: Размер пакета для обработки
            days: Период анализа (дней)
            top_n: Количество топ кандидатов на клиента
        
        Returns:
            DataFrame с кандидатами [client_id, sku_id, score, ...]
        """
        if not client_ids:
            return pd.DataFrame()
        
        logger.info(f"📋 Получение кандидатов для {len(client_ids)} клиентов...")
        
        if self._candidate_repo:
            candidates = self._candidate_repo.get_candidates(
                client_ids=client_ids,
                batch_size=batch_size,
                days=days,
                top_n=top_n
            )
        else:
            # Fallback: пустой DataFrame
            logger.warning("Репозиторий кандидатов не настроен, возвращаем пустой DataFrame")
            return pd.DataFrame(columns=['client_id', 'sku_id', 'score'])
        
        logger.info(f"   ✅ Получено {len(candidates)} кандидатов")
        return candidates
    
    def filter_purchased_skus(
        self,
        candidates: pd.DataFrame,
        client_id: str,
        purchased_skus: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Фильтрует уже купленные клиентом товары.
        
        Args:
            candidates: DataFrame с кандидатами
            client_id: Идентификатор клиента
            purchased_skus: Список купленных SKU (опционально)
        
        Returns:
            Отфильтрованный DataFrame
        """
        if candidates.empty:
            return candidates
        
        # Если список покупок не передан, пробуем получить из репозитория
        if purchased_skus is None and self._purchase_repo:
            try:
                history = self._purchase_repo.get_client_history(client_id, days=365)
                purchased_skus = history['sku_id'].unique().tolist()
            except Exception as e:
                logger.warning(f"Не удалось получить историю покупок: {e}")
                purchased_skus = []
        
        if not purchased_skus:
            return candidates
        
        # Фильтрация
        filtered = candidates[~candidates['sku_id'].isin(purchased_skus)]
        
        removed_count = len(candidates) - len(filtered)
        if removed_count > 0:
            logger.debug(f"   🗑️ Удалено {removed_count} уже купленных SKU для {client_id}")
        
        return filtered
    
    def rank_candidates(
        self,
        candidates: pd.DataFrame,
        score_column: str = 'score',
        descending: bool = True
    ) -> pd.DataFrame:
        """
        Ранжирует кандидатов по score.
        
        Args:
            candidates: DataFrame с кандидатами
            score_column: Название колонки со score
            descending: Сортировка по убыванию
        
        Returns:
            DataFrame с добавленной колонкой 'rank'
        """
        if candidates.empty:
            candidates['rank'] = []
            return candidates
        
        result = candidates.copy()
        result['rank'] = result.groupby('client_id')[score_column].rank(
            ascending=not descending,
            method='first'
        ).astype(int)
        
        return result.sort_values(['client_id', 'rank'])
    
    def get_top_candidates(
        self,
        candidates: pd.DataFrame,
        n: int = 10
    ) -> pd.DataFrame:
        """
        Получает топ-N кандидатов для каждого клиента.
        
        Args:
            candidates: DataFrame с кандидатами (должен иметь колонку 'rank')
            n: Количество кандидатов на клиента
        
        Returns:
            DataFrame с топ-N кандидатами
        """
        if candidates.empty:
            return candidates
        
        if 'rank' not in candidates.columns:
            candidates = self.rank_candidates(candidates)
        
        return candidates[candidates['rank'] <= n]
