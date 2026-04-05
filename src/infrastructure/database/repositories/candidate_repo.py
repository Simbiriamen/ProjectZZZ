# -*- coding: utf-8 -*-
"""
Репозиторий кандидатов.

Реализация CandidateRepositoryProtocol для работы с БД.
"""

import logging
from typing import List, Optional
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from ....domain.protocols.repositories import CandidateRepositoryProtocol
from ..connection import Database

logger = logging.getLogger(__name__)


class CandidateRepository(CandidateRepositoryProtocol):
    """
    Репозиторий для работы с кандидатами на рекомендацию.
    
    Реализует методы для получения кандидатов из БД.
    """
    
    def __init__(self, db: Database):
        """
        Args:
            db: Экземпляр подключения к БД
        """
        self._db = db
    
    def get_candidates(
        self,
        client_ids: List[str],
        batch_size: int = 200,
        days: int = 90,
        top_n: int = 200
    ) -> pd.DataFrame:
        """Получает кандидатов для рекомендации."""
        if not client_ids:
            return pd.DataFrame()
        
        cutoff_date = date.today() - timedelta(days=days)
        
        # Получаем популярные SKU за период
        placeholders = ",".join(f":client_id_{i}" for i in range(len(client_ids)))
        query = text(f"""
            WITH popular_skus AS (
                SELECT 
                    sku_id,
                    COUNT(*) as purchase_count
                FROM sales_full
                WHERE purchase_date >= :cutoff_date
                GROUP BY sku_id
                ORDER BY purchase_count DESC
                LIMIT :top_n
            )
            SELECT DISTINCT
                c.client_id,
                ps.sku_id,
                ps.purchase_count as score
            FROM (SELECT DISTINCT UNNEST(ARRAY[{placeholders}]) as client_id) c
            CROSS JOIN popular_skus ps
            WHERE NOT EXISTS (
                SELECT 1 FROM sales_full sf
                WHERE sf.client_id = c.client_id
                  AND sf.sku_id = ps.sku_id
            )
        """)
        
        params = {
            **{f"client_id_{i}": cid for i, cid in enumerate(client_ids)},
            "cutoff_date": cutoff_date,
            "top_n": top_n
        }
        
        with self._db.get_connection() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame(columns=['client_id', 'sku_id', 'score'])
        
        df = pd.DataFrame(rows, columns=['client_id', 'sku_id', 'score'])
        return df
    
    def filter_purchased_candidates(
        self,
        candidates: pd.DataFrame,
        client_id: str
    ) -> pd.DataFrame:
        """Фильтрует уже купленные кандидаты."""
        if candidates.empty:
            return candidates
        
        # Получение купленных SKU клиента
        query = text("""
            SELECT DISTINCT sku_id
            FROM sales_full
            WHERE client_id = :client_id
        """)
        
        with self._db.get_connection() as conn:
            result = conn.execute(query, {"client_id": client_id})
            rows = result.fetchall()
        
        purchased_skus = {row[0] for row in rows}
        
        if not purchased_skus:
            return candidates
        
        # Фильтрация
        filtered = candidates[~candidates['sku_id'].isin(purchased_skus)]
        
        logger.debug(
            f"Удалено {len(candidates) - len(filtered)} купленных SKU для {client_id}"
        )
        
        return filtered
