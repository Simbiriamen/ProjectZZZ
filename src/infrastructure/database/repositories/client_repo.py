# -*- coding: utf-8 -*-
"""
Репозиторий клиентов.

Реализация ClientRepositoryProtocol для работы с БД.
"""

import logging
from typing import List, Dict, Optional
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from ....domain.protocols.repositories import ClientRepositoryProtocol
from ..connection import Database

logger = logging.getLogger(__name__)


class ClientRepository(ClientRepositoryProtocol):
    """
    Репозиторий для работы с клиентами.
    
    Реализует методы для получения данных о клиентах из БД.
    """
    
    def __init__(self, db: Database):
        """
        Args:
            db: Экземпляр подключения к БД
        """
        self._db = db
    
    def get_active_clients(
        self,
        months: int = 12,
        min_purchases: int = 3
    ) -> List[str]:
        """Получает список активных клиентов."""
        query = text("""
            SELECT DISTINCT client_id
            FROM sales_full
            WHERE purchase_date >= :cutoff_date
            GROUP BY client_id
            HAVING COUNT(*) >= :min_purchases
        """)
        
        cutoff_date = date.today() - timedelta(days=months * 30)
        
        with self._db.get_connection() as conn:
            result = conn.execute(
                query,
                {"cutoff_date": cutoff_date, "min_purchases": min_purchases}
            )
            rows = result.fetchall()
        
        return [row[0] for row in rows]
    
    def get_clients_for_visit(self, visit_date: date) -> List[str]:
        """Получает клиентов для визита на указанную дату."""
        # Логика выбора клиентов для визита
        query = text("""
            SELECT DISTINCT client_id
            FROM sales_full
            WHERE purchase_date >= :cutoff_date
              AND purchase_date < :visit_date
            ORDER BY purchase_date DESC
            LIMIT 1000
        """)
        
        cutoff_date = visit_date - timedelta(days=90)
        
        with self._db.get_connection() as conn:
            result = conn.execute(
                query,
                {"cutoff_date": cutoff_date, "visit_date": visit_date}
            )
            rows = result.fetchall()
        
        return [row[0] for row in rows]
    
    def get_client_names(self, client_ids: List[str]) -> Dict[str, str]:
        """Получает имена клиентов по идентификаторам."""
        if not client_ids:
            return {}
        
        placeholders = ",".join(f":client_id_{i}" for i in range(len(client_ids)))
        query = text(f"""
            SELECT client_id, client_name
            FROM clients
            WHERE client_id IN ({placeholders})
        """)
        
        params = {f"client_id_{i}": cid for i, cid in enumerate(client_ids)}
        
        with self._db.get_connection() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()
        
        return {row[0]: row[1] for row in rows}
    
    def get_client_features(
        self,
        client_ids: List[str],
        reference_date: date
    ) -> pd.DataFrame:
        """Получает признаки клиентов для модели."""
        if not client_ids:
            return pd.DataFrame()
        
        # Упрощённая версия - загрузка базовых признаков
        placeholders = ",".join(f":client_id_{i}" for i in range(len(client_ids)))
        query = text(f"""
            SELECT 
                client_id,
                COUNT(*) as total_purchases,
                SUM(amount) as total_amount,
                MIN(purchase_date) as first_purchase_date,
                MAX(purchase_date) as last_purchase_date
            FROM sales_full
            WHERE client_id IN ({placeholders})
              AND purchase_date < :reference_date
            GROUP BY client_id
        """)
        
        params = {
            **{f"client_id_{i}": cid for i, cid in enumerate(client_ids)},
            "reference_date": reference_date
        }
        
        with self._db.get_connection() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=[
            'client_id', 'total_purchases', 'total_amount',
            'first_purchase_date', 'last_purchase_date'
        ])
        
        return df
