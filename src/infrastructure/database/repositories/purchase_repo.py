# -*- coding: utf-8 -*-
"""
Репозиторий покупок.

Реализация PurchaseRepositoryProtocol для работы с БД.
"""

import logging
from typing import List, Dict, Optional
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from ....domain.protocols.repositories import PurchaseRepositoryProtocol
from ..connection import Database

logger = logging.getLogger(__name__)


class PurchaseRepository(PurchaseRepositoryProtocol):
    """
    Репозиторий для работы с покупками.
    
    Реализует методы для получения данных о покупках из БД.
    """
    
    def __init__(self, db: Database):
        """
        Args:
            db: Экземпляр подключения к БД
        """
        self._db = db
    
    def get_client_history(
        self,
        client_id: str,
        days: int = 90
    ) -> pd.DataFrame:
        """Получает историю покупок клиента."""
        cutoff_date = date.today() - timedelta(days=days)
        
        query = text("""
            SELECT 
                purchase_date,
                sku_id,
                amount,
                quantity
            FROM sales_full
            WHERE client_id = :client_id
              AND purchase_date >= :cutoff_date
            ORDER BY purchase_date DESC
        """)
        
        with self._db.get_connection() as conn:
            result = conn.execute(
                query,
                {"client_id": client_id, "cutoff_date": cutoff_date}
            )
            rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame(columns=['purchase_date', 'sku_id', 'amount', 'quantity'])
        
        df = pd.DataFrame(rows, columns=['purchase_date', 'sku_id', 'amount', 'quantity'])
        return df
    
    def get_raw_purchases_chunk(
        self,
        client_ids: List[str],
        months: int = 12
    ) -> pd.DataFrame:
        """Получает сырые данные о покупках для группы клиентов."""
        if not client_ids:
            return pd.DataFrame()
        
        cutoff_date = date.today() - timedelta(days=months * 30)
        
        placeholders = ",".join(f":client_id_{i}" for i in range(len(client_ids)))
        query = text(f"""
            SELECT 
                client_id,
                purchase_date,
                sku_id,
                amount,
                quantity
            FROM sales_full
            WHERE client_id IN ({placeholders})
              AND purchase_date >= :cutoff_date
            ORDER BY client_id, purchase_date
        """)
        
        params = {
            **{f"client_id_{i}": cid for i, cid in enumerate(client_ids)},
            "cutoff_date": cutoff_date
        }
        
        with self._db.get_connection() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=[
            'client_id', 'purchase_date', 'sku_id', 'amount', 'quantity'
        ])
        
        return df
    
    def get_popular_skus(
        self,
        min_purchases: int = 2
    ) -> pd.Series:
        """Получает популярные SKU."""
        query = text("""
            SELECT sku_id, COUNT(*) as purchase_count
            FROM sales_full
            GROUP BY sku_id
            HAVING COUNT(*) >= :min_purchases
            ORDER BY purchase_count DESC
        """)
        
        with self._db.get_connection() as conn:
            result = conn.execute(query, {"min_purchases": min_purchases})
            rows = result.fetchall()
        
        if not rows:
            return pd.Series(dtype=int)
        
        df = pd.DataFrame(rows, columns=['sku_id', 'purchase_count'])
        return df.set_index('sku_id')['purchase_count']
    
    def get_last_purchase_dates(
        self,
        client_ids: List[str]
    ) -> Dict[str, date]:
        """Получает даты последних покупок клиентов."""
        if not client_ids:
            return {}
        
        placeholders = ",".join(f":client_id_{i}" for i in range(len(client_ids)))
        query = text(f"""
            SELECT client_id, MAX(purchase_date) as last_purchase_date
            FROM sales_full
            WHERE client_id IN ({placeholders})
            GROUP BY client_id
        """)
        
        params = {f"client_id_{i}": cid for i, cid in enumerate(client_ids)}
        
        with self._db.get_connection() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()
        
        return {row[0]: row[1] for row in rows}
