# -*- coding: utf-8 -*-
"""
database.py v1.0
🔧 ЦЕНТРАЛИЗОВАННОЕ УПРАВЛЕНИЕ БД
Назначение:
  - Единая точка подключения к БД
  - Репозитории для работы с данными
  - Абстракция над SQL запросами

Использование:
    from src.database import Database, ClientRepository, PurchaseRepository
    
    db = Database.from_config()
    
    clients = ClientRepository(db).get_active_clients(months=12)
    purchases = PurchaseRepository(db).get_client_history('C123', days=90)
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import date, timedelta
from contextlib import contextmanager

import pandas as pd
import yaml
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.pool import QueuePool

from .config_loader import load_config, get_database_url

logger = logging.getLogger(__name__)


# ==============================================================================
# КЛАССЫ
# ==============================================================================

class Database:
    """
    Менеджер подключений к базе данных.
    """
    
    def __init__(
        self,
        engine: Engine,
        config: Optional[Dict] = None
    ):
        """
        Args:
            engine: SQLAlchemy engine
            config: Конфигурация базы данных
        """
        self.engine = engine
        self.config = config or {}
    
    @classmethod
    def from_config(cls, config_path: Optional[Path] = None) -> 'Database':
        """
        Создаёт подключение из конфигурации.
        
        Args:
            config_path: Путь к config.yaml
        
        Returns:
            Database instance
        """
        config = load_config(config_path=config_path or (Path(__file__).parent.parent / "config" / "config.yaml"))
        
        db_config = config['database']
        
        # 🔧 Оптимизированная конфигурация подключения
        engine = create_engine(
            get_database_url(config),
            pool_size=20,
            max_overflow=40,
            pool_pre_ping=True,
            pool_recycle=3600,
            poolclass=QueuePool
        )
        
        return cls(engine, db_config)
    
    @contextmanager
    def get_connection(self):
        """
        Контекстный менеджер для подключения.
        
        Usage:
            with db.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
        """
        conn = self.engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def dispose(self):
        """Освобождает ресурсы подключения"""
        self.engine.dispose()
    
    def test_connection(self) -> bool:
        """Проверяет подключение к БД"""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"❌ Ошибка подключения: {e}")
            return False


class ClientRepository:
    """
    Репозиторий для работы с клиентами.
    """
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_active_clients(self, months: int = 12, min_purchases: int = 3) -> List[str]:
        """
        Получает список активных клиентов.
        
        Args:
            months: Период в месяцах
            min_purchases: Минимальное количество покупок
        
        Returns:
            Список client_id
        """
        query = text("""
            SELECT DISTINCT client_id
            FROM purchases
            WHERE purchase_date >= CURRENT_DATE - INTERVAL :months MONTH
            GROUP BY client_id
            HAVING COUNT(*) >= :min_purchases
            ORDER BY client_id
        """)
        
        df = pd.read_sql(
            query,
            self.db.engine,
            params={'months': months, 'min_purchases': min_purchases}
        )
        
        logger.info(f"📊 Найдено активных клиентов: {len(df):,}")
        return df['client_id'].tolist()
    
    def get_clients_for_visit(self, visit_date: date) -> List[str]:
        """
        Получает клиентов на визит.
        
        Args:
            visit_date: Дата визита
        
        Returns:
            Список client_id
        """
        query = text("""
            SELECT DISTINCT client_id
            FROM visits_schedule
            WHERE planned_visit_date = :visit_date
              AND status != 'completed'
            ORDER BY client_id
        """)
        
        df = pd.read_sql(
            query,
            self.db.engine,
            params={'visit_date': visit_date}
        )
        
        if not df.empty:
            logger.info(f"📊 Найдено клиентов из расписания: {len(df)}")
            return df['client_id'].tolist()
        
        # Fallback: активные за 90 дней
        return self.get_active_clients(months=3)
    
    def get_client_names(self, client_ids: List[str]) -> Dict[str, str]:
        """
        Получает имена клиентов.
        
        Args:
            client_ids: Список идентификаторов
        
        Returns:
            Dict {client_id: client_name}
        """
        if not client_ids:
            return {}
        
        query = text("""
            SELECT client_id, client_name
            FROM clients
            WHERE client_id = ANY(:client_ids)
        """)
        
        df = pd.read_sql(query, self.db.engine, params={'client_ids': client_ids})
        return dict(zip(df['client_id'], df['client_name']))


class PurchaseRepository:
    """
    Репозиторий для работы с покупками.
    """
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_client_history(
        self,
        client_id: str,
        days: int = 90,
        sku_id: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Получает историю покупок клиента.
        
        Args:
            client_id: Идентификатор клиента
            days: Период в днях
            sku_id: Фильтр по SKU (опционально)
        
        Returns:
            DataFrame с покупками
        """
        base_query = """
            SELECT client_id, sku_id, purchase_date, quantity, amount
            FROM purchases
            WHERE client_id = :client_id
              AND purchase_date >= CURRENT_DATE - INTERVAL :days DAY
        """
        
        if sku_id:
            base_query += " AND sku_id = :sku_id"
        
        base_query += " ORDER BY purchase_date"
        
        query = text(base_query)
        params = {'client_id': client_id, 'days': days}
        
        if sku_id:
            params['sku_id'] = sku_id
        
        df = pd.read_sql(query, self.db.engine, params=params)
        df['purchase_date'] = pd.to_datetime(df['purchase_date'])
        
        return df
    
    def get_raw_purchases_chunk(
        self,
        client_ids: List[str],
        months: int = 12
    ) -> pd.DataFrame:
        """
        Загружает данные для пакета клиентов.
        
        Args:
            client_ids: Список идентификаторов
            months: Период в месяцах
        
        Returns:
            DataFrame с покупками
        """
        if not client_ids:
            return pd.DataFrame()
        
        query = text("""
            SELECT
                client_id,
                sku_id,
                purchase_date
            FROM purchases
            WHERE purchase_date >= CURRENT_DATE - INTERVAL :months MONTH
              AND client_id = ANY(:client_ids)
            ORDER BY client_id, sku_id, purchase_date
        """)
        
        df = pd.read_sql(
            query,
            self.db.engine,
            params={'months': months, 'client_ids': client_ids}
        )
        
        df['purchase_date'] = pd.to_datetime(df['purchase_date'])
        return df
    
    def get_popular_skus(self, min_purchases: int = 2) -> pd.Series:
        """
        Получает популярные SKU.
        
        Args:
            min_purchases: Минимальное количество покупок
        
        Returns:
            Series с popular sku_id
        """
        query = text("""
            SELECT sku_id, COUNT(*) as purchase_count
            FROM purchases
            WHERE purchase_date >= CURRENT_DATE - INTERVAL '12' MONTH
            GROUP BY sku_id
            HAVING COUNT(*) >= :min_purchases
            ORDER BY purchase_count DESC
        """)
        
        df = pd.read_sql(query, self.db.engine, params={'min_purchases': min_purchases})
        return df['sku_id']


class CandidateRepository:
    """
    Репозиторий для работы с кандидатами на рекомендацию.
    """
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_candidates(
        self,
        client_ids: List[str],
        batch_size: int = 200,
        days: int = 90,
        top_n: int = 200
    ) -> pd.DataFrame:
        """
        Получает кандидатов для рекомендаций.
        
        Args:
            client_ids: Список идентификаторов клиентов
            batch_size: Размер пакета для обработки
            days: Период для истории
            top_n: Количество топ SKU
        
        Returns:
            DataFrame с кандидатами
        """
        if not client_ids:
            return pd.DataFrame()
        
        all_dfs = []
        
        for i in range(0, len(client_ids), batch_size):
            batch = client_ids[i:i + batch_size]
            
            query = text("""
            WITH client_history AS (
                SELECT DISTINCT client_id, sku_id
                FROM sales_enriched
                WHERE client_id = ANY(:client_ids)
                  AND purchase_date >= CURRENT_DATE - INTERVAL :days DAY
            ),
            global_top_skus AS (
                SELECT
                    s.sku_id,
                    se.article,
                    s.brand, s.sku_name, s.marketing_group1 AS marketing_group,
                    s.category, s.price, s.margin, s.stock, s.is_new, s.applicability,
                    COUNT(*) as sales_cnt
                FROM sales_enriched se
                JOIN skus s ON se.sku_id = s.sku_id
                WHERE s.stock >= 1
                  AND se.purchase_date >= CURRENT_DATE - INTERVAL :days DAY
                GROUP BY s.sku_id, se.article, s.brand, s.sku_name, s.marketing_group1,
                         s.category, s.price, s.margin, s.stock, s.is_new, s.applicability
                ORDER BY sales_cnt DESC
                LIMIT :top_n
            ),
            candidates AS (
                SELECT
                    se.client_id, se.sku_id, se.article, s.brand, s.sku_name,
                    s.marketing_group1 AS marketing_group, s.category, s.price, s.margin,
                    s.stock, s.is_new, s.applicability,
                    0 AS is_new_for_client,
                    COALESCE(se.days_since_last_purchase, 999) AS days_since_last_purchase,
                    COALESCE(se.frequency_30d, 0) AS frequency_30d,
                    COALESCE(se.frequency_90d, 0) AS frequency_90d,
                    COALESCE(se.rolling_sales_2w, 0) AS rolling_sales_2w,
                    COALESCE(se.rolling_sales_4w, 0) AS rolling_sales_4w,
                    COALESCE(se.rolling_sales_8w, 0) AS rolling_sales_8w,
                    COALESCE(se.global_popularity, 0) AS global_popularity,
                    COALESCE(se.portfolio_diversity, 1) AS portfolio_diversity,
                    COALESCE(se.group_trend_6m, 0) AS group_trend_6m,
                    COALESCE(se.group_share_in_portfolio, 0) AS group_share_in_portfolio,
                    COALESCE(se.days_since_last_purchase_group, 999) AS days_since_last_purchase_group
                FROM sales_enriched se
                JOIN skus s ON se.sku_id = s.sku_id
                WHERE se.client_id = ANY(:client_ids) AND s.stock >= 1
                  AND se.purchase_date = (
                      SELECT MAX(purchase_date) FROM sales_enriched se2
                      WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
                  )
                UNION
                SELECT
                    c.client_id, g.sku_id, g.article, g.brand, g.sku_name,
                    g.marketing_group, g.category, g.price, g.margin, g.stock, g.is_new,
                    g.applicability,
                    1 AS is_new_for_client,
                    999 AS days_since_last_purchase, 0 AS frequency_30d, 0 AS frequency_90d,
                    0 AS rolling_sales_2w, 0 AS rolling_sales_4w, 0 AS rolling_sales_8w,
                    0 AS global_popularity, 1 AS portfolio_diversity, 0 AS group_trend_6m,
                    0 AS group_share_in_portfolio, 999 AS days_since_last_purchase_group
                FROM (SELECT DISTINCT client_id FROM client_history) c
                CROSS JOIN global_top_skus g
                LEFT JOIN client_history ch ON c.client_id = ch.client_id AND g.sku_id = ch.sku_id
                WHERE ch.sku_id IS NULL
            )
            SELECT * FROM candidates ORDER BY client_id, sku_id
            """)
            
            df = pd.read_sql(
                query,
                self.db.engine,
                params={'client_ids': batch, 'days': days, 'top_n': top_n}
            )
            
            all_dfs.append(df)
        
        if all_dfs:
            result = pd.concat(all_dfs, ignore_index=True)
            logger.info(f"📊 Загружено кандидатов: {len(result):,}")
            return result
        
        return pd.DataFrame()


# ==============================================================================
# FACTORY
# ==============================================================================

def create_repositories(db: Database) -> Dict[str, Any]:
    """
    Создаёт все репозитории.
    
    Args:
        db: Database instance
    
    Returns:
        Dict с репозиториями
    """
    return {
        'clients': ClientRepository(db),
        'purchases': PurchaseRepository(db),
        'candidates': CandidateRepository(db)
    }


# ==============================================================================
# CLI
# ==============================================================================

if __name__ == "__main__":
    import sys
    
    print("="*70)
    print("🔧 ProjectZZZ - Database Connection Test")
    print("="*70)
    
    try:
        db = Database.from_config()
        
        if db.test_connection():
            print("\n✅ Подключение к БД успешно!")
            
            # Тест репозиториев
            client_repo = ClientRepository(db)
            clients = client_repo.get_active_clients(months=3)
            print(f"📊 Активных клиентов (3 мес): {len(clients)}")
            
            db.dispose()
            print("\n✅ Все тесты пройдены")
        else:
            print("\n❌ Не удалось подключиться к БД")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        sys.exit(1)
