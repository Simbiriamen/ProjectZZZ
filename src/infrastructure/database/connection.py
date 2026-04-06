# -*- coding: utf-8 -*-
"""
Подключение к базе данных.

Инкапсулирует логику создания и управления подключением.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager

from sqlalchemy import create_engine, text, Engine
from sqlalchemy.pool import QueuePool

from ...config.settings import Settings, DatabaseSettings

logger = logging.getLogger(__name__)


class Database:
    """
    Менеджер подключений к базе данных.
    
    Использование:
        # Из настроек
        settings = Settings()
        db = Database.from_settings(settings.database)
        
        # Или напрямую из URL
        db = Database.from_url("postgresql://user:pass@host:5432/db")
        
        # Использование
        with db.get_connection() as conn:
            result = conn.execute(text("SELECT 1"))
    """
    
    def __init__(
        self,
        engine: Engine,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Args:
            engine: SQLAlchemy engine
            config: Дополнительная конфигурация
        """
        self.engine = engine
        self.config = config or {}
    
    @classmethod
    def from_settings(cls, db_settings: DatabaseSettings) -> "Database":
        """
        Создаёт подключение из настроек.
        
        Args:
            db_settings: Настройки БД
        
        Returns:
            Database instance
        """
        logger.info(f"🔌 Подключение к БД {db_settings.database}@{db_settings.host}...")
        
        engine = create_engine(
            db_settings.url,
            pool_size=db_settings.pool_size,
            max_overflow=db_settings.max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            poolclass=QueuePool
        )
        
        logger.info("   ✅ Подключение успешно")
        return cls(engine, db_settings.model_dump())
    
    @classmethod
    def from_url(
        cls,
        url: str,
        pool_size: int = 20,
        max_overflow: int = 40
    ) -> "Database":
        """
        Создаёт подключение из URL.
        
        Args:
            url: URL подключения
            pool_size: Размер пула
            max_overflow: Максимальное переполнение
        
        Returns:
            Database instance
        """
        engine = create_engine(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            poolclass=QueuePool
        )
        
        return cls(engine)
    
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
            logger.error(f"❌ Ошибка транзакции: {e}")
            raise
        finally:
            conn.close()
    
    def test_connection(self) -> bool:
        """Проверяет подключение к БД."""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
                row = result.fetchone()
                return row is not None and row[0] == 1
        except Exception as e:
            logger.error(f"Ошибка подключения: {e}")
            return False
    
    def dispose(self) -> None:
        """Закрывает все подключения."""
        self.engine.dispose()
        logger.info("🔌 Подключения к БД закрыты")
