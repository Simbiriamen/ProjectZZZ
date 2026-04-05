# -*- coding: utf-8 -*-
"""
Протоколы для репозиториев.

Определяют интерфейсы для работы с данными, обеспечивая:
- Инверсию зависимостей (DIP)
- Возможность подмены реализаций (моки в тестах)
- Слабую связанность между слоями
"""

from typing import Protocol, List, Dict, Any, Optional
from datetime import date
import pandas as pd


class ClientRepositoryProtocol(Protocol):
    """
    Протокол репозитория клиентов.
    
    Определяет методы для получения данных о клиентах.
    """
    
    def get_active_clients(
        self,
        months: int = 12,
        min_purchases: int = 3
    ) -> List[str]:
        """
        Получает список активных клиентов.
        
        Args:
            months: Период активности (месяцев)
            min_purchases: Минимальное количество покупок
        
        Returns:
            Список идентификаторов клиентов
        """
        ...
    
    def get_clients_for_visit(self, visit_date: date) -> List[str]:
        """
        Получает клиентов для визита на указанную дату.
        
        Args:
            visit_date: Дата визита
        
        Returns:
            Список идентификаторов клиентов
        """
        ...
    
    def get_client_names(self, client_ids: List[str]) -> Dict[str, str]:
        """
        Получает имена клиентов по идентификаторам.
        
        Args:
            client_ids: Список идентификаторов клиентов
        
        Returns:
            Словарь {client_id: name}
        """
        ...
    
    def get_client_features(
        self,
        client_ids: List[str],
        reference_date: date
    ) -> pd.DataFrame:
        """
        Получает признаки клиентов для модели.
        
        Args:
            client_ids: Список идентификаторов клиентов
            reference_date: Дата среза признаков
        
        Returns:
            DataFrame с признаками
        """
        ...


class PurchaseRepositoryProtocol(Protocol):
    """
    Протокол репозитория покупок.
    
    Определяет методы для получения данных о покупках.
    """
    
    def get_client_history(
        self,
        client_id: str,
        days: int = 90
    ) -> pd.DataFrame:
        """
        Получает историю покупок клиента.
        
        Args:
            client_id: Идентификатор клиента
            days: Период истории (дней)
        
        Returns:
            DataFrame с покупками
        """
        ...
    
    def get_raw_purchases_chunk(
        self,
        client_ids: List[str],
        months: int = 12
    ) -> pd.DataFrame:
        """
        Получает сырые данные о покупках для группы клиентов.
        
        Args:
            client_ids: Список идентификаторов клиентов
            months: Период (месяцев)
        
        Returns:
            DataFrame с покупками
        """
        ...
    
    def get_popular_skus(
        self,
        min_purchases: int = 2
    ) -> pd.Series:
        """
        Получает популярные SKU.
        
        Args:
            min_purchases: Минимальное количество покупок
        
        Returns:
            Series {sku_id: purchase_count}
        """
        ...
    
    def get_last_purchase_dates(
        self,
        client_ids: List[str]
    ) -> Dict[str, date]:
        """
        Получает даты последних покупок клиентов.
        
        Args:
            client_ids: Список идентификаторов клиентов
        
        Returns:
            Словарь {client_id: last_purchase_date}
        """
        ...


class CandidateRepositoryProtocol(Protocol):
    """
    Протокол репозитория кандидатов.
    
    Определяет методы для получения кандидатов на рекомендацию.
    """
    
    def get_candidates(
        self,
        client_ids: List[str],
        batch_size: int = 200,
        days: int = 90,
        top_n: int = 200
    ) -> pd.DataFrame:
        """
        Получает кандидатов для рекомендации.
        
        Args:
            client_ids: Список идентификаторов клиентов
            batch_size: Размер пакета для обработки
            days: Период анализа (дней)
            top_n: Количество топ кандидатов
        
        Returns:
            DataFrame с кандидатами
        """
        ...
    
    def filter_purchased_candidates(
        self,
        candidates: pd.DataFrame,
        client_id: str
    ) -> pd.DataFrame:
        """
        Фильтрует уже купленные кандидаты.
        
        Args:
            candidates: DataFrame с кандидатами
            client_id: Идентификатор клиента
        
        Returns:
            Отфильтрованный DataFrame
        """
        ...


class ModelRepositoryProtocol(Protocol):
    """
    Протокол репозитория моделей.
    
    Определяет методы для работы с ML моделями.
    """
    
    def load_active_model(self) -> Dict[str, Any]:
        """
        Загружает активную модель.
        
        Returns:
            Словарь с моделью и метаданными
        """
        ...
    
    def register_model(
        self,
        model_name: str,
        model_path: str,
        metrics: Dict[str, float],
        is_active: bool = False
    ) -> None:
        """
        Регистрирует модель в реестре.
        
        Args:
            model_name: Имя модели
            model_path: Путь к файлу модели
            metrics: Метрики качества
            is_active: Сделать модель активной
        """
        ...
    
    def promote_model(self, model_name: str) -> None:
        """
        Делает модель активной.
        
        Args:
            model_name: Имя модели
        """
        ...
    
    def get_model_info(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о модели.
        
        Args:
            model_name: Имя модели
        
        Returns:
            Информация о модели или None
        """
        ...
