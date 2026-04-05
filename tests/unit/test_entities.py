# -*- coding: utf-8 -*-
"""
Unit тесты для доменных сущностей.

Тестируют Client, SKU, Recommendation.
"""

import pytest
from datetime import date, timedelta
from src.domain.entities.client import Client
from src.domain.entities.sku import SKU
from src.domain.entities.recommendation import Recommendation, RecommendationSource


class TestClient:
    """Тесты сущности Client."""
    
    def test_create_client(self):
        """Создание клиента."""
        client = Client(client_id="C123")
        
        assert client.client_id == "C123"
        assert client.is_active is True
        assert client.total_purchases == 0
        assert client.total_amount == 0.0
    
    def test_add_purchase(self):
        """Добавление покупки."""
        client = Client(client_id="C123")
        today = date.today()
        
        client.add_purchase("SKU001", 1000.0, today)
        
        assert client.total_purchases == 1
        assert client.total_amount == 1000.0
        assert "SKU001" in client.purchased_skus
        assert client.first_purchase_date == today
        assert client.last_purchase_date == today
    
    def test_days_since_last_purchase(self):
        """Расчёт дней с последней покупки."""
        client = Client(client_id="C123")
        past_date = date.today() - timedelta(days=30)
        
        client.add_purchase("SKU001", 1000.0, past_date)
        
        assert client.days_since_last_purchase == 30
    
    def test_average_purchase_amount(self):
        """Расчёт средней суммы покупки."""
        client = Client(client_id="C123")
        
        client.add_purchase("SKU001", 1000.0, date.today())
        client.add_purchase("SKU002", 2000.0, date.today())
        
        assert client.average_purchase_amount == 1500.0
    
    def test_to_dict(self):
        """Конвертация в словарь."""
        client = Client(client_id="C123", name="Test Client")
        
        data = client.to_dict()
        
        assert data["client_id"] == "C123"
        assert data["name"] == "Test Client"
        assert isinstance(data, dict)
    
    def test_from_dict(self):
        """Создание из словаря."""
        data = {
            "client_id": "C123",
            "name": "Test Client",
            "total_purchases": 5,
            "total_amount": 5000.0
        }
        
        client = Client.from_dict(data)
        
        assert client.client_id == "C123"
        assert client.name == "Test Client"
        assert client.total_purchases == 5


class TestSKU:
    """Тесты сущности SKU."""
    
    def test_create_sku(self):
        """Создание SKU."""
        sku = SKU(sku_id="SKU001")
        
        assert sku.sku_id == "SKU001"
        assert sku.is_active is True
        assert sku.purchase_count == 0
    
    def test_add_purchase(self):
        """Добавление покупки."""
        sku = SKU(sku_id="SKU001")
        
        sku.add_purchase("C123")
        sku.add_purchase("C456")
        sku.add_purchase("C123")  # Повторная покупка
        
        assert sku.purchase_count == 3
        assert len(sku.client_ids) == 2  # Уникальные клиенты
    
    def test_is_popular(self):
        """Проверка популярности."""
        sku = SKU(sku_id="SKU001")
        
        assert sku.is_popular is False
        
        for i in range(10):
            sku.add_purchase(f"C{i}")
        
        assert sku.is_popular is True


class TestRecommendation:
    """Тесты сущности Recommendation."""
    
    def test_create_recommendation(self):
        """Создание рекомендации."""
        rec = Recommendation(
            client_id="C123",
            sku_id="SKU001",
            score=0.85
        )
        
        assert rec.client_id == "C123"
        assert rec.sku_id == "SKU001"
        assert rec.score == 0.85
        assert rec.source == RecommendationSource.ML_MODEL
    
    def test_comparison(self):
        """Сравнение рекомендаций."""
        rec1 = Recommendation(client_id="C1", sku_id="S1", score=0.9)
        rec2 = Recommendation(client_id="C1", sku_id="S2", score=0.7)
        
        assert rec1 < rec2  # Больше score = лучше (меньше в сортировке)
    
    def test_to_dict(self):
        """Конвертация в словарь."""
        rec = Recommendation(
            client_id="C123",
            sku_id="SKU001",
            score=0.85,
            rank=1
        )
        
        data = rec.to_dict()
        
        assert data["client_id"] == "C123"
        assert data["score"] == 0.85
        assert data["rank"] == 1
