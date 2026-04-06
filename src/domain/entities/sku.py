# -*- coding: utf-8 -*-
"""
Сущность SKU (товар).

Представляет товар/артикул в системе рекомендаций.
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class SKU:
    """
    Бизнес-сущность SKU (Stock Keeping Unit).
    
    Attributes:
        sku_id: Уникальный идентификатор артикула
        name: Наименование товара
        category: Категория товара
        subcategory: Подкатегория товара
        brand: Бренд товара
        price: Цена товара
        is_active: Флаг активности товара
        purchase_count: Количество покупок этого товара
        client_ids: Список клиентов, купивших этот товар
    """
    
    sku_id: str
    name: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    brand: Optional[str] = None
    price: float = 0.0
    is_active: bool = True
    purchase_count: int = 0
    client_ids: List[str] = field(default_factory=list)
    
    @property
    def is_popular(self) -> bool:
        """Проверяет, является ли товар популярным."""
        return self.purchase_count >= 10
    
    def add_purchase(self, client_id: str) -> None:
        """
        Регистрирует покупку товара клиентом.
        
        Args:
            client_id: Идентификатор клиента
        """
        self.purchase_count += 1
        if client_id not in self.client_ids:
            self.client_ids.append(client_id)
    
    def to_dict(self) -> dict:
        """Конвертирует сущность в словарь."""
        return {
            "sku_id": self.sku_id,
            "name": self.name,
            "category": self.category,
            "subcategory": self.subcategory,
            "brand": self.brand,
            "price": self.price,
            "is_active": self.is_active,
            "purchase_count": self.purchase_count,
            "client_ids": self.client_ids,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "SKU":
        """Создаёт сущность из словаря."""
        return cls(
            sku_id=data["sku_id"],
            name=data.get("name"),
            category=data.get("category"),
            subcategory=data.get("subcategory"),
            brand=data.get("brand"),
            price=data.get("price", 0.0),
            is_active=data.get("is_active", True),
            purchase_count=data.get("purchase_count", 0),
            client_ids=data.get("client_ids", []),
        )
