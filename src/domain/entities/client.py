# -*- coding: utf-8 -*-
"""
Сущность Клиент.

Представляет клиента в системе рекомендаций.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, List


@dataclass
class Client:
    """
    Бизнес-сущность Клиент.
    
    Attributes:
        client_id: Уникальный идентификатор клиента
        name: Имя клиента (опционально)
        first_purchase_date: Дата первой покупки
        last_purchase_date: Дата последней покупки
        total_purchases: Общее количество покупок
        total_amount: Общая сумма покупок
        segment: Сегмент клиента (опционально)
        is_active: Флаг активности клиента
    """
    
    client_id: str
    name: Optional[str] = None
    first_purchase_date: Optional[date] = None
    last_purchase_date: Optional[date] = None
    total_purchases: int = 0
    total_amount: float = 0.0
    segment: Optional[str] = None
    is_active: bool = True
    purchased_skus: List[str] = field(default_factory=list)
    
    @property
    def days_since_last_purchase(self) -> Optional[int]:
        """Количество дней с последней покупки."""
        if self.last_purchase_date is None:
            return None
        return (date.today() - self.last_purchase_date).days
    
    @property
    def average_purchase_amount(self) -> float:
        """Средняя сумма покупки."""
        if self.total_purchases == 0:
            return 0.0
        return self.total_amount / self.total_purchases
    
    def mark_inactive(self) -> None:
        """Помечает клиента как неактивного."""
        self.is_active = False
    
    def add_purchase(
        self,
        sku: str,
        amount: float,
        purchase_date: date
    ) -> None:
        """
        Добавляет информацию о покупке.
        
        Args:
            sku: Артикул товара
            amount: Сумма покупки
            purchase_date: Дата покупки
        """
        self.total_purchases += 1
        self.total_amount += amount
        
        if sku not in self.purchased_skus:
            self.purchased_skus.append(sku)
        
        if self.first_purchase_date is None or purchase_date < self.first_purchase_date:
            self.first_purchase_date = purchase_date
        
        if self.last_purchase_date is None or purchase_date > self.last_purchase_date:
            self.last_purchase_date = purchase_date
    
    def to_dict(self) -> dict:
        """Конвертирует сущность в словарь."""
        return {
            "client_id": self.client_id,
            "name": self.name,
            "first_purchase_date": str(self.first_purchase_date) if self.first_purchase_date else None,
            "last_purchase_date": str(self.last_purchase_date) if self.last_purchase_date else None,
            "total_purchases": self.total_purchases,
            "total_amount": self.total_amount,
            "segment": self.segment,
            "is_active": self.is_active,
            "purchased_skus": self.purchased_skus,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Client":
        """Создаёт сущность из словаря."""
        return cls(
            client_id=data["client_id"],
            name=data.get("name"),
            first_purchase_date=(
                date.fromisoformat(data["first_purchase_date"])
                if data.get("first_purchase_date")
                else None
            ),
            last_purchase_date=(
                date.fromisoformat(data["last_purchase_date"])
                if data.get("last_purchase_date")
                else None
            ),
            total_purchases=data.get("total_purchases", 0),
            total_amount=data.get("total_amount", 0.0),
            segment=data.get("segment"),
            is_active=data.get("is_active", True),
            purchased_skus=data.get("purchased_skus", []),
        )
