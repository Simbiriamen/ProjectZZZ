# -*- coding: utf-8 -*-
"""
Сущность Рекомендации.

Представляет рекомендацию товара для клиента.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Dict, Any
from enum import Enum


class RecommendationSource(Enum):
    """Источник рекомендации."""
    ML_MODEL = "ml_model"
    POPULAR_ITEMS = "popular_items"
    FALLBACK = "fallback"
    MANUAL = "manual"


@dataclass
class Recommendation:
    """
    Бизнес-сущность Рекомендации.
    
    Attributes:
        client_id: Идентификатор клиента
        sku_id: Рекомендуемый артикул
        score: Score рекомендации (вероятность покупки)
        rank: Позиция в списке рекомендаций
        source: Источник рекомендации
        generated_at: Дата генерации рекомендации
        metadata: Дополнительные данные
    """
    
    client_id: str
    sku_id: str
    score: float
    rank: int = 0
    source: RecommendationSource = RecommendationSource.ML_MODEL
    generated_at: date = field(default_factory=date.today)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Конвертирует сущность в словарь."""
        return {
            "client_id": self.client_id,
            "sku_id": self.sku_id,
            "score": self.score,
            "rank": self.rank,
            "source": self.source.value,
            "generated_at": str(self.generated_at),
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Recommendation":
        """Создаёт сущность из словаря."""
        return cls(
            client_id=data["client_id"],
            sku_id=data["sku_id"],
            score=data["score"],
            rank=data.get("rank", 0),
            source=RecommendationSource(data.get("source", "ml_model")),
            generated_at=(
                date.fromisoformat(data["generated_at"])
                if data.get("generated_at")
                else date.today()
            ),
            metadata=data.get("metadata", {}),
        )
    
    def __lt__(self, other: "Recommendation") -> bool:
        """Сравнение по score для сортировки."""
        return self.score > other.score  # Больше = лучше
    
    def __eq__(self, other: object) -> bool:
        """Проверка равенства."""
        if not isinstance(other, Recommendation):
            return False
        return self.client_id == other.client_id and self.sku_id == other.sku_id
    
    def __hash__(self) -> int:
        """Хэш для использования в множествах."""
        return hash((self.client_id, self.sku_id))
