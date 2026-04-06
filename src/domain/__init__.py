"""
Доменная область - бизнес-сущности и логика.

Содержит:
- entities: бизнес-объекты (Client, SKU, Recommendation)
- services: доменные сервисы с бизнес-логикой
- protocols: абстракции (интерфейсы) для репозиториев
- exceptions: доменные исключения
"""

from .entities.client import Client
from .entities.sku import SKU
from .entities.recommendation import Recommendation
from .protocols.repositories import (
    ClientRepositoryProtocol,
    PurchaseRepositoryProtocol,
    CandidateRepositoryProtocol,
    ModelRepositoryProtocol,
)

__all__ = [
    # Entities
    "Client",
    "SKU",
    "Recommendation",
    # Protocols
    "ClientRepositoryProtocol",
    "PurchaseRepositoryProtocol",
    "CandidateRepositoryProtocol",
    "ModelRepositoryProtocol",
]
