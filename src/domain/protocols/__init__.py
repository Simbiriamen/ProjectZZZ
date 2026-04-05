"""Протоколы (интерфейсы) для репозиториев."""

from .repositories import (
    ClientRepositoryProtocol,
    PurchaseRepositoryProtocol,
    CandidateRepositoryProtocol,
    ModelRepositoryProtocol,
)

__all__ = [
    "ClientRepositoryProtocol",
    "PurchaseRepositoryProtocol",
    "CandidateRepositoryProtocol",
    "ModelRepositoryProtocol",
]
