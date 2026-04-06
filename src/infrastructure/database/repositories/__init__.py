"""Репозитории для работы с данными."""

from .client_repo import ClientRepository
from .purchase_repo import PurchaseRepository
from .candidate_repo import CandidateRepository

__all__ = [
    "ClientRepository",
    "PurchaseRepository",
    "CandidateRepository",
]
