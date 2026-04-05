"""Модуль работы с базой данных."""

from .connection import Database
from .repositories.client_repo import ClientRepository
from .repositories.purchase_repo import PurchaseRepository
from .repositories.candidate_repo import CandidateRepository

__all__ = [
    "Database",
    "ClientRepository",
    "PurchaseRepository",
    "CandidateRepository",
]
