"""Инфраструктурный слой - реализации репозиториев, ML, БД."""

from .database.connection import Database

__all__ = ["Database"]
