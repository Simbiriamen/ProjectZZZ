"""
Конфигурация приложения на базе pydantic-settings.

Использует dependency-injector для внедрения зависимостей.
"""

from pathlib import Path
from typing import Optional, List
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Настройки подключения к базе данных."""

    host: str = Field(default="localhost", description="Хост БД")
    port: int = Field(default=5432, description="Порт БД")
    database: str = Field(default="projectzzz", description="Имя БД")
    user: str = Field(default="postgres", description="Пользователь БД")
    password: str = Field(default="postgres", description="Пароль БД")
    pool_size: int = Field(default=20, ge=1, le=100, description="Размер пула подключений")
    max_overflow: int = Field(default=40, ge=0, le=100, description="Максимальное переполнение пула")

    @property
    def url(self) -> str:
        """Возвращает URL подключения к БД."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def url_async(self) -> str:
        """Возвращает асинхронный URL для asyncpg."""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    model_config = SettingsConfigDict(
        env_prefix="DB_",
        case_sensitive=False,
        extra="ignore",
    )


class MLSettings(BaseSettings):
    """Настройки машинного обучения."""

    models_dir: Path = Field(default=Path("models"), description="Директория моделей")
    features_cache_dir: Path = Field(default=Path("data/cache"), description="Кэш признаков")
    default_model_version: str = Field(default="v1.0", description="Версия модели по умолчанию")
    min_confidence_threshold: float = Field(default=0.3, ge=0.0, le=1.0, description="Минимальный порог уверенности")

    model_config = SettingsConfigDict(
        env_prefix="ML_",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("models_dir", "features_cache_dir")
    @classmethod
    def validate_paths(cls, v: Path) -> Path:
        """Валидация путей."""
        if not v.is_absolute():
            return Path(__file__).parent.parent.parent / v
        return v


class AppSettings(BaseSettings):
    """Общие настройки приложения."""

    batch_size: int = Field(default=1000, ge=100, le=10000, description="Размер пакета обработки")
    debug: bool = Field(default=False, description="Режим отладки")
    log_level: str = Field(default="INFO", description="Уровень логирования")
    ab_test_enabled: bool = Field(default=True, description="Включить A/B тестирование")
    ab_test_split: float = Field(default=0.5, ge=0.0, le=1.0, description="Доля трафика для теста B")

    model_config = SettingsConfigDict(
        env_prefix="APP_",
        case_sensitive=False,
        extra="ignore",
    )


class Settings(BaseSettings):
    """Основной класс настроек приложения."""

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    ml: MLSettings = Field(default_factory=MLSettings)
    app: AppSettings = Field(default_factory=AppSettings)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# Глобальный экземпляр (для обратной совместимости)
settings = Settings()

__all__ = ["Settings", "settings", "DatabaseSettings", "MLSettings", "AppSettings"]
