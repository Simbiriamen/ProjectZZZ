# -*- coding: utf-8 -*-
"""
Типизированная конфигурация приложения на базе pydantic-settings.

Поддерживает:
- Загрузку из .env файлов
- Environment variables
- Валидацию типов данных
- Значения по умолчанию

Использование:
    from src.config import Settings
    
    settings = Settings()
    
    db_url = settings.database.url
    model_path = settings.ml.models_dir
    batch_size = settings.app.batch_size
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
    def url_sync(self) -> str:
        """Возвращает синхронный URL для psycopg2."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    model_config = SettingsConfigDict(
        env_prefix="DB_",
        case_sensitive=False,
        extra="ignore"
    )


class MLSettings(BaseSettings):
    """Настройки машинного обучения."""
    
    models_dir: Path = Field(
        default=Path(__file__).parent.parent.parent / "models",
        description="Директория с моделями"
    )
    default_model: str = Field(default="lightgbm_v1", description="Модель по умолчанию")
    cache_predictions: bool = Field(default=True, description="Кэшировать предсказания")
    prediction_cache_ttl: int = Field(default=3600, description="TTL кэша предсказаний (сек)")
    batch_size: int = Field(default=200, ge=1, le=1000, description="Размер пакета для обработки")
    n_jobs: int = Field(default=-1, description="Количество потоков для ML")
    
    @field_validator("models_dir", mode="before")
    @classmethod
    def validate_models_dir(cls, v) -> Path:
        """Валидирует и создаёт директорию моделей."""
        path = Path(v) if isinstance(v, (str, Path)) else v
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    model_config = SettingsConfigDict(
        env_prefix="ML_",
        case_sensitive=False,
        extra="ignore"
    )


class AppSettings(BaseSettings):
    """Общие настройки приложения."""
    
    debug: bool = Field(default=False, description="Режим отладки")
    log_level: str = Field(default="INFO", description="Уровень логирования")
    batch_size: int = Field(default=200, ge=1, le=1000, description="Размер пакета по умолчанию")
    data_dir: Path = Field(
        default=Path(__file__).parent.parent.parent / "data",
        description="Директория с данными"
    )
    output_dir: Path = Field(
        default=Path(__file__).parent.parent.parent / "output",
        description="Директория для результатов"
    )
    
    @field_validator("data_dir", "output_dir", mode="before")
    @classmethod
    def validate_dirs(cls, v) -> Path:
        """Валидирует и создаёт директории."""
        path = Path(v) if isinstance(v, (str, Path)) else v
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    model_config = SettingsConfigDict(
        env_prefix="APP_",
        case_sensitive=False,
        extra="ignore"
    )


class Settings(BaseSettings):
    """
    Корневой класс конфигурации приложения.
    
    Объединяет все настройки в единую структуру.
    """
    
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    ml: MLSettings = Field(default_factory=MLSettings)
    app: AppSettings = Field(default_factory=AppSettings)
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    @classmethod
    def load(cls, env_file: Optional[Path] = None) -> "Settings":
        """
        Загружает конфигурацию из указанного .env файла.
        
        Args:
            env_file: Путь к .env файлу (по умолчанию .env в корне проекта)
        
        Returns:
            Настроенный экземпляр Settings
        """
        if env_file:
            return Settings(_env_file=env_file)
        return Settings()
