"""
Тесты для модуля конфигурации.

Проверяют:
- Корректную загрузку настроек
- Валидацию значений
- Работу DI контейнера
"""

import pytest
from pathlib import Path

from src.infrastructure.config import (
    Settings,
    DatabaseSettings,
    MLSettings,
    AppSettings,
    container,
)


class TestDatabaseSettings:
    """Тесты настроек базы данных."""

    def test_default_values(self):
        """Проверка значений по умолчанию."""
        settings = DatabaseSettings()
        
        assert settings.host == "localhost"
        assert settings.port == 5432
        assert settings.database == "projectzzz"
        assert settings.user == "postgres"
        assert settings.password == "postgres"
        assert settings.pool_size == 20
        assert settings.max_overflow == 40

    def test_url_generation(self):
        """Проверка генерации URL подключения."""
        settings = DatabaseSettings(
            host="db.example.com",
            port=5433,
            database="test_db",
            user="test_user",
            password="secret",
        )
        
        expected = "postgresql://test_user:secret@db.example.com:5433/test_db"
        assert settings.url == expected

    def test_url_async_generation(self):
        """Проверка генерации асинхронного URL."""
        settings = DatabaseSettings()
        assert "postgresql+asyncpg://" in settings.url_async

    def test_pool_size_validation(self):
        """Проверка валидации размера пула."""
        with pytest.raises(ValueError):
            DatabaseSettings(pool_size=0)
        
        with pytest.raises(ValueError):
            DatabaseSettings(pool_size=101)

    def test_max_overflow_validation(self):
        """Проверка валидации переполнения пула."""
        with pytest.raises(ValueError):
            DatabaseSettings(max_overflow=-1)
        
        with pytest.raises(ValueError):
            DatabaseSettings(max_overflow=101)


class TestMLSettings:
    """Тесты настроек машинного обучения."""

    def test_default_values(self):
        """Проверка значений по умолчанию."""
        settings = MLSettings()
        
        assert settings.default_model_version == "v1.0"
        assert settings.min_confidence_threshold == 0.3

    def test_models_dir_relative_path(self):
        """Проверка разрешения относительных путей."""
        settings = MLSettings()
        assert settings.models_dir.is_absolute()

    def test_confidence_threshold_validation(self):
        """Проверка валидации порога уверенности."""
        with pytest.raises(ValueError):
            MLSettings(min_confidence_threshold=-0.1)
        
        with pytest.raises(ValueError):
            MLSettings(min_confidence_threshold=1.1)


class TestAppSettings:
    """Тесты общих настроек приложения."""

    def test_default_values(self):
        """Проверка значений по умолчанию."""
        settings = AppSettings()
        
        assert settings.batch_size == 1000
        assert settings.debug is False
        assert settings.log_level == "INFO"
        assert settings.ab_test_enabled is True
        assert settings.ab_test_split == 0.5

    def test_batch_size_validation(self):
        """Проверка валидации размера пакета."""
        with pytest.raises(ValueError):
            AppSettings(batch_size=50)
        
        with pytest.raises(ValueError):
            AppSettings(batch_size=20000)

    def test_ab_test_split_validation(self):
        """Проверка валидации доли A/B теста."""
        with pytest.raises(ValueError):
            AppSettings(ab_test_split=-0.1)
        
        with pytest.raises(ValueError):
            AppSettings(ab_test_split=1.1)


class TestSettings:
    """Тесты основных настроек приложения."""

    def test_composition(self):
        """Проверка композиции настроек."""
        settings = Settings()
        
        assert isinstance(settings.database, DatabaseSettings)
        assert isinstance(settings.ml, MLSettings)
        assert isinstance(settings.app, AppSettings)

    def test_nested_access(self):
        """Проверка доступа к вложенным настройкам."""
        settings = Settings()
        
        assert settings.database.host == "localhost"
        assert settings.ml.default_model_version == "v1.0"
        assert settings.app.batch_size == 1000


class TestContainer:
    """Тесты DI контейнера."""

    def test_container_provides_config(self):
        """Проверка предоставления конфигурации."""
        config = container.config()
        
        assert isinstance(config, Settings)
        assert isinstance(config.database, DatabaseSettings)

    def test_singleton_behavior(self):
        """Проверка поведения singleton."""
        config1 = container.config()
        config2 = container.config()
        
        # Singleton должен возвращать тот же экземпляр
        assert config1 is config2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
