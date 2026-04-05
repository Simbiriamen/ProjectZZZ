# -*- coding: utf-8 -*-
"""
Unit тесты для конфигурации.

Тестируют pydantic settings и валидацию.
"""

import pytest
from pathlib import Path
from src.config.settings import Settings, DatabaseSettings, MLSettings, AppSettings


class TestDatabaseSettings:
    """Тесты настроек БД."""
    
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
        """Проверка генерации URL."""
        settings = DatabaseSettings(
            host="db.example.com",
            port=5433,
            database="test_db",
            user="test_user",
            password="secret"
        )
        
        expected = "postgresql://test_user:secret@db.example.com:5433/test_db"
        assert settings.url == expected
    
    def test_pool_size_validation(self):
        """Проверка валидации размера пула."""
        with pytest.raises(Exception):
            DatabaseSettings(pool_size=0)
        
        with pytest.raises(Exception):
            DatabaseSettings(pool_size=101)


class TestMLSettings:
    """Тесты ML настроек."""
    
    def test_default_values(self):
        """Проверка значений по умолчанию."""
        settings = MLSettings()
        
        assert settings.default_model == "lightgbm_v1"
        assert settings.cache_predictions is True
        assert settings.prediction_cache_ttl == 3600
        assert settings.batch_size == 200
        assert settings.n_jobs == -1
    
    def test_models_dir_creation(self):
        """Проверка создания директории моделей."""
        settings = MLSettings()
        
        assert settings.models_dir.exists()
        assert settings.models_dir.is_dir()


class TestAppSettings:
    """Тесты общих настроек приложения."""
    
    def test_default_values(self):
        """Проверка значений по умолчанию."""
        settings = AppSettings()
        
        assert settings.debug is False
        assert settings.log_level == "INFO"
        assert settings.batch_size == 200


class TestSettings:
    """Тесты корневых настроек."""
    
    def test_composition(self):
        """Проверка композиции настроек."""
        settings = Settings()
        
        assert isinstance(settings.database, DatabaseSettings)
        assert isinstance(settings.ml, MLSettings)
        assert isinstance(settings.app, AppSettings)
    
    def test_load_from_env(self):
        """Проверка загрузки из environment."""
        settings = Settings.load()
        
        assert settings is not None
        assert hasattr(settings, 'database')
        assert hasattr(settings, 'ml')
        assert hasattr(settings, 'app')
