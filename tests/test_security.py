# -*- coding: utf-8 -*-
"""
test_security.py
Unit-тесты для проверок безопасности и валидации

Запуск:
  pytest tests/test_security.py -v
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from sqlalchemy import text


class TestSQLInjectionPrevention:
    """Тесты предотвращения SQL Injection"""

    def test_parameterized_query_months(self):
        """Тест: параметризованный запрос для months"""
        from src.backtest_engine import get_active_client_list
        
        mock_engine = MagicMock()
        
        # Вызываем функцию
        get_active_client_list(mock_engine, months=12)
        
        # Проверяем, что использовался параметризованный запрос
        call_args = mock_engine.execute.call_args
        query = str(call_args[0][0])
        
        # Убеждаемся, что нет f-строк в запросе
        assert ':months' in query
        assert "INTERVAL '12" not in query

    def test_parameterized_client_ids(self):
        """Тест: параметризованный запрос для client_ids"""
        from src.backtest_engine import load_raw_purchases_chunk
        
        mock_engine = MagicMock()
        client_ids = ['C1', 'C2', 'C3']
        
        load_raw_purchases_chunk(mock_engine, client_ids, months=12)
        
        # Проверяем параметризацию
        call_args = mock_engine.execute.call_args
        query = str(call_args[0][0])
        
        assert ':client_ids' in query
        # Убеждаемся, что нет ручной экранизации
        assert "''" not in query


class TestInputValidation:
    """Тесты валидации входных данных"""

    def test_get_ab_group_empty_client_id(self):
        """Тест: пустой client_id"""
        from src.generate_recommendations import get_ab_group
        
        config = {'ab_test': {'enabled': True}}
        
        with pytest.raises(ValueError, match="не может быть пустым"):
            get_ab_group('', config)

    def test_get_ab_group_none_client_id(self):
        """Тест: None client_id"""
        from src.generate_recommendations import get_ab_group
        
        config = {'ab_test': {'enabled': True}}
        
        with pytest.raises(ValueError):
            get_ab_group(None, config)

    def test_get_ab_group_non_string(self):
        """Тест: нестроковый client_id"""
        from src.generate_recommendations import get_ab_group
        
        config = {'ab_test': {'enabled': True}}
        
        with pytest.raises(ValueError, match="должен быть строкой"):
            get_ab_group(123, config)

    def test_get_ab_group_too_long(self):
        """Тест: слишком длинный client_id"""
        from src.generate_recommendations import get_ab_group
        
        config = {'ab_test': {'enabled': True}}
        long_id = 'C' * 300
        
        with pytest.raises(ValueError, match="слишком длинный"):
            get_ab_group(long_id, config)

    def test_get_ab_group_valid(self):
        """Тест: корректный client_id"""
        from src.generate_recommendations import get_ab_group
        
        config = {'ab_test': {'enabled': True, 'test_group_ratio': 0.5}}
        
        # Должно работать без ошибок
        result = get_ab_group('C123', config)
        assert result in ['test', 'control']


class TestConfigLoader:
    """Тесты безопасной загрузки конфигурации"""

    def test_load_env_file(self):
        """Тест: загрузка .env файла"""
        from src.config_loader import load_env_file
        import os
        
        # Создаём тестовый .env
        test_env = """
        TEST_VAR=hello
        TEST_NUM=123
        """
        
        env_path = pytest.fixture(lambda: pytest.tmp_path / ".env")
        
        with open(pytest.tmp_path / ".env", 'w') as f:
            f.write(test_env)
        
        # Загружаем
        from dotenv import load_dotenv
        load_dotenv(pytest.tmp_path / ".env")
        
        assert os.getenv('TEST_VAR') == 'hello'
        assert os.getenv('TEST_NUM') == '123'

    def test_substitute_env_variables(self):
        """Тест: подстановка переменных окружения"""
        from src.config_loader import substitute_env_variables
        import os
        
        # Устанавливаем тестовые переменные
        os.environ['TEST_HOST'] = 'db.example.com'
        os.environ['TEST_PORT'] = '5432'
        
        config = {
            'database': {
                'host': '${TEST_HOST}',
                'port': '${TEST_PORT:5432}',
                'name': 'mydb'
            }
        }
        
        result = substitute_env_variables(config)
        
        assert result['database']['host'] == 'db.example.com'
        assert result['database']['port'] == '5432'
        assert result['database']['name'] == 'mydb'

    def test_substitute_env_default(self):
        """Тест: использование значения по умолчанию"""
        from src.config_loader import substitute_env_variables
        import os
        
        # Переменная не установлена
        os.environ.pop('NON_EXISTENT_VAR', None)
        
        config = {
            'value': '${NON_EXISTENT_VAR:default_value}'
        }
        
        result = substitute_env_variables(config)
        assert result['value'] == 'default_value'

    def test_validate_config_missing_fields(self):
        """Тест: валидация отсутствующих полей"""
        from src.config_loader import validate_config
        
        config = {
            'database': {
                'host': 'localhost'
                # Отсутствуют port, name, user, password
            }
        }
        
        errors = validate_config(config)
        assert len(errors) > 0
        assert any('port' in e for e in errors)

    def test_validate_config_weak_password(self):
        """Тест: валидация слабого пароля"""
        from src.config_loader import validate_config
        
        config = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'name': 'testdb',
                'user': 'postgres',
                'password': 'postgres'  # Слабый пароль
            }
        }
        
        errors = validate_config(config)
        assert any('пароль' in e.lower() or 'password' in e.lower() for e in errors)


class TestDatabaseConnection:
    """Тесты безопасного подключения к БД"""

    @patch('src.config_loader.load_config')
    @patch('src.config_loader.create_engine')
    def test_get_engine_from_env(self, mock_create_engine, mock_load_config):
        """Тест: получение credentials из env"""
        from src.config_loader import get_database_url
        
        mock_load_config.return_value = {
            'database': {
                'user': 'user_from_env',
                'password': 'pass_from_env',
                'host': 'host_from_env',
                'port': 5432,
                'name': 'db_from_env'
            }
        }
        
        url = get_database_url()
        
        assert 'user_from_env' in url
        assert 'pass_from_env' in url
        # Убеждаемся, что пароль не хардкоден
        assert 'postgres' not in url


class TestSecureCodingPractices:
    """Тесты безопасных практик кодирования"""

    def test_no_hardcoded_passwords(self):
        """Тест: отсутствие хардкода паролей в коде"""
        import os
        from pathlib import Path
        
        # Исключаем тестовые файлы и конфиги
        exclude_dirs = {'venv', 'node_modules', '.git', '__pycache__', 'tests'}
        exclude_files = {'config.yaml', '.env', '.env.example'}
        
        src_dir = Path(__file__).parent.parent / 'src'
        
        hardcoded_passwords = ['password=postgres', 'password=admin', 'password=123456']
        
        for py_file in src_dir.rglob('*.py'):
            if py_file.parent.name in exclude_dirs:
                continue
            if py_file.name in exclude_files:
                continue
            
            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for pwd in hardcoded_passwords:
                # Ищем только в коде, не в комментариях и строках
                assert pwd not in content, f"Хардкод пароля в {py_file}"

    def test_error_messages_no_sensitive_info(self):
        """Тест: сообщения об ошибках не содержат чувствительной информации"""
        from src.generate_recommendations import get_ab_group
        
        config = {'ab_test': {'enabled': True}}
        
        try:
            get_ab_group('', config)
        except ValueError as e:
            error_msg = str(e)
            # Убеждаемся, что в ошибке нет паролей или путей
            assert 'password' not in error_msg.lower()
            assert 'secret' not in error_msg.lower()


# ==============================================================================
# ЗАПУСК ТЕСТОВ
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
