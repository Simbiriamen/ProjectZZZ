# -*- coding: utf-8 -*-
"""
config_loader.py v1.0
🔧 БЕЗОПАСНАЯ ЗАГРУЗКА КОНФИГУРАЦИИ
Назначение:
  - Загрузка переменных окружения из .env
  - Подстановка env variables в config.yaml
  - Валидация обязательных параметров

Использование:
    from src.config_loader import load_config, load_config_with_env
    
    config = load_config()  # Загружает config.yaml с подстановкой env
"""

import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from dotenv import load_dotenv

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
ENV_FILE = PROJECT_ROOT / ".env"


# ==============================================================================
# ФУНКЦИИ
# ==============================================================================
def load_env_file(env_file: Path = ENV_FILE) -> bool:
    """
    Загружает переменные окружения из .env файла.
    
    Args:
        env_file: Путь к .env файлу
    
    Returns:
        True если файл загружен, False если не найден
    """
    if env_file.exists():
        load_dotenv(dotenv_path=env_file)
        return True
    else:
        # Пытаемся загрузить из текущей директории
        load_dotenv()
        return False


def substitute_env_variables(value: Any, env_prefix: str = '') -> Any:
    """
    Рекурсивно заменяет ${VAR_NAME:default} на значения из env.
    
    Args:
        value: Значение для обработки (может быть dict, list, str)
        env_prefix: Префикс для вложенных ключей
    
    Returns:
        Обработанное значение с подставленными переменными
    """
    if isinstance(value, dict):
        return {
            k: substitute_env_variables(v, f"{env_prefix}{k}_")
            for k, v in value.items()
        }
    elif isinstance(value, list):
        return [
            substitute_env_variables(item, env_prefix)
            for item in value
        ]
    elif isinstance(value, str):
        # Паттерн для ${VAR_NAME:default_value}
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
        
        def replace_var(match):
            var_name = match.group(1)
            default_value = match.group(2)
            
            env_value = os.getenv(var_name)
            
            if env_value is not None:
                return env_value
            elif default_value is not None:
                return default_value
            else:
                raise ValueError(
                    f"Переменная окружения '{var_name}' не установлена "
                    f"и не имеет значения по умолчанию"
                )
        
        return re.sub(pattern, replace_var, value)
    else:
        return value


def validate_config(config: Dict) -> list:
    """
    Валидирует конфигурацию на наличие обязательных полей.
    
    Args:
        config: Загруженная конфигурация
    
    Returns:
        Список ошибок валидации
    """
    errors = []
    
    # Проверка базы данных
    db = config.get('database', {})
    required_db_fields = ['host', 'port', 'name', 'user', 'password']
    
    for field in required_db_fields:
        if field not in db:
            errors.append(f"Отсутствует обязательное поле: database.{field}")
        elif not db[field]:
            errors.append(f"Поле database.{field} не должно быть пустым")
    
    # Проверка пароля (не должен быть дефолтным)
    if db.get('password') in ['postgres', 'password', '123456']:
        errors.append(
            f"Пароль базы данных слишком простой! "
            f"Используйте сложную комбинацию."
        )
    
    # Проверка путей
    paths = config.get('paths', {})
    for path_name, path_value in paths.items():
        if not Path(path_value).parent.exists():
            errors.append(f"Путь не существует: {path_name} = {path_value}")
    
    return errors


def load_config(
    config_path: Path = CONFIG_PATH,
    env_file: Path = ENV_FILE,
    validate: bool = True
) -> Dict:
    """
    Загружает конфигурацию из YAML с подстановкой переменных окружения.
    
    Args:
        config_path: Путь к config.yaml
        env_file: Путь к .env файлу
        validate: Валидировать конфигурацию
    
    Returns:
        Словарь конфигурации
    
    Raises:
        FileNotFoundError: Если config.yaml не найден
        ValueError: Если валидация не пройдена
    """
    # 1. Загружаем .env
    load_env_file(env_file)
    
    # 2. Загружаем YAML
    if not config_path.exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 3. Подставляем переменные окружения
    config = substitute_env_variables(config)
    
    # 4. Валидация
    if validate:
        errors = validate_config(config)
        if errors:
            error_msg = "Ошибки валидации конфигурации:\n" + "\n".join(
                f"  • {e}" for e in errors
            )
            raise ValueError(error_msg)
    
    return config


def get_database_url(config: Optional[Dict] = None) -> str:
    """
    Возвращает URL базы данных для подключения.
    
    Args:
        config: Конфигурация (если None, загружается автоматически)
    
    Returns:
        Database URL в формате postgresql://user:pass@host:port/dbname
    """
    if config is None:
        config = load_config()
    
    db = config['database']
    return (
        f"postgresql://{db['user']}:{db['password']}@"
        f"{db['host']}:{db['port']}/{db['name']}"
    )


# ==============================================================================
# CLI INTERFACE
# ==============================================================================
if __name__ == "__main__":
    import sys
    
    print("="*70)
    print("🔧 ProjectZZZ - Config Loader")
    print("="*70)
    
    # Загружаем конфигурацию
    try:
        config = load_config()
        print("\n✅ Конфигурация загружена успешно!")
        
        # Выводим информацию
        db = config['database']
        print(f"\n📊 База данных:")
        print(f"   Host: {db['host']}")
        print(f"   Port: {db['port']}")
        print(f"   Name: {db['name']}")
        print(f"   User: {db['user']}")
        print(f"   Password: {'*' * len(db['password'])}")
        
        # Проверяем переменные окружения
        print(f"\n🌍 Переменные окружения:")
        env_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        for var in env_vars:
            value = os.getenv(var, '❌ Не установлено')
            if var == 'DB_PASSWORD' and value != '❌ Не установлено':
                value = '****'
            print(f"   {var}: {value}")
        
        print("\n" + "="*70)
        print("✅ Все проверки пройдены")
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        sys.exit(1)
