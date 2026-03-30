# -*- coding: utf-8 -*-
"""
diagnose_database.py
Диагностика структуры и наполнения таблиц БД ProjectZZZ
Версия: 1.0
"""

import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
import yaml
import pandas as pd

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "diagnose_database.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# СПИСОК ОЖИДАЕМЫХ КОЛОНОК (согласно ReadMe и нашим требованиям)
# ==============================================================================
EXPECTED_COLUMNS = {
    'clients': {
        'required': ['client_id', 'client_name', 'manager_id'],
        'optional': ['sales_channel', 'map_oil_autochem', 'network_name', 
                     'branch', 'segment', 'metadata', 'source_file']
    },
    'skus': {
        'required': ['sku_id', 'sku_name', 'brand'],
        'optional': ['article', 'applicability', 'applicable_brands', 
                     'applicability_entry_count', 'brand_specialization',
                     'marketing_group1', 'marketing_group2', 
                     'product_group', 'financial_group',
                     'stock', 'margin', 'price', 'category', 
                     'group_id', 'is_new', 'source_file']
    },
    'minmax_norms': {
        'required': ['sku_id', 'warehouse_code'],
        'optional': ['paired_item', 'is_kit', 'replacement_set', 
                     'replacement_set_code', 'analog_set', 'analog_set_code',
                     'max_norm', 'brand', 'article', 'sku_name',
                     'marketing_group', 'deals_180d', 'sold_180d',
                     'source_file']
    },
    'purchases': {
        'required': ['client_id', 'sku_id', 'purchase_date', 'quantity'],
        'optional': ['amount', 'price', 'warehouse', 'quarter', 
                     'source_file', 'client_name', 'brand', 
                     'product_group', 'marketing_group']
    }
}


# ==============================================================================
# ФУНКЦИИ
# ==============================================================================
def load_config():
    """Загружаем конфиг БД"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_engine(config):
    """Подключение к PostgreSQL"""
    db = config['database']
    return create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )


def get_table_columns(engine, table_name):
    """Получает список колонок таблицы"""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """))
        return [dict(row._mapping) for row in result.fetchall()]


def get_table_count(engine, table_name):
    """Получает количество записей в таблице"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.fetchone()[0]
    except:
        return 0


def get_sample_data(engine, table_name, limit=3):
    """Получает пример данных из таблицы"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
            columns = result.keys()
            rows = result.fetchall()
            return columns, rows
    except Exception as e:
        return None, str(e)


def check_column_exists(columns, column_name):
    """Проверяет наличие колонки в списке"""
    return any(col['column_name'] == column_name for col in columns)


def diagnose_table(engine, table_name, expected):
    """Диагностика одной таблицы"""
    logger.info(f"\n{'='*70}")
    logger.info(f"ТАБЛИЦА: {table_name.upper()}")
    logger.info(f"{'='*70}")
    
    # Проверяем существование таблицы
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = '{table_name}'
            )
        """))
        exists = result.fetchone()[0]
    
    if not exists:
        logger.error(f"❌ Таблица {table_name} НЕ СУЩЕСТВУЕТ!")
        return {
            'exists': False,
            'columns': [],
            'count': 0,
            'missing_required': expected['required'],
            'missing_optional': expected['optional']
        }
    
    logger.info(f"✅ Таблица существует")
    
    # Получаем колонки
    columns = get_table_columns(engine, table_name)
    column_names = [col['column_name'] for col in columns]
    
    logger.info(f"\n📋 ВСЕ КОЛОНКИ ({len(columns)}):")
    for col in columns:
        nullable = "NULL" if col['is_nullable'] == "YES" else "NOT NULL"
        logger.info(f"   • {col['column_name']:<30} {col['data_type']:<15} {nullable}")
    
    # Проверяем required колонки
    missing_required = [c for c in expected['required'] if not check_column_exists(columns, c)]
    if missing_required:
        logger.error(f"\n❌ ОТСУТСТВУЮТ REQUIRED КОЛОНКИ: {missing_required}")
    else:
        logger.info(f"\n✅ ВСЕ REQUIRED КОЛОНКИ НА МЕСТЕ")
    
    # Проверяем optional колонки
    missing_optional = [c for c in expected['optional'] if not check_column_exists(columns, c)]
    if missing_optional:
        logger.warning(f"\n⚠️ ОТСУТСТВУЮТ OPTIONAL КОЛОНКИ ({len(missing_optional)}):")
        for col in missing_optional:
            logger.warning(f"      - {col}")
    else:
        logger.info(f"\n✅ ВСЕ OPTIONAL КОЛОНКИ НА МЕСТЕ")
    
    # Количество записей
    count = get_table_count(engine, table_name)
    logger.info(f"\n📊 КОЛИЧЕСТВО ЗАПИСЕЙ: {count:,}")
    
    # Пример данных
    logger.info(f"\n📄 ПРИМЕР ДАННЫХ (первые 3 строки):")
    cols, rows = get_sample_data(engine, table_name)
    if cols:
        # Заголовки
        header = " | ".join([str(c)[:20] for c in cols])
        logger.info(f"   {header}")
        logger.info(f"   {'-'*len(header)}")
        # Строки
        for row in rows:
            line = " | ".join([str(v)[:20] if v is not None else 'NULL' for v in row])
            logger.info(f"   {line}")
    else:
        logger.warning(f"   ⚠️ Не удалось получить данные: {rows}")
    
    return {
        'exists': True,
        'columns': column_names,
        'count': count,
        'missing_required': missing_required,
        'missing_optional': missing_optional
    }


def generate_migration_sql(table_name, missing_columns):
    """Генерирует SQL для добавления недостающих колонок"""
    if not missing_columns:
        return ""
    
    sql_lines = [f"\n-- ============================================================================",
                 f"-- ДОБАВЛЕНИЕ КОЛОНОК В {table_name.upper()}",
                 f"-- ============================================================================"]
    
    # Типы данных по умолчанию
    type_mapping = {
        'stock': 'INTEGER DEFAULT 0',
        'margin': 'DECIMAL(5,4)',
        'price': 'DECIMAL(12,2)',
        'sales_channel': 'TEXT',
        'map_oil_autochem': 'TEXT',
        'network_name': 'TEXT',
        'product_group': 'TEXT',
        'financial_group': 'TEXT',
        'applicable_brands': 'TEXT[]',
        'applicability_entry_count': 'INTEGER',
        'brand_specialization': 'TEXT',
        'paired_item': 'TEXT',
        'is_kit': 'TEXT',
        'replacement_set': 'TEXT',
        'replacement_set_code': 'TEXT',
        'analog_set': 'TEXT',
        'analog_set_code': 'TEXT',
    }
    
    for col in missing_columns:
        col_type = type_mapping.get(col, 'TEXT')
        sql_lines.append(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} {col_type};")
    
    return "\n".join(sql_lines)


def main():
    logger.info("="*70)
    logger.info("ProjectZZZ - Диагностика Базы Данных")
    logger.info(f"Дата: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    # Подключение к БД
    try:
        config = load_config()
        engine = get_engine(config)
        logger.info(f"✅ Подключение к БД: {config['database']['name']}")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        return 1
    
    # Диагностика всех таблиц
    results = {}
    all_missing = {'required': [], 'optional': []}
    
    for table_name, expected in EXPECTED_COLUMNS.items():
        result = diagnose_table(engine, table_name, expected)
        results[table_name] = result
        
        all_missing['required'].extend([(table_name, c) for c in result['missing_required']])
        all_missing['optional'].extend([(table_name, c) for c in result['missing_optional']])
    
    # Итоговый отчет
    logger.info("\n" + "="*70)
    logger.info("ИТОГОВЫЙ ОТЧЕТ")
    logger.info("="*70)
    
    logger.info("\n📊 СТАТУС ТАБЛИЦ:")
    for table_name, result in results.items():
        status = "✅" if result['exists'] else "❌"
        count_str = f"{result['count']:,}" if result['exists'] else "N/A"
        logger.info(f"   {status} {table_name:<20} {count_str:>12} записей")
    
    logger.info("\n❌ КРИТИЧЕСКИЕ ПРОБЛЕМЫ (missing REQUIRED):")
    if all_missing['required']:
        for table, col in all_missing['required']:
            logger.error(f"   • {table}.{col}")
    else:
        logger.info("   ✅ Нет критических проблем")
    
    logger.info("\n⚠️ РЕКОМЕНДУЕМЫЕ ДОБАВЛЕНИЯ (missing OPTIONAL):")
    if all_missing['optional']:
        for table, col in all_missing['optional']:
            logger.warning(f"   • {table}.{col}")
    else:
        logger.info("   ✅ Все optional колонки на месте")
    
    # Генерация SQL миграции
    logger.info("\n" + "="*70)
    logger.info("SQL СКРИПТ ДЛЯ МИГРАЦИИ")
    logger.info("="*70)
    logger.info("\n-- Скопируйте этот скрипт и выполните в pgAdmin:")
    logger.info("-- Файл: D:\\ProjectZZZ\\config\\sql\\add_missing_columns.sql\n")
    
    migration_sql = []
    for table_name, result in results.items():
        if result['exists'] and result['missing_optional']:
            sql = generate_migration_sql(table_name, result['missing_optional'])
            migration_sql.append(sql)
    
    if migration_sql:
        for sql in migration_sql:
            logger.info(sql)
    else:
        logger.info("-- Все колонки на месте, миграция не требуется")
    
    # Сохранение SQL в файл
    sql_file = LOG_DIR / "add_missing_columns.sql"
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write("-- ============================================================================\n")
        f.write("-- ProjectZZZ: Добавление недостающих колонок\n")
        f.write(f"-- Дата генерации: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("-- ============================================================================\n\n")
        for sql in migration_sql:
            f.write(sql + "\n\n")
    
    logger.info(f"\n💾 SQL скрипт сохранён: {sql_file}")
    
    engine.dispose()
    
    logger.info("\n" + "="*70)
    logger.info("ДИАГНОСТИКА ЗАВЕРШЕНА")
    logger.info("="*70)
    
    return 0 if not all_missing['required'] else 1


if __name__ == "__main__":
    sys.exit(main())