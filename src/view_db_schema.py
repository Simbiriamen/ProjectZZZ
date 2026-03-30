# -*- coding: utf-8 -*-
"""
view_db_schema.py
Инструмент просмотра структуры таблиц PostgreSQL (ProjectZZZ)
"""

import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text, inspect
import yaml

# ==============================================================================
# НАСТРОЙКИ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_engine(config):
    db = config['database']
    return create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )


def view_all_tables(engine):
    """Показать все таблицы"""
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    logger.info("\n" + "=" * 70)
    logger.info("📁 ТАБЛИЦЫ БАЗЫ ДАННЫХ")
    logger.info("=" * 70)
    logger.info(f"База данных: {engine.url.database}")
    logger.info(f"Всего таблиц: {len(tables)}")
    logger.info("=" * 70)
    
    for table in sorted(tables):
        logger.info(f"\n📊 {table}")
        logger.info("-" * 50)
        view_table_structure(engine, table, inspector)


def view_table_structure(engine, table_name, inspector=None):
    """Показать структуру одной таблицы"""
    if inspector is None:
        inspector = inspect(engine)
    
    # Колонки
    columns = inspector.get_columns(table_name)
    
    logger.info(f"  {'Колонка':<30} {'Тип':<20} {'Null':<6} {'Default'}")
    logger.info(f"  {'-'*30} {'-'*20} {'-'*6} {'-'*20}")
    
    for col in columns:
        name = col['name'][:28]
        dtype = str(col['type'])[:18]
        nullable = "Да" if col['nullable'] else "Нет"
        default = str(col['default'])[:18] if col['default'] else "-"
        logger.info(f"  {name:<30} {dtype:<20} {nullable:<6} {default}")
    
    # Первичные ключи
    pk = inspector.get_pk_constraint(table_name)
    if pk and pk.get('constrained_columns'):
        logger.info(f"\n  🔑 Первичный ключ: {', '.join(pk['constrained_columns'])}")
    
    # Индексы
    indexes = inspector.get_indexes(table_name)
    if indexes:
        logger.info(f"\n  📑 Индексы:")
        for idx in indexes:
            logger.info(f"     - {idx['name']}: {', '.join(idx['column_names'])}")
    
    # Внешние ключи
    fk = inspector.get_foreign_keys(table_name)
    if fk:
        logger.info(f"\n  🔗 Внешние ключи:")
        for f in fk:
            logger.info(f"     - {f['constrained_columns']} → {f['referred_table']}.{f['referred_columns']}")
    
    # Количество строк
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.fetchone()[0]
            logger.info(f"\n  📊 Записей: {count:,}")
    except:
        pass


def view_table(table_name, engine):
    """Просмотр конкретной таблицы"""
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    if table_name not in tables:
        logger.info(f"❌ Таблица '{table_name}' не найдена")
        logger.info(f"Доступные таблицы: {', '.join(tables)}")
        return
    
    logger.info("\n" + "=" * 70)
    logger.info(f"📊 СТРУКТУРА ТАБЛИЦЫ: {table_name}")
    logger.info("=" * 70)
    view_table_structure(engine, table_name, inspector)


def view_sample_data(table_name, engine, limit=5):
    """Показать пример данных из таблицы"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
            rows = result.fetchall()
            columns = result.keys()
            
            logger.info(f"\n📄 ПРИМЕР ДАННЫХ ({limit} строк):")
            logger.info("-" * 70)
            
            # Заголовки
            header = " | ".join([str(c)[:15] for c in columns])
            logger.info(header)
            logger.info("-" * 70)
            
            # Строки
            for row in rows:
                line = " | ".join([str(v)[:15] if v is not None else "NULL" for v in row])
                logger.info(line)
            
            logger.info("-" * 70)
            
    except Exception as e:
        logger.info(f"❌ Ошибка: {e}")


def main():
    logger.info("\n" + "=" * 70)
    logger.info("🔍 ПРОСМОТР СТРУКТУРЫ БД ProjectZZZ")
    logger.info("=" * 70)
    
    config = load_config()
    engine = get_engine(config)
    
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    if not tables:
        logger.info("❌ Таблицы не найдены в базе данных")
        engine.dispose()
        return
    
    logger.info(f"\n✅ Найдено таблиц: {len(tables)}")
    logger.info("\nСписок таблиц:")
    for i, table in enumerate(sorted(tables), 1):
        logger.info(f"  {i}. {table}")
    
    # Меню
    logger.info("\n" + "=" * 70)
    logger.info("МЕНЮ:")
    logger.info("  1 - Показать структуру всех таблиц")
    logger.info("  2 - Показать структуру конкретной таблицы")
    logger.info("  3 - Показать пример данных из таблицы")
    logger.info("  0 - Выход")
    logger.info("=" * 70)
    
    while True:
        try:
            choice = input("\nВаш выбор: ").strip()
            
            if choice == "0":
                logger.info("👋 Выход")
                break
            
            elif choice == "1":
                view_all_tables(engine)
            
            elif choice == "2":
                table_name = input("Введите имя таблицы: ").strip()
                view_table(table_name, engine)
            
            elif choice == "3":
                table_name = input("Введите имя таблицы: ").strip()
                limit = input("Количество строк (по умолчанию 5): ").strip()
                limit = int(limit) if limit.isdigit() else 5
                view_sample_data(table_name, engine, limit)
            
            else:
                logger.info("❌ Неверный выбор")
        
        except KeyboardInterrupt:
            logger.info("\n👋 Прервано")
            break
        except Exception as e:
            logger.info(f"❌ Ошибка: {e}")
    
    engine.dispose()


if __name__ == "__main__":
    main()