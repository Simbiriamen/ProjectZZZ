# -*- coding: utf-8 -*-
"""
update_marketing_hierarchy.py
Обновление иерархии маркетинговых групп в таблице skus
Родитель → marketing_group (уровень 1)
Наименование → marketing_group_2 (уровень 2)
ProjectZZZ v3.1
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
        logging.FileHandler(LOG_DIR / "update_marketing_hierarchy.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# ФУНКЦИИ
# ==============================================================================
def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_engine(config):
    db = config['database']
    return create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )


def update_marketing_hierarchy(engine):
    """Обновляет иерархию маркетинговых групп"""
    logger.info("\n" + "="*70)
    logger.info("ОБНОВЛЕНИЕ ИЕРАРХИИ МАРКЕТИНГОВЫХ ГРУПП")
    logger.info("="*70)
    
    with engine.connect() as conn:
        # Проверяем какие колонки есть в таблице skus
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'skus'
            ORDER BY ordinal_position
        """))
        
        existing_columns = [row[0] for row in result.fetchall()]
        logger.info(f"\n📋 Существующие колонки в skus: {len(existing_columns)}")
        
        # Ищем колонки с маркетинговыми группами
        marketing_cols = [c for c in existing_columns if 'marketing' in c.lower() or 'родитель' in c.lower() or 'наименование' in c.lower()]
        logger.info(f"📋 Колонки связанные с маркетингом: {marketing_cols}")
        
        # Определяем какие колонки использовать
        parent_col = None
        level2_col = None
        
        for col in existing_columns:
            col_lower = col.lower()
            if 'родитель' in col_lower and parent_col is None:
                parent_col = col
            elif 'наименование' in col_lower and 'маркетинг' in col_lower:
                level2_col = col
        
        # Если не нашли точные названия, пробуем альтернативы
        if not parent_col:
            for col in ['marketing_group1', 'marketing_group', 'parent_group']:
                if col in existing_columns:
                    parent_col = col
                    break
        
        if not level2_col:
            for col in ['marketing_group2', 'marketing_group_level2']:
                if col in existing_columns:
                    level2_col = col
                    break
        
        logger.info(f"\n📌 Колонка уровня 1 (Родитель): {parent_col}")
        logger.info(f"📌 Колонка уровня 2 (Наименование): {level2_col}")
        
        # Добавляем колонки если их нет
        logger.info("\n💾 Добавление колонок marketing_group1 и marketing_group2...")
        
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'skus' AND column_name = 'marketing_group1'
                ) THEN
                    ALTER TABLE skus ADD COLUMN marketing_group1 TEXT;
                    RAISE NOTICE 'Колонка marketing_group1 добавлена';
                END IF;
            END $$;
        """))
        conn.commit()
        
        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'skus' AND column_name = 'marketing_group2'
                ) THEN
                    ALTER TABLE skus ADD COLUMN marketing_group2 TEXT;
                    RAISE NOTICE 'Колонка marketing_group2 добавлена';
                END IF;
            END $$;
        """))
        conn.commit()
        
        logger.info("✅ Колонки добавлены")
        
        # Обновляем данные из существующих колонок
        if parent_col:
            logger.info(f"\n🔄 Копирование данных из {parent_col} → marketing_group1...")
            conn.execute(text(f"""
                UPDATE skus 
                SET marketing_group1 = "{parent_col}"
                WHERE "{parent_col}" IS NOT NULL
            """))
            conn.commit()
            
            # Считаем сколько обновили
            result = conn.execute(text("""
                SELECT COUNT(*) FROM skus WHERE marketing_group1 IS NOT NULL
            """))
            count = result.fetchone()[0]
            logger.info(f"   ✅ Обновлено: {count:,} записей")
        
        if level2_col:
            logger.info(f"\n🔄 Копирование данных из {level2_col} → marketing_group2...")
            conn.execute(text(f"""
                UPDATE skus 
                SET marketing_group2 = "{level2_col}"
                WHERE "{level2_col}" IS NOT NULL
            """))
            conn.commit()
            
            result = conn.execute(text("""
                SELECT COUNT(*) FROM skus WHERE marketing_group2 IS NOT NULL
            """))
            count = result.fetchone()[0]
            logger.info(f"   ✅ Обновлено: {count:,} записей")
        
        # Создаём индексы
        logger.info("\n📑 Создание индексов...")
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_skus_marketing1 ON skus(marketing_group1)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_skus_marketing2 ON skus(marketing_group2)"))
        conn.commit()
        logger.info("✅ Индексы созданы")
        
        # Статистика по группам
        logger.info("\n📊 СТАТИСТИКА ПО МАРКЕТИНГОВЫМ ГРУППАМ:")
        
        result = conn.execute(text("""
            SELECT 
                marketing_group1,
                COUNT(*) as sku_count,
                COUNT(DISTINCT marketing_group2) as subgroups
            FROM skus
            WHERE marketing_group1 IS NOT NULL
            GROUP BY marketing_group1
            ORDER BY sku_count DESC
            LIMIT 15
        """))
        
        logger.info("\n📋 Топ групп уровня 1:")
        for row in result.fetchall():
            logger.info(f"   {row[0] or 'NULL'}: {row[1]:,} SKU, {row[2]} подгрупп")
    
    return True


def main():
    logger.info("="*70)
    logger.info("ProjectZZZ - Обновление иерархии маркетинговых групп")
    logger.info("="*70)
    
    config = load_config()
    engine = get_engine(config)
    
    success = update_marketing_hierarchy(engine)
    
    engine.dispose()
    
    if success:
        logger.info("\n✅ Обновление завершено успешно!")
    else:
        logger.info("\n⚠️ Обновление завершено с предупреждениями")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())