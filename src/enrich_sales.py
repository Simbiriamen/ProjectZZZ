# -*- coding: utf-8 -*-
"""
enrich_sales.py
Обогащение продаж данными из справочников для ProjectZZZ
Версия: 1.5 - ИСПРАВЛЕНИЕ: корректный расчёт дней в PostgreSQL (DATE - DATE = INTEGER)
"""

import sys
import logging
import time
from pathlib import Path
from sqlalchemy import create_engine, text
import yaml
import pandas as pd
from datetime import datetime, timedelta

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
        logging.FileHandler(LOG_DIR / "enrich_sales.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# ФУНКЦИИ ВСПОМОГАТЕЛЬНЫЕ
# ==============================================================================
def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_engine(config):
    db = config['database']
    return create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )


def get_skus_column_mapping(engine):
    """
    Получает маппинг колонок таблицы skus.
    Возвращает dict: {expected_name: actual_db_column_name}
    """
    mapping = {}
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = 'skus'
            ORDER BY ordinal_position
        """))
        
        columns = [row[0] for row in result.fetchall()]
        
        logger.info(f"📋 Все колонки в skus: {columns}")
        
        expected_cols = [
            'sku_id', 'brand', 'sku_name', 'article', 'applicability',
            'product_group', 'financial_group', 'marketing_group1', 'marketing_group2',
            'category', 'margin', 'stock', 'is_new', 'applicable_brands', 'brand_specialization'
        ]
        
        for expected in expected_cols:
            if expected in columns:
                mapping[expected] = expected
            elif expected + ' ' in columns:
                mapping[expected] = expected + ' '
            elif expected == 'article' and 'Артикул' in [c.strip() for c in columns]:
                for col in columns:
                    if col.strip() == 'Артикул':
                        mapping['article'] = col
                        break
            elif expected == 'product_group' and 'Группы товаров' in [c.strip() for c in columns]:
                for col in columns:
                    if col.strip() == 'Группы товаров':
                        mapping['product_group'] = col
                        break
            elif expected == 'financial_group' and 'Финансовая группа' in [c.strip() for c in columns]:
                for col in columns:
                    if col.strip() == 'Финансовая группа':
                        mapping['financial_group'] = col
                        break
        
        logger.info(f"✅ Маппинг колонок skus: {mapping}")
        return mapping


def create_sales_enriched_table(engine):
    logger.info("\n" + "="*70)
    logger.info("СОЗДАНИЕ ТАБЛИЦЫ sales_enriched")
    logger.info("="*70)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'sales_enriched'
            )
        """))
        exists = result.fetchone()[0]
        
        if exists:
            logger.info("✅ Таблица sales_enriched уже существует")
            result = conn.execute(text("SELECT COUNT(*) FROM sales_enriched"))
            count = result.fetchone()[0]
            logger.info(f"   📊 Записей в таблице: {count:,}")
        else:
            logger.info("📁 Создаём таблицу sales_enriched...")
            
            create_sql = """
            CREATE TABLE sales_enriched (
                id SERIAL PRIMARY KEY,
                purchase_id INTEGER,
                client_id TEXT NOT NULL,
                sku_id TEXT NOT NULL,
                purchase_date DATE NOT NULL,
                warehouse TEXT,
                quantity INTEGER DEFAULT 1,
                amount NUMERIC(12,2),
                price NUMERIC(12,2),
                brand TEXT,
                product_group TEXT,
                marketing_group TEXT,
                marketing_group2 TEXT,
                category TEXT,
                margin NUMERIC(5,4),
                stock INTEGER DEFAULT 0,
                is_new TEXT,
                article TEXT,
                applicability TEXT,
                applicable_brands TEXT[],
                brand_specialization TEXT,
                client_name TEXT,
                client_segment TEXT,
                sales_channel TEXT,
                network_name TEXT,
                manager_id TEXT,
                warehouse_code TEXT,
                max_norm INTEGER,
                paired_item TEXT,
                is_kit TEXT,
                replacement_set TEXT,
                analog_set TEXT,
                days_since_last_purchase INTEGER,
                frequency_30d INTEGER,
                frequency_90d INTEGER,
                rolling_sales_2w NUMERIC,
                rolling_sales_4w NUMERIC,
                rolling_sales_8w NUMERIC,
                group_trend_6m NUMERIC(5,4),
                group_share_in_portfolio NUMERIC,
                days_since_last_purchase_group INTEGER,
                quarter VARCHAR(10),
                source_file TEXT,
                enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                model_ready BOOLEAN DEFAULT TRUE
            )
            """
            
            conn.execute(text(create_sql))
            conn.commit()
            logger.info("✅ Таблица sales_enriched создана")
        
        # Индексы
        logger.info("\n📑 Создание индексов...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_enriched_client ON sales_enriched(client_id)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_sku ON sales_enriched(sku_id)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_date ON sales_enriched(purchase_date)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_brand ON sales_enriched(brand)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_product_group ON sales_enriched(product_group)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_marketing_group ON sales_enriched(marketing_group)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_quarter ON sales_enriched(quarter)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_client_sku ON sales_enriched(client_id, sku_id)",
            "CREATE INDEX IF NOT EXISTS idx_enriched_client_date ON sales_enriched(client_id, purchase_date)",
        ]
        
        for idx_sql in indexes:
            conn.execute(text(idx_sql))
        
        conn.commit()
        logger.info("✅ Индексы созданы")
        
        return True


def _quote_identifier(name):
    """Корректно экранирует имя колонки для PostgreSQL"""
    if not name or name == 'NULL':
        return 'NULL'
    if any(c in name for c in ' \t\n\r') or not all(c.isascii() or c.isdigit() or c == '_' for c in name):
        escaped = name.replace('"', '""')
        return f'"{escaped}"'
    return name


def enrich_from_purchases(engine):
    """Обогащение данных из purchases + JOIN с справочниками"""
    logger.info("\n" + "="*70)
    logger.info("ОБОГАЩЕНИЕ ДАННЫХ ИЗ PURCHASES")
    logger.info("="*70)
    
    col_map = get_skus_column_mapping(engine)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM purchases"))
        purchases_count = result.fetchone()[0]
        logger.info(f"📊 Записей в purchases: {purchases_count:,}")
        
        if purchases_count == 0:
            logger.warning("⚠️ Таблица purchases пуста — обогащение невозможно!")
            return 0
        
        result = conn.execute(text("SELECT COUNT(*) FROM sales_enriched"))
        enriched_count = result.fetchone()[0]
        logger.info(f"📊 Записей в sales_enriched: {enriched_count:,}")
        
        if enriched_count >= purchases_count:
            logger.info("✅ Все записи уже обогащены")
            return 0
        else:
            records_to_enrich = purchases_count - enriched_count
            logger.info(f"🔄 Нужно обогатить: {records_to_enrich:,} записей")
        
        logger.info("\n💾 Запуск обогащения (JOIN с справочниками)...")
        start_time = time.time()
        
        def safe_col(col_name, default_expr='NULL'):
            actual = col_map.get(col_name)
            if actual:
                return f's.{_quote_identifier(actual)}'
            return default_expr
        
        sku_select_parts = [
            safe_col('brand'),
            f"{safe_col('product_group')} AS product_group",
            "s.marketing_group1 AS marketing_group",
            "s.marketing_group2 AS marketing_group2",
            safe_col('category'),
            safe_col('margin'),
            safe_col('stock'),
            safe_col('is_new'),
            f"{safe_col('article')} AS article",
            safe_col('applicability'),
            safe_col('applicable_brands'),
            safe_col('brand_specialization'),
        ]
        
        enrich_sql = f"""
        INSERT INTO sales_enriched (
            purchase_id, client_id, sku_id, purchase_date, warehouse,
            quantity, amount, price,
            brand, product_group, marketing_group, marketing_group2,
            category, margin, stock, is_new, article, applicability,
            applicable_brands, brand_specialization,
            client_name, client_segment, sales_channel, network_name, manager_id,
            warehouse_code, max_norm, paired_item, is_kit, replacement_set, analog_set,
            quarter, source_file, enriched_at, model_ready
        )
        SELECT 
            p.id AS purchase_id,
            p.client_id,
            p.sku_id,
            p.purchase_date,
            p.warehouse,
            p.quantity,
            p.amount,
            p.price,
            {', '.join(sku_select_parts)},
            c.client_name,
            c.segment AS client_segment,
            c.sales_channel,
            c.network_name,
            c.manager_id,
            m.warehouse_code,
            m.max_norm,
            m.paired_item,
            m.is_kit,
            m.replacement_set,
            m.analog_set,
            p.quarter,
            p.source_file,
            CURRENT_TIMESTAMP AS enriched_at,
            TRUE AS model_ready
        FROM purchases p
        LEFT JOIN skus s ON p.sku_id = s.sku_id
        LEFT JOIN clients c ON p.client_id = c.client_id
        LEFT JOIN minmax_norms m ON p.sku_id = m.sku_id AND p.warehouse = m.warehouse_code
        WHERE p.id NOT IN (SELECT purchase_id FROM sales_enriched WHERE purchase_id IS NOT NULL)
        ORDER BY p.purchase_date
        LIMIT 50000
        """
        
        logger.info(f"🔍 SQL-запрос (первые 500 символов):\n{enrich_sql[:500]}...")
        
        result = conn.execute(text(enrich_sql))
        conn.commit()
        
        enriched_this_run = result.rowcount
        elapsed = time.time() - start_time
        
        logger.info(f"✅ Обогащено записей: {enriched_this_run:,}")
        logger.info(f"⏱️ Время выполнения: {elapsed:.1f} сек")
        
        return enriched_this_run


def calculate_dynamic_features(engine):
    """
    Расчёт динамических признаков
    🔧 ИСПРАВЛЕНИЕ: в PostgreSQL DATE - DATE = INTEGER (дни), не нужен EXTRACT
    """
    logger.info("\n" + "="*70)
    logger.info("РАСЧЁТ ДИНАМИЧЕСКИХ ПРИЗНАКОВ")
    logger.info("="*70)
    
    with engine.connect() as conn:
        start_time = time.time()
        
        # 1. Days since last purchase (для данного SKU)
        # 🔧 В PostgreSQL: DATE - DATE возвращает INTEGER (число дней)
        logger.info("\n📊 Расчёт days_since_last_purchase...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET days_since_last_purchase = (
                SELECT (se.purchase_date - MAX(se2.purchase_date))
                FROM sales_enriched se2
                WHERE se2.client_id = se.client_id
                  AND se2.sku_id = se.sku_id
                  AND se2.purchase_date < se.purchase_date
            )
            WHERE days_since_last_purchase IS NULL
        """))
        conn.commit()
        logger.info("   ✅ days_since_last_purchase рассчитан")
        
        # 2. Frequency 30d / 90d
        logger.info("\n📊 Расчёт frequency_30d / frequency_90d...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET 
                frequency_30d = (
                    SELECT COUNT(*) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '30 days'
                      AND se2.purchase_date < se.purchase_date
                ),
                frequency_90d = (
                    SELECT COUNT(*) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '90 days'
                      AND se2.purchase_date < se.purchase_date
                )
            WHERE frequency_30d IS NULL OR frequency_90d IS NULL
        """))
        conn.commit()
        logger.info("   ✅ frequency_30d / frequency_90d рассчитаны")
        
        # 3. Rolling sales averages (2w, 4w, 8w)
        logger.info("\n📊 Расчёт rolling_sales_2w / 4w / 8w...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET 
                rolling_sales_2w = (
                    SELECT AVG(se2.quantity) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '14 days'
                      AND se2.purchase_date < se.purchase_date
                ),
                rolling_sales_4w = (
                    SELECT AVG(se2.quantity) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '28 days'
                      AND se2.purchase_date < se.purchase_date
                ),
                rolling_sales_8w = (
                    SELECT AVG(se2.quantity) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '56 days'
                      AND se2.purchase_date < se.purchase_date
                )
            WHERE rolling_sales_2w IS NULL OR rolling_sales_4w IS NULL OR rolling_sales_8w IS NULL
        """))
        conn.commit()
        logger.info("   ✅ rolling_sales рассчитаны")
        
        elapsed = time.time() - start_time
        logger.info(f"\n⏱️ Общее время расчёта динамических признаков: {elapsed:.1f} сек")


def calculate_group_features(engine):
    """
    Расчёт групповых признаков
    🔧 ИСПРАВЛЕНИЕ: корректный расчёт дней для PostgreSQL
    """
    logger.info("\n" + "="*70)
    logger.info("РАСЧЁТ ГРУППОВЫХ ПРИЗНАКОВ")
    logger.info("="*70)
    
    with engine.connect() as conn:
        start_time = time.time()
        
        # 1. Days since last purchase in group
        logger.info("\n📊 Расчёт days_since_last_purchase_group...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET days_since_last_purchase_group = (
                SELECT (se.purchase_date - MAX(se2.purchase_date))
                FROM sales_enriched se2
                WHERE se2.client_id = se.client_id
                  AND se2.marketing_group = se.marketing_group
                  AND se2.purchase_date < se.purchase_date
            )
            WHERE days_since_last_purchase_group IS NULL AND se.marketing_group IS NOT NULL
        """))
        conn.commit()
        logger.info("   ✅ days_since_last_purchase_group рассчитан")
        
        # 2. Group share in portfolio
        logger.info("\n📊 Расчёт group_share_in_portfolio...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET group_share_in_portfolio = (
                SELECT COUNT(*)::NUMERIC / NULLIF(
                    (SELECT COUNT(*) FROM sales_enriched se3 
                     WHERE se3.client_id = se.client_id AND se3.purchase_date = se.purchase_date), 0
                )
                FROM sales_enriched se2
                WHERE se2.client_id = se.client_id
                  AND se2.marketing_group = se.marketing_group
                  AND se2.purchase_date = se.purchase_date
            )
            WHERE group_share_in_portfolio IS NULL AND se.marketing_group IS NOT NULL
        """))
        conn.commit()
        logger.info("   ✅ group_share_in_portfolio рассчитан")
        
        # 3. Group trend 6m
        logger.info("\n📊 Расчёт group_trend_6m...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET group_trend_6m = (
                SELECT CASE WHEN prev_6m = 0 THEN 0
                       ELSE (curr_6m - prev_6m)::NUMERIC / prev_6m END
                FROM (
                    SELECT 
                        COUNT(*) FILTER (WHERE purchase_date >= se.purchase_date - INTERVAL '6 months' 
                                          AND purchase_date < se.purchase_date) AS curr_6m,
                        COUNT(*) FILTER (WHERE purchase_date >= se.purchase_date - INTERVAL '12 months' 
                                          AND purchase_date < se.purchase_date - INTERVAL '6 months') AS prev_6m
                    FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id AND se2.marketing_group = se.marketing_group
                ) sub
            )
            WHERE group_trend_6m IS NULL AND se.marketing_group IS NOT NULL
        """))
        conn.commit()
        logger.info("   ✅ group_trend_6m рассчитан")
        
        elapsed = time.time() - start_time
        logger.info(f"\n⏱️ Общее время расчёта групповых признаков: {elapsed:.1f} сек")


def validate_enrichment(engine):
    logger.info("\n" + "="*70)
    logger.info("ПРОВЕРКА КАЧЕСТВА ОБОГАЩЕНИЯ")
    logger.info("="*70)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) as total_records,
                   COUNT(brand) as with_brand,
                   COUNT(product_group) as with_product_group,
                   COUNT(marketing_group) as with_marketing_group
            FROM sales_enriched
        """))
        row = result.fetchone()
        
        total = row[0]
        if total == 0:
            logger.warning("⚠️ Таблица sales_enriched пуста!")
            return False
        
        logger.info(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
        logger.info(f"   Всего записей: {total:,}")
        logger.info(f"   С брендом: {row[1]:,} ({row[1]/total*100:.1f}%)")
        logger.info(f"   С группой товаров: {row[2]:,} ({row[2]/total*100:.1f}%)")
        logger.info(f"   С маркетинговой группой: {row[3]:,} ({row[3]/total*100:.1f}%)")
        
        logger.info(f"\n📋 ПРОВЕРКА КРИТИЧЕСКИХ ПРИЗНАКОВ:")
        critical = ['brand', 'marketing_group', 'stock', 'margin', 'days_since_last_purchase']
        all_ok = True
        for field in critical:
            result = conn.execute(text(f"SELECT COUNT(*) FROM sales_enriched WHERE {field} IS NOT NULL"))
            count = result.fetchone()[0]
            pct = count / total * 100
            status = "✅" if pct >= 50 else "⚠️"
            logger.info(f"   {status} {field}: {count:,} ({pct:.1f}%)")
            if pct < 50:
                all_ok = False
        
        logger.info(f"\n📄 ПРИМЕР ДАННЫХ:")
        result = conn.execute(text("""
            SELECT client_id, sku_id, brand, marketing_group, stock, days_since_last_purchase
            FROM sales_enriched LIMIT 3
        """))
        for row in result.fetchall():
            logger.info(f"   {row[0]} | {row[1]} | {row[2]} | {row[3]} | stock={row[4]} | days_since={row[5]}")
        
        return all_ok


def main():
    logger.info("="*70)
    logger.info("🚀 ProjectZZZ - Обогащение продаж (sales_enriched)")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    start_time = time.time()
    
    try:
        config = load_config()
        engine = get_engine(config)
        logger.info(f"✅ Подключение к БД: {config['database']['name']}")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        return 1
    
    try:
        create_sales_enriched_table(engine)
        enriched_count = enrich_from_purchases(engine)
        
        if enriched_count > 0:
            calculate_dynamic_features(engine)
            calculate_group_features(engine)
        
        validation_ok = validate_enrichment(engine)
        elapsed = time.time() - start_time
        
        logger.info("\n" + "="*70)
        logger.info("📊 ИТОГИ ОБОГАЩЕНИЯ")
        logger.info("="*70)
        logger.info(f"✅ Обогащено записей: {enriched_count:,}")
        logger.info(f"✅ Проверка качества: {'ПРОЙДЕНА' if validation_ok else 'ТРЕБУЕТ ВНИМАНИЯ'}")
        logger.info(f"⏱️ Общее время: {elapsed:.1f} сек")
        logger.info("="*70)
        
        return 0 if validation_ok else 1
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())