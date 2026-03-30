# -*- coding: utf-8 -*-
"""
enrich_sales_full.py v2.1
🔧 ИСПРАВЛЕНИЯ:
  1. Добавлен РАСЧЁТ global_popularity (популярность у всех клиентов)
  2. Добавлен РАСЧЁТ portfolio_diversity (уникальных категорий у клиента)
  3. Добавлен is_new_flag (новинка каталога)
  4. Окно покупки: [визит, визит+9] = 10 дней включительно
"""
import sys
import logging
import time
from pathlib import Path
from sqlalchemy import create_engine, text
import yaml
from datetime import datetime

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
        logging.FileHandler(LOG_DIR / "enrich_sales_full.log", encoding='utf-8', mode='w'),
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


def get_skus_columns(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'skus'
        """))
        return set(row[0] for row in result.fetchall())


def get_clients_columns(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'clients'
        """))
        return set(row[0] for row in result.fetchall())


def enrich_all_purchases(engine):
    """Обогащение ВСЕХ данных с новыми признаками"""
    logger.info("\n" + "="*70)
    logger.info("ПОЛНОЕ ОБОГАЩЕНИЕ ВСЕХ ДАННЫХ (v2.1 — НОВЫЕ ПРИЗНАКИ + РАСЧЁТ)")
    logger.info("="*70)
    
    skus_cols = get_skus_columns(engine)
    clients_cols = get_clients_columns(engine)
    
    logger.info(f"📋 Реальные колонки в skus: {sorted(skus_cols)}")
    logger.info(f"📋 Реальные колонки в clients: {sorted(clients_cols)}")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM purchases"))
        purchases_count = result.fetchone()[0]
        logger.info(f"📊 Записей в purchases: {purchases_count:,}")
        
        result = conn.execute(text("SELECT COUNT(*) FROM sales_enriched"))
        enriched_count = result.fetchone()[0]
        logger.info(f"📊 Уже обогащено: {enriched_count:,}")
        
        remaining = purchases_count - enriched_count
        logger.info(f"🔄 Осталось обогатить: {remaining:,}")
        
        if remaining <= 0:
            logger.info("✅ Все данные уже обогащены!")
            return 0
        
        logger.info("\n💾 Запуск обогащения (JOIN с справочниками)...")
        start_time = time.time()
        
        def col_or_null(col_name, table_alias='s', default='NULL'):
            if table_alias == 's' and col_name in skus_cols:
                if any(c in col_name for c in ' \t\n\r') or not col_name.isascii():
                    return f's."{col_name}"'
                return f's.{col_name}'
            elif table_alias == 'c' and col_name in clients_cols:
                if any(c in col_name for c in ' \t\n\r') or not col_name.isascii():
                    return f'c."{col_name}"'
                return f'c.{col_name}'
            return default
        
        article_col = 'Артикул' if 'Артикул' in skus_cols else 'Артикул ' if 'Артикул ' in skus_cols else 'sku_id'
        
        sku_select = [
            col_or_null('brand'),
            f"{col_or_null('product_group')} AS product_group",
            f"{col_or_null('marketing_group1')} AS marketing_group",
            f"{col_or_null('marketing_group2')} AS marketing_group2",
            col_or_null('category'),
            col_or_null('margin'),
            col_or_null('stock'),
            col_or_null('is_new'),  # 🔧 Новинка
            f"{col_or_null(article_col)} AS article",
            col_or_null('applicability'),  # 🔧 Автомобиль
            col_or_null('applicable_brands'),
            col_or_null('brand_specialization'),
            col_or_null('analog_set'),  # 🔧 Аналоги
            col_or_null('replacement_set'),
        ]
        
        client_select = [
            'c.client_name',
            'c.segment AS client_segment',
            col_or_null('sales_channel', 'c'),  # 🔧 Канал продаж
            'c.network_name',
            'c.manager_id',
        ]
        
        enrich_sql = f"""
        INSERT INTO sales_enriched (
            purchase_id, client_id, sku_id, purchase_date, warehouse,
            quantity, amount, price,
            brand, product_group, marketing_group, marketing_group2,
            category, margin, stock, is_new, article, applicability,
            applicable_brands, brand_specialization, analog_set, replacement_set,
            client_name, client_segment, sales_channel, network_name, manager_id,
            warehouse_code, max_norm, paired_item, is_kit,
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
            {', '.join(sku_select)},
            {', '.join(client_select)},
            m.warehouse_code,
            m.max_norm,
            m.paired_item,
            m.is_kit,
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
        """
        
        result = conn.execute(text(enrich_sql))
        conn.commit()
        
        enriched_this_run = result.rowcount
        elapsed = time.time() - start_time
        
        logger.info(f"\n✅ Обогащено записей: {enriched_this_run:,}")
        logger.info(f"⏱️ Время выполнения: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        
        return enriched_this_run


def calculate_all_dynamic_features(engine):
    """🔧 РАСЧЁТ ДИНАМИЧЕСКИХ ПРИЗНАКОВ v2.1 — с global_popularity и portfolio_diversity"""
    logger.info("\n" + "="*70)
    logger.info("РАСЧЁТ ДИНАМИЧЕСКИХ ПРИЗНАКОВ (v2.1 — НОВЫЕ ПРИЗНАКИ)")
    logger.info("="*70)
    
    with engine.connect() as conn:
        start_time = time.time()
        
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
        
        logger.info("\n📊 Расчёт frequency_30d / frequency_90d...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET 
                frequency_30d = (
                    SELECT COUNT(*) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '30 days'
                      AND se2.purchase_date <= se.purchase_date  -- 🔧 <= включает день визита
                ),
                frequency_90d = (
                    SELECT COUNT(*) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '90 days'
                      AND se2.purchase_date <= se.purchase_date
                )
            WHERE frequency_30d IS NULL OR frequency_90d IS NULL
        """))
        conn.commit()
        logger.info("   ✅ frequency_30d / frequency_90d рассчитаны")
        
        logger.info("\n📊 Расчёт rolling_sales_2w / 4w / 8w...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET 
                rolling_sales_2w = (
                    SELECT AVG(se2.quantity) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '14 days'
                      AND se2.purchase_date <= se.purchase_date
                ),
                rolling_sales_4w = (
                    SELECT AVG(se2.quantity) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '28 days'
                      AND se2.purchase_date <= se.purchase_date
                ),
                rolling_sales_8w = (
                    SELECT AVG(se2.quantity) FROM sales_enriched se2
                    WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
                      AND se2.purchase_date >= se.purchase_date - INTERVAL '56 days'
                      AND se2.purchase_date <= se.purchase_date
                )
            WHERE rolling_sales_2w IS NULL OR rolling_sales_4w IS NULL OR rolling_sales_8w IS NULL
        """))
        conn.commit()
        logger.info("   ✅ rolling_sales рассчитаны")
        
        # 🔧 НОВЫЙ: global_popularity (частота покупок ВСЕМИ клиентами за 90 дней)
        logger.info("\n📊 Расчёт global_popularity (популярность SKU у всех клиентов за 90 дней)...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET global_popularity = (
                SELECT COUNT(*) FROM sales_enriched se2
                WHERE se2.sku_id = se.sku_id
                  AND se2.purchase_date >= se.purchase_date - INTERVAL '90 days'
                  AND se2.purchase_date <= se.purchase_date  -- 🔧 Включая текущий день
            )
            WHERE global_popularity IS NULL
        """))
        conn.commit()
        logger.info("   ✅ global_popularity рассчитан")
        
        # 🔧 НОВЫЙ: portfolio_diversity (уникальных категорий у клиента за 6 мес)
        logger.info("\n📊 Расчёт portfolio_diversity (уникальных категорий у клиента за 6 мес)...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET portfolio_diversity = (
                SELECT COUNT(DISTINCT se2.category) FROM sales_enriched se2
                WHERE se2.client_id = se.client_id
                  AND se2.purchase_date >= se.purchase_date - INTERVAL '6 months'
                  AND se2.purchase_date <= se.purchase_date  -- 🔧 Включая текущий день
                  AND se2.category IS NOT NULL
            )
            WHERE portfolio_diversity IS NULL
        """))
        conn.commit()
        logger.info("   ✅ portfolio_diversity рассчитан")
        
        elapsed = time.time() - start_time
        logger.info(f"\n⏱️ Общее время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")


def calculate_all_group_features(engine):
    """Расчёт групповых признаков"""
    logger.info("\n" + "="*70)
    logger.info("РАСЧЁТ ГРУППОВЫХ ПРИЗНАКОВ (ВСЕ ДАННЫЕ)")
    logger.info("="*70)
    
    with engine.connect() as conn:
        start_time = time.time()
        
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
        
        logger.info("\n📊 Расчёт group_trend_6m...")
        conn.execute(text("""
            UPDATE sales_enriched se
            SET group_trend_6m = (
                SELECT CASE WHEN prev_6m = 0 THEN 0
                       ELSE (curr_6m - prev_6m)::NUMERIC / NULLIF(prev_6m, 0) END 
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
        logger.info(f"\n⏱️ Общее время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")


def validate_all_enrichment(engine):
    """Финальная проверка ВСЕХ данных"""
    logger.info("\n" + "="*70)
    logger.info("ФИНАЛЬНАЯ ПРОВЕРКА ВСЕХ ДАННЫХ")
    logger.info("="*70)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(brand) as with_brand,
                COUNT(marketing_group) as with_marketing_group,
                COUNT(days_since_last_purchase) as with_days_since,
                COUNT(global_popularity) as with_global_pop,
                COUNT(portfolio_diversity) as with_portfolio_div
            FROM sales_enriched
        """))
        row = result.fetchone()
        
        total = row[0]
        
        logger.info(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
        logger.info(f"   ✅ Всего записей: {total:,}")
        logger.info(f"   ✅ С брендом: {row[1]:,} ({row[1]/total*100:.1f}%)")
        logger.info(f"   ✅ С маркетинговой группой: {row[2]:,} ({row[2]/total*100:.1f}%)")
        logger.info(f"   ✅ С days_since: {row[3]:,} ({row[3]/total*100:.1f}%)")
        logger.info(f"   ✅ С global_popularity: {row[4]:,} ({row[4]/total*100:.1f}%)")
        logger.info(f"   ✅ С portfolio_diversity: {row[5]:,} ({row[5]/total*100:.1f}%)")
        
        logger.info(f"\n📄 ПРИМЕР ДАННЫХ:")
        result = conn.execute(text("""
            SELECT client_id, sku_id, brand, marketing_group, stock, 
                   days_since_last_purchase, global_popularity, portfolio_diversity
            FROM sales_enriched 
            WHERE days_since_last_purchase IS NOT NULL
            LIMIT 5
        """))
        for row in result.fetchall():
            logger.info(f"   {row[0]} | {row[1]} | {row[2]} | {row[3]} | stock={row[4]} | days_since={row[5]} | global_pop={row[6]} | portfolio_div={row[7]}")
        
        return total


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info("="*70)
    logger.info("🚀 ProjectZZZ - ПОЛНОЕ ОБОГАЩЕНИЕ (v2.1 — НОВЫЕ ПРИЗНАКИ + РАСЧЁТ)")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    start_time = time.time()
    engine = None

    try:
        config = load_config()
        engine = get_engine(config)
        logger.info(f"✅ Подключение к БД: {config['database']['name']}")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        return 1

    try:
        enriched_count = enrich_all_purchases(engine)
        
        if enriched_count > 0:
            calculate_all_dynamic_features(engine)
            calculate_all_group_features(engine) 
        
        total = validate_all_enrichment(engine)
        
        elapsed = time.time() - start_time
        
        logger.info("\n" + "="*70)
        logger.info("🎉 ОБОГАЩЕНИЕ ЗАВЕРШЕНО!")
        logger.info("="*70)
        logger.info(f"✅ Всего записей в sales_enriched: {total:,}")
        logger.info(f"⏱️ Общее время: {elapsed:.1f} сек ({elapsed/3600:.2f} ч)")
        logger.info("="*70)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if engine:
            engine.dispose()


if __name__ == "__main__":
    sys.exit(main())