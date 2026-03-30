# -*- coding: utf-8 -*-
"""
diagnose_fallback.py v1.1 — Диагностика причин высокого fallback
Запуск: python src\diagnose_fallback.py
🔧 ИСПРАВЛЕНИЕ: корректная обработка TEXT-поля is_new в PostgreSQL
"""
import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd
import yaml
from datetime import datetime

PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "diagnose_fallback.log", encoding='utf-8', mode='w'),
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

def analyze_fallback_reasons(engine):
    """Анализирует причины fallback из visit_proposals"""
    logger.info("\n🔍 АНАЛИЗ ПРИЧИН FALLBACK")
    logger.info("="*60)
    
    query = """
    SELECT 
        fallback_reason,
        COUNT(DISTINCT client_id) as client_count,
        COUNT(*) as recommendation_count,
        AVG(predicted_prob) as avg_prob
    FROM visit_proposals
    WHERE visit_date = CURRENT_DATE
    GROUP BY fallback_reason
    ORDER BY client_count DESC
    """
    
    df = pd.read_sql(text(query), engine)
    
    for _, row in df.iterrows():
        logger.info(f"📊 {row['fallback_reason'] or 'NULL'}:")
        logger.info(f"   • Клиентов: {row['client_count']:,}")
        logger.info(f"   • Рекомендаций: {row['recommendation_count']:,}")
        logger.info(f"   • Средняя вероятность: {row['avg_prob']:.1%}")
        logger.info("")
    
    return df

def analyze_candidate_availability(engine):
    """
    🔧 ИСПРАВЛЕНИЕ: Корректная обработка TEXT-поля is_new
    Анализирует доступность кандидатов для клиентов
    """
    logger.info("\n🔍 АНАЛИЗ ДОСТУПНОСТИ КАНДИДАТОВ")
    logger.info("="*60)
    
    query = """
    WITH client_candidates AS (
        SELECT 
            se.client_id,
            COUNT(DISTINCT se.sku_id) as candidate_count,
            AVG(se.stock) as avg_stock,
            -- 🔧 Исправлено: is_new — TEXT, проверяем через LOWER()
            COUNT(CASE WHEN LOWER(s.is_new) IN ('да', 'true', '1', 'yes', 't') THEN 1 END) as new_skus,
            COUNT(CASE WHEN LOWER(s.is_new) NOT IN ('да', 'true', '1', 'yes', 't') 
                      AND se.group_trend_6m > 0.02 THEN 1 END) as develop_skus,
            COUNT(CASE WHEN LOWER(s.is_new) NOT IN ('да', 'true', '1', 'yes', 't') 
                      AND se.group_trend_6m < -0.02 THEN 1 END) as retain_skus
        FROM sales_enriched se
        JOIN skus s ON se.sku_id = s.sku_id
        WHERE se.client_id IN (
            SELECT DISTINCT client_id FROM purchases 
            WHERE purchase_date >= CURRENT_DATE - INTERVAL '90 days'
        )
        AND s.stock >= 1
        AND se.purchase_date = (
            SELECT MAX(purchase_date) FROM sales_enriched se2 
            WHERE se2.client_id = se.client_id
        )
        GROUP BY se.client_id
    )
    SELECT 
        CASE 
            WHEN candidate_count = 0 THEN '0 кандидатов'
            WHEN candidate_count BETWEEN 1 AND 4 THEN '1-4 кандидата'
            WHEN candidate_count BETWEEN 5 AND 9 THEN '5-9 кандидатов'
            WHEN candidate_count BETWEEN 10 AND 19 THEN '10-19 кандидатов'
            ELSE '20+ кандидатов'
        END as candidate_range,
        COUNT(*) as client_count,
        AVG(candidate_count) as avg_candidates,
        AVG(new_skus) as avg_new,
        AVG(develop_skus) as avg_develop,
        AVG(retain_skus) as avg_retain
    FROM client_candidates
    GROUP BY candidate_range
    ORDER BY avg_candidates
    """
    
    df = pd.read_sql(text(query), engine)
    
    for _, row in df.iterrows():
        logger.info(f"📦 {row['candidate_range']}:")
        logger.info(f"   • Клиентов: {row['client_count']:,}")
        logger.info(f"   • Ср. кандидатов: {row['avg_candidates']:.1f}")
        logger.info(f"   • Ср. новых: {row['avg_new']:.1f}")
        logger.info(f"   • Ср. развитие: {row['avg_develop']:.1f}")
        logger.info(f"   • Ср. возврат: {row['avg_retain']:.1f}")
        logger.info("")
    
    return df

def analyze_probability_distribution(engine):
    """Анализирует распределение вероятностей"""
    logger.info("\n🔍 АНАЛИЗ РАСПРЕДЕЛЕНИЯ ВЕРОЯТНОСТЕЙ")
    logger.info("="*60)
    
    query = """
    SELECT 
        CASE 
            WHEN predicted_prob < 0.1 THEN '< 10%'
            WHEN predicted_prob < 0.2 THEN '10-20%'
            WHEN predicted_prob < 0.3 THEN '20-30%'
            WHEN predicted_prob < 0.4 THEN '30-40%'
            WHEN predicted_prob < 0.5 THEN '40-50%'
            WHEN predicted_prob < 0.6 THEN '50-60%'
            WHEN predicted_prob < 0.7 THEN '60-70%'
            WHEN predicted_prob < 0.8 THEN '70-80%'
            ELSE '>= 80%'
        END as prob_range,
        COUNT(*) as count,
        COUNT(CASE WHEN selection_type = 'new' THEN 1 END) as new_count,
        COUNT(CASE WHEN selection_type = 'develop' THEN 1 END) as develop_count,
        COUNT(CASE WHEN selection_type = 'retain' THEN 1 END) as retain_count
    FROM visit_proposals
    WHERE visit_date = CURRENT_DATE
    GROUP BY prob_range
    ORDER BY prob_range
    """
    
    df = pd.read_sql(text(query), engine)
    
    for _, row in df.iterrows():
        total = row['count']
        new_pct = row['new_count']/total*100 if total > 0 else 0
        dev_pct = row['develop_count']/total*100 if total > 0 else 0
        ret_pct = row['retain_count']/total*100 if total > 0 else 0
        
        logger.info(f"🎯 {row['prob_range']}:")
        logger.info(f"   • Всего: {total:,} (100.0%)")
        logger.info(f"   • New: {row['new_count']:,} ({new_pct:.1f}%)")
        logger.info(f"   • Develop: {row['develop_count']:,} ({dev_pct:.1f}%)")
        logger.info(f"   • Retain: {row['retain_count']:,} ({ret_pct:.1f}%)")
        logger.info("")
    
    return df

def main():
    logger.info("="*70)
    logger.info("🔧 ProjectZZZ - ДИАГНОСТИКА FALLBACK v1.1")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    try:
        config = load_config()
        engine = get_engine(config)
        
        # Запуск анализов
        analyze_fallback_reasons(engine)
        analyze_candidate_availability(engine)
        analyze_probability_distribution(engine)
        
        logger.info("\n" + "="*70)
        logger.info("✅ ДИАГНОСТИКА ЗАВЕРШЕНА")
        logger.info("="*70)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())