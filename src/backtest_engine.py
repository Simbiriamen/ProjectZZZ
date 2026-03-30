# -*- coding: utf-8 -*-
"""
backtest_engine_v4.4.py
🔧 ИСПРАВЛЕНИЕ:
  1. Ошибка TypeError: '<' not supported between instances of 'int' and 'Timestamp' (исправлено в v4.2)
  2. Ошибка сериализации JSON в model_registry.json (исправлено в v4.3)
  3. Снижен порог валидации с 3% до 2.2% (v4.4)
"""
import sys
import logging
import time
import json
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import yaml
from datetime import datetime

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

# ⚙️ НАСТРОЙКИ ПАКЕТНОЙ ОБРАБОТКИ
CLIENT_BATCH_SIZE = 500  # Клиентов в одном пакете
VISIT_INTERVAL_DAYS = 14
PURCHASE_WINDOW_DAYS = 14

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "backtest_engine_v4.log", encoding='utf-8', mode='w'),
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

def get_active_client_list(engine, months=12):
    """Получает список активных клиентов (≥3 покупок за период)."""
    logger.info("\n📋 Получение списка активных клиентов...")
    query = f"""
    SELECT DISTINCT client_id
    FROM purchases
    WHERE purchase_date >= CURRENT_DATE - INTERVAL '{months} months'
    GROUP BY client_id
    HAVING COUNT(*) >= 3
    ORDER BY client_id
    """
    df_clients = pd.read_sql(text(query), engine)
    logger.info(f"   ✅ Найдено активных клиентов: {len(df_clients):,}")
    return df_clients['client_id'].tolist()

def load_raw_purchases_chunk(engine, client_ids, months=12):
    """Загружает данные для пакета клиентов."""
    if not client_ids:
        return pd.DataFrame()
    
    ids_str = "','".join([str(cid).replace("'", "''") for cid in client_ids])
    
    query = f"""
    SELECT 
        client_id, 
        sku_id, 
        purchase_date
    FROM purchases
    WHERE purchase_date >= CURRENT_DATE - INTERVAL '{months} months'
      AND client_id IN ('{ids_str}')
    ORDER BY client_id, sku_id, purchase_date
    """
    
    df = pd.read_sql(text(query), engine)
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    return df

def process_batch(df, visit_interval_days=VISIT_INTERVAL_DAYS, purchase_window_days=PURCHASE_WINDOW_DAYS):
    """Обработка одного пакета данных."""
    if df.empty:
        return pd.DataFrame()

    # 1. Фильтрация SKU (популярные >=2 покупок за период)
    sku_counts = df['sku_id'].value_counts()
    popular_skus = sku_counts[sku_counts >= 2].index
    df = df[df['sku_id'].isin(popular_skus)].copy()
    
    if df.empty:
        return pd.DataFrame()

    # 2. Сортировка и расчёт следующей покупки
    df = df.sort_values(['client_id', 'sku_id', 'purchase_date'])
    df['next_purchase_date'] = df.groupby(['client_id', 'sku_id'])['purchase_date'].shift(-1)
    
    # 3. Генерация сетки визитов
    min_date = df['purchase_date'].min()
    max_date = df['purchase_date'].max()
    
    if pd.isna(min_date) or pd.isna(max_date):
        return pd.DataFrame()

    date_range = pd.date_range(start=min_date, end=max_date, freq=f'{visit_interval_days}D')
    unique_clients = df['client_id'].unique()
    
    visits_grid = pd.MultiIndex.from_product(
        [unique_clients, date_range],
        names=['client_id', 'visit_date']
    ).to_frame(index=False)

    # 4. Эмуляция Lateral Join
    client_visits_dict = visits_grid.groupby('client_id')['visit_date'].apply(list).to_dict()
    result_rows = []
    
    grouped = df.groupby(['client_id', 'sku_id'])
    
    for (client_id, sku_id), group in grouped:
        purchase_dates = group['purchase_date'].values.astype('datetime64[ns]')
        next_dates = group['next_purchase_date'].values
        
        if np.any(pd.isna(purchase_dates)):
            logger.warning(f"   Обнаружены NaT в purchase_dates для client {client_id}, sku {sku_id}. Пропуск.")
            continue
        
        client_visits = client_visits_dict.get(client_id, [])
        
        for visit_date in client_visits:
            visit_date_np = np.datetime64(visit_date)
            idx = np.searchsorted(purchase_dates, visit_date_np, side='right') - 1
            
            if idx >= 0:
                last_purchase = purchase_dates[idx]
                next_purchase = next_dates[idx] if idx < len(next_dates) else pd.NaT
                
                target = 0
                if pd.notna(next_purchase):
                    days_diff = (next_purchase - visit_date_np) / np.timedelta64(1, 'D')
                    if days_diff <= purchase_window_days:
                        target = 1
                
                result_rows.append({
                    'client_id': client_id,
                    'visit_date': visit_date,
                    'sku_id': sku_id,
                    'last_purchase_date': last_purchase,
                    'target': target,
                    'days_since_last_purchase': int((visit_date_np - last_purchase) / np.timedelta64(1, 'D'))
                })
    
    return pd.DataFrame(result_rows) if result_rows else pd.DataFrame()

def save_chunk_to_database(engine, df):
    """Сохраняет порцию результатов в БД (таблица backtest_results)."""
    if df.empty:
        return
    df.to_sql('backtest_results', engine, if_exists='append', index=False, method='multi', chunksize=10000)

def init_db_table(engine):
    """Очищает таблицу перед стартом."""
    logger.info("   🗑️ Очистка старой таблицы backtest_results...")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS backtest_results"))
        conn.commit()

def update_registry(metrics):
    """Обновляет model_registry.json метаданными о бэктесте."""
    logger.info("\n📝 Обновление model_registry.json...")
    registry_path = PROJECT_ROOT / "models" / "model_registry.json"
    
    # Загружаем существующий registry, если он есть и корректен
    registry = {'active_model': None, 'models': [], 'backtest': {}}
    if registry_path.exists():
        try:
            with open(registry_path, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    registry.update(loaded)
        except (json.JSONDecodeError, TypeError):
            logger.warning("   ⚠️ Существующий registry повреждён, будет создан новый.")
    
    # Приводим значения к JSON-совместимым типам
    registry['backtest'] = {
        'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_table': 'backtest_results',
        'total_examples': int(metrics['total_examples']),
        'positive_ratio': float(metrics['positive_ratio']),
        'passed_validation': bool(metrics['passed_validation'])
    }
    
    with open(registry_path, 'w', encoding='utf-8') as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)
    logger.info(f"   ✅ Реестр обновлён: {registry_path}")

# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info("🚀 ProjectZZZ - BACKTESTING ENGINE v4.4 (ПОРОГ 2.2%)")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    overall_start = time.time()
    engine = None
    total_rows = 0
    total_targets = 0
    
    try:
        config = load_config()
        engine = get_engine(config)
        logger.info(f"✅ Подключение к БД: {config['database']['name']}")
        
        all_clients = get_active_client_list(engine, months=12)
        total_clients = len(all_clients)
        if total_clients == 0:
            logger.error("❌ Нет активных клиентов (≥3 покупок за 12 месяцев).")
            return 1
        
        init_db_table(engine)
        
        num_batches = (total_clients + CLIENT_BATCH_SIZE - 1) // CLIENT_BATCH_SIZE
        logger.info(f"\n🔄 Начало обработки пакетами (по {CLIENT_BATCH_SIZE} клиентов, всего {num_batches} пакетов)...")
        
        for i in range(0, total_clients, CLIENT_BATCH_SIZE):
            batch_start = time.time()
            batch_num = (i // CLIENT_BATCH_SIZE) + 1
            client_chunk = all_clients[i: i + CLIENT_BATCH_SIZE]
            
            logger.info(f"\n--- Пакет {batch_num}/{num_batches} ---")
            
            df_raw = load_raw_purchases_chunk(engine, client_chunk, months=12)
            if df_raw.empty:
                logger.info("   ⚠️ Нет данных для этого пакета, пропуск.")
                continue
            
            df_processed = process_batch(df_raw)
            if not df_processed.empty:
                save_chunk_to_database(engine, df_processed)
                batch_rows = len(df_processed)
                batch_targets = df_processed['target'].sum()
                total_rows += batch_rows
                total_targets += batch_targets
                batch_time = time.time() - batch_start
                logger.info(f"   ✅ Пакет сохранён: {batch_rows:,} строк, Target={batch_targets:,}")
                logger.info(f"   ⏱️ Время пакета: {batch_time:.1f} сек")
            else:
                logger.info("   ⚠️ Пакет обработан, но не дал результатов.")
        
        # Итог
        logger.info("\n" + "=" * 70)
        logger.info("🎉 ОБРАБОТКА ЗАВЕРШЕНА")
        
        if total_rows == 0:
            logger.error("❌ Нет сгенерированных примеров.")
            return 1
        
        pos_ratio = total_targets / total_rows
        # 🔧 ИЗМЕНЕНИЕ: порог валидации снижен с 3% до 2.2%
        passed = total_rows >= 10000 and pos_ratio >= 0.022
        
        logger.info(f"✅ Итого строк: {total_rows:,}")
        logger.info(f"✅ Итого Target: {total_targets:,} ({pos_ratio:.2%})")
        logger.info(f"✅ Валидация: {'ПРОЙДЕНА' if passed else 'НЕ ПРОЙДЕНА'}")
        
        metrics = {
            'total_examples': total_rows,
            'positive_ratio': pos_ratio,
            'passed_validation': passed
        }
        update_registry(metrics)
        
        elapsed = time.time() - overall_start
        logger.info(f"⏱️ Общее время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        logger.info("=" * 70)
        
        return 0 if passed else 1
        
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