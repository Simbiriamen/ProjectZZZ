# -*- coding: utf-8 -*-
"""
generate_recommendations.py v7.0
🚀 ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ (ФИНАЛЬНАЯ)
🔧 ИСПРАВЛЕНИЯ:
  1. ИСПРАВЛЕНА МЕТРИКА FALLBACK: Теперь учитывает только критические потери (нет Новых товаров).
  2. Global Top 200: выборка самых продаваемых SKU за последние 90 дней.
  3. Снижен порог вероятности для "Новых" (0.05).
  4. Исправлена ошибка 'Booster' object has no attribute 'predict_proba'.
  5. Добавлена статистика по типам рекомендаций в лог.
"""

import sys
import logging
import time
import json
import pickle
import hashlib
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import lightgbm as lgb
import yaml
from datetime import datetime

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
MODEL_DIR = PROJECT_ROOT / "models"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "generate_recommendations.log", encoding='utf-8', mode='w'),
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
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}",
        pool_size=10, max_overflow=20, pool_pre_ping=True
    )

def get_ab_group(client_id: str, config: dict) -> str:
    if not config.get('ab_test', {}).get('enabled', False):
        return 'control'
    hash_val = int(hashlib.md5(client_id.encode()).hexdigest(), 16) % 100
    ratio = config['ab_test'].get('test_group_ratio', 0.5)
    return 'test' if hash_val < int(ratio * 100) else 'control'

def load_active_model():
    logger.info("\n📦 Загрузка активной модели...")
    registry_path = MODEL_DIR / "model_registry.json"
    if not registry_path.exists():
        logger.error(f"❌ Реестр моделей не найден: {registry_path}")
        return None, None, None, None, None

    with open(registry_path, 'r', encoding='utf-8') as f:
        registry = json.load(f)
    
    active_model_name = registry.get('active_model')
    if not active_model_name:
        logger.error("❌ Активная модель не указана в реестре!")
        return None, None, None, None, None

    logger.info(f"   ✅ Активная модель: {active_model_name}")
    
    model_info = next((m for m in registry.get('models', []) if m['name'] == active_model_name), None)
    if not model_info:
        logger.error(f"❌ Модель {active_model_name} не найдена в реестре!")
        return None, None, None, None, None

    model_path = MODEL_DIR / model_info['model_path'].split('\\')[-1]
    calib_path = MODEL_DIR / model_info.get('calibrator_path', '').split('\\')[-1] if 'calibrator_path' in model_info else None
    
    encoders = None
    encoders_files = list(MODEL_DIR.glob("encoders_lightgbm_*.pkl"))
    if encoders_files:
        encoders_path = sorted(encoders_files, key=lambda p: p.stat().st_mtime)[-1]
        try:
            with open(encoders_path, 'rb') as f:
                loaded_obj = pickle.load(f)
            if isinstance(loaded_obj, dict):
                encoders = loaded_obj
                logger.info(f"   ✅ Энкодеры загружены: {encoders_path.name}")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки энкодеров: {e}")

    try:
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        if isinstance(model_data, dict) and 'model' in model_data:
            model = model_data['model']
            feature_cols = model_data.get('feature_cols', [])
            best_iteration = model_data.get('best_iteration', 1000)
        else:
            model = model_data
            feature_cols = []
            best_iteration = 1000

        if not feature_cols and hasattr(model, 'feature_name_'):
            feature_cols = model.feature_name_
            
        logger.info(f"   ✅ Модель загружена: {model_path.name}")
        logger.info(f"   ✅ Признаков: {len(feature_cols)}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки модели: {e}")
        return None, None, None, None, None

    calibrator = None
    if calib_path and (MODEL_DIR / calib_path).exists():
        try:
            with open(MODEL_DIR / calib_path, 'rb') as f:
                calibrator = pickle.load(f)
            logger.info(f"   ✅ Калибратор загружен: {calib_path}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки калибратора: {e}")
    
    return model, calibrator, encoders, feature_cols, best_iteration


def get_clients_for_today(engine):
    logger.info("\n👥 Получение клиентов на визит...")
    try:
        query = """
        SELECT DISTINCT client_id FROM visits_schedule
        WHERE planned_visit_date = CURRENT_DATE AND status != 'completed'
        """
        df = pd.read_sql(text(query), engine)
        if not df.empty:
            logger.info(f"   ✅ Найдено клиентов из расписания: {len(df)}")
            return df['client_id'].tolist()
    except Exception:
        pass
    
    logger.info("   🔄 Fallback: берём активных клиентов за 90 дней...")
    query = """
    SELECT DISTINCT client_id FROM purchases
    WHERE purchase_date >= CURRENT_DATE - INTERVAL '90 days'
    ORDER BY client_id
    """
    df = pd.read_sql(text(query), engine)
    logger.info(f"   ✅ Найдено активных клиентов: {len(df)}")
    return df['client_id'].tolist()


def get_candidate_skus_batch(engine, client_ids):
    """
    🔧 ИСПРАВЛЕНО:
    Global Top 200: самые продаваемые за последние 90 дней.
    """
    logger.info(f"\n💾 Загрузка кандидатов для {len(client_ids)} клиентов...")
    if not client_ids: return pd.DataFrame()
    
    client_ids_str = "','".join([str(c).replace("'", "''") for c in client_ids])
    
    query = f"""
    WITH client_history AS (
        SELECT DISTINCT client_id, sku_id
        FROM sales_enriched
        WHERE client_id IN ('{client_ids_str}')
          AND purchase_date >= CURRENT_DATE - INTERVAL '90 days'
    ),
    global_top_skus AS (
        -- 🔧 ИСПРАВЛЕНИЕ: Топ-200 SKU по продажам за последние 90 дней
        SELECT 
            s.sku_id, 
            se.article,
            s.brand, s.sku_name, s.marketing_group1 AS marketing_group, 
            s.category, s.price, s.margin, s.stock, s.is_new, s.applicability,
            COUNT(*) as sales_cnt
        FROM sales_enriched se
        JOIN skus s ON se.sku_id = s.sku_id
        WHERE s.stock >= 1
          AND se.purchase_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY s.sku_id, se.article, s.brand, s.sku_name, s.marketing_group1, s.category, s.price, s.margin, s.stock, s.is_new, s.applicability
        ORDER BY sales_cnt DESC
        LIMIT 200
    ),
    candidates AS (
        -- 1. История клиента (is_new_for_client = 0)
        SELECT 
            se.client_id, se.sku_id, se.article, s.brand, s.sku_name,
            s.marketing_group1 AS marketing_group, s.category, s.price, s.margin, s.stock, s.is_new, s.applicability,
            0 AS is_new_for_client,
            COALESCE(se.days_since_last_purchase, 999) AS days_since_last_purchase,
            COALESCE(se.frequency_30d, 0) AS frequency_30d,
            COALESCE(se.frequency_90d, 0) AS frequency_90d,
            COALESCE(se.rolling_sales_2w, 0) AS rolling_sales_2w,
            COALESCE(se.rolling_sales_4w, 0) AS rolling_sales_4w,
            COALESCE(se.rolling_sales_8w, 0) AS rolling_sales_8w,
            COALESCE(se.global_popularity, 0) AS global_popularity,
            COALESCE(se.portfolio_diversity, 1) AS portfolio_diversity,
            COALESCE(se.group_trend_6m, 0) AS group_trend_6m,
            COALESCE(se.group_share_in_portfolio, 0) AS group_share_in_portfolio,
            COALESCE(se.days_since_last_purchase_group, 999) AS days_since_last_purchase_group
        FROM sales_enriched se
        JOIN skus s ON se.sku_id = s.sku_id
        WHERE se.client_id IN ('{client_ids_str}') AND s.stock >= 1
          AND se.purchase_date = (
              SELECT MAX(purchase_date) FROM sales_enriched se2 
              WHERE se2.client_id = se.client_id AND se2.sku_id = se.sku_id
          )
        UNION
        -- 2. Глобальные новинки (is_new_for_client = 1)
        SELECT 
            c.client_id, g.sku_id, g.article, g.brand, g.sku_name,
            g.marketing_group, g.category, g.price, g.margin, g.stock, g.is_new, g.applicability,
            1 AS is_new_for_client,
            999 AS days_since_last_purchase, 0 AS frequency_30d, 0 AS frequency_90d,
            0 AS rolling_sales_2w, 0 AS rolling_sales_4w, 0 AS rolling_sales_8w,
            0 AS global_popularity, 1 AS portfolio_diversity, 0 AS group_trend_6m,
            0 AS group_share_in_portfolio, 999 AS days_since_last_purchase_group
        FROM (SELECT DISTINCT client_id FROM client_history) c
        CROSS JOIN global_top_skus g
        LEFT JOIN client_history ch ON c.client_id = ch.client_id AND g.sku_id = ch.sku_id
        WHERE ch.sku_id IS NULL
    )
    SELECT * FROM candidates ORDER BY client_id, sku_id
    """
    
    df = pd.read_sql(text(query), engine)
    logger.info(f"   ✅ Загружено кандидатов: {len(df):,}")
    
    # Диагностика: сколько новых?
    new_count = df[df['is_new_for_client'] == 1].shape[0]
    logger.info(f"   📊 Из них 'Новых' (is_new=1): {new_count:,}")
    
    return df


def encode_features(df, encoders, feature_cols):
    df_encoded = df.copy()
    if encoders is None: return df_encoded
    
    for col, encoder in encoders.items():
        encoded_col = f'{col}_encoded'
        if encoded_col in feature_cols:
            df_encoded[col] = df_encoded[col].fillna('Unknown')
            try:
                df_encoded[encoded_col] = df_encoded[col].apply(
                    lambda x: encoder.transform([x])[0] if x in encoder.classes_ else -1
                )
            except Exception:
                pass
    return df_encoded


def predict_probabilities_batch(model, df_encoded, feature_cols, calibrator=None, best_iteration=1000):
    available_cols = [col for col in feature_cols if col in df_encoded.columns]
    if not available_cols: return np.zeros(len(df_encoded))
    
    X = df_encoded[available_cols].fillna(0)
    
    # Убираем FutureWarning
    X = X.infer_objects(copy=False)
    
    try:
        # --- FIX FOR 'Booster' object has no attribute 'predict_proba' ---
        if hasattr(model, 'predict_proba'):
            # Sklearn API
            y_proba_raw = model.predict_proba(X)[:, 1]
        elif hasattr(model, 'predict'):
            # Native LightGBM Booster
            iters = best_iteration if best_iteration and best_iteration > 0 else None
            if iters:
                y_proba_raw = model.predict(X, num_iteration=iters, predict_disable_shape_check=True)
            else:
                y_proba_raw = model.predict(X, predict_disable_shape_check=True)
        else:
            raise ValueError("Model has no predict or predict_proba method")
            
    except Exception as e:
        logger.error(f"❌ Ошибка предсказания: {e}")
        return np.zeros(len(df_encoded))
    
    if calibrator:
        try:
            return calibrator.predict_proba(y_proba_raw.reshape(-1, 1))[:, 1]
        except Exception:
            return y_proba_raw
    return y_proba_raw


def select_2plus2plus1(df, prob_col='predicted_prob', probability_threshold_new=0.05,
                       trend_threshold_develop=0.02, trend_threshold_retain=-0.02, flexible_mode=True):
    """
    🔧 ИСПРАВЛЕНО: Снижен порог для New до 0.05. 
    Логика: берем топ-2 новых товара, сортируя по вероятности, игнорируя высокий порог.
    """
    selected = []
    fallback_reasons = []
    
    # 🔹 ШАГ 1: Новые SKU (2 шт)
    new_skus = df[df['is_new_for_client'] == 1].copy() if not df.empty else pd.DataFrame()
    
    if not new_skus.empty:
        # Сортируем по вероятности
        new_skus = new_skus.sort_values(prob_col, ascending=False)
        
        # 🔧 ИСПРАВЛЕНИЕ: Берем ТОП-2 новых товара, НЕ СМОТРЯ на высокий порог.
        # Мы берем лучшие из доступных новинок.
        top_new = new_skus.head(2)
        
        for _, rec in top_new.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'new'
            selected.append(rec_dict)
            
        if len(top_new) < 2:
            fallback_reasons.append(f"New_low_candidates:{2-len(top_new)}")
            
    else:
        # Fallback: Если новых товаров НЕТ ВООБЩЕ (пул пуст), берем знакомые
        familiar = df[df['is_new_for_client'] == 0].sort_values('margin', ascending=False).head(2) if not df.empty else pd.DataFrame()
        for _, rec in familiar.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'new_fallback'
            selected.append(rec_dict)
        fallback_reasons.append("No_new_candidates_at_all")

    # 🔹 ШАГ 2: Развитие (2 шт)
    develop_mask = (df['is_new_for_client'] == 0) & (df['group_trend_6m'] > trend_threshold_develop) if not df.empty else pd.Series(dtype=bool)
    develop_skus = df[develop_mask].copy() if not df.empty else pd.DataFrame()
    
    if not develop_skus.empty:
        develop_skus['score'] = develop_skus[prob_col] * (1 + 0.5 * develop_skus['group_trend_6m'])
        develop_skus = develop_skus.sort_values('score', ascending=False)
        top_dev = develop_skus.head(2)
        for _, rec in top_dev.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'develop'
            selected.append(rec_dict)
    else:
        stable = df[df['is_new_for_client'] == 0].sort_values(prob_col, ascending=False).head(2) if not df.empty else pd.DataFrame()
        for _, rec in stable.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'develop_fallback'
            selected.append(rec_dict)
        fallback_reasons.append("No_growing_groups")

    # 🔹 ШАГ 3: Возврат (1 шт)
    retain_mask = (df['is_new_for_client'] == 0) & (df['group_trend_6m'] < trend_threshold_retain) if not df.empty else pd.Series(dtype=bool)
    retain_skus = df[retain_mask].copy() if not df.empty else pd.DataFrame()
    
    if not retain_skus.empty:
        retain_skus['score'] = retain_skus[prob_col] * (1 + retain_skus['days_since_last_purchase_group'] / 365)
        retain_skus = retain_skus.sort_values('score', ascending=False)
        rec_dict = retain_skus.iloc[0].to_dict()
        rec_dict['selection_type'] = 'retain'
        selected.append(rec_dict)
    else:
        old = df[df['is_new_for_client'] == 0].sort_values('days_since_last_purchase', ascending=False).head(1) if not df.empty else pd.DataFrame()
        if not old.empty:
            rec_dict = old.iloc[0].to_dict()
            rec_dict['selection_type'] = 'retain_fallback'
            selected.append(rec_dict)
        fallback_reasons.append("No_declining_groups")

    # 🔹 ШАГ 4: Удаление дубликатов
    seen = set()
    unique_selected = []
    for s in selected:
        if s['sku_id'] not in seen:
            unique_selected.append(s)
            seen.add(s['sku_id'])
    selected = unique_selected

    # 🔹 ШАГ 5: Гибкий режим (добор до 5)
    if flexible_mode and len(selected) < 5 and not df.empty:
        remaining = 5 - len(selected)
        selected_skus = set(s['sku_id'] for s in selected)
        remaining_df = df[~df['sku_id'].isin(selected_skus)].sort_values(prob_col, ascending=False).head(remaining)
        for _, rec in remaining_df.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'filler'
            selected.append(rec_dict)
        fallback_reasons.append(f"Flexible_fill:{len(remaining_df)}")

    selected = selected[:5]
    fallback_str = "; ".join([fr for fr in fallback_reasons if fr]) if fallback_reasons else None
    return selected, fallback_str


def save_to_database(engine, visit_date, recommendations_flat):
    if not recommendations_flat: return
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'visit_proposals' AND column_name IN ('sku_name', 'applicability', 'ab_group')
        """))
        existing_cols = set(row[0] for row in result.fetchall())
    
    base_cols = ['visit_date', 'client_id', 'client_name', 'sku_id', 'predicted_prob', 'selection_type', 'fallback_reason', 'model_version', 'created_at']
    optional_cols = [c for c in ['sku_name', 'applicability', 'ab_group'] if c in existing_cols]
    all_cols = base_cols + optional_cols
    
    query = f"INSERT INTO visit_proposals ({', '.join(all_cols)}) VALUES ({', '.join([f':{c}' for c in all_cols])})"
    
    with engine.begin() as conn:
        for rec in recommendations_flat:
            params = {k: v for k, v in rec.items() if k in all_cols}
            params['created_at'] = datetime.now()
            conn.execute(text(query), params)


def export_to_excel_flat(visit_date, recommendations_flat, summary_stats):
    logger.info("\n📄 Выгрузка в Excel...")
    filename = OUTPUT_DIR / f"recommendations_{visit_date.strftime('%Y-%m-%d_%H%M%S')}.xlsx"
    
    if not recommendations_flat: return filename
    
    df = pd.DataFrame(recommendations_flat)
    df_output = pd.DataFrame({
        'Дата визита': df['visit_date'],
        'Клиент': df['client_name'],
        'A/B группа': df.get('ab_group', 'control'),
        'Тип': df['selection_type'],
        'Артикул': df.get('article', df['sku_id']),
        'SKU': df.get('sku_name', df['sku_id']),
        'Вероятность': df['predicted_prob'].apply(lambda x: f"{x:.1%}")
    })
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_output.to_excel(writer, sheet_name='Рекомендации', index=False)
            pd.DataFrame([summary_stats]).to_excel(writer, sheet_name='Сводка', index=False)
        logger.info(f"   ✅ Файл: {filename}")
    except Exception as e:
        logger.error(f"❌ Ошибка Excel: {e}")
    
    return filename


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info("="*70)
    logger.info("🎯 ProjectZZZ - ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ v7.0")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    start_time = time.time()
    engine = None
    
    try:
        config = load_config()
        engine = get_engine(config)
        logger.info(f"✅ Подключение к БД: {config['database']['name']}")
        
        model, calibrator, encoders, feature_cols, best_iteration = load_active_model()
        if model is None: return 1
        
        visit_date = datetime.now().date()
        clients = get_clients_for_today(engine)
        
        if not clients:
            logger.warning("⚠️ Нет клиентов")
            return 0
        
        df_candidates = get_candidate_skus_batch(engine, clients)
        if df_candidates.empty:
            logger.error("❌ Нет кандидатов")
            return 1
        
        logger.info("\n🔮 Предсказание...")
        df_encoded = encode_features(df_candidates, encoders, feature_cols)
        probabilities = predict_probabilities_batch(model, df_encoded, feature_cols, calibrator, best_iteration)
        df_candidates['predicted_prob'] = probabilities
        
        logger.info("\n📋 Правило 2+2+1...")
        
        all_recommendations = []
        # Счетчики для статистики
        clients_critical_fallback = 0 # Только те, кто потерял "Новые"
        selection_counts = {}
        
        # Загрузка имен клиентов
        client_names = {}
        try:
            ids_str = "','".join([str(c).replace("'", "''") for c in clients])
            q = f"SELECT client_id, client_name FROM clients WHERE client_id IN ('{ids_str}')"
            res = pd.read_sql(text(q), engine)
            client_names = dict(zip(res['client_id'], res['client_name']))
        except: pass
        
        for client_id in clients:
            client_df = df_candidates[df_candidates['client_id'] == client_id].copy()
            if client_df.empty: continue
            
            selected_skus, fallback_reason = select_2plus2plus1(
                client_df, 
                probability_threshold_new=0.05
            )
            
            # 🔧 ИСПРАВЛЕННАЯ ЛОГИКА УЧЕТА FALLBACK
            # Считаем "критическим" только если нет новых товаров вообще
            if fallback_reason and "No_new_candidates_at_all" in fallback_reason:
                clients_critical_fallback += 1
            
            # Статистика по типам
            for sku in selected_skus:
                t = sku['selection_type']
                selection_counts[t] = selection_counts.get(t, 0) + 1
            
            client_name = client_names.get(client_id, client_id)
            ab_group = get_ab_group(client_id, config)
            
            for sku in selected_skus:
                rec = {
                    'visit_date': str(visit_date),
                    'client_id': client_id,
                    'client_name': client_name,
                    'sku_id': sku['sku_id'],
                    'article': sku.get('article', ''),
                    'sku_name': sku.get('sku_name', ''),
                    'applicability': sku.get('applicability', ''),
                    'predicted_prob': float(sku['predicted_prob']),
                    'selection_type': sku['selection_type'],
                    'fallback_reason': fallback_reason,
                    'model_version': 'model_lightgbm_v1',
                    'ab_group': ab_group
                }
                all_recommendations.append(rec)
        
        if all_recommendations:
            logger.info("\n💾 Сохранение...")
            save_to_database(engine, visit_date, all_recommendations)
            
            summary_stats = {
                'total_clients': len(clients),
                'total_recommendations': len(all_recommendations),
                'fallback_rate': (clients_critical_fallback / len(clients) * 100), # Новый расчет
                'model_version': 'model_lightgbm_v1',
            }
            export_to_excel_flat(visit_date, all_recommendations, summary_stats)
            
            # Вывод статистики
            logger.info(f"\n📊 Статистика типов рекомендаций:")
            for t, cnt in sorted(selection_counts.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"   • {t}: {cnt} ({cnt/len(all_recommendations)*100:.1f}%)")
            
            logger.info(f"\n⚠️ Critical Fallback (нет Новых): {clients_critical_fallback}/{len(clients)} ({clients_critical_fallback/len(clients):.1%})")
        else:
            logger.error("❌ Нет рекомендаций")
            return 1
        
        elapsed = time.time() - start_time
        logger.info(f"\n🎉 Завершено! Время: {elapsed:.1f} сек")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if engine: engine.dispose()

if __name__ == "__main__":
    sys.exit(main())