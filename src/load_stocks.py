# -*- coding: utf-8 -*-
"""
generate_recommendations.py v1.2
🔧 ИСПРАВЛЕНИЯ:
  1. Загрузка энкодеров по шаблону (glob) для файлов с меткой времени
  2. SQLAlchemy 2.0 совместимость (with engine.connect())
  3. Обработка NULL в признаках

ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ SKU ДЛЯ МЕНЕДЖЕРОВ
Согласно ReadMe_ProjectZZZ.txt раздел 4 (Правило 2+2+1)
"""
import sys
import logging
import time
import json
import pickle
from pathlib import Path
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import lightgbm as lgb
import yaml
from datetime import datetime, timedelta

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
    """Загружает конфигурацию из YAML"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_engine(config):
    """Создаёт подключение к PostgreSQL"""
    db = config['database']
    return create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )


def load_active_model():
    """
    Загружает активную модель из реестра
    
    🔧 ИСПРАВЛЕНИЕ: поиск энкодеров по шаблону с меткой времени
    """
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
    
    # 🔍 Ищем модель в реестре
    model_info = None
    for m in registry.get('models', []):
        if m['name'] == active_model_name:
            model_info = m
            break
    
    if not model_info:
        logger.error(f"❌ Модель {active_model_name} не найдена в реестре!")
        return None, None, None, None, None
    
    # 📦 Загружаем файлы модели
    model_path = MODEL_DIR / model_info['model_path'].split('\\')[-1]
    calib_path = MODEL_DIR / model_info.get('calibrator_path', '').split('\\')[-1] if 'calibrator_path' in model_info else None
    
    # 🔧 Загрузка энкодеров (ИСПРАВЛЕНИЕ: поиск по шаблону с меткой времени)
    encoders = None
    encoders_pattern = f"encoders_{active_model_name}_*.pkl"
    encoders_files = list(MODEL_DIR.glob(encoders_pattern))
    
    if encoders_files:
        # Берём самый новый файл энкодеров
        encoders_path = sorted(encoders_files, key=lambda p: p.stat().st_mtime)[-1]
        with open(encoders_path, 'rb') as f:
            encoders = pickle.load(f)
        logger.info(f"   ✅ Энкодеры загружены: {encoders_path.name}")
    else:
        logger.warning("   ⚠️ Энкодеры не найдены — модель может работать некорректно!")
    
    # Загрузка базовой модели
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    
    model = model_data['model']
    feature_cols = model_data['feature_cols']
    best_iteration = model_data.get('best_iteration', 1000)
    
    logger.info(f"   ✅ Модель загружена: {model_path.name}")
    logger.info(f"   ✅ Признаков: {len(feature_cols)}")
    
    # Загрузка калибратора (если есть)
    calibrator = None
    if calib_path and (MODEL_DIR / calib_path).exists():
        with open(MODEL_DIR / calib_path, 'rb') as f:
            calibrator = pickle.load(f)
        logger.info(f"   ✅ Калибратор загружен: {calib_path}")
    
    return model, calibrator, encoders, feature_cols, best_iteration


def get_clients_for_today(engine, visit_date=None):
    """
    Получает список клиентов с визитами на сегодня
    
    🔧 SQLAlchemy 2.0: используем with engine.connect()
    """
    logger.info("\n👥 Получение клиентов на визит...")
    
    if visit_date is None:
        visit_date = datetime.now().date()
    
    query = """
    SELECT DISTINCT client_id
    FROM visits_schedule
    WHERE planned_visit_date = :visit_date
      AND status != 'completed'
    """
    
    try:
        # 🔧 SQLAlchemy 2.0 syntax
        with engine.connect() as conn:
            result = conn.execute(text(query), {"visit_date": str(visit_date)})
            clients = [row[0] for row in result.fetchall()]
        
        if len(clients) > 0:
            logger.info(f"   ✅ Найдено клиентов из расписания: {len(clients)}")
            return clients
    except Exception as e:
        logger.warning(f"⚠️ Таблица visits_schedule пуста или не существует: {e}")
    
    logger.info("   🔄 Fallback: берём активных клиентов за 90 дней...")
    
    query = """
    SELECT DISTINCT client_id
    FROM purchases
    WHERE purchase_date >= CURRENT_DATE - INTERVAL '90 days'
    ORDER BY client_id
    """
    
    # 🔧 SQLAlchemy 2.0 syntax
    with engine.connect() as conn:
        result = conn.execute(text(query))
        clients = [row[0] for row in result.fetchall()]
    
    logger.info(f"   ✅ Найдено активных клиентов: {len(clients)}")
    
    return clients


def get_candidate_skus(engine, client_id, feature_cols):
    """
    Получает кандидатов SKU для клиента + признаки
    
    🔧 SQLAlchemy 2.0: используем pd.read_sql с text()
    🔧 Фильтр: stock >= 1 (остаток должен быть >= 1)
    """
    query = """
    WITH client_history AS (
        SELECT DISTINCT sku_id,
               MAX(purchase_date) AS last_purchase_date
        FROM sales_enriched
        WHERE client_id = :client_id
        GROUP BY sku_id
    ),
    candidates AS (
        SELECT 
            s.sku_id,
            s.brand,
            s.marketing_group1 AS marketing_group,
            s.category,
            s.price,
            s.margin,
            s.stock,
            s.is_new,
            ch.last_purchase_date,
            CASE WHEN ch.sku_id IS NULL THEN 1 ELSE 0 END AS is_new_for_client,
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
        FROM skus s
        LEFT JOIN client_history ch ON s.sku_id = ch.sku_id
        LEFT JOIN sales_enriched se ON s.sku_id = se.sku_id 
            AND se.client_id = :client_id
            AND se.purchase_date = (
                SELECT MAX(purchase_date) 
                FROM sales_enriched 
                WHERE client_id = :client_id
            )
        WHERE s.stock >= 1
        ORDER BY s.sku_id
    )
    SELECT * FROM candidates
    """
    
    # 🔧 SQLAlchemy 2.0: используем text() для параметров
    df = pd.read_sql(text(query), engine, params={"client_id": client_id})
    
    return df


def encode_features(df, encoders, feature_cols):
    """
    Кодирует категориальные признаки для модели
    
    🔧 Использует те же энкодеры, что и при обучении
    🔧 Если энкодеров нет — возвращает DataFrame как есть (с предупреждением)
    """
    df_encoded = df.copy()
    
    if encoders is None:
        logger.warning("⚠️ Энкодеры не загружены — пропускаем кодирование категорий!")
        return df_encoded
    
    for col, encoder in encoders.items():
        encoded_col = f'{col}_encoded'
        if encoded_col in feature_cols:
            # Заполняем пропуски
            df_encoded[col] = df_encoded[col].fillna('Unknown')
            # Кодируем (неизвестные значения = -1)
            df_encoded[encoded_col] = df_encoded[col].apply(
                lambda x: encoder.transform([x])[0] if x in encoder.classes_ else -1
            )
    
    return df_encoded


def predict_probabilities(model, df_encoded, feature_cols, calibrator=None, best_iteration=1000):
    """
    Предсказывает вероятности покупки для всех кандидатов
    
    🔧 Применяет калибровку если есть
    """
    # 🔍 Фильтруем только нужные признаки (которые есть в DataFrame)
    available_cols = [col for col in feature_cols if col in df_encoded.columns]
    missing_cols = [col for col in feature_cols if col not in df_encoded.columns]
    
    if missing_cols:
        logger.warning(f"⚠️ Отсутствуют признаки: {missing_cols} — заполняем нулями")
    
    X = df_encoded[available_cols].fillna(0)
    
    # 🔮 Предсказание
    if hasattr(model, 'predict'):
        y_proba_raw = model.predict(X, num_iteration=best_iteration)
    else:
        y_proba_raw = model.predict_proba(X)[:, 1]
    
    # 🔧 Калибровка (если есть)
    if calibrator is not None:
        y_proba_calib = calibrator.predict_proba(y_proba_raw.reshape(-1, 1))[:, 1]
    else:
        y_proba_calib = y_proba_raw
    
    return y_proba_calib


def select_2plus2plus1(df, prob_col='predicted_prob', visit_date=None):
    """
    Применяет правило 2+2+1 для отбора 5 SKU
    
    📚 ReadMe раздел 4:
    [1] 2 SKU — Новые (is_new_for_client=1, P > 0.8 приоритет)
    [2] 2 SKU — Развитие (привычные + trend > +0.05)
    [3] 1 SKU — Возврат (привычные + trend < -0.05)
    """
    logger.info("   📋 Применение правила 2+2+1...")
    
    selected = []
    fallback_reasons = []
    
    # 🔹 ШАГ 1: Новые SKU (2 шт)
    new_skus = df[df['is_new_for_client'] == 1].copy()
    if not new_skus.empty:
        new_skus['score'] = new_skus[prob_col] * (1 + (new_skus[prob_col] > 0.8).astype(int) * 0.5)
        new_skus = new_skus.sort_values('score', ascending=False)
        
        new_high_prob = new_skus[new_skus[prob_col] > 0.8].head(2)
        
        if len(new_high_prob) >= 2:
            for _, rec in new_high_prob.head(2).iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'new'
                selected.append(rec_dict)
            fallback_reasons.append(None)
        elif len(new_high_prob) > 0:
            for _, rec in new_high_prob.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'new'
                selected.append(rec_dict)
            
            remaining = 2 - len(new_high_prob)
            new_remaining = new_skus[new_skus[prob_col] <= 0.8].head(remaining)
            for _, rec in new_remaining.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'new'
                selected.append(rec_dict)
            fallback_reasons.append(f"New_low_prob:{len(new_remaining)}")
        else:
            familiar_high_margin = df[df['is_new_for_client'] == 0].sort_values('margin', ascending=False).head(2)
            for _, rec in familiar_high_margin.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'new_fallback'
                selected.append(rec_dict)
            fallback_reasons.append("No_new_candidates")
    else:
        familiar_high_margin = df[df['is_new_for_client'] == 0].sort_values('margin', ascending=False).head(2)
        for _, rec in familiar_high_margin.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'new_fallback'
            selected.append(rec_dict)
        fallback_reasons.append("No_new_candidates")
    
    # 🔹 ШАГ 2: Развитие (2 шт) — привычные + растущие группы
    develop_skus = df[
        (df['is_new_for_client'] == 0) & 
        (df['group_trend_6m'] > 0.05)
    ].copy()
    
    if not develop_skus.empty:
        develop_skus['score'] = develop_skus[prob_col] * (1 + 0.5 * develop_skus['group_trend_6m'])
        develop_skus = develop_skus.sort_values('score', ascending=False)
        
        if len(develop_skus) >= 2:
            for _, rec in develop_skus.head(2).iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'develop'
                selected.append(rec_dict)
            fallback_reasons.append(None)
        elif len(develop_skus) > 0:
            for _, rec in develop_skus.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'develop'
                selected.append(rec_dict)
            
            remaining = 2 - len(develop_skus)
            stable_skus = df[
                (df['is_new_for_client'] == 0) & 
                (df['group_trend_6m'].between(-0.05, 0.05))
            ].sort_values(prob_col, ascending=False).head(remaining)
            for _, rec in stable_skus.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'develop_fallback'
                selected.append(rec_dict)
            fallback_reasons.append(f"Develop_low_candidates:{len(stable_skus)}")
    else:
        stable_skus = df[
            (df['is_new_for_client'] == 0) & 
            (df['group_trend_6m'].between(-0.05, 0.05))
        ].sort_values(prob_col, ascending=False).head(2)
        for _, rec in stable_skus.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'develop_fallback'
            selected.append(rec_dict)
        fallback_reasons.append("No_growing_groups")
    
    # 🔹 ШАГ 3: Возврат (1 шт) — привычные + падающие группы
    retain_skus = df[
        (df['is_new_for_client'] == 0) & 
        (df['group_trend_6m'] < -0.05)
    ].copy()
    
    if not retain_skus.empty:
        retain_skus['score'] = retain_skus[prob_col] * (1 + retain_skus['days_since_last_purchase_group'] / 365)
        retain_skus = retain_skus.sort_values('score', ascending=False)
        rec_dict = retain_skus.iloc[0].to_dict()
        rec_dict['selection_type'] = 'retain'
        selected.append(rec_dict)
        fallback_reasons.append(None)
    else:
        stable_old = df[
            (df['is_new_for_client'] == 0) & 
            (df['group_trend_6m'].between(-0.05, 0.05))
        ].sort_values('days_since_last_purchase_group', ascending=False).head(1)
        
        if not stable_old.empty:
            rec_dict = stable_old.iloc[0].to_dict()
            rec_dict['selection_type'] = 'retain_fallback'
            selected.append(rec_dict)
            fallback_reasons.append("No_declining_groups")
        else:
            any_familiar = df[df['is_new_for_client'] == 0].sort_values(prob_col, ascending=False).head(1)
            if not any_familiar.empty:
                rec_dict = any_familiar.iloc[0].to_dict()
                rec_dict['selection_type'] = 'retain_fallback'
                selected.append(rec_dict)
                fallback_reasons.append("No_retain_candidates")
            else:
                fallback_reasons.append("No_retain_candidates_at_all")
    
    # 🔹 ШАГ 4: Проверка на дубликаты
    sku_ids = [s['sku_id'] for s in selected]
    if len(sku_ids) != len(set(sku_ids)):
        logger.warning("   ⚠️ Обнаружены дубликаты SKU — удаляем...")
        seen = set()
        unique_selected = []
        for s in selected:
            if s['sku_id'] not in seen:
                unique_selected.append(s)
                seen.add(s['sku_id'])
        selected = unique_selected
    
    # 🔹 ШАГ 5: Добор до 5 SKU (если нужно)
    if len(selected) < 5:
        remaining = 5 - len(selected)
        selected_skus = set(s['sku_id'] for s in selected)
        
        remaining_skus = df[~df['sku_id'].isin(selected_skus)].sort_values(prob_col, ascending=False).head(remaining)
        
        for _, rec in remaining_skus.iterrows():
            rec_dict = rec.to_dict()
            rec_dict['selection_type'] = 'filler'
            selected.append(rec_dict)
        
        fallback_reasons.append(f"Filled_with:{len(remaining_skus)}_SKU")
    
    selected = selected[:5]
    
    fallback_str = "; ".join([fr for fr in fallback_reasons if fr]) if fallback_reasons else None
    
    return selected, fallback_str


def save_to_database(engine, visit_date, client_id, client_name, manager_id, selected_skus, fallback_reason, model_version):
    """
    Сохраняет рекомендации в таблицу visit_proposals
    
    🔧 SQLAlchemy 2.0: используем with engine.begin()
    """
    query = """
    INSERT INTO visit_proposals (
        visit_date, client_id, client_name, manager_id,
        sku_id, predicted_prob, selection_type, fallback_reason, model_version, created_at
    ) VALUES (
        :visit_date, :client_id, :client_name, :manager_id,
        :sku_id, :predicted_prob, :selection_type, :fallback_reason, :model_version, CURRENT_TIMESTAMP
    )
    """
    
    # 🔧 SQLAlchemy 2.0: используем with engine.begin() для транзакции
    with engine.begin() as conn:
        for sku in selected_skus:
            conn.execute(text(query), {
                'visit_date': str(visit_date),
                'client_id': client_id,
                'client_name': client_name,
                'manager_id': manager_id,
                'sku_id': sku['sku_id'],
                'predicted_prob': float(sku['predicted_prob']),
                'selection_type': sku['selection_type'],
                'fallback_reason': fallback_reason,
                'model_version': model_version
            })


def export_to_excel(visit_date, recommendations_df, summary_stats):
    """Выгружает рекомендации в Excel (БЕЗ ЗАЩИТЫ)"""
    logger.info("\n📄 Выгрузка в Excel...")
    
    timestamp = visit_date.strftime('%Y-%m-%d')
    filename = OUTPUT_DIR / f"recommendations_{timestamp}.xlsx"
    
    pivot_df = recommendations_df.pivot_table(
        index=['visit_date', 'client_id', 'client_name', 'manager_id', 'fallback_reason', 'model_version'],
        columns='sku_order',
        values=['sku_id', 'selection_type', 'marketing_group', 'predicted_prob', 'margin'],
        aggfunc='first'
    ).reset_index()
    
    pivot_df.columns = ['_'.join(str(col)).strip('_') for col in pivot_df.columns.values]
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            pivot_df.to_excel(writer, sheet_name='Recommendations', index=False)
            
            summary_df = pd.DataFrame([summary_stats])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            model_info_df = pd.DataFrame([{
                'Parameter': ['Model Version', 'Generation Date', 'Total Clients', 'Avg Probability', 'Fallback Rate'],
                'Value': [
                    summary_stats['model_version'],
                    timestamp,
                    summary_stats['total_clients'],
                    f"{summary_stats['avg_probability']:.2%}",
                    f"{summary_stats['fallback_rate']:.1f}%"
                ]
            }])
            model_info_df.to_excel(writer, sheet_name='Model Info', index=False)
        
        logger.info(f"   ✅ Файл сохранён: {filename}")
        logger.info(f"   📊 Размер файла: {filename.stat().st_size / 1024 / 1024:.1f} МБ")
    except Exception as e:
        logger.error(f"❌ Ошибка записи Excel: {e}")
        logger.warning("⚠️ Пробуем без листов Summary и Model Info...")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            pivot_df.to_excel(writer, sheet_name='Recommendations', index=False)
        
        logger.info(f"   ✅ Файл сохранён (упрощённый): {filename}")
    
    return filename


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info("="*70)
    logger.info("🎯 ProjectZZZ - ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ v1.2 (ИСПРАВЛЕННАЯ)")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    start_time = time.time()
    engine = None
    
    try:
        config = load_config()
        engine = get_engine(config)
        logger.info(f"✅ Подключение к БД: {config['database']['name']}")
        
        model, calibrator, encoders, feature_cols, best_iteration = load_active_model()
        
        if model is None:
            logger.error("❌ Не удалось загрузить модель — генерация отменена!")
            return 1
        
        visit_date = datetime.now().date()
        clients = get_clients_for_today(engine, visit_date)
        
        if len(clients) == 0:
            logger.warning("⚠️ Нет клиентов на визит сегодня — генерация отменена!")
            return 0
        
        logger.info(f"\n🎯 Генерация рекомендаций для {len(clients)} клиентов...")
        
        all_recommendations = []
        clients_with_fallback = 0
        total_probability = 0
        
        for i, client_id in enumerate(clients, 1):
            if i % 50 == 0:
                logger.info(f"   📈 Обработано клиентов: {i}/{len(clients)}")
            
            df_candidates = get_candidate_skus(engine, client_id, feature_cols)
            
            if len(df_candidates) == 0:
                logger.warning(f"   ⚠️ Клиент {client_id}: нет кандидатов с остатком >= 1")
                continue
            
            df_encoded = encode_features(df_candidates, encoders, feature_cols)
            
            probabilities = predict_probabilities(model, df_encoded, feature_cols, calibrator, best_iteration)
            df_candidates['predicted_prob'] = probabilities
            
            selected_skus, fallback_reason = select_2plus2plus1(df_candidates, 'predicted_prob', visit_date)
            
            if len(selected_skus) < 5:
                logger.warning(f"   ⚠️ Клиент {client_id}: не удалось набрать 5 SKU (только {len(selected_skus)})")
            
            if fallback_reason:
                clients_with_fallback += 1
            
            client_info_query = """
            SELECT client_name, manager_id
            FROM clients
            WHERE client_id = :client_id
            """
            with engine.connect() as conn:
                result = conn.execute(text(client_info_query), {"client_id": client_id})
                row = result.fetchone()
                client_name = row[0] if row else client_id
                manager_id = row[1] if row and row[1] else 'UNKNOWN'
            
            model_version = 'model_lightgbm_v1'
            save_to_database(engine, visit_date, client_id, client_name, manager_id, selected_skus, fallback_reason, model_version)
            
            for idx, sku in enumerate(selected_skus, 1):
                sku['client_id'] = client_id
                sku['client_name'] = client_name
                sku['manager_id'] = manager_id
                sku['visit_date'] = visit_date
                sku['sku_order'] = idx
                sku['fallback_reason'] = fallback_reason
                sku['model_version'] = model_version
                all_recommendations.append(sku)
                total_probability += sku['predicted_prob']
        
        if len(all_recommendations) > 0:
            recommendations_df = pd.DataFrame(all_recommendations)
            
            summary_stats = {
                'total_clients': len(clients),
                'total_recommendations': len(all_recommendations),
                'avg_probability': total_probability / len(all_recommendations) if all_recommendations else 0,
                'fallback_rate': (clients_with_fallback / len(clients) * 100) if clients else 0,
                'model_version': 'model_lightgbm_v1'
            }
            
            export_to_excel(visit_date, recommendations_df, summary_stats)
        else:
            logger.error("❌ Не удалось сгенерировать ни одной рекомендации!")
            return 1
        
        elapsed = time.time() - start_time
        
        logger.info("\n" + "="*70)
        logger.info("🎉 ГЕНЕРАЦИЯ РЕКОМЕНДАЦИЙ ЗАВЕРШЕНА!")
        logger.info("="*70)
        logger.info(f"✅ Клиентов обработано: {len(clients)}")
        logger.info(f"✅ Рекомендаций сгенерировано: {len(all_recommendations)}")
        if all_recommendations:
            logger.info(f"✅ Средняя вероятность: {total_probability / len(all_recommendations):.2%}")
        if clients:
            logger.info(f"✅ Fallback клиентов: {clients_with_fallback} ({clients_with_fallback / len(clients) * 100:.1f}%)")
        logger.info(f"⏱️ Общее время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
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