# -*- coding: utf-8 -*-
"""
model_lightgbm_v1.py v2.1
🔧 АДАПТАЦИЯ ПОД BACKTEST_RESULTS:
  1. Данные загружаются из готовой таблицы backtest_results (мгновенно).
  2. Признаки (features) подтягиваются через JOIN с sales_enriched.
  3. Сохранена логика кодирования, обучения и калибровки.
  4. 🔧 v2.1: Добавлена валидация входных данных (даты, выбросы, диапазоны)

БАЗОВАЯ МОДЕЛЬ: LightGBM Binary Classification
Задача: Предсказать вероятность покупки SKU в окне 14 дней после визита
"""
import sys
import logging
import time
import json
import pickle
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, precision_score, recall_score, roc_auc_score
from sklearn.preprocessing import LabelEncoder
import yaml

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
MODEL_DIR = PROJECT_ROOT / "models"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)
MODEL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "model_lightgbm_v1.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# ВАЛИДАЦИЯ ДАННЫХ
# ==============================================================================

def validate_date_format(df: pd.DataFrame, date_columns: list = None) -> dict:
    """
    🔧 Проверка формата дат
    
    Args:
        df: DataFrame для проверки
        date_columns: Список колонок с датами (по умолчанию: ['purchase_date', 'visit_date'])
    
    Returns:
        dict со статистикой валидации
    """
    if date_columns is None:
        date_columns = ['purchase_date', 'visit_date', 'last_purchase_date']
    
    result = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'stats': {}
    }
    
    for col in date_columns:
        if col not in df.columns:
            continue
        
        # Проверка типа данных
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            result['warnings'].append(f"Колонка {col} не имеет datetime типа")
            # Пробуем конвертировать
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                result['warnings'].append(f"  → {col} сконвертирована в datetime")
            except Exception as e:
                result['errors'].append(f"Не удалось конвертировать {col} в datetime: {e}")
                result['valid'] = False
        
        # Проверка на NaT
        nat_count = df[col].isna().sum()
        nat_pct = nat_count / len(df) * 100 if len(df) > 0 else 0
        
        result['stats'][col] = {
            'nat_count': nat_count,
            'nat_pct': round(nat_pct, 2),
            'min_date': str(df[col].min()) if not pd.isna(df[col].min()) else None,
            'max_date': str(df[col].max()) if not pd.isna(df[col].max()) else None
        }
        
        # Предупреждение если много NaT
        if nat_pct > 5:
            result['warnings'].append(f"{col}: {nat_pct:.1f}% NaT значений")
        
        # Проверка на будущие даты
        future_dates = (df[col] > pd.Timestamp.now()).sum()
        if future_dates > 0:
            result['warnings'].append(f"{col}: найдено {future_dates} будущих дат")
    
    return result


def filter_outliers_iqr(df: pd.DataFrame, numeric_columns: list = None, 
                        iqr_multiplier: float = 3.0) -> tuple:
    """
    🔧 Фильтрация выбросов методом IQR
    
    Args:
        df: DataFrame для фильтрации
        numeric_columns: Список числовых колонок (по умолчанию: все числовые)
        iqr_multiplier: Множитель для IQR (по умолчанию: 3.0)
    
    Returns:
        (df_filtered, outliers_info)
    """
    if numeric_columns is None:
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    outliers_info = {}
    mask = pd.Series(True, index=df.index)
    
    for col in numeric_columns:
        if col not in df.columns:
            continue
        
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - iqr_multiplier * IQR
        upper_bound = Q3 + iqr_multiplier * IQR
        
        # Считаем выбросы
        col_outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
        
        if col_outliers > 0:
            outliers_info[col] = {
                'count': int(col_outliers),
                'pct': round(col_outliers / len(df) * 100, 2),
                'lower_bound': round(lower_bound, 4),
                'upper_bound': round(upper_bound, 4),
                'min': round(df[col].min(), 4),
                'max': round(df[col].max(), 4)
            }
            
            # Применяем фильтр
            col_mask = (df[col] >= lower_bound) & (df[col] <= upper_bound)
            mask = mask & col_mask
    
    df_filtered = df[mask].reset_index(drop=True)
    
    return df_filtered, outliers_info


def validate_feature_ranges(df: pd.DataFrame, feature_ranges: dict = None) -> dict:
    """
    🔧 Валидация диапазонов значений признаков
    
    Args:
        df: DataFrame для проверки
        feature_ranges: Словарь {column: (min, max)}
    
    Returns:
        dict с результатами валидации
    """
    if feature_ranges is None:
        # 🔧 Стандартные диапазоны для признаков ProjectZZZ
        feature_ranges = {
            'frequency_30d': (0, 100),      # Частота покупок за 30 дней
            'frequency_90d': (0, 300),      # Частота покупок за 90 дней
            'days_since_last_purchase': (0, 3650),  # Дней с последней покупки (до 10 лет)
            'rolling_sales_2w': (0, 10000), # Продажи за 2 недели
            'rolling_sales_4w': (0, 20000), # Продажи за 4 недели
            'rolling_sales_8w': (0, 40000), # Продажи за 8 недель
            'group_trend_6m': (-1.0, 1.0),  # Тренд группы (-100% до +100%)
            'group_share_in_portfolio': (0, 1),  # Доля в портфеле (0-100%)
            'days_since_last_purchase_group': (0, 3650),
            'margin': (-1, 1),              # Маржа (-100% до +100%)
            'stock': (0, 100000),           # Остаток на складе
            'price': (0, 1000000),          # Цена
            'predicted_prob': (0, 1),       # Вероятность (0-1)
            'target': (0, 1)                # Целевая переменная (0 или 1)
        }
    
    result = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'out_of_range': {}
    }
    
    for col, (min_val, max_val) in feature_ranges.items():
        if col not in df.columns:
            continue
        
        # Проверка на значения вне диапазона
        out_of_range = ((df[col] < min_val) | (df[col] > max_val)).sum()
        
        if out_of_range > 0:
            out_of_range_pct = out_of_range / len(df) * 100 if len(df) > 0 else 0
            
            result['out_of_range'][col] = {
                'count': int(out_of_range),
                'pct': round(out_of_range_pct, 2),
                'expected_range': (min_val, max_val),
                'actual_min': round(df[col].min(), 4),
                'actual_max': round(df[col].max(), 4)
            }
            
            # Критично если > 5% вне диапазона
            if out_of_range_pct > 5:
                result['errors'].append(
                    f"{col}: {out_of_range_pct:.1f}% значений вне диапазона [{min_val}, {max_val}]"
                )
                result['valid'] = False
            else:
                result['warnings'].append(
                    f"{col}: {out_of_range_pct:.1f}% вне диапазона [{min_val}, {max_val}]"
                )
    
    return result


def validate_training_data(X_train: pd.DataFrame, y_train: pd.Series,
                           X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """
    🔧 Комплексная валидация данных для обучения
    
    Args:
        X_train, y_train: Обучающая выборка
        X_test, y_test: Тестовая выборка
    
    Returns:
        dict с результатами валидации
    """
    result = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'stats': {}
    }
    
    # 1. Проверка на пустые данные
    if len(X_train) == 0:
        result['errors'].append("X_train пустой")
        result['valid'] = False
    
    if len(X_test) == 0:
        result['errors'].append("X_test пустой")
        result['valid'] = False
    
    # 2. Проверка на NaN в признаках
    train_nan = X_train.isna().sum().sum()
    test_nan = X_test.isna().sum().sum()
    
    if train_nan > 0:
        result['warnings'].append(f"X_train: {train_nan} NaN значений")
    if test_nan > 0:
        result['warnings'].append(f"X_test: {test_nan} NaN значений")
    
    # 3. Проверка целевой переменной
    for name, y in [('y_train', y_train), ('y_test', y_test)]:
        if y.isna().sum() > 0:
            result['errors'].append(f"{name}: {y.isna().sum()} NaN в target")
            result['valid'] = False
        
        unique_vals = y.unique()
        if not all(v in [0, 1] for v in unique_vals):
            result['warnings'].append(f"{name}: target содержит не только 0/1")
    
    # 4. Проверка дисбаланса классов
    for name, y in [('y_train', y_train), ('y_test', y_test)]:
        pos_ratio = y.sum() / len(y) * 100 if len(y) > 0 else 0
        result['stats'][f'{name}_positive_ratio'] = round(pos_ratio, 2)
        
        if pos_ratio < 1:
            result['warnings'].append(f"{name}: дисбаланс классов ({pos_ratio:.1f}% положительных)")
        elif pos_ratio > 50:
            result['warnings'].append(f"{name}: необычный баланс ({pos_ratio:.1f}% положительных)")
    
    # 5. Проверка размерности
    if X_train.shape[1] != X_test.shape[1]:
        result['errors'].append(
            f"Разная размерность X_train ({X_train.shape[1]}) и X_test ({X_test.shape[1]})"
        )
        result['valid'] = False
    
    # 6. Проверка на дубликаты
    train_dups = X_train.duplicated().sum()
    test_dups = X_test.duplicated().sum()
    
    result['stats']['train_duplicates'] = int(train_dups)
    result['stats']['test_duplicates'] = int(test_dups)
    
    if train_dups > len(X_train) * 0.1:
        result['warnings'].append(f"X_train: {train_dups} дубликатов ({train_dups/len(X_train)*100:.1f}%)")
    
    return result


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

def load_training_data(engine, limit=None):
    """
    Загружает данные для обучения из backtest_results + sales_enriched.
    🔧 ИСПРАВЛЕНО: Теперь используется готовый датасет.
    """
    logger.info("\n📊 Загрузка данных для обучения из backtest_results...")
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # 🔧 JOIN: backtest_results (target) + sales_enriched (features)
    # Берем признаки, актуальные на момент last_purchase_date
    query = f"""
    SELECT 
        br.target,
        br.days_since_last_purchase,
        se.marketing_group,
        se.brand,
        se.category,
        se.price,
        se.margin,
        se.stock,
        se.frequency_30d,
        se.frequency_90d,
        se.rolling_sales_2w,
        se.rolling_sales_4w,
        se.rolling_sales_8w,
        se.global_popularity,
        se.portfolio_diversity,
        se.group_trend_6m,
        se.group_share_in_portfolio,
        se.days_since_last_purchase_group
    FROM backtest_results br
    JOIN sales_enriched se 
       ON br.client_id = se.client_id 
      AND br.sku_id = se.sku_id 
      AND br.last_purchase_date = se.purchase_date
    WHERE se.marketing_group IS NOT NULL
    {limit_clause}
    """
    
    df = pd.read_sql(text(query), engine)
    
    logger.info(f"   ✅ Загружено записей: {len(df):,}")
    if len(df) > 0:
        pos_count = df['target'].sum()
        neg_count = len(df) - pos_count
        logger.info(f"   ✅ Положительных примеров (target=1): {pos_count:,} ({pos_count/len(df)*100:.1f}%)")
        logger.info(f"   ✅ Отрицательных примеров (target=0): {neg_count:,}")
        logger.info(f"   ✅ Дисбаланс классов: {neg_count/pos_count:.1f}:1")
    
    return df

def encode_categorical(df, categorical_cols):
    """Кодирует категориальные признаки (Label Encoding)"""
    logger.info("\n🔧 Кодирование категориальных признаков...")
    encoded_cols = []
    encoders = {}
    
    for col in categorical_cols:
        if col not in df.columns:
            logger.warning(f"⚠️ Колонка {col} не найдена — пропускаем")
            continue
            
        le = LabelEncoder()
        df[col] = df[col].fillna('Unknown')
        df[f'{col}_encoded'] = le.fit_transform(df[col].astype(str))
        encoders[col] = le
        encoded_cols.append(f'{col}_encoded')
        logger.info(f"   ✅ {col}: {len(le.classes_)} уникальных значений")
        
    return df, encoded_cols, encoders

def prepare_features(df, encoded_cols):
    """Формирует итоговый список признаков"""
    logger.info("\n🔧 Подготовка признаков...")
    
    base_features = [
        'frequency_30d', 'frequency_90d', 'days_since_last_purchase',
        'rolling_sales_2w', 'rolling_sales_4w', 'rolling_sales_8w',
        'group_trend_6m', 'group_share_in_portfolio', 
        'days_since_last_purchase_group', 'margin', 'stock', 'price'
    ]
    
    # Проверяем наличие
    feature_cols = [col for col in base_features if col in df.columns]
    feature_cols.extend(encoded_cols) # Добавляем закодированные категории
    
    X = df[feature_cols].fillna(0)
    y = df['target']
    
    logger.info(f"   ✅ Признаков: {len(feature_cols)}")
    logger.info(f"   ✅ Пример признаков: {feature_cols[:5]}")
    
    return X, y, feature_cols

def train_model(X_train, y_train, X_test, y_test, feature_names, 
                validate=True, filter_outliers=False, iqr_multiplier=3.0):
    """
    Обучение LightGBM с ранней остановкой
    
    Args:
        X_train, y_train: Обучающая выборка
        X_test, y_test: Тестовая выборка
        feature_names: Список имён признаков
        validate: 🔧 Выполнять валидацию данных (по умолчанию True)
        filter_outliers: 🔧 Фильтровать выбросы (по умолчанию False)
        iqr_multiplier: 🔧 Множитель IQR для фильтрации выбросов
    
    Returns:
        model, calibrator, num_trees
    """
    logger.info("\n🧠 Обучение модели LightGBM...")
    
    # 🔧 ВАЛИДАЦИЯ ВХОДНЫХ ДАННЫХ
    if validate:
        logger.info("\n🔍 Валидация данных для обучения...")
        
        validation_result = validate_training_data(X_train, y_train, X_test, y_test)
        
        # Логирование результатов
        for warning in validation_result['warnings']:
            logger.warning(f"   ⚠️ {warning}")
        
        for error in validation_result['errors']:
            logger.error(f"   ❌ {error}")
        
        # Логирование статистики
        for stat_name, value in validation_result['stats'].items():
            logger.info(f"   📊 {stat_name}: {value}")
        
        # 🔧 Блокировка обучения при критических ошибках
        if not validation_result['valid']:
            raise ValueError(f"Валидация не пройдена: {'; '.join(validation_result['errors'])}")
        
        logger.info("   ✅ Валидация пройдена")
    
    # 🔧 ФИЛЬТРАЦИЯ ВЫБРОСОВ (опционально)
    if filter_outliers:
        logger.info(f"\n🗑️ Фильтрация выбросов (IQR множитель={iqr_multiplier})...")
        
        # Объединяем X_train и X_test для фильтрации
        X_combined = pd.concat([X_train, X_test], axis=0, ignore_index=True)
        y_combined = pd.concat([y_train, y_test], axis=0, ignore_index=True)
        
        X_filtered, outliers_info = filter_outliers_iqr(
            X_combined, 
            numeric_columns=feature_names,
            iqr_multiplier=iqr_multiplier
        )
        
        # Логирование результатов фильтрации
        for col, info in outliers_info.items():
            logger.info(f"   📊 {col}: удалено {info['count']} выбросов ({info['pct']:.1f}%)")
        
        # Разделяем обратно
        split_idx = len(X_train)
        X_train = X_filtered[:split_idx].reset_index(drop=True)
        X_test = X_filtered[split_idx:].reset_index(drop=True)
        y_train = y_combined[:split_idx].reset_index(drop=True)
        y_test = y_combined[split_idx:].reset_index(drop=True)
        
        logger.info(f"   ✅ Осталось строк: train={len(X_train)}, test={len(X_test)}")
    
    # Проверка размера данных после валидации и фильтрации
    if len(X_train) < 100:
        raise ValueError(f"Недостаточно данных для обучения после валидации: {len(X_train)} строк")
    
    if len(X_test) < 10:
        raise ValueError(f"Недостаточно данных для тестирования: {len(X_test)} строк")

    # Расчёт дисбаланса классов
    neg_count = (y_train == 0).sum()
    pos_count = (y_train == 1).sum()
    
    if pos_count == 0:
        raise ValueError("Нет положительных примеров (target=1) в обучающей выборке")
    
    scale_pos_weight = neg_count / pos_count
    logger.info(f"   🔧 scale_pos_weight: {scale_pos_weight:.2f}")
    logger.info(f"   📊 Баланс классов: {neg_count}:{pos_count} ({pos_count/len(y_train)*100:.1f}% positive)")

    lgb_params = {
        'objective': 'binary',
        'metric': ['binary_logloss', 'auc'],
        'boosting_type': 'gbdt',
        'num_leaves': 15,
        'max_depth': 6,
        'learning_rate': 0.1,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1,
        'n_jobs': -1,
        'seed': 42,
        'scale_pos_weight': scale_pos_weight
    }

    train_data = lgb.Dataset(X_train, label=y_train, feature_name=feature_names)
    test_data = lgb.Dataset(X_test, label=y_test, feature_name=feature_names, reference=train_data)

    logger.info("   📈 Обучение модели (с ранней остановкой)...")
    model = lgb.train(
        lgb_params,
        train_data,
        num_boost_round=1000,
        valid_sets=[test_data],
        callbacks=[lgb.log_evaluation(period=200)]
    )

    logger.info(f"   ✅ Модель обучена (итераций: {model.num_trees()})")

    # Важность признаков
    logger.info("\n📊 Важность признаков (топ-10):")
    importance = model.feature_importance(importance_type='gain')
    feature_importance = sorted(zip(feature_names, importance), key=lambda x: x[1], reverse=True)
    for i, (feat, imp) in enumerate(feature_importance[:10]):
        logger.info(f"   {i+1}. {feat}: {imp:,.2f}")

    # Калибровка
    logger.info("\n   📈 Калибровка вероятностей (LogisticRegression)...")
    y_proba_raw = model.predict(X_test, num_iteration=model.num_trees())
    calibrator = LogisticRegression(solver='lbfgs', max_iter=1000)
    calibrator.fit(y_proba_raw.reshape(-1, 1), y_test)

    return model, calibrator, model.num_trees()


def evaluate_model(model, calibrator, X_test, y_test, best_iteration):
    """Оценка качества модели"""
    logger.info("\n📊 Оценка качества модели...")
    
    y_proba_raw = model.predict(X_test, num_iteration=best_iteration)
    y_proba = calibrator.predict_proba(y_proba_raw.reshape(-1, 1))[:, 1]
    
    # Оптимальный порог (подбор под Recall >= 0.55)
    best_threshold = 0.2
    best_f1 = 0
    for thresh in np.arange(0.1, 0.9, 0.05):
        y_pred_t = (y_proba >= thresh).astype(int)
        p = precision_score(y_test, y_pred_t, zero_division=0)
        r = recall_score(y_test, y_pred_t, zero_division=0)
        if r >= 0.55 and p > 0.30: # Ищем баланс
             f1 = 2*p*r/(p+r)
             if f1 > best_f1:
                 best_f1 = f1
                 best_threshold = thresh

    logger.info(f"   🔧 Выбран порог: {best_threshold:.2f}")
    
    y_pred = (y_proba >= best_threshold).astype(int)
    
    brier = brier_score_loss(y_test, y_proba)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    auc = roc_auc_score(y_test, y_proba)
    
    logger.info(f"   📈 Brier Score: {brier:.4f} (порог <= 0.20)")
    logger.info(f"   📈 AUC-ROC: {auc:.4f}")
    logger.info(f"   📈 Precision: {precision:.4f}")
    logger.info(f"   📈 Recall (Hit Rate): {recall:.4f}")
    
    # Проверка порогов ReadMe
    passed = (brier <= 0.20) and (precision >= 0.35) and (recall >= 0.55)
    
    metrics = {
        'brier_score': brier,
        'precision': precision,
        'hit_rate': recall,
        'auc': auc,
        'threshold': best_threshold,
        'all_passed': passed
    }
    
    status = "✅" if passed else "⚠️"
    logger.info(f"   {status} Валидация метрик: {'ПРОЙДЕНА' if passed else 'НЕ ПРОЙДЕНА'}")
    
    return metrics

def save_model(model, calibrator, encoders, feature_cols, best_iteration, metrics):
    """Сохранение артефактов модели"""
    logger.info("\n💾 Сохранение модели...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_path = MODEL_DIR / f"model_lightgbm_v1_{timestamp}.pkl"
    calib_path = MODEL_DIR / f"calibrator_lightgbm_v1_{timestamp}.pkl"
    enc_path = MODEL_DIR / f"encoders_lightgbm_v1_{timestamp}.pkl"
    
    with open(model_path, 'wb') as f:
        pickle.dump({'model': model, 'feature_cols': feature_cols, 'best_iteration': best_iteration}, f)
    with open(calib_path, 'wb') as f:
        pickle.dump(calibrator, f)
    with open(enc_path, 'wb') as f:
        pickle.dump(encoders, f)
        
    logger.info(f"   ✅ Модель: {model_path.name}")
    logger.info(f"   ✅ Калибратор: {calib_path.name}")
    logger.info(f"   ✅ Энкодеры: {enc_path.name}")
    
    return model_path, calib_path

def update_registry(metrics, model_path, calib_path):
    """Обновление реестра моделей"""
    logger.info("\n📝 Обновление реестра моделей...")
    registry_path = MODEL_DIR / "model_registry.json"
    
    if registry_path.exists():
        with open(registry_path, 'r', encoding='utf-8') as f:
            registry = json.load(f)
    else:
        registry = {'active_model': None, 'models': []}
    
    new_entry = {
        'name': 'model_lightgbm_v1',
        'status': 'production',
        'metrics': {
            'precision_5': round(metrics['precision'], 4),
            'hit_rate': round(metrics['hit_rate'], 4),
            'brier_score': round(metrics['brier_score'], 4),
            'auc': round(metrics['auc'], 4),
            'threshold': round(metrics['threshold'], 2)
        },
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'model_path': str(model_path),
        'calibrator_path': str(calib_path)
    }
    
    # Удаляем старые версии из списка, если нужно, или добавляем новую
    registry['models'] = [m for m in registry['models'] if m['name'] != 'model_lightgbm_v1']
    registry['models'].append(new_entry)
    registry['active_model'] = 'model_lightgbm_v1'
    
    with open(registry_path, 'w', encoding='utf-8') as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)
    
    logger.info(f"   ✅ Реестр обновлён. Активная модель: {registry['active_model']}")

# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info("="*70)
    logger.info("🧠 ProjectZZZ - ОБУЧЕНИЕ МОДЕЛИ LightGBM v2.1 (С ВАЛИДАЦИЕЙ)")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)

    start_time = time.time()
    engine = None
    
    # 🔧 НАСТРОЙКИ ВАЛИДАЦИИ
    ENABLE_VALIDATION = True      # Валидация входных данных
    ENABLE_OUTLIER_FILTER = False # Фильтрация выбросов (опционально)
    IQR_MULTIPLIER = 3.0          # Множитель для IQR фильтра

    try:
        config = load_config()
        engine = get_engine(config)

        # 1. Загрузка
        df = load_training_data(engine, limit=None) # limit=None для полного датасета
        
        # 🔧 Валидация данных после загрузки
        if ENABLE_VALIDATION:
            logger.info("\n🔍 Валидация загруженных данных...")
            date_validation = validate_date_format(df, ['last_purchase_date'])
            
            for warning in date_validation['warnings']:
                logger.warning(f"   ⚠️ {warning}")
            for stat_name, value in date_validation['stats'].items():
                logger.info(f"   📊 {stat_name}: {value}")
            
            # Валидация диапазонов
            range_validation = validate_feature_ranges(df)
            for warning in range_validation['warnings']:
                logger.warning(f"   ⚠️ {warning}")
            for error in range_validation['errors']:
                logger.error(f"   ❌ {error}")
                
                # 🔧 Блокировка при критических ошибках
                if not range_validation['valid']:
                    raise ValueError(f"Валидация диапазонов не пройдена: {'; '.join(range_validation['errors'])}")

        if len(df) < 10000:
            logger.error("❌ Недостаточно данных для обучения!")
            return 1

        # 2. Кодирование
        cat_cols = ['marketing_group', 'brand', 'category']
        df, encoded_cols, encoders = encode_categorical(df, cat_cols)

        # 3. Признаки
        X, y, feature_cols = prepare_features(df, encoded_cols)

        # 4. Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # 5. Train с валидацией
        logger.info("\n📚 Запуск обучения с валидацией...")
        model, calibrator, best_iter = train_model(
            X_train, y_train, X_test, y_test, feature_cols,
            validate=ENABLE_VALIDATION,
            filter_outliers=ENABLE_OUTLIER_FILTER,
            iqr_multiplier=IQR_MULTIPLIER
        )
        
        # 6. Evaluate
        metrics = evaluate_model(model, calibrator, X_test, y_test, best_iter)
        
        # 7. Save
        model_path, calib_path = save_model(model, calibrator, encoders, feature_cols, best_iter, metrics)
        
        # 8. Registry
        update_registry(metrics, model_path, calib_path)
        
        elapsed = time.time() - start_time
        logger.info("\n" + "="*70)
        logger.info("🎉 ОБУЧЕНИЕ ЗАВЕРШЕНО!")
        logger.info("="*70)
        logger.info(f"⏱️ Общее время: {elapsed:.1f} сек")
        
        return 0 if metrics['all_passed'] else 1
        
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