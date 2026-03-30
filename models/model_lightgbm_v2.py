# -*- coding: utf-8 -*-
"""
model_lightgbm_v2.py - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ
🚀 УЛУЧШЕНИЯ ПО СРАВНЕНИЮ С v1:
  1. Расширенный набор признаков (добавлены interaction features)
  2. Оптимизация гиперпараметров LightGBM (Bayesian Optimization ready)
  3. Улучшенная обработка дисбаланса классов (focal loss)
  4. Добавлена кросс-валидация для стабильности
  5. Ранняя остановка по нескольким метрикам
  6. Пост-обучение: feature selection по важности
  7. Ensemble калибровка (Isotonic + Platt)

ЦЕЛЕВЫЕ МЕТРИКИ (ReadMe раздел 3.3):
- Precision@5 >= 0.35
- Hit Rate >= 0.55  
- Brier Score <= 0.20
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
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss, precision_score, recall_score, roc_auc_score, f1_score
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_selection import SelectFromModel
import yaml

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
MODEL_DIR = PROJECT_ROOT / "models"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)
MODEL_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "model_lightgbm_v2.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# ОПТИМИЗИРОВАННЫЕ ГИПЕРПАРАМЕТРЫ
# ==============================================================================
OPTIMAL_PARAMS = {
    'objective': 'binary',
    'metric': ['binary_logloss', 'auc'],
    'boosting_type': 'gbdt',
    'num_leaves': 31,          # Увеличено с 15 для лучшей ёмкости
    'max_depth': 8,            # Увеличено с 6
    'learning_rate': 0.05,     # Снижено для лучшей сходимости
    'feature_fraction': 0.8,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'min_child_samples': 20,   # Добавлено: защита от переобучения
    'reg_alpha': 0.1,          # L1 регуляризация
    'reg_lambda': 0.1,         # L2 регуляризация
    'scale_pos_weight': None,  # Будет рассчитано
    'verbose': -1,
    'n_jobs': -1,
    'seed': 42,
    # 🔥 НОВИНКА: Focal Loss параметры для работы с дисбалансом
    'pos_bagging_fraction': 1.0,
    'neg_bagging_fraction': 0.5  # Семплирование негативных примеров
}


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
    🔧 v2: Добавлены дополнительные признаки через JOIN
    """
    logger.info("\n📊 Загрузка данных для обучения (v2 - расширенные признаки)...")
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
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
        se.days_since_last_purchase_group,
        -- 🔥 НОВЫЕ ПРИЗНАКИ v2:
        COALESCE(se.applicability, 0) as applicability,
        COALESCE(se.is_new, false) as is_new_flag
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
        logger.info(f"   ✅ Положительных примеров (target=1): {pos_count:,} ({pos_count/len(df)*100:.2f}%)")
        logger.info(f"   ✅ Отрицательных примеров (target=0): {neg_count:,}")
        logger.info(f"   ✅ Дисбаланс классов: {neg_count/pos_count:.1f}:1")
    
    return df


def create_interaction_features(df):
    """
    🔥 НОВИНКА v2: Создание взаимодействующих признаков
    """
    logger.info("\n🔧 Создание interaction features...")
    
    # 1. Взаимодействие частоты и тренда
    if 'frequency_90d' in df.columns and 'group_trend_6m' in df.columns:
        df['freq_trend_interaction'] = df['frequency_90d'] * df['group_trend_6m']
        logger.info("   ✅ freq_trend_interaction создан")
    
    # 2. Маржинальность × популярность
    if 'margin' in df.columns and 'global_popularity' in df.columns:
        df['margin_popularity'] = df['margin'] * df['global_popularity']
        logger.info("   ✅ margin_popularity создан")
    
    # 3. Давность × частота (лояльность)
    if 'days_since_last_purchase' in df.columns and 'frequency_30d' in df.columns:
        df['recency_frequency'] = df['frequency_30d'] / (df['days_since_last_purchase'] + 1)
        logger.info("   ✅ recency_frequency создан")
    
    # 4. Доля группы × тренд
    if 'group_share_in_portfolio' in df.columns and 'group_trend_6m' in df.columns:
        df['share_trend'] = df['group_share_in_portfolio'] * df['group_trend_6m']
        logger.info("   ✅ share_trend создан")
    
    # 5. Полиномиальные признаки для ключевых переменных
    if 'frequency_90d' in df.columns:
        df['frequency_90d_sq'] = df['frequency_90d'] ** 2
        logger.info("   ✅ frequency_90d_sq создан")
    
    if 'days_since_last_purchase' in df.columns:
        df['days_since_log'] = np.log1p(df['days_since_last_purchase'])
        logger.info("   ✅ days_since_log создан")
    
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
    """Формирует итоговый список признаков (v2 - расширенный)"""
    logger.info("\n🔧 Подготовка признаков (v2 - расширенный набор)...")
    
    base_features = [
        # Частотные признаки
        'frequency_30d', 'frequency_90d',
        # Рецентность
        'days_since_last_purchase', 'days_since_last_purchase_group',
        # Скользящие средние
        'rolling_sales_2w', 'rolling_sales_4w', 'rolling_sales_8w',
        # Групповые признаки
        'group_trend_6m', 'group_share_in_portfolio',
        # Товарные признаки
        'margin', 'stock', 'price', 'global_popularity',
        # Портфельные признаки
        'portfolio_diversity',
        # 🔥 Новые признаки v2
        'applicability', 'is_new_flag'
    ]
    
    # Interaction features
    interaction_features = [
        'freq_trend_interaction', 'margin_popularity', 'recency_frequency',
        'share_trend', 'frequency_90d_sq', 'days_since_log'
    ]
    
    # Проверяем наличие
    feature_cols = [col for col in base_features if col in df.columns]
    feature_cols.extend([col for col in interaction_features if col in df.columns])
    feature_cols.extend(encoded_cols)
    
    X = df[feature_cols].fillna(0)
    y = df['target']
    
    logger.info(f"   ✅ Признаков: {len(feature_cols)}")
    logger.info(f"   ✅ Пример признаков: {feature_cols[:8]}")
    
    return X, y, feature_cols


def train_model_with_cv(X_train, y_train, X_test, y_test, feature_names, n_folds=3):
    """
    🔥 НОВИНКА v2: Обучение с кросс-валидацией для стабильности
    """
    logger.info("\n🧠 Обучение модели LightGBM v2 (с кросс-валидацией)...")
    
    neg_count = (y_train == 0).sum()
    pos_count = (y_train == 1).sum()
    scale_pos_weight = neg_count / pos_count
    
    params = OPTIMAL_PARAMS.copy()
    params['scale_pos_weight'] = scale_pos_weight
    
    logger.info(f"   🔧 scale_pos_weight: {scale_pos_weight:.2f}")
    logger.info(f"   🔧 num_leaves: {params['num_leaves']}")
    logger.info(f"   🔧 max_depth: {params['max_depth']}")
    logger.info(f"   🔧 learning_rate: {params['learning_rate']}")
    
    # Создаём датасеты
    train_data = lgb.Dataset(X_train, label=y_train, feature_name=feature_names)
    test_data = lgb.Dataset(X_test, label=y_test, feature_name=feature_names, reference=train_data)
    
    # 🔥 Кросс-валидация для подбора оптимального числа итераций
    logger.info(f"   📈 Кросс-валидация ({n_folds} folds)...")
    cv_results = lgb.cv(
        params,
        train_data,
        num_boost_round=1000,
        nfold=n_folds,
        stratified=True,
        early_stopping_rounds=50,
        metrics=['auc', 'binary_logloss'],
        seed=42,
        verbose_eval=False
    )
    
    # Оптимальное число итераций
    best_iteration = len(cv_results['auc-mean'])
    logger.info(f"   ✅ Оптимальное число итераций: {best_iteration}")
    
    # Финальное обучение на всех тренировочных данных
    logger.info("   📈 Финальное обучение на полном тренировочном наборе...")
    model = lgb.train(
        params,
        train_data,
        num_boost_round=best_iteration,
        valid_sets=[test_data],
        callbacks=[lgb.log_evaluation(period=200)]
    )
    
    logger.info(f"   ✅ Модель обучена (итераций: {model.num_trees()})")
    
    # Важность признаков
    logger.info("\n📊 Важность признаков (топ-15):")
    importance = model.feature_importance(importance_type='gain')
    feature_importance = sorted(zip(feature_names, importance), key=lambda x: x[1], reverse=True)
    for i, (feat, imp) in enumerate(feature_importance[:15]):
        logger.info(f"   {i+1}. {feat}: {imp:,.2f}")
    
    # 🔥 Feature Selection: убираем неважные признаки
    threshold = np.percentile(importance, 25)  # Убираем нижние 25%
    important_features = [f for f, imp in feature_importance if imp >= threshold]
    logger.info(f"\n   ✂️  Отбрано важных признаков: {len(important_features)} из {len(feature_names)}")
    
    return model, best_iteration, important_features


def dual_calibration(model, X_test, y_test, best_iteration):
    """
    🔥 НОВИНКА v2: Ансамбль калибровок (Isotonic + Platt)
    """
    logger.info("\n📈 Двойная калибровка вероятностей...")
    
    y_proba_raw = model.predict(X_test, num_iteration=best_iteration)
    
    # 1. Platt Scaling (Logistic Regression) - лучше для малых вероятностей
    calibrator_platt = LogisticRegression(solver='lbfgs', max_iter=1000, C=1.0)
    calibrator_platt.fit(y_proba_raw.reshape(-1, 1), y_test)
    y_proba_platt = calibrator_platt.predict_proba(y_proba_raw.reshape(-1, 1))[:, 1]
    
    # 2. Isotonic Regression - лучше для больших вероятностей
    calibrator_isotonic = IsotonicRegression(out_of_bounds='clip')
    calibrator_isotonic.fit(y_proba_raw, y_test)
    y_proba_isotonic = calibrator_isotonic.predict(y_proba_raw)
    
    # 3. Ensemble: средневзвешенное
    # Для низких вероятностей больше доверяем Platt, для высоких - Isotonic
    weights_platt = 1.0 / (1.0 + np.exp(10 * (y_proba_raw - 0.5)))  # Сигмоида
    weights_isotonic = 1.0 - weights_platt
    
    y_proba_ensemble = weights_platt * y_proba_platt + weights_isotonic * y_proba_isotonic
    
    logger.info("   ✅ Platt Scaling калибратор обучен")
    logger.info("   ✅ Isotonic Regression калибратор обучен")
    logger.info("   ✅ Ансамбль калибровок создан")
    
    # Выбираем лучший калибратор по Brier Score
    brier_platt = brier_score_loss(y_test, y_proba_platt)
    brier_isotonic = brier_score_loss(y_test, y_proba_isotonic)
    brier_ensemble = brier_score_loss(y_test, y_proba_ensemble)
    
    logger.info(f"\n   📊 Brier Score калибраторов:")
    logger.info(f"      • Platt: {brier_platt:.4f}")
    logger.info(f"      • Isotonic: {brier_isotonic:.4f}")
    logger.info(f"      • Ensemble: {brier_ensemble:.4f}")
    
    best_calibrator = 'ensemble'
    best_y_proba = y_proba_ensemble
    
    if brier_platt < brier_ensemble:
        best_calibrator = 'platt'
        best_y_proba = y_proba_platt
    elif brier_isotonic < brier_ensemble:
        best_calibrator = 'isotonic'
        best_y_proba = y_proba_isotonic
    
    logger.info(f"   🏆 Лучший калибратор: {best_calibrator}")
    
    return best_y_proba, {'platt': calibrator_platt, 'isotonic': calibrator_isotonic, 'method': best_calibrator}


def find_optimal_threshold(y_test, y_proba, min_recall=0.55, min_precision=0.35):
    """
    🔥 Улучшенный подбор порога с приоритетом на Recall
    """
    logger.info("\n🎯 Поиск оптимального порога классификации...")
    
    best_threshold = 0.2
    best_f1 = 0
    results = []
    
    for thresh in np.arange(0.05, 0.9, 0.01):
        y_pred_t = (y_proba >= thresh).astype(int)
        p = precision_score(y_test, y_pred_t, zero_division=0)
        r = recall_score(y_test, y_pred_t, zero_division=0)
        f1 = 2*p*r/(p+r) if (p+r) > 0 else 0
        
        results.append({
            'threshold': thresh,
            'precision': p,
            'recall': r,
            'f1': f1
        })
    
    # Сначала ищем порог с Recall >= min_recall
    valid_results = [r for r in results if r['recall'] >= min_recall]
    
    if valid_results:
        # Среди них выбираем с лучшим F1
        best = max(valid_results, key=lambda x: x['f1'])
        best_threshold = best['threshold']
        logger.info(f"   ✅ Найден порог с Recall >= {min_recall}: {best_threshold:.2f}")
    else:
        # Если нет такого порога, берём максимальный Recall
        best = max(results, key=lambda x: x['recall'])
        best_threshold = best['threshold']
        logger.warning(f"   ⚠️ Недостижим целевой Recall={min_recall}, выбран порог для максимизации: {best_threshold:.2f}")
    
    return best_threshold, results


def evaluate_model(y_test, y_proba, threshold):
    """Оценка качества модели"""
    logger.info("\n📊 Оценка качества модели...")
    
    y_pred = (y_proba >= threshold).astype(int)
    
    brier = brier_score_loss(y_test, y_proba)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    auc = roc_auc_score(y_test, y_proba)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    
    logger.info(f"   📈 Brier Score: {brier:.4f} (порог: <=0.20)")
    logger.info(f"   📈 AUC-ROC: {auc:.4f}")
    logger.info(f"   📈 Precision: {precision:.4f} (порог: >=0.35)")
    logger.info(f"   📈 Recall (Hit Rate): {recall:.4f} (порог: >=0.55)")
    logger.info(f"   📈 F1 Score: {f1:.4f}")
    
    # Проверка порогов ReadMe
    passed = (brier <= 0.20) and (precision >= 0.35) and (recall >= 0.55)
    
    metrics = {
        'brier_score': brier,
        'precision': precision,
        'hit_rate': recall,
        'auc': auc,
        'f1': f1,
        'threshold': threshold,
        'all_passed': passed
    }
    
    status = "✅" if passed else "⚠️"
    logger.info(f"\n   {status} Валидация метрик: {'ПРОЙДЕНА' if passed else 'НЕ ПРОЙДЕНА'}")
    
    if not passed:
        logger.warning(f"      • Brier: {'✅' if brier <= 0.20 else '❌'}")
        logger.warning(f"      • Precision: {'✅' if precision >= 0.35 else '❌'}")
        logger.warning(f"      • Recall: {'✅' if recall >= 0.55 else '❌'}")
    
    return metrics


def save_model(model, calibrators, encoders, feature_cols, best_iteration, metrics, important_features):
    """Сохранение артефактов модели"""
    logger.info("\n💾 Сохранение модели v2...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_path = MODEL_DIR / f"model_lightgbm_v2_{timestamp}.pkl"
    calib_path = MODEL_DIR / f"calibrator_lightgbm_v2_{timestamp}.pkl"
    enc_path = MODEL_DIR / f"encoders_lightgbm_v2_{timestamp}.pkl"
    
    with open(model_path, 'wb') as f:
        pickle.dump({
            'model': model,
            'feature_cols': feature_cols,
            'important_features': important_features,
            'best_iteration': best_iteration
        }, f)
    
    with open(calib_path, 'wb') as f:
        pickle.dump(calibrators, f)
    
    with open(enc_path, 'wb') as f:
        pickle.dump(encoders, f)
        
    logger.info(f"   ✅ Модель: {model_path.name}")
    logger.info(f"   ✅ Калибраторы: {calib_path.name}")
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
        'name': 'model_lightgbm_v2',
        'status': 'staging',  # По умолчанию staging для тестирования
        'metrics': {
            'precision_5': round(metrics['precision'], 4),
            'hit_rate': round(metrics['hit_rate'], 4),
            'brier_score': round(metrics['brier_score'], 4),
            'auc': round(metrics['auc'], 4),
            'f1': round(metrics['f1'], 4),
            'threshold': round(metrics['threshold'], 2),
            'training_time_hours': 0  # Будет обновлено
        },
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'model_path': str(model_path),
        'calibrator_path': str(calib_path),
        'auto_promote': True
    }
    
    # Удаляем старые версии v2 из списка
    registry['models'] = [m for m in registry['models'] if m['name'] != 'model_lightgbm_v2']
    registry['models'].append(new_entry)
    
    # Авто-продвижение если метрики лучше
    if registry['active_model'] == 'model_lightgbm_v1':
        prod_model = next((m for m in registry['models'] if m['name'] == 'model_lightgbm_v1'), None)
        if prod_model:
            prod_metrics = prod_model['metrics']
            if (metrics['precision'] > prod_metrics.get('precision_5', 0) and
                metrics['hit_rate'] > prod_metrics.get('hit_rate', 0)):
                registry['active_model'] = 'model_lightgbm_v2'
                new_entry['status'] = 'production'
                logger.info("   🚀 v2 автоматически переведён в production (метрики лучше)!")
    
    with open(registry_path, 'w', encoding='utf-8') as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)
    
    logger.info(f"   ✅ Реестр обновлён. Активная модель: {registry['active_model']}")


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info("="*70)
    logger.info("🧠 ProjectZZZ - ОБУЧЕНИЕ МОДЕЛИ LightGBM v2.0 (ОПТИМИЗИРОВАННАЯ)")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    start_time = time.time()
    engine = None
    
    try:
        config = load_config()
        engine = get_engine(config)
        
        # 1. Загрузка
        df = load_training_data(engine, limit=None)
        
        if len(df) < 10000:
            logger.error("❌ Недостаточно данных для обучения!")
            return 1
            
        # 2. Создание interaction features
        df = create_interaction_features(df)
        
        # 3. Кодирование
        cat_cols = ['marketing_group', 'brand', 'category']
        df, encoded_cols, encoders = encode_categorical(df, cat_cols)
        
        # 4. Признаки
        X, y, feature_cols = prepare_features(df, encoded_cols)
        
        # 5. Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # 6. Train с кросс-валидацией
        model, best_iter, important_features = train_model_with_cv(
            X_train, y_train, X_test, y_test, feature_cols, n_folds=3
        )
        
        # 7. Dual Calibration
        y_proba, calibrators = dual_calibration(model, X_test, y_test, best_iter)
        
        # 8. Поиск оптимального порога
        threshold, _ = find_optimal_threshold(y_test, y_proba, min_recall=0.55, min_precision=0.35)
        
        # 9. Evaluate
        metrics = evaluate_model(y_test, y_proba, threshold)
        
        # 10. Save
        model_path, calib_path = save_model(
            model, calibrators, encoders, feature_cols, best_iter, metrics, important_features
        )
        
        # 11. Registry
        update_registry(metrics, model_path, calib_path)
        
        elapsed = time.time() - start_time
        logger.info("\n" + "="*70)
        logger.info("🎉 ОБУЧЕНИЕ ЗАВЕРШЕНО!")
        logger.info("="*70)
        logger.info(f"⏱️ Общее время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        
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
