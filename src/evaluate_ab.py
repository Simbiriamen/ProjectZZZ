# -*- coding: utf-8 -*-
"""
evaluate_ab.py v1.0
Анализ A/B теста и автоматическое переключение моделей
Согласно ReadMe_ProjectZZZ.txt раздел 7
"""
import sys
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from scipy import stats
import yaml

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
        logging.FileHandler(LOG_DIR / "evaluate_ab.log", encoding='utf-8', mode='w'),
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

def get_ab_config(config) -> dict:
    ab = config.get('ab_test', {})
    return {
        'enabled': ab.get('enabled', False),
        'test_group_ratio': ab.get('test_group_ratio', 0.5),
        'min_duration_days': ab.get('promotion', {}).get('min_duration_days', 14),
        'significance_level': ab.get('promotion', {}).get('significance_level', 0.05),
        'min_uplift': ab.get('promotion', {}).get('min_uplift', 0.03),
        'auto_promote': ab.get('promotion', {}).get('auto_promote', True),
        'critical_threshold': ab.get('degradation', {}).get('critical_threshold', 0.05)
    }

def calculate_metrics(engine, group: str, days: int = 14) -> dict:
    """Рассчитывает метрики для группы за период"""
    start_date = datetime.now().date() - timedelta(days=days)
    
    query = text("""
    SELECT 
        vp.sku_id,
        vp.predicted_prob,
        CASE WHEN p.id IS NOT NULL THEN 1 ELSE 0 END AS purchased
    FROM visit_proposals vp
    LEFT JOIN purchases p ON vp.client_id = p.client_id 
        AND vp.sku_id = p.sku_id
        AND p.purchase_date BETWEEN vp.visit_date AND vp.visit_date + INTERVAL '10 days'
    WHERE vp.visit_date >= :start_date
      AND vp.ab_group = :group
    """)
    
    df = pd.read_sql(query, engine, params={'start_date': start_date, 'group': group})
    
    if df.empty:
        return {'precision_5': 0, 'hit_rate': 0, 'brier_score': 1.0, 'count': 0}
    
    precision = df['purchased'].mean()
    hit_rate = df.groupby(['visit_date', 'client_id'])['purchased'].max().mean()
    
    df['brier'] = (df['predicted_prob'] - df['purchased']) ** 2
    brier_score = df['brier'].mean()
    
    return {
        'precision_5': precision,
        'hit_rate': hit_rate,
        'brier_score': brier_score,
        'count': len(df)
    }

def test_significance(control_metrics: dict, test_metrics: dict, 
                     significance_level: float = 0.05) -> dict:
    """Проверяет статистическую значимость различий"""
    results = {}
    
    for metric in ['precision_5', 'hit_rate']:
        p1 = control_metrics[metric]
        p2 = test_metrics[metric]
        n1 = control_metrics['count']
        n2 = test_metrics['count']
        
        if n1 < 100 or n2 < 100:
            results[f'{metric}_significant'] = False
            results[f'{metric}_pvalue'] = None
            continue
        
        p_pooled = (p1 * n1 + p2 * n2) / (n1 + n2)
        se = np.sqrt(p_pooled * (1 - p_pooled) * (1/n1 + 1/n2))
        z_stat = (p2 - p1) / se if se > 0 else 0
        p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))
        
        results[f'{metric}_significant'] = p_value < significance_level
        results[f'{metric}_pvalue'] = p_value
        results[f'{metric}_uplift'] = (p2 - p1) / p1 if p1 > 0 else 0
    
    results['brier_improved'] = test_metrics['brier_score'] < control_metrics['brier_score']
    
    return results

def make_decision(ab_config: dict, control_metrics: dict, test_metrics: dict, 
                 significance_results: dict) -> dict:
    """Принимает решение о переключении модели"""
    min_uplift = ab_config['min_uplift']
    critical_threshold = ab_config['critical_threshold']
    
    # 🔴 Проверка на деградацию
    for metric in ['precision_5', 'hit_rate']:
        control_val = control_metrics[metric]
        test_val = test_metrics[metric]
        if control_val > 0:
            drop = (control_val - test_val) / control_val
            if drop >= critical_threshold:
                return {
                    'action': 'rollback',
                    'reason': f"{metric} упал на {drop*100:.1f}% (порог: {critical_threshold*100}%)"
                }
    
    brier_worsen = (test_metrics['brier_score'] - control_metrics['brier_score']) / control_metrics['brier_score']
    if brier_worsen >= critical_threshold:
        return {
            'action': 'rollback',
            'reason': f"Brier Score ухудшился на {brier_worsen*100:.1f}% (порог: {critical_threshold*100}%)"
        }
    
    # 🟢 Проверка на улучшение
    uplift_ok = False
    reason_parts = []
    
    for metric in ['precision_5', 'hit_rate']:
        uplift = significance_results[f'{metric}_uplift']
        significant = significance_results[f'{metric}_significant']
        
        if uplift >= min_uplift and significant:
            uplift_ok = True
            reason_parts.append(f"{metric} +{uplift*100:.1f}% (p={significance_results[f'{metric}_pvalue']:.3f})")
    
    if uplift_ok and significance_results.get('brier_improved', False):
        return {
            'action': 'promote',
            'reason': "; ".join(reason_parts) + "; Brier improved"
        }
    
    return {'action': 'continue', 'reason': 'Нет статистически значимых улучшений'}

def update_registry(controller, decision: dict, test_model: str, production_model: str):
    """Обновляет реестр моделей на основе решения"""
    if decision['action'] == 'promote':
        controller.promote_to_production(test_model, reason=f"ab_test: {decision['reason']}")
        logger.info(f"🚀 АВТО-ПРОДВИЖЕНИЕ: {test_model} → production")
    elif decision['action'] == 'rollback':
        controller.rollback(reason=f"ab_test_degradation: {decision['reason']}")
        logger.warning(f"⚠️ АВТО-ОТКАТ: {production_model} → предыдущая версия")


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info("="*70)
    logger.info("🧪 ProjectZZZ - A/B TEST EVALUATOR v1.0")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)

    engine = None

    try:
        config = load_config()
        ab_config = get_ab_config(config)

        if not ab_config['enabled']:
            logger.info("ℹ️ A/B тестирование отключено в config.yaml")
            return 0

        engine = get_engine(config)

        registry_path = PROJECT_ROOT / "models" / "model_registry.json"
        with open(registry_path, 'r', encoding='utf-8') as f:
            registry = json.load(f)

        production_model = registry.get('active_model')
        staging_models = [m['name'] for m in registry['models']
                         if m['status'] == 'staging' and m.get('auto_promote')]

        if not production_model:
            logger.error("❌ Нет активной production-модели")
            return 1

        if not staging_models:
            logger.info("ℹ️ Нет staging-моделей с auto_promote для тестирования")
            return 0

        test_model = staging_models[0]
        logger.info(f"🔍 Сравнение: {production_model} (control) vs {test_model} (test)")

        days = ab_config['min_duration_days']
        control_metrics = calculate_metrics(engine, 'control', days)
        test_metrics = calculate_metrics(engine, 'test', days)

        logger.info(f"\n📊 Метрики за {days} дней:")
        logger.info(f"   Control ({production_model}):")
        logger.info(f"      • Precision@5: {control_metrics['precision_5']:.3f}")
        logger.info(f"      • Hit Rate: {control_metrics['hit_rate']:.3f}")
        logger.info(f"      • Brier Score: {control_metrics['brier_score']:.3f}")
        logger.info(f"      • Записей: {control_metrics['count']:,}")

        logger.info(f"   Test ({test_model}):")
        logger.info(f"      • Precision@5: {test_metrics['precision_5']:.3f}")
        logger.info(f"      • Hit Rate: {test_metrics['hit_rate']:.3f}")
        logger.info(f"      • Brier Score: {test_metrics['brier_score']:.3f}")
        logger.info(f"      • Записей: {test_metrics['count']:,}")

        significance = test_significance(
            control_metrics, test_metrics,
            ab_config['significance_level']
        )

        logger.info(f"\n🔬 Статистическая значимость (α={ab_config['significance_level']}):")
        for metric in ['precision_5', 'hit_rate']:
            sig = significance[f'{metric}_significant']
            pval = significance[f'{metric}_pvalue']
            uplift = significance[f'{metric}_uplift']
            logger.info(f"   • {metric}: {'✅' if sig else '❌'} (p={pval:.3f}, uplift={uplift*100:.1f}%)")

        decision = make_decision(ab_config, control_metrics, test_metrics, significance)

        logger.info(f"\n🎯 РЕШЕНИЕ: {decision['action'].upper()}")
        logger.info(f"   Причина: {decision['reason']}")

        if ab_config['auto_promote'] and decision['action'] != 'continue':
            from models.model_controller import ModelController
            controller = ModelController(
                models_dir=PROJECT_ROOT / "models",
                registry_path=registry_path
            )
            update_registry(controller, decision, test_model, production_model)

        logger.info("\n" + "="*70)
        logger.info("✅ A/B EVALUATION COMPLETED")
        logger.info("="*70)

        return 0

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # 🔧 ИСПРАВЛЕНИЕ v1.1: Освобождение ресурсов
        if engine:
            engine.dispose()


if __name__ == "__main__":
    sys.exit(main())