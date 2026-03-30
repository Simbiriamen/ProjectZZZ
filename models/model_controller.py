# -*- coding: utf-8 -*-
"""
model_controller.py v1.0
ДИСПЕТЧЕР МОДЕЛЕЙ: выбирает активную модель на основе метрик
Согласно ReadMe_ProjectZZZ.txt раздел 2.3
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
import pickle

logger = logging.getLogger(__name__)


class ModelController:
    """Контроллер управления версиями моделей"""
    
    def __init__(self, models_dir: Path, registry_path: Path):
        self.models_dir = models_dir
        self.registry_path = registry_path
        self.registry = self._load_registry()
    
    def _load_registry(self) -> Dict:
        """Загружает реестр моделей"""
        if self.registry_path.exists():
            with open(self.registry_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {'active_model': None, 'models': [], 'history': []}
    
    def _save_registry(self):
        """Сохраняет реестр"""
        with open(self.registry_path, 'w', encoding='utf-8') as f:
            json.dump(self.registry, f, indent=2, ensure_ascii=False)
    
    def register_model(self, name: str, metrics: Dict, 
                      auto_promote: bool = True, status: str = 'staging'):
        """Регистрирует новую модель в реестре"""
        model_entry = {
            'name': name,
            'status': status,
            'metrics': metrics,
            'activated_date': datetime.now().strftime('%Y-%m-%d') if status == 'production' else None,
            'auto_promote': auto_promote
        }
        self.registry['models'].append(model_entry)
        self._save_registry()
        logger.info(f"✅ Модель {name} зарегистрирована в реестре")
    
    def get_active_model(self) -> Optional[str]:
        """Возвращает имя активной модели"""
        return self.registry.get('active_model')
    
    def load_model(self, model_name: Optional[str] = None):
        """Загружает модель (активную или указанную)"""
        name = model_name or self.get_active_model()
        if not name:
            raise ValueError("Нет активной модели и не указана конкретная")
        
        model_path = self.models_dir / f"{name}.pkl"
        if not model_path.exists():
            raise FileNotFoundError(f"Модель {name} не найдена: {model_path}")
        
        with open(model_path, 'rb') as f:
            return pickle.load(f)
    
    def evaluate_promotion(self, staging_model: str, production_model: str) -> bool:
        """
        Оценивает, можно ли продвинуть staging-модель в production
        Критерии из ReadMe раздел 2.3:
        - precision_5 улучшился на >= 3%
        - hit_rate улучшился на >= 3%
        - brier_score не ухудшился
        - training_time не увеличился более чем в 2 раза
        """
        staging = next((m for m in self.registry['models'] if m['name'] == staging_model), None)
        production = next((m for m in self.registry['models'] if m['name'] == production_model), None)
        
        if not staging or not production:
            logger.error("❌ Одна из моделей не найдена в реестре")
            return False
        
        s_metrics = staging['metrics']
        p_metrics = production['metrics']
        
        precision_improved = s_metrics.get('precision_5', 0) >= p_metrics.get('precision_5', 0) * 1.03
        hitrate_improved = s_metrics.get('hit_rate', 0) >= p_metrics.get('hit_rate', 0) * 1.03
        brier_ok = s_metrics.get('brier_score', 1.0) <= p_metrics.get('brier_score', 1.0)
        time_ok = s_metrics.get('training_time_hours', 0) <= p_metrics.get('training_time_hours', 0) * 2
        
        logger.info(f"📊 Оценка продвижения {staging_model} → {production_model}:")
        logger.info(f"   • Precision: {s_metrics.get('precision_5'):.3f} vs {p_metrics.get('precision_5'):.3f} → {'✅' if precision_improved else '❌'}")
        logger.info(f"   • Hit Rate: {s_metrics.get('hit_rate'):.3f} vs {p_metrics.get('hit_rate'):.3f} → {'✅' if hitrate_improved else '❌'}")
        logger.info(f"   • Brier Score: {s_metrics.get('brier_score'):.3f} vs {p_metrics.get('brier_score'):.3f} → {'✅' if brier_ok else '❌'}")
        logger.info(f"   • Training Time: {s_metrics.get('training_time_hours'):.1f}h vs {p_metrics.get('training_time_hours'):.1f}h → {'✅' if time_ok else '❌'}")
        
        return precision_improved and hitrate_improved and brier_ok and time_ok
    
    def promote_to_production(self, model_name: str, reason: str = "manual"):
        """Переводит модель в production статус"""
        for model in self.registry['models']:
            if model['name'] == self.registry['active_model']:
                model['status'] = 'archived'
            if model['name'] == model_name:
                model['status'] = 'production'
                model['activated_date'] = datetime.now().strftime('%Y-%m-%d')
        
        self.registry['active_model'] = model_name
        self.registry['history'].append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'action': 'promote',
            'model': model_name,
            'reason': reason
        })
        self._save_registry()
        logger.info(f"🚀 Модель {model_name} переведена в production")
    
    def rollback(self, reason: str = "degradation"):
        """Откат на предыдущую production-модель"""
        production_models = [m for m in self.registry['models'] 
                           if m['status'] == 'production' and m['name'] != self.registry['active_model']]
        
        if not production_models:
            logger.error("❌ Нет предыдущих production-моделей для отката")
            return False
        
        prev_model = max(production_models, key=lambda m: m.get('activated_date', ''))
        
        for model in self.registry['models']:
            if model['name'] == self.registry['active_model']:
                model['status'] = 'disabled'
            if model['name'] == prev_model['name']:
                model['status'] = 'production'
                model['activated_date'] = datetime.now().strftime('%Y-%m-%d')
        
        old_active = self.registry['active_model']
        self.registry['active_model'] = prev_model['name']
        self.registry['history'].append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'action': 'rollback',
            'from_model': old_active,
            'to_model': prev_model['name'],
            'reason': reason
        })
        self._save_registry()
        logger.warning(f"⚠️ ОТКАТ: {old_active} → {prev_model['name']} ({reason})")
        return True
    
    def weekly_health_check(self, current_metrics: Dict) -> Dict[str, bool]:
        """
        Еженедельная проверка здоровья активной модели
        Критерии из ReadMe раздел 8.3
        """
        active_name = self.get_active_model()
        if not active_name:
            return {'healthy': False, 'action': 'no_active_model'}
        
        active_model = next((m for m in self.registry['models'] if m['name'] == active_name), None)
        if not active_model:
            return {'healthy': False, 'action': 'model_not_in_registry'}
        
        baseline = active_model['metrics']
        results = {}
        
        if baseline.get('precision_5'):
            precision_drop = (baseline['precision_5'] - current_metrics.get('precision_5', 0)) / baseline['precision_5']
            results['precision_ok'] = precision_drop < 0.05
            if not results['precision_ok']:
                logger.critical(f"🔴 Precision упал на {precision_drop*100:.1f}% (порог: 5%)")
        
        if baseline.get('hit_rate'):
            hitrate_drop = (baseline['hit_rate'] - current_metrics.get('hit_rate', 0)) / baseline['hit_rate']
            results['hitrate_ok'] = hitrate_drop < 0.05
            if not results['hitrate_ok']:
                logger.critical(f"🔴 Hit Rate упал на {hitrate_drop*100:.1f}% (порог: 5%)")
        
        if baseline.get('brier_score'):
            brier_worsen = (current_metrics.get('brier_score', 0) - baseline['brier_score']) / baseline['brier_score']
            results['brier_ok'] = brier_worsen < 0.10
            if not results['brier_ok']:
                logger.critical(f"🔴 Brier Score ухудшился на {brier_worsen*100:.1f}% (порог: 10%)")
        
        critical_failures = [k for k, v in results.items() if k.endswith('_ok') and not v]
        
        if critical_failures:
            results['healthy'] = False
            results['action'] = 'rollback'
            logger.critical(f"🚨 КРИТИЧЕСКАЯ ДЕГРАДАЦИЯ: {critical_failures} → требуется откат!")
        else:
            results['healthy'] = True
            results['action'] = 'continue'
            logger.info("✅ Модель здорова, продолжаем работу")
        
        return results