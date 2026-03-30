# -*- coding: utf-8 -*-
"""
services.py v1.0
🔧 БИЗНЕС-ЛОГИКА В СЕРВИСАХ
Назначение:
  - Разделение ответственности (SRP)
  - Инкапсуляция бизнес-логики
  - Упрощение тестирования

Использование:
    from src.services import RecommendationService, ModelService
    
    model_service = ModelService()
    model, calibrator, encoders = model_service.load_active_model()
    
    rec_service = RecommendationService()
    recommendations = rec_service.generate_for_clients(clients, model, ...)
"""

import logging
from pathlib import Path
from datetime import date
from typing import List, Dict, Tuple, Optional, Any

import pandas as pd
import numpy as np
import pickle
import json

logger = logging.getLogger(__name__)


# ==============================================================================
# MODEL SERVICE
# ==============================================================================

class ModelService:
    """
    Сервис для работы с ML моделями.
    Ответственность: загрузка модели, предсказания, кодирование.
    """
    
    def __init__(self, models_dir: Optional[Path] = None):
        """
        Args:
            models_dir: Директория с моделями
        """
        self.models_dir = models_dir or (Path(__file__).parent.parent / "models")
        self._model_cache = {}
    
    def load_active_model(self) -> Tuple[Any, Any, Dict, List[str], int]:
        """
        Загружает активную модель из реестра.
        
        Returns:
            (model, calibrator, encoders, feature_cols, best_iteration)
        """
        logger.info("\n📦 Загрузка активной модели...")
        
        registry_path = self.models_dir / "model_registry.json"
        
        if not registry_path.exists():
            raise FileNotFoundError(f"Реестр моделей не найден: {registry_path}")
        
        with open(registry_path, 'r', encoding='utf-8') as f:
            registry = json.load(f)
        
        active_model_name = registry.get('active_model')
        
        if not active_model_name:
            raise ValueError("Активная модель не указана в реестре!")
        
        logger.info(f"   ✅ Активная модель: {active_model_name}")
        
        model_info = next(
            (m for m in registry.get('models', []) if m['name'] == active_model_name),
            None
        )
        
        if not model_info:
            raise ValueError(f"Модель {active_model_name} не найдена в реестре!")
        
        # Загрузка модели
        model_path = self.models_dir / model_info['model_path'].split('\\')[-1]
        model = self._load_pickle(model_path)
        
        # Загрузка калибратора
        calib_path = model_info.get('calibrator_path')
        calibrator = None
        if calib_path:
            calib_file = self.models_dir / calib_path.split('\\')[-1]
            calibrator = self._load_pickle(calib_file)
        
        # Загрузка энкодеров
        encoders = self._load_latest_encoders()
        
        # Извлечение метаданных
        if isinstance(model, dict) and 'model' in model:
            feature_cols = model.get('feature_cols', [])
            best_iteration = model.get('best_iteration', 1000)
            model = model['model']
        else:
            feature_cols = getattr(model, 'feature_name_', [])
            best_iteration = 1000
        
        logger.info(f"   ✅ Признаков: {len(feature_cols)}")
        
        return model, calibrator, encoders, feature_cols, best_iteration
    
    def _load_pickle(self, path: Path) -> Any:
        """Загружает pickle файл"""
        try:
            with open(path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            raise FileNotFoundError(f"Ошибка загрузки {path}: {e}")
    
    def _load_latest_encoders(self) -> Dict:
        """Загружает последние энкодеры"""
        encoders_files = list(self.models_dir.glob("encoders_lightgbm_*.pkl"))
        
        if not encoders_files:
            logger.warning("⚠️ Энкодеры не найдены")
            return {}
        
        latest = sorted(encoders_files, key=lambda p: p.stat().st_mtime)[-1]
        logger.info(f"   ✅ Энкодеры: {latest.name}")
        
        return self._load_pickle(latest)
    
    def predict(
        self,
        model: Any,
        df: pd.DataFrame,
        feature_cols: List[str],
        calibrator: Optional[Any] = None,
        best_iteration: int = 1000
    ) -> np.ndarray:
        """
        Генерирует предсказания.
        
        Args:
            model: Обученная модель
            df: DataFrame с признаками
            feature_cols: Список признаков
            calibrator: Калибратор (опционально)
            best_iteration: Количество итераций
        
        Returns:
            Массив вероятностей
        """
        available_cols = [col for col in feature_cols if col in df.columns]
        
        if not available_cols:
            logger.warning("⚠️ Нет доступных признаков для предсказания")
            return np.zeros(len(df))
        
        X = df[available_cols].fillna(0)
        X = X.infer_objects(copy=False)
        
        # Предсказание
        if hasattr(model, 'predict_proba'):
            y_proba_raw = model.predict_proba(X)[:, 1]
        elif hasattr(model, 'predict'):
            y_proba_raw = model.predict(
                X,
                num_iteration=best_iteration if best_iteration > 0 else None,
                predict_disable_shape_check=True
            )
        else:
            raise ValueError("Модель не имеет метода predict")
        
        # Калибровка
        if calibrator:
            try:
                return calibrator.predict_proba(y_proba_raw.reshape(-1, 1))[:, 1]
            except Exception:
                return y_proba_raw
        
        return y_proba_raw


# ==============================================================================
# CANDIDATE SERVICE
# ==============================================================================

class CandidateService:
    """
    Сервис для работы с кандидатами на рекомендацию.
    Ответственность: загрузка, фильтрация, подготовка кандидатов.
    """
    
    def __init__(self, db_repository=None):
        """
        Args:
            db_repository: Репозиторий для работы с БД
        """
        self.db_repo = db_repository
    
    def load_candidates(
        self,
        client_ids: List[str],
        batch_size: int = 200
    ) -> pd.DataFrame:
        """
        Загружает кандидатов для клиентов.
        
        Args:
            client_ids: Список идентификаторов
            batch_size: Размер пакета
        
        Returns:
            DataFrame с кандидатами
        """
        if self.db_repo is None:
            raise ValueError("db_repository не установлен")
        
        return self.db_repo.get_candidates(client_ids, batch_size=batch_size)
    
    def filter_by_stock(self, df: pd.DataFrame, min_stock: int = 1) -> pd.DataFrame:
        """Фильтрует кандидатов по остатку"""
        if 'stock' not in df.columns:
            return df
        
        return df[df['stock'] >= min_stock]
    
    def filter_by_probability(
        self,
        df: pd.DataFrame,
        prob_col: str = 'predicted_prob',
        min_prob: float = 0.0
    ) -> pd.DataFrame:
        """Фильтрует кандидатов по вероятности"""
        if prob_col not in df.columns:
            return df
        
        return df[df[prob_col] >= min_prob]


# ==============================================================================
# RECOMMENDATION SERVICE
# ==============================================================================

class RecommendationService:
    """
    Сервис генерации рекомендаций.
    Ответственность: применение бизнес-правил (2+2+1).
    """
    
    def __init__(
        self,
        probability_threshold_new: float = 0.05,
        trend_threshold_develop: float = 0.02,
        trend_threshold_retain: float = -0.02
    ):
        """
        Args:
            probability_threshold_new: Порог для новых товаров
            trend_threshold_develop: Порог тренда для развития
            trend_threshold_retain: Порог тренда для возврата
        """
        self.prob_threshold_new = probability_threshold_new
        self.trend_develop = trend_threshold_develop
        self.trend_retain = trend_threshold_retain
    
    def generate_for_client(
        self,
        client_df: pd.DataFrame,
        prob_col: str = 'predicted_prob'
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Генерирует рекомендации для одного клиента.
        
        Args:
            client_df: DataFrame с кандидатами для клиента
            prob_col: Колонка с вероятностями
        
        Returns:
            (selected_skus, fallback_reason)
        """
        selected = []
        fallback_reasons = []
        
        # ШАГ 1: Новые SKU (2 шт)
        new_skus = client_df[client_df['is_new_for_client'] == 1].copy() \
            if not client_df.empty else pd.DataFrame()
        
        if not new_skus.empty:
            new_skus = new_skus.sort_values(prob_col, ascending=False)
            top_new = new_skus.head(2)
            
            for _, rec in top_new.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'new'
                selected.append(rec_dict)
            
            if len(top_new) < 2:
                fallback_reasons.append(f"New_low_candidates:{2-len(top_new)}")
        else:
            familiar = client_df[client_df['is_new_for_client'] == 0] \
                .sort_values('margin', ascending=False).head(2) \
                if not client_df.empty else pd.DataFrame()
            
            for _, rec in familiar.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'new_fallback'
                selected.append(rec_dict)
            
            fallback_reasons.append("No_new_candidates_at_all")
        
        # ШАГ 2: Развитие (2 шт)
        develop_mask = (client_df['is_new_for_client'] == 0) & \
                       (client_df['group_trend_6m'] > self.trend_develop) \
                       if not client_df.empty else pd.Series(dtype=bool)
        
        develop_skus = client_df[develop_mask].copy() if not client_df.empty else pd.DataFrame()
        
        if not develop_skus.empty:
            develop_skus['score'] = develop_skus[prob_col] * (1 + 0.5 * develop_skus['group_trend_6m'])
            develop_skus = develop_skus.sort_values('score', ascending=False)
            top_dev = develop_skus.head(2)
            
            for _, rec in top_dev.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'develop'
                selected.append(rec_dict)
        else:
            stable = client_df[client_df['is_new_for_client'] == 0] \
                .sort_values(prob_col, ascending=False).head(2) \
                if not client_df.empty else pd.DataFrame()
            
            for _, rec in stable.iterrows():
                rec_dict = rec.to_dict()
                rec_dict['selection_type'] = 'develop_fallback'
                selected.append(rec_dict)
            
            fallback_reasons.append("No_growing_groups")
        
        # ШАГ 3: Возврат (1 шт)
        retain_mask = (client_df['is_new_for_client'] == 0) & \
                      (client_df['group_trend_6m'] < self.trend_retain) \
                      if not client_df.empty else pd.Series(dtype=bool)
        
        retain_skus = client_df[retain_mask].copy() if not client_df.empty else pd.DataFrame()
        
        if not retain_skus.empty:
            retain_skus['score'] = retain_skus[prob_col] * \
                                   (1 + retain_skus['days_since_last_purchase_group'] / 365)
            retain_skus = retain_skus.sort_values('score', ascending=False)
            
            rec_dict = retain_skus.iloc[0].to_dict()
            rec_dict['selection_type'] = 'retain'
            selected.append(rec_dict)
        else:
            old = client_df[client_df['is_new_for_client'] == 0] \
                .sort_values('days_since_last_purchase', ascending=False).head(1) \
                if not client_df.empty else pd.DataFrame()
            
            if not old.empty:
                rec_dict = old.iloc[0].to_dict()
                rec_dict['selection_type'] = 'retain_fallback'
                selected.append(rec_dict)
            
            fallback_reasons.append("No_declining_groups")
        
        # ШАГ 4: Удаление дубликатов
        seen = set()
        unique_selected = []
        
        for s in selected:
            if s['sku_id'] not in seen:
                unique_selected.append(s)
                seen.add(s['sku_id'])
        
        selected = unique_selected[:5]
        
        fallback_str = "; ".join([fr for fr in fallback_reasons if fr]) if fallback_reasons else None
        
        return selected, fallback_str
    
    def generate_for_clients(
        self,
        df_candidates: pd.DataFrame,
        client_ids: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Генерирует рекомендации для всех клиентов (групповая обработка).
        
        Args:
            df_candidates: DataFrame со всеми кандидатами
            client_ids: Список клиентов (опционально)
        
        Returns:
            DataFrame с рекомендациями
        """
        all_recommendations = []
        
        # Группировка по клиентам
        grouped = df_candidates.groupby('client_id')
        
        for client_id, client_df in grouped:
            if client_ids and client_id not in client_ids:
                continue
            
            selected, fallback = self.generate_for_client(client_df)
            
            for rec in selected:
                rec['client_id'] = client_id
                rec['fallback_reason'] = fallback
                all_recommendations.append(rec)
        
        return pd.DataFrame(all_recommendations)


# ==============================================================================
# PERSISTENCE SERVICE
# ==============================================================================

class PersistenceService:
    """
    Сервис для сохранения результатов.
    Ответственность: сохранение в БД, экспорт в файлы.
    """
    
    def __init__(self, db_repository=None):
        """
        Args:
            db_repository: Репозиторий для работы с БД
        """
        self.db_repo = db_repository
    
    def save_to_database(
        self,
        engine: Any,
        visit_date: date,
        recommendations: List[Dict]
    ):
        """
        Сохраняет рекомендации в БД.
        
        Args:
            engine: SQLAlchemy engine
            visit_date: Дата визита
            recommendations: Список рекомендаций
        """
        if not recommendations:
            return
        
        # Проверка существования колонок
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'visit_proposals'
                  AND column_name IN ('sku_name', 'applicability', 'ab_group')
            """))
            existing_cols = set(row[0] for row in result.fetchall())
        
        base_cols = [
            'visit_date', 'client_id', 'client_name', 'sku_id',
            'predicted_prob', 'selection_type', 'fallback_reason',
            'model_version', 'created_at'
        ]
        optional_cols = [c for c in ['sku_name', 'applicability', 'ab_group'] if c in existing_cols]
        all_cols = base_cols + optional_cols
        
        query = f"INSERT INTO visit_proposals ({', '.join(all_cols)}) VALUES ({', '.join([f':{c}' for c in all_cols])})"
        
        with engine.begin() as conn:
            for rec in recommendations:
                params = {k: v for k, v in rec.items() if k in all_cols}
                params['created_at'] = pd.Timestamp.now()
                conn.execute(text(query), params)
        
        logger.info(f"💾 Сохранено {len(recommendations)} рекомендаций в БД")
    
    def export_to_excel(
        self,
        visit_date: date,
        recommendations: List[Dict],
        output_dir: Path,
        summary_stats: Optional[Dict] = None
    ) -> Path:
        """
        Экспортирует рекомендации в Excel.
        
        Args:
            visit_date: Дата визита
            recommendations: Список рекомендаций
            output_dir: Директория для вывода
            summary_stats: Статистика для сводки
        
        Returns:
            Путь к файлу
        """
        import pandas as pd
        
        if not recommendations:
            return None
        
        df = pd.DataFrame(recommendations)
        
        df_output = pd.DataFrame({
            'Дата визита': df['visit_date'],
            'Клиент': df['client_name'],
            'A/B группа': df.get('ab_group', 'control'),
            'Тип': df['selection_type'],
            'Артикул': df.get('article', df['sku_id']),
            'SKU': df.get('sku_name', df['sku_id']),
            'Вероятность': df['predicted_prob'].apply(lambda x: f"{x:.1%}")
        })
        
        filename = output_dir / f"recommendations_{visit_date.strftime('%Y-%m-%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_output.to_excel(writer, sheet_name='Рекомендации', index=False)
            
            if summary_stats:
                pd.DataFrame([summary_stats]).to_excel(
                    writer, sheet_name='Сводка', index=False
                )
        
        logger.info(f"📄 Экспорт в Excel: {filename}")
        return filename
