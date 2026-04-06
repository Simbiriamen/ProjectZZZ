# -*- coding: utf-8 -*-
"""
Сервис работы с ML моделями.

Инкапсулирует логику загрузки, предсказания и управления моделями.
Не зависит от конкретных реализаций репозиториев (использует протоколы).
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
import pickle
import json

import numpy as np
import pandas as pd

from ..protocols.repositories import ModelRepositoryProtocol

logger = logging.getLogger(__name__)


class ModelService:
    """
    Доменный сервис для работы с ML моделями.
    
    Ответственность:
    - Загрузка активной модели из реестра
    - Генерация предсказаний
    - Кодирование признаков
    - Калибровка вероятностей
    
    Зависимости:
    - ModelRepositoryProtocol (абстракция)
    """
    
    def __init__(
        self,
        model_repo: Optional[ModelRepositoryProtocol] = None,
        models_dir: Optional[Path] = None
    ):
        """
        Args:
            model_repo: Репозиторий моделей (опционально)
            models_dir: Директория с моделями
        """
        self._model_repo = model_repo
        self._models_dir = models_dir
        self._model_cache: Dict[str, Any] = {}
        self._calibrator_cache: Dict[str, Any] = {}
        self._encoders_cache: Dict[str, Dict] = {}
    
    def load_active_model(self) -> Dict[str, Any]:
        """
        Загружает активную модель из реестра.
        
        Returns:
            Словарь с компонентами модели:
            - model: ML модель
            - calibrator: Калибратор (опционально)
            - encoders: Энкодеры категориальных признаков
            - feature_cols: Список признаков
            - best_iteration: Лучшая итерация для LightGBM
        
        Raises:
            FileNotFoundError: Реестр моделей не найден
            ValueError: Активная модель не указана
        """
        logger.info("📦 Загрузка активной модели...")
        
        if self._model_repo:
            return self._model_repo.load_active_model()
        
        # Fallback: загрузка из файловой системы
        registry_path = self._get_registry_path()
        
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
        model_path = self._get_models_dir() / model_info['model_path'].split('\\')[-1]
        model = self._load_pickle(model_path)
        
        # Загрузка калибратора
        calibrator = self._load_calibrator(model_info)
        
        # Загрузка энкодеров
        encoders = self._load_encoders()
        
        # Извлечение метаданных
        feature_cols, best_iteration = self._extract_metadata(model)
        
        result = {
            'model': model,
            'calibrator': calibrator,
            'encoders': encoders,
            'feature_cols': feature_cols,
            'best_iteration': best_iteration,
        }
        
        logger.info("   ✅ Модель успешно загружена")
        return result
    
    def predict(
        self,
        features_df: pd.DataFrame,
        model_components: Dict[str, Any],
        apply_calibration: bool = True
    ) -> np.ndarray:
        """
        Генерирует предсказания модели.
        
        Args:
            features_df: DataFrame с признаками
            model_components: Компоненты модели (из load_active_model)
            apply_calibration: Применить калибровку
        
        Returns:
            Массив вероятностей
        """
        model = model_components['model']
        calibrator = model_components.get('calibrator')
        feature_cols = model_components['feature_cols']
        best_iteration = model_components.get('best_iteration', 1000)
        
        # Подготовка данных
        X = features_df.reindex(columns=feature_cols, fill_value=0)
        
        # Предсказание
        if hasattr(model, 'predict_proba'):
            raw_probs = model.predict_proba(X)[:, 1]
        else:
            raw_probs = model.predict(X, num_iteration=best_iteration)
        
        # Калибровка
        if apply_calibration and calibrator is not None:
            calibrated_probs = calibrator.predict(raw_probs.reshape(-1, 1))[:, 0]
            logger.debug("   🎯 Применена калибровка вероятностей")
            return calibrated_probs
        
        return raw_probs
    
    def encode_features(
        self,
        df: pd.DataFrame,
        encoders: Dict[str, Any],
        categorical_cols: List[str]
    ) -> pd.DataFrame:
        """
        Кодирует категориальные признаки.
        
        Args:
            df: Исходный DataFrame
            encoders: Словарь энкодеров {column: encoder}
            categorical_cols: Список категориальных колонок
        
        Returns:
            DataFrame с закодированными признаками
        """
        result = df.copy()
        
        for col in categorical_cols:
            if col in encoders:
                encoder = encoders[col]
                # Обработка неизвестных категорий
                result[col] = df[col].apply(
                    lambda x: encoder.get(x, -1)
                )
        
        return result
    
    def _get_registry_path(self) -> Path:
        """Получает путь к реестру моделей."""
        return self._get_models_dir() / "model_registry.json"
    
    def _get_models_dir(self) -> Path:
        """Получает директорию моделей."""
        if self._models_dir:
            return self._models_dir
        return Path(__file__).parent.parent.parent / "models"
    
    def _load_pickle(self, path: Path) -> Any:
        """Загружает объект из pickle файла."""
        if not path.exists():
            raise FileNotFoundError(f"Файл не найден: {path}")
        
        with open(path, 'rb') as f:
            return pickle.load(f)
    
    def _load_calibrator(self, model_info: Dict) -> Optional[Any]:
        """Загружает калибратор."""
        calib_path = model_info.get('calibrator_path')
        if not calib_path:
            return None
        
        calib_file = self._get_models_dir() / calib_path.split('\\')[-1]
        return self._load_pickle(calib_file)
    
    def _load_encoders(self) -> Dict[str, Any]:
        """Загружает энкодеры."""
        # Попытка загрузить последние энкодеры
        encoders_dir = self._get_models_dir() / "encoders"
        
        if not encoders_dir.exists():
            return {}
        
        # Поиск последнего файла с энкодерами
        encoder_files = list(encoders_dir.glob("encoders_*.pkl"))
        
        if not encoder_files:
            return {}
        
        latest_encoder = max(encoder_files, key=lambda p: p.stat().st_mtime)
        return self._load_pickle(latest_encoder)
    
    def _extract_metadata(
        self,
        model: Any
    ) -> Tuple[List[str], int]:
        """Извлекает метаданные из модели."""
        feature_cols = []
        best_iteration = 1000
        
        if isinstance(model, dict):
            feature_cols = model.get('feature_cols', [])
            best_iteration = model.get('best_iteration', 1000)
        elif hasattr(model, 'feature_name'):
            feature_cols = model.feature_name()
        elif hasattr(model, 'booster_') and hasattr(model.booster_, 'feature_name'):
            feature_cols = model.booster_.feature_name()
        
        return feature_cols, best_iteration
