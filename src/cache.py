# -*- coding: utf-8 -*-
"""
cache.py v1.0
🔧 КЭШИРОВАНИЕ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ
Назначение:
  - Кэширование признаков в Parquet
  - Кэширование результатов предсказаний
  - Автоматическая инвалидация по TTL

Использование:
    from src.cache import FeatureCache
    
    cache = FeatureCache(cache_dir="data/cache", ttl_days=7)
    
    # Загрузка с кэшированием
    features = cache.get_or_compute(
        key="training_features_2024",
        compute_fn=load_training_features,
        engine=engine
    )
"""

import hashlib
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


# ==============================================================================
# КЛАССЫ
# ==============================================================================

class FeatureCache:
    """
    Менеджер кэширования признаков с автоматической инвалидацией.
    """
    
    def __init__(
        self,
        cache_dir: Path,
        ttl_days: int = 7,
        compression: str = 'snappy'
    ):
        """
        Args:
            cache_dir: Директория для кэша
            ttl_days: Время жизни кэша в днях
            compression: Тип сжатия Parquet
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_days = ttl_days
        self.compression = compression
        
        # Метаданные кэша
        self.meta_file = self.cache_dir / "_cache_meta.json"
        self.meta = self._load_meta()
    
    def _load_meta(self) -> Dict:
        """Загружает метаданные кэша"""
        if self.meta_file.exists():
            try:
                with open(self.meta_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось загрузить метаданные кэша: {e}")
        return {}
    
    def _save_meta(self):
        """Сохраняет метаданные кэша"""
        with open(self.meta_file, 'w', encoding='utf-8') as f:
            json.dump(self.meta, f, indent=2, ensure_ascii=False)
    
    def _get_cache_path(self, key: str) -> Path:
        """Генерирует путь к файлу кэша по ключу"""
        # Хешируем ключ для безопасного имени файла
        key_hash = hashlib.md5(key.encode()).hexdigest()[:16]
        safe_name = f"{key}_{key_hash}.parquet"
        return self.cache_dir / safe_name
    
    def _is_expired(self, key: str) -> bool:
        """Проверяет, истёк ли срок жизни кэша"""
        if key not in self.meta:
            return True
        
        cached_at = datetime.fromisoformat(self.meta[key]['cached_at'])
        expiry = cached_at + timedelta(days=self.ttl_days)
        
        return datetime.now() > expiry
    
    def _is_stale(self, key: str, data_source_hash: Optional[str] = None) -> bool:
        """
        Проверяет, устарел ли кэш относительно источника данных.
        
        Args:
            key: Ключ кэша
            data_source_hash: Хэш источника данных (опционально)
        """
        if key not in self.meta:
            return True
        
        if data_source_hash is None:
            return False
        
        cached_hash = self.meta[key].get('data_source_hash')
        return cached_hash != data_source_hash
    
    def get(
        self,
        key: str,
        data_source_hash: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Получает данные из кэша.
        
        Args:
            key: Ключ кэша
            data_source_hash: Хэш источника данных для проверки актуальности
        
        Returns:
            DataFrame или None если кэш отсутствует/устарел
        """
        # Проверка на устаревание
        if self._is_expired(key):
            logger.debug(f"🗑️ Кэш '{key}' устарел по TTL")
            self.delete(key)
            return None
        
        if self._is_stale(key, data_source_hash):
            logger.debug(f"🗑️ Кэш '{key}' устарел относительно источника")
            self.delete(key)
            return None
        
        # Загрузка из файла
        cache_path = self._get_cache_path(key)
        
        if not cache_path.exists():
            logger.debug(f"🗑️ Кэш '{key}' не найден на диске")
            return None
        
        try:
            df = pd.read_parquet(cache_path)
            logger.info(f"✅ Загружено из кэша: {key} ({len(df):,} строк)")
            return df
        except Exception as e:
            logger.warning(f"⚠️ Ошибка чтения кэша '{key}': {e}")
            self.delete(key)
            return None
    
    def set(
        self,
        key: str,
        df: pd.DataFrame,
        data_source_hash: Optional[str] = None
    ):
        """
        Сохраняет данные в кэш.
        
        Args:
            key: Ключ кэша
            df: DataFrame для сохранения
            data_source_hash: Хэш источника данных
        """
        cache_path = self._get_cache_path(key)
        
        try:
            # Сохранение в Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                cache_path,
                compression=self.compression
            )
            
            # Обновление метаданных
            self.meta[key] = {
                'cached_at': datetime.now().isoformat(),
                'rows': len(df),
                'columns': list(df.columns),
                'file_size_mb': cache_path.stat().st_size / (1024 * 1024),
                'data_source_hash': data_source_hash
            }
            self._save_meta()
            
            logger.info(
                f"💾 Сохранено в кэш: {key} "
                f"({len(df):,} строк, {self.meta[key]['file_size_mb']:.2f} МБ)"
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка записи в кэш '{key}': {e}")
            raise
    
    def get_or_compute(
        self,
        key: str,
        compute_fn: Callable,
        data_source_hash: Optional[str] = None,
        force_recompute: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """
        Получает данные из кэша или вычисляет заново.
        
        Args:
            key: Ключ кэша
            compute_fn: Функция для вычисления данных
            data_source_hash: Хэш источника данных
            force_recompute: Принудительное перевычисление
            **kwargs: Аргументы для compute_fn
        
        Returns:
            DataFrame с данными
        """
        # Проверка кэша
        if not force_recompute:
            cached = self.get(key, data_source_hash)
            if cached is not None:
                return cached
        
        # Вычисление заново
        logger.info(f"🔄 Вычисление: {key}...")
        start_time = datetime.now()
        
        df = compute_fn(**kwargs)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"   ⏱️ Время вычисления: {elapsed:.1f} сек")
        
        # Сохранение в кэш
        self.set(key, df, data_source_hash)
        
        return df
    
    def delete(self, key: str) -> bool:
        """Удаляет кэш по ключу"""
        cache_path = self._get_cache_path(key)
        
        if cache_path.exists():
            cache_path.unlink()
            logger.debug(f"🗑️ Удалён кэш: {key}")
        
        if key in self.meta:
            del self.meta[key]
            self._save_meta()
        
        return True
    
    def clear(self):
        """Очищает весь кэш"""
        for f in self.cache_dir.glob("*.parquet"):
            f.unlink()
        
        self.meta = {}
        self._save_meta()
        
        logger.info("🗑️ Кэш полностью очищен")
    
    def stats(self) -> Dict:
        """Возвращает статистику кэша"""
        total_size = sum(
            f.stat().st_size for f in self.cache_dir.glob("*.parquet")
        )
        
        return {
            'entries': len(self.meta),
            'total_size_mb': total_size / (1024 * 1024),
            'oldest_entry': min(
                (v['cached_at'] for v in self.meta.values()),
                default=None
            ),
            'newest_entry': max(
                (v['cached_at'] for v in self.meta.values()),
                default=None
            )
        }


class PredictionCache(FeatureCache):
    """
    Кэш для предсказаний модели с коротким TTL.
    """
    
    def __init__(self, cache_dir: Path, ttl_hours: int = 24):
        super().__init__(cache_dir, ttl_days=ttl_hours / 24)
    
    def get_predictions(
        self,
        client_id: str,
        model_hash: str
    ) -> Optional[pd.DataFrame]:
        """Получает предсказания для клиента"""
        key = f"predictions_{client_id}"
        return self.get(key, data_source_hash=model_hash)
    
    def set_predictions(
        self,
        client_id: str,
        predictions: pd.DataFrame,
        model_hash: str
    ):
        """Сохраняет предсказания для клиента"""
        key = f"predictions_{client_id}"
        self.set(key, predictions, data_source_hash=model_hash)


# ==============================================================================
# DECORATORS
# ==============================================================================

def cache_result(
    cache: FeatureCache,
    key_prefix: str = '',
    ttl_days: Optional[int] = None
):
    """
    Декоратор для кэширования результатов функций.
    
    Usage:
        cache = FeatureCache("data/cache")
        
        @cache_result(cache, key_prefix="training_data", ttl_days=7)
        def load_training_data(engine):
            # Дорогостоящая операция
            return df
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            # Генерация ключа
            key_parts = [key_prefix, func.__name__]
            
            # Хэш аргументов
            args_hash = hashlib.md5(
                json.dumps((args, kwargs), default=str).encode()
            ).hexdigest()[:16]
            key_parts.append(args_hash)
            
            key = '_'.join(filter(None, key_parts))
            
            # Попытка загрузки из кэша
            cached = cache.get(key)
            if cached is not None:
                return cached
            
            # Вычисление
            result = func(*args, **kwargs)
            
            # Сохранение
            cache.set(key, result)
            
            return result
        
        return wrapper
    return decorator


# ==============================================================================
# CLI
# ==============================================================================

if __name__ == "__main__":
    import sys
    
    cache_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/cache")
    
    cache = FeatureCache(cache_dir)
    
    print("="*70)
    print("📊 Статистика кэша")
    print("="*70)
    
    stats = cache.stats()
    print(f"Записей: {stats['entries']}")
    print(f"Размер: {stats['total_size_mb']:.2f} МБ")
    print(f"Старейшая запись: {stats['oldest_entry']}")
    print(f"Новейшая запись: {stats['newest_entry']}")
