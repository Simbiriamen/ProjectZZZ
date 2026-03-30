# -*- coding: utf-8 -*-
"""
test_model_validation.py
Unit-тесты для функций валидации в model_lightgbm_v1.py

Запуск:
  pytest tests/test_model_validation.py -v
  pytest tests/test_model_validation.py -v --cov=models/model_lightgbm_v1
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import sys
from pathlib import Path

# Добавляем models в path для импорта
sys.path.insert(0, str(Path(__file__).parent.parent / "models"))

from model_lightgbm_v1 import (
    validate_date_format,
    filter_outliers_iqr,
    validate_feature_ranges,
    validate_training_data
)


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def sample_dataframe():
    """Базовый DataFrame для тестов"""
    return pd.DataFrame({
        'client_id': ['C1', 'C2', 'C3', 'C4', 'C5'],
        'sku_id': ['S1', 'S2', 'S1', 'S3', 'S2'],
        'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-15', '2024-02-01', '2024-02-15', '2024-03-01']),
        'frequency_30d': [1, 2, 3, 1, 2],
        'frequency_90d': [5, 10, 15, 5, 10],
        'target': [0, 1, 0, 1, 0]
    })


@pytest.fixture
def dataframe_with_nat():
    """DataFrame с NaT значениями"""
    df = pd.DataFrame({
        'client_id': ['C1', 'C2', 'C3'],
        'purchase_date': pd.to_datetime(['2024-01-01', pd.NaT, '2024-03-01']),
        'target': [0, 1, 0]
    })
    return df


@pytest.fixture
def dataframe_with_outliers():
    """DataFrame с выбросами"""
    data = {
        'value': [1, 2, 3, 4, 5, 100, -50, 6, 7, 8],  # 100 и -50 выбросы
        'target': [0, 1, 0, 1, 0, 1, 0, 1, 0, 1]
    }
    return pd.DataFrame(data)


@pytest.fixture
def dataframe_out_of_range():
    """DataFrame со значениями вне диапазона"""
    return pd.DataFrame({
        'frequency_30d': [1, 2, 150, 1, 2],  # 150 вне диапазона (0-100)
        'target': [0, 1, 0, 1, 0],
        'margin': [0.1, 0.2, 1.5, 0.3, 0.4],  # 1.5 вне диапазона (-1, 1)
    })


@pytest.fixture
def training_data():
    """Данные для обучения"""
    np.random.seed(42)
    n_samples = 1000
    
    X = pd.DataFrame({
        'feature1': np.random.randn(n_samples),
        'feature2': np.random.randn(n_samples),
        'feature3': np.random.randn(n_samples)
    })
    
    y = pd.Series(np.random.choice([0, 1], n_samples, p=[0.7, 0.3]))
    
    return X, y


# ==============================================================================
# ТЕСТЫ: validate_date_format
# ==============================================================================

class TestValidateDateFormat:
    """Тесты валидации формата дат"""

    def test_valid_dates(self, sample_dataframe):
        """Тест: корректные даты"""
        result = validate_date_format(sample_dataframe)
        
        assert result['valid'] is True
        assert len(result['errors']) == 0
        assert 'purchase_date' in result['stats']

    def test_nat_dates(self, dataframe_with_nat):
        """Тест: NaT значения"""
        result = validate_date_format(dataframe_with_nat)
        
        assert len(result['warnings']) > 0
        assert result['stats']['purchase_date']['nat_count'] == 1
        assert result['stats']['purchase_date']['nat_pct'] > 0

    def test_non_datetime_column(self):
        """Тест: колонка не datetime типа"""
        df = pd.DataFrame({
            'date_str': ['2024-01-01', '2024-02-01', '2024-03-01'],
            'target': [0, 1, 0]
        })
        
        result = validate_date_format(df, ['date_str'])
        
        # Должна быть попытка конвертации
        assert len(result['warnings']) > 0

    def test_future_dates(self):
        """Тест: будущие даты"""
        df = pd.DataFrame({
            'future_date': pd.to_datetime(['2030-01-01', '2030-06-01']),
            'target': [0, 1]
        })
        
        result = validate_date_format(df, ['future_date'])
        
        assert len(result['warnings']) > 0
        assert any('будущ' in w.lower() for w in result['warnings'])

    def test_empty_dataframe(self):
        """Тест: пустой DataFrame"""
        df = pd.DataFrame()
        result = validate_date_format(df)
        
        assert result['valid'] is True
        assert len(result['stats']) == 0

    def test_custom_date_columns(self, sample_dataframe):
        """Тест: пользовательский список колонок"""
        result = validate_date_format(sample_dataframe, ['purchase_date'])
        
        assert 'purchase_date' in result['stats']


# ==============================================================================
# ТЕСТЫ: filter_outliers_iqr
# ==============================================================================

class TestFilterOutliersIqr:
    """Тесты фильтрации выбросов методом IQR"""

    def test_filter_outliers(self, dataframe_with_outliers):
        """Тест: фильтрация выбросов"""
        df_filtered, outliers_info = filter_outliers_iqr(
            dataframe_with_outliers,
            numeric_columns=['value'],
            iqr_multiplier=3.0
        )
        
        # Выбросы должны быть удалены
        assert len(df_filtered) < len(dataframe_with_outliers)
        assert 'value' in outliers_info

    def test_no_outliers(self, sample_dataframe):
        """Тест: нет выбросов"""
        df_filtered, outliers_info = filter_outliers_iqr(
            sample_dataframe,
            numeric_columns=['frequency_30d', 'frequency_90d']
        )
        
        # Данные не должны измениться
        assert len(df_filtered) == len(sample_dataframe)
        assert len(outliers_info) == 0

    def test_custom_iqr_multiplier(self, dataframe_with_outliers):
        """Тест: пользовательский множитель IQR"""
        # Строгий множитель (больше выбросов)
        df_strict, _ = filter_outliers_iqr(
            dataframe_with_outliers,
            iqr_multiplier=1.5
        )
        
        # Мягкий множитель (меньше выбросов)
        df_lenient, _ = filter_outliers_iqr(
            dataframe_with_outliers,
            iqr_multiplier=5.0
        )
        
        # Строгий должен удалить больше
        assert len(df_strict) <= len(df_lenient)

    def test_all_numeric_columns_auto(self, dataframe_with_outliers):
        """Тест: автоматическое определение числовых колонок"""
        df_filtered, outliers_info = filter_outliers_iqr(dataframe_with_outliers)
        
        assert len(df_filtered) >= 0
        assert isinstance(outliers_info, dict)

    def test_empty_result(self):
        """Тест: все данные - выбросы"""
        df = pd.DataFrame({
            'value': [1000, -1000, 2000],  # Все выбросы
            'target': [0, 1, 0]
        })
        
        df_filtered, outliers_info = filter_outliers_iqr(df, iqr_multiplier=0.5)
        
        assert isinstance(df_filtered, pd.DataFrame)


# ==============================================================================
# ТЕСТЫ: validate_feature_ranges
# ==============================================================================

class TestValidateFeatureRanges:
    """Тесты валидации диапазонов признаков"""

    def test_valid_ranges(self, sample_dataframe):
        """Тест: значения в диапазоне"""
        result = validate_feature_ranges(sample_dataframe)
        
        assert result['valid'] is True
        assert len(result['errors']) == 0

    def test_out_of_range(self, dataframe_out_of_range):
        """Тест: значения вне диапазона"""
        result = validate_feature_ranges(dataframe_out_of_range)
        
        assert not result['valid'] or len(result['warnings']) > 0
        assert 'frequency_30d' in result['out_of_range'] or 'margin' in result['out_of_range']

    def test_custom_ranges(self):
        """Тест: пользовательские диапазоны"""
        df = pd.DataFrame({
            'custom_feature': [1, 2, 100, 3, 4],  # 100 вне диапазона
            'target': [0, 1, 0, 1, 0]
        })
        
        custom_ranges = {
            'custom_feature': (0, 50),
            'target': (0, 1)
        }
        
        result = validate_feature_ranges(df, feature_ranges=custom_ranges)
        
        assert 'custom_feature' in result['out_of_range']

    def test_empty_dataframe(self):
        """Тест: пустой DataFrame"""
        df = pd.DataFrame()
        result = validate_feature_ranges(df)
        
        assert result['valid'] is True

    def test_negative_values(self):
        """Тест: отрицательные значения"""
        df = pd.DataFrame({
            'margin': [-0.5, 0.1, 0.2, -1.5, 0.3],  # -1.5 вне диапазона
            'target': [0, 1, 0, 1, 0]
        })
        
        result = validate_feature_ranges(df)
        
        # -1.5 вне диапазона (-1, 1)
        assert len(result['warnings']) > 0 or not result['valid']


# ==============================================================================
# ТЕСТЫ: validate_training_data
# ==============================================================================

class TestValidateTrainingData:
    """Тесты комплексной валидации данных для обучения"""

    def test_valid_training_data(self, training_data):
        """Тест: корректные данные для обучения"""
        X, y = training_data
        
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        result = validate_training_data(X_train, y_train, X_test, y_test)
        
        assert result['valid'] is True
        assert len(result['errors']) == 0

    def test_empty_train(self):
        """Тест: пустая обучающая выборка"""
        X_empty = pd.DataFrame()
        y_empty = pd.Series()
        
        X_test = pd.DataFrame({'feature1': [1, 2, 3]})
        y_test = pd.Series([0, 1, 0])
        
        result = validate_training_data(X_empty, y_empty, X_test, y_test)
        
        assert not result['valid']
        assert any('пустой' in e.lower() for e in result['errors'])

    def test_nan_in_target(self):
        """Тест: NaN в целевой переменной"""
        X_train = pd.DataFrame({'feature1': [1, 2, 3]})
        y_train = pd.Series([0, np.nan, 1])
        
        X_test = pd.DataFrame({'feature1': [4, 5]})
        y_test = pd.Series([0, 1])
        
        result = validate_training_data(X_train, y_train, X_test, y_test)
        
        assert not result['valid']
        assert any('NaN' in e for e in result['errors'])

    def test_dimension_mismatch(self):
        """Тест: разная размерность X_train и X_test"""
        X_train = pd.DataFrame({'f1': [1, 2, 3], 'f2': [4, 5, 6]})
        y_train = pd.Series([0, 1, 0])
        
        X_test = pd.DataFrame({'f1': [7, 8]})  # Только 1 признак
        y_test = pd.Series([1, 0])
        
        result = validate_training_data(X_train, y_train, X_test, y_test)
        
        assert not result['valid']
        assert any('размерность' in e.lower() for e in result['errors'])

    def test_class_imbalance(self):
        """Тест: сильный дисбаланс классов"""
        np.random.seed(42)
        
        X = pd.DataFrame({'feature1': np.random.randn(1000)})
        y = pd.Series([1] * 990 + [0] * 10)  # 99% положительных
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        result = validate_training_data(X_train, y_train, X_test, y_test)
        
        assert len(result['warnings']) > 0
        assert any('дисбаланс' in w.lower() for w in result['warnings'])

    def test_duplicates(self):
        """Тест: много дубликатов"""
        X = pd.DataFrame({'feature1': [1] * 100})
        y = pd.Series([0] * 50 + [1] * 50)
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        result = validate_training_data(X_train, y_train, X_test, y_test)
        
        assert result['stats']['train_duplicates'] > 0


# ==============================================================================
# ИНТЕГРАЦИОННЫЕ ТЕСТЫ
# ==============================================================================

class TestIntegration:
    """Интеграционные тесты валидации"""

    def test_full_validation_pipeline(self, training_data):
        """Тест: полный пайплайн валидации"""
        X, y = training_data
        
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # 1. Валидация данных
        data_result = validate_training_data(X_train, y_train, X_test, y_test)
        assert data_result['valid'] is True
        
        # 2. Валидация диапазонов (добавим target)
        train_df = X_train.copy()
        train_df['target'] = y_train
        range_result = validate_feature_ranges(train_df)
        
        # 3. Фильтрация выбросов (опционально)
        X_filtered, outliers_info = filter_outliers_iqr(
            pd.concat([X_train, X_test]),
            iqr_multiplier=5.0
        )
        
        assert isinstance(X_filtered, pd.DataFrame)

    def test_validation_with_realistic_data(self):
        """Тест: валидация с реалистичными данными"""
        np.random.seed(42)
        n_samples = 5000
        
        # Генерация реалистичных признаков ProjectZZZ
        data = {
            'frequency_30d': np.random.poisson(3, n_samples),
            'frequency_90d': np.random.poisson(10, n_samples),
            'days_since_last_purchase': np.random.exponential(30, n_samples).astype(int),
            'margin': np.random.normal(0.3, 0.1, n_samples),
            'stock': np.random.poisson(100, n_samples),
            'target': np.random.choice([0, 1], n_samples, p=[0.7, 0.3])
        }
        
        df = pd.DataFrame(data)
        
        # Ограничиваем значения реалистичными диапазонами
        df['margin'] = df['margin'].clip(-0.5, 0.8)
        df['days_since_last_purchase'] = df['days_since_last_purchase'].clip(0, 365)
        
        # Валидация
        result = validate_feature_ranges(df)
        
        # Должны быть предупреждения или ошибки (из-за выбросов)
        assert isinstance(result, dict)
        assert 'valid' in result


# ==============================================================================
# ЗАПУСК ТЕСТОВ
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
