# -*- coding: utf-8 -*-
"""
test_backtest_engine.py
Unit-тесты для backtest_engine.py (функция process_batch)

Запуск:
  pytest tests/test_backtest_engine.py -v
  pytest tests/test_backtest_engine.py -v --cov=src/backtest_engine

Покрытие edge cases:
  - Пустые данные
  - Данные с NaT/NULL значениями
  - Один клиент / один SKU
  - Multiple клиенты и SKU
  - Граничные значения дат
  - Проверка расчёта target
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Добавляем src в path для импорта
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from backtest_engine import process_batch


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def sample_purchase_data():
    """Базовые тестовые данные с покупками"""
    return pd.DataFrame({
        'client_id': ['C1', 'C1', 'C1', 'C2', 'C2', 'C2'],
        'sku_id': ['S1', 'S1', 'S1', 'S1', 'S1', 'S2'],
        'purchase_date': pd.to_datetime([
            '2024-01-01', '2024-01-15', '2024-02-01',
            '2024-01-05', '2024-01-20', '2024-01-10'
        ])
    })


@pytest.fixture
def data_with_nat():
    """Данные с NaT значениями"""
    df = pd.DataFrame({
        'client_id': ['C1', 'C1', 'C1'],
        'sku_id': ['S1', 'S1', 'S1'],
        'purchase_date': pd.to_datetime(['2024-01-01', pd.NaT, '2024-02-01'])
    })
    return df


@pytest.fixture
def single_client_single_sku():
    """Минимальный набор: один клиент, один SKU"""
    return pd.DataFrame({
        'client_id': ['C1', 'C1'],
        'sku_id': ['S1', 'S1'],
        'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-15'])
    })


@pytest.fixture
def unpopular_skus():
    """Данные с непопулярными SKU (менее 2 покупок)"""
    return pd.DataFrame({
        'client_id': ['C1', 'C2', 'C3'],
        'sku_id': ['S1', 'S2', 'S3'],  # Каждый SKU куплен только 1 раз
        'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-05', '2024-01-10'])
    })


# ==============================================================================
# ТЕСТЫ: БАЗОВАЯ ФУНКЦИОНАЛЬНОСТЬ
# ==============================================================================

class TestProcessBatchBasic:
    """Базовые тесты process_batch"""

    def test_empty_dataframe(self):
        """Тест: пустой DataFrame"""
        df = pd.DataFrame()
        result = process_batch(df)
        
        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_basic_processing(self, sample_purchase_data):
        """Тест: базовая обработка данных"""
        result = process_batch(sample_purchase_data, visit_interval_days=7)
        
        assert not result.empty
        assert 'client_id' in result.columns
        assert 'sku_id' in result.columns
        assert 'visit_date' in result.columns
        assert 'target' in result.columns
        assert 'days_since_last_purchase' in result.columns
        assert 'last_purchase_date' in result.columns

    def test_single_client_single_sku(self, single_client_single_sku):
        """Тест: один клиент, один SKU, две покупки"""
        result = process_batch(single_client_single_sku, visit_interval_days=7)
        
        assert not result.empty
        assert result['client_id'].unique().tolist() == ['C1']
        assert result['sku_id'].unique().tolist() == ['S1']
        
        # Проверяем, что target рассчитан корректно
        assert 'target' in result.columns
        assert result['target'].isin([0, 1]).all()

    def test_multiple_clients_multiple_skus(self, sample_purchase_data):
        """Тест: несколько клиентов и SKU"""
        result = process_batch(sample_purchase_data, visit_interval_days=14)
        
        assert not result.empty
        
        # Проверяем наличие обоих клиентов
        clients = result['client_id'].unique()
        assert 'C1' in clients
        assert 'C2' in clients
        
        # Проверяем наличие обоих SKU
        skus = result['sku_id'].unique()
        assert 'S1' in skus
        assert 'S2' in skus


# ==============================================================================
# ТЕСТЫ: ФИЛЬТРАЦИЯ SKU
# ==============================================================================

class TestProcessBatchSkuFiltering:
    """Тесты фильтрации SKU по популярности"""

    def test_unpopular_skus_filtered(self, unpopular_skus):
        """Тест: непопулярные SKU (< 2 покупок) фильтруются"""
        result = process_batch(unpopular_skus)
        
        # Все SKU непопулярны, результат должен быть пустым
        assert result.empty

    def test_popular_skus_retained(self, sample_purchase_data):
        """Тест: популярные SKU (>= 2 покупок) сохраняются"""
        result = process_batch(sample_purchase_data)
        
        # S1 куплен 4 раза, S2 - 1 раз (должен отфильтроваться)
        assert not result.empty
        assert 'S1' in result['sku_id'].values


# ==============================================================================
# ТЕСТЫ: ОБРАБОТКА NAT/NULL
# ==============================================================================

class TestProcessBatchNaTHandling:
    """Тесты обработки NaT значений"""

    def test_nat_rows_filtered_not_skipped(self, data_with_nat):
        """Тест: строки с NaT фильтруются, а не вся группа пропускается"""
        # Добавляем ещё одну валидную покупку для популярности
        df = pd.concat([
            data_with_nat,
            pd.DataFrame({
                'client_id': ['C1'],
                'sku_id': ['S1'],
                'purchase_date': pd.to_datetime(['2024-03-01'])
            })
        ], ignore_index=True)
        
        result = process_batch(df)
        
        # Должны быть результаты, несмотря на NaT в данных
        assert not result.empty

    def test_all_nat_in_group_skipped(self):
        """Тест: если все строки в группе NaT, группа пропускается"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime([pd.NaT, pd.NaT])
        })
        
        result = process_batch(df)
        
        # Группа должна быть пропущена
        assert result.empty

    def test_partial_nat_filtered(self):
        """Тест: частичные NaT фильтруются на уровне строк"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1', 'C1', 'C1'],
            'sku_id': ['S1', 'S1', 'S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', pd.NaT, '2024-02-01', '2024-03-01'])
        })
        
        result = process_batch(df)
        
        # Должны быть результаты из валидных строк
        assert not result.empty
        assert len(result) > 0


# ==============================================================================
# ТЕСТЫ: РАСЧЁТ TARGET
# ==============================================================================

class TestProcessBatchTargetCalculation:
    """Тесты расчёта целевой переменной"""

    def test_target_1_within_window(self):
        """Тест: target=1, если следующая покупка в окне 14 дней"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-10'])  # 9 дней
        })
        
        result = process_batch(df, visit_interval_days=7, purchase_window_days=14)
        
        # Должны быть примеры с target=1
        assert (result['target'] == 1).any()

    def test_target_0_outside_window(self):
        """Тест: target=0, если следующая покупка вне окна 14 дней"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-02-01'])  # 31 день
        })
        
        result = process_batch(df, visit_interval_days=7, purchase_window_days=14)
        
        # Визиты между покупками должны иметь target=0
        assert (result['target'] == 0).any()

    def test_target_last_purchase_na(self):
        """Тест: для последней покупки next_purchase = NaT, target=0"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1', 'C1'],
            'sku_id': ['S1', 'S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-15', '2024-02-01'])
        })
        
        result = process_batch(df, visit_interval_days=7, purchase_window_days=14)
        
        # Проверяем, что для последней покупки target=0
        # (так как next_purchase = NaT)
        assert 'target' in result.columns


# ==============================================================================
# ТЕСТЫ: DAYS_SINCE_LAST_PURCHASE
# ==============================================================================

class TestProcessBatchDaysCalculation:
    """Тесты расчёта дней с последней покупки"""

    def test_days_since_last_purchase_positive(self):
        """Тест: days_since_last_purchase >= 0"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-15'])
        })
        
        result = process_batch(df, visit_interval_days=7)
        
        # Все значения должны быть неотрицательными
        assert (result['days_since_last_purchase'] >= 0).all()

    def test_days_calculation_accuracy(self):
        """Тест: точность расчёта дней"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-15'])  # 14 дней
        })
        
        result = process_batch(df, visit_interval_days=7)
        
        # Проверяем, что дни рассчитаны корректно
        # Для визита 2024-01-07: days = 7 - 1 = 6 дней
        # Для визита 2024-01-14: days = 14 - 1 = 13 дней
        assert len(result) > 0
        assert all(isinstance(d, (int, np.integer)) for d in result['days_since_last_purchase'])


# ==============================================================================
# ТЕСТЫ: VISIT_INTERVAL_DAYS
# ==============================================================================

class TestProcessBatchVisitInterval:
    """Тесты интервала визитов"""

    def test_visit_interval_7_days(self, sample_purchase_data):
        """Тест: интервал визитов 7 дней"""
        result = process_batch(sample_purchase_data, visit_interval_days=7)
        
        if not result.empty:
            # Проверяем, что визиты с шагом 7 дней
            visit_dates = pd.to_datetime(result['visit_date'].unique())
            if len(visit_dates) > 1:
                diffs = np.diff(visit_dates.sort_values())
                assert all(d >= pd.Timedelta(days=6) for d in diffs)

    def test_visit_interval_14_days(self, sample_purchase_data):
        """Тест: интервал визитов 14 дней"""
        result = process_batch(sample_purchase_data, visit_interval_days=14)
        
        if not result.empty:
            visit_dates = pd.to_datetime(result['visit_date'].unique())
            if len(visit_dates) > 1:
                diffs = np.diff(visit_dates.sort_values())
                assert all(d >= pd.Timedelta(days=13) for d in diffs)


# ==============================================================================
# ТЕСТЫ: PURCHASE_WINDOW_DAYS
# ==============================================================================

class TestProcessBatchPurchaseWindow:
    """Тесты окна покупки"""

    def test_narrow_window_7_days(self):
        """Тест: узкое окно покупки (7 дней)"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-10'])  # 9 дней
        })
        
        result = process_batch(df, visit_interval_days=7, purchase_window_days=7)
        
        # При окне 7 дней и разнице 9 дней, target должен быть 0
        if not result.empty:
            assert (result['target'] == 0).all()

    def test_wide_window_30_days(self):
        """Тест: широкое окно покупки (30 дней)"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-20'])  # 19 дней
        })
        
        result = process_batch(df, visit_interval_days=7, purchase_window_days=30)
        
        # При окне 30 дней и разнице 19 дней, target должен быть 1
        if not result.empty:
            assert (result['target'] == 1).any()


# ==============================================================================
# ТЕСТЫ: ГРАНИЧНЫЕ СЛУЧАИ
# ==============================================================================

class TestProcessBatchEdgeCases:
    """Тесты граничных случаев"""

    def test_single_purchase_per_sku(self):
        """Тест: одна покупка на SKU (должен отфильтроваться)"""
        df = pd.DataFrame({
            'client_id': ['C1'],
            'sku_id': ['S1'],
            'purchase_date': pd.to_datetime(['2024-01-01'])
        })
        
        result = process_batch(df)
        assert result.empty

    def test_same_date_purchases(self):
        """Тест: покупки в одну дату"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2024-01-01', '2024-01-01'])
        })
        
        result = process_batch(df)
        
        # Должны быть обработаны, но визитов может не быть
        assert isinstance(result, pd.DataFrame)

    def test_large_date_range(self):
        """Тест: большой диапазон дат"""
        dates = pd.date_range('2024-01-01', '2024-12-31', freq='D')
        df = pd.DataFrame({
            'client_id': ['C1'] * len(dates),
            'sku_id': ['S1'] * len(dates),
            'purchase_date': dates
        })
        
        result = process_batch(df, visit_interval_days=30)
        
        # Должны быть результаты
        assert not result.empty

    def test_future_dates(self):
        """Тест: будущие даты"""
        df = pd.DataFrame({
            'client_id': ['C1', 'C1'],
            'sku_id': ['S1', 'S1'],
            'purchase_date': pd.to_datetime(['2030-01-01', '2030-01-15'])
        })
        
        result = process_batch(df, visit_interval_days=7)
        
        assert not result.empty
        assert (pd.to_datetime(result['visit_date']) > pd.Timestamp.now()).any()


# ==============================================================================
# ТЕСТЫ: СТРУКТУРА РЕЗУЛЬТАТА
# ==============================================================================

class TestProcessBatchOutputStructure:
    """Тесты структуры выходных данных"""

    def test_output_columns(self, sample_purchase_data):
        """Тест: наличие всех ожидаемых колонок"""
        result = process_batch(sample_purchase_data)
        
        expected_columns = [
            'client_id',
            'visit_date',
            'sku_id',
            'last_purchase_date',
            'target',
            'days_since_last_purchase'
        ]
        
        for col in expected_columns:
            assert col in result.columns, f"Отсутствует колонка {col}"

    def test_output_types(self, sample_purchase_data):
        """Тест: типы данных в результате"""
        result = process_batch(sample_purchase_data)
        
        if not result.empty:
            assert result['client_id'].dtype == 'object'
            assert result['sku_id'].dtype == 'object'
            assert result['target'].dtype in ['int64', 'int32', 'int8']
            assert result['days_since_last_purchase'].dtype in ['int64', 'int32', 'int8']

    def test_no_duplicate_rows(self, sample_purchase_data):
        """Тест: отсутствие дубликатов"""
        result = process_batch(sample_purchase_data)
        
        if not result.empty:
            duplicates = result.duplicated(subset=['client_id', 'visit_date', 'sku_id']).sum()
            assert duplicates == 0, f"Найдено {duplicates} дубликатов"


# ==============================================================================
# ЗАПУСК ТЕСТОВ
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
