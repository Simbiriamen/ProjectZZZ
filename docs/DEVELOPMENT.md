# Руководство разработчика ProjectZZZ

## Содержание

1. [Настройка окружения](#настройка-окружения)
2. [Структура проекта](#структура-проекта)
3. [Стиль кода](#стиль-кода)
4. [Тестирование](#тестирование)
5. [Отладка](#отладка)
6. [Добавление новых функций](#добавление-новых-функций)
7. [Работа с Git](#работа-s-git)
8. [CI/CD](#cicd)

---

## Настройка окружения

### Минимальные требования

- Python 3.10+
- PostgreSQL 14+
- 8 GB RAM (рекомендуется 16 GB для обучения)
- 50 GB свободного места на диске

### Быстрый старт

```bash
# Клонирование репозитория
git clone https://github.com/Simbiriamen/ProjectZZZ.git
cd ProjectZZZ

# Запуск скрипта настройки
# Linux/Mac:
bash scripts/setup.sh

# Windows:
scripts\setup.bat
```

### Ручная настройка

#### 1. Создание виртуального окружения

```bash
python -m venv venv

# Активация
# Linux/Mac:
source venv/bin/activate

# Windows:
venv\Scripts\activate
```

#### 2. Установка зависимостей

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

#### 3. Настройка переменных окружения

```bash
cp .env.example .env
```

**Заполните `.env`:**
```ini
DB_HOST=localhost
DB_PORT=5432
DB_NAME=project_zzz_db
DB_USER=postgres
DB_PASSWORD=your_secure_password

AB_TEST_ENABLED=true
AB_TEST_RATIO=0.5

LOG_LEVEL=INFO
```

#### 4. Запуск PostgreSQL (Docker)

```bash
docker-compose up -d db
```

Или установите PostgreSQL локально и создайте базу данных:

```sql
CREATE DATABASE project_zzz_db;
```

#### 5. Инициализация схемы БД

```bash
psql -U postgres -d project_zzz_db -f config/schema.sql
```

#### 6. Проверка установки

```bash
pytest tests/test_security.py -v
```

---

## Структура проекта

```
ProjectZZZ/
├── config/                     # Конфигурация
│   ├── config.yaml            # Основной конфиг
│   ├── schema.sql             # Схема БД
│   └── sql/                   # SQL скрипты
│       ├── add_minmax_columns.sql
│       └── add_missing_columns.sql
│
├── data/                       # Данные
│   ├── raw/                   # Исходные файлы Excel
│   ├── processed/             # Обработанные данные (Parquet)
│   ├── output/                # Результаты (рекомендации)
│   └── cache/                 # Кэш признаков
│
├── models/                     # ML модели
│   ├── model_controller.py    # Диспетчер моделей
│   ├── model_lightgbm_v1.py   # Базовая модель
│   ├── model_registry.json    # Реестр версий
│   ├── encoders_*.pkl         # Кодировщики
│   ├── calibrator_*.pkl       # Калибраторы
│   └── model_*.pkl            # Бинарные файлы
│
├── src/                        # Исходный код
│   ├── __init__.py
│   ├── config_loader.py       # Загрузка конфига
│   ├── database.py            # Работа с БД
│   ├── services.py            # Бизнес-логика
│   ├── backtest_engine.py     # Backtesting
│   ├── generate_recommendations.py
│   ├── load_references.py     # ETL: справочники
│   ├── load_sales.py          # ETL: продажи
│   ├── load_stocks.py         # ETL: остатки
│   ├── evaluate_ab.py         # A/B тесты
│   ├── features_cache.py      # Кэш признаков
│   └── ...
│
├── tests/                      # Тесты
│   ├── test_security.py
│   ├── test_backtest_engine.py
│   └── test_model_validation.py
│
├── docs/                       # Документация
│   ├── ARCHITECTURE.md        # Архитектура
│   ├── API.md                 # API документация
│   ├── DEVELOPMENT.md         # Это руководство
│   └── logs/                  # Логи выполнения
│
├── scripts/                    # Скрипты настройки
│   ├── setup.sh
│   └── setup.bat
│
├── docker-compose.yml          # Docker конфигурация
├── Dockerfile
├── requirements.txt
├── pytest.ini
├── .env.example
├── .gitignore
└── README.md
```

---

## Стиль кода

### Общие принципы

1. **PEP 8** — соблюдайте стандарты Python
2. **Типизация** — используйте type hints
3. **Docstrings** — документируйте функции и классы
4. **Логирование** — логируйте все важные события

### Пример оформления функции

```python
# -*- coding: utf-8 -*-
"""
Модуль: generate_recommendations.py
Назначение: Генерация рекомендаций для клиентов
"""

import logging
from typing import List, Dict, Optional
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def generate_for_client(
    client_id: str,
    visit_date: pd.Timestamp,
    model: Any,
    min_probability: float = 0.2
) -> List[Dict[str, Any]]:
    """
    Генерирует рекомендации для одного клиента.
    
    Args:
        client_id: Уникальный идентификатор клиента
        visit_date: Дата планируемого визита
        model: Обученная ML модель
        min_probability: Минимальная вероятность покупки
        
    Returns:
        Список рекомендаций формата:
        [{"sku_id": str, "probability": float, "type": str}, ...]
        
    Raises:
        ValueError: Если client_id некорректен
        ModelNotLoadedError: Если модель не загружена
        
    Example:
        >>> recs = generate_for_client("C123", pd.Timestamp("2024-03-25"), model)
        >>> len(recs)
        5
    """
    # Валидация входных данных
    if not client_id or len(client_id) > 256:
        raise ValueError("Некорректный client_id")
    
    if not isinstance(visit_date, pd.Timestamp):
        raise TypeError("visit_date должен быть pd.Timestamp")
    
    logger.info(f"Генерация рекомендаций для клиента {client_id}")
    
    # Основная логика
    recommendations = []
    
    # ... код генерации ...
    
    logger.info(f"Сгенерировано {len(recommendations)} рекомендаций")
    
    return recommendations
```

### Логирование

**Уровни логирования:**

| Уровень | Когда использовать |
|---------|-------------------|
| `DEBUG` | Отладочная информация (значения переменных) |
| `INFO` | Штатное выполнение операций |
| `WARNING` | Некритические проблемы (fallback, пропуск файлов) |
| `ERROR` | Ошибки выполнения (неудачная загрузка, сбой предсказания) |
| `CRITICAL` | Критические ошибки (деградация модели, потеря данных) |

**Пример:**

```python
logger.info("✅ Загрузка модели завершена")
logger.warning(f"⚠️ Fallback применён для клиента {client_id}")
logger.error(f"❌ Ошибка загрузки файла: {file_path}")
logger.critical("🚨 Критическая деградация метрик!")
```

### Обработка ошибок

```python
try:
    result = risky_operation()
except FileNotFoundError as e:
    logger.error(f"Файл не найден: {e}")
    raise
except ValueError as e:
    logger.warning(f"Ошибка валидации: {e}")
    return default_value
except Exception as e:
    logger.critical(f"Неожиданная ошибка: {e}", exc_info=True)
    raise
```

---

## Тестирование

### Запуск тестов

```bash
# Все тесты
pytest tests/ -v

# С покрытием
pytest tests/ -v --cov=src --cov-report=html

# Только unit-тесты
pytest tests/ -v -m unit

# Исключая медленные тесты
pytest tests/ -v -m "not slow"

# Один файл
pytest tests/test_security.py -v

# Одна функция
pytest tests/test_security.py::test_sql_injection_protection -v
```

### Написание тестов

**Структура теста:**

```python
# -*- coding: utf-8 -*-
"""
test_example.py
Unit-тесты для модуля example.py
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.example import example_function


@pytest.fixture
def sample_data():
    """Фикстура с тестовыми данными"""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    })


class TestExampleFunction:
    """Тесты для example_function"""
    
    def test_normal_case(self, sample_data):
        """Тест нормального случая"""
        result = example_function(sample_data)
        assert len(result) == 3
        assert all(v > 0 for v in result['value'])
    
    def test_empty_input(self):
        """Тест пустого ввода"""
        empty_df = pd.DataFrame()
        with pytest.raises(ValueError):
            example_function(empty_df)
    
    @pytest.mark.edge_case
    def test_large_values(self):
        """Тест больших значений"""
        large_df = pd.DataFrame({
            'id': [1],
            'value': [1e10]
        })
        result = example_function(large_df)
        assert result['value'][0] == 1e10
```

### Маркеры тестов

```python
@pytest.mark.unit          # Unit-тесты
@pytest.mark.integration   # Интеграционные тесты
@pytest.mark.slow          # Медленные тесты
@pytest.mark.edge_case     # Граничные случаи
```

### Покрытие кода

Целевое покрытие: **≥ 70%**

```bash
# Проверка покрытия
pytest --cov=src --cov-report=term-missing

# HTML отчёт
pytest --cov=src --cov-report=html:docs/coverage
# Открыть: docs/coverage/index.html
```

---

## Отладка

### Логирование

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
```

### Отладка в Docker

```bash
# Запуск контейнера с интерактивной оболочкой
docker-compose run --rm recommendations /bin/bash

# Просмотр логов
docker-compose logs -f recommendations

# Доступ к БД
docker-compose exec db psql -U postgres -d project_zzz_db
```

### Профилирование

```python
import cProfile
import pstats

def profile_function():
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Вызов функции
    result = expensive_function()
    
    profiler.disable()
    
    # Вывод статистики
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(10)
    
    return result
```

### Анализ производительности SQL

```sql
-- Включение анализа запросов
EXPLAIN ANALYZE
SELECT * FROM purchases
WHERE client_id = 'C123'
  AND purchase_date >= CURRENT_DATE - INTERVAL '90 days';

-- Поиск медленных запросов
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;
```

---

## Добавление новых функций

### Чеклист разработки

1. [ ] Создать ветку Git (`feature/xxx`)
2. [ ] Написать тесты
3. [ ] Реализовать функциональность
4. [ ] Проверить покрытие (>70%)
5. [ ] Обновить документацию
6. [ ] Пройти code review
7. [ ] Слить в main

### Добавление новой модели

**Шаг 1: Создание файла модели**

```python
# models/model_new_v1.py
# -*- coding: utf-8 -*-
"""
model_new_v1.py
Новая улучшенная модель
"""

import logging
from typing import Tuple, Dict, Any
import lightgbm as lgb
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def train(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series
) -> Tuple[Any, Any, Dict, List[str], int]:
    """
    Обучает новую модель.
    
    Returns:
        (model, calibrator, metrics, feature_cols, best_iteration)
    """
    # Реализация обучения
    ...
    
    return model, calibrator, metrics, feature_cols, best_iter


def predict(model: Any, X: pd.DataFrame) -> np.ndarray:
    """Делает предсказания"""
    return model.predict(X)
```

**Шаг 2: Регистрация в реестре**

```python
from models.model_controller import ModelController
from pathlib import Path

controller = ModelController(
    models_dir=Path("models"),
    registry_path=Path("models/model_registry.json")
)

controller.register_model(
    name='model_new_v1',
    metrics=metrics,
    auto_promote=True,
    status='staging'
)
```

**Шаг 3: A/B тестирование**

```python
# Запуск A/B теста через evaluate_ab.py
python src/evaluate_ab.py --model=model_new_v1 --duration=14
```

**Шаг 4: Продвижение в production**

```python
if controller.evaluate_promotion('model_new_v1', 'model_lightgbm_v1'):
    controller.promote_to_production('model_new_v1', reason='ab_test_success')
```

### Добавление нового признака

**Шаг 1: Генерация в backtest_engine.py**

```python
def calculate_new_feature(df: pd.DataFrame) -> pd.Series:
    """Расчёт нового признака"""
    return df.groupby('client_id')['value'].transform('mean')
```

**Шаг 2: Использование в generate_recommendations.py**

```python
def prepare_features(client_data: pd.DataFrame) -> pd.DataFrame:
    features = pd.DataFrame()
    features['new_feature'] = calculate_new_feature(client_data)
    return features
```

**Шаг 3: Обновление документации**

Добавьте признак в `docs/ARCHITECTURE.md` в раздел "Признаки (Features)".

**Шаг 4: Переобучение модели**

```bash
python src/backtest_engine.py
python models/model_lightgbm_v1.py
```

---

## Работа с Git

### Ветвление

```bash
# Создание новой ветки
git checkout -b feature/new-model

# Или для исправлений
git checkout -b fix/sql-injection

# Для релизов
git checkout -b release/v3.3
```

### Коммиты

**Формат сообщений:**

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Типы коммитов:**

| Тип | Описание |
|-----|----------|
| `feat` | Новая функциональность |
| `fix` | Исправление ошибки |
| `docs` | Изменения в документации |
| `style` | Форматирование, пробелы |
| `refactor` | Рефакторинг без изменений функциональности |
| `test` | Добавление тестов |
| `chore` | Изменения в сборке, зависимостях |

**Примеры:**

```bash
git commit -m "feat(models): добавить модель LightGBM v2 с новыми признаками"

git commit -m "fix(database): исправить SQL injection в load_sales.py"

git commit -m "docs(api): обновить примеры использования RecommendationService"
```

### Pull Request

**Чеклист перед PR:**

1. [ ] Все тесты проходят
2. [ ] Покрытие ≥ 70%
3. [ ] Код соответствует стилю
4. [ ] Документация обновлена
5. [ ] Нет конфликтов с main

**Шаблон описания PR:**

```markdown
## Описание
Краткое описание изменений.

## Тип изменений
- [ ] Новая функциональность
- [ ] Исправление ошибки
- [ ] Рефакторинг
- [ ] Документация

## Тестирование
Описание проведённых тестов.

## Чеклист
- [ ] Код соответствует PEP 8
- [ ] Добавлены unit-тесты
- [ ] Обновлена документация
- [ ] Проверено локально
```

---

## CI/CD

### GitHub Actions

Конфигурация в `.github/workflows/`:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:14
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
    
    - name: Run tests
      run: |
        pytest tests/ -v --cov=src
```

### Автоматические проверки

- ✅ Линтинг (flake8, black)
- ✅ Тесты (pytest)
- ✅ Покрытие кода
- ✅ Сборка Docker

---

## Развёртывание

### Локальное

```bash
# Установка
pip install -r requirements.txt

# Настройка
cp .env.example .env
# Заполнить переменными

# Запуск
python src/generate_recommendations.py
```

### Docker

```bash
# Сборка
docker-compose build

# Запуск
docker-compose up -d db recommendations

# Обучение
docker-compose --profile training up training

# Просмотр логов
docker-compose logs -f recommendations
```

### Production

**Требования:**

- PostgreSQL с репликацией
- Резервное копирование БД ежедневно
- Мониторинг метрик модели
- Alerting при деградации

**Команды развёртывания:**

```bash
# Обновление кода
git pull origin main

# Миграции БД
psql -U postgres -d project_zzz_db -f config/schema.sql

# Перезапуск сервисов
docker-compose restart recommendations
```

---

## Troubleshooting

### Частые проблемы

#### 1. Ошибка подключения к БД

```
sqlalchemy.exc.OperationalError: could not connect to server
```

**Решение:**
```bash
# Проверить статус PostgreSQL
docker-compose ps

# Перезапустить БД
docker-compose restart db

# Проверить переменные окружения
cat .env | grep DB_
```

#### 2. Модель не найдена

```
FileNotFoundError: Реестр моделей не найден
```

**Решение:**
```bash
# Проверить наличие реестра
ls models/model_registry.json

# Обучить модель
python models/model_lightgbm_v1.py
```

#### 3. Недостаточно памяти

```
MemoryError: Unable to allocate array
```

**Решение:**
- Уменьшить `CLIENT_BATCH_SIZE` в `backtest_engine.py`
- Добавить `max_rows_per_client` ограничение
- Использовать Parquet кэш

#### 4. Долгая загрузка данных

**Решение:**
```bash
# Проверить индексы
psql -U postgres -d project_zzz_db -c "\di"

# Добавить индексы
psql -U postgres -d project_zzz_db -f config/sql/add_missing_indexes.sql
```

---

## Контакты

- **Репозиторий:** https://github.com/Simbiriamen/ProjectZZZ
- **Issues:** https://github.com/Simbiriamen/ProjectZZZ/issues
- **Документация:** `/docs/`
