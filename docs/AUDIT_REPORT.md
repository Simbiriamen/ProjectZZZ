# 🔍 Аудит кодовой базы ProjectZZZ

**Дата аудита:** 2026-04-05  
**Аудитор:** AI Code Expert  
**Статус:** ✅ Завершён

---

## 📋 Содержание

1. [Резюме](#резюме)
2. [Структура проекта](#структура-проекта)
3. [Нарушения принципов SOLID](#нарушения-принципов-solid)
4. [Дублирование кода](#дублирование-кода)
5. [Сложные зависимости](#сложные-зависимости)
6. [Узкие места производительности](#узкие-места-производительности)
7. [Проблемы архитектуры](#проблемы-архитектуры)
8. [План рефакторинга](#план-рефакторинга)

---

## 📊 Резюме

| Категория | Статус | Критичность |
|-----------|--------|-------------|
| Структура проекта | ⚠️ Требует улучшений | Средняя |
| Принципы SOLID | ❌ Нарушены | Высокая |
| Дублирование кода | ⚠️ Присутствует | Средняя |
| Зависимости | ⚠️ Сильная связанность | Высокая |
| Производительность | ✅ Оптимизировано частично | Низкая |
| Тестируемость | ❌ Низкая | Высокая |
| Документация | ✅ Хорошая | - |

**Общая оценка:** 5/10 — Требуется значительный рефакторинг

---

## 📁 Структура проекта

### Текущая структура

```
/workspace
├── src/                      # Исходный код (8,397 строк)
│   ├── database.py           # 485 строк - Репозитории и БД
│   ├── services.py           # 524 строки - Бизнес-логика
│   ├── generate_recommendations.py  # 699 строк - Генерация рекомендаций
│   ├── backtest_engine.py    # 339 строк - Backtesting
│   ├── load_*.py             # 3 файла загрузки данных
│   ├── enrich_*.py           # 2 файла обогащения данных
│   ├── cache.py              # 383 строки - Кэширование
│   └── ...                   # Другие скрипты
├── models/                   # ML модели
│   ├── model_controller.py   # 195 строк - Управление моделями
│   ├── model_lightgbm_v1.py  # Модель LightGBM
│   └── *.pkl                 # Артефакты моделей
├── tests/                    # Тесты (3 файла)
│   ├── test_backtest_engine.py
│   ├── test_model_validation.py
│   └── test_security.py
├── config/                   # Конфигурация
│   └── config.yaml
├── docs/                     # Документация
│   ├── ARCHITECTURE.md
│   ├── API.md
│   └── DEVELOPMENT.md
└── data/                     # Данные
```

### Проблемы структуры

| Проблема | Описание | Влияние |
|----------|----------|---------|
| **Плоская структура src/** | Все файлы в одной папке | Сложно ориентироваться |
| **Смешение ответственности** | Скрипты содержат бизнес-логику, SQL, UI | Нарушение SRP |
| **Отсутствие явных слоёв** | Нет разделения на domain/application/infrastructure | Сложная тестируемость |
| **Глобальные константы** | PROJECT_ROOT захардкожен в файлах | Невозможность переиспользования |

---

## ❌ Нарушения принципов SOLID

### 1. Single Responsibility Principle (SRP)

**Нарушен в:** `generate_recommendations.py`, `backtest_engine.py`

```python
# ❌ generate_recommendations.py (699 строк)
# Один файл делает ВСЁ:
def load_config(): ...           # Конфигурация
def get_engine(config): ...      # Подключение к БД
def get_clients_for_today(): ... # Бизнес-логика
def get_candidate_skus_batch(): ... # SQL + бизнес-логика
def encode_features(): ...       # ML preprocessing
def predict_probabilities_batch(): ... # ML inference
def select_2plus2plus1(): ...    # Бизнес-правила
def save_to_database(): ...      # Персистентность
def export_to_excel_flat(): ...  # Экспорт в Excel
def main(): ...                  # Оркестрация
```

**Проблема:** Файл имеет 7+ причин для изменения

**Решение:** Разделить на модули:
- `services/recommendation_service.py`
- `repositories/candidate_repository.py`
- `ml/model_predictor.py`
- `export/excel_exporter.py`

### 2. Open/Closed Principle (OCP)

**Нарушен в:** `services.py`, `model_controller.py`

```python
# ❌ Жёсткая зависимость от конкретных классов
class RecommendationService:
    def __init__(self, db_repository=None):
        self.db_repo = db_repository  # Конкретная реализация
    
    # ❌ Для добавления нового источника данных нужно менять код
```

**Проблема:** Невозможно добавить новый источник без модификации

**Решение:** Использовать абстракции (интерфейсы/протоколы):

```python
# ✅ Через Protocol (Python 3.8+)
from typing import Protocol

class CandidateRepository(Protocol):
    def get_candidates(self, client_ids: List[str]) -> pd.DataFrame: ...

class RecommendationService:
    def __init__(self, repo: CandidateRepository):
        self.repo = repo  # Любая реализация
```

### 3. Liskov Substitution Principle (LSP)

**Нарушен в:** `database.py`

```python
# ❌ Репозитории имеют разные сигнатуры методов
class ClientRepository:
    def get_active_clients(self, months: int = 12, min_purchases: int = 3) -> List[str]: ...

class PurchaseRepository:
    def get_client_history(self, client_id: str, days: int = 90) -> pd.DataFrame: ...
```

**Проблема:** Невозможно взаимозаменять репозитории

**Решение:** Создать базовый класс/протокол для всех репозиториев

### 4. Interface Segregation Principle (ISP)

**Нарушен в:** `services.py`

```python
# ❌ "Жирный" сервис с избыточными методами
class ModelService:
    def load_active_model(): ...
    def predict(): ...
    def _load_pickle(): ...        # Внутренний метод публичный
    def _load_latest_encoders(): ...  # Детали реализации
```

**Проблема:** Клиенты зависят от методов, которые не используют

**Решение:** Разделить на специализированные интерфейсы:
- `ModelLoader` (загрузка)
- `ModelPredictor` (предсказания)
- `EncoderManager` (кодирование)

### 5. Dependency Inversion Principle (DIP)

**Нарушен повсеместно**

```python
# ❌ Зависимость от конкретных реализаций
from src.database import Database, ClientRepository

def main():
    db = Database.from_config()  # Конкретный класс
    repo = ClientRepository(db)  # Конкретный класс
```

**Проблема:** Невозможно протестировать без реальной БД

**Решение:** Внедрение зависимостей через конструктор:

```python
# ✅ Абстракции + DI
def create_recommendations(repo: CandidateRepository, predictor: ModelPredictor):
    # Работает с любой реализацией
```

---

## 🔁 Дублирование кода

### 1. SQL-запросы

**Дублируется в:** `database.py`, `generate_recommendations.py`, `backtest_engine.py`

```python
# ❌ Одинаковый запрос в 3 местах
query = text("""
    SELECT DISTINCT client_id
    FROM purchases
    WHERE purchase_date >= CURRENT_DATE - INTERVAL :months MONTH
    GROUP BY client_id
    HAVING COUNT(*) >= :min_purchases
""")
```

**Встречается:** 5 раз с вариациями

**Решение:** Вынести в репозиторий `ClientRepository.get_active_clients()`

### 2. Логика загрузки модели

**Дублируется в:** `services.py`, `generate_recommendations.py`, `model_controller.py`

```python
# ❌ Три реализации загрузки модели
registry_path = MODEL_DIR / "model_registry.json"
with open(registry_path, 'r') as f:
    registry = json.load(f)
active_model_name = registry.get('active_model')
```

**Решение:** Единый сервис `ModelLoader` с кэшированием

### 3. Обработка ошибок кодирования

**Дублируется в:** `generate_recommendations.py`, `services.py`

```python
# ❌ Одинаковая обработка NULL
df_encoded[col] = df_encoded[col].fillna('Unknown')
try:
    df_encoded[encoded_col] = encoder.transform(...)
except Exception as e:
    logger.error(f"Ошибка кодирования {col}: {e}")
    df_encoded[encoded_col] = -1
```

**Решение:** Утилита `FeatureEncoder.encode_with_fallback()`

### 4. A/B тестирование

**Дублируется в:** `generate_recommendations.py`, `evaluate_ab.py`

```python
# ❌ Одинаковый хэш для распределения
hash_val = int(hashlib.md5(client_id.encode()).hexdigest(), 16) % 100
ratio = config['ab_test'].get('test_group_ratio', 0.5)
return 'test' if hash_val < int(ratio * 100) else 'control'
```

**Решение:** Сервис `ABTestingService.assign_group(client_id)`

---

## 🔗 Сложные зависимости

### Граф зависимостей

```
generate_recommendations.py
├── database.py (прямой импорт)
├── services.py (частичное использование)
├── config_loader.py
├── pandas, numpy, lightgbm, sklearn
└── sqlalchemy (через database.py)

backtest_engine.py
├── standalone (все функции в одном файле)
├── pandas, numpy, sqlalchemy
└── yaml (конфигурация)

services.py
├── database.py (репозитории)
├── pandas, numpy, pickle, json
└── sklearn (калибраторы)
```

### Проблемы

| Тип | Описание | Риск |
|-----|----------|------|
| **Циклические зависимости** | Потенциально между `services.py` ↔ `database.py` | Высокий |
| **Глобальное состояние** | `PROJECT_ROOT = Path("D:/ProjectZZZ")` | Средний |
| **Жёсткая связанность** | Прямые импорты конкретных классов | Высокий |
| **Отсутствие абстракций** | Нет интерфейсов между слоями | Высокий |

---

## 🐌 Узкие места производительности

### 1. Пакетная обработка

**Текущее состояние:** ✅ Частично оптимизировано

```python
# ✅ В backtest_engine.py
CLIENT_BATCH_SIZE = 500  # Пакетная загрузка

# ✅ В generate_recommendations.py
CLIENT_BATCH_SIZE = 100  # Меньший размер
SAVE_BATCH_SIZE = 500    # Пакетное сохранение
```

**Проблема:** Размеры пакетов захардкожены, нет адаптивности

**Решение:** Конфигурируемые размеры + автотюнинг

### 2. Группировка по клиентам

**Текущее состояние:** ✅ Оптимизировано в v7.2

```python
# ✅ groupby вместо цикла O(n²)
grouped = df_candidates.groupby('client_id')
for client_id, client_df in grouped:
    selected_skus, fallback_reason = select_2plus2plus1(client_df)
```

**Было:** ❌ Цикл с фильтрацией DataFrame для каждого клиента  
**Стало:** ✅ GroupBy + итерация

### 3. SQL-запросы

**Текущее состояние:** ⚠️ Параметризованы, но есть проблемы

```python
# ✅ Защита от SQL Injection
params={'client_ids': batch_clients}

# ❌ UNION в candidates query может быть медленным
SELECT ... FROM sales_enriched se
UNION
SELECT ... FROM global_top_skus g
```

**Решение:** 
- Добавить индексы на `sales_enriched(client_id, sku_id, purchase_date)`
- Рассмотреть материализованные представления для `global_top_skus`

### 4. Кэширование

**Текущее состояние:** ⚠️ Есть `cache.py` и `features_cache.py`, но не используются активно

```python
# ❌ Ручное управление кэшем
if not os.path.exists(cache_path):
    # Пересчитать признаки
    save_cache()
```

**Решение:** Автоматическое кэширование с TTL и инвалидацией

---

## 🏗️ Проблемы архитектуры

### 1. Сравнение документации и кода

| Документация | Реальность | Расхождение |
|--------------|------------|-------------|
| **Сервисная архитектура** | Скрипты с функциями | ❌ Сервисы есть, но используются частично |
| **Репозитории** | Есть в `database.py` | ✅ Реализованы |
| **Model Controller** | Есть в `models/` | ✅ Реализован |
| **A/B тестирование** | Заявлено в конфиге | ⚠️ Частично в коде |
| **Кэширование признаков** | Таблица `features_cache` | ⚠️ Модуль есть, интеграции нет |

### 2. Масштабируемость

**Проблемы:**

1. **Монолитные скрипты**
   - `generate_recommendations.py` (699 строк)
   - Невозможно масштабировать горизонтально

2. **Отсутствие очереди задач**
   - Всё выполняется синхронно
   - Нет retry logic

3. **Локальное хранилище**
   - `PROJECT_ROOT = Path("D:/ProjectZZZ")`
   - Не работает в Docker/Kubernetes

### 3. Тестируемость

**Проблемы:**

1. **Мало тестов**
   - Только 3 файла тестов
   - Покрытие < 30%

2. **Интеграционные тесты отсутствуют**
   - Нет тестов end-to-end
   - Нет моков для БД

3. **Зависимость от глобального состояния**
   ```python
   # ❌ Невозможно протестировать без реальной БД
   db = Database.from_config()
   ```

### 4. Поддерживаемость

**Проблемы:**

1. **Разные стили кода**
   - Смесь snake_case и CamelCase
   - Разные подходы к логированию

2. **Документация функций**
   - ⚠️ Частично есть docstrings
   - ❌ Нет type hints везде

3. **Обработка ошибок**
   - ⚠️ Есть try/except
   - ❌ Нет единой стратегии

---

## 📝 План рефакторинга

### Этап 1: Реструктуризация (Неделя 1-2)

#### 1.1 Новая структура папок

```
src/
├── __init__.py
├── main.py                    # Точка входа
├── config/
│   ├── __init__.py
│   ├── settings.py            # Настройки (pydantic)
│   └── config.yaml
├── domain/                    # Бизнес-логика (ядро)
│   ├── __init__.py
│   ├── entities/              # Бизнес-объекты
│   │   ├── client.py
│   │   ├── sku.py
│   │   └── recommendation.py
│   ├── services/              # Бизнес-сервисы
│   │   ├── recommendation_service.py
│   │   ├── model_service.py
│   │   └── ab_testing_service.py
│   └── exceptions/            # Бизнес-исключения
│       └── recommendations.py
├── application/               # Прикладной слой
│   ├── __init__.py
│   ├── commands/              # Команды (CQRS)
│   │   ├── generate_recommendations.py
│   │   └── train_model.py
│   └── handlers/              # Обработчики команд
├── infrastructure/            # Инфраструктура
│   ├── __init__.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py      # Подключение к БД
│   │   └── repositories/      # Реализации репозиториев
│   │       ├── client_repo.py
│   │       ├── purchase_repo.py
│   │       └── candidate_repo.py
│   ├── ml/
│   │   ├── __init__.py
│   │   ├── model_loader.py
│   │   ├── predictor.py
│   │   └── encoders.py
│   ├── cache/
│   │   ├── __init__.py
│   │   └── feature_cache.py
│   └── export/
│       ├── __init__.py
│       ├── excel_exporter.py
│       └── csv_exporter.py
├── interfaces/                # Внешние интерфейсы
│   ├── __init__.py
│   ├── cli/                   # CLI команды
│   ├── api/                   # REST API (если будет)
│   └── gui/                   # GUI (dashboard)
└── shared/                    # Общие утилиты
    ├── __init__.py
    ├── logging.py
    ├── validators.py
    └── types.py
```

#### 1.2 Миграция файлов

| Старый файл | Новый файл | Изменения |
|-------------|------------|-----------|
| `src/database.py` | `infrastructure/database/repositories/*.py` | Разделить на 3 файла |
| `src/services.py` | `domain/services/*.py` | Убрать зависимость от БД |
| `src/generate_recommendations.py` | `application/commands/generate_recommendations.py` | Выделить бизнес-логику |
| `src/backtest_engine.py` | `application/commands/run_backtest.py` | Рефакторинг функций |
| `models/model_controller.py` | `domain/services/model_controller.py` | Добавить абстракции |

### Этап 2: Внедрение абстракций (Неделя 3)

#### 2.1 Протоколы для репозиториев

```python
# domain/protocols/repositories.py
from typing import Protocol, List
import pandas as pd

class ClientRepositoryProtocol(Protocol):
    def get_active_clients(self, months: int = 12) -> List[str]: ...
    def get_client_names(self, client_ids: List[str]) -> dict: ...

class CandidateRepositoryProtocol(Protocol):
    def get_candidates(self, client_ids: List[str]) -> pd.DataFrame: ...
```

#### 2.2 Dependency Injection

```python
# shared/di_container.py
from dependency_injector import containers, providers

class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    
    database = providers.Singleton(Database, url=config.database.url)
    
    client_repo = providers.Factory(
        ClientRepository,
        db=database
    )
    
    recommendation_service = providers.Factory(
        RecommendationService,
        client_repo=client_repo,
        model_service=model_service
    )
```

### Этап 3: Устранение дублирования (Неделя 4)

#### 3.1 Централизация SQL

```python
# infrastructure/database/queries.py
class Queries:
    GET_ACTIVE_CLIENTS = text("""
        SELECT DISTINCT client_id ...
    """)
    
    GET_CANDIDATES = text("""
        WITH client_history AS (...)
        SELECT * FROM candidates
    """)
```

#### 3.2 Общие утилиты

```python
# shared/utils/encoding.py
class FeatureEncoder:
    @staticmethod
    def encode_with_fallback(df, encoder, col):
        # Единая логика кодирования
```

### Этап 4: Улучшение тестируемости (Неделя 5)

#### 4.1 Моки для БД

```python
# tests/conftest.py
@pytest.fixture
def mock_client_repo():
    repo = Mock(spec=ClientRepositoryProtocol)
    repo.get_active_clients.return_value = ['C1', 'C2']
    return repo
```

#### 4.2 Интеграционные тесты

```python
# tests/integration/test_recommendations.py
def test_full_recommendation_pipeline():
    # End-to-end тест
```

### Этап 5: Документация и CI/CD (Неделя 6)

#### 5.1 Обновление документации

- Переписать README с новой структурой
- Добавить диаграммы компонентов
- Document API endpoints

#### 5.2 CI/CD пайплайн

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run tests
        run: pytest --cov=src --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

---

## 🎯 Приоритеты

| Приоритет | Задача | Оценка усилий | Влияние |
|-----------|--------|---------------|---------|
| **P0** | Реструктуризация `src/` | Высокие | 🔴 Критическое |
| **P0** | Внедрение DI | Средние | 🔴 Критическое |
| **P1** | Устранение дублирования | Средние | 🟡 Важное |
| **P1** | Увеличение покрытия тестов | Высокие | 🟡 Важное |
| **P2** | Оптимизация SQL | Низкие | 🟢 Полезное |
| **P2** | Автоматическое кэширование | Средние | 🟢 Полезное |

---

## ✅ Критерии успеха

После рефакторинга:

- [ ] Покрытие тестами ≥ 70%
- [ ] Время генерации рекомендаций ≤ 5 мин для 1000 клиентов
- [ ] Отсутствие циклических зависимостей
- [ ] Все сервисы имеют интерфейсы
- [ ] Конфигурация через environment variables
- [ ] Документация актуализирована

---

## 📚 Рекомендации

1. **Начать с малого**: Рефакторить по одному модулю за раз
2. **Сохранять обратную совместимость**: Не ломать существующий функционал
3. **Писать тесты перед рефакторингом**: Safety net
4. **Использовать инструменты**:
   - `pylint` / `flake8` для линтинга
   - `mypy` для типизации
   - `black` для форматирования
   - `pytest-cov` для покрытия

---

**Заключение:** Кодовая база требует значительного рефакторинга для улучшения поддерживаемости, тестируемости и масштабируемости. Предложенный план позволит постепенно улучшить архитектуру без остановки разработки.
