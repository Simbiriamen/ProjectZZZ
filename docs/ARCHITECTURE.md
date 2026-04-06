# Архитектура ProjectZZZ

## Обзор системы

ProjectZZZ — это модульная ML-система для генерации персонализированных рекомендаций товаров (SKU) на основе анализа истории покупок клиентов.

### Основные принципы

1. **Модульность** — каждый компонент независим и заменяем
2. **Расширяемость** — новые модели добавляются без изменения ядра
3. **Наблюдаемость** — полное логирование и мониторинг метрик
4. **Безопасность** — параметризованные SQL-запросы, валидация входных данных
5. **Автоматизация** — A/B тесты, авто-откат при деградации

---

## Высокоуровневая архитектура

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ProjectZZZ Architecture                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐            │
│  │   Excel      │     │   PostgreSQL │     │   Parquet    │            │
│  │   (raw/)     │────▶│   (БД)       │────▶│   (processed/)│           │
│  └──────────────┘     └──────────────┘     └──────────────┘            │
│         │                    │                    │                     │
│         ▼                    ▼                    ▼                     │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐            │
│  │ ETL Pipeline │     │ Backtest     │     │ Generation   │            │
│  │              │     │ Engine       │     │ Service      │            │
│  └──────────────┘     └──────────────┘     └──────────────┘            │
│                              │                    │                     │
│                              ▼                    ▼                     │
│                       ┌──────────────┐     ┌──────────────┐            │
│                       │ Model        │     │ Visit        │            │
│                       │ Registry     │     │ Proposals    │            │
│                       └──────────────┘     └──────────────┘            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Компоненты системы

### 1. Уровень данных (Data Layer)

#### Источники данных
| Тип | Формат | Расположение | Обновление |
|-----|--------|--------------|------------|
| Справочники | Excel | `data/raw/` | Еженедельно |
| Продажи | Excel | `data/raw/` | Ежедневно |
| Остатки | Excel | `data/raw/` | Ежедневно |

#### Хранилище (PostgreSQL)
**Основные таблицы:**
- `clients` — профили клиентов (сегмент, менеджер, метаданные)
- `skus` — каталог товаров (категория, группа, цена, маржа, остатки)
- `purchases` — история покупок (клиент, SKU, дата, количество, цена)
- `marketing_groups` — маркетинговые группы и тренды
- `visits_schedule` — план визитов менеджеров
- `visit_proposals` — сгенерированные рекомендации
- `features_cache` — кэш признаков для ускорения генерации
- `load_history` — отслеживание загруженных файлов (инкрементальность)
- `backtest_results` — результаты ретроспективного тестирования

**Схема БД:** `config/schema.sql`

#### Кэш (Parquet)
- Обработанные данные для быстрого доступа
- Кэш признаков (`data/cache/`)
- Архив результатов (`data/output/`)

---

### 2. ETL Pipeline

#### Модули загрузки
| Модуль | Назначение | Особенности |
|--------|------------|-------------|
| `load_references.py` | Справочники номенклатуры и клиентов | Валидация дубликатов |
| `load_sales.py` | История продаж | Инкрементальная загрузка по хэшу |
| `load_stocks.py` | Остатки на складах | Актуализация ежедневно |
| `update_marketing_hierarchy.py` | Маркетинговые группы | Расчёт трендов |

#### Инкрементальная загрузка
```python
# Алгоритм проверки изменений
1. Вычислить хэш файла (MD5 для больших файлов — первый 1MB)
2. Сравнить с last_hash в load_history
3. Если хэш не изменился → пропустить файл
4. Если изменился → загрузить новые записи
5. Зафиксировать в load_history
```

---

### 3. Backtest Engine (`src/backtest_engine.py`)

**Назначение:** Генерация обучающей выборки из исторических данных

**Процесс:**
1. Выбор активных клиентов (≥3 покупок за период)
2. Для каждого клиента:
   - Генерация гипотетических визитов (каждые 14 дней)
   - Определение окна покупки [визит, визит+10 дней]
   - Проверка фактических покупок в окне
   - Присвоение таргета: 1 если куплен, 0 если нет
3. Формирование датасета с признаками
4. Валидация качества данных

**Оптимизации:**
- Пакетная обработка (CLIENT_BATCH_SIZE = 500)
- Фильтрация NaT значений
- Ограничение строк на клиента (max_rows_per_client = 1000)
- Параметризованные SQL-запросы (защита от SQL Injection)

**Выход:** Таблица `backtest_results` в БД

---

### 4. ML Models Layer

#### Архитектура моделей
```
models/
├── model_controller.py      # Диспетчер моделей
├── model_lightgbm_v1.py     # Базовая модель (production)
├── model_registry.json      # Реестр версий и метрик
├── encoders_*.pkl           # Кодировщики признаков
├── calibrator_*.pkl         # Калибратор вероятностей
└── model_*.pkl              # Бинарные файлы моделей
```

#### Model Controller
**Ответственность:**
- Регистрация новых моделей в реестре
- Выбор активной модели для генерации
- Оценка продвижения staging → production
- Автоматический откат при деградации
- Еженедельный health check

**Критерии продвижения модели:**
```python
precision_improved >= 3%
hit_rate_improved >= 3%
brier_score_not_worsened
training_time_not_increased_2x
```

**Критерии отката (критические):**
```python
precision_drop >= 5%    # ТРЕБУЕТ НЕМЕДЛЕННОЙ РЕАКЦИИ
hit_rate_drop >= 5%     # ТРЕБУЕТ НЕМЕДЛЕННОЙ РЕАКЦИИ
brier_score_worsen >= 10%
```

#### LightGBM v1
**Конфигурация:**
- Задача: Binary Classification
- Метрика: Brier Score (калибровка вероятностей)
- Калибровка: Platt scaling (sigmoid) на отложенной выборке
- Минимум примеров: 10,000

**Признаки (Features):**
| Категория | Признаки | Описание |
|-----------|----------|----------|
| Клиентские | frequency_30d, frequency_90d | Частота покупок |
| | avg_check_trend | Динамика среднего чека |
| | seasonality_index | Сезонность (месяц/квартал) |
| | portfolio_diversity | Уникальных категорий за 6 мес |
| Товарные | global_popularity | Популярность SKU |
| | margin_coeff | Нормализованная маржинальность |
| | stock_level | Остаток (признак, не фильтр) |
| | is_new_flag | Новинка (< 30 дней) |
| Динамические | days_since_last_purchase_sku | Давность покупки SKU |
| | rolling_sales_2w, 4w, 8w | Скользящие средние |
| | cannibalization_score | Корреляция с другими покупками |
| Групповые | group_trend_6m | Тренд маркетинговой группы |
| | group_share_in_portfolio | Доля группы в покупках |
| | days_since_last_purchase_in_group | Давность покупки группы |
| Эмбеддинги | client_vec_1..50, sku_vec_1..50 | ALS эмбеддинги (implicit) |

---

### 5. Recommendation Service (`src/services.py`)

#### Сервисы
| Сервис | Ответственность |
|--------|-----------------|
| `ModelService` | Загрузка модели, предсказания, кодирование |
| `RecommendationService` | Генерация рекомендаций по правилу 2+2+1 |

#### Правило 2+2+1
**Состав предложения на визит:**
1. **2 новых товара** — клиент никогда не покупал (P > 0.8)
2. **2 товара на развитие** — из привычных, растущие группы (trend > +0.02)
3. **1 товар на возврат** — из привычных, падающие группы (trend < -0.02)

**Алгоритм формирования:**
```python
1. Предсказание P(buy) для всех кандидатов (остаток >= 1)
2. Применение калибровки вероятностей
3. Скоринг с учётом бизнес-правил:
   - Базовый скор: P_calibrated * margin_coeff
   - Бонус для "Развития": скор *= (1 + 0.5 * group_trend)
   - Бонус для "Возврата": скор *= (days_since_last / max_days)
4. Отбор по категориям (новые → развитие → возврат)
5. Fallback механизм при недостатке кандидатов
6. Финальные проверки (дубликаты, остатки, ровно 5 SKU)
```

**Fallback стратегия:**
- Если "Возврат" пуст → брать из "Стабильных" групп с максимальной давностью
- Если после всех шагов < 5 SKU → дополнить Global Top 200
- Все замены логировать в `fallback_reason`

---

### 6. A/B Testing Framework

#### Конфигурация (`config/config.yaml`)
```yaml
ab_test:
  enabled: true
  test_group_ratio: 0.5  # 50% клиентов в тесте
  assignment_method: "hash"  # детерминированное распределение
  metrics:
    primary: "hit_rate"
    secondary: ["precision_5", "brier_score"]
  promotion:
    min_duration_days: 14
    significance_level: 0.05
    min_uplift: 0.03  # мин. улучшение 3%
    auto_promote: true
  degradation:
    critical_threshold: 0.05  # падение ≥5% = критическое
    action: "rollback"
```

#### Распределение клиентов
```python
def get_ab_group(client_id: str, config: dict) -> str:
    hash_val = int(hashlib.md5(client_id.encode()).hexdigest(), 16) % 100
    ratio = config['ab_test'].get('test_group_ratio', 0.5)
    return 'test' if hash_val < int(ratio * 100) else 'control'
```

#### Метрики
| Метрика | Формула | Порог MVP |
|---------|---------|-----------|
| Precision@5 | Купленные SKU / 5 | ≥ 0.35 |
| Hit Rate | Клиенты с ≥1 покупкой / Всего | ≥ 0.55 |
| Brier Score | MSE вероятностей | ≤ 0.20 |

---

### 7. Database Layer (`src/database.py`)

#### Классы репозиториев
```python
Database
├── ClientRepository
│   ├── get_active_clients(months=12)
│   ├── get_client_profile(client_id)
│   └── get_client_segment(client_id)
│
├── PurchaseRepository
│   ├── get_client_history(client_id, days=90)
│   ├── get_purchase_window(client_id, start_date, end_date)
│   └── get_category_stats(client_id, category)
│
├── SKURepository
│   ├── get_available_skus(min_stock=1)
│   ├── get_sku_features(sku_id)
│   └── get_global_popularity(top_n=200)
│
└── ModelRepository
    ├── save_model_metadata(name, metrics, paths)
    ├── get_active_model()
    └── update_model_status(name, status)
```

#### Подключение
```python
from src.database import Database

db = Database.from_config()
clients = ClientRepository(db).get_active_clients(months=12)
```

---

### 8. Configuration Management (`src/config_loader.py`)

#### Загрузка конфигурации
```python
from src.config_loader import load_config

config = load_config()  # Загружает config.yaml с подстановкой env
```

#### Переменные окружения
**Формат:** `${VAR_NAME:default_value}`

**Пример (`config/config.yaml`):**
```yaml
database:
  host: ${DB_HOST:localhost}
  port: ${DB_PORT:5432}
  name: ${DB_NAME:project_zzz_db}
  user: ${DB_USER:postgres}
  password: ${DB_PASSWORD}  # Обязательно из env!
```

#### Валидация
```python
def validate_config(config: Dict) -> list:
    errors = []
    
    # Обязательные поля
    required_db_fields = ['host', 'port', 'name', 'user', 'password']
    for field in required_db_fields:
        if not config.get('database', {}).get(field):
            errors.append(f"database.{field} is required")
    
    # Валидация диапазонов
    if config.get('ab_test', {}).get('test_group_ratio', 0) > 1:
        errors.append("test_group_ratio must be <= 1.0")
    
    return errors
```

---

## Поток данных (Data Flow)

### 1. Обучение модели
```
[Исторические данные]
        │
        ▼
┌───────────────────┐
│ backtest_engine   │ → backtest_results (БД)
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ model_lightgbm_v1 │ → model.pkl, calibrator.pkl, encoders.pkl
└───────────────────┘
        │
        ▼
┌───────────────────┐
│ model_controller  │ → Обновление model_registry.json
└───────────────────┘
```

### 2. Генерация рекомендаций
```
[План визитов]
        │
        ▼
┌───────────────────┐
│ generate_         │
│ recommendations   │
└───────────────────┘
        │
        ├──────────────┬──────────────┬──────────────┐
        ▼              ▼              ▼              ▼
┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐
│ Load      │  │ Load      │  │ Predict   │  │ Apply     │
│ clients   │  │ model     │  │ P(buy)    │  │ 2+2+1     │
└───────────┘  └───────────┘  └───────────┘  └───────────┘
        │              │              │              │
        └──────────────┴──────────────┴──────────────┘
                                   │
                                   ▼
                          ┌───────────────────┐
                          │ Save to           │
                          │ visit_proposals   │
                          │ + Excel output    │
                          └───────────────────┘
```

---

## Развёртывание

### Docker Compose
```bash
# Запуск БД и сервиса рекомендаций
docker-compose up -d db recommendations

# Запуск обучения (профиль training)
docker-compose --profile training up training

# Запуск backtesting (профиль training)
docker-compose --profile training up backtest

# PGAdmin для разработки (профиль dev)
docker-compose --profile dev up pgadmin
```

### Локальная разработка
```bash
# Установка зависимостей
pip install -r requirements.txt

# Создание .env
cp .env.example .env
# Заполнить переменными

# Запуск тестов
pytest tests/ -v

# Загрузка данных
python src/load_references.py
python src/load_sales.py

# Backtesting
python src/backtest_engine.py

# Обучение модели
python models/model_lightgbm_v1.py

# Генерация рекомендаций
python src/generate_recommendations.py
```

---

## Мониторинг и логирование

### Уровни логирования
| Уровень | Использование |
|---------|---------------|
| INFO | Штатное выполнение операций |
| WARNING | Некритические проблемы (fallback, пропуск файлов) |
| ERROR | Ошибки выполнения (неудачная загрузка, сбой предсказания) |
| CRITICAL | Критическая деградация модели (требуется откат) |

### Лог-файлы
```
docs/logs/
├── load_references.log
├── load_sales.log
├── load_stocks.log
├── backtest_engine.log
├── model_lightgbm_v1.log
├── generate_recommendations.log
└── evaluate_ab.log
```

### Health Check
Еженедельная проверка активной модели:
```python
controller = ModelController(models_dir, registry_path)
health = controller.weekly_health_check(current_metrics)

if not health['healthy']:
    if health['action'] == 'rollback':
        controller.rollback(reason="degradation")
```

---

## Безопасность

### SQL Injection Protection
Все запросы параметризованы:
```python
# ✅ Правильно
query = text("SELECT * FROM purchases WHERE client_id = :client_id")
df = pd.read_sql(query, engine, params={'client_id': client_id})

# ❌ Неправильно
query = f"SELECT * FROM purchases WHERE client_id = '{client_id}'"
```

### Валидация входных данных
```python
def get_ab_group(client_id: str, config: dict) -> str:
    if not client_id:
        raise ValueError("client_id не может быть пустым")
    if not isinstance(client_id, str):
        raise ValueError(f"client_id должен быть строкой")
    if len(client_id) > 256:
        raise ValueError(f"client_id слишком длинный")
    # ...
```

### Защита учётных данных
- Пароли только в переменных окружения (`.env`)
- `.env` исключён из `.gitignore`
- Шаблоны в `.env.example`

---

## Расширяемость

### Добавление новой модели
1. Создать файл `models/model_new_v1.py`
2. Реализовать интерфейс:
   ```python
   def train(X_train, y_train, X_val, y_val) -> Tuple[model, calibrator, encoders, metrics]
   def predict(model, X) -> np.ndarray
   ```
3. Зарегистрировать в реестре:
   ```python
   controller.register_model(
       name='model_new_v1',
       metrics=metrics,
       auto_promote=True,
       status='staging'
   )
   ```
4. Запустить A/B тестирование
5. При успехе → авто-продвижение в production

### Добавление нового признака
1. Добавить расчёт в `backtest_engine.py` (генерация признаков)
2. Добавить в `generate_recommendations.py` (инференс)
3. Обновить документацию признаков
4. Переобучить модель

---

## Производительность

### Бенчмарки (на данных 26 месяцев)
| Операция | Время | Объём данных |
|----------|-------|--------------|
| Backtesting | ~2 часа | 3M+ примеров |
| Обучение LightGBM | ~30 минут | 500K строк |
| Генерация рекомендаций | ~5 минут | 10K клиентов |
| Загрузка продаж (инкрементально) | ~1 минута | 50K записей |

### Оптимизации
- Индексы PostgreSQL на `client_id`, `sku_id`, `purchase_date`
- Кэш признаков в Parquet
- Пакетная обработка (batch processing)
- Connection pooling (SQLAlchemy QueuePool)

---

## Тестирование

### Структура тестов
```
tests/
├── test_security.py          # SQL injection, валидация
├── test_backtest_engine.py   # Логика backtesting
└── test_model_validation.py  # Валидация данных и моделей
```

### Запуск
```bash
# Все тесты
pytest tests/ -v

# С покрытием
pytest tests/ -v --cov=src --cov-report=html

# Только unit-тесты
pytest tests/ -v -m unit

# Исключая медленные тесты
pytest tests/ -v -m "not slow"
```

---

## Changelog архитектуры

### Версия 3.2 (текущая)
- ✅ Модульный контроллер моделей
- ✅ A/B тестирование с авто-переключением
- ✅ Инкрементальная загрузка по хэшу
- ✅ Параметризованные SQL-запросы
- ✅ Health check моделей

### Версия 3.1
- ✅ Backtest engine v4 с оптимизацией памяти
- ✅ Калибровка вероятностей (Platt scaling)
- ✅ Fallback механизм для рекомендаций

### Версия 3.0
- ✅ Переход на PostgreSQL
- ✅ Docker-окружение
- ✅ Разделение на сервисы

---

## Контакты и поддержка

- **Репозиторий:** https://github.com/Simbiriamen/ProjectZZZ
- **Документация:** `/docs/`
- **Логи:** `/docs/logs/`
- **Лицензия:** MIT
