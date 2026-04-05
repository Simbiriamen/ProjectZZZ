# API Документация ProjectZZZ

## Обзор

ProjectZZZ предоставляет программный интерфейс для генерации рекомендаций, управления моделями и анализа данных.

---

## Содержание

1. [Recommendation API](#recommendation-api)
2. [Model Management API](#model-management-api)
3. [Data Access API](#data-access-api)
4. [Configuration API](#configuration-api)
5. [Примеры использования](#примеры-использования)

---

## Recommendation API

### Генерация рекомендаций для всех клиентов

**Модуль:** `src/generate_recommendations.py`

**Запуск:**
```bash
python src/generate_recommendations.py
```

**Выходные данные:**
- Таблица `visit_proposals` в БД
- Excel-файл: `data/output/recommendations_YYYY-MM-DD.xlsx`

**Структура выходных данных:**
```python
{
    "client_id": str,           # ID клиента
    "visit_date": datetime,     # Дата визита
    "sku_id": str,              # ID товара
    "predicted_prob": float,    # Предсказанная вероятность
    "calibrated_prob": float,   # Калиброванная вероятность
    "selection_type": str,      # new/develop/retain
    "fallback_reason": str,     # Причина замены (если была)
    "model_version": str        # Версия модели
}
```

---

### Генерация для конкретного клиента

**Модуль:** `src/services.py`

```python
from src.services import RecommendationService
from pathlib import Path

# Инициализация сервиса
rec_service = RecommendationService(
    models_dir=Path("models"),
    config_path=Path("config/config.yaml")
)

# Генерация для одного клиента
recommendations = rec_service.generate_for_client(
    client_id="C12345",
    visit_date="2024-03-25"
)

print(recommendations)
# Output:
# [
#     {"sku_id": "S001", "type": "new", "probability": 0.85},
#     {"sku_id": "S002", "type": "new", "probability": 0.82},
#     {"sku_id": "S003", "type": "develop", "probability": 0.76},
#     {"sku_id": "S004", "type": "develop", "probability": 0.71},
#     {"sku_id": "S005", "type": "retain", "probability": 0.68}
# ]
```

---

### Пакетная генерация

```python
from src.services import RecommendationService

rec_service = RecommendationService()

client_ids = ["C001", "C002", "C003"]
all_recommendations = rec_service.generate_batch(
    client_ids=client_ids,
    visit_date="2024-03-25",
    batch_size=100
)
```

---

## Model Management API

### Загрузка активной модели

**Модуль:** `src/services.py`

```python
from src.services import ModelService

model_service = ModelService(models_dir=Path("models"))

# Загрузка активной модели
model, calibrator, encoders, feature_cols, best_iteration = \
    model_service.load_active_model()

# Предсказание
X_test = ...  # DataFrame с признаками
predictions = model_service.predict(model, X_test)
calibrated_probs = model_service.calibrate(calibrator, predictions)
```

---

### Регистрация новой модели

**Модуль:** `models/model_controller.py`

```python
from models.model_controller import ModelController
from pathlib import Path

controller = ModelController(
    models_dir=Path("models"),
    registry_path=Path("models/model_registry.json")
)

# Регистрация после обучения
controller.register_model(
    name='model_lightgbm_v2',
    metrics={
        'precision_5': 0.45,
        'hit_rate': 0.71,
        'brier_score': 0.13,
        'auc': 0.85,
        'training_time_hours': 2.5
    },
    auto_promote=True,
    status='staging'
)
```

---

### Продвижение модели в production

```python
# Оценка возможности продвижения
can_promote = controller.evaluate_promotion(
    staging_model='model_lightgbm_v2',
    production_model='model_lightgbm_v1'
)

if can_promote:
    controller.promote_to_production(
        model_name='model_lightgbm_v2',
        reason='auto_promotion_after_ab_test'
    )
```

---

### Откат модели

```python
# Автоматический откат при деградации
success = controller.rollback(reason="metric_degradation")

# Ручной откат
success = controller.rollback(reason="manual_rollback")
```

---

### Еженедельный Health Check

```python
current_metrics = {
    'precision_5': 0.32,
    'hit_rate': 0.13,
    'brier_score': 0.022
}

health_status = controller.weekly_health_check(current_metrics)

if not health_status['healthy']:
    if health_status['action'] == 'rollback':
        controller.rollback(reason="degradation")
        # Отправить уведомление команде
```

---

## Data Access API

### Работа с базой данных

**Модуль:** `src/database.py`

```python
from src.database import Database, ClientRepository, PurchaseRepository
from pathlib import Path

# Подключение к БД
db = Database.from_config()

# Получение активных клиентов
clients = ClientRepository(db).get_active_clients(months=12)

# История покупок клиента
purchases = PurchaseRepository(db).get_client_history(
    client_id='C12345',
    days=90
)

# Покупки в окне дат
window_purchases = PurchaseRepository(db).get_purchase_window(
    client_id='C12345',
    start_date='2024-01-01',
    end_date='2024-03-31'
)
```

---

### Репозиторий клиентов

```python
from src.database import ClientRepository

repo = ClientRepository(db)

# Активные клиенты
active = repo.get_active_clients(months=12)

# Профиль клиента
profile = repo.get_client_profile('C12345')

# Сегмент клиента
segment = repo.get_client_segment('C12345')

# Статистика по категории
stats = repo.get_category_stats('C12345', category='Electronics')
```

---

### Репозиторий товаров (SKU)

```python
from src.database import SKURepository

repo = SKURepository(db)

# Доступные товары (остаток >= 1)
available = repo.get_available_skus(min_stock=1)

# Признаки товара
features = repo.get_sku_features('S001')

# Популярные товары (Global Top 200)
popular = repo.get_global_popularity(top_n=200)

# Товары по категории
by_category = repo.get_by_category('Electronics')
```

---

### Репозиторий моделей

```python
from src.database import ModelRepository

repo = ModelRepository(db)

# Сохранение метаданных модели
repo.save_model_metadata(
    name='model_lightgbm_v2',
    metrics={'precision_5': 0.45, 'hit_rate': 0.71},
    paths={
        'model': 'models/model_lightgbm_v2.pkl',
        'calibrator': 'models/calibrator_lightgbm_v2.pkl',
        'encoders': 'models/encoders_lightgbm_v2.pkl'
    }
)

# Активная модель
active = repo.get_active_model()

# Обновление статуса
repo.update_model_status('model_lightgbm_v1', 'archived')
```

---

## Configuration API

### Загрузка конфигурации

**Модуль:** `src/config_loader.py`

```python
from src.config_loader import load_config, validate_config

# Загрузка с подстановкой переменных окружения
config = load_config()

# Валидация
errors = validate_config(config)
if errors:
    for error in errors:
        print(f"Configuration error: {error}")
    raise ValueError("Invalid configuration")

# Доступ к параметрам
db_host = config['database']['host']
ab_ratio = config['ab_test']['test_group_ratio']
min_prob = config['recommendations']['min_probability_threshold']
```

---

### Переменные окружения

**Формат:** `${VAR_NAME:default_value}`

**Пример использования в коде:**
```python
import os
from dotenv import load_dotenv

load_dotenv()  # Загрузка из .env

db_password = os.getenv('DB_PASSWORD')
log_level = os.getenv('LOG_LEVEL', 'INFO')
```

---

## Примеры использования

### Полный цикл: от загрузки данных до рекомендаций

```python
from pathlib import Path
import logging

from src.config_loader import load_config
from src.database import Database
from src.services import ModelService, RecommendationService
from models.model_controller import ModelController

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # 1. Загрузка конфигурации
    config = load_config()
    logger.info("✅ Конфигурация загружена")
    
    # 2. Подключение к БД
    db = Database.from_config()
    logger.info("✅ Подключение к БД установлено")
    
    # 3. Загрузка активной модели
    model_service = ModelService()
    model, calibrator, encoders, feature_cols, best_iter = \
        model_service.load_active_model()
    logger.info(f"✅ Модель загружена: {feature_cols}")
    
    # 4. Получение списка клиентов для рекомендаций
    from src.database import ClientRepository
    clients = ClientRepository(db).get_active_clients(months=12)
    logger.info(f"✅ Найдено {len(clients)} активных клиентов")
    
    # 5. Генерация рекомендаций
    rec_service = RecommendationService()
    
    for client_id in clients[:10]:  # Первые 10 для примера
        recommendations = rec_service.generate_for_client(
            client_id=client_id,
            visit_date=pd.Timestamp.now()
        )
        
        logger.info(f"Клиент {client_id}:")
        for rec in recommendations:
            logger.info(f"  - {rec['sku_id']} ({rec['type']}): {rec['probability']:.2f}")
    
    logger.info("✅ Генерация завершена")

if __name__ == "__main__":
    main()
```

---

### A/B тестирование моделей

```python
from src.services import ModelService, RecommendationService
from models.model_controller import ModelController
from pathlib import Path
import pandas as pd

def run_ab_test(test_model_name: str, duration_days: int = 14):
    """
    Запуск A/B теста для новой модели.
    """
    controller = ModelController(
        models_dir=Path("models"),
        registry_path=Path("models/model_registry.json")
    )
    
    config = load_config()
    rec_service = RecommendationService()
    
    # Получение клиентов
    db = Database.from_config()
    from src.database import ClientRepository
    all_clients = ClientRepository(db).get_active_clients(months=12)
    
    # Распределение по группам
    test_clients = []
    control_clients = []
    
    for client_id in all_clients:
        group = get_ab_group(client_id, config)
        if group == 'test':
            test_clients.append(client_id)
        else:
            control_clients.append(client_id)
    
    logger.info(f"Test group: {len(test_clients)}, Control group: {len(control_clients)}")
    
    # Генерация рекомендаций для обеих групп
    test_recs = rec_service.generate_batch(
        client_ids=test_clients,
        model_name=test_model_name
    )
    
    control_recs = rec_service.generate_batch(
        client_ids=control_clients,
        model_name=None  # Активная модель
    )
    
    # Сбор метрик через duration_days
    # (в реальности — ожидание и анализ покупок)
    
    return test_recs, control_recs


def analyze_ab_results(test_recs, control_recs):
    """
    Анализ результатов A/B теста.
    """
    # Расчёт метрик
    test_metrics = calculate_metrics(test_recs)
    control_metrics = calculate_metrics(control_recs)
    
    # Сравнение
    precision_uplift = (test_metrics['precision_5'] - control_metrics['precision_5']) / control_metrics['precision_5']
    hitrate_uplift = (test_metrics['hit_rate'] - control_metrics['hit_rate']) / control_metrics['hit_rate']
    
    logger.info(f"Precision uplift: {precision_uplift*100:.2f}%")
    logger.info(f"Hit Rate uplift: {hitrate_uplift*100:.2f}%")
    
    # Решение о продвижении
    if precision_uplift >= 0.03 and hitrate_uplift >= 0.03:
        logger.info("✅ Тест успешен, рекомендуется продвижение")
        return True
    else:
        logger.info("❌ Тест не показал улучшений")
        return False
```

---

### Мониторинг качества модели

```python
from models.model_controller import ModelController
from src.database import Database
from pathlib import Path
import pandas as pd

def monitor_model_quality():
    """
    Еженедельный мониторинг качества активной модели.
    """
    controller = ModelController(
        models_dir=Path("models"),
        registry_path=Path("models/model_registry.json")
    )
    
    db = Database.from_config()
    
    # Получение последних рекомендаций
    query = """
        SELECT client_id, sku_id, calibrated_prob, selection_type
        FROM visit_proposals
        WHERE visit_date >= CURRENT_DATE - INTERVAL '7 days'
    """
    recent_propsals = pd.read_sql(query, db.engine)
    
    # Получение фактических покупок
    purchases_query = """
        SELECT client_id, sku_id, purchase_date
        FROM purchases
        WHERE purchase_date >= CURRENT_DATE - INTERVAL '7 days'
    """
    purchases = pd.read_sql(purchases_query, db.engine)
    
    # Сопоставление и расчёт метрик
    merged = recent_propsals.merge(
        purchases[['client_id', 'sku_id']].drop_duplicates(),
        on=['client_id', 'sku_id'],
        how='left',
        indicator=True
    )
    
    merged['purchased'] = (merged['_merge'] == 'both').astype(int)
    
    # Метрики
    total_recommendations = len(merged)
    purchased_count = merged['purchased'].sum()
    
    current_metrics = {
        'precision_5': purchased_count / total_recommendations if total_recommendations > 0 else 0,
        'hit_rate': merged.groupby('client_id')['purchased'].any().mean(),
        'brier_score': ((merged['calibrated_prob'] - merged['purchased']) ** 2).mean()
    }
    
    logger.info(f"Current metrics: {current_metrics}")
    
    # Health check
    health = controller.weekly_health_check(current_metrics)
    
    if not health['healthy']:
        logger.critical(f"🚨 Критическая деградация модели!")
        if health['action'] == 'rollback':
            controller.rollback(reason="weekly_monitoring_degradation")
            send_alert("Model rolled back due to degradation")
    
    return health
```

---

### Backtesting

```python
from src.backtest_engine import run_backtesting
from pathlib import Path

def setup_and_run_backtest():
    """
    Запуск backtesting для создания обучающей выборки.
    """
    config = load_config()
    
    result = run_backtesting(
        config=config,
        months=12,  # Период истории
        client_batch_size=500,
        visit_interval_days=14,
        purchase_window_days=14
    )
    
    logger.info(f"Backtesting completed:")
    logger.info(f"  Total examples: {result['total_examples']:,}")
    logger.info(f"  Positive ratio: {result['positive_ratio']:.4f}")
    logger.info(f"  Passed validation: {result['passed_validation']}")
    
    return result
```

---

## Обработка ошибок

### Типичные исключения

```python
from src.services import ModelService
from models.model_controller import ModelController
from src.database import Database
from pathlib import Path

try:
    model_service = ModelService()
    model, calibrator, encoders, feature_cols, best_iter = \
        model_service.load_active_model()
    
except FileNotFoundError as e:
    logger.error(f"Файл модели не найден: {e}")
    # Действие: проверить реестр моделей
    
except ValueError as e:
    logger.error(f"Ошибка валидации: {e}")
    # Действие: проверить конфигурацию
    
except Exception as e:
    logger.critical(f"Неожиданная ошибка: {e}")
    # Действие: откатить последнюю модель
    controller = ModelController(Path("models"), Path("models/model_registry.json"))
    controller.rollback(reason="critical_error")
```

---

## Best Practices

### 1. Всегда проверяйте наличие активной модели

```python
controller = ModelController(models_dir, registry_path)
active_model = controller.get_active_model()

if not active_model:
    raise RuntimeError("Нет активной модели! Обучите модель перед генерацией.")
```

### 2. Используйте контекстный менеджер для БД

```python
from src.database import Database

with Database.from_config().engine.connect() as conn:
    # Работа с БД
    pass
# Соединение автоматически закрывается
```

### 3. Логируйте все fallback операции

```python
if fallback_applied:
    logger.warning(
        f"Fallback применён для клиента {client_id}: {fallback_reason}"
    )
```

### 4. Валидируйте входные данные

```python
def generate_for_client(client_id: str, visit_date: pd.Timestamp):
    if not client_id or len(client_id) > 256:
        raise ValueError("Некорректный client_id")
    
    if not isinstance(visit_date, pd.Timestamp):
        raise ValueError("visit_date должен быть pd.Timestamp")
```

### 5. Кэшируйте тяжёлые операции

```python
from functools import lru_cache

@lru_cache(maxsize=100)
def get_client_features(client_id: str):
    # Тяжёлая операция получения признаков
    return features
```

---

## Changelog API

### Версия 2.0 (текущая)
- ✅ Сервисная архитектура (`src/services.py`)
- ✅ Model Controller API
- ✅ Репозитории для работы с БД
- ✅ A/B тестирование

### Версия 1.5
- ✅ Калибровка вероятностей
- ✅ Fallback механизм
- ✅ Инкрементальная загрузка

### Версия 1.0
- ✅ Базовая генерация рекомендаций
- ✅ LightGBM модель
- ✅ PostgreSQL хранилище

---

## Поддержка

- **Документация:** `/docs/ARCHITECTURE.md`
- **Примеры:** `/tests/`
- **Логи:** `/docs/logs/`
