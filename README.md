# ProjectZZZ — Система рекомендаций SKU на базе ML

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![LightGBM](https://img.shields.io/badge/LightGBM-4.5.0-green.svg)](https://lightgbm.readthedocs.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**ProjectZZZ** — это полнофункциональная ML-система для генерации рекомендаций товаров (SKU) для клиентов на основе анализа истории покупок, поведения и товарных предпочтений.

---

## 📋 Содержание

- [Возможности](#-возможности)
- [Архитектура](#-архитектура)
- [Установка](#-установка)
- [Быстрый старт](#-быстрый-старт)
- [Использование](#-использование)
- [API](#-api)
- [Тестирование](#-тестирование)
- [Структура проекта](#-структура-проекта)
- [Зависимости](#-зависимости)
- [Производительность](#-производительность)
- [Вклад в проект](#-вклад-в-проект)
- [Лицензия](#-лицензия)
- [Документация](#-документация)

---

## ✨ Возможности

### 🔷 Основные функции

- **Генерация рекомендаций** по правилу 2+2+1:
  - 2 новых товара для клиента
  - 2 товара для развития категории
  - 1 товар для возврата ушедшего клиента

- **ML-модель LightGBM** с калибровкой вероятностей
- **A/B тестирование** моделей с авто-переключением
- **Backtesting engine** для ретроспективного обучения
- **Умная загрузка данных** с проверкой изменений файлов

### 🔷 Управление данными

- ETL-пайплайн для загрузки из Excel в PostgreSQL
- Инкрементальная загрузка с хешированием
- Конвертация в Parquet для сжатия данных
- Архивация старых результатов

### 🔷 Мониторинг и контроль

- GUI панель управления (tkinter)
- Еженедельная проверка здоровья модели
- Авто-откат при деградации метрик
- Логирование всех процессов

---

## 🏗 Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        ProjectZZZ Architecture                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
│  │   Excel      │     │   PostgreSQL │     │   Parquet    │   │
│  │   (raw/)     │────▶│   (БД)       │────▶│   (processed/)│  │
│  └──────────────┘     └──────────────┘     └──────────────┘   │
│         │                    │                    │            │
│         ▼                    ▼                    ▼            │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐   │
│  │ load_        │     │ backtest_    │     │ generate_    │   │
│  │ references   │     │ engine       │     │ recommend.   │   │
│  └──────────────┘     └──────────────┘     └──────────────┘   │
│                              │                    │            │
│                              ▼                    ▼            │
│                       ┌──────────────┐     ┌──────────────┐   │
│                       │ model_       │     │ visit_       │   │
│                       │ lightgbm_v1  │     │ proposals    │   │
│                       └──────────────┘     └──────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Компоненты системы

| Компонент | Описание | Файл |
|-----------|----------|------|
| **ETL Pipeline** | Загрузка справочников и продаж | `load_references.py`, `load_sales.py` |
| **Backtest Engine** | Генерация обучающей выборки | `backtest_engine.py` |
| **Model Training** | Обучение LightGBM + калибровка | `model_lightgbm_v1.py` |
| **Recommendations** | Генерация рекомендаций 2+2+1 | `generate_recommendations.py` |
| **A/B Testing** | Сравнение моделей, авто-переключение | `evaluate_ab.py` |
| **Model Controller** | Управление версиями моделей | `model_controller.py` |

---

## 📦 Установка

### Требования

- Python 3.10+
- PostgreSQL 14+
- 4+ GB RAM (для больших датасетов)
- 10+ GB свободного места на диске

### Шаг 1: Клонирование репозитория

```bash
git clone https://github.com/yourusername/projectzzz.git
cd projectzzz
```

### Шаг 2: Создание виртуального окружения

```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
```

### Шаг 3: Установка зависимостей

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Шаг 4: Настройка базы данных

```sql
-- Создание БД
CREATE DATABASE project_zzz;
CREATE USER pzzz_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE project_zzz TO pzzz_user;
```

### Шаг 5: Конфигурация

Создайте файл `config/config.yaml`:

```yaml
database:
  host: localhost
  port: 5432
  name: project_zzz
  user: pzzz_user
  password: your_password

ab_test:
  enabled: true
  test_group_ratio: 0.5
  promotion:
    min_duration_days: 14
    significance_level: 0.05
    min_uplift: 0.03
    auto_promote: true
  degradation:
    critical_threshold: 0.05
```

---

## 🚀 Быстрый старт

### 1. Загрузка данных

```bash
# Умная загрузка справочников
python src/load_references.py

# Умная загрузка продаж
python src/load_sales.py

# Принудительная загрузка (удаление старых данных)
python src/load_sales.py --force
```

### 2. Обучение модели

```bash
# Генерация обучающей выборки (backtesting)
python src/backtest_engine.py

# Обучение модели LightGBM
python src/model_lightgbm_v1.py
```

### 3. Генерация рекомендаций

```bash
# Ежедневная генерация рекомендаций
python src/generate_recommendations.py
```

### 4. Запуск GUI панели

```bash
# Панель управления v2.0
python src/gui_control_panel.py

# Простая панель
python src/dashboard_gui.py
```

---

## 💻 Использование

### Python API

#### Генерация рекомендаций

```python
from src.generate_recommendations import (
    load_active_model,
    get_clients_for_today,
    get_candidate_skus_batch,
    select_2plus2plus1
)
from sqlalchemy import create_engine

# Подключение к БД
engine = create_engine("postgresql://user:pass@localhost/project_zzz")

# Загрузка модели
model, calibrator, encoders, feature_cols, best_iter = load_active_model()

# Получение клиентов
clients = get_clients_for_today(engine)

# Загрузка кандидатов
df_candidates = get_candidate_skus_batch(engine, clients)

# Генерация рекомендаций для клиента
client_df = df_candidates[df_candidates['client_id'] == 'C123']
selected, fallback = select_2plus2plus1(client_df)

print(f"Рекомендации: {len(selected)} SKU")
for rec in selected:
    print(f"  • {rec['sku_name']} ({rec['selection_type']})")
```

#### Управление моделями

```python
from src.model_controller import ModelController
from pathlib import Path

controller = ModelController(
    models_dir=Path("models"),
    registry_path=Path("models/model_registry.json")
)

# Получить активную модель
active = controller.get_active_model()
print(f"Active model: {active}")

# Загрузить модель
model_data = controller.load_model()

# Проверка здоровья
health = controller.weekly_health_check(current_metrics={
    'precision_5': 0.40,
    'hit_rate': 0.65,
    'brier_score': 0.16
})

if not health['healthy']:
    controller.rollback(reason="degradation_detected")
```

### Конвертация в Parquet

```bash
# Конвертация всех CSV в processed/
python src/convert_to_parquet.py

# Конвертация одного файла
python src/convert_to_parquet.py -i data/processed/backtest_dataset.csv

# С сжатием gzip
python src/convert_to_parquet.py -c gzip
```

### Архивация результатов

```bash
# Архивация файлов старше 30 дней
python src/archive_output.py --days 30

# С удалением оригиналов
python src/archive_output.py --days 30 --delete

# Тестовый режим (без действий)
python src/archive_output.py --dry-run
```

---

## 🧪 Тестирование

### Запуск unit-тестов

```bash
# Все тесты
pytest tests/ -v

# С покрытием
pytest tests/ -v --cov=src

# Только тесты backtest_engine
pytest tests/test_backtest_engine.py -v

# Тесты с маркером edge_case
pytest tests/ -v -m edge_case
```

### Покрытие кода

Отчёт о покрытии генерируется в `docs/coverage/index.html`:

```bash
pytest tests/ --cov=src --cov-report=html
```

### Написание тестов

```python
# tests/test_example.py
import pytest
from src.backtest_engine import process_batch

def test_empty_dataframe():
    """Тест обработки пустого DataFrame"""
    import pandas as pd
    result = process_batch(pd.DataFrame())
    assert result.empty
```

---

## 📁 Структура проекта

```
D:\ProjectZZZ/
├── config/
│   ├── config.yaml              # Основная конфигурация
│   ├── model_registry.json      # Реестр моделей
│   └── references_meta.json     # Метаданные справочников
├── data/
│   ├── raw/                     # Исходные Excel файлы
│   ├── processed/               # Обработанные данные (CSV/Parquet)
│   ├── output/                  # Рекомендации (XLSX)
│   └── archive/                 # Архивы старых файлов
├── models/
│   ├── model_lightgbm_v1_*.pkl  # Сохранённые модели
│   ├── calibrator_*.pkl         # Калибраторы
│   ├── encoders_*.pkl           # Энкодеры
│   └── model_controller.py      # Управление версиями
├── src/
│   ├── load_references.py       # Загрузка справочников
│   ├── load_sales.py            # Загрузка продаж
│   ├── backtest_engine.py       # Backtesting
│   ├── model_lightgbm_v1.py     # Обучение модели
│   ├── generate_recommendations.py  # Генерация рекомендаций
│   ├── evaluate_ab.py           # A/B тестирование
│   ├── convert_to_parquet.py    # Конвертация в Parquet
│   ├── archive_output.py        # Архивация
│   └── *.py                     # Другие утилиты
├── tests/
│   ├── test_backtest_engine.py  # Тесты backtest_engine
│   └── test_*.py                # Другие тесты
├── docs/
│   ├── logs/                    # Логи выполнения
│   └── coverage/                # Отчёты о покрытии
├── venv/                        # Виртуальное окружение
├── requirements.txt             # Python зависимости
├── pytest.ini                   # Конфигурация pytest
└── README.md                    # Этот файл
```

---

## 📦 Зависимости

### Основные

| Пакет | Версия | Назначение |
|-------|--------|------------|
| pandas | 2.2.3 | Обработка данных |
| numpy | 1.26.4 | Численные вычисления |
| scipy | 1.13.1 | Статистика |
| scikit-learn | 1.5.2 | ML утилиты |
| lightgbm | 4.5.0 | Градиентный бустинг |
| implicit | 0.7.2 | Коллаборативная фильтрация |

### База данных

| Пакет | Версия | Назначение |
|-------|--------|------------|
| SQLAlchemy | 2.0.36 | ORM |
| psycopg2-binary | 2.9.10 | PostgreSQL драйвер |

### Файлы

| Пакет | Версия | Назначение |
|-------|--------|------------|
| openpyxl | 3.1.5 | Чтение Excel |
| pyyaml | 6.0.2 | YAML конфиги |
| pyarrow | 15.0+ | Parquet (опционально) |

### Тестирование

| Пакет | Версия | Назначение |
|-------|--------|------------|
| pytest | 7.4.0+ | Тестирование |
| pytest-cov | 4.1.0 | Покрытие кода |

---

## ⚡ Производительность

### Оптимизации

1. **Пакетная обработка** — загрузка клиентов пакетами по 100-500
2. **Индексы PostgreSQL** — композитные индексы для JOIN
3. **Parquet сжатие** — сокращение размера в 3-5 раз
4. **Кэширование признаков** — ускорение предсказаний

### Бенчмарки

| Операция | Время | Данные |
|----------|-------|--------|
| Загрузка справочников | ~30 сек | 3 файла Excel |
| Загрузка продаж | ~2 мин | 12 файлов Excel |
| Backtesting | ~10 мин | 100K клиентов |
| Обучение модели | ~5 мин | 1M примеров |
| Генерация рекомендаций | ~1 мин | 1000 клиентов |

---

## 🤝 Вклад в проект

### Pull Request Process

1. Создайте fork репозитория
2. Создайте ветку (`git checkout -b feature/AmazingFeature`)
3. Закоммитьте изменения (`git commit -m 'Add AmazingFeature'`)
4. Запушьте (`git push origin feature/AmazingFeature`)
5. Откройте Pull Request

### Требования к коду

- Следуйте PEP 8
- Добавляйте type hints
- Покрывайте тестами новую функциональность
- Обновляйте документацию

---

## 📄 Лицензия

Этот проект распространяется под лицензией MIT. См. файл [LICENSE](LICENSE) для деталей.

---

## 📚 Документация

Полная документация проекта расположена в папке [`/docs/`](docs/):

| Документ | Описание |
|----------|----------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Подробное описание архитектуры системы, компонентов и потоков данных |
| [API.md](docs/API.md) | API документация с примерами использования всех модулей |
| [DEVELOPMENT.md](docs/DEVELOPMENT.md) | Руководство разработчика: настройка окружения, стиль кода, тестирование |

### Быстрые ссылки

- 🏗 [Архитектура](docs/ARCHITECTURE.md) — компоненты, поток данных, развёртывание
- 🔌 [API](docs/API.md) — Recommendation API, Model Management, Data Access
- 👨‍💻 [Разработка](docs/DEVELOPMENT.md) — настройка, стиль кода, добавление функций

---

## 📞 Контакты

- **Project Page**: [GitHub](https://github.com/Simbiriamen/ProjectZZZ)
- **Issues**: [GitHub Issues](https://github.com/Simbiriamen/ProjectZZZ/issues)

---

## 🙏 Благодарности

- [LightGBM](https://lightgbm.readthedocs.io/) — градиентный бустинг
- [pandas](https://pandas.pydata.org/) — обработка данных
- [PostgreSQL](https://www.postgresql.org/) — база данных
- [pytest](https://docs.pytest.org/) — тестирование
