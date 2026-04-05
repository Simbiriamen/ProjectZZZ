# 📋 План реализации рефакторинга ProjectZZZ

**Версия:** 1.0  
**Дата начала:** 2026-04-05  
**Расчётная длительность:** 6 недель

---

## 🎯 Цели рефакторинга

1. **Улучшить тестируемость** — покрытие ≥ 70%
2. **Повысить поддерживаемость** — чёткое разделение ответственности
3. **Обеспечить масштабируемость** — подготовка к Docker/Kubernetes
4. **Устранить дублирование** — DRY принцип
5. **Внедрить абстракции** — DIP, Protocol-based архитектура

---

## 📅 Этап 1: Реструктуризация (Неделя 1-2)

### Задачи

#### 1.1 Создание новой структуры папок

```bash
# Создать директорию новой структуры
mkdir -p src/{config,domain,application,infrastructure,interfaces,shared}
mkdir -p src/domain/{entities,services,exceptions,protocols}
mkdir -p src/application/{commands,handlers}
mkdir -p src/infrastructure/{database,ml,cache,export}
mkdir -p src/interfaces/{cli,api,gui}
mkdir -p tests/{unit,integration,e2e}
```

**Файлы для создания:**
- [ ] `src/__init__.py`
- [ ] `src/main.py` (новая точка входа)
- [ ] `src/config/__init__.py`
- [ ] `src/config/settings.py` (pydantic settings)
- [ ] `src/domain/__init__.py`
- [ ] `src/domain/entities/__init__.py`
- [ ] `src/domain/entities/client.py`
- [ ] `src/domain/entities/sku.py`
- [ ] `src/domain/entities/recommendation.py`
- [ ] `src/domain/services/__init__.py`
- [ ] `src/domain/protocols/__init__.py`
- [ ] `src/domain/protocols/repositories.py`
- [ ] `src/domain/exceptions/__init__.py`
- [ ] `src/application/__init__.py`
- [ ] `src/infrastructure/__init__.py`
- [ ] `src/shared/__init__.py`

#### 1.2 Миграция database.py

**Текущий файл:** `src/database.py` (485 строк)

**Новые файлы:**
- [ ] `src/infrastructure/database/connection.py` — подключение к БД
- [ ] `src/infrastructure/database/repositories/__init__.py`
- [ ] `src/infrastructure/database/repositories/client_repo.py` — ClientRepository
- [ ] `src/infrastructure/database/repositories/purchase_repo.py` — PurchaseRepository
- [ ] `src/infrastructure/database/repositories/candidate_repo.py` — CandidateRepository
- [ ] `src/infrastructure/database/queries.py` — централизованные SQL запросы

**Изменения:**
- Убрать зависимость от `PROJECT_ROOT`
- Использовать dependency injection
- Добавить type hints
- Вынести SQL в отдельный модуль

#### 1.3 Миграция services.py

**Текущий файл:** `src/services.py` (524 строки)

**Новые файлы:**
- [ ] `src/domain/services/model_service.py` — ModelService (без зависимости от БД)
- [ ] `src/domain/services/candidate_service.py` — CandidateService
- [ ] `src/domain/services/recommendation_service.py` — RecommendationService
- [ ] `src/domain/services/persistence_service.py` — PersistenceService

**Изменения:**
- Убрать прямые импорты из infrastructure
- Использовать протоколы вместо конкретных классов
- Добавить абстрактные базовые классы где нужно

---

## 📅 Этап 2: Внедрение абстракций (Неделя 3)

### Задачи

#### 2.1 Протоколы для репозиториев

**Файл:** `src/domain/protocols/repositories.py`

```python
from typing import Protocol, List
import pandas as pd
from datetime import date

class ClientRepositoryProtocol(Protocol):
    def get_active_clients(self, months: int = 12, min_purchases: int = 3) -> List[str]: ...
    def get_clients_for_visit(self, visit_date: date) -> List[str]: ...
    def get_client_names(self, client_ids: List[str]) -> dict[str, str]: ...

class PurchaseRepositoryProtocol(Protocol):
    def get_client_history(self, client_id: str, days: int = 90) -> pd.DataFrame: ...
    def get_raw_purchases_chunk(self, client_ids: List[str], months: int = 12) -> pd.DataFrame: ...
    def get_popular_skus(self, min_purchases: int = 2) -> pd.Series: ...

class CandidateRepositoryProtocol(Protocol):
    def get_candidates(
        self,
        client_ids: List[str],
        batch_size: int = 200,
        days: int = 90,
        top_n: int = 200
    ) -> pd.DataFrame: ...
```

#### 2.2 Dependency Injection контейнер

**Файл:** `src/shared/di_container.py`

```python
from dependency_injector import containers, providers
from .config.settings import Settings

class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    settings = providers.Singleton(Settings)
    
    # Infrastructure
    database = providers.Singleton(
        Database,
        url=settings.database.url
    )
    
    # Repositories
    client_repo = providers.Factory(
        ClientRepository,
        db=database
    )
    
    candidate_repo = providers.Factory(
        CandidateRepository,
        db=database
    )
    
    # Domain Services
    model_service = providers.Factory(ModelService)
    
    recommendation_service = providers.Factory(
        RecommendationService,
        model_service=model_service
    )
```

#### 2.3 Конфигурация через pydantic

**Файл:** `src/config/settings.py`

```python
from pydantic import BaseSettings, Field
from typing import Optional

class DatabaseSettings(BaseSettings):
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    name: str = Field(default="project_zzz_db", env="DB_NAME")
    user: str = Field(default="postgres", env="DB_USER")
    password: str = Field(env="DB_PASSWORD")
    
    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

class ABTestSettings(BaseSettings):
    enabled: bool = False
    test_group_ratio: float = 0.5
    start_date: Optional[str] = None

class Settings(BaseSettings):
    database: DatabaseSettings
    ab_test: ABTestSettings
    paths: dict
    
    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"
```

---

## 📅 Этап 3: Рефакторинг generate_recommendations.py (Неделя 4)

### Задачи

#### 3.1 Разделение на команды CQRS

**Старый файл:** `src/generate_recommendations.py` (699 строк)

**Новые файлы:**
- [ ] `src/application/commands/generate_recommendations.py` — команда
- [ ] `src/application/handlers/generate_recommendations_handler.py` — обработчик
- [ ] `src/domain/services/feature_encoder.py` — кодирование признаков
- [ ] `src/domain/services/model_predictor.py` — предсказания модели
- [ ] `src/infrastructure/export/excel_exporter.py` — экспорт в Excel
- [ ] `src/infrastructure/export/recommendations_exporter.py` — общий экспортер

#### 3.2 Новая структура команды

```python
# src/application/commands/generate_recommendations.py
from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class GenerateRecommendationsCommand:
    visit_date: date
    client_ids: Optional[List[str]] = None
    batch_size: int = 100
    force_regenerate: bool = False

@dataclass
class GenerateRecommendationsResult:
    total_clients: int
    total_recommendations: int
    fallback_rate: float
    output_file: str
```

#### 3.3 Обработчик команды

```python
# src/application/handlers/generate_recommendations_handler.py
class GenerateRecommendationsHandler:
    def __init__(
        self,
        client_repo: ClientRepositoryProtocol,
        candidate_repo: CandidateRepositoryProtocol,
        recommendation_service: RecommendationService,
        model_predictor: ModelPredictor,
        exporter: RecommendationsExporter
    ):
        self.client_repo = client_repo
        self.candidate_repo = candidate_repo
        self.recommendation_service = recommendation_service
        self.model_predictor = model_predictor
        self.exporter = exporter
    
    def handle(self, command: GenerateRecommendationsCommand) -> GenerateRecommendationsResult:
        # 1. Получить клиентов
        client_ids = command.client_ids or self._get_clients_for_visit(command.visit_date)
        
        # 2. Загрузить кандидатов (пакетами)
        candidates = self.candidate_repo.get_candidates(client_ids, batch_size=command.batch_size)
        
        # 3. Сгенерировать предсказания
        predictions = self.model_predictor.predict(candidates)
        
        # 4. Применить бизнес-правила
        recommendations = self.recommendation_service.apply_rules(predictions)
        
        # 5. Сохранить результаты
        self.exporter.export(recommendations, command.visit_date)
        
        return GenerateRecommendationsResult(...)
```

---

## 📅 Этап 4: Улучшение тестируемости (Неделя 5)

### Задачи

#### 4.1 Моки и фикстуры

**Файл:** `tests/conftest.py`

```python
import pytest
from unittest.mock import Mock, MagicMock
import pandas as pd

@pytest.fixture
def mock_client_repository():
    repo = Mock(spec=ClientRepositoryProtocol)
    repo.get_active_clients.return_value = ['C1', 'C2', 'C3']
    repo.get_client_names.return_value = {
        'C1': 'Client 1',
        'C2': 'Client 2'
    }
    return repo

@pytest.fixture
def mock_candidate_repository():
    repo = Mock(spec=CandidateRepositoryProtocol)
    repo.get_candidates.return_value = pd.DataFrame({
        'client_id': ['C1', 'C1', 'C2'],
        'sku_id': ['S1', 'S2', 'S1'],
        'is_new_for_client': [0, 1, 0],
        # ... другие колонки
    })
    return repo

@pytest.fixture
def sample_recommendations():
    return [
        {
            'client_id': 'C1',
            'sku_id': 'S1',
            'predicted_prob': 0.85,
            'selection_type': 'new',
            # ...
        }
    ]
```

#### 4.2 Unit тесты для сервисов

**Файл:** `tests/unit/domain/test_recommendation_service.py`

```python
class TestRecommendationService:
    def test_generate_for_client_with_new_skus(self, sample_candidates):
        service = RecommendationService()
        selected, fallback = service.generate_for_client(sample_candidates)
        
        assert len(selected) == 5
        assert any(s['selection_type'] == 'new' for s in selected)
    
    def test_fallback_when_no_new_candidates(self):
        service = RecommendationService()
        empty_new_df = pd.DataFrame({...})  # Только знакомые SKU
        
        selected, fallback = service.generate_for_client(empty_new_df)
        
        assert 'No_new_candidates_at_all' in fallback
```

#### 4.3 Интеграционные тесты

**Файл:** `tests/integration/test_full_pipeline.py`

```python
@pytest.mark.integration
class TestFullPipeline:
    def test_end_to_end_recommendation_generation(self, test_database):
        # Arrange
        handler = GenerateRecommendationsHandler(...)
        command = GenerateRecommendationsCommand(
            visit_date=date.today(),
            client_ids=['TEST_C1']
        )
        
        # Act
        result = handler.handle(command)
        
        # Assert
        assert result.total_clients == 1
        assert result.total_recommendations == 5
        assert os.path.exists(result.output_file)
```

---

## 📅 Этап 5: Backtest Engine и документация (Неделя 6)

### Задачи

#### 5.1 Рефакторинг backtest_engine.py

**Старый файл:** `src/backtest_engine.py` (339 строк)

**Новые файлы:**
- [ ] `src/application/commands/run_backtest.py` — команда backtest
- [ ] `src/application/handlers/backtest_handler.py` — обработчик
- [ ] `src/domain/services/backtest_service.py` — бизнес-логика backtest
- [ ] `src/infrastructure/database/backtest_repository.py` — сохранение результатов

#### 5.2 Обновление документации

- [ ] Переписать `README.md` с новой структурой
- [ ] Добавить диаграммы в `docs/ARCHITECTURE.md`
- [ ] Обновить `docs/API.md` с новыми endpoints
- [ ] Создать `docs/MIGRATION_GUIDE.md` для перехода со старой версии

#### 5.3 CI/CD настройка

**Файл:** `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install flake8 black mypy
      - name: Lint
        run: |
          flake8 src/
          black --check src/
          mypy src/

  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest --cov=src --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

---

## 📊 Метрики прогресса

### Неделя 1
- [ ] Создана новая структура папок
- [ ] Перемещён `database.py`
- [ ] Настроены протоколы

### Неделя 2
- [ ] Перемещён `services.py`
- [ ] Настроен DI контейнер
- [ ] Pydantic settings работают

### Неделя 3
- [ ] Все протоколы реализованы
- [ ] DI внедрён во все сервисы
- [ ] Конфигурация через env vars

### Неделя 4
- [ ] `generate_recommendations.py` разделён
- [ ] Команды CQRS работают
- [ ] Экспорт вынесен в инфраструктуру

### Неделя 5
- [ ] Покрытие тестами ≥ 50%
- [ ] Моки для всех репозиториев
- [ ] Интеграционные тесты работают

### Неделя 6
- [ ] `backtest_engine.py` рефакторён
- [ ] Документация обновлена
- [ ] CI/CD пайплайн настроен
- [ ] Покрытие тестами ≥ 70%

---

## ⚠️ Риски и митигация

| Риск | Вероятность | Влияние | Митигация |
|------|-------------|---------|-----------|
| Поломка существующего функционала | Средняя | Высокое | Писать тесты перед рефакторингом |
| Конфликты при слиянии | Средняя | Среднее | Рефакторить маленькими PR |
| Потеря производительности | Низкая | Высокое | Бенчмарки до/после |
| Сопротивление команды | Низкая | Среднее | Объяснить преимущества, провести демо |

---

## ✅ Чеклист готовности к продакшену

- [ ] Все unit тесты проходят
- [ ] Интеграционные тесты проходят
- [ ] Покрытие ≥ 70%
- [ ] Нет циклических зависимостей (проверить `pylint`)
- [ ] Типизация проверена (`mypy`)
- [ ] Код отформатирован (`black`)
- [ ] Документация актуальна
- [ ] CI/CD пайплайн зелёный
- [ ] Бенчмарки производительности в норме
- [ ] Откат возможен (старая версия сохранена)

---

**Примечание:** Этот план может корректироваться по ходу реализации в зависимости от выявленных проблем и приоритетов бизнеса.
