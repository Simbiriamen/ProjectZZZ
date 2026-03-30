-- ============================================================================
-- ProjectZZZ: Схема базы данных PostgreSQL (Версия 3.1)
-- Согласно ReadMe_ProjectZZZ.txt, раздел 5.2
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Маркетинговые группы (справочник)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marketing_groups (
    group_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    parent_group_id INTEGER REFERENCES marketing_groups(group_id),
    trend_config VARCHAR(50) DEFAULT 'auto',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. Клиенты
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clients (
    client_id VARCHAR(50) PRIMARY KEY,
    client_name VARCHAR(255),
    segment VARCHAR(50) DEFAULT 'standard',
    manager_id VARCHAR(50),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 3. Товары (SKU)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skus (
    sku_id VARCHAR(50) PRIMARY KEY,
    sku_name VARCHAR(255),
    category VARCHAR(100),
    group_id INTEGER REFERENCES marketing_groups(group_id),
    price DECIMAL(12, 2),
    margin DECIMAL(5, 4),
    stock INTEGER DEFAULT 0,
    is_new BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 4. История покупок
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS purchases (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) NOT NULL REFERENCES clients(client_id),
    sku_id VARCHAR(50) NOT NULL REFERENCES skus(sku_id),
    purchase_date DATE NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_purchases_client_date ON purchases(client_id, purchase_date);

-- ----------------------------------------------------------------------------
-- 5. План визитов менеджеров
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS visits_schedule (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) NOT NULL REFERENCES clients(client_id),
    planned_visit_date DATE NOT NULL,
    status VARCHAR(50) DEFAULT 'planned',
    actual_visit_date DATE,
    manager_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_visits_date ON visits_schedule(planned_visit_date);

-- ----------------------------------------------------------------------------
-- 6. Рекомендации + Обратная связь (в одной таблице!)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS visit_proposals (
    id SERIAL PRIMARY KEY,
    visit_date DATE NOT NULL,
    client_id VARCHAR(50) NOT NULL REFERENCES clients(client_id),
    sku_id VARCHAR(50) NOT NULL REFERENCES skus(sku_id),
    predicted_prob DECIMAL(5, 4),
    calibrated_prob DECIMAL(5, 4),
    selection_type VARCHAR(20),
    was_recommended BOOLEAN DEFAULT FALSE,
    fallback_reason VARCHAR(255),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_proposals_visit_client ON visit_proposals(visit_date, client_id);

-- ----------------------------------------------------------------------------
-- 7. Кэш признаков (для ускорения генерации)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features_cache (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) NOT NULL,
    sku_id VARCHAR(50) NOT NULL,
    feature_name VARCHAR(100) NOT NULL,
    feature_value DECIMAL(15, 6),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(client_id, sku_id, feature_name)
);

CREATE INDEX IF NOT EXISTS idx_features_client_sku ON features_cache(client_id, sku_id);

-- ----------------------------------------------------------------------------
-- 8. Лог предсказаний модели (для аудита)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_predictions_log (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) NOT NULL,
    sku_id VARCHAR(50) NOT NULL,
    prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_version VARCHAR(50) NOT NULL,
    raw_probability DECIMAL(5, 4),
    calibrated_probability DECIMAL(5, 4),
    features_snapshot JSONB
);

-- ----------------------------------------------------------------------------
-- 9. Результаты Backtesting
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS backtest_results (
    id SERIAL PRIMARY KEY,
    test_date DATE NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    precision_5 DECIMAL(5, 4),
    hit_rate DECIMAL(5, 4),
    brier_score DECIMAL(5, 4),
    training_time_hours DECIMAL(8, 2),
    samples_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 10. Справочник замен и аналогов
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS substitutes (
    id SERIAL PRIMARY KEY,
    sku_id VARCHAR(50) NOT NULL REFERENCES skus(sku_id),
    substitute_sku_id VARCHAR(50) NOT NULL REFERENCES skus(sku_id),
    substitute_type VARCHAR(50) DEFAULT 'analog',
    confidence DECIMAL(5, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sku_id, substitute_sku_id)
);

-- ----------------------------------------------------------------------------
-- 11. A/B тестирование
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ab_test_assignments (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) NOT NULL UNIQUE REFERENCES clients(client_id),
    test_group VARCHAR(20) NOT NULL,
    assigned_date DATE DEFAULT CURRENT_DATE,
    model_version VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_ab_test_group ON ab_test_assignments(test_group);