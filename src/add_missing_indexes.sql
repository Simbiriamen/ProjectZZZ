-- =============================================================================
-- add_missing_indexes.sql v1.0
-- 🔧 ДОБАВЛЕНИЕ ИНДЕКСОВ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ
-- Назначение:
--   - Создание индексов для ускорения JOIN и WHERE
--   - Проверка существующих индексов
--   - Ускорение массовых UPDATE операций
--
-- Использование:
--   psql -U postgres -d project_zzz -f add_missing_indexes.sql
-- =============================================================================

-- =============================================================================
-- 1. ПРОВЕРКА СУЩЕСТВУЮЩИХ ИНДЕКСОВ
-- =============================================================================

\echo '=================================================='
\echo '📊 ПРОВЕРКА СУЩЕСТВУЮЩИХ ИНДЕКСОВ'
\echo '=================================================='

SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

-- =============================================================================
-- 2. ИНДЕКСЫ ДЛЯ TABLE: purchases
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '📁 ИНДЕКСЫ: purchases'
\echo '=================================================='

-- Индекс для JOIN по client_id
CREATE INDEX IF NOT EXISTS idx_purchases_client_id 
ON purchases(client_id);

-- Индекс для JOIN по sku_id
CREATE INDEX IF NOT EXISTS idx_purchases_sku_id 
ON purchases(sku_id);

-- Индекс для фильтрации по датам
CREATE INDEX IF NOT EXISTS idx_purchases_purchase_date 
ON purchases(purchase_date);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для частых запросов (client + date)
CREATE INDEX IF NOT EXISTS idx_purchases_client_date 
ON purchases(client_id, purchase_date DESC);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для группировок (client + sku)
CREATE INDEX IF NOT EXISTS idx_purchases_client_sku 
ON purchases(client_id, sku_id);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для backtest_engine (client + sku + date)
CREATE INDEX IF NOT EXISTS idx_purchases_client_sku_date 
ON purchases(client_id, sku_id, purchase_date DESC);

-- =============================================================================
-- 3. ИНДЕКСЫ ДЛЯ TABLE: sales_enriched
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '📁 ИНДЕКСЫ: sales_enriched'
\echo '=================================================='

-- Индекс для JOIN по client_id
CREATE INDEX IF NOT EXISTS idx_sales_enriched_client_id 
ON sales_enriched(client_id);

-- Индекс для JOIN по sku_id
CREATE INDEX IF NOT EXISTS idx_sales_enriched_sku_id 
ON sales_enriched(sku_id);

-- Индекс для фильтрации по датам
CREATE INDEX IF NOT EXISTS idx_sales_enriched_purchase_date 
ON sales_enriched(purchase_date);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для частых запросов
CREATE INDEX IF NOT EXISTS idx_sales_enriched_client_date 
ON sales_enriched(client_id, purchase_date DESC);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для группировок
CREATE INDEX IF NOT EXISTS idx_sales_enriched_client_sku 
ON sales_enriched(client_id, sku_id);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для backtest_results JOIN
CREATE INDEX IF NOT EXISTS idx_sales_enriched_client_sku_date 
ON sales_enriched(client_id, sku_id, purchase_date DESC);

-- =============================================================================
-- 4. ИНДЕКСЫ ДЛЯ TABLE: backtest_results
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '📁 ИНДЕКСЫ: backtest_results'
\echo '=================================================='

-- Индекс для JOIN с sales_enriched
CREATE INDEX IF NOT EXISTS idx_backtest_client_id 
ON backtest_results(client_id);

CREATE INDEX IF NOT EXISTS idx_backtest_sku_id 
ON backtest_results(sku_id);

-- Индекс для фильтрации по датам
CREATE INDEX IF NOT EXISTS idx_backtest_last_purchase_date 
ON backtest_results(last_purchase_date);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для JOIN (client + sku + date)
CREATE INDEX IF NOT EXISTS idx_backtest_client_sku_date 
ON backtest_results(client_id, sku_id, last_purchase_date);

-- Индекс для фильтрации по target (баланс классов)
CREATE INDEX IF NOT EXISTS idx_backtest_target 
ON backtest_results(target);

-- =============================================================================
-- 5. ИНДЕКСЫ ДЛЯ TABLE: visit_proposals
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '📁 ИНДЕКСЫ: visit_proposals'
\echo '=================================================='

-- Индекс для поиска по клиенту
CREATE INDEX IF NOT EXISTS idx_visit_proposals_client_id 
ON visit_proposals(client_id);

-- Индекс для фильтрации по датам
CREATE INDEX IF NOT EXISTS idx_visit_proposals_visit_date 
ON visit_proposals(visit_date);

-- Индекс для A/B тестирования
CREATE INDEX IF NOT EXISTS idx_visit_proposals_ab_group 
ON visit_proposals(ab_group);

-- 🔧 КОМПОЗИТНЫЙ ИНДЕКС для отчётов
CREATE INDEX IF NOT EXISTS idx_visit_proposals_date_client 
ON visit_proposals(visit_date, client_id);

-- =============================================================================
-- 6. ИНДЕКСЫ ДЛЯ TABLE: clients
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '📁 ИНДЕКСЫ: clients'
\echo '=================================================='

-- Индекс для JOIN по client_id (primary key обычно уже есть)
CREATE INDEX IF NOT EXISTS idx_clients_client_id 
ON clients(client_id);

-- Индекс для фильтрации по менеджеру
CREATE INDEX IF NOT EXISTS idx_clients_manager_id 
ON clients(manager_id);

-- Индекс для фильтрации по каналу сбыта
CREATE INDEX IF NOT EXISTS idx_clients_sales_channel 
ON clients(sales_channel);

-- =============================================================================
-- 7. ИНДЕКСЫ ДЛЯ TABLE: skus
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '📁 ИНДЕКСЫ: skus'
\echo '=================================================='

-- Индекс для JOIN по sku_id
CREATE INDEX IF NOT EXISTS idx_skus_sku_id 
ON skus(sku_id);

-- Индекс для фильтрации по наличию
CREATE INDEX IF NOT EXISTS idx_skus_stock 
ON skus(stock);

-- Индекс для фильтрации по новинкам
CREATE INDEX IF NOT EXISTS idx_skus_is_new 
ON skus(is_new);

-- Индекс для группировки по маркетинговой группе
CREATE INDEX IF NOT EXISTS idx_skus_marketing_group 
ON skus(marketing_group1);

-- =============================================================================
-- 8. АНАЛИЗ РАЗМЕРА ИНДЕКСОВ
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '📊 РАЗМЕР ИНДЕКСОВ'
\echo '=================================================='

SELECT 
    relname AS index_name,
    pg_size_pretty(pg_relation_size(relid)) AS index_size,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY pg_relation_size(relid) DESC
LIMIT 20;

-- =============================================================================
-- 9. РЕКОМЕНДАЦИИ ПО ОБСЛУЖИВАНИЮ
-- =============================================================================

\echo ''
\echo '=================================================='
\echo '🔧 РЕКОМЕНДАЦИИ ПО ОБСЛУЖИВАНИЮ'
\echo '=================================================='

\echo 'Для оптимизации индексов выполните:'
\echo '  REINDEX TABLE purchases;'
\echo '  REINDEX TABLE sales_enriched;'
\echo '  REINDEX TABLE backtest_results;'
\echo ''
\echo 'Для обновления статистики:'
\echo '  ANALYZE purchases;'
\echo '  ANALYZE sales_enriched;'
\echo '  ANALYZE backtest_results;'
\echo ''
\echo 'Для мониторинга неиспользуемых индексов:'
\echo '  SELECT * FROM pg_stat_user_indexes WHERE idx_scan = 0;'

\echo ''
\echo '=================================================='
\echo '✅ СОЗДАНИЕ ИНДЕКСОВ ЗАВЕРШЕНО'
\echo '=================================================='
