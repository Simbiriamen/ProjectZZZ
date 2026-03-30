-- ============================================================================
-- ProjectZZZ: Добавление недостающих колонок
-- Дата генерации: 2026-03-21
-- Основано на: diagnose_database.py
-- ============================================================================

-- ============================================================================
-- 1. ТАБЛИЦА: skus (КРИТИЧНО для логики отбора!)
-- ============================================================================

-- Остатки (критично: фильтр "остаток >= 1")
ALTER TABLE skus ADD COLUMN IF NOT EXISTS stock INTEGER DEFAULT 0;
COMMENT ON COLUMN skus.stock IS 'Остаток на складе (шт)';

-- Маржинальность (критично: для скоринга рекомендаций)
ALTER TABLE skus ADD COLUMN IF NOT EXISTS margin DECIMAL(5,4);
COMMENT ON COLUMN skus.margin IS 'Маржинальность (0.0000 - 1.0000)';

-- Цена (критично: для расчёта суммы предложения)
ALTER TABLE skus ADD COLUMN IF NOT EXISTS price DECIMAL(12,2);
COMMENT ON COLUMN skus.price IS 'Цена продажи (руб)';

-- Категория (для группировки)
ALTER TABLE skus ADD COLUMN IF NOT EXISTS category TEXT;
COMMENT ON COLUMN skus.category IS 'Категория товара';

-- ID группы (для связей)
ALTER TABLE skus ADD COLUMN IF NOT EXISTS group_id TEXT;
COMMENT ON COLUMN skus.group_id IS 'ID маркетинговой группы';

-- Флаг новинки (для правила "2 новых")
ALTER TABLE skus ADD COLUMN IF NOT EXISTS is_new TEXT;
COMMENT ON COLUMN skus.is_new IS 'Флаг новинки (TRUE/FALSE)';

-- Индексы для ускорения отбора
CREATE INDEX IF NOT EXISTS idx_skus_stock ON skus(stock);
CREATE INDEX IF NOT EXISTS idx_skus_margin ON skus(margin);
CREATE INDEX IF NOT EXISTS idx_skus_is_new ON skus(is_new);

-- ============================================================================
-- 2. ТАБЛИЦА: clients (ДОПОЛНИТЕЛЬНО)
-- ============================================================================

-- Сегмент клиента (A/B/C)
ALTER TABLE clients ADD COLUMN IF NOT EXISTS segment TEXT;
COMMENT ON COLUMN clients.segment IS 'Сегмент клиента (A/B/C)';

-- Метаданные (JSON для гибкости)
ALTER TABLE clients ADD COLUMN IF NOT EXISTS metadata JSONB;
COMMENT ON COLUMN clients.metadata IS 'Дополнительные данные (JSON)';

-- ============================================================================
-- 3. ТАБЛИЦА: purchases (НЕ ТРЕБУЕТСЯ)
-- ============================================================================
-- Колонки brand, product_group, marketing_group НЕ добавляем!
-- Они будут подгружаться через JOIN с skus при обогащении.
-- Это нормализованная структура БД.

-- ============================================================================
-- 4. ПРОВЕРКА РЕЗУЛЬТАТОВ
-- ============================================================================

-- Показать все колонки skus
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'skus'
ORDER BY ordinal_position;

-- Показать все колонки clients
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'clients'
ORDER BY ordinal_position;

-- ============================================================================
-- 5. ОБНОВЛЕНИЕ ДАННЫХ (опционально, после добавления колонок)
-- ============================================================================

-- Если есть данные о остатках/ценах в других таблицах - можно обновить
-- Пример (если есть таблица stock_remains):
-- UPDATE skus SET stock = sr.quantity 
-- FROM stock_remains sr WHERE skus.sku_id = sr.sku_id;

-- Пример установки флага is_new для новых товаров (последние 30 дней)
-- UPDATE skus SET is_new = 'TRUE' 
-- WHERE sku_id IN (SELECT sku_id FROM purchases 
--                  WHERE purchase_date >= CURRENT_DATE - INTERVAL '30 days');