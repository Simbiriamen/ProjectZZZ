-- ============================================================================
-- ProjectZZZ: Добавление недостающих колонок в таблицу minmax_norms
-- Версия: 1.0
-- Дата: 2026-03-21
-- ============================================================================

-- Проверяем и добавляем колонку "Парная номенклатура"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'minmax_norms' AND column_name = 'paired_item'
    ) THEN
        ALTER TABLE minmax_norms ADD COLUMN paired_item TEXT;
        RAISE NOTICE 'Колонка paired_item добавлена';
    ELSE
        RAISE NOTICE 'Колонка paired_item уже существует';
    END IF;
END $$;

-- Проверяем и добавляем колонку "Комплект"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'minmax_norms' AND column_name = 'is_kit'
    ) THEN
        ALTER TABLE minmax_norms ADD COLUMN is_kit TEXT;
        RAISE NOTICE 'Колонка is_kit добавлена';
    ELSE
        RAISE NOTICE 'Колонка is_kit уже существует';
    END IF;
END $$;

-- Проверяем и добавляем колонку "Набор замен"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'minmax_norms' AND column_name = 'replacement_set'
    ) THEN
        ALTER TABLE minmax_norms ADD COLUMN replacement_set TEXT;
        RAISE NOTICE 'Колонка replacement_set добавлена';
    ELSE
        RAISE NOTICE 'Колонка replacement_set уже существует';
    END IF;
END $$;

-- Проверяем и добавляем колонку "Код набора замен"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'minmax_norms' AND column_name = 'replacement_set_code'
    ) THEN
        ALTER TABLE minmax_norms ADD COLUMN replacement_set_code TEXT;
        RAISE NOTICE 'Колонка replacement_set_code добавлена';
    ELSE
        RAISE NOTICE 'Колонка replacement_set_code уже существует';
    END IF;
END $$;

-- Проверяем и добавляем колонку "Набор аналогов"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'minmax_norms' AND column_name = 'analog_set'
    ) THEN
        ALTER TABLE minmax_norms ADD COLUMN analog_set TEXT;
        RAISE NOTICE 'Колонка analog_set добавлена';
    ELSE
        RAISE NOTICE 'Колонка analog_set уже существует';
    END IF;
END $$;

-- Проверяем и добавляем колонку "Код набора аналогов"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'minmax_norms' AND column_name = 'analog_set_code'
    ) THEN
        ALTER TABLE minmax_norms ADD COLUMN analog_set_code TEXT;
        RAISE NOTICE 'Колонка analog_set_code добавлена';
    ELSE
        RAISE NOTICE 'Колонка analog_set_code уже существует';
    END IF;
END $$;

-- Создаём индексы для ускорения поиска по наборам
CREATE INDEX IF NOT EXISTS idx_minmax_paired ON minmax_norms(paired_item);
CREATE INDEX IF NOT EXISTS idx_minmax_replacement ON minmax_norms(replacement_set);
CREATE INDEX IF NOT EXISTS idx_minmax_analog ON minmax_norms(analog_set);

-- Проверяем результат
SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM information_schema.columns 
WHERE table_name = 'minmax_norms' 
  AND column_name IN ('paired_item', 'is_kit', 'replacement_set', 
                      'replacement_set_code', 'analog_set', 'analog_set_code')
ORDER BY ordinal_position;