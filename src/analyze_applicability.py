# -*- coding: utf-8 -*-
"""
analyze_applicability.py v2.1
Анализ столбца "Применяемость" в таблице skus
ИСПРАВЛЕНИЯ: 
- Убран фильтр LENGTH > 20
- Расширен список российских брендов
- Добавлен fallback-поиск по исходному тексту
ProjectZZZ v3.1
"""

import sys
import logging
import re
from pathlib import Path
from sqlalchemy import create_engine, text
import yaml
import pandas as pd
from collections import Counter

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "analyze_applicability.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# СПРАВОЧНИК АВТОМОБИЛЬНЫХ БРЕНДОВ (РАСШИРЕННЫЙ)
# ==============================================================================
CAR_BRANDS = {
    # ================= РОССИЙСКИЕ БРЕНДЫ (ПРИОРИТЕТ) =================
    'LADA': ['LADA', 'ЛАДА', 'VAZ', 'ВАЗ', 'NIVA', 'НИВА', 'VESTA', 'GRANTA', 'KALINA', 'PRIORA', 'XRAY', 'ARGO', 'SAMARA', 'OКА', 'ОКА'],
    'GAZ': ['GAZ', 'ГАЗ', 'GAZELLE', 'ГАЗЕЛЬ', 'SOBEL', 'СОБОЛЬ', 'VOLGA', 'ВОЛГА', 'NEXT', 'САДКО', 'VALDAI'],
    'UAZ': ['UAZ', 'УАЗ', 'PATRIOT', 'ПАТРИОТ', 'HUNTER', 'ХАНТЕР', 'PROFI', 'Пикап', 'БУХАНКА'],
    'AZLK': ['AZLK', 'АЗЛК', 'МОСКВИЧ', 'MOSKVICH', '412', '2141', 'СВЯТОГОР', 'КНЯЗЬ ВЛАДИМИР'],
    'KAMAZ': ['KAMAZ', 'КАМАЗ', 'КАМА', 'НЕФАЗ'],
    'URAL': ['URAL', 'УРАЛ', 'УРАЛ-4320'],
    'ZIL': ['ZIL', 'ЗИЛ', 'БЫЧОК', '130', '131', '157'],
    'PAZ': ['PAZ', 'ПАЗ', 'ПАЗ-3205', 'ПАЗ-4234'],
    'LIAZ': ['LIAZ', 'ЛИАЗ', 'ЛИАЗ-5256'],
    'MAZ_RU': ['МАЗ', 'MAZ BELARUS'],  # Белорусский, но часто в РФ
    
    # ================= ЯПОНСКИЕ =================
    'TOYOTA': ['TOYOTA', 'ТОЙОТА', 'LEXUS', 'ЛЕКСУС'],
    'NISSAN': ['NISSAN', 'НИССАН', 'INFINITI', 'ИНФИНИТИ'],
    'HONDA': ['HONDA', 'ХОНДА', 'ACURA', 'АКУРА'],
    'MAZDA': ['MAZDA', 'МАЗДА'],
    'MITSUBISHI': ['MITSUBISHI', 'МИЦУБИСИ'],
    'SUBARU': ['SUBARU', 'СУБАРУ'],
    'SUZUKI': ['SUZUKI', 'СУЗУКИ'],
    'ISUZU': ['ISUZU', 'ИСУЗУ'],
    'DAIHATSU': ['DAIHATSU', 'ДАЙХАЦУ'],
    
    # ================= КОРЕЙСКИЕ =================
    'HYUNDAI': ['HYUNDAI', 'ХЁНДЭ', 'ХЕНДЕ', 'GENESIS', 'ДЖЕНЕСИС'],
    'KIA': ['KIA', 'КИА'],
    'SSANGYONG': ['SSANGYONG', 'САНГЙОНГ', 'KYONDO', 'ACTYON', 'REXTON'],
    'DAEWOO': ['DAEWOO', 'ДЭУ', 'CHEVROLET KOREA', 'RAVON', 'RAVON'],
    
    # ================= ЕВРОПЕЙСКИЕ =================
    'VOLKSWAGEN': ['VOLKSWAGEN', 'ФОЛЬКСВАГЕН', 'VW', 'VAG', 'SKODA', 'ШКОДА', 'SEAT', 'СЕАТ'],
    'BMW': ['BMW', 'БМВ', 'MINI', 'МИНИ'],
    'MERCEDES': ['MERCEDES', 'МЕРСЕДЕС', 'BENZ', 'MERCEDES-BENZ', 'SMART'],
    'AUDI': ['AUDI', 'АУДИ'],
    'OPEL': ['OPEL', 'ОПЕЛЬ', 'VAUXHALL', 'ВОКСХОЛЛ'],
    'FORD': ['FORD', 'ФОРД'],
    'RENAULT': ['RENAULT', 'РЕНАУЛЬТ', 'DACIA', 'ДАЧИЯ', 'ЛАДА РЕНАУЛЬТ'],
    'PEUGEOT': ['PEUGEOT', 'ПЕЖО'],
    'CITROEN': ['CITROEN', 'СИТРОЕН', 'CITROËN', 'DS', 'ДС'],
    'VOLVO': ['VOLVO', 'ВОЛЬВО'],
    'FIAT': ['FIAT', 'ФИАТ', 'ALFA ROMEO', 'ALFA-ROMEO', 'ALFA', 'LANCIA'],
    'IVECO': ['IVECO', 'ИВЕКО'],
    'MAN': ['MAN', 'МАН'],
    'SCANIA': ['SCANIA', 'СКАНИЯ'],
    'DAF': ['DAF', 'ДАФ'],
    'PORSCHE': ['PORSCHE', 'ПОРШЕ'],
    
    # ================= АМЕРИКАНСКИЕ =================
    'CHEVROLET': ['CHEVROLET', 'ШЕВРОЛЕ', 'CHEVY', 'CADILLAC', 'КАДИЛЛАК', 'GMC', 'BUICK', 'БЬЮИК'],
    'FORD_US': ['FORD USA', 'FORD US', 'LINCOLN', 'ЛИНКОЛЬН'],
    'DODGE': ['DODGE', 'ДОДЖ', 'RAM', 'JEEP', 'ДЖИП', 'CHRYSLER', 'КРАЙСЛЕР'],
    
    # ================= КИТАЙСКИЕ =================
    'CHERY': ['CHERY', 'ЧЕРИ', 'EXEED', 'OMODA'],
    'GEELY': ['GEELY', 'ДЖИЛИ', 'LYNK & CO', 'VOLVE CHINA'],
    'GREAT_WALL': ['GREAT WALL', 'GWM', 'HAVAL', 'WINGLE', 'WEY', 'TANK', 'ОРУС'],
    'FAW': ['FAW', 'ФАВ', 'BESTUNE', 'HONGQI'],
    'JAC': ['JAC', 'ДЖАК'],
    'BYD': ['BYD', 'БИД'],
    'LIFAN': ['LIFAN', 'ЛИФАН'],
    'CHANGAN': ['CHANGAN', 'ЧАНГАН', 'DEEPAL'],
    'DFSK': ['DFSK', 'DFM', 'DONGFENG', 'ДОНОНГ'],
    
    # ================= СПЕЦТЕХНИКА / ГРУЗОВЫЕ =================
    'COMMERCIAL': ['ГРУЗОВИК', 'ГРУЗОВОЙ', 'TRUCK', 'КОММЕРЧЕСКИЙ', 'CARGO', 'FURGON', 'ФУРГОН'],
    'BUS': ['АВТОБУС', 'BUS', 'ПАЗ', 'ЛИАЗ', 'МАЗ АВТОБУС', 'NEOPLAN', 'SETRA'],
    'TRACTOR': ['ТРАКТОР', 'TRACTOR', 'МТЗ', 'БЕЛАРУС', 'BELARUS', 'ЮМЗ', 'Т-40', 'Т-25'],
    'AGRI': ['ТРАКТОР', 'СЕЛЬХОЗ', 'AGRI', 'AGRICULTURAL', 'FARM', 'КОМБАЙН', 'HARVESTER'],
    
    # ================= МОТО / ВОДНАЯ ТЕХНИКА =================
    'MOTO': ['МОТО', 'MOTO', 'МОТОЦИКЛ', 'QUAD', 'КУАДРО', 'ATV', 'SCOOTER', 'СКУТЕР', 'БАЙК'],
    'MARINE': ['MARINE', 'ЛОДОЧНЫЙ', 'КАТЕР', 'ЯХТА', 'BOAT', 'OUTBOARD', 'ПОДВЕСНОЙ'],
    'SNOWMOBILE': ['СНЕГОХОД', 'SNOWMOBILE', 'БУРАН', 'RYSS', 'ТАЙГА'],
    
    # ================= УНИВЕРСАЛЬНЫЕ =================
    'UNIVERSAL': ['УНИВЕРСАЛЬНЫЙ', 'UNIVERSAL', 'MULTI', 'ВСЕ МОДЕЛИ', 'ДЛЯ ВСЕХ', 'ОБЩЕЕ'],
}

# Разделители между записями автомобилей (от самых специфичных к общим)
APPLICABILITY_DELIMITERS = [
    r'\s*//\s*',      # //
    r'\s*/\s*',       # /
    r'\s*\|\s*',      # |
    r'\s*;\s*',       # ;
    r'\s*,\s*',       # ,
]


# ==============================================================================
# ФУНКЦИИ ПАРСИНГА
# ==============================================================================
def split_applicability_entries(text):
    """
    Разбивает строку применяемости на отдельные записи автомобилей.
    """
    if pd.isna(text) or not isinstance(text, str) or not text.strip():
        return []
    
    entries = [text]
    
    for delimiter in APPLICABILITY_DELIMITERS:
        new_entries = []
        for entry in entries:
            parts = re.split(delimiter, entry)
            new_entries.extend([p.strip() for p in parts if p.strip()])
        entries = new_entries
    
    # 🔧 УБРАН ФИЛЬТР ПО ДЛИНЕ - оставляем даже короткие записи
    entries = [e for e in entries if len(e) >= 3]  # Минимум 3 символа
    
    return entries


def extract_brands_from_entry(entry_text):
    """
    Извлекает марки автомобилей из одной записи.
    🔧 ДОБАВЛЕН FALLBACK: если не нашли после сплита - ищем в исходном тексте
    """
    if not entry_text:
        return []
    
    text_upper = entry_text.upper()
    found_brands = []
    
    for brand, keywords in CAR_BRANDS.items():
        for keyword in keywords:
            if keyword in text_upper:
                found_brands.append(brand)
                break
    
    return list(set(found_brands))


def analyze_applicability_detailed(text):
    """
    Детальный анализ одной записи применяемости.
    """
    entries = split_applicability_entries(text)
    
    all_brands = []
    
    for entry in entries:
        brands = extract_brands_from_entry(entry)
        all_brands.extend(brands)
    
    # 🔧 FALLBACK: если после сплита ничего не нашли - пробуем по исходному тексту
    if not all_brands and text and isinstance(text, str):
        all_brands = extract_brands_from_entry(text)
    
    return {
        'brands': list(set(all_brands)),
        'entry_count': len(entries) if entries else 0
    }


# ==============================================================================
# ФУНКЦИИ РАБОТЫ С БД
# ==============================================================================
def load_config():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_engine(config):
    db = config['database']
    return create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )


def get_dominant_brand(brands_list):
    """Определяет доминирующий бренд из списка"""
    if not brands_list:
        return 'UNIVERSAL'
    counter = Counter(brands_list)
    return counter.most_common(1)[0][0]


def analyze_applicability(engine):
    """Анализирует столбец применяемости и обновляет skus"""
    logger.info("\n" + "="*70)
    logger.info("АНАЛИЗ ПРИМЕНЯЕМОСТИ (v2.1 - РОССИЙСКИЕ БРЕНДЫ)")
    logger.info("="*70)
    
    # 🔧 УБРАН ФИЛЬТР LENGTH > 20 - загружаем ВСЕ записи с применяемостью
    logger.info("\n📥 Загрузка данных из skus...")
    df = pd.read_sql_query("""
        SELECT sku_id, sku_name, applicability 
        FROM skus 
        WHERE applicability IS NOT NULL 
          AND applicability != ''
    """, engine)
    
    logger.info(f"   Загружено записей с применяемостью: {len(df):,}")
    
    if df.empty:
        logger.warning("⚠️ Нет данных для анализа")
        return False
    
    # Анализируем применяемость
    logger.info("\n🔍 Детальный анализ применяемости...")
    
    df['analysis'] = df['applicability'].apply(analyze_applicability_detailed)
    df['brands_found'] = df['analysis'].apply(lambda x: x['brands'])
    df['entry_count'] = df['analysis'].apply(lambda x: x['entry_count'])
    df['dominant_brand'] = df['brands_found'].apply(get_dominant_brand)
    
    # 🔧 СПЕЦИАЛЬНАЯ СТАТИСТИКА ПО РОССИЙСКИМ БРЕНДАМ
    logger.info(f"\n🇷🇺 РОССИЙСКИЕ БРЕНДЫ (детально):")
    russian_brands = ['LADA', 'GAZ', 'UAZ', 'AZLK', 'KAMAZ', 'URAL', 'ZIL', 'PAZ', 'LIAZ']
    
    for brand in russian_brands:
        count = df['brands_found'].apply(lambda x: brand in x).sum()
        dominant_count = (df['dominant_brand'] == brand).sum()
        if count > 0 or dominant_count > 0:
            logger.info(f"   {brand}: {count:,} вхождений, {dominant_count:,} доминирующих")
    
    # Статистика по количеству записей в ячейке
    logger.info(f"\n📊 Статистика записей в ячейке:")
    entry_stats = df['entry_count'].value_counts().sort_index().head(15)
    for count, freq in entry_stats.items():
        pct = freq / len(df) * 100
        logger.info(f"   {count} записей: {freq:,} SKU ({pct:.1f}%)")
    
    # Статистика по брендам
    logger.info(f"\n📊 СТАТИСТИКА ПО БРЕНДАМ (с учётом множественных вхождений):")
    all_brands = [b for brands in df['brands_found'] for b in brands if brands]
    brand_counter = Counter(all_brands)
    
    for brand, count in brand_counter.most_common(25):
        pct = count / len(all_brands) * 100 if all_brands else 0
        logger.info(f"   {brand}: {count:,} вхождений ({pct:.1f}%)")
    
    # Статистика по доминирующим брендам
    logger.info(f"\n📊 ДОМИНИРУЮЩИЕ БРЕНДЫ (по одному на SKU):")
    dominant_stats = df['dominant_brand'].value_counts().head(25)
    for brand, count in dominant_stats.items():
        pct = count / len(df) * 100
        logger.info(f"   {brand}: {count:,} SKU ({pct:.1f}%)")
    
    # Обновление колонок в БД
    logger.info("\n💾 Обновление колонок в skus...")
    
    with engine.connect() as conn:
        for col_name, col_type in [
            ('brand_specialization', 'TEXT'),
            ('applicable_brands', 'TEXT[]'),
            ('applicability_entry_count', 'INTEGER')
        ]:
            conn.execute(text(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'skus' AND column_name = '{col_name}'
                    ) THEN
                        ALTER TABLE skus ADD COLUMN {col_name} {col_type};
                        RAISE NOTICE 'Колонка {col_name} добавлена';
                    END IF;
                END $$;
            """))
        conn.commit()
        logger.info("✅ Колонки добавлены/проверены")
    
    # Пакетное обновление данных
    logger.info("\n🔄 Обновление данных в skus (пакетно)...")
    
    updated_count = 0
    batch_size = 5000
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        
        with engine.connect() as conn:
            for _, row in batch.iterrows():
                brands_array = f"{{{','.join(row['brands_found'])}}}" if row['brands_found'] else '{}'
                
                conn.execute(text("""
                    UPDATE skus 
                    SET brand_specialization = :brand,
                        applicable_brands = :brands,
                        applicability_entry_count = :entry_count
                    WHERE sku_id = :sku_id
                """), {
                    'brand': row['dominant_brand'],
                    'brands': brands_array,
                    'entry_count': row['entry_count'],
                    'sku_id': row['sku_id']
                })
            
            conn.commit()
            updated_count += len(batch)
            if updated_count % 25000 == 0:
                logger.info(f"   Обработано: {updated_count:,} / {len(df):,}")
    
    logger.info(f"\n✅ Обновлено записей: {updated_count:,}")
    
    # Итоговая статистика
    logger.info("\n" + "="*70)
    logger.info("ИТОГИ ДЕТАЛЬНОГО АНАЛИЗА ПРИМЕНЯЕМОСТИ")
    logger.info("="*70)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                brand_specialization,
                COUNT(*) as sku_count,
                AVG(applicability_entry_count) as avg_entries
            FROM skus
            WHERE brand_specialization IS NOT NULL
            GROUP BY brand_specialization
            ORDER BY sku_count DESC
            LIMIT 20
        """))
        
        logger.info("\n📊 Топ специализаций (с средним кол-вом записей):")
        for row in result.fetchall():
            logger.info(f"   {row[0]}: {row[1]:,} SKU, ср. {row[2]:.1f} записей/ячейку")
    
    return True


def main():
    logger.info("="*70)
    logger.info("ProjectZZZ - Детальный анализ применяемости v2.1")
    logger.info("="*70)
    
    config = load_config()
    engine = get_engine(config)
    
    success = analyze_applicability(engine)
    
    engine.dispose()
    
    if success:
        logger.info("\n✅ Детальный анализ завершён успешно!")
    else:
        logger.info("\n⚠️ Анализ завершён с предупреждениями")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())