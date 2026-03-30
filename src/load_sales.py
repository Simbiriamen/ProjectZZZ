# -*- coding: utf-8 -*-
"""
load_sales.py
Загрузка истории продаж для ProjectZZZ (PostgreSQL)
Версия: 3.5 - ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА (с проверкой хэша)
"""

import pandas as pd
import logging
import sys
import re
import time
import hashlib
from pathlib import Path
from sqlalchemy import create_engine, text
import yaml
from datetime import datetime

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"
META_DIR = PROJECT_ROOT / "config"

LOG_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "load_sales.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# УТИЛИТЫ: ХЭШ И ПРОВЕРКА ФАЙЛОВ
# ==============================================================================
def calculate_file_hash(file_path: Path, algorithm: str = 'md5') -> str:
    """Вычисляет хэш файла"""
    hash_func = hashlib.new(algorithm)
    try:
        # Для больших файлов читаем частями
        if file_path.stat().st_size > 10_000_000:
            with open(file_path, 'rb') as f:
                sample = f.read(1_000_000)
            hash_func.update(sample)
        else:
            with open(file_path, 'rb') as f:
                hash_func.update(f.read())
        return hash_func.hexdigest()[:16]
    except Exception as e:
        logger.error(f"Ошибка вычисления хэша {file_path.name}: {e}")
        return None


def get_file_mtime(file_path: Path) -> datetime:
    """Возвращает время последнего изменения файла"""
    return datetime.fromtimestamp(file_path.stat().st_mtime)


def extract_quarter(filename):
    """Извлекаем квартал из имени файла"""
    match = re.search(r'(\d)\s*кв\s*(\d{2})\.xlsx$', filename)
    if match:
        q, y = match.groups()
        return f"20{y}-Q{q}"
    return None


# ==============================================================================
# РАБОТА С БД: ИСТОРИЯ ЗАГРУЗОК
# ==============================================================================
def init_load_history(conn):
    """Создаёт таблицу отслеживания загрузок, если её нет"""
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS load_history (
            id SERIAL PRIMARY KEY,
            file_name TEXT NOT NULL,
            file_hash TEXT NOT NULL,
            file_mtime TEXT NOT NULL,
            quarter_processed TEXT NOT NULL,
            records_loaded INTEGER DEFAULT 0,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(file_name, quarter_processed)
        )
    """))
    conn.commit()
    logger.info("Таблица load_history готова")


def get_loaded_file_info(conn, file_name: str, quarter: str) -> dict:
    """Получает информацию о предыдущей загрузке файла"""
    result = conn.execute(text("""
        SELECT file_hash, file_mtime, records_loaded, loaded_at
        FROM load_history
        WHERE file_name = :fname AND quarter_processed = :qtr
    """), {"fname": file_name, "qtr": quarter})
    
    row = result.fetchone()
    if row:
        return {
            'file_hash': row[0],
            'file_mtime': row[1],
            'records_loaded': row[2],
            'loaded_at': row[3]
        }
    return None


def update_load_history(conn, file_name: str, file_hash: str, file_mtime: datetime,
                        quarter: str, records_count: int):
    """Обновляет или добавляет запись в историю загрузок"""
    conn.execute(text("""
        INSERT INTO load_history 
        (file_name, file_hash, file_mtime, quarter_processed, records_loaded, loaded_at)
        VALUES (:fname, :fhash, :fmtime, :qtr, :count, :loaded_at)
        ON CONFLICT (file_name, quarter_processed) 
        DO UPDATE SET 
            file_hash = :fhash,
            file_mtime = :fmtime,
            records_loaded = :count,
            loaded_at = CURRENT_TIMESTAMP
    """), {
        "fname": file_name,
        "fhash": file_hash,
        "fmtime": file_mtime.strftime('%Y-%m-%d %H:%M:%S'),
        "qtr": quarter,
        "count": records_count,
        "loaded_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    conn.commit()


def should_skip_file(conn, file_path: Path, quarter: str, force: bool = False) -> bool:
    """Определяет, можно ли пропустить загрузку файла"""
    if force:
        logger.info("   💪 Принудительный режим — пропускаем проверку")
        return False
    
    file_name = file_path.name
    current_hash = calculate_file_hash(file_path)
    
    if not current_hash:
        logger.warning("   ⚠️ Не удалось вычислить хэш — загружаем файл")
        return False
    
    loaded_info = get_loaded_file_info(conn, file_name, quarter)
    
    if loaded_info:
        if loaded_info['file_hash'] == current_hash:
            logger.info("   ✅ Файл не изменился — ПРОПУСКАЕМ")
            logger.info(f"      Загружен: {loaded_info['loaded_at']} | Записей: {loaded_info['records_loaded']:,}")
            return True
        else:
            logger.info("   🔄 Файл изменён — перезагружаем")
            logger.info(f"      Старый хэш: {loaded_info['file_hash']}")
            logger.info(f"      Новый хэш: {current_hash}")
    else:
        logger.info("   🆕 Новый файл — загружаем")
    
    return False


# ==============================================================================
# ОБРАБОТКА ФАЙЛА ПРОДАЖ
# ==============================================================================
def extract_client_code(val):
    """Извлекаем код клиента из строки 'Имя, КОД'"""
    if pd.isna(val) or str(val).strip() == '':
        return None
    parts = str(val).strip().rsplit(',', 1)
    if len(parts) == 2:
        return parts[1].strip()
    return parts[0].strip()


def process_sales_file(file_path: Path):
    """Обработка файла продаж"""
    logger.info(f"\n📄 Файл: {file_path.name}")
    
    try:
        # ЧИТАЕМ сырые данные БЕЗ заголовков
        df_raw = pd.read_excel(file_path, sheet_name="TDSheet", header=None, dtype=str)
        logger.info(f"   📊 Всего строк в файле: {len(df_raw):,}")
        
        # Заголовки в строке 5 (индекс 4)
        header_row = 4
        
        if len(df_raw) > header_row:
            df = df_raw.iloc[header_row:].reset_index(drop=True)
        else:
            logger.error("   ❌ Файл слишком короткий")
            return None, None
        
        # Фильтруем пустые строки
        df = df[df.iloc[:, 0].notna()].copy()
        
        if df.empty:
            logger.error("   ❌ Нет данных после фильтрации")
            return None, None
        
        logger.info(f"   📊 Строк данных: {len(df):,}")
        
        # ЧТЕНИЕ ПО ПОЗИЦИЯМ КОЛОНОК
        try:
            df_parsed = pd.DataFrame({
                'warehouse': df.iloc[:, 0].str.strip(),
                'client_raw': df.iloc[:, 3].str.strip(),
                'sku_id': df.iloc[:, 5].str.strip().str.upper(),
                'purchase_date': df.iloc[:, 6],
                'quantity': pd.to_numeric(df.iloc[:, 7], errors='coerce'),
                'amount': pd.to_numeric(df.iloc[:, 8], errors='coerce'),
            })
            logger.info("   ✅ Колонки извлечены по позициям")
        except IndexError as e:
            logger.error(f"   ❌ Ошибка парсинга колонок: {e}")
            logger.error(f"   📋 Доступно колонок: {len(df.columns)}")
            return None, None
        
        # Фильтрация пустых значений
        df_parsed = df_parsed.dropna(subset=['sku_id', 'amount']).copy()
        df_parsed = df_parsed[df_parsed['sku_id'] != ''].copy()
        df_parsed = df_parsed[df_parsed['amount'] > 0].copy()
        
        logger.info(f"   📊 После фильтрации: {len(df_parsed):,} строк")
        
        if df_parsed.empty:
            logger.error("   ❌ Нет валидных записей")
            return None, None
        
        # Разделяем client_raw на client_id и client_name
        if 'client_raw' in df_parsed.columns:
            client_split = df_parsed['client_raw'].str.split(',', n=1, expand=True)
            df_parsed['client_name'] = client_split[0].str.strip()
            df_parsed['client_id'] = client_split[1].str.strip().str.replace(r'\s+', '', regex=True) if len(client_split.columns) > 1 else client_split[0]
            df_parsed = df_parsed.drop(columns=['client_raw'])
            logger.info("   ✅ Клиенты разделены на ID и имя")
        
        # Нормализация склада
        def norm_warehouse(wh):
            if pd.isna(wh):
                return None
            wh = str(wh).strip()
            if 'Стройиндустрии' in wh:
                return 'Братск_СИ' if 'Братск' in wh else wh
            elif 'Братск' in wh:
                return 'Братск'
            elif 'Усть' in wh:
                return 'Усть-Илимск'
            return wh
        
        df_parsed['warehouse'] = df_parsed['warehouse'].apply(norm_warehouse)
        df_parsed = df_parsed[df_parsed['warehouse'].notna()].copy()
        
        # Парсинг даты
        df_parsed['purchase_date'] = pd.to_datetime(
            df_parsed['purchase_date'], 
            errors='coerce', 
            dayfirst=True
        )
        df_parsed = df_parsed.dropna(subset=['purchase_date']).copy()
        
        if df_parsed.empty:
            logger.error("   ❌ Нет валидных дат")
            return None, None
        
        df_parsed['purchase_date'] = df_parsed['purchase_date'].dt.strftime('%Y-%m-%d')
        
        # Определяем квартал из имени файла
        quarter = extract_quarter(file_path.name)
        if not quarter:
            first_date = pd.to_datetime(df_parsed['purchase_date'].min())
            quarter = f"{first_date.year}-Q{((first_date.month - 1) // 3) + 1}"
        
        logger.info(f"   📅 Квартал: {quarter}")
        
        # Удаляем дубликаты
        df_parsed = df_parsed.drop_duplicates(
            subset=['warehouse', 'client_id', 'sku_id', 'purchase_date']
        )
        
        df_parsed['source_file'] = file_path.name
        df_parsed['quarter'] = quarter
        
        # Рассчитываем цену за единицу
        df_parsed['price'] = df_parsed.apply(
            lambda row: round(row['amount'] / row['quantity'], 2)
            if row['quantity'] > 0 and pd.notna(row['amount'])
            else 0,
            axis=1
        )
        
        logger.info(f"   📊 Итоговых записей: {len(df_parsed):,}")
        
        # Статистика
        stats = df_parsed.groupby('warehouse').agg({
            'sku_id': 'nunique',
            'amount': 'sum',
            'price': 'mean'
        }).round(2)
        logger.info("   📊 Склады:")
        for wh, row in stats.iterrows():
            logger.info(f"      {wh}: {int(row['sku_id']):,} товаров, {row['amount']:,.0f} руб.")
        
        return df_parsed, quarter
        
    except Exception as e:
        logger.error(f"   ❌ Ошибка обработки файла: {e}")
        import traceback
        traceback.print_exc()
        return None, None


# ==============================================================================
# СОЗДАНИЕ ТАБЛИЦЫ
# ==============================================================================
def create_tables(conn):
    """Создаёт таблицу purchases"""
    logger.info("\n💾 Создание таблицы purchases...")
    
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS purchases (
            id SERIAL PRIMARY KEY,
            client_id TEXT NOT NULL,
            client_name TEXT,
            sku_id TEXT NOT NULL,
            purchase_date DATE,
            quantity INTEGER DEFAULT 1,
            amount NUMERIC(12,2),
            price NUMERIC(12,2),
            warehouse TEXT,
            quarter VARCHAR(10),
            source_file TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, sku_id, purchase_date, warehouse)
        )
    """))
    conn.commit()
    
    # Индексы
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchases_client ON purchases(client_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchases_sku ON purchases(sku_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchases_date ON purchases(purchase_date)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchases_quarter ON purchases(quarter)"))
    conn.commit()
    
    logger.info("   ✅ Таблица и индексы созданы")


# ==============================================================================
# СОХРАНЕНИЕ В БД
# ==============================================================================
def save_to_db(conn, df: pd.DataFrame, quarter: str, force: bool = False) -> int:
    """Сохраняет данные в БД"""
    if df is None or df.empty:
        logger.warning("   ⚠️ Пустой DataFrame — сохранение пропущено")
        return 0
    
    df_save = df.copy()
    df_save['quarter'] = quarter
    
    cols_to_save = ['client_id', 'client_name', 'sku_id', 'purchase_date', 
                    'quantity', 'amount', 'price', 'warehouse', 'quarter', 'source_file']
    df_to_save = df_save[cols_to_save].copy()
    
    # Если force — очищаем квартал перед загрузкой
    if force:
        logger.info(f"   🗑️ Принудительная очистка квартала {quarter}...")
        conn.execute(text("DELETE FROM purchases WHERE quarter = :qtr"), {"qtr": quarter})
        conn.commit()
    
    # Загружаем данные (INSERT ... ON CONFLICT DO NOTHING для избежания дубликатов)
    df_to_save.to_sql('purchases', conn, if_exists='append', index=False, chunksize=5000)
    count = len(df_to_save)
    conn.commit()
    
    logger.info(f"   ✅ Сохранено: {count:,} записей")
    return count


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    logger.info(" ")
    logger.info("=" * 60)
    logger.info("🚀 ЗАГРУЗКА ПРОДАЖ ProjectZZZ (INCREMENTAL)")
    logger.info("=" * 60)
    logger.info(f"Папка: {RAW_DIR}")
    logger.info(f"Старт: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Проверка аргументов командной строки
    force = "--force" in sys.argv or "-f" in sys.argv
    logger.info(f"Режим: {'ПРИНУДИТЕЛЬНЫЙ' if force else 'УМНЫЙ (только изменения)'}")
    
    start_time = time.time()
    
    # Ищем файлы
    files = sorted(RAW_DIR.glob("Отгрузки КА*.xlsx"))
    files = [f for f in files if 'Отгрузки' in f.name]
    
    logger.info(f"📁 Найдено файлов: {len(files)}")
    
    if not files:
        logger.error("❌ Нет файлов для обработки")
        return 1
    
    # Подключаемся к БД
    config = yaml.safe_load(open(CONFIG_PATH, 'r', encoding='utf-8'))
    db = config['database']
    engine = create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )
    
    total_loaded = 0
    files_processed = 0
    files_skipped = 0
    
    with engine.connect() as conn:
        # Создаём таблицу истории загрузок
        init_load_history(conn)
        
        # Создаём таблицу purchases
        create_tables(conn)
        
        # Обрабатываем файлы
        for f in files:
            quarter = extract_quarter(f.name)
            if not quarter:
                logger.warning(f"   ⚠️ Не удалось определить квартал для {f.name}")
                continue
            
            # 🔧 ПРОВЕРКА: можно ли пропустить файл?
            if should_skip_file(conn, f, quarter, force):
                files_skipped += 1
                continue
            
            # Обрабатываем файл
            result, quarter = process_sales_file(f)
            
            if result is not None:
                count = save_to_db(conn, result, quarter, force)
                if count > 0:
                    total_loaded += count
                    files_processed += 1
                    
                    # 🔧 Записываем в историю загрузок
                    file_hash = calculate_file_hash(f)
                    file_mtime = get_file_mtime(f)
                    if file_hash:
                        update_load_history(conn, f.name, file_hash, file_mtime, quarter, count)
                        logger.info("   📝 Запись в load_history обновлена")
        
        # Итоги
        logger.info("\n" + "=" * 60)
        logger.info("📊 ИТОГИ ЗАГРУЗКИ")
        logger.info("=" * 60)
        
        result = conn.execute(text("""
            SELECT warehouse, COUNT(*), SUM(amount)
            FROM purchases
            GROUP BY warehouse
            ORDER BY COUNT(*) DESC
        """))
        
        logger.info("ПО СКЛАДАМ:")
        for row in result.fetchall():
            logger.info(f"   {row[0]}: {row[1]:,} записей, {row[2]:,.0f} руб.")
        
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT warehouse), COUNT(DISTINCT client_id), 
                   COUNT(DISTINCT sku_id), COUNT(*), SUM(amount)
            FROM purchases
        """))
        
        row = result.fetchone()
        logger.info(f"\n📊 Сводка:")
        logger.info(f"   Складов: {row[0]}")
        logger.info(f"   Клиентов: {row[1]:,}")
        logger.info(f"   Товаров: {row[2]:,}")
        logger.info(f"   Всего записей: {row[3]:,}")
        logger.info(f"   Общий оборот: {row[4]:,.0f} руб.")
        
        logger.info(f"\n✅ Обработано файлов: {files_processed}")
        logger.info(f"⏭️ Пропущено (не изменены): {files_skipped}")
        logger.info(f"✅ Загружено записей: {total_loaded:,}")
        logger.info(f"⏱️ Время: {time.time() - start_time:.1f} сек")
        logger.info("=" * 60)
    
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())