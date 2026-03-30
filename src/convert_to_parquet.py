# -*- coding: utf-8 -*-
"""
convert_to_parquet.py v1.0
🔧 КОНВЕРТАЦИЯ CSV → PARQUET
Назначение:
  - Конвертация больших CSV файлов в формат Parquet
  - Сжатие данных (сокращение размера в 3-5 раз)
  - Ускорение загрузки для последующего анализа

Использование:
  python src/convert_to_parquet.py [--input <file>] [--compress snappy]
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "convert_to_parquet.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# ФУНКЦИИ
# ==============================================================================
def get_file_size_mb(file_path: Path) -> float:
    """Возвращает размер файла в МБ"""
    return file_path.stat().st_size / (1024 * 1024)


def convert_csv_to_parquet(
    input_path: Path,
    output_path: Path = None,
    compression: str = 'snappy',
    chunksize: int = 100000
) -> dict:
    """
    Конвертирует CSV файл в Parquet с разбивкой на чанки.
    
    Args:
        input_path: Путь к входному CSV файлу
        output_path: Путь для выходного Parquet файла (по умолчанию: то же имя + .parquet)
        compression: Тип сжатия (snappy, gzip, zstd, None)
        chunksize: Размер чанка для чтения
    
    Returns:
        dict со статистикой конвертации
    """
    if output_path is None:
        output_path = input_path.with_suffix('.parquet')
    
    logger.info(f"\n🔄 Конвертация: {input_path.name} → {output_path.name}")
    logger.info(f"   📊 Сжатие: {compression}")
    
    # Статистика до
    input_size_mb = get_file_size_mb(input_path)
    logger.info(f"   📦 Размер CSV: {input_size_mb:.2f} МБ")
    
    # Чтение и запись чанками
    total_rows = 0
    first_chunk = True
    parquet_writer = None
    
    start_time = datetime.now()
    
    try:
        for chunk in pd.read_csv(input_path, chunksize=chunksize):
            total_rows += len(chunk)
            
            # 🔧 Оптимизация типов данных
            for col in chunk.columns:
                if chunk[col].dtype == 'float64':
                    chunk[col] = chunk[col].astype('float32')
                elif chunk[col].dtype == 'int64':
                    chunk[col] = pd.to_numeric(chunk[col], downcast='integer')
            
            if first_chunk:
                # Создание файла с первой партицией
                table = pa.Table.from_pandas(chunk)
                parquet_writer = pq.ParquetWriter(
                    output_path,
                    table.schema,
                    compression=compression
                )
                first_chunk = False
            else:
                # Добавление партиции
                table = pa.Table.from_pandas(chunk)
                parquet_writer.write_table(table)
            
            logger.debug(f"   📝 Обработано строк: {total_rows:,}")
        
        if parquet_writer:
            parquet_writer.close()
        
        # Статистика после
        output_size_mb = get_file_size_mb(output_path)
        compression_ratio = (1 - output_size_mb / input_size_mb) * 100 if input_size_mb > 0 else 0
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"   ✅ Конвертация завершена")
        logger.info(f"   📊 Строк: {total_rows:,}")
        logger.info(f"   📦 Размер Parquet: {output_size_mb:.2f} МБ")
        logger.info(f"   📉 Сжатие: {compression_ratio:.1f}%")
        logger.info(f"   ⏱️ Время: {elapsed:.1f} сек")
        
        return {
            'input_path': str(input_path),
            'output_path': str(output_path),
            'input_size_mb': input_size_mb,
            'output_size_mb': output_size_mb,
            'compression_ratio': compression_ratio,
            'total_rows': total_rows,
            'elapsed_sec': elapsed
        }
    
    except Exception as e:
        logger.error(f"   ❌ Ошибка конвертации: {e}")
        if parquet_writer:
            parquet_writer.close()
        raise


def convert_all_csv_in_directory(
    directory: Path,
    compression: str = 'snappy'
) -> list:
    """Конвертирует все CSV файлы в директории"""
    results = []
    
    csv_files = list(directory.glob("*.csv"))
    
    if not csv_files:
        logger.info(f"   📁 CSV файлы не найдены в {directory}")
        return results
    
    logger.info(f"\n📁 Найдено CSV файлов: {len(csv_files)}")
    
    for csv_file in csv_files:
        # Пропускаем уже сконвертированные или маленькие файлы
        if csv_file.stat().st_size < 1024 * 1024:  # < 1 МБ
            logger.info(f"   ⏭️ Пропущен малый файл: {csv_file.name}")
            continue
        
        try:
            result = convert_csv_to_parquet(csv_file, compression=compression)
            results.append(result)
        except Exception as e:
            logger.error(f"   ❌ Ошибка {csv_file.name}: {e}")
    
    return results


def compare_csv_parquet(csv_path: Path, parquet_path: Path) -> bool:
    """Сравнивает данные CSV и Parquet для проверки целостности"""
    logger.info(f"\n🔍 Сравнение {csv_path.name} и {parquet_path.name}...")
    
    # Чтение первых 1000 строк для сравнения
    df_csv = pd.read_csv(csv_path, nrows=1000)
    df_parquet = pd.read_parquet(parquet_path)
    
    if len(df_parquet) != len(df_csv):
        logger.warning(f"   ⚠️ Разное количество строк: CSV={len(df_csv)}, Parquet={len(df_parquet)}")
        return False
    
    # Сравнение
    try:
        pd.testing.assert_frame_equal(
            df_csv.reset_index(drop=True),
            df_parquet.reset_index(drop=True),
            check_dtype=False
        )
        logger.info("   ✅ Данные идентичны")
        return True
    except AssertionError as e:
        logger.error(f"   ❌ Различия в данных: {e}")
        return False


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description='Конвертация CSV → Parquet для ProjectZZZ'
    )
    parser.add_argument(
        '--input', '-i',
        type=str,
        default=None,
        help='Путь к CSV файлу (по умолчанию: все файлы в data/processed/)'
    )
    parser.add_argument(
        '--compress', '-c',
        type=str,
        choices=['snappy', 'gzip', 'zstd', 'none'],
        default='snappy',
        help='Тип сжатия (по умолчанию: snappy)'
    )
    parser.add_argument(
        '--verify', '-v',
        action='store_true',
        help='Проверить целостность после конвертации'
    )
    
    args = parser.parse_args()
    
    logger.info("="*70)
    logger.info("🔄 ProjectZZZ - CSV TO PARQUET CONVERTER v1.0")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    compression = args.compress if args.compress != 'none' else None
    
    if args.input:
        # Конвертация одного файла
        input_path = Path(args.input)
        if not input_path.exists():
            logger.error(f"❌ Файл не найден: {input_path}")
            return 1
        
        output_path = input_path.with_suffix('.parquet')
        result = convert_csv_to_parquet(input_path, output_path, compression)
        
        if args.verify and output_path.exists():
            compare_csv_parquet(input_path, output_path)
    
    else:
        # Конвертация всех файлов в processed/
        results = convert_all_csv_in_directory(PROCESSED_DIR, compression)
        
        if results:
            logger.info("\n" + "="*70)
            logger.info("📊 ИТОГИ КОНВЕРТАЦИИ")
            logger.info("="*70)
            
            total_input_size = sum(r['input_size_mb'] for r in results)
            total_output_size = sum(r['output_size_mb'] for r in results)
            total_compression = (1 - total_output_size / total_input_size) * 100 if total_input_size > 0 else 0
            
            logger.info(f"   📁 Файлов конвертировано: {len(results)}")
            logger.info(f"   📦 Было (CSV): {total_input_size:.2f} МБ")
            logger.info(f"   📦 Стало (Parquet): {total_output_size:.2f} МБ")
            logger.info(f"   📉 Общее сжатие: {total_compression:.1f}%")
            logger.info(f"   💾 Экономия места: {total_input_size - total_output_size:.2f} МБ")
    
    logger.info("\n" + "="*70)
    logger.info("✅ КОНВЕРТАЦИЯ ЗАВЕРШЕНА")
    logger.info("="*70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
