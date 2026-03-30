# -*- coding: utf-8 -*-
"""
archive_output.py v1.0
🔧 АРХИВАЦИЯ ВЫХОДНЫХ ФАЙЛОВ
Назначение:
  - Архивация старых рекомендаций (output/*.xlsx)
  - Удаление файлов старше N дней
  - Освобождение места на диске

Использование:
  python src/archive_output.py [--days 30] [--delete]
"""

import sys
import logging
import argparse
import zipfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta

# ==============================================================================
# КОНФИГУРАЦИЯ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = DATA_DIR / "output"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "archive_output.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# ФУНКЦИИ
# ==============================================================================
def get_file_age_days(file_path: Path) -> int:
    """Возвращает возраст файла в днях"""
    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
    age = datetime.now() - mtime
    return age.days


def get_file_size_mb(file_path: Path) -> float:
    """Возвращает размер файла в МБ"""
    return file_path.stat().st_size / (1024 * 1024)


def find_old_files(
    directory: Path,
    pattern: str = "*.xlsx",
    max_age_days: int = 30
) -> list:
    """Находит файлы старше указанного возраста"""
    old_files = []
    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    
    for file_path in directory.glob(pattern):
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
        if mtime < cutoff_date:
            old_files.append(file_path)
    
    return old_files


def create_archive(
    files: list,
    archive_path: Path,
    remove_original: bool = False
) -> dict:
    """
    Создаёт ZIP архив из файлов.
    
    Args:
        files: Список файлов для архивации
        archive_path: Путь к выходному ZIP файлу
        remove_original: Удалить оригиналы после архивации
    
    Returns:
        dict со статистикой архивации
    """
    logger.info(f"\n📦 Создание архива: {archive_path.name}")
    
    total_original_size = sum(f.stat().st_size for f in files)
    logger.info(f"   📊 Файлов: {len(files)}")
    logger.info(f"   📦 Исходный размер: {total_original_size / (1024*1024):.2f} МБ")
    
    start_time = datetime.now()
    
    try:
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files:
                # Добавляем файл в архив с относительным путём
                arcname = file_path.name
                zipf.write(file_path, arcname)
                logger.debug(f"   📝 Добавлен: {file_path.name}")
        
        # Получаем размер архива
        archive_size = archive_path.stat().st_size
        compression_ratio = (1 - archive_size / total_original_size) * 100 if total_original_size > 0 else 0
        
        # Удаление оригиналов
        if remove_original:
            for file_path in files:
                file_path.unlink()
                logger.debug(f"   🗑️ Удалён: {file_path.name}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"   ✅ Архив создан")
        logger.info(f"   📦 Размер архива: {archive_size / (1024*1024):.2f} МБ")
        logger.info(f"   📉 Сжатие: {compression_ratio:.1f}%")
        logger.info(f"   ⏱️ Время: {elapsed:.1f} сек")
        
        return {
            'archive_path': str(archive_path),
            'files_count': len(files),
            'original_size_mb': total_original_size / (1024*1024),
            'archive_size_mb': archive_size / (1024*1024),
            'compression_ratio': compression_ratio,
            'removed_originals': remove_original,
            'elapsed_sec': elapsed
        }
    
    except Exception as e:
        logger.error(f"   ❌ Ошибка создания архива: {e}")
        raise


def cleanup_old_archives(
    directory: Path,
    retention_days: int = 365
) -> int:
    """Удаляет архивы старше указанного периода"""
    deleted_count = 0
    
    for archive_path in directory.glob("*.zip"):
        age_days = get_file_age_days(archive_path)
        if age_days > retention_days:
            archive_path.unlink()
            logger.info(f"   🗑️ Удалён старый архив: {archive_path.name} ({age_days} дней)")
            deleted_count += 1
    
    return deleted_count


def get_directory_stats(directory: Path) -> dict:
    """Возвращает статистику директории"""
    files = list(directory.glob("*"))
    total_size = sum(f.stat().st_size for f in files if f.is_file())
    
    return {
        'file_count': len(files),
        'total_size_mb': total_size / (1024 * 1024),
        'newest_file': max(files, key=lambda f: f.stat().st_mtime).name if files else None,
        'oldest_file': min(files, key=lambda f: f.stat().st_mtime).name if files else None
    }


# ==============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description='Архивация выходных файлов ProjectZZZ'
    )
    parser.add_argument(
        '--days', '-d',
        type=int,
        default=30,
        help='Архивировать файлы старше N дней (по умолчанию: 30)'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Удалить оригиналы после архивации'
    )
    parser.add_argument(
        '--retention', '-r',
        type=int,
        default=365,
        help='Хранить архивы N дней (по умолчанию: 365)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Тестовый режим (без реальных действий)'
    )
    
    args = parser.parse_args()
    
    logger.info("="*70)
    logger.info("📦 ProjectZZZ - OUTPUT ARCHIVER v1.0")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    # Статистика до
    logger.info("\n📊 Статистика OUTPUT до архивации:")
    stats_before = get_directory_stats(OUTPUT_DIR)
    logger.info(f"   📁 Файлов: {stats_before['file_count']}")
    logger.info(f"   📦 Размер: {stats_before['total_size_mb']:.2f} МБ")
    if stats_before['oldest_file']:
        logger.info(f"   📅 Старейший файл: {stats_before['oldest_file']}")
    
    # Поиск старых файлов
    old_files = find_old_files(OUTPUT_DIR, "*.xlsx", args.days)
    
    if not old_files:
        logger.info(f"\nℹ️ Нет файлов старше {args.days} дней для архивации")
    else:
        logger.info(f"\n📋 Найдено файлов для архивации: {len(old_files)}")
        for f in old_files:
            age = get_file_age_days(f)
            size = get_file_size_mb(f)
            logger.info(f"   • {f.name} ({size:.2f} МБ, {age} дней)")
        
        if args.dry_run:
            logger.info("\n⏭️ DRY-RUN: Реальная архивация пропущена")
        else:
            # Создание архива
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_name = f"output_archive_{timestamp}.zip"
            archive_path = ARCHIVE_DIR / archive_name
            
            create_archive(old_files, archive_path, remove_original=args.delete)
    
    # Очистка старых архивов
    logger.info(f"\n🗑️ Очистка архивов старше {args.retention} дней...")
    deleted = cleanup_old_archives(ARCHIVE_DIR, args.retention)
    logger.info(f"   ✅ Удалено архивов: {deleted}")
    
    # Статистика после
    logger.info("\n📊 Статистика OUTPUT после архивации:")
    stats_after = get_directory_stats(OUTPUT_DIR)
    logger.info(f"   📁 Файлов: {stats_after['file_count']}")
    logger.info(f"   📦 Размер: {stats_after['total_size_mb']:.2f} МБ")
    
    if stats_before['total_size_mb'] > 0:
        freed = stats_before['total_size_mb'] - stats_after['total_size_mb']
        logger.info(f"   💾 Освобождено: {freed:.2f} МБ")
    
    # Статистика архивов
    archive_stats = get_directory_stats(ARCHIVE_DIR)
    logger.info(f"\n📊 Статистика ARCHIVE:")
    logger.info(f"   📁 Архивов: {archive_stats['file_count']}")
    logger.info(f"   📦 Общий размер: {archive_stats['total_size_mb']:.2f} МБ")
    
    logger.info("\n" + "="*70)
    logger.info("✅ АРХИВАЦИЯ ЗАВЕРШЕНА")
    logger.info("="*70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
