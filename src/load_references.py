# -*- coding: utf-8 -*-
"""
load_references.py
Загрузка справочников для ProjectZZZ (PostgreSQL)
Версия: 3.4 - ИСПРАВЛЕНИЕ: используем append вместо replace
"""

import sys
import pandas as pd
import logging
import time
import json
import hashlib
from pathlib import Path
from sqlalchemy import create_engine, text, inspect
import psycopg2
import yaml
from datetime import datetime

# ==============================================================================
# НАСТРОЙКИ ПУТЕЙ
# ==============================================================================
PROJECT_ROOT = Path("D:/ProjectZZZ")
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
META_PATH = PROJECT_ROOT / "config" / "references_meta.json"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "docs" / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)
(CONFIG_PATH.parent).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "load_references.log", encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ==============================================================================
# КЛАСС ЗАГРУЗЧИКА
# ==============================================================================
class ReferenceLoader:
    def __init__(self, force_reload: bool = False):
        self.meta = self._load_meta()
        self.current_meta = {}
        self.force_reload = force_reload
        self.config = self._load_config()
        self.engine = self._get_db_engine()
        
    def _load_config(self):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _get_db_engine(self):
        db = self.config['database']
        return create_engine(
            f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
        )
    
    def _load_meta(self):
        if META_PATH.exists() and META_PATH.stat().st_size > 0:
            try:
                with open(META_PATH, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def _save_meta(self):
        with open(META_PATH, 'w', encoding='utf-8') as f:
            json.dump(self.current_meta, f, ensure_ascii=False, indent=2)
    
    def _get_file_signature(self, file_path):
        try:
            stat = file_path.stat()
            file_size = stat.st_size
            file_mtime = stat.st_mtime
            
            if file_size > 10_000_000:
                with open(file_path, 'rb') as f:
                    sample = f.read(1_000_000)
                file_hash = hashlib.md5(sample).hexdigest()[:8]
            else:
                with open(file_path, 'rb') as f:
                    file_hash = hashlib.md5(f.read()).hexdigest()[:8]
            
            return {
                "hash": file_hash,
                "size": file_size,
                "mtime": file_mtime,
                "mtime_str": datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                "name": file_path.name
            }
        except Exception as e:
            logger.error(f"   ❌ Ошибка чтения файла: {e}")
            return {"hash": "error", "size": 0, "mtime": 0, "mtime_str": "error", "name": file_path.name}
    
    def _needs_reload(self, file_key, file_sig):
        if self.force_reload:
            return True
        if file_key not in self.meta:
            return True
        old = self.meta[file_key]
        return (
            old.get('hash') != file_sig.get('hash') or
            old.get('size') != file_sig.get('size') or
            old.get('mtime') != file_sig.get('mtime')
        )
    
    def _find_file_by_prefix(self, prefix):
        if not RAW_DIR.exists():
            return None
        
        prefix_lower = prefix.lower()
        excel_files = list(RAW_DIR.glob("*.xlsx")) + list(RAW_DIR.glob("*.xls"))
        
        for file_path in excel_files:
            if file_path.name.lower().startswith(prefix_lower):
                return file_path
        
        return None
    
    def _table_exists(self, table_name):
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except:
            return False
    
    def _column_exists(self, table_name, column_name):
        try:
            inspector = inspect(self.engine)
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            return column_name in columns
        except:
            return False
    
    def _add_column_if_not_exists(self, table_name, column_name, column_type):
        if not self._column_exists(table_name, column_name):
            with self.engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))
                conn.commit()
            logger.info(f"   📁 Колонка {column_name} добавлена в {table_name}")
            return True
        return False
    
    def load_customers(self, file_path):
        """Загрузка клиентов (Справочник КА*)"""
        logger.info(f"\n👥 ЗАГРУЗКА КЛИЕНТОВ: {file_path.name}")
        try:
            # 🔧 ИСПРАВЛЕНИЕ v3.5: Контекстный менеджер для Excel
            with pd.ExcelFile(file_path, engine='openpyxl') as xl:
                df = pd.read_excel(
                    xl,
                    sheet_name="TDSheet",
                    skiprows=4,
                    dtype=str
                )
            
            df = df.loc[:, df.columns.notna() & (df.columns != '')]
            df = df.loc[:, ~df.columns.str.startswith('Unnamed', na=False)]
            
            logger.info(f"   📊 Колонок: {len(df.columns)}, строк: {len(df)}")
            
            column_mapping = {
                'Контрагент.Родитель.Код': 'client_id',
                'Контрагент.Родитель.Наименование': 'client_name',
                'Контрагент.Основной менеджер покупателя': 'manager_id',
                'Контрагент.Филиал': 'branch',
                'Контрагент.Родитель.Канал сбыта': 'sales_channel',
                'Контрагент.Родитель.Относится к с': 'network_name',
                'Контрагент.МАП по маслам и автохим': 'map_oil_autochem'
            }
            
            rename_map = {}
            for excel_col, db_col in column_mapping.items():
                matching = [c for c in df.columns if excel_col in str(c)]
                if matching:
                    rename_map[matching[0]] = db_col
            
            df.rename(columns=rename_map, inplace=True)
            
            if 'client_id' not in df.columns:
                logger.error("   ❌ Не найдена колонка client_id")
                return 0
            
            df['client_id'] = df['client_id'].fillna('').astype(str).str.strip()
            df = df[df['client_id'].str.len() > 0]
            df = df.drop_duplicates(subset=['client_id'], keep='last')
            
            logger.info(f"   📊 После очистки: {len(df):,} записей")
            
            # Добавляем недостающие колонки
            self._add_column_if_not_exists('clients', 'segment', 'TEXT')
            self._add_column_if_not_exists('clients', 'metadata', 'JSONB')
            
            df['source_file'] = file_path.name
            
            # 🔧 ИСПРАВЛЕНИЕ: используем append вместо replace!
            df.to_sql('clients', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            logger.info(f"   ✅ Загружено: {len(df):,} записей")
            return len(df)
            
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def load_items(self, file_path):
        """Загрузка товаров (Справочник номенклатуры*)"""
        logger.info(f"\n📦 ЗАГРУЗКА ТОВАРОВ: {file_path.name}")
        try:
            # 🔧 ИСПРАВЛЕНИЕ v3.5: Контекстный менеджер для Excel
            with pd.ExcelFile(file_path, engine='openpyxl') as xl:
                df = pd.read_excel(
                    xl,
                    sheet_name="TDSheet",
                    skiprows=6,
                    dtype=str
                )
            
            df = df.loc[:, df.columns.notna() & (df.columns != '')]
            df = df.loc[:, ~df.columns.str.startswith('Unnamed', na=False)]
            
            logger.info(f"   📊 Колонок: {len(df.columns)}, строк: {len(df)}")
            
            column_mapping = {
                'Код': 'sku_id',
                'Бренд': 'brand',
                'Номенклатура': 'sku_name',
                'Артикул': 'article',
                'Применяемость': 'applicability',
                'Группы товаров': 'product_group',
                'Финансовая группа': 'financial_group',
                'Родитель': 'marketing_group1',
                'Наименование': 'marketing_group2'
            }
            
            rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
            df.rename(columns=rename_map, inplace=True)
            
            if 'sku_id' not in df.columns:
                logger.error("   ❌ Не найдена колонка sku_id")
                return 0
            
            df['sku_id'] = df['sku_id'].fillna('').astype(str).str.strip()
            df = df[df['sku_id'].str.len() > 0]
            df = df.drop_duplicates(subset=['sku_id'], keep='last')
            
            logger.info(f"   📊 После очистки: {len(df):,} записей")
            
            # 🔧 Добавляем НОВЫЕ колонки (критично для отбора!)
            self._add_column_if_not_exists('skus', 'stock', 'INTEGER DEFAULT 0')
            self._add_column_if_not_exists('skus', 'margin', 'DECIMAL(5,4)')
            self._add_column_if_not_exists('skus', 'price', 'DECIMAL(12,2)')
            self._add_column_if_not_exists('skus', 'category', 'TEXT')
            self._add_column_if_not_exists('skus', 'group_id', 'TEXT')
            self._add_column_if_not_exists('skus', 'is_new', 'TEXT')
            self._add_column_if_not_exists('skus', 'applicable_brands', 'TEXT[]')
            self._add_column_if_not_exists('skus', 'applicability_entry_count', 'INTEGER')
            self._add_column_if_not_exists('skus', 'brand_specialization', 'TEXT')
            
            df['source_file'] = file_path.name
            
            # 🔧 ИСПРАВЛЕНИЕ: используем append вместо replace!
            df.to_sql('skus', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            logger.info(f"   ✅ Загружено: {len(df):,} записей")
            return len(df)
            
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def load_minmax(self, file_path):
        """Загрузка мин-макс норм (Мин-макс*)"""
        logger.info(f"\n📊 ЗАГРУЗКА МИН-МАКС: {file_path.name}")

        selected_columns = [
            'Код НСИ', 'Бренд', 'Артикул', 'Номенклатура',
            'Код склада получателя', 'Макс получателя',
            'Маркетинговая группа', 'Кол сделок за посл 180 дней',
            'Продано за последние 180 дней шт',
            'Парная номенклатура', 'Комплект', 'Набор замен',
            'Код набора замен', 'Набор аналогов', 'Код набора аналогов'
        ]

        column_mapping = {
            'Код НСИ': 'sku_id',
            'Бренд': 'brand',
            'Артикул': 'article',
            'Номенклатура': 'sku_name',
            'Код склада получателя': 'warehouse_code',
            'Макс получателя': 'max_norm',
            'Маркетинговая группа': 'marketing_group',
            'Кол сделок за посл 180 дней': 'deals_180d',
            'Продано за последние 180 дней шт': 'sold_180d',
            'Парная номенклатура': 'paired_item',
            'Комплект': 'is_kit',
            'Набор замен': 'replacement_set',
            'Код набора замен': 'replacement_set_code',
            'Набор аналогов': 'analog_set',
            'Код набора аналогов': 'analog_set_code'
        }

        try:
            # 🔧 ИСПРАВЛЕНИЕ v3.5: Контекстный менеджер для Excel
            with pd.ExcelFile(file_path, engine='openpyxl') as xl:
                df_full = pd.read_excel(xl, sheet_name="TDSheet", header=0, dtype=str)
            
            logger.info(f"   📊 Всего: {len(df_full):,} строк, {len(df_full.columns)} колонок")
            
            available_cols = [c for c in selected_columns if c in df_full.columns]
            df = df_full[available_cols].copy()
            df.rename(columns=column_mapping, inplace=True)
            df = df.loc[:, ~df.columns.duplicated(keep='first')]
            
            if 'sku_id' in df.columns:
                df['sku_id'] = df['sku_id'].fillna('').astype(str).str.strip()
                df = df[df['sku_id'].str.len() > 0]
            if 'warehouse_code' in df.columns:
                df['warehouse_code'] = df['warehouse_code'].fillna('').astype(str).str.strip()
            
            numeric_cols = ['max_norm', 'deals_180d', 'sold_180d']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            if 'warehouse_code' in df.columns and 'sku_id' in df.columns:
                before_dedup = len(df)
                df = df.drop_duplicates(subset=['sku_id', 'warehouse_code'], keep='last')
                logger.info(f"   📊 Удалено дубликатов: {before_dedup - len(df):,}")
            
            logger.info(f"   📊 После очистки: {len(df):,} записей")
            
            df['source_file'] = file_path.name
            
            # 🔧 ИСПРАВЛЕНИЕ: используем append вместо replace!
            df.to_sql('minmax_norms', self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
            
            logger.info(f"   ✅ Загружено: {len(df):,} записей")
            return len(df)
            
        except Exception as e:
            logger.error(f"   ❌ Ошибка: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def run(self):
        start_time = time.time()
        
        logger.info("=" * 70)
        logger.info("🚀 ЗАГРУЗКА СПРАВОЧНИКОВ ProjectZZZ v3.4")
        logger.info(f"📅 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
        
        references = [
            ("Справочник КА", self.load_customers, "Клиенты"),
            ("Справочник номенклатуры", self.load_items, "Товары"),
            ("Мин-макс", self.load_minmax, "Мин-макс"),
        ]
        
        results = {}
        total = 0
        
        for prefix, func, name in references:
            file_path = self._find_file_by_prefix(prefix)
            
            if not file_path:
                logger.error(f"❌ Файл не найден (префикс '{prefix}'): {RAW_DIR}")
                results[name] = 0
                continue
            
            logger.info(f"\n🔍 Найден файл: {file_path.name}")
            
            file_sig = self._get_file_signature(file_path)
            file_key = prefix.replace(" ", "_").lower()
            
            if not self._needs_reload(file_key, file_sig):
                logger.info(f"✅ {name}: файл не изменился (пропущено)")
                self.current_meta[file_key] = file_sig
                continue
            
            count = func(file_path)
            results[name] = count
            
            if count > 0:
                total += count
                self.current_meta[file_key] = file_sig
                logger.info(f"   💾 Мета-файл обновлён")
        
        self._save_meta()
        
        logger.info("\n" + "=" * 70)
        logger.info("📊 ИТОГИ")
        logger.info("=" * 70)
        for name, count in results.items():
            status = "✅" if count > 0 else "❌"
            logger.info(f"  {status} {name}: {count:,}")
        logger.info(f"\n  📥 Всего: {total:,} записей")
        logger.info(f"  ⏱️ Время: {time.time() - start_time:.1f} сек")
        logger.info("=" * 70)
        
        self.engine.dispose()


# ==============================================================================
# ЗАПУСК
# ==============================================================================
if __name__ == "__main__":
    force = "--force" in sys.argv or "-f" in sys.argv
    ReferenceLoader(force_reload=force).run()