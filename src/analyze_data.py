# -*- coding: utf-8 -*-
"""
analyze_data.py
Анализ структуры исходных файлов Excel
"""

import os
import sys
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path("D:/ProjectZZZ")
RAW_DIR = PROJECT_ROOT / "data" / "raw"

def analyze_excel(file_path):
    """Анализирует структуру Excel файла"""
    print(f"\n{'='*70}")
    print(f"📄 ФАЙЛ: {file_path.name}")
    print(f"{'='*70}")
    
    try:
        # Получаем список листов
        xl = pd.ExcelFile(file_path)
        print(f"📑 Листы: {xl.sheet_names}")
        
        for sheet in xl.sheet_names:
            print(f"\n--- Лист: {sheet} ---")
            
            # Читаем первые 5 строк для анализа заголовков
            df = pd.read_excel(file_path, sheet_name=sheet, nrows=5)
            print(f"Колонки ({len(df.columns)}):")
            for i, col in enumerate(df.columns):
                print(f"  {i+1}. {col}")
            
            # Читаем файл полностью для статистики
            df_full = pd.read_excel(file_path, sheet_name=sheet)
            print(f"\n📊 Строк: {len(df_full):,}")
            print(f"📊 Колонок: {len(df_full.columns)}")
            
            # Типы данных
            print(f"\n📌 Типы данных:")
            for col, dtype in df_full.dtypes.items():
                print(f"  {col}: {dtype}")
            
            # Пропуски
            missing = df_full.isnull().sum()
            if missing.any():
                print(f"\n⚠️ Пропуски:")
                for col, count in missing[missing > 0].items():
                    print(f"  {col}: {count:,} ({count/len(df_full)*100:.1f}%)")
        
        xl.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def main():
    print("🔍 АНАЛИЗ ИСХОДНЫХ ДАННЫХ")
    print(f"📂 Папка: {RAW_DIR}")
    
    if not RAW_DIR.exists():
        print(f"❌ Папка не существует: {RAW_DIR}")
        return
    
    excel_files = list(RAW_DIR.glob("*.xlsx")) + list(RAW_DIR.glob("*.xls"))
    
    if not excel_files:
        print("❌ Excel файлы не найдены!")
        print(f"💡 Положите файлы в: {RAW_DIR}")
        return
    
    print(f"✅ Найдено файлов: {len(excel_files)}")
    
    for file_path in excel_files:
        analyze_excel(file_path)

if __name__ == "__main__":
    main()