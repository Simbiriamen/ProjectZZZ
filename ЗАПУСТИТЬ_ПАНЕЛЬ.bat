@echo off
chcp 65001 >nul
title ProjectZZZ - Панель Управления

echo ============================================================
echo    ProjectZZZ - Система Рекомендаций SKU
echo    Запуск Панели Управления
echo ============================================================
echo.

cd /d D:\ProjectZZZ

if not exist venv\Scripts\activate.bat (
    echo [ERROR] Виртуальное окружение не найдено!
    echo Создайте его командой: python -m venv venv
    pause
    exit /b 1
)

echo [OK] Активация виртуального окружения...
call venv\Scripts\activate.bat

if not exist src\gui_control_panel.py (
    echo [ERROR] Файл панели не найден: src\gui_control_panel.py
    pause
    exit /b 1
)

echo [OK] Запуск панели управления...
echo.
python src\gui_control_panel.py

if errorlevel 1 (
    echo.
    echo [ERROR] Панель завершена с ошибкой!
    pause
)