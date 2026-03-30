@echo off
REM =============================================================================
REM ProjectZZZ - Setup Script (Windows)
REM =============================================================================
REM Автоматическая настройка окружения для разработки
REM =============================================================================

echo ==============================================
echo 🚀 ProjectZZZ - Setup Script
echo ==============================================

REM Проверка Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python не найден!
    exit /b 1
)

echo ✅ Python установлен

REM Создание виртуального окружения
if not exist "venv" (
    echo 📦 Создание виртуального окружения...
    python -m venv venv
) else (
    echo ✅ Виртуальное окружение существует
)

REM Активация
echo 🔌 Активация виртуального окружения...
call venv\Scripts\activate.bat

REM Обновление pip
echo 📦 Обновление pip...
python -m pip install --upgrade pip

REM Установка зависимостей
echo 📦 Установка зависимостей...
pip install -r requirements.txt

REM Создание директорий
echo 📁 Создание директорий...
if not exist "data\output" mkdir data\output
if not exist "data\cache" mkdir data\cache
if not exist "docs\logs" mkdir docs\logs
if not exist "config" mkdir config

REM Создание .env из .env.example
if not exist ".env" (
    if exist ".env.example" (
        echo ⚙️ Создание .env из .env.example...
        copy .env.example .env
        echo ⚠️ Не забудьте заполнить .env своими данными!
    )
) else (
    echo ✅ .env существует
)

REM Проверка конфигурации
echo 🔍 Проверка конфигурации...
if exist "config\config.yaml" (
    echo ✅ config.yaml существует
) else (
    echo ⚠️ config.yaml не найден!
)

echo.
echo ==============================================
echo ✅ Setup завершен!
echo ==============================================
echo.
echo Следующие шаги:
echo 1. Заполните .env своими данными
echo 2. Настройте config.yaml
echo 3. Запустите: docker-compose up -d db
echo 4. Запустите: python src\load_references.py
echo.
