#!/bin/bash
# =============================================================================
# ProjectZZZ - Setup Script
# =============================================================================
# Автоматическая настройка окружения для разработки
# =============================================================================

set -e

echo "=============================================="
echo "🚀 ProjectZZZ - Setup Script"
echo "=============================================="

# Проверка Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 не найден!"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo "✅ Python $PYTHON_VERSION"

# Создание виртуального окружения
if [ ! -d "venv" ]; then
    echo "📦 Создание виртуального окружения..."
    python3 -m venv venv
else
    echo "✅ Виртуальное окружение существует"
fi

# Активация
echo "🔌 Активация виртуального окружения..."
source venv/bin/activate

# Обновление pip
echo "📦 Обновление pip..."
pip install --upgrade pip

# Установка зависимостей
echo "📦 Установка зависимостей..."
pip install -r requirements.txt

# Создание директорий
echo "📁 Создание директорий..."
mkdir -p data/output data/cache docs/logs config

# Создание .env из .env.example
if [ ! -f ".env" ] && [ -f ".env.example" ]; then
    echo "⚙️ Создание .env из .env.example..."
    cp .env.example .env
    echo "⚠️ Не забудьте заполнить .env своими данными!"
else
    echo "✅ .env существует"
fi

# Проверка конфигурации
echo "🔍 Проверка конфигурации..."
if [ -f "config/config.yaml" ]; then
    echo "✅ config.yaml существует"
else
    echo "⚠️ config.yaml не найден!"
fi

# Запуск тестов
echo "🧪 Запуск тестов..."
pytest tests/test_security.py -v --tb=short || true

echo ""
echo "=============================================="
echo "✅ Setup завершен!"
echo "=============================================="
echo ""
echo "Следующие шаги:"
echo "1. Заполните .env своими данными"
echo "2. Настройте config.yaml"
echo "3. Запустите: docker-compose up -d db"
echo "4. Запустите: python src/load_references.py"
echo ""
