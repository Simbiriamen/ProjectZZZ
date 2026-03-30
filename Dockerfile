# =============================================================================
# ProjectZZZ Dockerfile
# =============================================================================
# Многоэтапная сборка для минимизации размера образа
# =============================================================================

# -----------------------------------------------------------------------------
# ЭТАП 1: BUILD
# -----------------------------------------------------------------------------
FROM python:3.10-slim as builder

WORKDIR /app

# Системные зависимости для компиляции
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python зависимости
COPY requirements.txt .

RUN pip install --no-cache-dir --user -r requirements.txt

# -----------------------------------------------------------------------------
# ЭТАП 2: RUNTIME
# -----------------------------------------------------------------------------
FROM python:3.10-slim as runtime

WORKDIR /app

# Метки
LABEL org.opencontainers.image.source="https://github.com/Simbiriamen/ProjectZZZ"
LABEL org.opencontainers.image.description="ProjectZZZ ML Recommendation System"
LABEL org.opencontainers.image.licenses="MIT"

# Создаём пользователя для безопасности
RUN useradd --create-home --shell /bin/bash appuser

# Копируем зависимости из builder этапа
COPY --from=builder /root/.local /home/appuser/.local

# Копируем код приложения
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser models/ ./models/
COPY --chown=appuser:appuser config/ ./config/

# Копируем скрипты инициализации
COPY --chown=appuser:appuser scripts/ ./scripts/

# Создаём директории для данных и логов
RUN mkdir -p /app/data/output /app/data/cache /app/docs/logs \
    && chown -R appuser:appuser /app

# Переключаемся на не-root пользователя
USER appuser

# Добавляем локальные бинарники в PATH
ENV PATH=/home/appuser/.local/bin:$PATH
ENV PYTHONPATH=/app

# Переменные окружения
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Точка входа
ENTRYPOINT ["python"]

# Команда по умолчанию
CMD ["src/generate_recommendations.py"]
