#!/usr/bin/env python3
"""
Скрипт для загрузки файлов в базу знаний Open WebUI через API.
Обходит папку проекта, исключая только указанные подпапки.
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path
from typing import Set

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Загрузка файлов в базу знаний Open WebUI.')
    parser.add_argument('--url', required=True, help='URL сервера Open WebUI')
    parser.add_argument('--api-key', required=True, help='API ключ')
    parser.add_argument('--knowledge-id', required=True, help='ID базы знаний')
    parser.add_argument('--project-path', required=True, help='Путь к корню проекта')
    parser.add_argument('--exclude', default=(
        '.git,.svn,.hg,.env,node_modules,__pycache__,logs,tmp,temp,backup,venv,env,.idea,.vscode,'
        '.pytest_cache,.mypy_cache,dist,build,*.egg-info'
    ), help='Папки для исключения по имени (через запятую)')
    parser.add_argument('--exclude-paths', default='data/output,data/processed,data/raw,data/stocks,.git',
                        help='Точные относительные пути для исключения (через запятую)')
    parser.add_argument('--exclude-files', default='',
                        help='Имена файлов для исключения (через запятую)')
    parser.add_argument('--extensions', default=(
        '.txt,.md,.pdf,.docx,.py,.js,.html,.css,.json,.yaml,.yml,.xml,.csv,.xlsx,.log,.ini,.cfg,.sql'
    ), help='Расширения для загрузки (через запятую)')
    parser.add_argument('--delay', type=float, default=1.0, help='Задержка между загрузками (сек)')
    parser.add_argument('--verbose', action='store_true', help='Подробный вывод')
    return parser.parse_args()


def is_excluded_by_path(path: Path, project_root: Path, exclude_paths: Set[str]) -> bool:
    try:
        rel_path = path.relative_to(project_root)
    except ValueError:
        return False
    current = rel_path
    while current != Path('.'):
        if str(current) in exclude_paths:
            return True
        current = current.parent
    return False


def is_excluded_path(path: Path, project_root: Path, exclude_dirs: Set[str], exclude_files: Set[str], exclude_paths: Set[str]) -> bool:
    if is_excluded_by_path(path, project_root, exclude_paths):
        return True
    for part in path.parts:
        if part in exclude_dirs:
            return True
    if path.is_file() and path.name in exclude_files:
        return True
    return False


def get_supported_extensions(ext_list: str) -> Set[str]:
    return {ext.strip().lower() for ext in ext_list.split(',') if ext.strip()}


def upload_file(session: requests.Session, api_url: str, knowledge_id: str, api_key: str, file_path: Path, delay: float) -> bool:
    upload_endpoint = f"{api_url}/api/v1/knowledge/{knowledge_id}/documents/upload"
    headers = {'Authorization': f'Bearer {api_key}'}
    with open(file_path, 'rb') as f:
        files = {'file': (file_path.name, f, 'application/octet-stream')}
        data = {'name': file_path.name}
        try:
            response = session.post(upload_endpoint, headers=headers, files=files, data=data, timeout=60)
            response.raise_for_status()
            logger.info(f"Загружен: {file_path}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка загрузки {file_path}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.debug(f"Ответ сервера: {e.response.text}")
            return False
        finally:
            time.sleep(delay)


def main():
    args = parse_arguments()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    project_path = Path(args.project_path).resolve()
    if not project_path.exists():
        logger.error(f"Путь не существует: {project_path}")
        sys.exit(1)

    exclude_dirs = set(args.exclude.split(','))
    exclude_paths = set(filter(None, args.exclude_paths.split(',')))
    exclude_files = set(filter(None, args.exclude_files.split(',')))
    extensions = get_supported_extensions(args.extensions)

    session = requests.Session()

    logger.info(f"Обход папки: {project_path}")
    logger.info(f"Исключаемые папки (по имени): {', '.join(exclude_dirs)}")
    logger.info(f"Исключаемые пути: {', '.join(exclude_paths) if exclude_paths else 'нет'}")
    logger.info(f"Исключаемые файлы: {', '.join(exclude_files) if exclude_files else 'нет'}")
    logger.info(f"Поддерживаемые расширения: {', '.join(extensions)}")

    file_count = success_count = error_count = 0

    for root, dirs, files in os.walk(project_path):
        root_path = Path(root)
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        if is_excluded_path(root_path, project_path, exclude_dirs, set(), exclude_paths):
            continue
        for file_name in files:
            file_path = root_path / file_name
            if is_excluded_path(file_path, project_path, exclude_dirs, exclude_files, exclude_paths):
                logger.debug(f"Пропущен (исключён): {file_path}")
                continue
            if file_path.suffix.lower() not in extensions:
                logger.debug(f"Пропущен (расширение): {file_path}")
                continue
            file_count += 1
            logger.info(f"Обработка файла {file_count}: {file_path}")
            if upload_file(session, args.url, args.knowledge_id, args.api_key, file_path, args.delay):
                success_count += 1
            else:
                error_count += 1

    logger.info(f"Завершено. Всего: {file_count}, успешно: {success_count}, ошибок: {error_count}")


if __name__ == "__main__":
    main()