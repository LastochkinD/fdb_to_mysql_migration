"""
Модуль конфигурации приложения
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv


class Config:
    """Класс конфигурации с поддержкой YAML и .env файлов"""

    def __init__(self, config_path: Optional[str] = None):
        """
        Инициализация конфигурации

        Args:
            config_path: Путь к YAML файлу конфигурации
        """
        self.config_path = config_path or self._find_config_file()
        self.config: Dict[str, Any] = {}
        self._load_config()

    def _find_config_file(self) -> str:
        """Поиск файла конфигурации в текущей директории"""
        possible_paths = [
            "config.yaml",
            "config.yml",
            os.path.join(os.path.dirname(__file__), "..", "config.yaml"),
        ]
        for path in possible_paths:
            if Path(path).exists():
                return path
        return "config.yaml"

    def _load_config(self) -> None:
        """Загрузка конфигурации из YAML файла"""
        if Path(self.config_path).exists():
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f) or {}
        else:
            self.config = {}

    def _load_env(self) -> None:
        """Загрузка переменных окружения"""
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        if Path(env_path).exists():
            load_dotenv(env_path)

    def get_firebird_config(self) -> Dict[str, Any]:
        """Получение конфигурации Firebird"""
        return {
            "host": os.environ.get("FIREBIRD_HOST") or self.config.get("firebird", {}).get("host", "localhost"),
            "port": int(os.environ.get("FIREBIRD_PORT") or self.config.get("firebird", {}).get("port", 3050)),
            "database": os.environ.get("FIREBIRD_DATABASE") or self.config.get("firebird", {}).get("database"),
            "user": os.environ.get("FIREBIRD_USER") or self.config.get("firebird", {}).get("user", "SYSDBA"),
            "password": os.environ.get("FIREBIRD_PASSWORD") or self.config.get("firebird", {}).get("password"),
            "charset": os.environ.get("FIREBIRD_CHARSET") or self.config.get("firebird", {}).get("charset", "UTF8"),
        }

    def get_mysql_config(self) -> Dict[str, Any]:
        """Получение конфигурации MySQL"""
        return {
            "host": os.environ.get("MYSQL_HOST") or self.config.get("mysql", {}).get("host", "localhost"),
            "port": int(os.environ.get("MYSQL_PORT") or self.config.get("mysql", {}).get("port", 3306)),
            "database": os.environ.get("MYSQL_DATABASE") or self.config.get("mysql", {}).get("database"),
            "user": os.environ.get("MYSQL_USER") or self.config.get("mysql", {}).get("user", "root"),
            "password": os.environ.get("MYSQL_PASSWORD") or self.config.get("mysql", {}).get("password"),
            "charset": os.environ.get("MYSQL_CHARSET") or self.config.get("mysql", {}).get("charset", "utf8mb4"),
        }

    def get_migration_config(self) -> Dict[str, Any]:
        """Получение настроек миграции"""
        return {
            "batch_size": int(self.config.get("migration", {}).get("batch_size", 1000)),
            "drop_tables": bool(self.config.get("migration", {}).get("drop_tables", False)),
            "transfer_data": bool(self.config.get("migration", {}).get("transfer_data", True)),
            "transfer_structure": bool(self.config.get("migration", {}).get("transfer_structure", True)),
        }
