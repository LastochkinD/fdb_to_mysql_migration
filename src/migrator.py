"""
Модуль миграции данных из Firebird в MySQL
"""

import sys
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from .config import Config
from .firebird_connector import FirebirdConnector
from .mysql_connector import MySQLConnector


# Mapping Firebird types to MySQL types
# ПРИМЕЧАНИЕ: DECIMAL(12) и NUMERIC(13) в этой базе могут содержать даты и время как строки
FIREBIRD_TO_MYSQL = {
    7: ("SMALLINT", None),       # SHORT
    8: ("INTEGER", None),        # LONG
    10: ("FLOAT", None),        # FLOAT
    11: ("DOUBLE", None),       # DOUBLE PRECISION
    12: ("VARCHAR(50)", None),  # DECIMAL -> VARCHAR (может содержать даты/время как строки)
    13: ("VARCHAR(50)", None),  # NUMERIC -> VARCHAR (может содержать даты/время как строки)
    14: ("VARCHAR", None),      # VARCHAR
    15: ("CHAR", None),          # CHAR
    16: ("BIGINT", None),       # BIGINT
    27: ("DOUBLE", None),       # DOUBLE (используется для SUMMA, PRICE)
    35: ("DATETIME", None),     # TIMESTAMP
    37: ("VARCHAR", None),      # VARCHAR (строки в Firebird)
    40: ("BLOB", None),          # BLOB
    261: ("LONGTEXT", None),    # BLOB SUB_TYPE TEXT
}


class Migrator:
    """Класс для миграции данных из Firebird в MySQL"""

    def __init__(self, config: Config, lowercase: bool = False, drop_tables: bool = False):
        """
        Инициализация мигратора

        Args:
            config: Объект конфигурации
            lowercase: Создавать таблицы и поля в нижнем регистре
            drop_tables: Удалять существующие таблицы перед миграцией
        """
        self.config = config
        self.firebird: Optional[FirebirdConnector] = None
        self.mysql: Optional[MySQLConnector] = None
        self.migration_config = config.get_migration_config()
        self.lowercase = lowercase
        self.drop_tables = drop_tables

    def _to_lower(self, name: str) -> str:
        """Преобразование в нижний регистр если включено"""
        return name.lower() if self.lowercase else name

    def connect(self) -> None:
        """Установление соединений с базами данных"""
        print("Подключение к Firebird...")
        fb_config = self.config.get_firebird_config()
        self.firebird = FirebirdConnector(fb_config)
        self.firebird.connect()
        print("✓ Подключение к Firebird успешно")

        print("Подключение к MySQL...")
        mysql_config = self.config.get_mysql_config()
        self.mysql = MySQLConnector(mysql_config)
        self.mysql.connect()
        print("✓ Подключение к MySQL успешно")

        # Создание базы данных если не существует
        if mysql_config.get("database"):
            print(f"Создание/выбор базы данных: {mysql_config['database']}")
            self.mysql.create_database(mysql_config["database"])
            self.mysql.use_database(mysql_config["database"])

        # Удаление всех существующих таблиц если настроено
        if self.migration_config.get("drop_tables", False) or self.drop_tables:
            print("Удаление всех существующих таблиц...")
            dropped_count = self.mysql.drop_all_tables()
            print(f"✓ Удалено таблиц: {dropped_count}")

    def disconnect(self) -> None:
        """Закрытие соединений"""
        if self.firebird:
            self.firebird.disconnect()
        if self.mysql:
            self.mysql.disconnect()

    def migrate_structure(self, tables: List[str] = None) -> None:
        """
        Миграция структуры таблиц

        Args:
            tables: Список таблиц для миграции (None - все таблицы)
        """
        print("\n=== Миграция структуры ===")

        if not tables:
            tables = [t[0] for t in self.firebird.get_tables()]

        for table_name in tables:
            table_name_lower = self._to_lower(table_name)
            print(f"Обработка таблицы: {table_name_lower}")

            # Получение информации о колонках
            columns = self.firebird.get_table_columns(table_name)
            pk_columns = self.firebird.get_primary_key(table_name)

            # Преобразование типов Firebird в MySQL
            mysql_columns = []
            for col in columns:
                mysql_type = self._convert_fb_type_to_mysql(col)
                mysql_columns.append({
                    "name": self._to_lower(col["name"]),
                    "mysql_type": mysql_type,
                    "nullable": col["nullable"],
                })

            # Преобразование первичного ключа
            pk_columns_lower = [self._to_lower(pk) for pk in pk_columns] if pk_columns else None

            # Создание таблицы
            print(f"  Создание таблицы...")
            self.mysql.create_table(table_name_lower, mysql_columns, pk_columns_lower)

        print(f"✓ Структура {len(tables)} таблиц создана")

    def migrate_data(self, tables: List[str] = None) -> None:
        """
        Миграция данных

        Args:
            tables: Список таблиц для миграции (None - все таблицы)
        """
        print("\n=== Миграция данных ===")

        if not tables:
            tables = [t[0] for t in self.firebird.get_tables()]

        batch_size = self.migration_config.get("batch_size", 1000)
        total_rows = 0

        for table_name in tables:
            table_name_lower = self._to_lower(table_name)
            print(f"Миграция таблицы: {table_name_lower}")

            # Получение информации о колонках
            columns = self.firebird.get_table_columns(table_name)
            column_names = [self._to_lower(col["name"]) for col in columns]

            # Получение количества записей
            count = self.firebird.get_table_count(table_name)
            print(f"  Записей в источнике: {count}")

            if count == 0:
                print("  Таблица пустая, пропуск...")
                continue

            # Миграция данных пакетами
            rows_inserted = 0
            batch = []

            for row in self.firebird.get_table_data(table_name, batch_size):
                # Преобразование данных
                converted_row = self._convert_row_data(row, columns)
                batch.append(converted_row)

                if len(batch) >= batch_size:
                    self.mysql.insert_data(table_name_lower, column_names, batch)
                    rows_inserted += len(batch)
                    batch = []

            # Вставка оставшихся записей
            if batch:
                self.mysql.insert_data(table_name_lower, column_names, batch)
                rows_inserted += len(batch)

            self.mysql.commit()
            total_rows += rows_inserted
            print(f"  Вставлено записей: {rows_inserted}")

        print(f"✓ Всего мигрировано записей: {total_rows}")

    def _convert_fb_type_to_mysql(self, column: Dict[str, Any]) -> str:
        """
        Преобразование типа Firebird в MySQL

        Args:
            column: Информация о колонке

        Returns:
            Строка с типом MySQL
        """
        fb_type = column["type"]
        length = column["length"]
        precision = column["precision"]
        scale = column["scale"]

        if fb_type in FIREBIRD_TO_MYSQL:
            mysql_type = FIREBIRD_TO_MYSQL[fb_type][0]

            if mysql_type in ("CHAR", "VARCHAR") and length and length > 0:
                return f"{mysql_type}({length})"
            elif mysql_type in ("DECIMAL", "NUMERIC"):
                if precision and scale:
                    return f"{mysql_type}({precision}, {abs(scale)})"
                elif precision:
                    return f"{mysql_type}({precision})"

            return mysql_type

        return "TEXT"

    def _convert_row_data(self, row: tuple, columns: List[Dict[str, Any]]) -> tuple:
        """
        Преобразование данных строки

        Args:
            row: Кортеж с данными
            columns: Информация о колонках

        Returns:
            Преобразованный кортеж
        """
        result = []
        for value, col in zip(row, columns):
            fb_type = col["type"]
            
            if value is None:
                result.append(None)
            elif isinstance(value, bytes):
                # Blob данные
                if fb_type == 261:  # TEXT BLOB
                    try:
                        result.append(value.decode("utf-8", errors="replace"))
                    except:
                        result.append(None)
                else:
                    result.append(value)
            elif isinstance(value, datetime):
                # DATETIME/TIMESTAMP преобразуем в строку
                result.append(value.strftime('%Y-%m-%d %H:%M:%S'))
            elif isinstance(value, date):
                # DATE преобразуем в строку YYYY-MM-DD
                result.append(value.strftime('%Y-%m-%d'))
            elif isinstance(value, str):
                # Для TIMESTAMP/DATE передаём как есть
                if fb_type == 35:
                    result.append(value)
                else:
                    result.append(value.strip() if value else None)
            else:
                result.append(value)

        return tuple(result)

    def run(self, tables: List[str] = None) -> None:
        """
        Запуск полной миграции

        Args:
            tables: Список таблиц для миграции (None - все таблицы)
        """
        try:
            self.connect()

            if self.migration_config.get("transfer_structure", True):
                self.migrate_structure(tables)

            if self.migration_config.get("transfer_data", True):
                self.migrate_data(tables)

            print("\n=== Миграция завершена успешно ===")

        except Exception as e:
            print(f"\nОшибка миграции: {e}", file=sys.stderr)
            raise
        finally:
            self.disconnect()


def run_migration(
    config_path: str = None,
    tables: List[str] = None,
    lowercase: bool = False,
    drop_tables: bool = False,
    structure_only: bool = False,
    data_only: bool = False
):
    """
    Запуск миграции

    Args:
        config_path: Путь к файлу конфигурации
        tables: Список таблиц для миграции
        lowercase: Создавать таблицы и поля в нижнем регистре
        drop_tables: Удалять существующие таблицы перед миграцией
        structure_only: Только структура
        data_only: Только данные
    """
    config = Config(config_path)
    
    # Обновляем конфигурацию для этой миграции
    migration_config = config.get_migration_config()
    migration_config["transfer_structure"] = not data_only
    migration_config["transfer_data"] = not structure_only
    migration_config["drop_tables"] = drop_tables
    
    migrator = Migrator(config, lowercase=lowercase, drop_tables=drop_tables)
    migrator.run(tables)
