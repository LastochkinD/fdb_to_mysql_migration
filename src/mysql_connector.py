"""
Модуль подключения к MySQL
"""

import mysql.connector
from mysql.connector import Error as MySQLError
from typing import Any, Dict, List, Optional


class MySQLConnector:
    """Класс для работы с базой данных MySQL"""

    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация подключения к MySQL

        Args:
            config: Словарь с параметрами подключения
        """
        self.config = config
        self.connection: Optional[mysql.connector.MySQLConnection] = None

    def connect(self) -> mysql.connector.MySQLConnection:
        """Установка соединения с базой данных"""
        self.connection = mysql.connector.connect(
            host=self.config["host"],
            port=self.config["port"],
            database=self.config.get("database"),
            user=self.config["user"],
            password=self.config["password"],
            charset=self.config.get("charset", "utf8mb4"),
        )
        return self.connection

    def disconnect(self) -> None:
        """Закрытие соединения"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.connection = None

    def execute(self, query: str, params: tuple = None) -> None:
        """Выполнение SQL запроса"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        cursor.close()

    def executemany(self, query: str, params_list: List[tuple]) -> None:
        """Выполнение множественных SQL запросов"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.executemany(query, params_list)
        cursor.close()

    def fetchall(self, query: str, params: tuple = None) -> List[tuple]:
        """Получение всех результатов запроса"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        
        return result

    def create_database(self, database_name: str) -> None:
        """Создание базы данных"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.close()

    def use_database(self, database_name: str) -> None:
        """Выбор базы данных"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(f"USE `{database_name}`")
        cursor.close()

    def drop_table(self, table_name: str) -> None:
        """Удаление таблицы"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        cursor.close()

    def create_table(self, table_name: str, columns: List[Dict[str, Any]], primary_key: List[str] = None) -> None:
        """
        Создание таблицы

        Args:
            table_name: Имя таблицы
            columns: Список описаний колонок
            primary_key: Список колонок первичного ключа
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        col_defs = []
        for col in columns:
            col_defs.append(f"`{col['name']}` {col['mysql_type']}")
        
        if primary_key:
            pk_cols = ", ".join(f"`{col}`" for col in primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")
        
        sql = f"CREATE TABLE `{table_name}` (\n    " + ",\n    ".join(col_defs) + "\n)"
        
        cursor = self.connection.cursor()
        cursor.execute(sql)
        cursor.close()

    def insert_data(self, table_name: str, columns: List[str], values: List[tuple]) -> None:
        """
        Вставка данных в таблицу

        Args:
            table_name: Имя таблицы
            columns: Список колонок
            values: Список кортежей значений
        """
        col_names = ", ".join(f"`{col}`" for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"
        self.executemany(query, values)

    def commit(self) -> None:
        """Фиксация транзакции"""
        if self.connection and self.connection.is_connected():
            self.connection.commit()

    def rollback(self) -> None:
        """Откат транзакции"""
        if self.connection and self.connection.is_connected():
            self.connection.rollback()

    def table_exists(self, table_name: str) -> bool:
        """Проверка существования таблицы"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        cursor.close()
        
        return result is not None

    def get_table_count(self, table_name: str) -> int:
        """Получение количества записей в таблице"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        
        return count

    def get_all_tables(self) -> List[str]:
        """Получение списка всех таблиц в текущей базе данных"""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        cursor.close()
        
        return tables

    def drop_all_tables(self) -> int:
        """
        Удаление всех таблиц в текущей базе данных

        Returns:
            Количество удалённых таблиц
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        # Получаем все таблицы
        tables = self.get_all_tables()
        
        if not tables:
            return 0
        
        # Удаляем все таблицы (с FOREIGN KEY CHECKS = 0 для избежания ошибок)
        cursor = self.connection.cursor()
        
        # Отключаем проверку внешних ключей
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
        
        # Включаем проверку обратно
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        cursor.close()
        
        return len(tables)
