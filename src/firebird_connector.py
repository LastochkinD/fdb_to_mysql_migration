"""
Модуль подключения к Firebird
"""

import fdb
from typing import Any, Dict, List, Optional, Tuple


class FirebirdConnector:
    """Класс для работы с базой данных Firebird"""

    def __init__(self, config: Dict[str, Any]):
        """
        Инициализация подключения к Firebird

        Args:
            config: Словарь с параметрами подключения
        """
        self.config = config
        self.connection: Optional[fdb.Connection] = None

    def connect(self) -> fdb.Connection:
        """Установка соединения с базой данных"""
        self.connection = fdb.connect(
            host=self.config["host"],
            port=self.config["port"],
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
            charset=self.config.get("charset", "UTF8"),
        )
        return self.connection

    def disconnect(self) -> None:
        """Закрытие соединения"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def get_tables(self) -> List[Tuple[str, ...]]:
        """
        Получение списка таблиц в базе данных

        Returns:
            Список кортежей (table_name,)
        """
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT RDB$RELATION_NAME
            FROM RDB$RELATIONS
            WHERE RDB$SYSTEM_FLAG = 0
            AND RDB$VIEW_BLR IS NULL
            ORDER BY RDB$RELATION_NAME
        """)
        tables = cursor.fetchall()
        cursor.close()
        # Обрезаем пробелы из имен таблиц
        return [(t[0].strip(),) for t in tables]

    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Получение информации о колонках таблицы

        Args:
            table_name: Имя таблицы

        Returns:
            Список словарей с информацией о колонках
        """
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        # Используем TRIM для корректного сравнения строк
        cursor.execute(f"""
            SELECT
                r.RDB$FIELD_NAME as column_name,
                f.RDB$FIELD_TYPE as field_type,
                f.RDB$FIELD_LENGTH as field_length,
                f.RDB$FIELD_PRECISION as field_precision,
                f.RDB$FIELD_SCALE as field_scale,
                r.RDB$NULL_FLAG as null_flag
            FROM RDB$RELATION_FIELDS r
            JOIN RDB$FIELDS f ON r.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
            WHERE TRIM(r.RDB$RELATION_NAME) = TRIM('{table_name}')
            ORDER BY r.RDB$FIELD_POSITION
        """)
        
        columns = cursor.fetchall()
        cursor.close()
        
        result = []
        for col in columns:
            result.append({
                "name": col[0].strip() if col[0] else "",
                "type": col[1] if len(col) > 1 else 0,
                "length": col[2] if len(col) > 2 else 0,
                "precision": col[3] if len(col) > 3 else None,
                "scale": col[4] if len(col) > 4 else 0,
                "nullable": col[5] != 1 if len(col) > 5 else True,
            })
        
        return result

    def get_table_data(self, table_name: str, batch_size: int = 1000) -> List[Tuple]:
        """
        Получение данных из таблицы

        Args:
            table_name: Имя таблицы
            batch_size: Размер пакета

        Returns:
            Список записей
        """
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        cursor.execute(f'SELECT * FROM "{table_name}"')
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield row
        
        cursor.close()

    def get_primary_key(self, table_name: str) -> List[str]:
        """
        Получение первичного ключа таблицы

        Args:
            table_name: Имя таблицы

        Returns:
            Список имен колонок первичного ключа
        """
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        cursor.execute(f"""
            SELECT
                s.RDB$FIELD_NAME as field_name
            FROM RDB$INDEX_SEGMENTS s
            JOIN RDB$INDICES i ON s.RDB$INDEX_NAME = i.RDB$INDEX_NAME
            JOIN RDB$RELATION_CONSTRAINTS c ON i.RDB$INDEX_NAME = c.RDB$INDEX_NAME
            WHERE TRIM(c.RDB$RELATION_NAME) = TRIM('{table_name}')
            AND c.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY s.RDB$FIELD_POSITION
        """)
        
        pk_columns = [row[0].strip() for row in cursor.fetchall()]
        cursor.close()
        
        return pk_columns

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Получение внешних ключей таблицы

        Args:
            table_name: Имя таблицы

        Returns:
            Список словарей с информацией о внешних ключах
        """
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        cursor.execute(f"""
            SELECT
                rc.RDB$CONSTRAINT_NAME as constraint_name,
                rc.RDB$RELATION_NAME as table_name,
                s.RDB$FIELD_NAME as field_name,
                refc.RDB$RELATION_NAME as ref_table,
                ref_s.RDB$FIELD_NAME as ref_field
            FROM RDB$RELATION_CONSTRAINTS rc
            JOIN RDB$INDEX_SEGMENTS s ON rc.RDB$INDEX_NAME = s.RDB$INDEX_NAME
            JOIN RDB$RELATION_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
            JOIN RDB$INDEX_SEGMENTS ref_s ON refc.RDB$INDEX_NAME = ref_s.RDB$INDEX_NAME
            WHERE TRIM(rc.RDB$RELATION_NAME) = TRIM('{table_name}')
            AND rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
        """)
        
        fks = cursor.fetchall()
        cursor.close()
        
        return [{"constraint_name": fk[0], "field": fk[2], "ref_table": fk[3], "ref_field": fk[4]} for fk in fks]

    def get_table_count(self, table_name: str) -> int:
        """
        Получение количества записей в таблице

        Args:
            table_name: Имя таблицы

        Returns:
            Количество записей
        """
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        count = cursor.fetchone()[0]
        cursor.close()
        
        return count
