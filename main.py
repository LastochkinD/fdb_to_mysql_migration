#!/usr/bin/env python3
"""
Основной скрипт миграции Firebird -> MySQL

Примечание: Для корректной работы в Linux/Mac необходимо установить переменную окружения:
  export LANG=en_US.UTF-8
"""

import argparse
import os
import sys

# Проверка кодировки
if sys.platform != 'win32':
    os.environ['LANG'] = 'en_US.UTF-8'

from src.migrator import run_migration


def main():
    """Точка входа в приложение"""
    parser = argparse.ArgumentParser(
        description="Миграция базы данных Firebird в MySQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python main.py                                    # Миграция всех таблиц
  python main.py --tables users,orders              # Миграция конкретных таблиц
  python main.py --config myconfig.yaml             # Использовать свой файл конфигурации
  python main.py --lowercase                        # Имена таблиц и полей в нижнем регистре
  python main.py --drop-tables                      # Удалить существующие таблицы перед миграцией
        """
    )

    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="Путь к файлу конфигурации (по умолчанию: config.yaml)"
    )

    parser.add_argument(
        "--tables", "-t",
        default=None,
        help="Список таблиц для миграции через запятую (по умолчанию: все таблицы)"
    )

    parser.add_argument(
        "--structure-only",
        action="store_true",
        help="Мигрировать только структуру (без данных)"
    )

    parser.add_argument(
        "--data-only",
        action="store_true",
        help="Мигрировать только данные (без структуры)"
    )

    parser.add_argument(
        "--lowercase",
        action="store_true",
        help="Создавать таблицы и поля в нижнем регистре"
    )

    parser.add_argument(
        "--drop-tables",
        action="store_true",
        help="Удалить существующие таблицы перед миграцией"
    )

    args = parser.parse_args()

    # Разбор списка таблиц
    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",")]

    try:
        run_migration(
            config_path=args.config,
            tables=tables,
            lowercase=args.lowercase,
            drop_tables=args.drop_tables,
            structure_only=args.structure_only,
            data_only=args.data_only
        )
    except KeyboardInterrupt:
        print("\nМиграция отменена пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\nОшибка: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
