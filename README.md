# Firebird to MySQL Migration Tool

Инструмент для миграции базы данных из Firebird в MySQL.

## Установка

1. Клонируйте репозиторий или скопируйте файлы проекта

2. Создайте виртуальную среду:
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# или
source venv/bin/activate  # Linux/Mac
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

## Настройка

1. Скопируйте файл `.env.example` в `.env`:
```bash
cp .env.example .env
```

2. Отредактируйте файл `.env` или `config.yaml` с вашими параметрами подключения:

**Вариант 1 - через .env:**
```env
FIREBIRD_HOST=localhost
FIREBIRD_PORT=3050
FIREBIRD_DATABASE=C:/path/to/source.fdb
FIREBIRD_USER=SYSDBA
FIREBIRD_PASSWORD=masterkey

MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=target_database
MYSQL_USER=root
MYSQL_PASSWORD=your_password
```

**Вариант 2 - через config.yaml:**
```yaml
firebird:
  host: "localhost"
  port: 3050
  database: "C:/path/to/source.fdb"
  user: "SYSDBA"
  password: "masterkey"

mysql:
  host: "localhost"
  port: 3306
  database: "target_database"
  user: "root"
  password: "your_password"
```

## Использование

### Полная миграция:
```bash
python main.py
```

### Миграция конкретных таблиц:
```bash
python main.py --tables users,orders,products
```

### Только структура (без данных):
```bash
python main.py --structure-only
```

### Только данные (без создания структуры):
```bash
python main.py --data-only
```

### Имена таблиц и полей в нижнем регистре:
```bash
python main.py --lowercase
```

### Удалить существующие таблицы перед миграцией:
```bash
python main.py --drop-tables
```

### Комбинирование опций:
```bash
python main.py --lowercase --drop-tables --tables cars,clients
```

### Использование своего файла конфигурации:
```bash
python main.py --config custom_config.yaml
```

## Структура проекта

```
fdb_to_mysql_migration/
├── main.py              # Основной скрипт запуска
├── config.yaml          # Файл конфигурации
├── .env.example         # Пример переменных окружения
├── requirements.txt     # Зависимости Python
├── README.md            # Документация
├── install.bat          # Скрипт установки для Windows
├── venv/               # Виртуальная среда (создаётся при установке)
└── src/
    ├── __init__.py      # Инициализация пакета
    ├── config.py        # Модуль конфигурации
    ├── firebird_connector.py  # Подключение к Firebird
    ├── mysql_connector.py     # Подключение к MySQL
    └── migrator.py     # Основной модуль миграции
```

## Требования

- Python 3.8+
- Firebird 2.0+
- MySQL 5.7+ / MariaDB 10.0+

## Зависимости

- `fdb` - драйвер Firebird для Python
- `mysql-connector-python` - драйвер MySQL для Python
- `python-dotenv` - загрузка переменных окружения
- `PyYAML` - парсинг YAML конфигурации

## Лицензия

MIT License
