@echo off
REM Скрипт создания виртуальной среды и установки зависимостей

echo Создание виртуальной среды...
python -m venv venv

echo Активация виртуальной среды и установка зависимостей...
call venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

echo.
echo Готово! Для активации используйте: venv\Scripts\activate
pause
