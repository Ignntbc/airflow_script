airflow_sync_dags
----------------------------------------------

Скрипт позволяет синхронизировать содержимое директории /app/airflow_deploy с соответствующими директориями в /app/airflow:
/app/airflow_deploy/dags -> /app/airflow/dags
/app/airflow_deploy/keytab -> /app/airflow/keytab
/app/airflow_deploy/scripts/ -> /app/airflow/scripts
/app/airflow_deploy/keys -> /app/airflow/keys
/app/airflow_deploy/csv -> /app/airflow/csv
/app/airflow_deploy/jar -> /app/airflow/jar
/app/airflow_deploy/user_data -> /app/airflow/user_data

Запуск скрипта через оболочку:
sudo -u airflow_deploy /app/airflow_deploy/airflow_sync_dags.sh

Запуск напрямую (без оболочки):
sudo -u airflow_deploy python3 /app/airflow_deploy/airflow_sync_dags.py [опции]

Usage: airflow_sync_dags.sh [-c] [-h] [-v] [--dry-run] [--delete] [--file] [--dir]

Описание ключей:
--delete   Удалить указанный файл или директорию из целевой папки (например, из /app/airflow/dags и других поддерживаемых директорий).
--file     Операция применяется к отдельному файлу.
--dir      Операция применяется к директории.
-c         Очистить директории назначения перед синхронизацией.
-h         Вывести справку по использованию скрипта.
-v         Включить подробный (verbose) режим вывода.
--dry-run  Выполнить пробный запуск без фактической синхронизации файлов.

Примеры:
sudo -u airflow_deploy /app/airflow_deploy/airflow_sync_dags.sh --dry-run
sudo -u airflow_deploy /app/airflow_deploy/airflow_sync_dags.sh -v --file dags/example
sudo -u airflow_deploy python3 /app/airflow_deploy/airflow_sync_dags.py --dry-run -v --delete scripts/test.json

Пример запуска с множественными целевыми путями:
python3 /app/airflow_deploy/airflow_sync_dags.py --file dags/test dags/test_2

В процессе работы ведется лог аудита (для просмотра: cat /app/airflow_deploy/log/deploy.log)

Матрица конфликтов ключей
+ конфликт
- допустимо


          |   h   |   c   |  dir  | file  | delete |  v   | dry-run
-----------------------------------------------------------------------
h         |   x   |   +   |   +   |   +   |   +    |  +   |   +
c         |   +   |   x   |   +   |   +   |   +    |  -   |   -
dir       |   +   |   +   |   x   |   +   |   +    |  -   |   -
file      |   +   |   +   |   +   |   x   |   +    |  -   |   -
delete    |   +   |   +   |   +   |   +   |   x    |  -   |   -
v         |   +   |   -   |   -   |   -   |   -    |  x   |   -
dry-run   |   +   |   -   |   -   |   -   |   -    |  -   |   x