airflow_sync_dags.sh (airflow_sync_dags_v2.sh)
----------------------------------------------

Скрипт позволяет синхронизировать содержимое директорий /app/airflow_deploy/dags, /app/airflow_deploy/keytab, /app/airflow_deploy/scripts, /app/airflow_deploy/keys, /app/airflow_deploy/csv, /app/airflow_deploy/jar, /app/airflow_deploy/user_data с соответствующими директориями в /app/airflow:
/app/airflow_deploy/dags -> /app/airflow/dags
/app/airflow_deploy/keytab -> /app/airflow/keytab
/app/airflow_deploy/scripts/ -> /app/airflow/scripts
/app/airflow_deploy/keys -> /app/airflow/keys
/app/airflow_deploy/csv -> /app/airflow/csv
/app/airflow_deploy/jar -> /app/airflow/jar
/app/airflow_deploy/user_data -> /app/airflow/user_data

Запуск скрипта: sudo -u airflow_deploy /app/airflow_deploy/airflow_sync_dags.sh

Usage: airflow_sync_dags.sh [-c] [-h]     
-c Очистить директории назначения (/app/airflow/dags, /app/airflow/keytab, /app/airflow/scripts, /app/airflow/keys, /app/airflow/csv, /app/airflow/jar, /app/airflow/user_data) перед синхронизацией
-h Help

Запуск скрипта версии 2: sudo -u airflow_deploy /app/airflow_deploy/airflow_sync_dags_v2.sh

Usage: airflow_sync_dags_v2.sh [-c] [-h]     
-c Очистить директории назначения (/app/airflow/dags, /app/airflow/keytab, /app/airflow/scripts, /app/airflow/keys, /app/airflow/csv, /app/airflow/jar, /app/airflow/user_data) перед синхронизацией
-h Help

В процессе работы ведется лог аудита (для просмотра: cat /app/airflow_deploy/log/deploy.log)

airflow_delete_dags.sh
----------------------

Позволяет удалить указанный в параметре файл из директории /app/airflow/dags, либо /app/airflow/keytab, либо /app/airflow/scripts, либо /app/airflow/keys, либо /app/airflow/csv, /app/airflow/jar, /app/airflow/user_data.
В качестве параметра указывается путь относительно /app/airflow, например, dags/example.py

Запуск скрипта: sudo -u airflow_deploy /app/airflow_deploy/airflow_delete_dags.sh dags/example.py

Usage: airflow_delete_dags.sh [-h] [-f] path 
-h Help
-f Не запрашивать подтверждение удаления

В процессе работы ведется лог аудита, который можно просмотреть (cat /app/airflow_deploy/log/delete.log)