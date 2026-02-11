#!/bin/bash
nice -n 20 python3 /home/smith/airflow_script/airflow_sync_dags.py "$@"

#/app/airflow-venv/bin/python
# if [[ $# -eq 0 ]]
# then
# nice -n 20 /app/airflow-venv/bin/python /app/airflow_deploy/airflow_sync_dags.py
# exit $?
# fi

# if [[ $1 != "-c" ]] && [[ $1 != "-h" ]] && [[ $1 != "--delete" ]] && [[ $1 != "--file" ]] && [[ $1 != "--dir" ]]
# then
# echo "******************************************* Run script *******************************************\n" >> /app/airflow_deploy/log/deploy.log
# echo "Ошибка ! Неизвестный ключ $1" >> /app/airflow_deploy/log/deploy.log
# echo "******************************************* End work script *******************************************\n\n" >> /app/airflow_deploy/log/deploy.log
# echo "1"
# exit 1
# fi

# if [[ $1 == "-c" ]]
# then
# nice -n 20 /app/airflow-venv/bin/python /app/airflow_deploy/airflow_sync_dags.py -c
# exit $?
# fi

# if [[ $1 == "-h" ]]
# then
# /app/airflow-venv/bin/python /app/airflow_deploy/airflow_sync_dags.py -h
# exit $?
# fi

# if [[ $1 == "--file" ]]
# then
# for i in "${@:2}"; do
# nice -n 20 /app/airflow-venv/bin/python /app/airflow_deploy/airflow_sync_dags.py --file $i
# done
# exit $?
# fi

# if [[ $1 == "--dir" ]]
# then
# for i in "${@:2}"; do
# nice -n 20 /app/airflow-venv/bin/python /app/airflow_deploy/airflow_sync_dags.py --dir $i
# done
# exit $?
# fi

# if [[ $1 == "--delete" ]]
# then
# for i in "${@:2}"; do
# nice -n 20 /app/airflow-venv/bin/python /app/airflow_deploy/airflow_sync_dags.py --delete $i
# done
# exit $?
# fi

