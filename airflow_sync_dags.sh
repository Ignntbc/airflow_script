#!/bin/bash
nice -n 20 /app/airflow-venv/bin/python /app/airflow_deploy/airflow_sync_dags.py "$@"
