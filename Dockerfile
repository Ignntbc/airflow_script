FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y openssh-client rsync && rm -rf /var/lib/apt/lists/*
CMD ["python", "airflow_sync_dags.py", "-h"]