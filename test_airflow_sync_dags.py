
AIRFLOW_DEPLOY_PATH = "app/airflow_deploy/"

ext_map = {
    f"{AIRFLOW_DEPLOY_PATH}dags/sql": ".sql",
    f"{AIRFLOW_DEPLOY_PATH}dags": ".py",
    f"{AIRFLOW_DEPLOY_PATH}keytab": ".keytab",
    f"{AIRFLOW_DEPLOY_PATH}scripts": ".sh .json",
    f"{AIRFLOW_DEPLOY_PATH}keys": ".pfx .p12 .jks .secret",
    f"{AIRFLOW_DEPLOY_PATH}csv": ".csv",
    f"{AIRFLOW_DEPLOY_PATH}jar": ".jar",
}


def is_dir_allowed(path: str) -> bool:
    """
    Проверяет, разрешён ли путь согласно ext_map.
    Путь разрешён, если он начинается с одного из ключей ext_map.
    """
    for allowed_prefix in ext_map:
        print(allowed_prefix)
        if path.startswith(allowed_prefix):# and len(path) > len(allowed_prefix) and path[len(allowed_prefix)] in ('/', '\\')
            return True
    return False


a = is_dir_allowed("app/airflow_deploy/my_dag.sql")
print(a)