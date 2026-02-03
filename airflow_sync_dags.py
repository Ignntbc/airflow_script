import shutil
import os
import hashlib
import json
import sys
import subprocess
import socket
from multiprocessing import Process, Queue
from datetime import datetime
from typing import List



CRITICAL_PERCENT = 80
CMD = ["id"]

ARGV_KEYS = ["-c", "-h", "--file", "--dir", "--skipped"]

GLOBAL_LIST_ERROR = set()

result_queue = Queue()
ALL_ERROR = Queue()

END_WORK_SCRIPT_STRING = "\n****************************************** END WORK SCRIPT *******************************************\n\n"
END_ALL_ERRORS_STRING = "\n******************************************* END ALL ERORRS *******************************************\n\n"
RSYNC_CHECKSUM_STRING = 'rsync --checksum -rogp --rsync-path="mkdir -p'
RSYNC_CHECKSUM_DR_STRING = 'rsync --checksum -nrogp --rsync-path="mkdir -p'
RSYNC_DRY_RUN = 'rsync --checksum -nrogp'
CHOWN_STRING = "--chown=airflow_deploy:airflow"
CHMOD_FG_FU_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx"
AIRFLOW_PATH = "/app/airflow/"
AIRFLOW_DEPLOY_PATH = "/app/airflow_deploy/"
LOCAL_DEPLOY = "airflow_deploy@127.0.0.1"
SSH_USER = "ssh airflow_deploy"
CHMOD_WITHOUT_FU_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo="
CHMOD_WITHOUT_DO_FU_DG_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo="


ext_map = {
    f"{AIRFLOW_DEPLOY_PATH}dags/sql": ".sql",
    f"{AIRFLOW_DEPLOY_PATH}dags": ".py",
    f"{AIRFLOW_DEPLOY_PATH}keytab": ".keytab",
    f"{AIRFLOW_DEPLOY_PATH}scripts": ".sh .json",
    f"{AIRFLOW_DEPLOY_PATH}keys": ".pfx .p12 .jks .secret",
    f"{AIRFLOW_DEPLOY_PATH}csv": ".csv",
    f"{AIRFLOW_DEPLOY_PATH}jar": ".jar",
}

size_airflow_deploy = int(os.popen("du -s app/airflow_deploy | cut -f1").read())
list_folders = ["dags"]#TODO вернуть все папки ,"csv", "jar", "keys", "keytab", "scripts", "user_data"

LOCAL_TEST = True
if LOCAL_TEST:
    description_path = "description.json"
else:
    description_path = "/app/app/etc/description.json"

with open(description_path, "r", encoding="utf-8") as file_description:
    data_description = json.load(file_description)

schedulers = data_description["software"]["app"]["nodes"]["airflow_scheduler"]
webs = data_description["software"]["app"]["nodes"]["airflow_web"]
workers = data_description["software"]["app"]["nodes"]["airflow_workers"]
all_hosts = schedulers + webs + workers

EXECUTOR_TYPE = data_description["software"]["app"]["executor"]

def check_configuratioon(executor_type: str) -> str:
    """
    Определяет тип конфигурации Airflow по типу executor.

    Параметры:
        executor_type (str): Тип executor из конфигурационного файла (например, 'localexecutor' или 'celeryexecutor').

    Возвращает:
        str: Строка с типом конфигурации ('one-way' для localexecutor, 'cluster' для остальных типов).
    """
    if executor_type == "localexecutor":
        config = "one-way"
    else:
        config = "cluster"
    return config

CONFIGURATION= check_configuratioon(EXECUTOR_TYPE)

def save_log(message: str, with_exit=False) -> None:
    """
    Сохраняет сообщение в лог-файл и при необходимости завершает выполнение скрипта.

    Параметры:
        message (str): Сообщение для записи в лог.
        with_exit (bool): Если True, завершает выполнение скрипта с кодом 1 после записи лога.
    """
    with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as log_file:
        log_file.write(message)
    if with_exit:
        print("1")
        sys.exit(1)


def check_real_user() -> str:
    """
    Определяет имя пользователя, под которым запущен скрипт (с учётом sudo).

    Возвращает:
        str: Имя пользователя, под которым выполняется скрипт, либо None при ошибке.
    """
    request_name = subprocess.Popen(
        "${SUDO_USER:-${USER}}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout_output = request_name.stdout.read().decode("utf-8") if request_name.stdout else ""
    stderr_output = request_name.stderr.read().decode("utf-8") if request_name.stderr else ""


    if stdout_output:
        # Например, строка: 'uid=0(root) gid=0(root) groups=0(root)\n'
        try:
            real_server_name = stdout_output.split(" ")[0].split("(")[1].split(")")[0]
        except (ValueError, IndexError) as e:
            print("[DEBUG] Ошибка разбора stdout:", e)
            real_server_name = None
    else:
        try:
            real_server_name = stderr_output.split(" ")[1].replace(":", "").strip()
        except (ValueError, IndexError) as e:
            print("[DEBUG] Ошибка разбора stderr:", e)
            real_server_name = None

    return real_server_name

real_name = check_real_user()

current_hostname = socket.gethostname()


def remove_destination_folder(host_name: str,
                              result_q: Queue) -> None:
    """
    Удаляет содержимое целевых папок на удалённом сервере airflow_deploy через ssh.
    
    Параметры:
        host_name (str): Имя или адрес удалённого хоста.
        result_q (Queue): Очередь для передачи результатов выполнения команд.
    
    Для папки dags пропускает каталоги __pycache__, для остальных удаляет все элементы.
    """
    for elem in list_folders:
        items = [x for x in os.popen(f"{SSH_USER}@{host_name} ls -a {AIRFLOW_PATH}{elem}/").read().split("\n") if x not in {".", "..", ""}]
        if elem == "dags":
            for item in items:
                if "__pycache__" in item:
                    continue
                result = os.popen(f"{SSH_USER}@{host_name} rm -rfv {AIRFLOW_PATH}dags/{item}").read()
                result_q.put(result)
            result_sql = os.popen(f"{SSH_USER}@{host_name} rm -rfv {AIRFLOW_PATH}dags/sql/*").read()
            result_q.put(result_sql)
        else:
            for item in items:
                result = os.popen(f"{SSH_USER}@{host_name} rm -rf {AIRFLOW_PATH}{elem}/{item}").read()
                result_q.put(result)


def param_run_script() -> None:
    """
    Записывает информацию о запуске скрипта в лог-файл  {AIRFLOW_DEPLOY_PATH}log/deploy.log.

    В лог добавляются:
        - отметка о запуске,
        - дата и время запуска,
        - имя пользователя,
        - параметр запуска (если передан -c, то отмечается, иначе пишется 'false key').
    """
    with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as run_script:
        run_script.write(
            "******************************************* Run script *******************************************\n"
        )
        current_datetime = datetime.now()
        run_script.write(f"Start run script: {current_datetime}\n")
        run_script.write(f"User: {real_name}\n")
        if len(sys.argv) == 2 and sys.argv[1] == "-c":
            run_script.write("Run param: -c\n\n")
        else:
            run_script.write("Run param: false key\n\n")

def run_command_with_log(
    command: str,
    log_message: str,
    with_exit: bool = False,
    rsync_error: bool = False
) -> str:
    """
    Выполняет команду через os.popen, записывает сообщение в лог.
    Если rsync_error=True, то при наличии 'rsync error' в результате записывает ошибку в лог и завершает выполнение (если with_exit=True).
    Иначе просто пишет log_message в лог.

    :param command: Команда для выполнения.
    :param log_message: Сообщение для записи в лог.
    :param with_exit: Завершать ли выполнение скрипта при ошибке (по умолчанию False).
    :param rsync_error: Проверять ли результат на 'rsync error' (по умолчанию False).
    :return: Результат выполнения команды.
    """
    result = os.popen(command).read()
    if rsync_error and "rsync error" in result:
        save_log(f"{log_message}{result}\n\n", with_exit=with_exit)
    else:
        save_log(log_message, with_exit=with_exit)
    return result

def check_permission(find_cmd: str,
                    error_prefix: str,
                    host: str,
                    all_error: Queue) -> None:
    """
    Выполняет проверку прав доступа к файлам/директориям на целевом хосте с помощью команды find.
    Для каждого найденного объекта с некорректными правами вызывает ls -l и добавляет ошибку в очередь all_error.

    Параметры:
        find_cmd (str): Команда find для поиска файлов/директорий с некорректными правами.
        error_prefix (str): Префикс сообщения об ошибке (например, "Ошибка !!! Некорректная группа на хосте").
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    result = os.popen(find_cmd).read().split("\n")
    for item in result:
        if item.strip():
            perm_error = os.popen(f"{SSH_USER}@{host} ls -l {item}").read()
            all_error.put(f"{error_prefix} {host} {perm_error}\n\n")

def remove_path(path: str,
            folders_set: set) -> set:
    """
    Удаляет файл или директорию по указанному пути и добавляет информацию об удалении в множество.

    Параметры:
        path (str): Путь к файлу или директории для удаления.
        folders_set (set): Множество, в которое добавляется строка с информацией об удалении.

    Возвращает:
        set: Обновлённое множество с информацией об удалённых файлах/директориях.
    """
    if os.path.isfile(path):
        os.remove(path)
        folders_set.add(f"removed  {path}")
    elif os.path.isdir(path):
        shutil.rmtree(path)
        folders_set.add(f"removed  {path}")
    return folders_set


def check_param_delete_key(
    script_args: List[str],
    current_datetime: datetime
) -> None:
    """
    Обрабатывает удаление файлов/директорий по ключу --delete.
    Проверяет существование, удаляет локально или по ssh, логирует действия и завершает выполнение.

    Аргументы:
        script_args (List[str]): Список путей к файлам/директориям для удаления (относительно AIRFLOW_PATH).
        current_datetime (datetime): Текущая дата и время для логирования.

    Использует глобальные переменные:
        real_name, CONFIGURATION, all_hosts, AIRFLOW_PATH, SSH_USER, save_log
    """
    missing = [f" {AIRFLOW_PATH}{x}" for x in script_args if not os.path.exists(f" {AIRFLOW_PATH}{x}")]
    if missing:
        save_log(f"{current_datetime} {real_name} Нет такого файла или директории {', '.join(missing)}\n\n", with_exit=True)

    for i_script_args in script_args:
        if CONFIGURATION == "one-way":
            path = f"{AIRFLOW_PATH}{i_script_args}"
            if os.path.isfile(path):
                os.remove(path)
                save_log(f"{current_datetime} {real_name} Delete file: {path}\n\n")
            elif os.path.isdir(path):
                shutil.rmtree(path)
                save_log(f"{current_datetime} {real_name} Delete directory: {path}\n\n")

        else:
            path = f"{AIRFLOW_PATH}{i_script_args}"
            for host in all_hosts:
                os.popen(
                    f"{SSH_USER}@{host} rm -rf {path}"
                ).read()
                save_log(f"{current_datetime} {real_name} {host}  Delete file: {path}\n\n")
    print("0")
    sys.exit(0)


def check_param_file_key(
    script_args: list[str],
    current_datetime: datetime
    ) -> None:
    """
    Универсальная функция деплоя файла/директории на все хосты для one-way и cluster.

    Аргументы:
        script_args (list[str]): Список путей к файлам/директориям для деплоя (относительно AIRFLOW_DEPLOY_PATH).
        current_datetime (datetime): Текущая дата и время для логирования.

    Использует глобальные переменные:
        real_name, CONFIGURATION, all_hosts, LOCAL_DEPLOY, AIRFLOW_PATH, AIRFLOW_DEPLOY_PATH, run_command_with_log,
        CHOWN_STRING, CHMOD_FG_FU_FO_STRING, CHMOD_WITHOUT_FU_FO_STRING, CHMOD_WITHOUT_DO_FU_DG_FO_STRING,
        RSYNC_CHECKSUM_DR_STRING, RSYNC_CHECKSUM_STRING, RSYNC_DRY_RUN, save_log
    """
    for i_script_args in script_args:
        airflow_deploy_dir_path = f"{AIRFLOW_DEPLOY_PATH}{i_script_args}"
        temp_folder_path = i_script_args.rpartition("/")[0]

        if not os.path.exists(airflow_deploy_dir_path):
            save_log(f"{current_datetime} {real_name} Файл не найден {airflow_deploy_dir_path} !\n\n", with_exit=True)

        if i_script_args.startswith("keytab") or i_script_args.startswith("keys"):
            if i_script_args.count("/") > 1:
                CHMOD_STRING = CHMOD_WITHOUT_DO_FU_DG_FO_STRING
            else:
                CHMOD_STRING = CHMOD_WITHOUT_FU_FO_STRING
        else:
            CHMOD_STRING = CHMOD_FG_FU_FO_STRING

        if CONFIGURATION == "one-way":
            hosts = [LOCAL_DEPLOY]
            host_prefix = ""
        else:
            hosts = all_hosts + ['127.0.0.1']
            host_prefix = f"airflow_deploy@{host}:"

        for host in hosts:
            if i_script_args.count("/") > 1:
                run_command_with_log(
                    f'{RSYNC_CHECKSUM_DR_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{i_script_args}',
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлен файл:  {AIRFLOW_PATH}{i_script_args}\n\n",
                    with_exit=True,
                    rsync_error=True
                )
                run_command_with_log(
                    f'{RSYNC_CHECKSUM_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{i_script_args}',
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлен файл:  {AIRFLOW_PATH}{i_script_args}\n\n",
                )
            else:
                run_command_with_log(
                    f"{RSYNC_DRY_RUN} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{i_script_args}",
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлен файл:  {AIRFLOW_PATH}{i_script_args}\n\n",
                    with_exit=True,
                    rsync_error=True
                )
                run_command_with_log(
                    f"{RSYNC_CHECKSUM_STRING} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{i_script_args}",
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлен файл:  {AIRFLOW_PATH}{i_script_args}\n\n",
                )
    print("0")
    sys.exit(0)

def check_param_c_key(remove_files_folders: set,
                    all_error:Queue) -> set:
    """
    Очищает содержимое всех целевых директорий Airflow (для one-way).
    Удаляет все файлы и поддиректории, кроме __pycache__ и dags/sql (последняя обрабатывается отдельно).

    Параметры:
        remove_files_folders (set): Множество для сбора информации об удалённых файлах/директориях.

    Возвращает:
        set: Обновлённое множество с информацией об удалённых файлах/директориях.
    """
    check_files_in_dirs(all_error)
    if CONFIGURATION == "one-way":
        for folder in list_folders:
            folder_path = f"{AIRFLOW_PATH}{folder}/"
            for entry in os.listdir(folder_path):
                full_path = f"{folder_path}{entry}"
                if "__pycache__" in full_path or full_path == f"{AIRFLOW_PATH}dags/sql":
                    continue
                remove_files_folders = remove_path(full_path,
                                                remove_files_folders)
                
            if folder == "dags":
                sql_dir = f"{AIRFLOW_PATH}dags/sql/"
                for sql_entry in os.listdir(sql_dir):
                    sql_path = f"{sql_dir}{sql_entry}"
                    remove_files_folders = remove_path(sql_path, remove_files_folders)
                    
    return remove_files_folders


def check_param_f_key() -> None:
    """
    Блок справки по ключу -h.
    """
    print(
        "\033[32m{}\033[0m".format(
            "\n             Доступны следующие расширения:\n"
        )
    )
    print("       Директория                     Расширение\n")
    print(f"{AIRFLOW_DEPLOY_PATH}dags            .py .sql .json\n")
    print(f"{AIRFLOW_DEPLOY_PATH}keytab          .keytab\n")
    print(f"{AIRFLOW_DEPLOY_PATH}keys            .pfx .p12 .jks .secret\n")
    print(f"{AIRFLOW_DEPLOY_PATH}csv             .csv\n")
    print(f"{AIRFLOW_DEPLOY_PATH}jar             .jar\n")
    print(f"{AIRFLOW_DEPLOY_PATH}user_data         *\n")
    print(
        "\033[32m{}\033[0m".format(
            "\nДОСТУПНЫЕ КЛЮЧИ: [-c], [-h], [--delete], [--file], [--dir], [skipped]\n\n"
        )
    )
    print("\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА БЕЗ ПАРАМЕТРОВ:"))
    print(
        f"    Синхронизация содержимого директорий  {AIRFLOW_DEPLOY_PATH}dags,  {AIRFLOW_DEPLOY_PATH}keytab,  {AIRFLOW_DEPLOY_PATH}scripts,"
    )
    print(
        f" {AIRFLOW_DEPLOY_PATH}keys,  {AIRFLOW_DEPLOY_PATH}csv,  {AIRFLOW_DEPLOY_PATH}jar,  {AIRFLOW_DEPLOY_PATH}user_data с соответствующими директориями в /app/airflow"
    )
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh\n\n"
        )
    )
    print("\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА С КЛЮЧОМ --skipped:"))
    print("    Отключение проверок")
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh --skipped\n\n"
        )
    )
    print("\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА С КЛЮЧОМ -c:"))
    print(
        f"    Производится очистка директории назначения ( {AIRFLOW_PATH}dags,  {AIRFLOW_PATH}keytab,  {AIRFLOW_PATH}scripts,)"
    )
    print(
        f" {AIRFLOW_PATH}keys,  {AIRFLOW_PATH}csv,  {AIRFLOW_PATH}jar,  {AIRFLOW_PATH}user_data) перед синхронизацией"
    )
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh -c\n\n"
        )
    )
    print("\033[32m{}\033[0m".format("Запуск скрипта с ключом -h:"))
    print("    Вывод справки")
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh -h\n\n"
        )
    )
    print("\033[32m{}\033[0m".format("Запуск скрипта с ключом --delete:"))
    print(
        f"    Удаление файла или директории(отсчет идет от  {AIRFLOW_DEPLOY_PATH})"
    )
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА (Удаление файла): sudo -u airflow_deploy ./airflow_sync_dags.sh --delete /dags/test.py"
        )
    )
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА (Удаление директории): sudo -u airflow_deploy ./airflow_sync_dags.sh --delete /dags/test_dir \n\n"
        )
    )
    print("\033[32m{}\033[0m".format("Запуск скрипта с ключом --file:"))
    print(
        f"    Деплой указанного файла (отсчет идет от  {AIRFLOW_DEPLOY_PATH}) из  {AIRFLOW_DEPLOY_PATH} в  {AIRFLOW_PATH}"
    )
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА : sudo -u airflow_deploy ./airflow_sync_dags.sh --file /dags/test.py\n\n"
        )
    )
    print("\033[32m{}\033[0m".format("Запуск скрипта с ключом --dir:"))
    print(
        f"    Деплой указанной директории (отсчет идет от  {AIRFLOW_DEPLOY_PATH}) из  {AIRFLOW_DEPLOY_PATH} в  {AIRFLOW_PATH}"
    )
    print(
        "\033[32m{}\033[0m".format(
            "ПРИМЕР ЗАПУСКА : sudo -u airflow_deploy ./airflow_sync_dags.sh --dir /dags/test_dir\n\n"
        )
    )
    print("\033[32m{}\033[0m".format("Ссылка на документацию Airflow:"))
    print(
        "    https://docs.cloud.vtb.ru/home/prod-catalog/application-integration/apache-airflow\n"
    )

    sys.exit(0)


def check_param_dir_key(
    script_args: list[str],
    current_datetime: datetime
) -> None:
    """
    Универсальная функция деплоя директории на все хосты для one-way и cluster.

    Аргументы:
        script_args (list[str]): Список путей к директориям для деплоя (относительно AIRFLOW_DEPLOY_PATH).
        current_datetime (datetime): Текущая дата и время для логирования.

    Использует глобальные переменные:
        real_name, CONFIGURATION, all_hosts, LOCAL_DEPLOY, AIRFLOW_PATH, AIRFLOW_DEPLOY_PATH, run_command_with_log,
        CHOWN_STRING, CHMOD_FG_FU_FO_STRING, CHMOD_WITHOUT_FU_FO_STRING, CHMOD_WITHOUT_DO_FU_DG_FO_STRING,
        RSYNC_CHECKSUM_DR_STRING, RSYNC_CHECKSUM_STRING, RSYNC_DRY_RUN, save_log

    Не возвращает значения. В случае ошибки завершает выполнение скрипта.
    """
    for i_script_args in script_args:
        temp_folder_path = i_script_args.rpartition("/")[0]
        airflow_deploy_dir_path = f"{AIRFLOW_DEPLOY_PATH}{i_script_args}"
        if not os.path.exists(airflow_deploy_dir_path):
            save_log(f"{current_datetime} {real_name} Директория не найдена {airflow_deploy_dir_path} \n\n",
                    with_exit=True)

        hosts = [LOCAL_DEPLOY] if CONFIGURATION == "one-way" else all_hosts + ["127.0.0.1"]
        host_prefix = "" if CONFIGURATION == "one-way" else "airflow_deploy@{host}:"

        if i_script_args.startswith("keytab") or i_script_args.startswith("keys"):
            CHMOD_STRING = CHMOD_WITHOUT_DO_FU_DG_FO_STRING if i_script_args.count("/") > 1 else CHMOD_WITHOUT_FU_FO_STRING
        else:
            CHMOD_STRING = CHMOD_FG_FU_FO_STRING

        for host in hosts:
            if i_script_args.count("/") > 1:
                run_command_with_log(
                    f'{RSYNC_CHECKSUM_DR_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/  {host_prefix.format(host=host)}{AIRFLOW_PATH}{i_script_args}',
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{i_script_args}\n\n",
                    with_exit=True,
                    rsync_error=True
                )
                run_command_with_log(
                    f'{RSYNC_CHECKSUM_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/  {host_prefix.format(host=host)}{AIRFLOW_PATH}{i_script_args}',
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{i_script_args}\n\n",
                )
            else:
                run_command_with_log(
                    f"{RSYNC_DRY_RUN} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/  {host_prefix.format(host=host)}{AIRFLOW_PATH}{i_script_args}",
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{i_script_args}\n\n",
                    with_exit=True,
                    rsync_error=True
                )
                run_command_with_log(
                    f"{RSYNC_CHECKSUM_STRING} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/  {host_prefix.format(host=host)}{AIRFLOW_PATH}{i_script_args}",
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{i_script_args}\n\n",
                )
        print("0")
        sys.exit(0)


def check_param_run(all_error: Queue) -> set:
    """
    Обрабатывает параметры командной строки для управления синхронизацией и удалением файлов/директорий Airflow.

    Параметры:
        all_error (Queue): Очередь для передачи ошибок.

    В зависимости от переданных ключей:
        --delete: удаляет указанные файлы/директории на локальном или удалённых хостах.
        --file: деплоит указанные файлы.
        --dir: деплоит указанные директории.
        -c: очищает директории назначения.
        -h: выводит справку.
    В случае неизвестного ключа — пишет ошибку в лог и завершает выполнение.
    """
    remove_files_folders = set()
    current_datetime = datetime.now()
    script_args = sys.argv[2:]
    if len(sys.argv) >= 2:

        def nonlocal_set(_, value):
            nonlocal remove_files_folders
            remove_files_folders = value

        key_func_map = {
            "--delete": lambda: check_param_delete_key(script_args, current_datetime),
            "--file": lambda: check_param_file_key(script_args, current_datetime),
            "--dir": lambda: check_param_dir_key(script_args, current_datetime),
            "-h": check_param_f_key,
            "-c": lambda: nonlocal_set('remove_files_folders', check_param_c_key(remove_files_folders, all_error)),
        }
        #TODO проверить работу с nonlocal_set
        func = key_func_map.get(sys.argv[1])
        if func:
            func()

    if (
        len(sys.argv) >= 2
        and sys.argv[1] not in ["--delete", "--file", "--dir", "--skipped", "-c", "-h"]
        and sys.argv[1] not in ARGV_KEYS
    ):
        save_log(f"{current_datetime} {real_name} Неизвестный ключ/и {sys.argv[1:]}\n\n")
    
    return remove_files_folders


def check_files_in_dirs(all_error: Queue) -> None:
    """
    Проверяет наличие файлов и директорий для переноса в  {AIRFLOW_DEPLOY_PATH}*.
    Если данных нет — добавляет ошибку в очередь all_error.

    Параметры:
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    files_in_dirs = 0
    for elem_list_folders in list_folders:
        for _, dirs, files in os.walk(f"{AIRFLOW_DEPLOY_PATH}{elem_list_folders}"):
            files_in_dirs += len(files)
            files_in_dirs += len(dirs)
            if files_in_dirs > 1:
                break

    if files_in_dirs <= 1:
        all_error.put(
            "Ошибка !!! В прикладных директориях /app/airflow_deploy (dags/csv/jar/keys/keytab/scripts/user_data) отсутствуют данные для переноса\n\n"
        )

def check_permission_type(
    host: str,
    folder: str,
    check_type: str,
    error_msg: str,
    all_error: Queue
) -> None:
    """
    Проверяет корректность групп или владельцев файлов/директорий на целевых хостах.
    Если найдены некорректные группы или владельцы — добавляет ошибку в очередь ALL_ERROR.
    Параметры:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        folder (str): Путь к директории для проверки.
        check_type (str): Тип проверки - "group" для групп, "user" для владельцев.
        error_msg (str): Сообщение об ошибке для добавления в очередь ALL_ERROR.
    """
    if check_type == "group":
        cmd = f"{SSH_USER}@{host} find {folder} ! -group airflow" #TODO вернуть проверку группы airflow_deploy ! -group airflow_deploy
    else:
        cmd = f"{SSH_USER}@{host} find {folder} ! -user airflow_deploy ! -user airflow"
    for_result = os.popen(cmd).read().split("\n")
    for item in for_result:
        if len(item) > 2:
            perm_error = os.popen(f"{SSH_USER}@{host} ls -l {item}").read()
            all_error.put(f"{error_msg} {perm_error}\n\n")

def check_groups_users(host: str, all_error: Queue) -> None:
    """
    Проверяет корректность групп и владельцев файлов/директорий на целевых хостах.
    Если найдены некорректные группы или владельцы — добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    #TODO: Проверить check_permission 
    for folder in list_folders:
        dir_path = f"{AIRFLOW_PATH}{folder}"
        if CONFIGURATION == "cluster":
            find_group_cmd = f"{SSH_USER}@{host} find {dir_path} ! -group airflow" #TODO вернуть проверку группы airflow_deploy ! -group airflow_deploy
            check_permission(find_group_cmd, "Ошибка !!! Некорректная группа на хосте", host, all_error)
            find_user_cmd = f"{SSH_USER}@{host} find {dir_path} ! -user airflow_deploy ! -user airflow"
            check_permission(find_user_cmd, "Ошибка !!! Некорректный владелец на хосте", host, all_error)

        if CONFIGURATION == "one-way":
            find_group_cmd = f"find {dir_path} ! -group airflow_deploy ! -group airflow"
            check_permission(find_group_cmd, "Ошибка !!! Некорректная группа", host, all_error)
            find_user_cmd = f"find {dir_path} ! -user airflow_deploy ! -user airflow"
            check_permission(find_user_cmd, "Ошибка !!! Некорректный владелец", host, all_error)



def check_permissions(host: str, all_error: Queue) -> None:
    """
    Проверяет права доступа к файлам и директориям на целевых хостах.
    Если права некорректны — добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    for folder in list_folders:
        dir_path = f"{AIRFLOW_PATH}{folder}"
        if CONFIGURATION == "cluster":
            # if folder in ("keytab", "keys"):
            #     check_permission_type(host, dir_path, "group", f"Ошибка !!! Некорректная группа на хосте {host}", all_error)
            # else:
            check_permission_type(host, dir_path, "group", f"Ошибка !!! Некорректная группа на хосте {host}", all_error)
            check_permission_type(host, dir_path, "user", f"Ошибка !!! Некорректный владелец на хосте {host}", all_error)

        # if CONFIGURATION == "one-way":
        #     # if folder in ("keytab", "keys"):
        #     #     check_permission_type("localhost", dir_path, "group", "Ошибка !!! Некорректная группа", all_error)
        #     # else:
        #     check_permission_type("localhost", dir_path, "group", "Ошибка !!! Некорректная группа", all_error)
        #     check_permission_type("localhost", dir_path, "user", "Ошибка !!! Некорректный владелец", all_error)

def copy_and_replace(source_path: str, destination_path: str) -> None:
    """
    Копирует файл из source_path в destination_path, заменяя существующий файл, если он есть.

    Параметры:
        source_path (str): Путь к исходному файлу.
        destination_path (str): Путь к целевому файлу.
    """
    if os.path.exists(destination_path):
        os.remove(destination_path)
    shutil.copy2(source_path, destination_path)


def check_type_file(dir_folder: str, type_files: list[str], all_error: Queue) -> None:
    """
    Проверяет типы файлов в указанной директории и добавляет ошибку в очередь, если найден недопустимый тип файла.

    Параметры:
        dir_folder (str): Путь к директории для проверки.
        type_files (list[str]): Список допустимых расширений файлов для директории.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    for root, _, files in os.walk(dir_folder):
        for file in files:
            temp_file = f"{root}{file}"
            if (
                dir_folder == f"{AIRFLOW_DEPLOY_PATH}dags/"
                and temp_file[:28] == f"{AIRFLOW_DEPLOY_PATH}dags/sql"
            ):
                continue

            if temp_file.rpartition(".")[2] not in type_files:
                for prefix, ext in ext_map.items():
                    if prefix == f"{AIRFLOW_DEPLOY_PATH}dags":
                        if temp_file.startswith(prefix) and not temp_file.startswith(f"{AIRFLOW_DEPLOY_PATH}dags/sql"):
                            all_error.put(
                                f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение {ext})\n\n"
                            )
                    else:
                        if temp_file.startswith(prefix):
                            all_error.put(
                                f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение {ext})\n\n"
                            )




def connect_write(host: str, all_error: Queue) -> None:
    """
    Проверяет доступность хоста с помощью команды ping.
    В случае недоступности хоста добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста для проверки доступности.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    data_connect_write = subprocess.Popen(
        f"ping -c 1 {host} ", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if "Name or service not known" in data_connect_write.stderr.read().decode("utf-8"):
        all_error.put(f"Ошибка !!! Проверьте доступ к хосту {host} \n")


def check_free_space(data_host: str, all_error: Queue) -> None:
    """
    Проверяет свободное место на разделе /app удалённого хоста и предупреждает,
    если после деплоя занятое место превысит критический порог.

    Параметры:
        data_host (str): Имя или адрес хоста для проверки.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    result_command = os.popen(
        f"{SSH_USER}@{data_host} df /app  --output=avail,used | tail -n +2 | tr -d '%'"
    ).read()
    free_disk_space = int(result_command.split(" ")[0])
    parts = result_command.split()
    if len(parts) < 2:
        all_error.put(f"Ошибка: некорректный вывод df: '{result_command}'\n")
        return

    used = int(parts[1])
    total = free_disk_space + used
    one_percent = total / 100
    used_percent = int((used + size_airflow_deploy) / one_percent)
    if used_percent > CRITICAL_PERCENT:
        if CONFIGURATION == "cluster":
            all_error.put(f"Предупреждение !!! После добавления файлов на хост {data_host} колличество занятого места превысит 80% в каталоге /app \n\n")
        else:
            all_error.put("Предупреждение !!! После добавления файлов на локальный хост колличество занятого места превысит 80% в каталоге /app \n\n")

def md5(fname: str) -> str:
    """
    Вычисляет MD5-хеш для указанного файла.

    Параметры:
        fname (str): Путь к файлу для вычисления хеша.

    Возвращает:
        str: Строка с MD5-хешем файла.
    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def path_sum_files() -> dict[str, str]:
    """
    Заполняет глобальный словарь PATH_SUM md5-хешами всех файлов во всех прикладных директориях.

    Для каждого файла в директориях из list_folders вычисляет md5-хеш и сохраняет его в PATH_SUM.
    Глобальная переменная:
        PATH_SUM (dict): Ключ — путь к файлу, значение — md5-хеш.
    """
    path_sum = {}
    for list_folder in list_folders:
        for root, _, files in os.walk(f"{AIRFLOW_DEPLOY_PATH}{list_folder}"):
            for file in files:
                path_sum[f"{root}/{file}"] = md5(f"{root}/{file}")

    return path_sum



def rsync_host(host_name: str, path_sum: dict[str, str]) -> None:
    """
    Выполняет синхронизацию директорий с помощью rsync на указанный хост.
    Для каждой директории из list_folders копирует содержимое на удалённый сервер airflow.
    Для файлов сохраняет информацию о копировании и md5-хеше в лог.

    Параметры:
        hostname (str): Имя или адрес хоста для синхронизации.
    """
    for folder in list_folders:
        if folder in ("keytab", "keys"):
            os.popen(f"{RSYNC_CHECKSUM_STRING} {CHOWN_STRING} {CHMOD_WITHOUT_FU_FO_STRING} {AIRFLOW_DEPLOY_PATH}{folder} airflow_deploy@{host_name}:{AIRFLOW_PATH} 2> /dev/null").read()
        else:
            os.popen(f"{RSYNC_CHECKSUM_STRING} {CHOWN_STRING} {CHMOD_FG_FU_FO_STRING} {AIRFLOW_DEPLOY_PATH}{folder} airflow_deploy@{host_name}:{AIRFLOW_PATH} 2> /dev/null").read()
        
        for root, _, files in os.walk(f"{AIRFLOW_DEPLOY_PATH}{folder}"):
            for file in files:
                with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as log_file:
                    temp_file = f"{root}/{file}"
                    temp_file_airflow = f"{AIRFLOW_PATH}" + f"{root}/{file}"[20:]
                    md5_sum = path_sum[temp_file]
                    log_file.write(f"  Source: {root}/{file}  Destination: {host_name}@{temp_file_airflow}  Md5hash: {md5_sum}\n\n"
)


def check_rsync_host(host_name: str, all_error: Queue) -> None:
    """
    Проверяет возможность синхронизации директорий с помощью rsync на указанный хост.
    В случае ошибки rsync добавляет сообщение об ошибке в очередь all_error.

    Параметры:
        host_name (str): Имя или адрес хоста для проверки синхронизации.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    for folder in list_folders:
        if folder in ("keytab", "keys"):
            chmod_string = CHMOD_WITHOUT_FU_FO_STRING
        else:
            chmod_string = CHMOD_FG_FU_FO_STRING

        command = f"{RSYNC_DRY_RUN} {CHOWN_STRING} {chmod_string} {AIRFLOW_DEPLOY_PATH}{folder} airflow_deploy@{host_name}:{AIRFLOW_PATH}"
        print(command)
        with subprocess.Popen(command,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE) as result:
            result_stderr = result.stderr.read().decode("utf-8")
            if "rsync error" in result_stderr:
                all_error.put(f"Ошибка!!! {host_name} \n {result_stderr}\n\n")

def host_checks(hostname: str, all_error: Queue) -> None:
    """
    Выполняет все проверки для одного хоста:
    - Проверка доступности (ping)
    - Проверка свободного места
    - Проверка прав доступа
    - Проверка групп и владельцев
    - Проверка возможности синхронизации через rsync

    Параметры:
        hostname (str): Имя или адрес хоста для проверки.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    connect_write(hostname, all_error)
    check_free_space(hostname, all_error)
    check_permissions(hostname, all_error)
    check_groups_users(hostname, all_error)
    check_rsync_host(hostname, all_error)

def main() -> None:
    """
    Основная функция скрипта, выполняющая синхронизацию директорий и проверку параметров.
    В зависимости от конфигурации (one-way или cluster) выполняет соответствующие действия.
    """
    for check_folder, check_extension in ext_map.items():
        check_type_file(check_folder, check_extension, ALL_ERROR)

    path_sum = path_sum_files()
    remove_files_folders = set()

    if CONFIGURATION == "one-way":
        remove_files_folders = check_param_run(ALL_ERROR)
        check_free_space(current_hostname, ALL_ERROR)
        check_permissions(current_hostname, ALL_ERROR)
        check_groups_users(current_hostname, ALL_ERROR)
        check_rsync_host(current_hostname, ALL_ERROR)
        check_files_in_dirs(ALL_ERROR)
        param_run_script()

        if ALL_ERROR.qsize() > 0:
            data_all_error = set(ALL_ERROR.get() for _ in range(ALL_ERROR.qsize()))
            with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as file_log:
                current_datetime_now = datetime.now()
                file_log.write(f"******************************************* ALL ERORRS {current_datetime_now}*******************************************\n\n")
                for one_data_all_error in data_all_error:
                    file_log.write(one_data_all_error)
                file_log.write(END_ALL_ERRORS_STRING)

            print("1")
            sys.exit(1)

        rsync_host(current_hostname, path_sum)

        with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as end_work_script:
            for remove_file in remove_files_folders:
                end_work_script.write(f"{remove_file}\n")

        with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as file_log:
            file_log.write(END_WORK_SCRIPT_STRING)

        print("0")
        sys.exit(0)

    if CONFIGURATION == "cluster":
        processes = [Process(target=host_checks, args=(hostname, ALL_ERROR)) for hostname in all_hosts]
        for p in processes:
            p.start()
        for p in processes:
            p.join()

        check_files_in_dirs(ALL_ERROR)
        param_run_script()

        if ALL_ERROR.qsize() > 0:
            data_all_error = [ALL_ERROR.get() for _ in range(ALL_ERROR.qsize())]
            with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as file_log:
                current_datetime_now = datetime.now()
                file_log.write(f"******************************************* ALL ERORRS {current_datetime_now}*******************************************\n\n")
                for one_data_all_error in data_all_error:
                    file_log.write(one_data_all_error)
                file_log.write(END_ALL_ERRORS_STRING)

                print("1")
                sys.exit(1)

        if len(sys.argv) == 2 and sys.argv[1] == "-c":
            for hostname in all_hosts:
                remove_destination_folder(hostname, result_queue)
            remove_files_folders = result_queue.get()

        for hostname in all_hosts:
            rsync_host(hostname, path_sum)

        with open(f"{AIRFLOW_DEPLOY_PATH}log/deploy.log", "a", encoding="utf-8") as end_work_script:
            if len(sys.argv) == 2 and sys.argv[1] == "-c":
                if f"removed directory '{AIRFLOW_PATH}dags/sql'\n" in remove_files_folders:
                    remove_files_folders.remove(f"removed directory '{AIRFLOW_PATH}dags/sql'\n")

                end_work_script.write("Удаленные файлы: \n\n")
                for remove_file in remove_files_folders:
                    if (remove_file == f"removed directory '{AIRFLOW_PATH}dags/sql'\n"):
                        continue

                    end_work_script.write(f"{remove_file}\n")
                end_work_script.write("\n")

            end_work_script.write(END_WORK_SCRIPT_STRING)

            print("0")
            sys.exit(0)

if __name__ == "__main__":
    main()
