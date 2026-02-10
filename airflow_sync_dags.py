import shutil
import os
import hashlib
import json
import sys
import subprocess
import socket
import logging
from multiprocessing import Process, Queue
from datetime import datetime
from typing import List



CRITICAL_PERCENT = 80
CMD = ["id"]

ALL_KEYS = ["--delete", "--file", "--dir", "-c", "-h", "--dry-run", "-v"]

KEY_MATRIX = {
    frozenset(['-v', '--delete']): True,
    frozenset(['-v', '--file']): True,
    frozenset(['-v', '--dir']): True,
    frozenset(['-v', '-c']): True,
    frozenset(['-v', '--dry-run']): True,
    frozenset(['--dry-run', '--delete']): True,
    frozenset(['--dry-run', '--dir']): True,
    frozenset(['--dry-run', '--file']): True,
    frozenset(['--dry-run', '-c']): True,
    frozenset(['-v', '--dry-run', '--delete']): True,
    frozenset(['-v', '--dry-run', '--dir']): True,
    frozenset(['-v', '--dry-run', '--file']): True,
    frozenset(['-v', '--dry-run', '-c']): True,
    # Остальные сочетания считаются запрещёнными по умолчанию
}

def is_key_combination_allowed(keys: List[str]) -> bool:
    """
    Проверяет, разрешена ли комбинация ключей согласно матрице.
    :param keys: список ключей без дефисов, например ['c', 'delete']
    :return: True если разрешено, иначе False
    """
    # Если три ключа, сначала проверяем тройку целиком
    if len(keys) == 3:
        triple = frozenset(keys)
        if triple in KEY_MATRIX:
            return KEY_MATRIX[triple]
    # Если два ключа, сначала проверяем пару целиком
    elif len(keys) == 2:
        pair = frozenset(keys)
        if pair in KEY_MATRIX:
            return KEY_MATRIX[pair]
    else:
        return keys[0] in ALL_KEYS
    # Проверяем каждую пару ключей
    # for i, key1 in enumerate(keys):
    #     for j in range(i + 1, len(keys)):
    #         pair = frozenset([key1, keys[j]])
    #         if not KEY_MATRIX.get(pair, False):
    #             return False
    return False


GLOBAL_LIST_ERROR = set()

result_queue = Queue()
ALL_ERROR = Queue()

END_WORK_SCRIPT_STRING = "\n****************************************** END WORK SCRIPT *******************************************\n\n"
END_ALL_ERRORS_STRING = "\n******************************************* END ALL ERORRS *******************************************\n\n"
RSYNC_CHECKSUM_STRING = 'rsync --checksum -rogp --rsync-path="mkdir -p'
RSYNC_CHECKSUM_DR_STRING = 'rsync --checksum -nrogp --rsync-path="mkdir -p'
RSYNC_DRY_RUN = 'rsync --checksum -nrogp'
RSYNC_CHECKSUM = "rsync --checksum -rogp" 
CHOWN_STRING = "--chown=airflow_deploy:airflow"
CHMOD_FG_FU_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx"
AIRFLOW_PATH = "/app/airflow/"
AIRFLOW_DEPLOY_PATH = "/app/airflow_deploy/"
LOCAL_DEPLOY = "airflow_deploy@127.0.0.1"
SSH_USER = "ssh airflow_deploy"
CHMOD_WITHOUT_FU_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo="
CHMOD_WITHOUT_DO_FU_DG_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo="

VERBOSE =  len(sys.argv) > 1 and sys.argv[1] == "-v"

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
    for allowed_prefix in ext_map.keys():
        if path.startswith(allowed_prefix):# and len(path) > len(allowed_prefix) and path[len(allowed_prefix)] in ('/', '\\')
            return True
    return False

size_airflow_deploy = int(os.popen("du -s app/airflow_deploy | cut -f1").read())
list_folders = ["dags","csv", "jar", "keys", "keytab", "scripts", "user_data"]

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
all_hosts = schedulers + webs + workers + ["127.0.0.1"]

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

LOG_DIR = '/app/airflow_deploy/log/'
LOG_FILE = os.path.join(LOG_DIR, 'script.log')

def setup_logger() -> logging.Logger:
    """
    Создаёт и настраивает экземпляр логгера.

    Логгер пишет сообщения в консоль (stdout) и в файл, расположенный по пути LOG_FILE.
    Уровень логирования определяется переменной VERBOSE (DEBUG или INFO).

    Returns:
        logging.Logger: Настроенный экземпляр логгера.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    
    level = logging.DEBUG if VERBOSE else logging.INFO
    logger_obj = logging.getLogger('airflow_sync')
    logger_obj.setLevel(level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    # stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger_obj.addHandler(sh)

    # file handler
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(formatter)
    logger_obj.addHandler(fh)

    return logger_obj

logger = setup_logger()

def save_log(message: str,
            with_exit=False,
            info_level=False) -> None:
    """
    Логирует сообщение через стандартный logger. Если with_exit=True, пишет как ошибку и завершает выполнение.

    :param message: Сообщение для лога.
    :param with_exit: Если True, завершает выполнение скрипта с кодом 1.
    :param info_level: Если True, логирует сообщение на уровне INFO.
    """
    if with_exit:
        logger.error(message)
        print("1")
        sys.exit(1)
    else:
        if VERBOSE:
            logger.debug(message)
        if info_level:
            logger.info(message)


def check_real_user() -> str:
    """
    Определяет имя пользователя, под которым запущен скрипт (с учётом sudo).

    Возвращает:
        str: Имя пользователя, под которым выполняется скрипт, либо None при ошибке.
    """
    save_log("Запуск определения имени пользователя (check_real_user)")
    try:
        with subprocess.Popen(
            "${SUDO_USER:-${USER}}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ) as request_name:
            stdout_output = request_name.stdout.read().decode("utf-8") if request_name.stdout else ""
            stderr_output = request_name.stderr.read().decode("utf-8") if request_name.stderr else ""

        if stdout_output:
            try:
                real_server_name = stdout_output.split(" ")[0].split("(")[1].split(")")[0]
                save_log(f"Пользователь определён по stdout: {real_server_name}")
            except (ValueError, IndexError) as e:
                save_log(f"Ошибка разбора stdout при определении пользователя: {e}", with_exit=True)
                real_server_name = None
        else:
            try:
                real_server_name = stderr_output.split(" ")[1].replace(":", "").strip()
                save_log(f"Пользователь определён по stderr: {real_server_name}", info_level=True)
            except (ValueError, IndexError) as e:
                save_log(f"Ошибка разбора stderr при определении пользователя: {e}", with_exit=True)
                real_server_name = None

        if real_server_name is None:
            save_log("Не удалось определить имя пользователя", with_exit=True)
        return real_server_name
    except Exception as e:
        save_log(f"Ошибка при определении имени пользователя: {e}", with_exit=True)
        return None

real_name = check_real_user()

current_hostname = socket.gethostname()


def param_run_script(keys: list[str]) -> None:
    """
    Записывает информацию о запуске скрипта в лог-файл  {AIRFLOW_DEPLOY_PATH}log/deploy.log.

    В лог добавляются:
        - отметка о запуске,
        - дата и время запуска,
        - имя пользователя,
        - параметр запуска (если передан -c, то отмечается, иначе пишется 'false key').
    """
    save_log("******************************************* Run script *******************************************", info_level=True)
    current_datetime = datetime.now()
    save_log(f"Start run script: {current_datetime}", info_level=True)
    save_log(f"User: {real_name}", info_level=True)

    for key in keys:
        if key not in ALL_KEYS:
            save_log(f"{current_datetime} {real_name} Неизвестный ключ/и {keys}\n\n", with_exit=True)
        if key == "-h":
            check_param_h_key()
            sys.exit(0)
           

def run_command_with_log(
    command: str,
    log_message: str,
    rsync_error: bool = False,
    info_level: bool = False
) -> str:
    """
    Выполняет команду через os.popen, записывает сообщение в лог.
    Если rsync_error=True, то при наличии 'rsync error' в результате записывает ошибку в лог и завершает выполнение.
    Иначе просто пишет log_message в лог.

    :param command: Команда для выполнения.
    :param log_message: Сообщение для записи в лог.
    :param rsync_error: Проверять ли результат на 'rsync error' (по умолчанию False).
    :param info_level: Записывать ли сообщение на уровне info (по умолчанию False).
    :return: Результат выполнения команды.
    """
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    stdout_decoded = stdout.decode("utf-8").strip()
    stderr_decoded = stderr.decode("utf-8").strip()

    if VERBOSE:
        logger.debug(log_message)
        # logger.debug("Выполненная команда: %s", command)
    if info_level:
        logger.info(log_message)

    if stdout_decoded:
        logger.info("stdout: %s", stdout_decoded)
    if stderr_decoded:
        logger.error("stderr: %s", stderr_decoded)

    # Проверка ошибок rsync
    if rsync_error and "rsync error" in stderr_decoded:
        save_log("rsync error detected", with_exit=True)

    return stdout_decoded


def check_permissions(host: str) -> None:
    """
    Проверяет права доступа к файлам и директориям на целевых хостах.
    Если права некорректны — добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    save_log(f"Запуск проверки прав доступа на хосте: {host}")
    try:
        for folder in list_folders:
            dir_path = f"{AIRFLOW_PATH}{folder}"
            save_log(f"Проверка группы для директории: {dir_path} на хосте {host}")
            check_permission_type(host, dir_path, "group", f"Ошибка !!! Некорректная группа на хосте {host}")
            save_log(f"Проверка владельца для директории: {dir_path} на хосте {host}")
            check_permission_type(host, dir_path, "user", f"Ошибка !!! Некорректный владелец на хосте {host}")
        save_log(f"Результат проверки прав доступа на хосте {host}: завершено без ошибок")
    except Exception as e:
        save_log(f"Ошибка при проверке прав на хосте {host}: {str(e)}", with_exit=True)


def check_permission_dir_and_files(find_cmd: str,
                    error_prefix: str,
                    host: str) -> None:
    """
    Выполняет проверку прав доступа к файлам/директориям на целевом хосте с помощью команды find.
    Для каждого найденного объекта с некорректными правами вызывает ls -l и добавляет ошибку в очередь all_error.

    Параметры:
        find_cmd (str): Команда find для поиска файлов/директорий с некорректными правами.
        error_prefix (str): Префикс сообщения об ошибке (например, "Ошибка !!! Некорректная группа на хосте").
        host (str): Имя или адрес хоста, на котором выполняется проверка.
    """
    try:
        result_str = run_command_with_log(find_cmd, f"Проверка разрешённых директорий: {find_cmd} на хосте {host}")
        result = result_str.split("\n")
        for item in result:
            if item.strip():
                # perm_error = os.popen(f"{SSH_USER}@{host} ls -l {item}").read()
                perm_error = run_command_with_log(f"{SSH_USER}@{host} ls -l {item}", f"Проверка прав доступа: {item} на хосте {host}")
                save_log(f"{error_prefix} {host} {perm_error}", with_exit=True)
        save_log(f"Результат проверки разрешённых директорий на хосте {host}: завершено без исключений")
    except Exception as e:
        save_log(f"Ошибка при проверке разрешённых директорий на хосте {host}: {str(e)}", with_exit=True)


# def remove_path(path: str,
#             folders_set: set) -> set:
#     """
#     Удаляет файл или директорию по указанному пути и добавляет информацию об удалении в множество.

#     Параметры:
#         path (str): Путь к файлу или директории для удаления.
#         folders_set (set): Множество, в которое добавляется строка с информацией об удалении.

#     Возвращает:
#         set: Обновлённое множество с информацией об удалённых файлах/директориях.
#     """
#     try:
#         save_log(f"Запуск удаления: {path}", info_level=True)
#         if os.path.isfile(path):
#             os.remove(path)
#             save_log(f"Удалён файл: {path}", info_level=True)
#             folders_set.add(f"removed  {path}")
#         elif os.path.isdir(path):
#             shutil.rmtree(path)
#             save_log(f"Удалена директория: {path}", info_level=True)
#             folders_set.add(f"removed  {path}")
#         save_log(f"Результат удаления: {path} успешно", info_level=True)
#         return folders_set
#     except Exception as e:
#         save_log(f"Ошибка при удалении {path}: {str(e)}", with_exit=True)


def check_param_delete_key(
    paths: list[str]
) -> None:
    """
    Обрабатывает удаление файлов/директорий по ключу --delete.
    Проверяет существование, удаляет локально или по ssh, логирует действия и завершает выполнение.

    Аргументы:
        paths (list[str]): Список путей к файлам/директориям для удаления (относительно AIRFLOW_PATH).

    Использует глобальные переменные:
        real_name, CONFIGURATION, all_hosts, AIRFLOW_PATH, SSH_USER, save_log
    """
    save_log(f"Запуск удаления файлов/директорий по ключу --delete: {paths}")
    current_datetime = datetime.now()
    try:
        missing = [f"{AIRFLOW_PATH}{x}" for x in paths if not os.path.exists(f"{AIRFLOW_PATH}{x}")]
        if missing:
            save_log(f"{current_datetime} {real_name} Нет такого файла или директории {', '.join(missing)}\n\n", with_exit=True)

        for i_script_args in paths:
            path = f"{AIRFLOW_PATH}{i_script_args}"
            if CONFIGURATION == "one-way":
                try:
                    if os.path.isfile(path):
                        os.remove(path)
                        save_log(f"Удалён файл: {path}", info_level=True)
                    elif os.path.isdir(path):
                        shutil.rmtree(path)
                        save_log(f"Удалена директория: {path}", info_level=True)
                except Exception as e:
                    save_log(f"Ошибка при удалении {path}: {str(e)}", with_exit=True)
                    save_log(f"{current_datetime} {real_name} Ошибка при удалении {path}: {str(e)}\n\n", with_exit=True)
            else:
                for host in all_hosts:
                    try:
                        run_command_with_log(f"{SSH_USER}@{host} rm -rf {path}", f"Удаление файла/директории: {path} на хосте {host}")
                        save_log(f"Удалён файл/директория: {path} на хосте {host}", info_level=True)
                    except Exception as e:
                        save_log(f"Ошибка при удалении {path} на хосте {host}: {str(e)}", with_exit=True)
                        save_log(f"{current_datetime} {real_name} Ошибка при удалении {path} на хосте {host}: {str(e)}\n\n", with_exit=True)
        save_log("Удаление файлов/директорий завершено успешно", info_level=True)
        print(0)
        sys.exit(0)
        
    except Exception as e:
        save_log(f"Ошибка при удалении: {str(e)}", with_exit=True)


def check_param_file_key(
    paths: list[str]
    ) -> None:
    """
    Универсальная функция деплоя файла/директории на все хосты для one-way и cluster.

    Аргументы:
        paths (list[str]): Список путей к файлам/директориям для деплоя (относительно AIRFLOW_DEPLOY_PATH).

    Использует глобальные переменные:
        real_name, CONFIGURATION, all_hosts, LOCAL_DEPLOY, AIRFLOW_PATH, AIRFLOW_DEPLOY_PATH, run_command_with_log,
        CHOWN_STRING, CHMOD_FG_FU_FO_STRING, CHMOD_WITHOUT_FU_FO_STRING, CHMOD_WITHOUT_DO_FU_DG_FO_STRING,
        RSYNC_CHECKSUM_DR_STRING, RSYNC_CHECKSUM_STRING, RSYNC_DRY_RUN, save_log
    """
    save_log(f"Запуск деплоя файлов: {paths}")
    current_datetime = datetime.now()
    try:
        for path in paths:
            airflow_deploy_dir_path = f"{AIRFLOW_DEPLOY_PATH}{path}"
            temp_folder_path = path.rpartition("/")[0]

            save_log(f"Проверка наличия файла для деплоя: {airflow_deploy_dir_path}")
            if not os.path.exists(airflow_deploy_dir_path):
                save_log(f"Файл не найден для деплоя: {airflow_deploy_dir_path}", with_exit=True)
            
            if path.startswith("keytab") or path.startswith("keys"):
                if path.count("/") > 1:
                    CHMOD_STRING = CHMOD_WITHOUT_DO_FU_DG_FO_STRING
                else:
                    CHMOD_STRING = CHMOD_WITHOUT_FU_FO_STRING
            else:
                CHMOD_STRING = CHMOD_FG_FU_FO_STRING

            if CONFIGURATION == "one-way":
                hosts = ["127.0.0.1"]
            else:
                hosts = all_hosts

            for host in hosts:
                host_prefix = f"airflow_deploy@{host}:"
                save_log(f"Запуск rsync для деплоя файла: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
                try:
                    if path.count("/") > 1:
                        run_command_with_log(
                            f'{RSYNC_CHECKSUM_DR_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{path}',
                            f"Dry-run rsync для файла:  {AIRFLOW_PATH}{path} на хосте {host}",
                            rsync_error=True
                        )
                        run_command_with_log(
                            f'{RSYNC_CHECKSUM_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{path}',
                            f"Деплой файла:  {AIRFLOW_PATH}{path} на хосте {host}",
                        )
                        save_log(f"Файл успешно скопирован: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
                    else:
                        run_command_with_log(
                            f"{RSYNC_DRY_RUN} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{path}",
                            f"Dry-run rsync для файла:  {AIRFLOW_PATH}{path} на хосте {host}",
                            rsync_error=True
                        )
                        run_command_with_log(
                            f"{RSYNC_CHECKSUM} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{path}",
                            f"Деплой файла:  {AIRFLOW_PATH}{path} на хосте {host}",
                        )
                        save_log(f"Файл успешно скопирован: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
                except Exception as e:
                    save_log(f"Ошибка копирования файла {airflow_deploy_dir_path} на хост {host}: {str(e)}", with_exit=True)

        save_log("Результат деплоя файлов: успешно", info_level=True)

    except Exception as e:
        save_log(f"{current_datetime} {real_name} Ошибка при деплое файла: {str(e)}\n\n", with_exit=True)


# def check_param_c_key(remove_files_folders: set,
#                     all_error:Queue) -> set:
#     """
#     Очищает содержимое всех целевых директорий Airflow (для one-way).
#     Удаляет все файлы и поддиректории, кроме __pycache__ и dags/sql (последняя обрабатывается отдельно).

#     Параметры:
#         remove_files_folders (set): Множество для сбора информации об удалённых файлах/директориях.

#     Возвращает:
#         set: Обновлённое множество с информацией об удалённых файлах/директориях.
#     """
#     logger.debug("Запуск очистки целевых директорий Airflow (ключ -c)")
#     try:
#         check_files_in_dirs(all_error)
#         logger.debug("Проверка наличия файлов для очистки завершена")
#         if CONFIGURATION == "one-way":
#             for folder in list_folders:
#                 folder_path = f"{AIRFLOW_PATH}{folder}/"
#                 logger.debug("Очистка директории: %s", folder_path)
#                 for entry in os.listdir(folder_path):
#                     full_path = f"{folder_path}{entry}"
#                     if "__pycache__" in full_path or full_path == f"{AIRFLOW_PATH}dags/sql":
#                         continue
#                     logger.debug("Удаление: %s", full_path)
#                     remove_files_folders = remove_path(full_path, remove_files_folders)
#                 if folder == "dags":
#                     sql_dir = f"{AIRFLOW_PATH}dags/sql/"
#                     logger.debug("Очистка директории SQL: %s", sql_dir)
#                     for sql_entry in os.listdir(sql_dir):
#                         sql_path = f"{sql_dir}{sql_entry}"
#                         logger.debug("Удаление: %s", sql_path)
#                         remove_files_folders = remove_path(sql_path, remove_files_folders)
#             logger.debug("Очистка целевых директорий завершена успешно")
#             return remove_files_folders
        
    # except Exception as e:
    #     logger.error("Ошибка при очистке директорий: %s", str(e))
    #     print(1)
    #     sys.exit(1)

def remote_delete_items(elem: str, host_name: str) -> None:
    """
    Удаляет все элементы в целевой директории на удалённом хосте через ssh.
    Для dags пропускает __pycache__, для остальных удаляет все элементы.
    """
    try:
        save_log(f"Запуск удаления содержимого директории: {AIRFLOW_PATH}{elem} на хосте {host_name}", info_level=True)
        items_str = run_command_with_log(f"{SSH_USER}@{host_name} ls -a {AIRFLOW_PATH}{elem}/", f"Получение списка элементов в {AIRFLOW_PATH}{elem} на хосте {host_name}")
        items = [x for x in items_str.split("\n") if x not in {".", "..", ""}]
        if elem == "dags":
            for item in items:
                if "__pycache__" in item or ".pyc" in item:
                    continue
                result = run_command_with_log(f"{SSH_USER}@{host_name} rm -rfv {AIRFLOW_PATH}dags/{item}", f"Удаление: {AIRFLOW_PATH}dags/{item} на хосте {host_name}", info_level=True)
                save_log(f"Результат удаления {AIRFLOW_PATH}dags/{item} на хосте {host_name}: {result.strip()}", info_level=True)
            result_sql = run_command_with_log(f"{SSH_USER}@{host_name} rm -rfv {AIRFLOW_PATH}dags/sql/*", f"Удаление SQL-файлов в директории dags/sql на хосте {host_name}", info_level=True)
            save_log(f"Результат удаления SQL-файлов на хосте {host_name}: {result_sql.strip()}", info_level=True)
        else:
            for item in items:
                result = run_command_with_log(f"{SSH_USER}@{host_name} rm -rf {AIRFLOW_PATH}{elem}/{item}", f"Удаление: {AIRFLOW_PATH}{elem}/{item} на хосте {host_name}", info_level=True)
                save_log(f"Результат удаления {AIRFLOW_PATH}{elem}/{item} на хосте {host_name}: {result.strip()}", info_level=True)
    except Exception as e:
        save_log(f"Ошибка при удалении содержимого {AIRFLOW_PATH}{elem} на хосте {host_name}: {str(e)}", with_exit=True)

def remove_destination_folders() -> None:
    """
    Удаляет содержимое целевых папок на удалённом сервере airflow_deploy через ssh.
    
    Параметры:
        host_name (str): Имя или адрес удалённого хоста.
        result_q (Queue): Очередь для передачи результатов выполнения команд.
    
    Для папки dags пропускает каталоги __pycache__, для остальных удаляет все элементы.
    """
    save_log("Запуск очистки целевых папок на удалённых хостах airflow_deploy через ssh", info_level=True)
    try:
        if CONFIGURATION == "one-way":
            hosts = ["127.0.0.1"]
        else:
            hosts = all_hosts

        for host_name in hosts:
            save_log(f"Очистка на хосте: {host_name}", info_level=True)
            for elem in list_folders:
                remote_delete_items(elem, host_name)
        
        save_log("Очистка целевых папок на удалённых хостах завершена успешно", info_level=True)
    
        print(0)
        sys.exit(0)

    except Exception as e:
        save_log(f"Ошибка при очистке целевых папок: {str(e)}", with_exit=True)



def check_param_h_key() -> None:
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
            "\nДОСТУПНЫЕ КЛЮЧИ: [-c], [-h], [--delete], [--file], [--dir]\n\n"
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
    paths: list[str]
) -> None:
    """
    Универсальная функция деплоя директории на все хосты для one-way и cluster.

    Аргументы:
        paths (list[str]): Список путей к директориям для деплоя (относительно AIRFLOW_DEPLOY_PATH).

    Использует глобальные переменные:
        real_name, CONFIGURATION, all_hosts, LOCAL_DEPLOY, AIRFLOW_PATH, AIRFLOW_DEPLOY_PATH, run_command_with_log,
        CHOWN_STRING, CHMOD_FG_FU_FO_STRING, CHMOD_WITHOUT_FU_FO_STRING, CHMOD_WITHOUT_DO_FU_DG_FO_STRING,
        RSYNC_CHECKSUM_DR_STRING, RSYNC_CHECKSUM_STRING, RSYNC_DRY_RUN, save_log

    Не возвращает значения. В случае ошибки завершает выполнение скрипта.
    """
    try:
        save_log(f"Запуск деплоя директорий: {paths}")
        current_datetime = datetime.now()
        for path in paths:
            temp_folder_path = path.rpartition("/")[0]
            airflow_deploy_dir_path = f"{AIRFLOW_DEPLOY_PATH}{path}"
            save_log(f"{current_datetime} {real_name} Проверка наличия директории для деплоя: {airflow_deploy_dir_path}")
            if not os.path.exists(airflow_deploy_dir_path):
                save_log(f"{current_datetime} {real_name} Директория не найдена {airflow_deploy_dir_path} \n\n",
                        with_exit=True)

            hosts = [LOCAL_DEPLOY] if CONFIGURATION == "one-way" else all_hosts
            host_prefix = "" if CONFIGURATION == "one-way" else "airflow_deploy@{host}:"

            if path.startswith("keytab") or path.startswith("keys"):
                CHMOD_STRING = CHMOD_WITHOUT_DO_FU_DG_FO_STRING if path.count("/") > 1 else CHMOD_WITHOUT_FU_FO_STRING
            else:
                CHMOD_STRING = CHMOD_FG_FU_FO_STRING

            for host in hosts:
                if path.count("/") > 1:
                    run_command_with_log(
                        f'{RSYNC_CHECKSUM_DR_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/ {host_prefix.format(host=host)}{AIRFLOW_PATH}{path}',
                        f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Dry-Run для директории:  {AIRFLOW_PATH}{path}\n\n",
                        rsync_error=True
                    )
                    run_command_with_log(
                        f'{RSYNC_CHECKSUM_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/ {host_prefix.format(host=host)}{AIRFLOW_PATH}{path}',
                        f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{path}\n\n",
                    )
                    save_log(f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Директория успешно скопирована: {airflow_deploy_dir_path}\n\n", info_level=True)
                else:
                    run_command_with_log(
                        f"{RSYNC_DRY_RUN} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/ {host_prefix.format(host=host)}{AIRFLOW_PATH}{path}",
                        f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Dry-Run для директории:  {AIRFLOW_PATH}{path}\n\n",
                        rsync_error=True
                    )
                    run_command_with_log(
                        f"{RSYNC_CHECKSUM} {CHOWN_STRING} {CHMOD_STRING} {airflow_deploy_dir_path}/ {host_prefix.format(host=host)}{AIRFLOW_PATH}{path}",
                        f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{path}\n\n",
                    )
                    save_log(f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Директория успешно скопирована: {airflow_deploy_dir_path}\n\n", info_level=True)

    except Exception as e:
        save_log(f"{current_datetime} {real_name} Ошибка при деплое директории: {str(e)}\n\n", with_exit=True)



def check_param_run(keys: list[str],
                    paths: list[str]) -> set:
    """
    Обрабатывает параметры командной строки для управления синхронизацией и удалением файлов/директорий Airflow.
    Параметры:
            keys (list[str]): Список ключей для определения действий (например, --delete, --file, --dir, -c, -h).
            paths (list[str]): Список путей к файлам/директориям для обработки (относительно AIRFLOW_DEPLOY_PATH).
    В зависимости от переданных ключей:
        --delete: удаляет указанные файлы/директории на локальном или удалённых хостах.
        --file: деплоит указанные файлы.
        --dir: деплоит указанные директории.
        -c: очищает директории назначения.
        -h: выводит справку.
    В случае неизвестного ключа — пишет ошибку в лог и завершает выполнение.
    """
    current_datetime = datetime.now()
    try:
        key_func_map = {
            "--delete": lambda: check_param_delete_key(paths),
            "--file": lambda: check_param_file_key(paths),
            "--dir": lambda: check_param_dir_key(paths),
            "-c": remove_destination_folders,
            "--dry-run": check_rsync_host
        }
        if "--dry-run" in keys:
            check_rsync_host()
            keys.remove("--dry-run")
            
        for key in keys:
            func = key_func_map.get(key)
            if func:
                func()

    except Exception as e:
        save_log(f"{current_datetime} {real_name} Ошибка при обработке параметров: {str(e)}\n\n", with_exit=True)



def check_files_in_dirs() -> None:
    """
    Проверяет наличие файлов и директорий для переноса в  {AIRFLOW_DEPLOY_PATH}*.
    Если данных нет — добавляет ошибку в очередь all_error.

    Параметры:
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    save_log(f"Запуск проверки наличия файлов и директорий для переноса в {AIRFLOW_DEPLOY_PATH}")
    try:
        files_in_dirs = 0
        for elem_list_folders in list_folders:
            for _, dirs, files in os.walk(f"{AIRFLOW_DEPLOY_PATH}{elem_list_folders}"):
                files_in_dirs += len(files)
                files_in_dirs += len(dirs)
                if files_in_dirs > 1:
                    break

        if files_in_dirs <= 1:
            save_log(f"{datetime.now()} {real_name} Ошибка !!! В прикладных директориях /app/airflow_deploy (dags/csv/jar/keys/keytab/scripts/user_data) отсутствуют данные для переноса\n\n", with_exit=True)
        else:
            save_log(f"Проверка наличия файлов для переноса завершена успешно. Найдено файлов/директорий: {files_in_dirs}")

    except Exception as e:
        save_log(f"{datetime.now()} {real_name} Ошибка при проверке наличия файлов в директориях: {str(e)}\n\n", with_exit=True)


def check_permission_type(
    host: str,
    folder: str,
    check_type: str,
    error_msg: str
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
    try:
        save_log(f"Запуск проверки {check_type} для {folder} на хосте {host}")
        folder_name = os.path.basename(folder.rstrip('/'))
        if check_type == "group":
            cmd = f"{SSH_USER}@{host} find {folder} ! -group airflow"
            log_prefix = "Проверка группы:"
        else:
            cmd = f"{SSH_USER}@{host} find {folder} ! -user airflow_deploy ! -user airflow"
            log_prefix = "Проверка владельца:"

        save_log(f"{log_prefix} {cmd}")
        for_result = run_command_with_log(cmd, f"Проверка {check_type} на хосте {host} для {folder}").strip().split("\n") 
        for item in for_result:
            if len(item) > 2:
                perm_error = run_command_with_log(f"{SSH_USER}@{host} ls -l {item}", f"Ошибка при проверке группы или пользователя на хосте {host} для {item}")
                save_log(f"{error_msg} {item} {perm_error.strip()}", with_exit=True)
    except Exception as e:
        save_log(f"Ошибка при проверке группы или пользователя {check_type} на хосте {host}: {str(e)}", with_exit=True)

    try:
        save_log(f"Запуск проверки прав доступа для {folder} на хосте {host}")
        folder_name = os.path.basename(folder.rstrip('/'))
        if folder_name in ("keys", "keytab"):
            perm_cmd = f"{SSH_USER}@{host} find {folder} ! -perm 0060"
        else:
            perm_cmd = f"{SSH_USER}@{host} find {folder} ! -perm 0755"
        
        perm_error_prefix = f"Ошибка !!! Некорректные права для {folder} на хосте"

        save_log(f"Проверка прав доступа: {perm_cmd}")
        perm_result = os.popen(perm_cmd).read().split("\n")
        for item in perm_result:
            if len(item) > 2:
                print(perm_result)
                perm_error = run_command_with_log(f"{SSH_USER}@{host} ls -l {item}", f"Ошибка при проверке прав на хосте {host} для {item}")
                save_log(f"{perm_error_prefix} {item} {perm_error.strip()}", with_exit=True)
    
    except Exception as e:
        save_log(f"Ошибка при проверке прав на хосте {host}: {str(e)}", with_exit=True)


def check_groups_users(host: str) -> None:
    """
    Проверяет корректность групп и владельцев файлов/директорий на целевых хостах.
    Если найдены некорректные группы или владельцы — добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
    """
    save_log(f"Запуск проверки групп и владельцев на хосте: {host}")
    try:
        for folder in list_folders:
            dir_path = f"{AIRFLOW_PATH}{folder}"
            if CONFIGURATION == "cluster":
                save_log(f"Проверка группы для директории: {dir_path} на хосте {host}")
                find_group_cmd = f"{SSH_USER}@{host} find {dir_path} ! -group airflow"
                check_permission_dir_and_files(find_group_cmd, "Ошибка !!! Некорректная группа на хосте", host)
                save_log(f"Проверка владельца для директории: {dir_path} на хосте {host}")
                find_user_cmd = f"{SSH_USER}@{host} find {dir_path} ! -user airflow_deploy ! -user airflow"
                check_permission_dir_and_files(find_user_cmd, "Ошибка !!! Некорректный владелец на хосте", host)
            else:
                save_log(f"Проверка группы для директории: {dir_path} на хосте {host}")
                find_group_cmd = f"find {dir_path} ! -group airflow_deploy ! -group airflow"
                check_permission_dir_and_files(find_group_cmd, "Ошибка !!! Некорректная группа", host)
                save_log(f"Проверка владельца для директории: {dir_path} на хосте {host}")
                find_user_cmd = f"find {dir_path} ! -user airflow_deploy ! -user airflow"
                check_permission_dir_and_files(find_user_cmd, "Ошибка !!! Некорректный владелец", host)
        save_log(f"Результат проверки групп и владельцев на хосте {host}: завершено без ошибок")
    except Exception as e:
        save_log(f"Ошибка при проверке групп и владельцев на хосте {host}: {str(e)}", with_exit=True)





# def copy_and_replace(source_path: str, destination_path: str) -> None:
#     """
#     Копирует файл из source_path в destination_path, заменяя существующий файл, если он есть.

#     Параметры:
#         source_path (str): Путь к исходному файлу.
#         destination_path (str): Путь к целевому файлу.
#     """
#     try:
#         if os.path.exists(destination_path):
#             os.remove(destination_path)
#             save_log(f"Файл {destination_path} удалён для замены", info_level=True)
#         shutil.copy2(source_path, destination_path)
#     except Exception as e:
#         save_log(f"Ошибка при копировании файла из {source_path} в {destination_path}: {str(e)}", with_exit=True)
def is_invalid_file_type(temp_file: str, dir_folder: str, type_files: list[str]) -> tuple[bool, str]:
    """
    Проверяет, является ли файл недопустимого типа для указанной директории.

    Параметры:
        temp_file (str): Полный путь к файлу для проверки.
        dir_folder (str): Путь к директории, в которой производится проверка.
        type_files (list[str]): Список допустимых расширений файлов для директории.

    Возвращает:
        tuple[bool, str]:
            - True и сообщение об ошибке, если тип файла недопустим;
            - False и пустую строку, если тип файла допустим.
    """
    for prefix, ext in ext_map.items():
        if prefix == f"{AIRFLOW_DEPLOY_PATH}dags":
            if temp_file.startswith(prefix) and not temp_file.startswith(f"{AIRFLOW_DEPLOY_PATH}dags/sql"):
                if temp_file.rpartition(".")[2] not in type_files:
                    return True, f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение {ext})"
        else:
            if temp_file.startswith(prefix):
                if temp_file.rpartition(".")[2] not in type_files:
                    return True, f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение {ext})"
    return False, ""

def check_type_file(dir_folder: str, type_files: list[str]) -> None:
    """
    Проверяет типы файлов в указанной директории и логирует ошибку, если найден недопустимый тип файла.

    Параметры:
        dir_folder (str): Путь к директории для проверки.
        type_files (list[str]): Список допустимых расширений файлов для директории.
    """
    save_log(f"Запуск проверки типов файлов в директории: {dir_folder}")
    try:
        for root, _, files in os.walk(dir_folder):
            for file in files:
                temp_file = f"{root}{file}"
                if (
                    dir_folder == f"{AIRFLOW_DEPLOY_PATH}dags/"
                    and temp_file.startswith(f"{AIRFLOW_DEPLOY_PATH}dags/sql")
                ):
                    continue

                invalid, msg = is_invalid_file_type(temp_file, dir_folder, type_files)
                if invalid:
                    save_log(msg, with_exit=True)

        save_log(f"Проверка типов файлов в директории {dir_folder} завершена успешно")
    except Exception as e:
        save_log(f"Ошибка при проверке типов файлов в директории {dir_folder}: {str(e)}", with_exit=True)




def connect_write(host: str) -> None:
    """
    Проверяет доступность хоста с помощью команды ping.
    В случае недоступности хоста добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста для проверки доступности.
    """
    save_log(f"Запуск проверки доступности хоста: {host}")
    data_connect_write = subprocess.Popen(
        f"ping -c 1 {host} ", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if "Name or service not known" in data_connect_write.stderr.read().decode("utf-8"):
        save_log(f"Ошибка !!! Проверьте доступ к хосту {host} \n", with_exit=True)


def check_free_space(data_host: str) -> None:
    """
    Проверяет свободное место на разделе /app удалённого хоста и предупреждает,
    если после деплоя занятое место превысит критический порог.

    Параметры:
        data_host (str): Имя или адрес хоста для проверки.
    """
    save_log(f"Запуск проверки свободного места на разделе /app хоста: {data_host}")
    try:
        result_command = run_command_with_log(
            f"{SSH_USER}@{data_host} df /app  --output=avail,used | tail -n +2 | tr -d '%'",
            f"Проверка свободного места на /app хоста {data_host}"
        )
        save_log(f"Результат команды df: '{result_command}'")
        parts = result_command.split()
        if len(parts) < 2:
            save_log(f"Ошибка: некорректный вывод df: '{result_command}'")
            return

        free_disk_space = int(parts[0])
        used = int(parts[1])
        total = free_disk_space + used
        one_percent = total / 100
        used_percent = int((used + size_airflow_deploy) / one_percent)
        save_log(f"Свободно: {free_disk_space}, Использовано: {used}, Всего: {total}, % после деплоя: {used_percent}")
        if used_percent > CRITICAL_PERCENT:
            warning_msg = "Предупреждение !!! После добавления файлов на локальный хост колличество занятого места превысит 80% в каталоге /app"
            if CONFIGURATION == "cluster":
                warning_msg = f"Предупреждение !!! После добавления файлов на хост {data_host} колличество занятого места превысит 80% в каталоге /app"
                
            save_log(warning_msg, info_level=True)
    except Exception as e:
        save_log(f"Ошибка при проверке свободного места на хосте {data_host}: {str(e)}", with_exit=True)

def md5(fname: str) -> str:
    """
    Вычисляет MD5-хеш для указанного файла.

    Параметры:
        fname (str): Путь к файлу для вычисления хеша.

    Возвращает:
        str: Строка с MD5-хешем файла.
    """
    save_log(f"Вычисление MD5-хеша для файла: {fname}")
    try:
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        save_log(f"Ошибка при вычислении MD5-хеша для файла {fname}: {str(e)}", with_exit=True)
        return ""


def path_sum_files() -> dict[str, str]:
    """
    Заполняет глобальный словарь PATH_SUM md5-хешами всех файлов во всех прикладных директориях.

    Для каждого файла в директориях из list_folders вычисляет md5-хеш и сохраняет его в PATH_SUM.
    Глобальная переменная:
        PATH_SUM (dict): Ключ — путь к файлу, значение — md5-хеш.
    """
    save_log("Запуск заполнения словаря PATH_SUM md5-хешами всех файлов во всех прикладных директориях")
    try:
        path_sum = {}
        for list_folder in list_folders:
            for root, _, files in os.walk(f"{AIRFLOW_DEPLOY_PATH}{list_folder}"):
                for file in files:
                    path_sum[f"{root}/{file}"] = md5(f"{root}/{file}")

        return path_sum
    except Exception as e:
        save_log(f"Ошибка при заполнении словаря PATH_SUM: {str(e)}", with_exit=True)

def rsync_host(paths: list[str], hosts: list[str]) -> bool:
    """
    Сравнивает md5-хэши между источником и целями.

    :param paths: Список относительных путей к файлам или директориям (от AIRFLOW_DEPLOY_PATH).
    :param hosts: Список хостов для проверки.
    :return: True если все хэши совпадают на всех хостах, иначе False.
    """
    for path in paths:
        src_full = os.path.join(AIRFLOW_DEPLOY_PATH, path)
        src_hashes = {}
        is_dir = os.path.isdir(src_full)

        if is_dir:
            for root, _, files in os.walk(src_full):
                for file in files:
                    rel = os.path.relpath(os.path.join(root, file), AIRFLOW_DEPLOY_PATH)
                    src_hashes[rel] = md5(os.path.join(root, file))
        else:
            rel = path
            src_hashes[rel] = md5(src_full)

        all_ok = True
        for host in hosts:
            dst_hashes = {}
            if is_dir:
                find_cmd = f"ssh airflow_deploy@{host} 'find {AIRFLOW_PATH}{path} -type f'"
                proc = subprocess.Popen(find_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _ = proc.communicate()
                files_list = out.decode("utf-8").strip().split("\n")
                for dst_file in files_list:
                    if dst_file:
                        rel = os.path.relpath(dst_file, AIRFLOW_PATH)
                        md5_cmd = f"ssh airflow_deploy@{host} 'md5sum {dst_file}'"
                        p = subprocess.Popen(md5_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        md5_out, _ = p.communicate()
                        md5_line = md5_out.decode("utf-8").strip().split()
                        if md5_line:
                            dst_hashes[rel] = md5_line[0]
            else:
                dst_file = f"{AIRFLOW_PATH}{path}"
                md5_cmd = f"ssh airflow_deploy@{host} 'md5sum {dst_file}'"
                p = subprocess.Popen(md5_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                md5_out, _ = p.communicate()
                md5_line = md5_out.decode("utf-8").strip().split()
                if md5_line:
                    dst_hashes[path] = md5_line[0]

            for rel, src_md5 in src_hashes.items():
                dst_md5 = dst_hashes.get(rel)
                if not dst_md5 or src_md5 != dst_md5:
                    save_log(f"Несовпадение md5 для {rel} на {host}: src={src_md5}, dst={dst_md5}", info_level=True)
                    all_ok = False
            for rel in dst_hashes:
                if rel not in src_hashes:
                    save_log(f"Лишний файл {rel} на {host}", info_level=True)
                    all_ok = False
        return all_ok

# def rsync_host(host_name: str, path_sum: dict[str, str]) -> None:
#     """
#     Выполняет синхронизацию директорий с помощью rsync на указанный хост.
#     Для каждой директории из list_folders копирует содержимое на удалённый сервер airflow.
#     Для файлов сохраняет информацию о копировании и md5-хеше в лог.

#     Параметры:
#         hostname (str): Имя или адрес хоста для синхронизации.
#         path_sum (dict[str, str]): Словарь с md5-хешами файлов для логирования информации о копировании.
#     """
#     save_log(f"Запуск rsync_host для хоста: {host_name}")
#     for folder in list_folders:
#         try:
#             if folder in ("keytab", "keys"):
#                 rsync_cmd = f"{RSYNC_CHECKSUM_STRING} {CHOWN_STRING} {CHMOD_WITHOUT_FU_FO_STRING} {AIRFLOW_DEPLOY_PATH}{folder} airflow_deploy@{host_name}:{AIRFLOW_PATH}"
#             else:
#                 rsync_cmd = f"{RSYNC_CHECKSUM_STRING} {CHOWN_STRING} {CHMOD_FG_FU_FO_STRING} {AIRFLOW_DEPLOY_PATH}{folder} airflow_deploy@{host_name}:{AIRFLOW_PATH}"
#             result = run_command_with_log(rsync_cmd, f"Запуск rsync для {folder} на хосте {host_name}")
#             save_log(f"Результат rsync для директории {folder} на хосте {host_name}: {result.strip()}")
#         except Exception as e:
#             save_log(f"Ошибка выполнения rsync для директории {folder} на хосте {host_name}: {str(e)}", with_exit=True)
#         try:
#             for root, _, files in os.walk(f"{AIRFLOW_DEPLOY_PATH}{folder}"):
#                 for file in files:
#                     temp_file = f"{root}/{file}"
#                     temp_file_airflow = f"{AIRFLOW_PATH}" + f"{root}/{file}"[20:]
#                     md5_sum = path_sum[temp_file]
#                     save_log(f"Копирование файла: Source: {temp_file} Destination: {host_name}@{temp_file_airflow} Md5hash: {md5_sum}")
#         except Exception as e:
#             save_log(f"Ошибка при логировании информации о файлах для директории {folder} на хосте {host_name}: {str(e)}", with_exit=True)


def check_rsync_host() -> None:
    """
    Проверяет возможность синхронизации директорий с помощью rsync на указанный хост.
    """
    hosts = ['127.0.0.1']
    if CONFIGURATION == "cluster":
        hosts = all_hosts
    
    for host_name in hosts:
        save_log(f"Запуск проверки запуска rsync на хосте: {host_name}")
        for folder in list_folders:
            try:
                if folder in ("keytab", "keys"):
                    chmod_string = CHMOD_WITHOUT_FU_FO_STRING
                else:
                    chmod_string = CHMOD_FG_FU_FO_STRING
                command = f"{RSYNC_DRY_RUN} {CHOWN_STRING} {chmod_string} {AIRFLOW_DEPLOY_PATH}{folder} airflow_deploy@{host_name}:{AIRFLOW_PATH}"
                # save_log(f"Запуск dry-run rsync для директории: {folder} на хосте {host_name}")
                run_command_with_log(command, f"Проверка dry-run rsync для {folder} на хосте {host_name}", rsync_error=True)
                save_log(f"Dry-run rsync для директории {folder} на хосте {host_name} выполнен успешно")
            except Exception as e:
                save_log(f"Ошибка при dry-run rsync для директории {folder} на хосте {host_name}: {str(e)}", with_exit=True)

    # print("0")
    # sys.exit(0)

def host_checks(hostname: str) -> None:
    """
    Выполняет все проверки для одного хоста:
    - Проверка доступности (ping)
    - Проверка свободного места
    - Проверка прав доступа
    - Проверка групп и владельцев
    - Проверка возможности синхронизации через rsync

    Параметры:
        hostname (str): Имя или адрес хоста для проверки.
    """
    connect_write(hostname)
    check_free_space(hostname)
    check_permissions(hostname)
    check_groups_users(hostname)

def parse_args(script_args: list[str]) -> bool:
    """
    Парсит аргументы командной строки для определения типа пути (файл или директория), а также для извлечения ключей.

    :param script_args: Аргументы командной строки (sys.argv[2:]), первый элемент — путь относительно AIRFLOW_DEPLOY_PATH.
    :return: True если все хэши совпали, иначе False.
    """
    save_log(f"Парсинг аргументов для определения типа пути и ключей: {script_args}")
    keys = []
    paths = []
    for arg in script_args[1:]:
        if arg.startswith('-'):
            keys.append(arg)
        else:
            paths.append(arg)

    return paths, keys


def main() -> None:
    """
    Основная функция скрипта, выполняющая синхронизацию директорий и проверку параметров.
    В зависимости от конфигурации (one-way или cluster) выполняет соответствующие действия.
    """
    for check_folder, check_extension in ext_map.items():
        check_type_file(check_folder, check_extension)

    paths, keys = parse_args(sys.argv)
    key_allowed = is_key_combination_allowed(keys)
    if not key_allowed:
        save_log(f"Ошибка: недопустимая комбинация ключей: {keys}", with_exit=True)
    
    for path in paths:
        dir_allowed = is_dir_allowed(path)
        if not dir_allowed:
            save_log(f"Ошибка: недопустимый путь для синхронизации: {path}", with_exit=True)

    param_run_script(keys)
    hosts = []

    if CONFIGURATION == "one-way":
        host_checks(current_hostname)
        hosts = [current_hostname]

        # check_files_in_dirs()
        # check_param_run(keys, paths)
        # rsync_host(paths, [current_hostname])
        # save_log("Синхронизация завершена успешно")


    if CONFIGURATION == "cluster":
        # processes = [Process(target=host_checks, args=(hostname,)) for hostname in all_hosts]
        # for p in processes:
        #     p.start()
        # for p in processes:
        #     p.join()
        for hostname in all_hosts:
            host_checks(hostname)
        hosts = all_hosts

    check_files_in_dirs()
    check_param_run(keys, paths)

    rsync_host(paths, hosts)
    save_log(f"Синхронизация завершена успешно для {hosts} хостов")

    print("0")
    sys.exit(0)

if __name__ == "__main__":
    main()
