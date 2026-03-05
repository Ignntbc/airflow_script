import shutil
import os
import hashlib
import json
import sys
import subprocess
import socket
import logging
from datetime import datetime
from typing import List, Optional, Literal



CRITICAL_DISK_USAGE_PERCENT = 80

ALL_KEYS = ["--delete", "--file", "--dir", "-c", "-h", "--dry-run", "-v", "", "--exclude"]


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
CHMOD_WITHOUT_FU_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=rx,Fu=rw,Fg=,Fo=" 
# CHMOD_WITHOUT_DO_FU_DG_FO_STRING = "--chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo="

VERBOSE = "-v" in sys.argv

LOCAL_TEST = True
list_folders = ["dags","csv", "jar", "keys", "keytab", "scripts", "user_data"]


def is_dir_allowed(path: str) -> bool:
    """
    Проверяет, разрешён ли путь согласно list_folders.
    Путь разрешён, если он начинается с одного из элементов list_folders.
    """
    for allowed_prefix in list_folders:
        if path.startswith(allowed_prefix):
            return True
    return False


def is_key_combination_allowed(keys: List[str]) -> bool:
    """
    Проверяет, что из списка ключей только один из конфликтующих может быть выбран одновременно.
    Конфликтующие ключи: "--file", "--delete", "--dir", "-c". Любой из них не может быть с другим.
    Возвращает True, если комбинация разрешена, иначе False.
    """
    conflict_keys = {"--file", "--delete", "--dir", "-c"}
    found = [k for k in keys if k in conflict_keys]
    # Если найдено больше одного конфликтного ключа — запрещено
    if len(found) > 1:
        return False
    if ("--delete" in found or "--file" in found) and "--exclude" in keys:
        return False

    # Остальные ключи разрешены в любых сочетаниях
    return True


if LOCAL_TEST:
    description_path = "description.json"
else:
    description_path = "/app/app/etc/description.json"

with open(description_path, "r", encoding="utf-8") as file_description:
    data_description = json.load(file_description)

schedulers = data_description["software"]["app"]["nodes"]["airflow_scheduler"]
webs = data_description["software"]["app"]["nodes"]["airflow_web"]
workers = data_description["software"]["app"]["nodes"]["airflow_workers"]
all_hosts = schedulers + webs + workers# + ["127.0.0.1"]

EXECUTOR_TYPE = data_description["software"]["app"]["executor"]


def get_hosts() -> list:
    """
    Возвращает список хостов в зависимости от конфигурации.
    Для one-way — только localhost, иначе all_hosts.
    """
    if CONFIGURATION == "one-way":
        return ["127.0.0.1"]
    return all_hosts

def log_exceptions(log_message: str, context_arg_name: str| None = None):
    """
    Декоратор для обработки исключений с информативным логированием.
    context_arg_name (str): имя аргумента функции, который будет включён в лог при ошибке.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Получаем значение аргумента для контекста
                save_log(message=log_message, with_exit=False)
                context_value = None
                if context_arg_name and context_arg_name in func.__code__.co_varnames:
                    arg_index = func.__code__.co_varnames.index(context_arg_name)
                    if arg_index < len(args):
                        context_value = args[arg_index]
                    else:
                        context_value = kwargs.get(context_arg_name)
                    save_log(f"Ошибка в {func.__name__} для {context_arg_name}={context_value}: {str(e)}", with_exit=True)
                else:
                    save_log(f"Ошибка в {func.__name__}: {str(e)}", with_exit=True)
                return None
        return wrapper
    return decorator

def check_configuratioon(executor_type: str) -> str:
    """
    Определяет тип конфигурации Airflow по типу executor.

    Аргументы:
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
LOG_FILE_1 = os.path.join(LOG_DIR, 'deploy.log')
LOG_FILE_2 = os.path.join(LOG_DIR, 'deploy_2.log')
LOG_FILE_3 = os.path.join(LOG_DIR, 'deploy_3.log')

LOG_MAX_SIZE = 10 * 10 * 1024  # 10 МБ 


def file_size(path):
    """
    Возвращает размер файла в байтах по указанному пути.
    Если файл не существует, возвращает 0.
    :param path: Путь к файлу
    :return: Размер файла в байтах или 0, если файл не найден
    """
    return os.path.getsize(path) if os.path.exists(path) else 0

@log_exceptions(log_message="Ошибка при ротации логов")
def rotate_logs() -> None:
    """
    Выполняет ротацию лог-файлов. Если основной лог-файл превышает максимальный размер,
    старые логи сдвигаются, а самый старый удаляется. Используется для ограничения размера логов.
    """
    log_files = [LOG_FILE_1, LOG_FILE_2, LOG_FILE_3]

    if os.path.exists(log_files[0]) and file_size(log_files[0]) >= LOG_MAX_SIZE:
        if os.path.exists(log_files[-1]):
            os.remove(log_files[-1])
        for i in range(len(log_files) - 1, 0, -1):
            if os.path.exists(log_files[i - 1]):
                os.rename(log_files[i - 1], log_files[i])
    



def setup_logger() -> logging.Logger:
    """
    Создаёт и настраивает экземпляр логгера.

    Логгер пишет сообщения в консоль (stdout) и в файл, расположенный по пути LOG_FILE.
    Уровень логирования определяется переменной VERBOSE (DEBUG или INFO).

    Возвращает:
        logging.Logger: Настроенный экземпляр логгера.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    rotate_logs()

    level = logging.DEBUG if VERBOSE else logging.INFO
    logger_obj = logging.getLogger('airflow_sync')
    logger_obj.setLevel(level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    # stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger_obj.addHandler(sh)

    # file handler (всегда пишем в LOG_FILE_1)
    fh = logging.FileHandler(LOG_FILE_1, mode='a', encoding='utf-8')
    fh.setFormatter(formatter)
    logger_obj.addHandler(fh)

    return logger_obj

logger = setup_logger()

def save_log(message: str,
            with_exit=False,
            info_level=False) -> None:
    """
    Логирует сообщение через стандартный logger. Если with_exit=True, пишет как ошибку и завершает выполнение.

    Аргументы:
    message: Сообщение для лога.
    with_exit: Если True, завершает выполнение скрипта с кодом 1.
    info_level: Если True, логирует сообщение на уровне INFO.
    """
    if with_exit:
        logger.error(message)
        sys.exit(1)
    else:
        if VERBOSE:
            logger.debug(message)
        if info_level:
            logger.info(message)

@log_exceptions(log_message="Ошибка при определении имени пользователя")
def check_real_user() -> str|None:
    """
    Определяет имя пользователя, под которым запущен скрипт (с учётом sudo).

    Возвращает:
        str: Имя пользователя, под которым выполняется скрипт, либо None при ошибке.
    """
    save_log("Запуск определения имени пользователя (check_real_user)")
    with subprocess.Popen(
        "whoami", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ) as request_name:
        stdout_output = request_name.stdout.read().decode("utf-8") if request_name.stdout else ""
        stderr_output = request_name.stderr.read().decode("utf-8") if request_name.stderr else ""

    if stdout_output:
        try:
            real_server_name = stdout_output.strip()
            save_log(f"Пользователь определён по stdout: {real_server_name}")
        except (ValueError, IndexError) as e:
            save_log(f"Ошибка разбора stdout при определении пользователя: {e}", with_exit=True)
            real_server_name = None
    else:
        try:
            real_server_name = stderr_output.strip()
            save_log(f"Пользователь определён по stderr: {real_server_name}", info_level=True)
        except (ValueError, IndexError) as e:
            save_log(f"Ошибка разбора stderr при определении пользователя: {e}", with_exit=True)
            real_server_name = None

    if real_server_name is None:
        save_log("Не удалось определить имя пользователя", with_exit=True)

    return real_server_name

real_name = check_real_user()

current_hostname = socket.gethostname()



def param_run_script(keys: list[str]) -> None:
    """
    Логирует запуск скрипта, дату и пользователя.
    Проверяет ключи, выводит справку при -h, завершает выполнение при неизвестном ключе.
    Аргументы:
        keys (list[str]): Список ключей командной строки.
    Поведение:
        - Логирует дату и пользователя.
        - Проверяет каждый ключ: если неизвестный — завершает выполнение.
        - Если -h — выводит справку и завершает выполнение.
    """
    current_datetime = datetime.now()
    save_log(f"{current_datetime} {real_name} Запуск скрипта с ключами: {keys}")
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

    Аргументы:
        command (str): Команда для выполнения.
        log_message (str): Сообщение для записи в лог.
        rsync_error (bool): Проверять ли результат на 'rsync error' (по умолчанию False).
        info_level (bool): Записывать ли сообщение на уровне info (по умолчанию False).
    Возвращает:
        str: Результат выполнения команды.
    """
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    stdout_decoded = stdout.decode("utf-8").strip()
    stderr_decoded = stderr.decode("utf-8").strip()

    if VERBOSE:
        logger.debug(log_message)

    if info_level:
        logger.info(log_message)

    if stdout_decoded:
        logger.info("stdout: %s", stdout_decoded)
    if stderr_decoded:
        logger.error("stderr: %s", stderr_decoded)
        sys.exit(1)

    if rsync_error and "rsync error" in stderr_decoded:
        save_log("rsync error detected", with_exit=True)

    return stdout_decoded

@log_exceptions(log_message="Ошибка при проверке прав доступа", context_arg_name="host")
def check_permissions(host: str) -> None:
    """
    Проверяет права доступа к файлам и директориям на целевых хостах.
    Если права некорректны — логирует ошибку и завершает выполнение скрипта.

    Аргументы:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
    """
    save_log(f"Запуск проверки прав доступа на хосте: {host}")
    for folder in list_folders:
        dir_path = f"{AIRFLOW_PATH}{folder}"
        save_log(f"Проверка группы для директории: {dir_path} на хосте {host}")
        check_permission_type(host, dir_path, "group", f"Ошибка !!! Некорректная группа на хосте {host}")
        save_log(f"Проверка владельца для директории: {dir_path} на хосте {host}")
        check_permission_type(host, dir_path, "user", f"Ошибка !!! Некорректный владелец на хосте {host}")
    save_log(f"Результат проверки прав доступа на хосте {host}: завершено без ошибок")


@log_exceptions(log_message="Ошибка при проверке прав доступа", context_arg_name="host")
def check_permission_dir_and_files(find_cmd: str,
                    error_prefix: str,
                    host: str) -> None:
    """
    Выполняет проверку прав доступа к файлам/директориям на целевом хосте с помощью команды find.
    Для каждого найденного объекта с некорректными правами вызывает ls -l и логирует ошибку, завершает выполнение скрипта.

    Аргументы:
        find_cmd (str): Команда find для поиска файлов/директорий с некорректными правами.
        error_prefix (str): Префикс сообщения об ошибке (например, "Ошибка !!! Некорректная группа на хосте").
        host (str): Имя или адрес хоста, на котором выполняется проверка.
    """
    result_str = run_command_with_log(find_cmd, f"Проверка разрешённых директорий: {find_cmd} на хосте {host}")
    result = result_str.split("\n")
    for item in result:
        if item.strip():
            perm_error = run_command_with_log(f"{SSH_USER}@{host} ls -l {item}", f"Проверка прав доступа: {item} на хосте {host}")
            save_log(f"{error_prefix} {host} {perm_error}", with_exit=True)
    save_log(f"Результат проверки разрешённых директорий на хосте {host}: завершено без исключений")


@log_exceptions(log_message="Ошибка при удалении файлов/директорий")
def check_param_delete_key(
    paths: list[str]
) -> None:
    """
    Обрабатывает удаление файлов/директорий по ключу --delete.
    Проверяет существование, удаляет локально или по ssh, логирует действия и завершает выполнение.

    Аргументы:
        paths (list[str]): Список путей к файлам/директориям для удаления (относительно AIRFLOW_PATH).
    """
    save_log(f"Запуск удаления файлов/директорий по ключу --delete: {paths}")
    current_datetime = datetime.now()
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
                save_log(f"{current_datetime} {real_name} Ошибка при удалении {path}: {str(e)}\n\n", with_exit=True)
        else:
            for host in all_hosts:
                try:
                    run_command_with_log(f"{SSH_USER}@{host} rm -rf {path}", f"Удаление файла/директории: {path} на хосте {host}")
                    save_log(f"Удалён файл/директория: {path} на хосте {host}", info_level=True)
                except Exception as e:
                    save_log(f"{current_datetime} {real_name} Ошибка при удалении {path} на хосте {host}: {str(e)}\n\n", with_exit=True)

    save_log("Удаление файлов/директорий завершено успешно", info_level=True)
    sys.exit(0)


def get_chmod_string(path: str) -> str:
    """
    Возвращает строку chmod для rsync в зависимости от типа пути.
    Для keytab/keys — CHMOD_WITHOUT_FU_FO_STRING, иначе CHMOD_FG_FU_FO_STRING.
    """
    if path.startswith("keytab") or path.startswith("keys"):
        return CHMOD_WITHOUT_FU_FO_STRING
    return CHMOD_FG_FU_FO_STRING


@log_exceptions(log_message="Ошибка при деплое файлов/директорий")
def check_param_file_key(
    paths: list[str]
    ) -> None:
    """
    Универсальная функция деплоя файла/директории на все хосты для one-way и cluster.

    Аргументы:
        paths (list[str]): Список путей к файлам/директориям для деплоя (относительно AIRFLOW_DEPLOY_PATH).
    """
    save_log(f"Запуск деплоя файлов: {paths}")
    for path in paths:
        airflow_deploy_dir_path = f"{AIRFLOW_DEPLOY_PATH}{path}"
        temp_folder_path = path.rpartition("/")[0]

        save_log(f"Проверка наличия файла для деплоя: {airflow_deploy_dir_path}")
        if not os.path.exists(airflow_deploy_dir_path):
            save_log(f"Файл не найден для деплоя: {airflow_deploy_dir_path}", with_exit=True)
        
        chmod_string = get_chmod_string(path)
        hosts = get_hosts()
        for host in hosts:
            host_prefix = f"airflow_deploy@{host}:"
            save_log(f"Запуск rsync для деплоя файла: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
            try:
                if path.count("/") > 1:
                    run_command_with_log(
                        f'{RSYNC_CHECKSUM_STRING} {AIRFLOW_PATH}{temp_folder_path} && rsync" {CHOWN_STRING} {chmod_string} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{path}',
                        f"Деплой файла:  {AIRFLOW_PATH}{path} на хосте {host}",
                    )
                    save_log(f"Файл успешно скопирован: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
                else:
                    run_command_with_log(
                        f"{RSYNC_CHECKSUM} {CHOWN_STRING} {chmod_string} {airflow_deploy_dir_path} {host_prefix}{AIRFLOW_PATH}{path}",
                        f"Деплой файла:  {AIRFLOW_PATH}{path} на хосте {host}",
                    )
                    save_log(f"Файл успешно скопирован: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
            except Exception as e:
                save_log(f"Ошибка копирования файла {airflow_deploy_dir_path} на хост {host}: {str(e)}", with_exit=True)

    save_log("Результат деплоя файлов: успешно", info_level=True)


@log_exceptions(log_message="Ошибка при удалении содержимого директории", context_arg_name="host_name")
def remote_delete_items(elem: str, host_name: str, exclude_exts: Optional[list[str]] = None) -> None:
    """
    Удаляет все элементы в целевой директории на удалённом хосте.
    Для dags пропускает __pycache__, для остальных удаляет все элементы.

    Аргументы:
        elem (str): Имя папки для очистки (например, "dags", "keys" и т.д.).
        host_name (str): Имя или IP-адрес удалённого хоста, на котором будет производиться очистка.
    """
    save_log(f"Запуск удаления содержимого директории: {AIRFLOW_PATH}{elem} на хосте {host_name}", info_level=True)
    items_str = run_command_with_log(f"{SSH_USER}@{host_name} ls -a {AIRFLOW_PATH}{elem}/", f"Получение списка элементов в {AIRFLOW_PATH}{elem} на хосте {host_name}")
    items = [x for x in items_str.split("\n") if x not in {".", "..", ""}]
    if elem == "dags":
        for item in items:
            ext = os.path.splitext(item)[1]
            if "__pycache__" in item or ".pyc" in item:
                continue
            if exclude_exts and ext in exclude_exts:
                continue
            result = run_command_with_log(f"{SSH_USER}@{host_name} rm -rfv {AIRFLOW_PATH}dags/{item}", f"Удаление: {AIRFLOW_PATH}dags/{item} на хосте {host_name}", info_level=True)
            save_log(f"Результат удаления {AIRFLOW_PATH}dags/{item} на хосте {host_name}: {result.strip()}", info_level=True)
        result_sql = run_command_with_log(f"{SSH_USER}@{host_name} rm -rfv {AIRFLOW_PATH}dags/sql/*", f"Удаление SQL-файлов в директории dags/sql на хосте {host_name}", info_level=True)
        save_log(f"Результат удаления SQL-файлов на хосте {host_name}: {result_sql.strip()}", info_level=True)
    else:
        for item in items:
            result = run_command_with_log(f"{SSH_USER}@{host_name} rm -rf {AIRFLOW_PATH}{elem}/{item}", f"Удаление: {AIRFLOW_PATH}{elem}/{item} на хосте {host_name}", info_level=True)
            save_log(f"Результат удаления {AIRFLOW_PATH}{elem}/{item} на хосте {host_name}: {result.strip()}", info_level=True)



@log_exceptions(log_message="Ошибка при очистке целевых папок")
def remove_destination_folders(exclude_exts: Optional[list[str]] = None) -> None:
    """
    Удаляет содержимое целевых папок на удалённом сервере airflow_deploy через ssh.
    Для папки dags пропускает каталоги __pycache__, для остальных удаляет все элементы.
    """
    save_log("Запуск очистки целевых папок на удалённых хостах airflow_deploy через ssh", info_level=True)
    hosts = get_hosts()
    for host_name in hosts:
        save_log(f"Очистка на хосте: {host_name}", info_level=True)
        for elem in list_folders:
            remote_delete_items(elem, host_name, exclude_exts)

    save_log("Очистка целевых папок на удалённых хостах завершена успешно", info_level=True)

    sys.exit(0)


def check_param_h_key() -> None:
    """
    Блок справки по ключу -h.
    """
    help_text = (
        "\033[32m{}\033[0m".format("\nДОСТУПНЫЕ КЛЮЧИ: [-c], [-h], [--delete], [--file], [--dir]\n\n") +
        "\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА БЕЗ ПАРАМЕТРОВ:") + "\n"
        f"    Синхронизация содержимого директорий  {AIRFLOW_DEPLOY_PATH}dags,  {AIRFLOW_DEPLOY_PATH}keytab,  {AIRFLOW_DEPLOY_PATH}scripts,\n"
        f" {AIRFLOW_DEPLOY_PATH}keys,  {AIRFLOW_DEPLOY_PATH}csv,  {AIRFLOW_DEPLOY_PATH}jar,  {AIRFLOW_DEPLOY_PATH}user_data с соответствующими директориями в /app/airflow\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh\n\n") +
        "\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА С КЛЮЧОМ -c:") + "\n"
        f"    Производится синхронизация директорий назначения с заменой ( {AIRFLOW_PATH}dags,  {AIRFLOW_PATH}keytab,  {AIRFLOW_PATH}scripts,)\n"
        f" {AIRFLOW_PATH}keys,  {AIRFLOW_PATH}csv,  {AIRFLOW_PATH}jar,  {AIRFLOW_PATH}user_data) перед синхронизацией\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh -c\n\n") +
        "\033[32m{}\033[0m".format("Запуск скрипта с ключом -h:") + "\n"
        "    Вывод справки\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh -h\n\n") +
        "\033[32m{}\033[0m".format("Запуск скрипта с ключом --delete:") + "\n"
        f"    Удаление файла или директории(отсчет идет от  {AIRFLOW_DEPLOY_PATH})\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА (Удаление файла): sudo -u airflow_deploy ./airflow_sync_dags.sh --delete dags/test.py\n") +
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА (Удаление директории): sudo -u airflow_deploy ./airflow_sync_dags.sh --delete dags/test_dir \n\n") +
        "\033[32m{}\033[0m".format("Запуск скрипта с ключом --file:") + "\n"
        f"    Деплой указанного файла (отсчет идет от  {AIRFLOW_DEPLOY_PATH}) из  {AIRFLOW_DEPLOY_PATH} в  {AIRFLOW_PATH}\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА : sudo -u airflow_deploy ./airflow_sync_dags.sh --file dags/test.py\n\n") +
        "\033[32m{}\033[0m".format("Запуск скрипта с ключом --dir:") + "\n"
        f"    Деплой указанной директории (отсчет идет от  {AIRFLOW_DEPLOY_PATH}) из  {AIRFLOW_DEPLOY_PATH} в  {AIRFLOW_PATH}\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА : sudo -u airflow_deploy ./airflow_sync_dags.sh --dir dags/test_dir\n\n") +
        "\033[32m{}\033[0m".format("Запуск скрипта с ключом --dry-run:") + "\n"
        "    Выполняет пробный запуск (dry run) без фактической синхронизации файлов. Показывает, какие изменения будут произведены, но не вносит их. Полезно для проверки перед реальным деплоем.\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh --dry-run\n\n") +
        "\033[32m{}\033[0m".format("Запуск скрипта с ключом -v:") + "\n"
        "    Включает подробный (verbose) режим вывода. Скрипт будет выводить дополнительную отладочную информацию о выполняемых действиях и командах.\n"
        "\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags.sh -v\n\n")
    )
    print(help_text)
    sys.exit(0)


@log_exceptions(log_message="Ошибка при деплое директорий")
def check_param_dir_key(
    paths: list[str],
    exclude_exts: Optional[list[str]]
) -> None:
    """
    Универсальная функция деплоя директории на все хосты для one-way и cluster.

    Аргументы:
        paths (list[str]): Список путей к директориям для деплоя (относительно AIRFLOW_DEPLOY_PATH).
        exclude_exts (Optional[list[str]]): Список расширений файлов для исключения из обработки.

    Не возвращает значения. В случае ошибки завершает выполнение скрипта.
    """
    save_log(f"Запуск деплоя директорий: {paths}")
    current_datetime = datetime.now()
    for path in paths:
        temp_folder_path = path.rpartition("/")[0]
        airflow_deploy_dir_path = f"{AIRFLOW_DEPLOY_PATH}{path}"
        save_log(f"{current_datetime} {real_name} Проверка наличия директории для деплоя: {airflow_deploy_dir_path}")
        if not os.path.exists(airflow_deploy_dir_path):
            save_log(f"{current_datetime} {real_name} Директория не найдена {airflow_deploy_dir_path} \n\n",
                    with_exit=True)

        hosts = get_hosts()
        host_prefix = "airflow_deploy@{host}:"

        chmod_string = get_chmod_string(path)

        exclude_args = ""

        if exclude_exts:
            exclude_args = ' '.join([f'--exclude="*{ext}"' for ext in exclude_exts])

        for host in hosts:
            if path.count("/") > 1:
                rsync_command = (
                    f'rsync --checksum -rogp --rsync-path="mkdir -p {AIRFLOW_PATH}{temp_folder_path} && rsync" '
                    f'{exclude_args} {CHOWN_STRING} {chmod_string} {airflow_deploy_dir_path}/ '
                    f'{host_prefix.format(host=host)}{AIRFLOW_PATH}{path}'
                )
                run_command_with_log(
                    rsync_command,
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{path}\n\n",
                )
                save_log(f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Директория успешно скопирована: {airflow_deploy_dir_path}\n\n", info_level=True)
            else:
                run_command_with_log(
                    f"{RSYNC_CHECKSUM} {exclude_args} {CHOWN_STRING} {chmod_string} {airflow_deploy_dir_path}/ {host_prefix.format(host=host)}{AIRFLOW_PATH}{path}",
                    f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Добавлена директория:  {AIRFLOW_PATH}{path}\n\n",
                )
                save_log(f"{current_datetime} {real_name} {host if CONFIGURATION == 'cluster' else ''} Директория успешно скопирована: {airflow_deploy_dir_path}\n\n", info_level=True)


@log_exceptions(log_message="Ошибка при полной синхронизации папок", context_arg_name="folder")
def check_full_sync(exclude_exts: Optional[list[str]] = []) -> None:
    """
    Переносит все папки из list_folders с нужными chmod.
    Для keytab и keys используется CHMOD_WITHOUT_FU_FO_STRING,
    для остальных CHMOD_FG_FU_FO_STRING.
    """
    save_log("Запуск полной синхронизации всех папок из list_folders", info_level=True)
    hosts = get_hosts()
    exclude_args = ""
    if exclude_exts:
        exclude_args = ' '.join([f'--exclude="*{ext}"' for ext in exclude_exts])

    for folder in list_folders:
        airflow_deploy_dir_path = f"{AIRFLOW_DEPLOY_PATH}{folder}"
        chmod_string = get_chmod_string(folder)
        for host in hosts:
            host_prefix = f"airflow_deploy@{host}:"
            save_log(f"Синхронизация папки: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
            try:
                run_command_with_log(
                    f"{RSYNC_CHECKSUM} {exclude_args} {CHOWN_STRING} {chmod_string} {airflow_deploy_dir_path}/ {host_prefix}{AIRFLOW_PATH}{folder}",
                    f"Синхронизация папки: {AIRFLOW_PATH}{folder} на хосте {host}",
                )
                save_log(f"Папка успешно скопирована: {airflow_deploy_dir_path} на хосте {host}", info_level=True)
            except Exception as e:
                save_log(f"Ошибка копирования папки {airflow_deploy_dir_path} на хост {host}: {str(e)}", with_exit=True)
    save_log("Полная синхронизация всех папок завершена успешно", info_level=True)



@log_exceptions(log_message="Ошибка при обработке параметров командной строки")
def check_param_run(keys: list[str],
                    paths: list[str],
                    exclude_exts: Optional[list[str]] = []) -> None:
    """
    Обрабатывает параметры командной строки для управления синхронизацией и удалением файлов/директорий Airflow.
    Аргументы:
            keys (list[str]): Список ключей для определения действий (например, --delete, --file, --dir, -c, -h).
            paths (list[str]): Список путей к файлам/директориям для обработки (относительно AIRFLOW_DEPLOY_PATH).
            exclude_exts (list[str]): Список расширений файлов для исключения из обработки.
    В зависимости от переданных ключей:
        --delete: удаляет указанные файлы/директории на локальном или удалённых хостах.
        --file: деплоит указанные файлы.
        --dir: деплоит указанные директории.
        -c: очищает директории назначения.
        -h: выводит справку.
        "" (пустой ключ): выполняет полную синхронизацию всех папок из list_folders.
    В случае неизвестного ключа — пишет ошибку в лог и завершает выполнение.
    """
    key_func_map = {
        "--delete": lambda: check_param_delete_key(paths),
        "--file": lambda: check_param_file_key(paths),
        "--dir": lambda: check_param_dir_key(paths, exclude_exts),
        "-c":  lambda: remove_destination_folders(exclude_exts),
        "--dry-run": check_rsync_host,
        "": lambda: check_full_sync(exclude_exts),
    }
    if "--dry-run" in keys:
        check_rsync_host()
        keys.remove("--dry-run")
        
    for key in keys:
        func = key_func_map.get(key)
        if func:
            func()


@log_exceptions(log_message="Ошибка при проверке наличия файлов и директорий для переноса")
def check_files_in_dirs() -> None:
    """
    Проверяет наличие файлов и директорий для переноса в  {AIRFLOW_DEPLOY_PATH}*.
    Если данных нет — логирует ошибку и завершает выполнение скрипта.
    """
    save_log(f"Запуск проверки наличия файлов и директорий для переноса в {AIRFLOW_DEPLOY_PATH}")
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


@log_exceptions(log_message="Ошибка при проверке прав доступа", context_arg_name="host")
def check_permission_type(
    host: str,
    folder: str,
    check_type: str,
    error_msg: str
) -> None:
    """
    Проверяет корректность групп или владельцев файлов/директорий на целевых хостах.
    Если найдены некорректные группы или владельцы — логирует ошибку и завершает выполнение скрипта.
    Аргументы:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        folder (str): Путь к директории для проверки.
        check_type (str): Тип проверки - "group" для групп, "user" для владельцев.
        error_msg (str): Сообщение об ошибке для логирования.
    """
    save_log(f"Запуск проверки {check_type} для {folder} на хосте {host}")
    folder_name = os.path.basename(folder.rstrip('/'))
    if check_type == "group":
        cmd = f"{SSH_USER}@{host} find {folder} ! -group airflow"
        log_prefix = "Проверка группы:"
    else:
        cmd = f"{SSH_USER}@{host} find {folder} ! -user airflow ! -user airflow_deploy"
        log_prefix = "Проверка владельца:"

    save_log(f"{log_prefix} {cmd}")
    for_result = run_command_with_log(cmd, f"Проверка {check_type} на хосте {host} для {folder}").strip().split("\n") 
    for item in for_result:
        if len(item) > 2:
            perm_error = run_command_with_log(f"{SSH_USER}@{host} ls -l {item}", f"Ошибка при проверке группы или пользователя на хосте {host} для {item}")
            save_log(f"{error_msg} {item} {perm_error.strip()}", with_exit=True)

    save_log(f"Запуск проверки прав доступа для {folder} на хосте {host}")
    folder_name = os.path.basename(folder.rstrip('/'))
    if folder_name in ("keys", "keytab"):
        perm_cmd = f"{SSH_USER}@{host} find {folder} -type f ! -perm 0600"
    else:
        perm_cmd = f"{SSH_USER}@{host} find {folder} -type d ! -perm 0755 ! -perm 0775"
    
    perm_error_prefix = f"Ошибка !!! Некорректные права для {folder} на хосте {host}"

    save_log(f"Проверка прав доступа: {perm_cmd}")
    perm_result = os.popen(perm_cmd).read().split("\n")
    for item in perm_result:
        if len(item) > 2:
            perm_error = run_command_with_log(f"{SSH_USER}@{host} ls -l {item}", f"Ошибка при проверке прав на хосте {host} для {item}")
            save_log(f"{perm_error_prefix} {item} {perm_error.strip()}", with_exit=True)


@log_exceptions(log_message="Ошибка при проверке групп и владельцев на хосте", context_arg_name="host")
def check_groups_users(host: str) -> None:
    """
    Проверяет корректность групп и владельцев файлов/директорий на целевых хостах.
    Если найдены некорректные группы или владельцы — логирует ошибку и завершает выполнение скрипта.

    Аргументы:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
    """
    save_log(f"Запуск проверки групп и владельцев на хосте: {host}")
    for folder in list_folders:
        dir_path = f"{AIRFLOW_PATH}{folder}"
        if CONFIGURATION == "cluster":
            save_log(f"Проверка группы для директории: {dir_path} на хосте {host}")
            find_group_cmd = f"{SSH_USER}@{host} find {dir_path} ! -group airflow"
            check_permission_dir_and_files(find_group_cmd, "Ошибка !!! Некорректная группа на хосте", host)
            save_log(f"Проверка владельца для директории: {dir_path} на хосте {host}")
            find_user_cmd = f"{SSH_USER}@{host} find {dir_path} ! -user airflow ! -user airflow_deploy"
            check_permission_dir_and_files(find_user_cmd, "Ошибка !!! Некорректный владелец на хосте", host)
        else:
            save_log(f"Проверка группы для директории: {dir_path} на хосте {host}")
            find_group_cmd = f"find {dir_path} ! -group airflow_deploy ! -group airflow"
            check_permission_dir_and_files(find_group_cmd, "Ошибка !!! Некорректная группа", host)
            save_log(f"Проверка владельца для директории: {dir_path} на хосте {host}")
            find_user_cmd = f"find {dir_path} ! -user airflow ! -user airflow_deploy"
            check_permission_dir_and_files(find_user_cmd, "Ошибка !!! Некорректный владелец", host)
    save_log(f"Результат проверки групп и владельцев на хосте {host}: завершено без ошибок")


def connect_write(host: str) -> None:
    """
    Проверяет доступность хоста с помощью команды ping.
    Аргументы:
        host (str): Имя или адрес хоста для проверки доступности.
    """
    save_log(f"Запуск проверки доступности хоста: {host}")
    data_connect_write = subprocess.Popen(
        f"ping -c 1 {host} ", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if data_connect_write.stderr and "Name or service not known" in data_connect_write.stderr.read().decode("utf-8"):
        save_log(f"Ошибка !!! Проверьте доступ к хосту {host} \n", with_exit=True)

def get_stdout_from_cmd(cmd: str) -> str:
    """Выполняет shell-команду и возвращает stdout как строку (без лишних пробелов)."""
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    if result.returncode != 0:
        save_log(f"[get_stdout_from_cmd] Ошибка выполнения команды: {cmd}\nStderr: {result.stderr.strip()}", with_exit=True)
        raise RuntimeError(f"Ошибка выполнения команды: {cmd}\nStderr: {result.stderr.strip()}")
    return result.stdout.strip()

def get_freed_space_by_delete(full_paths: list[str], data_host: str):
    """
    Для списка путей full_paths считает, сколько места освободится на сервере data_host при удалении этих путей.
    Возвращает общий объем в MB.
    """
    freed_by_delete = 0
    for path in full_paths:
        rel_path = os.path.relpath(path, AIRFLOW_DEPLOY_PATH)
        remote_find_cmd = (
            SSH_USER + "@" + data_host +
            " 'find " + AIRFLOW_PATH + rel_path +
            " -type f -exec stat -c \"%s\" {} \\; 2>/dev/null'"
        )
        remote_out = get_stdout_from_cmd(remote_find_cmd)
        remote_size = 0
        for line in remote_out.splitlines():
            try:
                size = int(line.strip())
                remote_size += size
            except Exception:
                continue
        mb = remote_size / 1024 / 1024
        freed_by_delete += mb
        save_log(f"Удаление {path} освободит {mb:.3f} mb на сервере {data_host}", info_level=True)

def check_required_space_and_percent(full_paths: list[str], data_host: str, keys: list[str], exclude_exts: Optional[list[str]]) -> None:
    """
    Считает, сколько потребуется места после деплоя, и проверяет процент занятого места.
    Выводит соответствующие логи и ошибки.
    """
    used_deploy = 0
    local_files = {}
    for path in full_paths:
        for root, _, files in os.walk(path):
            for file in files:
                if exclude_exts and any(file.endswith(ext) for ext in exclude_exts):
                    continue
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, AIRFLOW_DEPLOY_PATH)
                try:
                    local_files[rel_path] = os.path.getsize(abs_path)
                except Exception:
                    pass

    remote_find_cmd = (
        SSH_USER + "@" + data_host +
        " 'find " + AIRFLOW_PATH +
        " -type f -exec stat -c \"%n %s\" {} \\; 2>/dev/null'"
    )
    remote_files = {}
    remote_out = get_stdout_from_cmd(remote_find_cmd)
    for line in remote_out.splitlines():
        try:
            rel, sz = line.strip().rsplit(' ', 1)
            rel = os.path.relpath(rel, AIRFLOW_PATH)
            remote_files[rel] = int(sz)
        except Exception:
            save_log(f"Ошибка при обработке строки с удалённого хоста {data_host}: {line}", info_level=True)
            continue

    if '-c' in keys:
        local_size = sum(local_files.values())
        remote_size = sum(remote_files.values())
        diff = local_size - remote_size
        if diff < 0:
            save_log(f"После деплоя освободится дополнительно {abs(diff) / 1024 / 1024:.3f} mb на сервере {data_host}", info_level=True)
        else:
            save_log(f"После деплоя потребуется дополнительно {diff / 1024 / 1024:.3f} mb на сервере {data_host}", info_level=True)
    else:
        for rel, lsz in local_files.items():
            rsz = remote_files.get(rel, 0)
            diff = lsz - rsz
            if diff > 0:
                used_deploy += diff

        mb = used_deploy / 1024 / 1024
        disk_info_cmd = f"ssh airflow_deploy@{data_host} 'df --output=size,used,avail /app/airflow | tail -1'"
        disk_info_out = get_stdout_from_cmd(disk_info_cmd)
        try:
            size_str, used_str, avail_str = disk_info_out.strip().split()
            size = int(size_str) * 1024 
            used = int(used_str) * 1024
            used_after = used + used_deploy
            percent_after = int(used_after / size * 100)
            if percent_after >= CRITICAL_DISK_USAGE_PERCENT:
                save_log(f"ОШИБКА: После деплоя будет занято {percent_after}% места на сервере {data_host}, превышен порог {CRITICAL_DISK_USAGE_PERCENT}%. Потребуется дополнительно {mb:.3f} MB. Деплой прерван.", with_exit=True)
        except Exception as e:
            save_log(f"Ошибка при получении информации о размере диска на сервере {data_host}: {disk_info_out} ({e})", with_exit=True)

        save_log(f"После деплоя потребуется дополнительно {mb:.3f} mb на сервере {data_host}", info_level=True)



@log_exceptions("Ошибка при проверке свободного места на хосте", "data_host")
def check_free_space(data_host: str,
                    paths: list[str],
                    keys: list[str],
                    exclude_exts: Optional[list[str]] = None,
                    action: Literal['push', 'delete'] = 'push') -> None:
    """
    Проверяет свободное место на разделе {{ app_dir.path }} удалённого хоста и предупреждает,
    если после деплоя занятое место превысит критический порог.

    Аргументы:
        data_host (str): Имя или адрес хоста для проверки.
        paths (list[str]): Список путей (файлы/папки) для анализа размера.
        exclude_exts (list[str]): Список расширений для исключения из проверки.
        action (Literal['push', 'delete']): Тип действия для оценки - 'push' для деплоя, 'delete' для удаления.
    """
    full_paths = [f"{AIRFLOW_DEPLOY_PATH}{path}" for path in paths]
    if action == 'delete':
        get_freed_space_by_delete(full_paths, data_host)
    else:
        check_required_space_and_percent(full_paths, data_host, keys, exclude_exts)


@log_exceptions("Ошибка при вычислении MD5-хеша для файла", "fname")
def md5(fname: str) -> str:
    """
    Вычисляет MD5-хеш для указанного файла.

    Аргументы:
        fname (str): Путь к файлу для вычисления хеша.
    Возвращает:
        str: Строка с MD5-хешем файла.
    """
    save_log(f"Вычисление MD5-хеша для файла: {fname}")
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


@log_exceptions("Ошибка при заполнении словаря PATH_SUM")
def path_sum_files() -> dict[str, str]:
    """
    Заполняет глобальный словарь PATH_SUM md5-хешами всех файлов во всех прикладных директориях.

    Для каждого файла в директориях из list_folders вычисляет md5-хеш и сохраняет его в PATH_SUM.
    Возвращает:
        dict[str, str]: Словарь, где ключ - полный путь к файлу, а значение - его MD5-хеш.
    """
    save_log("Запуск заполнения словаря PATH_SUM md5-хешами всех файлов во всех прикладных директориях")
    path_sum = {}
    for list_folder in list_folders:
        for root, _, files in os.walk(f"{AIRFLOW_DEPLOY_PATH}{list_folder}"):
            for file in files:
                path_sum[f"{root}/{file}"] = md5(f"{root}/{file}")

    return path_sum


def get_dir_md5_hashes(base_dir: str, root_dir: str, exclude_exts: Optional[list[str]] = []) -> dict:
    """
    Возвращает словарь md5-хэшей для всех файлов в директории root_dir относительно base_dir.
    :param base_dir: Базовая директория для относительных путей.
    :param root_dir: Директория, в которой искать файлы.
    :param exclude_exts: Список расширений файлов для исключения.
    :return: dict {относительный_путь: md5}
    """
    hashes = {}
    for root, _, files in os.walk(root_dir):
        for file in files:
            if exclude_exts and any(file.endswith(ext) for ext in exclude_exts):
                continue
            abs_path = os.path.join(root, file)
            rel = os.path.relpath(abs_path, base_dir)
            hashes[rel] = md5(abs_path)

    return hashes

def get_remote_md5_hashes(host: str, path: str, is_dir: bool) -> dict:
    """
    Получает md5-хэши файлов на удалённом хосте airflow_deploy@host для указанного пути.
    :param host: имя хоста
    :param path: относительный путь (от AIRFLOW_DEPLOY_PATH)
    :param is_dir: True если директория, False если файл
    :return: dict {относительный_путь: md5}
    """
    hashes = {}
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
                    hashes[rel] = md5_line[0]
    else:
        dst_file = f"{AIRFLOW_PATH}{path}"
        md5_cmd = f"ssh airflow_deploy@{host} 'md5sum {dst_file}'"
        p = subprocess.Popen(md5_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        md5_out, _ = p.communicate()
        md5_line = md5_out.decode("utf-8").strip().split()
        if md5_line:
            hashes[path] = md5_line[0]

    return hashes


@log_exceptions("Ошибка при проверке md5-хэшей между источником и целями")
def check_hashes(paths: list[str], hosts: list[str],
                 exclude_exts: Optional[list[str]] = []) -> bool:
    """
    Сравнивает md5-хэши между источником и целями.

    Аргументы:
        paths (list[str]): Список относительных путей к файлам или директориям (от AIRFLOW_DEPLOY_PATH).
        hosts (list[str]): Список хостов для проверки.
    Возвращает:
        bool: True если все хэши совпадают на всех хостах, иначе False.
    """
    save_log(f"Запуск проверки md5-хэшей для путей: {paths} на хостах: {hosts}")
    all_ok = True
    for path in paths:
        src_full = os.path.join(AIRFLOW_DEPLOY_PATH, path)
        src_hashes = {}
        is_dir = os.path.isdir(src_full)

        if is_dir:
            src_hashes = get_dir_md5_hashes(AIRFLOW_DEPLOY_PATH, src_full, exclude_exts)
        else:
            rel = path
            src_hashes[rel] = md5(src_full)

        
        for host in hosts:
            dst_hashes = get_remote_md5_hashes(host, path, is_dir)
            for rel, src_md5 in src_hashes.items():
                dst_md5 = dst_hashes.get(rel)
                if not dst_md5 or src_md5 != dst_md5:
                    save_log(f"Несовпадение md5 для {rel} на {host}: src={src_md5}, dst={dst_md5}", info_level=True)
                    all_ok = False
                    return all_ok
        
    return all_ok


def check_rsync_host() -> None:
    """
    Проверяет возможность синхронизации директорий с помощью rsync на указанный хост.
    """
    hosts = get_hosts()
    for host_name in hosts:
        save_log(f"Запуск проверки запуска rsync на хосте: {host_name}")
        for folder in list_folders:
            try:
                chmod_string = get_chmod_string(folder)
                command = f"{RSYNC_DRY_RUN} {CHOWN_STRING} {chmod_string} {AIRFLOW_DEPLOY_PATH}{folder} airflow_deploy@{host_name}:{AIRFLOW_PATH}"
                run_command_with_log(command, f"Проверка dry-run rsync для {folder} на хосте {host_name}", rsync_error=True)
                save_log(f"Dry-run rsync для директории {folder} на хосте {host_name} выполнен успешно", info_level=True)
            except Exception as e:
                save_log(f"Ошибка при dry-run rsync для директории {folder} на хосте {host_name}: {str(e)}", with_exit=True)


def host_checks(hostname: str, paths: list[str], keys: list[str], exclude_exts: Optional[list[str]] = None) -> None:
    """
    Выполняет все проверки для одного хоста:
    - Проверка доступности (ping)
    - Проверка свободного места
    - Проверка прав доступа
    - Проверка групп и владельцев

    Аргументы:
        hostname (str): Имя или адрес хоста для проверки.
        paths (list[str]): Список путей для проверки свободного места.
        keys (list[str]): Список ключей для определения действий (например, --delete).
        exclude_exts (list[str]): Список расширений для исключения из проверки.
    """
    action = 'push'
    if keys:
        if "--delete" in keys:
            action = 'delete'
    connect_write(hostname)
    check_free_space(hostname, paths, keys, exclude_exts, action=action)
    check_permissions(hostname)
    check_groups_users(hostname)


@log_exceptions("Ошибка при парсинге аргументов командной строки")
def parse_args(script_args: list[str]) -> tuple[list[str], list[str], list[str]]:
    """
    Парсит аргументы командной строки для определения типа пути (файл или директория), а также для извлечения ключей.
    Аргументы:
        script_args (list[str]): Аргументы командной строки (sys.argv[2:]), первый элемент — путь относительно AIRFLOW_DEPLOY_PATH.
    :return: Кортеж из двух списков: paths и keys.
        - paths (list[str]): Список путей к файлам или директориям для обработки (относительно AIRFLOW_DEPLOY_PATH).
        - keys (list[str]): Список ключей для определения действий (например, --delete, --file, --dir, -c, -h).
        - exclude_exts (list[str]): Список расширений для исключения (например, ['.log', '.tmp'])
    """
    save_log(f"Парсинг аргументов для определения типа пути и ключей: {script_args}")
    keys = []
    paths = []
    exclude_exts = []
    i = 1
    argc = len(script_args)
    while i < argc:
        arg = script_args[i]
        if arg == "--exclude":
            if i + 1 < argc and not script_args[i + 1].startswith('-'):
                exts = script_args[i + 1].split(",")
                for e in exts:
                    e = e.strip()
                    if e and not e.startswith('-'):
                        exclude_exts.append(e if e.startswith(".") else f".{e}")
                save_log(f"Добавлены расширения для исключения: {exclude_exts}", info_level=True)
                i += 2
            else:
                save_log("Ошибка: ключ --exclude требует аргумент со списком расширений через запятую", with_exit=True)
        elif arg.startswith('-'):
            keys.append(arg)
            i += 1
        else:
            paths.append(f"{arg}")
            i += 1
    
    if set(keys) <= {"-v", "--exclude"}:
        keys.append("")
        for folder in list_folders:
            paths.append(f"{folder}")

    
    elif keys == ["--dry-run"]:
        for folder in list_folders:
            paths.append(f"{folder}")

    return paths, keys, exclude_exts


def main() -> None:
    """
    Основная функция скрипта, выполняющая синхронизацию директорий и проверку параметров.
    В зависимости от конфигурации (one-way или cluster) выполняет соответствующие действия.
    """
    save_log("Начало работы скрипта", info_level=True)
    paths, keys, exclude_exts = parse_args(sys.argv)
    key_allowed = is_key_combination_allowed(keys)
    if not key_allowed:
        save_log(f"Ошибка: недопустимая комбинация ключей: {keys}", with_exit=True)
    
    for path in paths:
        dir_allowed = is_dir_allowed(path)
        if not dir_allowed:
            save_log(f"Ошибка: недопустимый путь для синхронизации: {path}", with_exit=True)
    
    # for check_folder, check_extension in ext_map.items():
    #     check_type_file(check_folder, check_extension)

    param_run_script(keys)
    check_files_in_dirs()
    hosts = get_hosts()
    if CONFIGURATION == "one-way":
        host_checks(current_hostname, paths, keys, exclude_exts)


    if CONFIGURATION == "cluster":
        # processes = [Process(target=host_checks, args=(hostname, paths)) for hostname in all_hosts]
        # for p in processes:
        #     p.start()
        # for p in processes:
        #     p.join()
        for hostname in all_hosts:
            host_checks(hostname, paths, keys, exclude_exts)


    check_param_run(keys, paths, exclude_exts)
    if any(key in keys for key in ["--dir", "--file", "-c", ""]):
        ok_status = check_hashes(paths, hosts, exclude_exts)
        if not ok_status:
            save_log("Ошибка: md5-хэши не совпали после синхронизации", with_exit=True)
    
    save_log(f"Синхронизация завершена успешно для {hosts} хостов", info_level=True)

    sys.exit(0)

if __name__ == "__main__":
    main()
