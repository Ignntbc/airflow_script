import shutil
import os
import hashlib
import json
import sys
import subprocess
import socket
from multiprocessing import Process, Queue
from datetime import datetime



CRITICAL_PERCENT = 80
CMD = ["id"]

ARGV_KEYS = ["-c", "-h", "--file", "--dir", "--skipped"]
FOLDER_EXTENSION = dict()
FOLDER_EXTENSION["/app/airflow_deploy/dags/"] = ["py", "json"]
FOLDER_EXTENSION["/app/airflow_deploy/dags/sql/"] = ["sql"]
FOLDER_EXTENSION["/app/airflow_deploy/csv/"] = ["csv"]
FOLDER_EXTENSION["/app/airflow_deploy/jar/"] = ["jar"]
FOLDER_EXTENSION["/app/airflow_deploy/keys/"] = ["pfx", "p12", "jks", "secret"]
FOLDER_EXTENSION["/app/airflow_deploy/keytab/"] = ["keytab"]
GLOBAL_LIST_ERROR = set()

result_queue = Queue()
ALL_ERROR = Queue()

END_WORK_SCRIPT_STRING = "\n****************************************** END WORK SCRIPT *******************************************\n\n"
END_ALL_ERRORS_STRING = "\n******************************************* END ALL ERORRS *******************************************\n\n"

size_airflow_deploy = int(os.popen("du -s app/airflow_deploy | cut -f1").read())
list_folders = ["csv", "dags", "jar", "keys", "keytab", "scripts", "user_data"]

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

def log_and_exit(message: str)-> None:
    """
    Логирует сообщение в файл и завершает выполнение скрипта с кодом 1.
    """
    with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as log_file:
        log_file.write(message)
    print("1")
    sys.exit(1)

def log_without_exit(message: str) -> None:
    """
    Логирует сообщение в файл без завершения выполнения скрипта.
    """
    with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as log_file:
        log_file.write(message)

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
        if elem == "dags":
            result_command_dag = list(
                os.popen(f"ssh airflow_deploy@{host_name} ls -a /app/airflow/{elem}/")
                .read()
                .split("\n")
            )
            result_command_dag.remove(".")
            result_command_dag.remove("..")
            result_command_dag.remove("")
            for elem_dags_dir in result_command_dag:
                if "__pycache__" in elem_dags_dir:
                    continue
                result_command_dag = os.popen(
                    f"ssh airflow_deploy@{host_name} rm -rfv /app/airflow/dags/{elem_dags_dir}"
                ).read()
                result_q.put(result_command_dag)
            result_command_dag = os.popen(
                f"ssh airflow_deploy@{host_name} rm -rfv /app/airflow/dags/sql/*"
            ).read()
            result_q.put(result_command_dag)
        else:
            # remote_list_files_folders = os.popen(f"ssh airflow_deploy@{host_name} ls -R /app/airflow/{elem}/").read()
            result_command = list(os.popen(f"ssh airflow_deploy@{host_name} ls -a /app/airflow/{elem}/").read().split("\n"))
            result_command.remove(".")
            result_command.remove("..")
            result_command.remove("")
            for elem_result_command in result_command:
                result_command = os.popen(f"ssh airflow_deploy@{host_name} rm -rf /app/airflow/{elem}/{elem_result_command}").read()
                result_q.put(result_command)


def param_run_script() -> None:
    """
    Записывает информацию о запуске скрипта в лог-файл /app/airflow_deploy/log/deploy.log.

    В лог добавляются:
        - отметка о запуске,
        - дата и время запуска,
        - имя пользователя,
        - параметр запуска (если передан -c, то отмечается, иначе пишется 'false key').
    """
    with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as run_script:
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

    if len(sys.argv) >= 2:
        if sys.argv[1] == "--delete":
            script_args = sys.argv[2:]
            for i_script_args in script_args:
                if not os.path.exists(f"/app/airflow/{i_script_args}"):
                    log_and_exit(f"{current_datetime} {real_name} Нет такого файла или директории /app/airflow/{i_script_args}\n\n")

            if CONFIGURATION == "one-way":
                for i_script_args in script_args:
                    if os.path.isfile(f"/app/airflow/{i_script_args}"):
                        os.remove(f"/app/airflow/{i_script_args}")
                        log_without_exit(f"{current_datetime} {real_name} Delete file: /app/airflow/{i_script_args}\n\n")
                    if os.path.isdir(f"/app/airflow/{i_script_args}"):
                        shutil.rmtree(f"/app/airflow/{i_script_args}")
                        log_without_exit(f"{current_datetime} {real_name} Delete directory: /app/airflow/{i_script_args}\n\n")

                print("0")
                sys.exit(0)

            if CONFIGURATION == "cluster":
                for i_script_args in script_args:
                    for i_all_hosts in all_hosts:
                        result_command = os.popen(
                            f"ssh airflow_deploy@{i_all_hosts} rm -rf /app/airflow/{i_script_args}"
                        ).read()
                        log_without_exit(f"{current_datetime} {real_name} {i_all_hosts}  Delete file: /app/airflow/{i_script_args}\n\n")

            print("0")
            sys.exit(0)

        if sys.argv[1] == "--file":
            script_args = sys.argv[2:]
            for i_script_args in script_args:
                if not os.path.exists(f"/app/airflow_deploy/{i_script_args}"):
                    log_and_exit(f"{current_datetime} {real_name} Файл не найден /app/airflow_deploy/{i_script_args} !\n\n")

            if CONFIGURATION == "one-way":
                for i_script_args in script_args:
                    if i_script_args[:6] == "keytab" or i_script_args[:4] == "keys":
                        if i_script_args.count("/") > 1:
                            temp_folder_path = i_script_args.rpartition("/")[0]
                            result_command = os.popen(
                                f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")


                            if i_script_args[:6] == "keytab":
                                result_command = os.popen(
                                    f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null'
                                ).read()
                                log_and_exit(f"{current_datetime} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")
                                
                            elif i_script_args[:4] == "keys":
                                result_command = os.popen(
                                    f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")
                                  
                        else:
                            result_command = os.popen(
                                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/"
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                            result_command = os.popen(
                                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/ 2> /dev/null"
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")

                    else:
                        if i_script_args.count("/") > 1:
                            temp_folder_path = i_script_args.rpartition("/")[0]
                            result_command = os.popen(
                                f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")


                            result_command = os.popen(
                                f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")

                        else:
                            result_command = os.popen(
                                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")
                            result_command = os.popen(
                                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")
                print("0")
                sys.exit(0)

            if CONFIGURATION == "cluster":
                for i_script_args in script_args:
                    for i_all_hosts in all_hosts:
                        if i_script_args[:6] == "keytab" or i_script_args[:4] == "keys":
                            if i_script_args.count("/") > 1:
                                temp_folder_path = i_script_args.rpartition("/")[0]
                                result_command = os.popen(
                                    f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                                ).read()
                                if "rsync error" in result_command:
                                    log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                                result_command = os.popen(
                                    f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null'
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                            result_command = os.popen(
                                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}"
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                            result_command = os.popen(
                                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args} 2> /dev/null"
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                        else:
                            if i_script_args.count("/") > 1:
                                temp_folder_path = i_script_args.rpartition("/")[0]
                                result_command = os.popen(
                                    f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                                ).read()
                                if "rsync error" in result_command:
                                    log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                                result_command = os.popen(
                                    f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null'
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                            result_command = os.popen(
                                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}"
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                            result_command = os.popen(
                                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args} 2> /dev/null"
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                print(0)
                sys.exit(0)

        if sys.argv[1] == "--dir":
            script_args = sys.argv[2:]
            for i_script_args in script_args:
                if not os.path.exists(f"/app/airflow_deploy/{i_script_args}"):
                    log_and_exit(f"{current_datetime} {real_name} Директория не найдена /app/airflow_deploy/{i_script_args} \n\n")

            if CONFIGURATION == "one-way":
                for i_script_args in script_args:
                    if i_script_args[:6] == "keytab" or i_script_args[:4] == "keys":
                        if i_script_args.count("/") > 1:
                            temp_folder_path = i_script_args.rpartition("/")[0]
                            result_command = os.popen(
                                f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync" --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                            result_command = os.popen(
                                f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync" --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")
                        else:
                            result_command = os.popen(
                                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                            result_command = os.popen(
                                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null"
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                    else:
                        if i_script_args.count("/") > 1:
                            result_command = os.popen(
                                f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                            ).read()
                            if "rsync error" in result_command:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                            result_command = os.popen(
                                f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                        else:
                            result_command = os.popen(
                                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                            ).read()
                            result_command_string = str(result_command)
                            if "rsync error" in result_command_string:
                                log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                            result_command = os.popen(
                                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                            ).read()
                            log_without_exit(f"{current_datetime} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                print("0")
                sys.exit(0)

            if CONFIGURATION == "cluster":
                for i_script_args in script_args:
                    for i_all_hosts in all_hosts:
                        if i_script_args[:6] == "keytab" or i_script_args[:4] == "keys":
                            if i_script_args.count("/") > 1:
                                temp_folder_path = i_script_args.rpartition("/")[0]
                                result_command = os.popen(
                                    f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                                ).read()
                                if "rsync error" in result_command:
                                    log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                                result_command = os.popen(
                                    f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")
                            else:
                                result_command = os.popen(
                                    f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                                ).read()
                                if "rsync error" in result_command:
                                    log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                                result_command = os.popen(
                                    f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null"
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")

                        else:
                            if i_script_args.count("/") > 1:
                                temp_folder_path = i_script_args.rpartition("/")[0]
                                result_command = os.popen(
                                    f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                                ).read()
                                if "rsync error" in result_command:
                                    log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                                result_command = os.popen(
                                    f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}'
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")

                            else:
                                result_command = os.popen(
                                    f"rsync -a --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                                ).read()
                                if "rsync error" in result_command:
                                    log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")


                                result_command = os.popen(
                                    f"rsync -a --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}"
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")
                                result_command = os.popen(
                                    f"rsync -a --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}/"
                                ).read()
                                result_command_string = str(result_command)
                                if "rsync error" in result_command_string:
                                    log_and_exit(f"{current_datetime} {real_name} {result_command} \n\n")

                                result_command = os.popen(
                                    f"rsync -a --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}/"
                                ).read()
                                log_without_exit(f"{current_datetime} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")
                
                print("0")
                sys.exit(0)

        if sys.argv[1] == "-h":

            print(
                "\033[32m{}\033[0m".format(
                    "\n             Доступны следующие расширения:\n"
                )
            )

            print("       Директория                     Расширение\n")

            print("/app/airflow_deploy/dags            .py .sql .json\n")

            print("/app/airflow_deploy/keytab          .keytab\n")

            print("/app/airflow_deploy/keys            .pfx .p12 .jks .secret\n")

            print("/app/airflow_deploy/csv             .csv\n")

            print("/app/airflow_deploy/jar             .jar\n")

            print("/app/airflow_deploy/user_data         *\n")

            print(
                "\033[32m{}\033[0m".format(
                    "\nДОСТУПНЫЕ КЛЮЧИ: [-c], [-h], [--delete], [--file], [--dir], [skipped]\n\n"
                )
            )

            print("\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА БЕЗ ПАРАМЕТРОВ:"))

            print(
                "    Синхронизация содержимого директорий /app/airflow_deploy/dags, /app/airflow_deploy/keytab, /app/airflow_deploy/scripts,"
            )

            print(
                "/app/airflow_deploy/keys, /app/airflow_deploy/csv, /app/airflow_deploy/jar, /app/airflow_deploy/user_data с соответствующими директориями в /app/airflow"
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
                "    Производится очистка директории назначения (/app/airflow/dags, /app/airflow/keytab, /app/airflow/scripts,)"
            )

            print(
                "/app/airflow/keys, /app/airflow/csv, /app/airflow/jar, /app/airflow/user_data) перед синхронизацией"
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
                "    Удаление файла или директории(отсчет идет от /app/airflow_deploy/)"
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
                "    Деплой указанного файла (отсчет идет от /app/airflow_deploy/) из /app/airflow_deploy/ в /app/airflow/"
            )

            print(
                "\033[32m{}\033[0m".format(
                    "ПРИМЕР ЗАПУСКА : sudo -u airflow_deploy ./airflow_sync_dags.sh --file /dags/test.py\n\n"
                )
            )

            print("\033[32m{}\033[0m".format("Запуск скрипта с ключом --dir:"))

            print(
                "    Деплой указанной директории (отсчет идет от /app/airflow_deploy/) из /app/airflow_deploy/ в /app/airflow/"
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

        if sys.argv[1] == "-c":
            check_files_in_dirs(all_error)
            if CONFIGURATION == "one-way":
                for elem_list_folders in list_folders:
                    files_and_directories = os.listdir(
                        f"/app/airflow/{elem_list_folders}/"
                    )
                    for elem_files_and_directories in files_and_directories:
                        if (
                            f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}" == "/app/airflow/dags/sql"
                        ):
                            sql_files_and_directories = os.listdir(
                                "/app/airflow/dags/sql/"
                            )
                            for (
                                elem_sql_files_and_directories
                            ) in sql_files_and_directories:
                                if os.path.isfile(
                                    f"/app/airflow/dags/sql/{elem_sql_files_and_directories}"
                                ):
                                    os.remove(
                                        f"/app/airflow/dags/sql/{elem_sql_files_and_directories}"
                                    )
                                    remove_files_folders.add(
                                        f"removed /app/airflow/dags/sql/{elem_sql_files_and_directories}"
                                    )

                                if os.path.isdir(
                                    f"/app/airflow/dags/sql/{elem_sql_files_and_directories}"
                                ):
                                    shutil.rmtree(
                                        f"/app/airflow/dags/sql/{elem_sql_files_and_directories}"
                                    )
                                    remove_files_folders.add(
                                        f"removed /app/airflow/dags/sql/{elem_sql_files_and_directories}"
                                    )

                        if os.path.isfile(f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}"):
                            os.remove(f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}")
                            remove_files_folders.add( f"removed  /app/airflow/{elem_list_folders}/{elem_files_and_directories}")

                        if (f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}" == "/app/airflow/dags/sql"):
                            continue

                        if ("__pycache__" in f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}"):
                            continue

                        if os.path.isdir(
                            f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}"
                        ):
                            shutil.rmtree(
                                f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}"
                            )
                            remove_files_folders.add(
                                f"removed /app/airflow/{elem_list_folders}/{elem_files_and_directories}"
                            )

    if (
        len(sys.argv) >= 2
        and sys.argv[1] not in ["--delete", "--file", "--dir", "--skipped", "-c", "-h"]
        and sys.argv[1] not in ARGV_KEYS
    ):
        log_and_exit(f"{current_datetime} {real_name} Неизвестный ключ/и {sys.argv[1:]}\n\n")
    
    return remove_files_folders


def check_files_in_dirs(all_error: Queue) -> None:
    """
    Проверяет наличие файлов и директорий для переноса в /app/airflow_deploy/*.
    Если данных нет — добавляет ошибку в очередь all_error.

    Параметры:
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    files_in_dirs = 0
    for elem_list_folders in list_folders:
        for _, dirs, files in os.walk(f"/app/airflow_deploy/{elem_list_folders}"):
            files_in_dirs += len(files)
            files_in_dirs += len(dirs)
            if files_in_dirs > 1:
                break
    if files_in_dirs <= 1:
        all_error.put(
            "Ошибка !!! В прикладных директориях /app/airflow_deploy (dags/csv/jar/keys/keytab/scripts/user_data) отсутствуют данные для переноса\n\n"
        )


def check_groups_users(host: str, all_error: Queue) -> None:
    """
    Проверяет корректность групп и владельцев файлов/директорий на целевых хостах.
    Если найдены некорректные группы или владельцы — добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    if CONFIGURATION == "cluster":
        for elem_list_folders in list_folders:
            if elem_list_folders in ("keytab", "keys"):
                dir_path = f"/app/airflow/{elem_list_folders}"
                result_check_permission = os.popen(
                    f"ssh airflow_deploy@{host} find {dir_path} ! -group airflow_deploy ! -group airflow"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                if len(for_result_check_permission) > 1:
                    for i_result_check_permission in for_result_check_permission:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}"
                        ).read()
                        all_error.put(
                        f"Ошибка !!! Некорректная группа на хосте {host} {perm_error}\n\n"
                        )

            else:
                result_check_permission = os.popen(
                    f"ssh airflow_deploy@{host} find /app/airflow/{elem_list_folders} ! -group airflow_deploy ! -group airflow"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                for i_result_check_permission in for_result_check_permission:
                    if len(i_result_check_permission) > 2:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Некорректная группа на хосте {host}  {perm_error}\n\n"
                        )

                result_check_permission = os.popen(
                    f"ssh airflow_deploy@{host} find /app/airflow/{elem_list_folders}  ! -user airflow_deploy ! -user airflow"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                for i_result_check_permission in for_result_check_permission:
                    if len(i_result_check_permission) > 2:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Некорректный владелец на хосте {host}  {perm_error}\n\n"
                        )

    if CONFIGURATION == "one-way":
        for elem_list_folders in list_folders:
            if elem_list_folders in ("keytab", "keys"):
                dir_path = f"/app/airflow/{elem_list_folders}"
                result_check_permission = os.popen(
                    f"find {dir_path} ! -group airflow_deploy ! -group airflow"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                if len(for_result_check_permission) > 1:
                    for i_result_check_permission in for_result_check_permission:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}"
                        ).read()
                        all_error.put(f"Ошибка !!! Некорректная группа  {perm_error}\n\n")

            else:
                result_check_permission = os.popen(
                    f"find /app/airflow/{elem_list_folders} ! -group airflow_deploy ! -group airflow"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                for i_result_check_permission in for_result_check_permission:
                    if len(i_result_check_permission) > 2:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Некорректная группа  {perm_error}\n\n"
                        )
                result_check_permission = os.popen(
                    f"find /app/airflow/{elem_list_folders} ! -user airflow_deploy ! -user airflow"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                for i_result_check_permission in for_result_check_permission:
                    if len(i_result_check_permission) > 2:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Некорректный владелец  {perm_error}\n\n"
                        )



def check_permissions(host: str, all_error: Queue) -> None:
    """
    Проверяет права доступа к файлам и директориям на целевых хостах.
    Если права некорректны — добавляет ошибку в очередь all_error.

    Параметры:
        host (str): Имя или адрес хоста, на котором выполняется проверка.
        all_error (Queue): Очередь для передачи сообщений об ошибках.
    """
    if CONFIGURATION == "cluster":
        for elem_list_folders in list_folders:
            if elem_list_folders in ("keytab", "keys"):
                dir_path = f"/app/airflow/{elem_list_folders}"
                result_check_permission = os.popen(
                    f"ssh airflow_deploy@{host} find {dir_path} -type f ! -perm 0060"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                if len(for_result_check_permission) > 1:
                    for i_result_check_permission in for_result_check_permission:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} stat {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Отсутсвует право на запись на хосте {host} в {perm_error}\n\n"
                        )

            else:
                result_check_permission = os.popen(
                    f"ssh airflow_deploy@{host} find /app/airflow/{elem_list_folders} ! -perm 0775 ! -perm 0755"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                for i_result_check_permission in for_result_check_permission:
                    if len(i_result_check_permission) > 2:
                        if "__pycache__" in i_result_check_permission:
                            continue
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} stat {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Отсутсвует право на запись на хосте {host} в {perm_error}\n\n"
                        )

    if CONFIGURATION == "one-way":
        for elem_list_folders in list_folders:
            if elem_list_folders in ("keytab", "keys"):
                dir_path = f"/app/airflow/{elem_list_folders}"
                result_check_permission = os.popen(
                    f"find {dir_path} -type f ! -perm 0060"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                if len(for_result_check_permission) > 1:
                    for i_result_check_permission in for_result_check_permission:
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} stat {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Отсутсвует право на запись в {perm_error}\n"
                        )

            else:
                result_check_permission = os.popen(
                    f"find /app/airflow/{elem_list_folders} ! -perm 0775 ! -perm 0755"
                )
                for_result_check_permission = result_check_permission.read().split("\n")
                for i_result_check_permission in for_result_check_permission:
                    if len(i_result_check_permission) > 2:
                        if "__pycache__" in i_result_check_permission:
                            continue
                        perm_error = os.popen(
                            f"ssh airflow_deploy@{host} stat {i_result_check_permission}"
                        ).read()
                        all_error.put(
                            f"Ошибка !!! Отсутсвует право на запись в {perm_error}\n\n"
                        )


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
                dir_folder == "/app/airflow_deploy/dags/"
                and temp_file[:28] == "/app/airflow_deploy/dags/sql"
            ):
                continue
            if temp_file.rpartition(".")[2] not in type_files:
                # file_extension = temp_file.rpartition(".")[2]
                error_map = {
                    "/app/airflow_deploy/dags/sql": ".sql",
                    "/app/airflow_deploy/dags": ".py",
                    "/app/airflow_deploy/keytab": ".keytab",
                    "/app/airflow_deploy/scripts": ".sh .json",
                    "/app/airflow_deploy/keys": ".pfx .p12 .jks .secret",
                    "/app/airflow_deploy/csv": ".csv",
                    "/app/airflow_deploy/jar": ".jar",
                }

                for prefix, ext in error_map.items():
                    if prefix == "/app/airflow_deploy/dags":
                        if temp_file.startswith(prefix) and not temp_file.startswith("/app/airflow_deploy/dags/sql"):
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
        f"ssh airflow_deploy@{data_host} df /app  --output=avail,used | tail -n +2 | tr -d '%'"
    ).read()
    free_disk_space = int(result_command.split(" ")[0])
    parts = result_command.split()
    if len(parts) < 2:
        all_error.put(f"Ошибка: некорректный вывод df: '{result_command}'\n")
        return
    used = int(parts[1])
    # used = int(result_command.split(" ")[1])
    total = free_disk_space + used
    one_percent = total / 100
    used_percent = int((used + size_airflow_deploy) / one_percent)
    if used_percent > CRITICAL_PERCENT:
        if CONFIGURATION == "cluster":
            all_error.put(
                f"Предупреждение !!! После добавления файлов на хост {data_host} колличество занятого места превысит 80% в каталоге /app на хосте {data_host}\n\n"
            )
        else:
            all_error.put(
                f"Предупреждение !!! После добавления файлов на хост {data_host} колличество занятого места превысит 80% в каталоге /app \n\n"
            )

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
        for root, _, files in os.walk(f"/app/airflow_deploy/{list_folder}"):
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
            os.popen(
                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{folder} airflow_deploy@{host_name}:/app/airflow/ 2> /dev/null"
            ).read()
        else:
            os.popen(
                f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{folder} airflow_deploy@{host_name}:/app/airflow/ 2> /dev/null"
            ).read()
        for root, _, files in os.walk(f"/app/airflow_deploy/{folder}"):
            for file in files:
                with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as log_file:
                    temp_file = f"{root}/{file}"
                    temp_file_airflow = "/app/airflow/" + f"{root}/{file}"[20:]
                    md5_sum = path_sum[temp_file]
                    log_file.write(
                        f"  Source: {root}/{file}  Destination: {host_name}@{temp_file_airflow}  Md5hash: {md5_sum}\n\n"
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
            result_command_keytab = subprocess.Popen(
                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{folder} airflow_deploy@{host_name}:/app/airflow/",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            result_command_keytab_string = result_command_keytab.stderr.read().decode(
                "utf-8"
            )
            if "rsync error" in result_command_keytab_string:
                all_error.put(
                    f"Ошибка!!! {host_name} \n {result_command_keytab_string}\n\n"
                )

        else:
            result_command_other = subprocess.Popen(
                f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{folder} airflow_deploy@{host_name}:/app/airflow/",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            result_command_other_string = result_command_other.stderr.read().decode(
                "utf-8"
            )
            if "rsync error" in result_command_other_string:
                all_error.put(
                    f"Ошибка!!! {host_name} {result_command_other_string} \n\n"
                )


def main() -> None:
    """
    Основная функция скрипта, выполняющая синхронизацию директорий и проверку параметров.
    В зависимости от конфигурации (one-way или cluster) выполняет соответствующие действия.
    """
    if "--skipped" not in sys.argv:
        for check_folder, check_extension in FOLDER_EXTENSION.items():
            check_type_file(check_folder, check_extension, ALL_ERROR)

    path_sum = path_sum_files()
    remove_files_folders = set()
    if CONFIGURATION == "one-way":
        if "--skipped" not in sys.argv:
            remove_files_folders = check_param_run(ALL_ERROR)
            check_files_in_dirs(ALL_ERROR)
            check_free_space(current_hostname, ALL_ERROR)
            check_permissions(current_hostname, ALL_ERROR)
            check_groups_users(current_hostname, ALL_ERROR)
            check_rsync_host(current_hostname, ALL_ERROR)

        param_run_script()
        if ALL_ERROR.qsize() > 0:
            data_all_error = set(ALL_ERROR.get() for _ in range(ALL_ERROR.qsize()))
            with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as file_log:
                current_datetime_now = datetime.now()
                file_log.write(
                    f"******************************************* ALL ERORRS {current_datetime_now}*******************************************\n\n"
                )
                for one_data_all_error in data_all_error:
                    file_log.write(one_data_all_error)
                file_log.write(
                    END_ALL_ERRORS_STRING
                )
                print("1")
                sys.exit(1)

        rsync_host(current_hostname, path_sum)

        with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as end_work_script:
            for remove_file in remove_files_folders:
                end_work_script.write(f"{remove_file}\n")

        with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as file_log:
            file_log.write(
                END_WORK_SCRIPT_STRING
            )

        print("0")
        sys.exit(0)

    if CONFIGURATION == "cluster":
        if "--skipped" not in sys.argv:
            all_processes_connect_write = [
                Process(target=connect_write, args=(hostname, ALL_ERROR))
                for hostname in all_hosts
            ]
            for process_connect_write in all_processes_connect_write:
                process_connect_write.start()

            for process_connect_write in all_processes_connect_write:
                process_connect_write.join()

            remove_files_folders = check_param_run(ALL_ERROR)
            all_process_check_files_in_dirs = [
                Process(target=check_files_in_dirs, args=(ALL_ERROR,))
            ]

            for process_check_files_in_dirs in all_process_check_files_in_dirs:
                process_check_files_in_dirs.start()

            for process_check_files_in_dirs in all_process_check_files_in_dirs:
                process_check_files_in_dirs.join()

            all_processes_check_free_space = [
                Process(target=check_free_space, args=(hostname, ALL_ERROR))
                for hostname in all_hosts
            ]

            for process_check_free_space in all_processes_check_free_space:
                process_check_free_space.start()

            for process_check_free_space in all_processes_check_free_space:
                process_check_free_space.join()

            all_processes_check_permissions = [
                Process(target=check_permissions, args=(hostname, ALL_ERROR))
                for hostname in all_hosts
            ]

            for process_check_permissions in all_processes_check_permissions:
                process_check_permissions.start()

            for process_check_permissions in all_processes_check_permissions:
                process_check_permissions.join()

            all_processes_check_groups_users = [
                Process(target=check_groups_users, args=(hostname, ALL_ERROR))
                for hostname in all_hosts
            ]

            for process_check_groups_users in all_processes_check_groups_users:
                process_check_groups_users.start()

            for process_check_groups_users in all_processes_check_groups_users:
                process_check_groups_users.join()

            for hostname in all_hosts:
                check_rsync_host(hostname, ALL_ERROR)

        param_run_script()

        if ALL_ERROR.qsize() > 0:
            data_all_error = [ALL_ERROR.get() for _ in range(ALL_ERROR.qsize())]
            with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as file_log:

                current_datetime_now = datetime.now()

                file_log.write(
                    f"******************************************* ALL ERORRS {current_datetime_now}*******************************************\n\n"
                )

                for one_data_all_error in data_all_error:

                    file_log.write(one_data_all_error)

                file_log.write(
                    END_ALL_ERRORS_STRING
                )

                print("1")
                sys.exit(1)

        if len(sys.argv) == 2 and sys.argv[1] == "-c":
            for hostname in all_hosts:
                remove_destination_folder(hostname, result_queue)
            remove_files_folders = result_queue.get()

        for hostname in all_hosts:
            rsync_host(hostname, path_sum)

        with open("/app/airflow_deploy/log/deploy.log", "a", encoding="utf-8") as end_work_script:
            if len(sys.argv) == 2 and sys.argv[1] == "-c":
                if "removed directory '/app/airflow/dags/sql'\n" in remove_files_folders:
                    remove_files_folders.remove(
                        "removed directory '/app/airflow/dags/sql'\n"
                    )

                end_work_script.write("Удаленные файлы: \n\n")
                for remove_file in remove_files_folders:
                    if (remove_file == "removed directory '/app/airflow/dags/sql'\n"):
                        continue

                    end_work_script.write(f"{remove_file}\n")
                end_work_script.write("\n")

            end_work_script.write(
                END_WORK_SCRIPT_STRING
            )

            print("0")
            sys.exit(0)

if __name__ == "__main__":
    main()
