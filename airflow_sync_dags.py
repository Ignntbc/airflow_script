import shutil
import os
import hashlib
import datetime
import json
import sys
import subprocess
import socket
import sys
# import pwd
# import grp
from multiprocessing import Process, Queue


PATH_SUM = dict()

size_airflow_deploy = int(os.popen("du -s app/airflow_deploy | cut -f1").read())

CRITICAL_PERCENT = 80

list_folders = ["csv", "dags", "jar", "keys", "keytab", "scripts", "user_data"]

request_name = subprocess.Popen("${SUDO_USER:-${USER}}", shell=True,  stdout=subprocess.PIPE, stderr=subprocess.PIPE)

#######
# real_name = request_name.stderr.read().decode("utf-8").split("\n")[0].split(" ")[1].replace(":","")
import subprocess
cmd = ["id"]
print("[DEBUG] Running command:", cmd)
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout_output, stderr_output = proc.communicate()
stdout_output = stdout_output.decode("utf-8")
stderr_output = stderr_output.decode("utf-8")
print("[DEBUG] stdout output:", repr(stdout_output))
print("[DEBUG] stderr output:", repr(stderr_output))
# Пример разбора stdout для получения имени пользователя
if stdout_output:
    # Например, строка: 'uid=0(root) gid=0(root) groups=0(root)\n'
    try:
        real_name = stdout_output.split(" ")[0].split("(")[1].split(")")[0]
        print("[DEBUG] real_name:", real_name)
    except Exception as e:
        print("[DEBUG] Ошибка разбора stdout:", e)
        real_name = None
else:
    real_name = None
#######

current_hostname = socket.gethostname()

ARGV_KEYS = ['-c', '-h', '--file', '--dir', '--skipped']

# uid_airflow_deploy = pwd.getpwnam("airflow_deploy").pw_uid

# uid_group_airflow = grp.getgrnam("airflow").gr_gid

FOLDER_EXTENSION = dict()

FOLDER_EXTENSION["/app/airflow_deploy/dags/"] = ["py", "json"]

FOLDER_EXTENSION["/app/airflow_deploy/dags/sql/"] = ["sql"]

FOLDER_EXTENSION["/app/airflow_deploy/csv/"] = ["csv"]

FOLDER_EXTENSION["/app/airflow_deploy/jar/"] = ["jar"]

FOLDER_EXTENSION["/app/airflow_deploy/keys/"] = ["pfx", "p12", "jks", "secret"]

FOLDER_EXTENSION["/app/airflow_deploy/keytab/"] = ["keytab"]

GLOBAL_LIST_ERROR = set()

REMOVE_FILES_FOLDERS = set()

result_queue = Queue()

ALL_ERROR = Queue()


#REMOVE DESTITINATION FOLDER
def remove_destination_folder(hostname, result_queue):

    for elem in list_folders:

        if elem == "dags":

            result_command_dag = list(os.popen(f"ssh airflow_deploy@{hostname} ls -a /app/airflow/{elem}/").read().split("\n"))

            result_command_dag.remove(".")

            result_command_dag.remove("..")

            result_command_dag.remove("")

            for elem_dags_dir in result_command_dag:

                if  "__pycache__" in  elem_dags_dir:

                    continue

                result_command_dag = os.popen(f"ssh airflow_deploy@{hostname} rm -rfv /app/airflow/dags/{elem_dags_dir}").read()

                result_queue.put(result_command_dag)

            result_command_dag = os.popen(f"ssh airflow_deploy@{hostname} rm -rfv /app/airflow/dags/sql/*").read()

            result_queue.put(result_command_dag)

        else:

            remote_list_files_folders = os.popen(f"ssh airflow_deploy@{hostname} ls -R /app/airflow/{elem}/").read()

            result_command = list(os.popen(f"ssh airflow_deploy@{hostname} ls -a /app/airflow/{elem}/").read().split("\n"))

            result_command.remove(".")

            result_command.remove("..")

            result_command.remove("")

            for elem_result_command in result_command:

                result_command = os.popen(f"ssh airflow_deploy@{hostname} rm -rf /app/airflow/{elem}/{elem_result_command}").read()

                result_queue.put(result_command)



#PARAM RUN SCRIPT
def param_run_script():

    with open("/app/airflow_deploy/log/deploy.log", "a") as run_script:

        run_script.write("******************************************* Run script *******************************************\n")

        current_datetime_now = datetime.datetime.now()

        run_script.write(f"Start run script: {current_datetime_now}\n")

        run_script.write(f"User: {real_name}\n")

        if len(sys.argv) == 2 and sys.argv[1] == '-c':

            run_script.write(f"Run param: -c\n\n")

        else:
            run_script.write(f"Run param: false key\n\n")


### CHECK_CLASTER_OR_ONE-WAY
# with open("/app/app/etc/description.json", "r") as file_description:
with open("description.json", "r") as file_description:
    data_description = json.load(file_description)

if data_description["software"]["app"]['executor'] == "localexecutor":

    configuration = "one-way"

else:
    configuration = "claster"

schedulers = data_description["software"]["app"]["nodes"]["airflow_scheduler"]

webs = data_description["software"]["app"]["nodes"]["airflow_web"]

workers = data_description["software"]["app"]["nodes"]["airflow_workers"]


#CHECK PARAM RUN
def check_param_run(ALL_ERROR):
    all_hosts = schedulers + webs + workers
    current_datetime_now = datetime.datetime.now()

    if len(sys.argv) >= 2:
        if sys.argv[1] == "--delete":

            script_args = sys.argv[2:]

            for i_script_args in script_args:

                if not os.path.exists(f"/app/airflow/{i_script_args}"):

                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                        file_log.write(f"{current_datetime_now} {real_name} Нету такого файла или директории /app/airflow/{i_script_args}\n\n")

                        print("1")

                        sys.exit(1) 

            if configuration == "one-way":
                
                for i_script_args in script_args:

                    if os.path.isfile(f"/app/airflow/{i_script_args}"):

                        os.remove(f"/app/airflow/{i_script_args}")

                        with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                            file_log.write(f"{current_datetime_now} {real_name} Delete file: /app/airflow/{i_script_args}\n\n")

                    if os.path.isdir(f"/app/airflow/{i_script_args}"):

                        shutil.rmtree(f"/app/airflow/{i_script_args}")

                        with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                            file_log.write(f"{current_datetime_now} {real_name} Delete directory: /app/airflow/{i_script_args}\n\n")

                print("0")

                sys.exit(0)

            if configuration == "claster":

                for i_script_args in script_args:

                    for i_all_hosts in all_hosts:

                        result_command = os.popen(f"ssh airflow_deploy@{i_all_hosts} rm -rf /app/airflow/{i_script_args}").read()

                        with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                            file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts}  Delete file: /app/airflow/{i_script_args}\n\n")

            print("0")

            sys.exit(0)


        if sys.argv[1] == "--file":
            print("DEBUG: Entered --file parameter block")
            script_args = sys.argv[2:]
            print("DEBUG: script_args =", script_args)

            for i_script_args in script_args:

                if not os.path.exists(f"/app/airflow_deploy/{i_script_args}"):

                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                        file_log.write(f"{current_datetime_now} {real_name} Файл не найден /app/airflow_deploy/{i_script_args} !\n\n")

                    print("1")

                    sys.exit(1)

            if configuration == "one-way":
                for i_script_args in script_args:

                    if i_script_args[:6] == "keytab":

                        if  i_script_args.count("/") > 1:

                            temp_folder_path=i_script_args.rpartition("/")[0]

                            #check test rsync result
                            result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")
                        else:

                            #check test rsync result
                            result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/").read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/ 2> /dev/null").read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")

                    elif i_script_args[:4] == "keys":

                        if  i_script_args.count("/") > 1:

                            temp_folder_path=i_script_args.rpartition("/")[0]

                            #check test rsync result
                            result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")
                        else:

                            #check test rsync result
                            result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/").read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/ 2> /dev/null").read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")
                    else:

                        if  i_script_args.count("/") > 1:

                            temp_folder_path=i_script_args.rpartition("/")[0]

                            result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")
                        else:

                            result_command = os.popen(f'rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлен файл /app/airflow/{i_script_args} \n\n")

                print("0")

                sys.exit(0)

            if configuration == "claster":

                for i_script_args in script_args:

                    for i_all_hosts in all_hosts:

                        if i_script_args[:6] == "keytab":

                            if i_script_args.count("/") > 1:

                                temp_folder_path=i_script_args.rpartition("/")[0]

                                result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null').read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                            result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}").read()                                                   

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args} 2> /dev/null").read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                        elif i_script_args[:4] == "keys":

                            if i_script_args.count("/") > 1:

                                temp_folder_path=i_script_args.rpartition("/")[0]

                                result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null').read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")


                            result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}").read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args} 2> /dev/null").read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")
                        else:

                            if i_script_args.count("/") > 1:

                                temp_folder_path=i_script_args.rpartition("/")[0]

                                result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args} airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null').read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                            result_command = os.popen(f'rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args} airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args} 2> /dev/null').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлен файл: /app/airflow/{i_script_args}\n\n")

                print(0)

                sys.exit(0)

        if sys.argv[1] == "--dir":
            print("DEBUG: Entered --dir parameter block")

            script_args = sys.argv[2:]

            for i_script_args in script_args:

                if not os.path.exists(f"/app/airflow_deploy/{i_script_args}"):

                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                        file_log.write(f"{current_datetime_now} {real_name} Директория не найдена /app/airflow_deploy/{i_script_args} \n\n")

                    print("1")

                    sys.exit(1)

            if configuration == "one-way":
                print("DEBUG: Configuration is one-way")
                for i_script_args in script_args:

                    if i_script_args[:6] == "keytab":

                        if  i_script_args.count("/") > 1:

                            temp_folder_path=i_script_args.rpartition("/")[0]

                            result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                        else:

                            result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}").read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null").read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                    elif i_script_args[:4] == "keys":

                        if  i_script_args.count("/") > 1:

                            result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync" --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync" --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                        else:

                            result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}").read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null").read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                    else:

                        if  i_script_args.count("/") > 1:

                            result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            if "rsync error" in result_command:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                        else:

                            result_command = os.popen(f'rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            result_command_string = str(result_command)

                            if "rsync error" in result_command_string:

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                print("1")

                                sys.exit(1)

                            result_command = os.popen(f'rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                            with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                file_log.write(f"{current_datetime_now} {real_name} Добавлена директория /app/airflow_deploy/{i_script_args}/ \n\n")

                print("0")

                sys.exit(0)


            if configuration == "claster":
                print("DEBUG: Configuration is claster")

                for i_script_args in script_args:

                    for i_all_hosts in all_hosts:

                        if i_script_args[:6] == "keytab":
                            print("DEBUG: Processing keytab directory")
                            if  i_script_args.count("/") > 1:

                                temp_folder_path=i_script_args.rpartition("/")[0]
                                result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")
                            else:

                                result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}").read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null").read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлена : /app/airflow/{i_script_args}\n\n")

                        elif i_script_args[:4] == "keys":

                            if  i_script_args.count("/") > 1:

                                temp_folder_path=i_script_args.rpartition("/")[0]

                                result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлена директори: /app/airflow/{i_script_args}\n\n")

                            else:


                                result_command = os.popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}").read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args} 2> /dev/null").read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")

                        else:
                            print("DEBUG: Syncing general directory")

                            if  i_script_args.count("/") > 1:
                                print("DEBUG: Directory contains subdirectories")

                                temp_folder_path=i_script_args.rpartition("/")[0]

                                result_command = os.popen(f'rsync --checksum -nrogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)

                                result_command = os.popen(f'rsync --checksum -rogp --rsync-path="mkdir -p /app/airflow/{temp_folder_path} && rsync"  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")

                            else:
                                print("Предположительно падаем тут")
                                result_command = os.popen(f'rsync -a --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                if "rsync error" in result_command:

                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")

                                    print("1")

                                    sys.exit(1)
                                print("И тут")
                                result_command = os.popen(f'rsync -a --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@127.0.0.1:/app/airflow/{i_script_args}').read()

                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлена директория: /app/airflow/{i_script_args}\n\n")

                                
                                result_command = os.popen(f'rsync -a --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}/').read()
                                result_command_string = str(result_command)
                                if "rsync error" in result_command_string:
                                    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:
                                        file_log.write(f"{current_datetime_now} {real_name} {result_command} \n\n")
                                    print("1")
                                    sys.exit(1)
                                result_command = os.popen(f'rsync -a --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{i_script_args}/ airflow_deploy@{i_all_hosts}:/app/airflow/{i_script_args}/').read()
                                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:
                                    file_log.write(f"{current_datetime_now} {real_name} {i_all_hosts} Добавлена директория /app/airflow/{i_script_args}/ \n\n")

                print("0")

                sys.exit(0)

        if sys.argv[1] == '-h':

            print("\033[32m{}\033[0m".format("\n             Доступны следующие расширения:\n"))

            print("       Директория                     Расширение\n")

            print("/app/airflow_deploy/dags            .py .sql .json\n")

            print("/app/airflow_deploy/keytab          .keytab\n")

            print("/app/airflow_deploy/keys            .pfx .p12 .jks .secret\n")

            print("/app/airflow_deploy/csv             .csv\n")

            print("/app/airflow_deploy/jar             .jar\n")

            print("/app/airflow_deploy/user_data         *\n")

            print("\033[32m{}\033[0m".format("\nДОСТУПНЫЕ КЛЮЧИ: [-c], [-h], [--delete], [--file], [--dir], [skipped]\n\n"))

            print("\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА БЕЗ ПАРАМЕТРОВ:"))

            print("    Синхронизация содержимого директорий /app/airflow_deploy/dags, /app/airflow_deploy/keytab, /app/airflow_deploy/scripts,")

            print("/app/airflow_deploy/keys, /app/airflow_deploy/csv, /app/airflow_deploy/jar, /app/airflow_deploy/user_data с соответствующими директориями в /app/airflow")

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags_v2.sh\n\n"))

            print("\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА С КЛЮЧОМ --skipped:"))

            print("    Отключение проверок")

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags_v2.sh --skipped\n\n"))

            print("\033[32m{}\033[0m".format("ЗАПУСК СКРИПТА С КЛЮЧОМ -c:"))

            print("    Производится очистка директории назначения (/app/airflow/dags, /app/airflow/keytab, /app/airflow/scripts,)")

            print("/app/airflow/keys, /app/airflow/csv, /app/airflow/jar, /app/airflow/user_data) перед синхронизацией")

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags_v2.sh -c\n\n"))

            print("\033[32m{}\033[0m".format("Запуск скрипта с ключом -h:"))

            print("    Вывод справки")

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА: sudo -u airflow_deploy ./airflow_sync_dags_v2.sh -h\n\n"))

            print("\033[32m{}\033[0m".format("Запуск скрипта с ключом --delete:"))

            print("    Удаление файла или директории(отсчет идет от /app/airflow_deploy/)")

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА (Удаление файла): sudo -u airflow_deploy ./airflow_sync_dags_v2.sh --delete /dags/test.py"))

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА (Удаление директории): sudo -u airflow_deploy ./airflow_sync_dags_v2.sh --delete /dags/test_dir \n\n"))

            print("\033[32m{}\033[0m".format("Запуск скрипта с ключом --file:"))

            print("    Деплой указанного файла (отсчет идет от /app/airflow_deploy/) из /app/airflow_deploy/ в /app/airflow/")

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА : sudo -u airflow_deploy ./airflow_sync_dags_v2.sh --file /dags/test.py\n\n"))

            print("\033[32m{}\033[0m".format("Запуск скрипта с ключом --dir:"))

            print("    Деплой указанной директории (отсчет идет от /app/airflow_deploy/) из /app/airflow_deploy/ в /app/airflow/")

            print("\033[32m{}\033[0m".format("ПРИМЕР ЗАПУСКА : sudo -u airflow_deploy ./airflow_sync_dags_v2.sh --dir /dags/test_dir\n\n"))

            print("\033[32m{}\033[0m".format("Ссылка на документацию Airflow:"))

            print("    https://docs.cloud.vtb.ru/home/prod-catalog/application-integration/apache-airflow\n")

            sys.exit(0)

        if sys.argv[1] == '-c':

            check_files_in_dirs(ALL_ERROR)

            if configuration == "one-way":

                for elem_list_folders in list_folders:

                    files_and_directories = os.listdir(f"/app/airflow/{elem_list_folders}/")

                    for elem_files_and_directories in files_and_directories:

                        if f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}" == "/app/airflow/dags/sql":

                            sql_files_and_directories = os.listdir(f"/app/airflow/dags/sql/")

                            for elem_sql_files_and_directories in sql_files_and_directories:

                                if os.path.isfile(f"/app/airflow/dags/sql/{elem_sql_files_and_directories}"):

                                    os.remove(f"/app/airflow/dags/sql/{elem_sql_files_and_directories}")

                                    REMOVE_FILES_FOLDERS.add(f"removed /app/airflow/dags/sql/{elem_sql_files_and_directories}")

                                if os.path.isdir(f"/app/airflow/dags/sql/{elem_sql_files_and_directories}"):

                                    shutil.rmtree(f"/app/airflow/dags/sql/{elem_sql_files_and_directories}")

                                    REMOVE_FILES_FOLDERS.add(f"removed /app/airflow/dags/sql/{elem_sql_files_and_directories}")

                        if os.path.isfile(f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}"):

                            os.remove(f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}")

                            REMOVE_FILES_FOLDERS.add(f"removed  /app/airflow/{elem_list_folders}/{elem_files_and_directories}")

                        if f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}" =="/app/airflow/dags/sql":

                            continue

                        if "__pycache__" in f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}":

                            continue

                        if os.path.isdir(f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}"):

                            shutil.rmtree(f"/app/airflow/{elem_list_folders}/{elem_files_and_directories}")

                            REMOVE_FILES_FOLDERS.add(f"removed /app/airflow/{elem_list_folders}/{elem_files_and_directories}")


    if len(sys.argv) >= 2 and sys.argv[1] not in ["--delete", "--file", "--dir", "--skipped", "-c", "-h"] and sys.argv[1] not in ARGV_KEYS:

        with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

            file_log.write(f"{current_datetime_now} {real_name} Неизвестный ключ/и {sys.argv[1:]}\n\n")

        print("1")

        sys.exit(1)


### CHECK ANY FILES IN DIRECTORY
def check_files_in_dirs(ALL_ERROR):

    files_in_dirs = 0

    for elem_list_folders in list_folders:

        for root, dirs, files in os.walk(f"/app/airflow_deploy/{elem_list_folders}"):

            files_in_dirs += len(files)

            files_in_dirs += len(dirs)

            if files_in_dirs > 1:
                break

    if files_in_dirs <= 1:

        ALL_ERROR.put(f"Ошибка !!! В прикладных директориях /app/airflow_deploy (dags/csv/jar/keys/keytab/scripts/user_data) отсутствуют данные для переноса\n\n")


### CHECK GROUPS AIRFLOW, AIRFLOW_DEPLOY
def check_groups_users(host, ALL_ERROR):

    if configuration == "claster":

        for elem_list_folders in list_folders:

            if elem_list_folders == "keytab":

                result_check_permission = os.popen(f"ssh airflow_deploy@{host} find /app/airflow/keytab ! -group airflow_deploy ! -group airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректная группа {host} {perm_error}\n\n")

            elif elem_list_folders == "keys":

                result_check_permission = os.popen(f"ssh airflow_deploy@{host} find /app/airflow/keys ! -group airflow_deploy ! -group airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректная группа на хосту {host}  {perm_error}\n\n")

            else:

                result_check_permission = os.popen(f"ssh airflow_deploy@{host} find /app/airflow/{elem_list_folders} ! -group airflow_deploy ! -group airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                for i_result_check_permission in for_result_check_permission:

                    if len(i_result_check_permission) > 2:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректная группа на хосту {host}  {perm_error}\n\n")

                result_check_permission = os.popen(f"ssh airflow_deploy@{host} find /app/airflow/{elem_list_folders}  ! -user airflow_deploy ! -user airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                for i_result_check_permission in for_result_check_permission:

                    if len(i_result_check_permission) > 2:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректный владелец на хосту {host}  {perm_error}\n\n")

    if configuration == "one-way":

        for elem_list_folders in list_folders:

            if elem_list_folders == "keytab":

                result_check_permission = os.popen(f"find /app/airflow/keytab ! -group airflow_deploy ! -group airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректная группа  {perm_error}\n\n")

            elif elem_list_folders == "keys":

                result_check_permission = os.popen(f"find /app/airflow/keys ! -group airflow_deploy ! -group airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректная группа  {perm_error}\n")

            else:

                result_check_permission = os.popen(f"find /app/airflow/{elem_list_folders} ! -group airflow_deploy ! -group airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                for i_result_check_permission in for_result_check_permission:

                    if len(i_result_check_permission) > 2:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректная группа  {perm_error}\n\n")

                result_check_permission = os.popen(f"find /app/airflow/{elem_list_folders} ! -user airflow_deploy ! -user airflow")

                for_result_check_permission = result_check_permission.read().split("\n")

                for i_result_check_permission in for_result_check_permission:

                    if len(i_result_check_permission) > 2:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} ls -l {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Некорректный владелец  {perm_error}\n\n")


### CHECK PERMISSION
def check_permissions(host, ALL_ERROR):

    if configuration == "claster":

        for elem_list_folders in list_folders:

            if elem_list_folders == "keytab":

                result_check_permission = os.popen(f"ssh airflow_deploy@{host} find /app/airflow/keytab -type f ! -perm 0060")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} stat {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Отсутсвует право на запись на хосту {host} в {perm_error}\n\n")

            elif elem_list_folders == "keys":

                result_check_permission = os.popen(f"ssh airflow_deploy@{host} find /app/airflow/keys -type f ! -perm 0060")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} stat {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Отсутсвует право на запись на хосту {host} в {perm_error}\n\n")

            else:

                result_check_permission = os.popen(f"ssh airflow_deploy@{host} find /app/airflow/{elem_list_folders} ! -perm 0775 ! -perm 0755")

                for_result_check_permission = result_check_permission.read().split("\n")

                for i_result_check_permission in for_result_check_permission:

                    if len(i_result_check_permission) > 2:

                        if "__pycache__" in i_result_check_permission:

                            continue

                        perm_error = os.popen(f"ssh airflow_deploy@{host} stat {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Отсутсвует право на запись на хосту {host} в {perm_error}\n\n")

    if configuration == "one-way":

        for elem_list_folders in list_folders:

            if elem_list_folders == "keytab":

                result_check_permission = os.popen(f"find /app/airflow/keytab -type f ! -perm 0060")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} stat {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Отсутсвует право на запись в {perm_error}\n\n")

            elif elem_list_folders == "keys":

                result_check_permission = os.popen(f"find /app/airflow/keys -type f ! -perm 0060")

                for_result_check_permission = result_check_permission.read().split("\n")

                if len(for_result_check_permission) > 1:

                    for i_result_check_permission in for_result_check_permission:

                        perm_error = os.popen(f"ssh airflow_deploy@{host} stat {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Отсутсвует право на запись в {perm_error}\n")

            else:

                result_check_permission = os.popen(f"find /app/airflow/{elem_list_folders} ! -perm 0775 ! -perm 0755")

                for_result_check_permission = result_check_permission.read().split("\n")

                for i_result_check_permission in for_result_check_permission:

                    if len(i_result_check_permission) > 2:

                        if "__pycache__" in i_result_check_permission:
                            continue

                        perm_error = os.popen(f"ssh airflow_deploy@{host} stat {i_result_check_permission}").read()

                        ALL_ERROR.put(f"Ошибка !!! Отсутсвует право на запись в {perm_error}\n\n")


### COPY OR REPLACE FILE
def copy_and_replace(source_path, destination_path):

    if os.path.exists(destination_path):

        os.remove(destination_path)

    shutil.copy2(source_path, destination_path)


### CHECK TYPE FILE IN DIRECTORY
def check_type_file(dir_folder, type_files, ALL_ERROR):

    for root, dirs, files in os.walk(dir_folder):

        for file in files:

            temp_file = f"{root}{file}"

            if dir_folder == "/app/airflow_deploy/dags/" and temp_file[:28] == "/app/airflow_deploy/dags/sql":
                continue

            if temp_file.rpartition(".")[2] not in type_files:

                file_extension = temp_file.rpartition(".")[2]

                if temp_file.startswith("/app/airflow_deploy/dags/sql"):

                    ALL_ERROR.put(f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение .sql)\n\n")

                if temp_file.startswith("/app/airflow_deploy/dags") and not temp_file.startswith("/app/airflow_deploy/dags/sql"):

                    ALL_ERROR.put(f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение .py)\n\n")

                elif temp_file.startswith("/app/airflow_deploy/keytab"):

                    ALL_ERROR.put(f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение .keytab)\n\n")

                elif temp_file.startswith("/app/airflow_deploy/scripts"):

                    ALL_ERROR.put(f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение .sh .json)\n\n")

                elif temp_file.startswith("/app/airflow_deploy/keys"):

                    ALL_ERROR.put(f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение .pfx .p12 .jks .secret)\n\n")

                elif temp_file.startswith("/app/airflow_deploy/csv"):

                    ALL_ERROR.put(f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение .csv)\n\n")

                elif temp_file.startswith("/app/airflow_deploy/jar"):

                    ALL_ERROR.put(f"Ошибка !!! Недопустимый тип файла {temp_file} для директории {dir_folder} (Допустимое расширение .jar)\n\n")


if "--skipped" not in sys.argv:

    for check_folder, check_extension in FOLDER_EXTENSION.items():

        check_type_file(check_folder, check_extension, ALL_ERROR)


### CHECK CONNECT TO HOST SSH
def connect_write(host, ALL_ERROR):

    data_connect_write = subprocess.Popen(f"ping -c 1 {host} ", shell=True,  stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if "Name or service not known" in data_connect_write.stderr.read().decode("utf-8"):

        ALL_ERROR.put(f"Ошибка !!! Проверьте доступ к хосту {host} \n")


### CHECK FREE SPACE
def check_free_space(data_host, ALL_ERROR):

    if configuration == "claster":

        result_command = os.popen(f"ssh airflow_deploy@{data_host} df /app  --output=avail,used | tail -n +2 | tr -d '%'").read()

        free_disk_space = int(result_command.split(" ")[0])

        used = int(result_command.split(" ")[1])

        total = free_disk_space + used

        one_percent = total / 100

        used_percent = int((used + size_airflow_deploy) / one_percent)

        if used_percent > CRITICAL_PERCENT:

           ALL_ERROR.put(f"Предупреждение !!! После добавления файлов на хост {data_host} колличество занятого места превысит 80% в каталоге /app на хосту {data_host}\n\n")

    else:

        result_command = os.popen(f"ssh airflow_deploy@{data_host} df /app  --output=avail,used | tail -n +2 | tr -d '%'").read()

        free_disk_space = int(result_command.split(" ")[0])

        used = int(result_command.split(" ")[1])

        total = free_disk_space + used

        one_percent = total / 100

        used_percent  = int((used + size_airflow_deploy) / one_percent)

        if used_percent > CRITICAL_PERCENT:

            ALL_ERROR.put(f"Предупреждение !!! После добавления файлов на хост {data_host} колличество занятого места превысит 80% в каталоге /app \n\n")


### MD5 FUNC
def md5(fname):

    hash_md5 = hashlib.md5()

    with open(fname, "rb") as f:

        for chunk in iter(lambda: f.read(4096), b""):

            hash_md5.update(chunk)

    return hash_md5.hexdigest()


### MAKE DICT CONTROL SUM
def path_sum_files():

    global PATH_SUM

    for list_folder in list_folders:

        for root, dirs, files in os.walk(f"/app/airflow_deploy/{list_folder}"):

            for file in files:

                PATH_SUM[f"{root}/{file}"] = md5(f"{root}/{file}")

path_sum_files()

#REMOVE DESTITINATION FOLDER
# def remove_destination_folder(hostname, result_queue):

#     for elem in list_folders:

#         if elem == "dags":

#             result_command_dag = list(os.popen(f"ssh airflow_deploy@{hostname} ls -a /app/airflow/{elem}/").read().split("\n"))

#             result_command_dag.remove(".")

#             result_command_dag.remove("..")

#             result_command_dag.remove("")

#             for elem_dags_dir in result_command_dag:

#                 if "__pycache__" in elem_dags_dir:

#                     continue

#                 result_command_dag = os.popen(f"ssh airflow_deploy@{hostname} rm -rfv /app/airflow/dags/{elem_dags_dir}").read()

#                 result_queue.put(result_command_dag)

#             result_command_dag = os.popen(f"ssh airflow_deploy@{hostname} rm -rfv /app/airflow/dags/sql/*").read() 

#             result_queue.put(result_command_dag)

#         else:

#             remote_list_files_folders = os.popen(f"ssh airflow_deploy@{hostname} ls -R /app/airflow/{elem}/").read()

#             result_command = list(os.popen(f"ssh airflow_deploy@{hostname} ls -a /app/airflow/{elem}/").read().split("\n"))

#             result_command.remove(".")

#             result_command.remove("..")

#             result_command.remove("")

#             for elem_result_command in result_command:

#                 result_command = os.popen(f"ssh airflow_deploy@{hostname} rm -rf /app/airflow/{elem}/{elem_result_command}").read()

#                 result_queue.put(result_command)


### SYNC DATA HOST
def rsync_host(hostname):

    for list_folder in list_folders:

        if list_folder == "keytab":

            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/keytab airflow_deploy@{hostname}:/app/airflow/ 2> /dev/null").read()

        elif list_folder == "keys":

            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/keys airflow_deploy@{hostname}:/app/airflow/ 2> /dev/null").read()

        else:

            result_command = os.popen(f"rsync --checksum -rogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{list_folder} airflow_deploy@{hostname}:/app/airflow/ 2> /dev/null").read()

        for root, dirs, files in os.walk(f"/app/airflow_deploy/{list_folder}"):

            for file in files:

                with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

                    current_datetime_now = datetime.datetime.now()

                    temp_file = f"{root}/{file}"

                    temp_file_airflow = "/app/airflow/" + f"{root}/{file}"[20:]

                    md5_sum = PATH_SUM[temp_file]

                    file_log.write(f"  Source: {root}/{file}  Destination: {hostname}@{temp_file_airflow}  Md5hash: {md5_sum}\n\n")


### CHEK SYNC DATA HOST
def check_rsync_host(hostname, ALL_ERROR):

    for list_folder in list_folders:

        if list_folder == "keytab":

            result_command_keytab = subprocess.Popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/keytab airflow_deploy@{hostname}:/app/airflow/", shell=True,  stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            result_command_keytab_string = result_command_keytab.stderr.read().decode("utf-8")

            if "rsync error" in result_command_keytab_string:

                ALL_ERROR.put(f"Ошибка!!! {hostname} \n {result_command_keytab_string}\n\n")

        elif list_folder == "keys":

            result_command_keys = subprocess.Popen(f"rsync --checksum -nrogp --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=,Fg=rw,Fu=,Fo= /app/airflow_deploy/keys airflow_deploy@{hostname}:/app/airflow/", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            result_command_keys_string = result_command_keys.stderr.read().decode("utf-8")

            if "rsync error" in result_command_keys_string:

                ALL_ERROR.put(f"Ошибка!!! {hostname} \n {result_command_keys_string}\n\n")

        else:

            result_command_other = subprocess.Popen(f"rsync --checksum -nrogp  --chown=airflow_deploy:airflow --chmod=Du=rwx,Dg=rwx,Do=rx,Fg=rwx,Fu=rwx,Fo=rx /app/airflow_deploy/{list_folder} airflow_deploy@{hostname}:/app/airflow/", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            result_command_other_string = result_command_other.stderr.read().decode("utf-8")

            if "rsync error" in result_command_other_string:

                ALL_ERROR.put(f"Ошибка!!! {hostname} {result_command_other_string} \n\n")


################################################################### STANDALONE
if configuration == "one-way":

    if "--skipped" not in sys.argv:

        check_param_run(ALL_ERROR)

        check_files_in_dirs(ALL_ERROR)

        check_free_space(current_hostname, ALL_ERROR)

        check_permissions(current_hostname, ALL_ERROR)

        check_groups_users(current_hostname, ALL_ERROR)

        check_rsync_host(current_hostname, ALL_ERROR)

    param_run_script()

    if ALL_ERROR.qsize() > 0:

        data_all_error = set(ALL_ERROR.get() for _ in range(ALL_ERROR.qsize()))

        with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

            current_datetime_now = datetime.datetime.now()

            file_log.write(f"******************************************* ALL ERORRS {current_datetime_now}*******************************************\n\n")

            for one_data_all_error in data_all_error:

                file_log.write(one_data_all_error)

            file_log.write(f"\n******************************************* END ALL ERORRS *******************************************\n\n")

            print("1")

            sys.exit(1)

    rsync_host(current_hostname)

    with open("/app/airflow_deploy/log/deploy.log", "a") as end_work_script:

        for i_REMOVE_FILES_FOLDERS in REMOVE_FILES_FOLDERS:

            end_work_script.write(f"{i_REMOVE_FILES_FOLDERS}\n")

    with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

        file_log.write(f"******************************************* End work script *******************************************\n")

    print("0")

    sys.exit(0)

################################################################### CLASTER
if configuration == "claster":
    print(2)
    all_hosts = schedulers + webs + workers
    print(all_hosts)
    if "--skipped" not in sys.argv:
        print(3)
        all_processes_connect_write = [Process(target=connect_write, args=(hostname, ALL_ERROR)) for hostname in all_hosts]

        for process_connect_write in all_processes_connect_write:

            process_connect_write.start()

        for process_connect_write in all_processes_connect_write:

            process_connect_write.join()
        print(4)
        check_param_run(ALL_ERROR)
        print(5)
        all_process_check_files_in_dirs = [Process(target=check_files_in_dirs, args=(ALL_ERROR,))]

        for process_check_files_in_dirs in all_process_check_files_in_dirs:

            process_check_files_in_dirs.start()

        for process_check_files_in_dirs in all_process_check_files_in_dirs:

            process_check_files_in_dirs.join()

        all_processes_check_free_space = [Process(target=check_free_space, args=(hostname, ALL_ERROR)) for hostname in all_hosts]

        for process_check_free_space in all_processes_check_free_space:

            process_check_free_space.start()

        for process_check_free_space in all_processes_check_free_space:

            process_check_free_space.join()

        all_processes_check_permissions = [Process(target=check_permissions, args=(hostname, ALL_ERROR)) for hostname in all_hosts]

        for process_check_permissions in all_processes_check_permissions:

            process_check_permissions.start()

        for process_check_permissions in all_processes_check_permissions:

            process_check_permissions.join()

        all_processes_check_groups_users = [Process(target=check_groups_users, args=(hostname, ALL_ERROR)) for hostname in all_hosts]
        
        for process_check_groups_users in all_processes_check_groups_users:

            process_check_groups_users.start()

        for process_check_groups_users in all_processes_check_groups_users:

            process_check_groups_users.join()

        for hostname in all_hosts:

            check_rsync_host(hostname, ALL_ERROR)
    print(6)
    param_run_script()

    if ALL_ERROR.qsize() > 0:

        data_all_error = [ALL_ERROR.get() for _ in range(ALL_ERROR.qsize())]

        with open("/app/airflow_deploy/log/deploy.log", "a") as file_log:

            current_datetime_now = datetime.datetime.now()

            file_log.write(f"******************************************* ALL ERORRS {current_datetime_now}*******************************************\n\n")

            for one_data_all_error in data_all_error:

                file_log.write(one_data_all_error)

            file_log.write(f"\n******************************************* END ALL ERORRS *******************************************\n\n")

            print("1")

            sys.exit(1)

    if len(sys.argv) == 2 and sys.argv[1] == '-c':

        for hostname in all_hosts:

            remove_destination_folder(hostname,result_queue)

        REMOVE_FILES_FOLDERS = result_queue.get()

    for hostname in all_hosts:

        rsync_host(hostname)

    with open("/app/airflow_deploy/log/deploy.log", "a") as end_work_script:

        if len(sys.argv)== 2 and sys.argv[1] == '-c':

            if  "removed directory '/app/airflow/dags/sql'\n" in REMOVE_FILES_FOLDERS:

                REMOVE_FILES_FOLDERS.remove("removed directory '/app/airflow/dags/sql'\n")

            end_work_script.write(f"Удаленные файлы: \n\n")

            for i_REMOVE_FILES_FOLDERS in REMOVE_FILES_FOLDERS:

                if i_REMOVE_FILES_FOLDERS  == "removed directory '/app/airflow/dags/sql'\n":

                    continue

                end_work_script.write(f"{i_REMOVE_FILES_FOLDERS}")

            end_work_script.write("\n")

        end_work_script.write(f"******************************************* End work script *******************************************\n\n")

        print("0")

        sys.exit(0)

