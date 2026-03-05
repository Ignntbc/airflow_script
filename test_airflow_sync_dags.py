

# @log_exceptions(log_message="Ошибка при обновлении содержимого директории", context_arg_name="host_name")
# def remote_update_items(elem: str, host_name: str, exclude_exts: Optional[list[str]] = None) -> None:
#     """
#     Копирует с заменой все файлы из основной директории на сервер, исключая __pycache__ и указанные расширения.
#     Для dags пропускает __pycache__, .pyc и exclude_exts.
#     """
#     save_log(f"Запуск копирования с заменой содержимого директории: {AIRFLOW_DEPLOY_PATH}{elem} -> {AIRFLOW_PATH}{elem} на хосте {host_name}", info_level=True)
#     src_dir = f"{AIRFLOW_DEPLOY_PATH}{elem}"
#     dst_dir = f"{AIRFLOW_PATH}{elem}"
#     exclude_args = ''
#     if exclude_exts:
#         exclude_args = ' '.join([f'--exclude="*{ext}"' for ext in exclude_exts])
#     chmod_string = get_chmod_string(elem)
#     host_prefix = f"airflow_deploy@{host_name}:"
#     if elem == "dags":
#         # Копируем все кроме __pycache__, .pyc и exclude_exts
#         rsync_cmd = (
#             f"rsync --checksum -rogp {exclude_args} --exclude='__pycache__' --exclude='*.pyc' {CHOWN_STRING} {chmod_string} "
#             f"{src_dir}/ {host_prefix}{dst_dir}"
#         )
#         result = run_command_with_log(rsync_cmd, f"Копирование с заменой: {src_dir} -> {dst_dir} на хосте {host_name}", info_level=True)
#         save_log(f"Результат копирования {src_dir} -> {dst_dir} на хосте {host_name}: {result.strip()}", info_level=True)
        
#         src_sql = f"{AIRFLOW_DEPLOY_PATH}dags/sql"
#         dst_sql = f"{AIRFLOW_PATH}dags/sql"
#         if os.path.exists(src_sql):
#             rsync_sql_cmd = (
#                 f"rsync --checksum -rogp {CHOWN_STRING} {chmod_string} {src_sql}/ {host_prefix}{dst_sql}"
#             )
#             result_sql = run_command_with_log(rsync_sql_cmd, f"Копирование SQL-файлов: {src_sql} -> {dst_sql} на хосте {host_name}", info_level=True)
#             save_log(f"Результат копирования SQL-файлов на хосте {host_name}: {result_sql.strip()}", info_level=True)
#         else:
#             save_log(f"Директория {src_sql} не найдена, копирование SQL-файлов пропущено", info_level=True)
#     else:
#         rsync_cmd = (
#             f"rsync --checksum -rogp {exclude_args} {CHOWN_STRING} {chmod_string} "
#             f"{src_dir}/ {host_prefix}{dst_dir}"
#         )
#         result = run_command_with_log(rsync_cmd, f"Копирование с заменой: {src_dir} -> {dst_dir} на хосте {host_name}", info_level=True)
#         save_log(f"Результат копирования {src_dir} -> {dst_dir} на хосте {host_name}: {result.strip()}", info_level=True)


# @log_exceptions(log_message="Ошибка при обновлении целевых папок")
# def update_destination_folders(exclude_exts: Optional[list[str]] = None) -> None:
#     """
#     Копирует с заменой все файлы из основной директории на сервер для всех целевых папок.
#     Для dags пропускает каталоги __pycache__, .pyc и exclude_exts.
#     """
#     save_log("Запуск копирования с заменой содержимого целевых папок на удалённых хостах airflow_deploy через ssh", info_level=True)
#     hosts = get_hosts()
#     for host_name in hosts:
#         save_log(f"Копирование с заменой на хосте: {host_name}", info_level=True)
#         for elem in list_folders:
#             remote_update_items(elem, host_name, exclude_exts)
#     save_log("Копирование с заменой содержимого целевых папок на удалённых хостах завершено успешно", info_level=True)
#     sys.exit(0)