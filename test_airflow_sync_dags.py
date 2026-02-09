import sys
import pytest
from unittest.mock import patch, MagicMock

# Импортируем функции, которые будем тестировать
from airflow_sync_dags import check_configuratioon, setup_logger


def test_check_configuratioon_local():
    assert check_configuratioon("localexecutor") == "one-way"

def test_check_configuratioon_cluster():
    assert check_configuratioon("celery") == "cluster"


def test_setup_logger_creates_logger():
    logger = setup_logger()
    assert logger.name == "airflow_sync"


@patch("airflow_sync_dags.subprocess.run")
def test_dry_run_no_rsync(mock_run):
    # Пример: эмулируем запуск скрипта с --dry-run
    sys.argv = ["script.py", "--dry-run"]
    # Здесь должен быть вызов вашей основной функции, например main()
    # main()
    # mock_run.assert_not_called()  # rsync не должен вызываться
    pass  # Удалите, когда добавите main()

# Добавьте дополнительные тесты для других функций и сценариев
