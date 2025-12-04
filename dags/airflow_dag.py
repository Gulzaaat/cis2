from datetime import datetime, timedelta
import logging
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is on PYTHONPATH so "src" is importable
import sys

PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.scraper import run_scraper
from src.cleaner import run_cleaner
from src.loader import run_loader

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sulpak_pipeline",
    default_args=default_args,
    description="Scrape Sulpak, clean data, and load into SQLite",
    schedule_interval="@daily",  # no more than once every 24 hours
    start_date=datetime(2025, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sulpak", "selenium", "sqlite"],
) as dag:

    def _scrape_task(**context):
        category_url = "https://www.sulpak.kz/f/smartfoniy"
        logger.info("Starting scraper for %s", category_url)
        run_scraper(category_url=category_url, output_path="data/raw_products.csv")

    def _clean_task(**context):
        logger.info("Starting cleaner")
        run_cleaner()

    def _load_task(**context):
        logger.info("Starting loader")
        run_loader()

    scrape_op = PythonOperator(
        task_id="scrape_sulpak",
        python_callable=_scrape_task,
    )

    clean_op = PythonOperator(
        task_id="clean_products",
        python_callable=_clean_task,
    )

    load_op = PythonOperator(
        task_id="load_to_sqlite",
        python_callable=_load_task,
    )

    scrape_op >> clean_op >> load_op
