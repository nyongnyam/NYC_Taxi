"""
dags/nyc_taxi_dag.py — Airflow DAG
매월 1일 오전 6시에 '지난달' 데이터를 수집·정제·적재하고 품질을 검사한다.

설정:
    - 프로젝트 경로: 환경 변수 NYC_TAXI_PROJECT_DIR (기본값: 이 파일의 상위 폴더)
    - DB 접속 정보와 적재 설정(COPY_WORKERS 등): 프로젝트의 .env
    - 이 파일을 ~/airflow/dags/ 에 복사하거나 심볼릭 링크로 연결해서 사용 (Airflow 2.x / 3.x)
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator

PROJECT_DIR = os.getenv(
    "NYC_TAXI_PROJECT_DIR",
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
)
sys.path.insert(0, PROJECT_DIR)

log = logging.getLogger(__name__)

default_args = {
    "owner":       "nyc-taxi",
    "retries":     1,
    "retry_delay": timedelta(minutes=5),
}


def _target_month(context) -> tuple[int, int]:
    """실행 시점 기준 '지난달' (Airflow 3에서는 execution_date 대신 logical_date)"""
    date = context.get("logical_date") or context["data_interval_end"]
    prev = date.replace(day=1) - timedelta(days=1)
    return prev.year, prev.month


def _connect():
    import psycopg2
    import config
    return psycopg2.connect(**config.psycopg2_kwargs())


def task_extract(**context):
    from pipeline import DataNotPublished, download
    year, month = _target_month(context)
    try:
        download(year, month)
    except DataNotPublished as e:
        raise AirflowSkipException(str(e))


def task_transform_load(**context):
    import config
    from pipeline import extract, load, transform

    year, month = _target_month(context)
    start = time.time()
    try:
        cleaned = transform(extract(year, [month]), year, [month])
        loaded = load(cleaned, year, [month], config.COPY_WORKERS)
        duration = round(time.time() - start, 1)
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (year, month, rows_loaded, status, duration_sec)
                VALUES (%s, %s, %s, 'success', %s)
            """, (year, month, loaded, duration))
        log.info(f"완료: {loaded:,}행 / {duration}초")
    except Exception as e:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_runs (year, month, status, error_msg)
                VALUES (%s, %s, 'failed', %s)
            """, (year, month, str(e)))
        raise


def task_quality_check(**context):
    year, month = _target_month(context)
    first_day = f"{year}-{month:02d}-01"

    with _connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM clean_taxi_trips
            WHERE tpep_pickup_datetime >= %s::date
              AND tpep_pickup_datetime <  %s::date + INTERVAL '1 month'
        """, (first_day, first_day))
        count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM clean_taxi_trips
            WHERE fare_amount <= 0 OR fare_amount > 1000
        """)
        bad_fare = cur.fetchone()[0]

    if count == 0:
        raise ValueError(f"{year}-{month:02d} 데이터 없음")
    if bad_fare > 0:
        raise ValueError(f"이상 요금 {bad_fare}건 감지")
    log.info(f"품질 검사 통과: {count:,}행, 이상 요금 0건")


with DAG(
    dag_id       = "nyc_taxi_local_etl",
    description  = "NYC Taxi 월별 ETL",
    start_date   = datetime(2024, 1, 1),
    schedule     = "0 6 1 * *",   # 매월 1일 06:00
    catchup      = False,
    default_args = default_args,
    tags         = ["nyc-taxi"],
) as dag:

    t1 = PythonOperator(task_id="extract",            python_callable=task_extract)
    t2 = PythonOperator(task_id="transform_and_load", python_callable=task_transform_load)
    t3 = PythonOperator(task_id="quality_check",      python_callable=task_quality_check)

    t1 >> t2 >> t3
