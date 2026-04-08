"""
dags/nyc_taxi_dag.py — Airflow DAG (로컬 전용)
매월 1일 오전 6시에 자동 실행

파일 위치: ~/airflow/dags/nyc_taxi_dag.py
"""

import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# pipeline.py 경로 추가 (본인 경로로 수정)
PROJECT_DIR = os.path.expanduser("~/작업물/nyc_taxi_local")
sys.path.insert(0, PROJECT_DIR)

log = logging.getLogger(__name__)

default_args = {
    "owner":        "local",
    "retries":      1,
    "retry_delay":  timedelta(minutes=5),
}


def task_extract(**context):
    from pipeline import extract
    year  = context["execution_date"].year
    month = context["execution_date"].month
    df    = extract(year, month)
    # XCom으로 행 수만 전달 (DataFrame은 직렬화 불가)
    context["ti"].xcom_push(key="raw_rows", value=len(df))


def task_transform_load(**context):
    import time
    from pipeline import extract, transform, load
    import psycopg2

    year  = context["execution_date"].year
    month = context["execution_date"].month

    start = time.time()
    try:
        df      = extract(year, month)
        cleaned = transform(df)
        load(cleaned)
        duration = round(time.time() - start, 1)

        # 실행 이력 기록
        conn = psycopg2.connect(
            f"postgresql://hyukjunc@localhost:5432/nyctaxi"  # 수정 필요
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_runs (year, month, rows_loaded, status, duration_sec)
            VALUES (%s, %s, %s, 'success', %s)
        """, (year, month, len(cleaned), duration))
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"완료: {len(cleaned):,}행 / {duration}초")

    except Exception as e:
        conn = psycopg2.connect(
            f"postgresql://hyukjunc@localhost:5432/nyctaxi"
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_runs (year, month, status, error_msg)
            VALUES (%s, %s, 'failed', %s)
        """, (year, month, str(e)))
        conn.commit()
        cur.close()
        conn.close()
        raise


def task_quality_check(**context):
    import psycopg2
    year  = context["execution_date"].year
    month = context["execution_date"].month

    conn = psycopg2.connect(
        f"postgresql://hyukjunc@localhost:5432/nyctaxi"
    )
    cur = conn.cursor()

    # 행 수 확인
    cur.execute("""
        SELECT COUNT(*) FROM clean_taxi_trips
        WHERE DATE_TRUNC('month', tpep_pickup_datetime)
              = %s::date
    """, (f"{year}-{month:02d}-01",))
    count = cur.fetchone()[0]

    # 이상 요금 확인
    cur.execute("""
        SELECT COUNT(*) FROM clean_taxi_trips
        WHERE fare_amount <= 0 OR fare_amount > 1000
    """)
    bad_fare = cur.fetchone()[0]

    cur.close()
    conn.close()

    if count == 0:
        raise ValueError(f"{year}-{month:02d} 데이터 없음")
    if bad_fare > 0:
        raise ValueError(f"이상 요금 {bad_fare}건 감지")

    log.info(f"품질 검사 통과: {count:,}행, 이상 요금 0건")


with DAG(
    dag_id           = "nyc_taxi_local_etl",
    description      = "NYC Taxi 월별 로컬 ETL",
    start_date       = datetime(2024, 1, 1),
    schedule= "0 6 1 * *",   # 매월 1일 06:00
    catchup          = False,
    default_args     = default_args,
    tags             = ["nyc-taxi", "local"],
) as dag:

    t1 = PythonOperator(
        task_id         = "extract",
        python_callable = task_extract,
    )
    t2 = PythonOperator(
        task_id         = "transform_and_load",
        python_callable = task_transform_load,
    )
    t3 = PythonOperator(
        task_id         = "quality_check",
        python_callable = task_quality_check,
    )

    t1 >> t2 >> t3
