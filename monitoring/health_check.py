"""
monitoring/health_check.py — 파이프라인 상태 확인
실행: python monitoring/health_check.py
결과는 터미널 출력 + monitoring/pipeline.log 파일에 저장됩니다.
"""

import psycopg2
import logging
import os
from datetime import datetime, timedelta

# 로그 파일 설정
LOG_DIR  = os.path.dirname(__file__)
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    handlers= [
        logging.FileHandler(LOG_FILE),   # 파일 저장
        logging.StreamHandler(),          # 터미널 출력
    ]
)
log = logging.getLogger(__name__)

DB_URL = "postgresql://hyukjunc@localhost:5432/nyctaxi"  # 수정 필요


def get_conn():
    return psycopg2.connect(DB_URL)


def check_row_count(conn) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM clean_taxi_trips
        WHERE loaded_at >= NOW() - INTERVAL '48 hours'
    """)
    count = cur.fetchone()[0]
    cur.close()
    passed = count > 0
    status = "PASS" if passed else "FAIL"
    log.info(f"[{status}] 최근 48시간 적재 행 수: {count:,}행")
    return passed


def check_null_rate(conn) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT ROUND(
            100.0 * COUNT(*) FILTER (WHERE fare_amount IS NULL)
            / NULLIF(COUNT(*), 0), 2
        ) FROM clean_taxi_trips
    """)
    rate   = cur.fetchone()[0] or 0
    cur.close()
    passed = float(rate) < 1.0
    status = "PASS" if passed else "FAIL"
    log.info(f"[{status}] 결측률: {rate}%")
    return passed


def check_avg_fare(conn) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT ROUND(AVG(fare_amount)::NUMERIC, 2) FROM clean_taxi_trips")
    avg    = cur.fetchone()[0] or 0
    cur.close()
    passed = 5.0 <= float(avg) <= 100.0
    status = "PASS" if passed else "FAIL"
    log.info(f"[{status}] 평균 요금: ${avg} (정상 범위 $5~$100)")
    return passed


def check_last_pipeline_run(conn) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT status, created_at FROM pipeline_runs
        ORDER BY created_at DESC LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()

    if not row:
        log.warning("[FAIL] 파이프라인 실행 이력 없음")
        return False

    status_val, last_run = row
    hours_ago = (datetime.now() - last_run).total_seconds() / 3600
    passed    = status_val == "success"
    status    = "PASS" if passed else "FAIL"
    log.info(f"[{status}] 마지막 실행: {hours_ago:.1f}시간 전 ({status_val})")
    return passed


def run():
    log.info("=" * 50)
    log.info("헬스 체크 시작")

    try:
        conn = get_conn()
    except Exception as e:
        log.error(f"DB 연결 실패: {e}")
        return

    results = [
        check_row_count(conn),
        check_null_rate(conn),
        check_avg_fare(conn),
        check_last_pipeline_run(conn),
    ]
    conn.close()

    passed = all(results)
    summary = "전체 통과" if passed else f"{results.count(False)}개 실패"
    log.info(f"헬스 체크 완료: {summary}")
    log.info("=" * 50)
    return passed


if __name__ == "__main__":
    run()
