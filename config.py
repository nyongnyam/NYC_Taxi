"""
config.py — 접속 정보·경로·성능 설정을 환경 변수(.env)에서 읽는다.
"""

import os

from dotenv import load_dotenv
from sqlalchemy.engine import URL

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── PostgreSQL ───────────────────────────────────────
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD") or None
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME", "nyctaxi")
TABLE_NAME  = os.getenv("TABLE_NAME", "clean_taxi_trips")

# SQLAlchemy 2.1부터 'postgresql://'의 기본 드라이버가 바뀌었으므로 psycopg2를 명시한다
DB_URL = URL.create(
    "postgresql+psycopg2",
    username=DB_USER, password=DB_PASSWORD,
    host=DB_HOST, port=DB_PORT, database=DB_NAME,
)


def psycopg2_kwargs() -> dict:
    kwargs = dict(user=DB_USER, host=DB_HOST, port=DB_PORT, dbname=DB_NAME)
    if DB_PASSWORD:
        kwargs["password"] = DB_PASSWORD
    return kwargs


# ── 원본 데이터 ──────────────────────────────────────
RAW_DIR  = os.getenv("RAW_DIR", os.path.join(BASE_DIR, "data", "raw"))
BASE_URL = os.getenv("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")

# ── 적재 성능 ────────────────────────────────────────
# 동시에 COPY할 DB 연결 수. 0 = 자동(CPU 코어 수, 최대 8)
COPY_WORKERS    = int(os.getenv("COPY_WORKERS", "0") or 0)
# 연결 하나가 한 번에 보내는 행 수
COPY_CHUNK_ROWS = int(os.getenv("COPY_CHUNK_ROWS", "200000"))
# 인덱스를 적재 후에 다시 만들지: auto(적재량이 테이블의 20% 이상일 때만) / on / off
DEFER_INDEXES   = os.getenv("DEFER_INDEXES", "auto").lower()
