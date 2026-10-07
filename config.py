"""
config.py — 모든 접속 정보와 경로를 환경 변수에서 읽어오는 공통 설정
값은 .env 파일(로컬 실행) 또는 docker-compose의 environment(컨테이너 실행)로 주입한다.
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

DB_URL = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)


def psycopg2_kwargs() -> dict:
    """psycopg2.connect()에 바로 넘길 수 있는 접속 정보"""
    kwargs = dict(user=DB_USER, host=DB_HOST, port=DB_PORT, dbname=DB_NAME)
    if DB_PASSWORD:
        kwargs["password"] = DB_PASSWORD
    return kwargs


# ── 원본 데이터 ──────────────────────────────────────
RAW_DIR  = os.getenv("RAW_DIR", os.path.join(BASE_DIR, "data", "raw"))
BASE_URL = os.getenv("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")

# ── Kafka ────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "taxi-trips")
KAFKA_GROUP_ID  = os.getenv("KAFKA_GROUP_ID", "taxi-loader")
