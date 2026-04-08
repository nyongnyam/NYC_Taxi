import os
import urllib.request
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── 설정 ─────────────────────────────────────────────
DB_USER = "hyukjunc"       # whoami 결과로 수정
DB_NAME = "nyctaxi"
DB_URL  = f"postgresql://{DB_USER}@localhost:5432/{DB_NAME}"
RAW_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")

engine = create_engine(DB_URL)


# ── Extract ──────────────────────────────────────────
def extract(year: int, month: int) -> pd.DataFrame:
    filename   = f"yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = os.path.join(RAW_DIR, filename)

    if not os.path.exists(local_path):
        url = (f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}")
        log.info(f"다운로드 중: {url}")
        urllib.request.urlretrieve(url, local_path)
        log.info(f"  저장 완료: {local_path}")
    else:
        log.info(f"  이미 존재, 다운로드 건너뜀: {local_path}")

    df = pd.read_parquet(local_path)
    log.info(f"  로드 완료: {len(df):,}행")
    return df


# ── Transform ────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("정제 시작...")
    before = len(df)

    df.columns = df.columns.str.lower()
    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df = df[df["tpep_pickup_datetime"].dt.year == 2024]
    # 이상값 제거
    df = df[df["trip_distance"]   > 0]
    df = df[df["fare_amount"]     > 0]
    df = df[df["total_amount"]    > 0]
    df = df[df["passenger_count"].between(1, 6)]

    # 파생 컬럼
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)
    df["pickup_hour"]    = df["tpep_pickup_datetime"].dt.hour
    df["pickup_weekday"] = df["tpep_pickup_datetime"].dt.day_name()
    df["tip_rate"]       = (df["tip_amount"] / df["fare_amount"]).round(4)

    # 비정상 운행 제거
    df = df[df["trip_duration_min"].between(1, 180)]

    log.info(f"  정제 완료: {before:,} → {len(df):,}행")
    return df


# ── Load ─────────────────────────────────────────────
def load(df: pd.DataFrame):
    log.info("PostgreSQL 적재 중...")
    df.to_sql(
        "clean_taxi_trips",
        engine,
        if_exists = "append",
        index     = False,
        chunksize = 50_000,
        method    = "multi",
    )
    log.info(f"  적재 완료: {len(df):,}행")


# ── Run ──────────────────────────────────────────────
def run(year: int = 2024, month: int = 1):
    log.info(f"파이프라인 시작: {year}-{month:02d}")
    raw     = extract(year, month)
    cleaned = transform(raw)
    load(cleaned)
    log.info("파이프라인 완료")


if __name__ == "__main__":
    run(year=2024, month=1)
