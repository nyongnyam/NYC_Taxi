"""
pipeline.py — NYC Yellow Taxi 배치 ETL (Extract → Transform → Load)

실행 예:
    python pipeline.py --year 2024 --month 1
    python pipeline.py --year 2024 --month 1 --load-method copy
    python pipeline.py --year 2024 --month 1 --limit 200000   # 일부만 빠르게 실험

각 단계가 끝날 때마다 소요 시간과 프로세스 최대 메모리(RSS)를 로그로 남겨
어느 단계가 병목인지 바로 확인할 수 있다. (docs/BOTTLENECKS.md 참고)
"""

import argparse
import io
import logging
import os
import resource
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, inspect, text

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

engine = create_engine(config.DB_URL)

# 원본 parquet에서 실제로 쓰는 컬럼 (소문자 기준)
RAW_COLUMNS = [
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

# sql/setup.sql의 clean_taxi_trips 컬럼과 정확히 일치해야 한다 (loaded_at은 DB 기본값)
TABLE_COLUMNS = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "trip_duration_min",
    "pickup_hour",
    "pickup_weekday",
    "tip_rate",
]


class DataNotPublished(Exception):
    """NYC TLC에 아직 공개되지 않은 월 (보통 2~3개월 딜레이)"""


# ── 측정 도구 ────────────────────────────────────────
def peak_memory_mb() -> float:
    # Linux는 KB, macOS는 byte 단위로 반환
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024


@contextmanager
def stage(name: str, timings: dict):
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    timings[name] = elapsed
    log.info(f"  ⏱ {name}: {elapsed:.1f}초 (최대 메모리 {peak_memory_mb():,.0f}MB)")


# ── Extract ──────────────────────────────────────────
def download(year: int, month: int) -> str:
    """parquet 파일을 받아 로컬 경로를 반환. 이미 있으면 다운로드를 건너뛴다(캐싱)."""
    os.makedirs(config.RAW_DIR, exist_ok=True)
    filename   = f"yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = os.path.join(config.RAW_DIR, filename)

    if os.path.exists(local_path):
        log.info(f"  이미 존재, 다운로드 건너뜀: {local_path}")
        return local_path

    url = f"{config.BASE_URL}/{filename}"
    log.info(f"다운로드 중: {url}")
    tmp_path = local_path + ".part"
    try:
        urllib.request.urlretrieve(url, tmp_path)
    except urllib.error.HTTPError as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        if e.code in (403, 404):
            raise DataNotPublished(f"{year}-{month:02d} 데이터가 아직 공개되지 않음") from e
        raise
    # 다운로드가 끝까지 성공했을 때만 최종 이름으로 바꾼다.
    # 중간에 끊기면 .part만 남으므로 깨진 파일이 캐시로 재사용되지 않는다.
    os.replace(tmp_path, local_path)
    log.info(f"  저장 완료: {local_path}")
    return local_path


def extract(year: int, month: int, limit: int | None = None) -> pd.DataFrame:
    local_path = download(year, month)

    # 필요한 컬럼만 읽는다 (parquet은 컬럼 단위 저장이라 나머지는 디스크에서 읽지도 않음)
    schema  = pq.read_schema(local_path)
    columns = [name for name in schema.names if name.lower() in RAW_COLUMNS]
    df = pd.read_parquet(local_path, columns=columns)

    if limit:
        df = df.head(limit)
    log.info(f"  로드 완료: {len(df):,}행 × {len(df.columns)}컬럼")
    return df


# ── Transform ────────────────────────────────────────
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """행 단위 정제 + 파생 컬럼. 배치 파이프라인과 Kafka consumer가 함께 쓴다."""
    df = df.copy()
    df.columns = df.columns.str.lower()
    df = df.rename(columns={"vendorid": "vendor_id"})

    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # 이상값 제거
    df = df[df["trip_distance"]   > 0]
    df = df[df["fare_amount"]     > 0]
    df = df[df["total_amount"]    > 0]
    df = df[df["passenger_count"].between(1, 6)]

    # 파생 컬럼
    df = df.copy()
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)
    df["pickup_hour"]    = df["tpep_pickup_datetime"].dt.hour
    df["pickup_weekday"] = df["tpep_pickup_datetime"].dt.day_name()
    df["tip_rate"]       = (df["tip_amount"] / df["fare_amount"]).round(4)

    # 비정상 운행 제거
    df = df[df["trip_duration_min"].between(1, 180)]

    return df[TABLE_COLUMNS]


def transform(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    log.info("정제 시작...")
    before = len(df)

    # 해당 연·월 데이터만 유지 (파일 안에 섞여 있는 다른 기간 레코드 제거)
    pickup = pd.to_datetime(df["tpep_pickup_datetime"])
    df = df[(pickup.dt.year == year) & (pickup.dt.month == month)]

    df = clean(df)
    log.info(f"  정제 완료: {before:,} → {len(df):,}행")
    return df


# ── Load ─────────────────────────────────────────────
COPY_CHUNK_ROWS = int(os.getenv("COPY_CHUNK_ROWS", "200000"))


def copy_dataframe(dbapi_conn, df: pd.DataFrame, table: str = config.TABLE_NAME,
                   chunk_rows: int = COPY_CHUNK_ROWS):
    """PostgreSQL COPY로 DataFrame을 밀어 넣는다 (INSERT보다 훨씬 빠름).

    전체를 한 번에 CSV 문자열로 만들면 그 문자열만큼 메모리가 더 필요하므로
    chunk_rows 단위로 나눠 보낸다. 같은 트랜잭션 안이라 원자성은 그대로 유지된다.
    """
    sql = f'COPY "{table}" ({", ".join(df.columns)}) FROM STDIN WITH (FORMAT csv)'
    with dbapi_conn.cursor() as cur:
        for start in range(0, len(df), chunk_rows):
            buf = io.StringIO()
            df.iloc[start:start + chunk_rows].to_csv(buf, index=False, header=False)
            buf.seek(0)
            cur.copy_expert(sql, buf)


def load(df: pd.DataFrame, year: int, month: int, method: str = "multi"):
    log.info(f"PostgreSQL 적재 중... (방식: {method})")
    start = f"{year}-{month:02d}-01"
    end   = f"{year + (month == 12)}-{month % 12 + 1:02d}-01"

    with engine.begin() as conn:
        # 같은 월을 다시 돌려도 중복 적재되지 않도록 해당 월을 먼저 삭제 (멱등성)
        if inspect(conn).has_table(config.TABLE_NAME):
            deleted = conn.execute(
                text(f'DELETE FROM "{config.TABLE_NAME}" '
                     "WHERE tpep_pickup_datetime >= :start "
                     "AND tpep_pickup_datetime < :end"),
                {"start": start, "end": end},
            ).rowcount
            if deleted:
                log.info(f"  기존 {year}-{month:02d} 데이터 {deleted:,}행 삭제")

        if method == "copy":
            copy_dataframe(conn.connection, df)
        else:
            df.to_sql(
                config.TABLE_NAME,
                conn,
                if_exists = "append",
                index     = False,
                chunksize = 50_000 if method == "multi" else None,
                method    = "multi" if method == "multi" else None,
            )
    log.info(f"  적재 완료: {len(df):,}행")


# ── Run ──────────────────────────────────────────────
def run(year: int = 2024, month: int = 1, load_method: str = "multi",
        limit: int | None = None) -> int:
    log.info(f"파이프라인 시작: {year}-{month:02d}")
    timings: dict[str, float] = {}

    with stage("extract", timings):
        raw = extract(year, month, limit)
    with stage("transform", timings):
        cleaned = transform(raw, year, month)
    del raw  # 원본 DataFrame 메모리 해제
    with stage(f"load({load_method})", timings):
        load(cleaned, year, month, method=load_method)

    total = sum(timings.values())
    summary = " | ".join(f"{k} {v:.1f}s ({v / total:.0%})" for k, v in timings.items())
    log.info(f"파이프라인 완료: 총 {total:.1f}초 — {summary}")
    return len(cleaned)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi 배치 ETL")
    parser.add_argument("--year",  type=int, default=2024)
    parser.add_argument("--month", type=int, default=1, choices=range(1, 13))
    parser.add_argument("--load-method", default="multi",
                        choices=["single", "multi", "copy"],
                        help="single: 행 단위 INSERT / multi: 다중 행 INSERT(기존 방식) / copy: COPY")
    parser.add_argument("--limit", type=int, default=None,
                        help="앞에서부터 N행만 처리 (빠른 실험용)")
    args = parser.parse_args()

    try:
        run(args.year, args.month, args.load_method, args.limit)
    except DataNotPublished as e:
        log.warning(f"건너뜀: {e}")
