"""
pipeline.py — NYC Yellow Taxi 월별 ETL (Extract → Transform → Load)

    python pipeline.py                         # 2024년 1월
    python pipeline.py --year 2024 --months 1-3
    python pipeline.py --workers 16            # DB 연결 수 직접 지정 (기본: .env의 COPY_WORKERS, 없으면 자동)

적용한 성능 기법 (전·후 측정은 local-bottleneck-lab 브랜치의 docs/BOTTLENECKS.md 참고)
    ① 다운로드 캐싱          : 받은 parquet은 다시 받지 않는다 (중간에 끊긴 파일은 캐시로 남기지 않음)
    ② 컬럼 프루닝            : parquet 19개 컬럼 중 필요한 8개만 읽는다 → 읽기 메모리 약 절반
    ③ PostgreSQL COPY        : INSERT 대신 COPY로 적재 → 적재 시간 90% 이상 감소
    ④ Arrow CSV 변환         : pandas.to_csv 대신 pyarrow.csv (C++) → CSV 변환 약 6배 빠름
    ⑤ 병렬 COPY              : DB 연결 여러 개로 동시에 COPY → 여러 CPU 코어 활용
    ⑥ 인덱스 지연 생성        : 대량 적재 시 인덱스를 뺐다가 적재 후 한 번에 생성
"""

import argparse
import io
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from sqlalchemy import create_engine, inspect, text

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

engine = create_engine(config.DB_URL)

# 원본 parquet에서 읽는 컬럼 (소문자 기준)
RAW_COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "tip_amount", "total_amount",
]

# sql/setup.sql의 clean_taxi_trips 컬럼과 같아야 한다 (loaded_at은 DB 기본값)
TABLE_COLUMNS = [
    "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "tip_amount", "total_amount",
    "trip_duration_min", "pickup_hour", "pickup_weekday", "tip_rate",
]

DEFER_INDEX_RATIO = 0.2  # auto 모드: 적재량이 기존 테이블의 20% 이상이면 인덱스를 나중에 만든다


class DataNotPublished(Exception):
    """NYC TLC에 아직 공개되지 않은 월 (보통 2~3개월 딜레이)"""


@contextmanager
def timed(name: str, timings: dict):
    start = time.perf_counter()
    yield
    timings[name] = time.perf_counter() - start
    log.info(f"  ⏱ {name}: {timings[name]:.1f}초")


def parse_months(text) -> list[int]:
    """'1' / '1-3' / '1,4,7' / 3 → 월 목록"""
    if isinstance(text, int):
        return [text]
    months = []
    for part in str(text).split(","):
        if "-" in part:
            lo, hi = part.split("-")
            months += range(int(lo), int(hi) + 1)
        else:
            months.append(int(part))
    months = sorted(set(months))
    if not all(1 <= m <= 12 for m in months):
        raise ValueError(f"월은 1~12 사이여야 합니다: {text}")
    return months


def month_ranges(year: int, months: list[int]) -> list[tuple[str, str]]:
    return [(f"{year}-{m:02d}-01", f"{year + (m == 12)}-{m % 12 + 1:02d}-01") for m in months]


# ── Extract ──────────────────────────────────────────
def download(year: int, month: int) -> str:
    """parquet 경로를 반환한다. 이미 받은 파일이면 다운로드하지 않는다. (①)"""
    os.makedirs(config.RAW_DIR, exist_ok=True)
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = os.path.join(config.RAW_DIR, filename)
    if os.path.exists(local_path):
        log.info(f"  이미 존재, 다운로드 건너뜀: {filename}")
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
    os.replace(tmp_path, local_path)  # 끝까지 받았을 때만 최종 이름으로 (깨진 파일이 캐시로 남지 않게)
    return local_path


def extract(year: int, months: list[int]) -> pd.DataFrame:
    frames = []
    for month in months:
        path = download(year, month)
        # 필요한 컬럼만 읽는다. parquet은 컬럼 단위 저장이라 나머지는 디스크에서 읽지도 않는다 (②)
        columns = [c for c in pq.read_schema(path).names if c.lower() in RAW_COLUMNS]
        df = pd.read_parquet(path, columns=columns)
        df.columns = df.columns.str.lower()  # 달마다 컬럼 대소문자가 다른 경우가 있음
        frames.append(df)
    df = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
    log.info(f"  로드 완료: {len(df):,}행")
    return df


# ── Transform ────────────────────────────────────────
def transform(df: pd.DataFrame, year: int, months: list[int]) -> pd.DataFrame:
    before = len(df)
    df = df.rename(columns={"vendorid": "vendor_id"})
    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    pickup = df["tpep_pickup_datetime"]
    df = df[(pickup.dt.year == year) & pickup.dt.month.isin(months)]  # 파일에 섞인 다른 달 제거

    # 이상값 제거
    df = df[(df["trip_distance"] > 0) & (df["fare_amount"] > 0) & (df["fare_amount"] <= 1000) & (df["total_amount"] > 0)]
    df = df[df["passenger_count"].between(1, 6)].copy()

    # 파생 컬럼
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
    ).round(2)
    df["pickup_hour"]    = df["tpep_pickup_datetime"].dt.hour
    df["pickup_weekday"] = df["tpep_pickup_datetime"].dt.day_name()
    df["tip_rate"]       = (df["tip_amount"] / df["fare_amount"]).round(4)

    df = df[df["trip_duration_min"].between(1, 180)]  # 비정상 운행 제거

    log.info(f"  정제 완료: {before:,} → {len(df):,}행")
    return df[TABLE_COLUMNS]


# ── Load ─────────────────────────────────────────────
def _copy_chunks(df: pd.DataFrame, chunks: list[tuple[int, int]]) -> int:
    """DB 연결 하나로 맡은 구간들을 COPY한다 (③). CSV 변환은 pyarrow로 한다 (④)."""
    sql = f'COPY "{config.TABLE_NAME}" ({", ".join(df.columns)}) FROM STDIN WITH (FORMAT csv)'
    conn = psycopg2.connect(**config.psycopg2_kwargs())
    try:
        with conn.cursor() as cur:
            for lo, hi in chunks:
                buf = io.BytesIO()
                pacsv.write_csv(pa.Table.from_pandas(df.iloc[lo:hi], preserve_index=False), buf,
                                pacsv.WriteOptions(include_header=False))
                buf.seek(0)
                cur.copy_expert(sql, buf)
        conn.commit()
    finally:
        conn.close()
    return sum(hi - lo for lo, hi in chunks)


def parallel_copy(df: pd.DataFrame, workers: int) -> int:
    """연결 workers개로 동시에 COPY한다 (⑤).

    PostgreSQL은 연결 하나를 프로세스(코어) 하나로 처리하므로 연결을 늘리면 여러 코어가 함께 받는다.
    pyarrow는 변환 중 파이썬 GIL을 풀어 주므로 스레드들이 실제로 동시에 돈다.
    """
    step = config.COPY_CHUNK_ROWS
    chunks = [(i, min(i + step, len(df))) for i in range(0, len(df), step)]
    groups = [g for g in (chunks[i::workers] for i in range(workers)) if g]
    with ThreadPoolExecutor(max_workers=len(groups) or 1) as pool:
        return sum(pool.map(lambda g: _copy_chunks(df, g), groups))


def _should_defer_indexes(conn, rows: int) -> bool:
    """인덱스 지연 생성은 적재량이 테이블에 비해 클 때만 이득이다 (⑥).

    큰 테이블에 조금 추가할 때 인덱스를 지웠다 다시 만들면 테이블 전체를 다시 훑어서 오히려 느려진다.
    (측정: 1,760만 행 테이블에 30만 행 추가 → 빈 테이블 1.0초, 큰 테이블 7.7초)
    """
    if config.DEFER_INDEXES in ("on", "true", "1"):
        return True
    if config.DEFER_INDEXES in ("off", "false", "0"):
        return False
    existing = conn.execute(text(
        "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = to_regclass(:t)"),
        {"t": config.TABLE_NAME}).scalar() or 0
    return rows >= existing * DEFER_INDEX_RATIO


def _delete_months(conn, year: int, months: list[int]) -> int:
    return sum(conn.execute(text(
        f'DELETE FROM "{config.TABLE_NAME}" '
        "WHERE tpep_pickup_datetime >= :s AND tpep_pickup_datetime < :e"),
        {"s": s, "e": e}).rowcount for s, e in month_ranges(year, months))


def load(df: pd.DataFrame, year: int, months: list[int], workers: int) -> int:
    workers = workers if workers > 0 else min(os.cpu_count() or 1, 8)

    with engine.begin() as conn:
        if not inspect(conn).has_table(config.TABLE_NAME):
            raise RuntimeError(f"{config.TABLE_NAME} 테이블이 없습니다. sql/setup.sql을 먼저 실행하세요.")
        # 같은 월을 다시 돌려도 중복되지 않도록 해당 월을 먼저 지운다 (멱등성)
        deleted = _delete_months(conn, year, months)
        indexes = []
        if _should_defer_indexes(conn, len(df)):
            indexes = conn.execute(text(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = :t"), {"t": config.TABLE_NAME}).fetchall()
            for name, _ in indexes:
                conn.execute(text(f'DROP INDEX IF EXISTS "{name}"'))
    if deleted:
        log.info(f"  기존 데이터 {deleted:,}행 삭제")
    log.info(f"PostgreSQL 적재 중... (연결 {workers}개, 인덱스 나중에 생성: {bool(indexes)})")

    try:
        loaded = parallel_copy(df, workers)
    except Exception:
        # 연결마다 따로 커밋하므로 일부만 들어갔을 수 있다 → 해당 월을 지워 깨끗한 상태로 되돌린다
        log.error("적재 실패: 일부만 들어간 데이터를 지웁니다")
        with engine.begin() as conn:
            _delete_months(conn, year, months)
        raise
    finally:
        if indexes:  # 성공하든 실패하든 인덱스는 반드시 되살린다
            t = time.perf_counter()
            with engine.begin() as conn:
                for _, ddl in indexes:
                    conn.execute(text(ddl))
            log.info(f"  인덱스 {len(indexes)}개 다시 생성: {time.perf_counter() - t:.1f}초")

    log.info(f"  적재 완료: {loaded:,}행")
    return loaded


# ── Run ──────────────────────────────────────────────
def run(year: int = 2024, months=1, workers: int = config.COPY_WORKERS) -> int:
    months = parse_months(months)
    log.info(f"파이프라인 시작: {year}년 {months}월")
    timings: dict[str, float] = {}
    with timed("extract", timings):
        raw = extract(year, months)
    with timed("transform", timings):
        cleaned = transform(raw, year, months)
    del raw
    with timed("load", timings):
        loaded = load(cleaned, year, months, workers)
    total = sum(timings.values())
    log.info(f"파이프라인 완료: 총 {total:.1f}초 — "
             + " | ".join(f"{k} {v:.1f}s ({v / total:.0%})" for k, v in timings.items()))
    return loaded


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi 월별 ETL")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", "--months", dest="months", default="1",
                        help="처리할 월. 하나(1), 범위(1-3), 목록(1,4,7)")
    parser.add_argument("--workers", type=int, default=config.COPY_WORKERS,
                        help="동시에 COPY할 DB 연결 수 (0 = 자동: 코어 수, 최대 8)")
    args = parser.parse_args()
    try:
        run(args.year, args.months, args.workers)
    except DataNotPublished as e:
        log.warning(f"건너뜀: {e}")
        sys.exit(0)
