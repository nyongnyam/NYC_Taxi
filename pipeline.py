"""
pipeline.py — NYC Yellow Taxi 배치 ETL (Extract → Transform → Load)

기본값은 '최적화 전' 동작이다. 병목 해결 기법은 옵션으로 하나씩 켤 수 있어서
같은 데이터로 전·후를 비교할 수 있다. 전체 비교는 benchmark.py가 자동으로 해 준다.

    python pipeline.py --year 2024 --month 1                      # 기존 방식 (캐싱 O, 전체 컬럼, multi INSERT)
    python pipeline.py --no-cache                                 # 캐싱도 끈 '원래 병목' 상태
    python pipeline.py --prune-columns                            # + 필요한 컬럼만 읽기
    python pipeline.py --prune-columns --load-method copy         # + COPY 적재
    python pipeline.py --limit 300000                             # 앞에서 N행만 (빠른 실험용)
    python pipeline.py --months 1-3 --prune-columns --load-method copy   # 여러 달 한 번에

각 단계마다 소요 시간과 최대 메모리(RSS)를 로그로 남긴다. (docs/BOTTLENECKS.md 참고)
"""

import argparse
import io
import json
import logging
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.request

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, inspect, text

import config
from metrics import stage, summarize

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

COPY_CHUNK_ROWS = int(os.getenv("COPY_CHUNK_ROWS", "200000"))


class DataNotPublished(Exception):
    """NYC TLC에 아직 공개되지 않은 월 (보통 2~3개월 딜레이)"""


def parse_months(text: str | int | list) -> list[int]:
    """'1' / '1-3' / '1,4,7' / 3 / [1, 2] → 월 목록"""
    if isinstance(text, int):
        return [text]
    if isinstance(text, list):
        return text
    months = []
    for part in str(text).split(","):
        if "-" in part:
            lo, hi = part.split("-")
            months += list(range(int(lo), int(hi) + 1))
        else:
            months.append(int(part))
    months = sorted(set(months))
    if not all(1 <= m <= 12 for m in months):
        raise ValueError(f"월은 1~12 사이여야 합니다: {text}")
    return months


def month_ranges(year: int, months: list[int]) -> list[tuple[str, str]]:
    """각 월의 [시작일, 다음 달 시작일) 범위"""
    return [(f"{year}-{m:02d}-01", f"{year + (m == 12)}-{m % 12 + 1:02d}-01") for m in months]


# ── Extract ──────────────────────────────────────────
def _fetch(url: str, dest: str):
    """dest.part로 받은 뒤 끝까지 성공했을 때만 dest로 이름을 바꾼다."""
    tmp_path = dest + ".part"
    try:
        urllib.request.urlretrieve(url, tmp_path)
    except urllib.error.HTTPError as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        if e.code in (403, 404):
            raise DataNotPublished(f"{os.path.basename(dest)} 이(가) 아직 공개되지 않음") from e
        raise
    os.replace(tmp_path, dest)


def download(year: int, month: int, use_cache: bool = True) -> str:
    """parquet 파일 경로를 반환한다.

    use_cache=True : data/raw/에 있으면 다운로드를 건너뛴다 (병목 해결 ①)
    use_cache=False: 매번 임시 폴더에 새로 받는다 (캐싱 도입 전 상태 재현)
    """
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{config.BASE_URL}/{filename}"

    if not use_cache:
        tmp_dir = tempfile.mkdtemp(prefix="nyctaxi_nocache_")
        local_path = os.path.join(tmp_dir, filename)
        log.info(f"다운로드 중 (캐시 미사용): {url}")
        _fetch(url, local_path)
        return local_path

    os.makedirs(config.RAW_DIR, exist_ok=True)
    local_path = os.path.join(config.RAW_DIR, filename)
    if os.path.exists(local_path):
        log.info(f"  이미 존재, 다운로드 건너뜀: {local_path}")
        return local_path

    log.info(f"다운로드 중: {url}")
    _fetch(url, local_path)
    log.info(f"  저장 완료: {local_path}")
    return local_path


def extract(year: int, months, limit: int | None = None,
            use_cache: bool = True, prune_columns: bool = False) -> pd.DataFrame:
    """months의 parquet을 읽어 하나의 DataFrame으로 합친다. limit은 '각 달에서 앞에서 N행'."""
    frames = []
    for month in parse_months(months):
        local_path = download(year, month, use_cache)

        columns = None
        if prune_columns:
            # parquet은 컬럼 단위로 저장되므로, 지정하지 않은 컬럼은 디스크에서 읽지도 않는다 (병목 해결 ②)
            schema  = pq.read_schema(local_path)
            columns = [name for name in schema.names if name.lower() in RAW_COLUMNS]
        df = pd.read_parquet(local_path, columns=columns)
        df.columns = df.columns.str.lower()  # 달마다 컬럼 대소문자가 다른 경우가 있음

        if not use_cache:
            shutil.rmtree(os.path.dirname(local_path), ignore_errors=True)
        if limit:
            df = df.head(limit)
        frames.append(df)

    df = frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
    del frames
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

    # 테이블에 있는 컬럼만 남긴다 (원본 19개 컬럼을 그대로 넣으면 적재가 실패함)
    return df[TABLE_COLUMNS]


def transform(df: pd.DataFrame, year: int, months) -> pd.DataFrame:
    log.info("정제 시작...")
    before = len(df)

    # 해당 연·월 데이터만 유지 (파일 안에 섞여 있는 다른 기간 레코드 제거)
    pickup = pd.to_datetime(df["tpep_pickup_datetime"])
    df = df[(pickup.dt.year == year) & (pickup.dt.month.isin(parse_months(months)))]

    df = clean(df)
    log.info(f"  정제 완료: {before:,} → {len(df):,}행")
    return df


# ── Load ─────────────────────────────────────────────
def copy_dataframe(dbapi_conn, df: pd.DataFrame, table: str = config.TABLE_NAME,
                   chunk_rows: int = COPY_CHUNK_ROWS):
    """PostgreSQL COPY로 DataFrame을 밀어 넣는다 (병목 해결 ③).

    chunk_rows > 0 이면 그 단위로 나눠 보내 CSV 버퍼 메모리를 줄인다 (병목 해결 ④).
    chunk_rows = 0 이면 전체를 한 번에 CSV로 만든다.
    같은 트랜잭션 안이라 나눠 보내도 원자성은 유지된다.
    """
    sql = f'COPY "{table}" ({", ".join(df.columns)}) FROM STDIN WITH (FORMAT csv)'
    step = chunk_rows if chunk_rows > 0 else max(len(df), 1)
    with dbapi_conn.cursor() as cur:
        for start in range(0, len(df), step):
            buf = io.StringIO()
            df.iloc[start:start + step].to_csv(buf, index=False, header=False)
            buf.seek(0)
            cur.copy_expert(sql, buf)


def delete_months(conn, year: int, months, table: str = config.TABLE_NAME) -> int:
    """같은 월을 다시 돌려도 중복 적재되지 않도록 해당 월을 먼저 삭제 (멱등성)"""
    if not inspect(conn).has_table(table):
        return 0
    deleted = 0
    for start, end in month_ranges(year, parse_months(months)):
        deleted += conn.execute(
            text(f'DELETE FROM "{table}" WHERE tpep_pickup_datetime >= :start AND tpep_pickup_datetime < :end'),
            {"start": start, "end": end},
        ).rowcount
    return deleted


def load(df: pd.DataFrame, year: int, months, method: str = "multi",
         chunk_rows: int = COPY_CHUNK_ROWS):
    log.info(f"PostgreSQL 적재 중... (방식: {method})")

    with engine.begin() as conn:
        deleted = delete_months(conn, year, months)
        if deleted:
            log.info(f"  기존 데이터 {deleted:,}행 삭제")

        if method == "copy":
            copy_dataframe(conn.connection, df, chunk_rows=chunk_rows)
        else:
            # multi : 기존 방식 — 5만 행씩 다중 행 INSERT
            # single: chunksize 없이 executemany — 메모리 병목 재현용
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
def run(year: int = 2024, months=1, load_method: str = "multi",
        limit: int | None = None, use_cache: bool = True, prune_columns: bool = False,
        copy_chunk_rows: int = COPY_CHUNK_ROWS) -> dict:
    months = parse_months(months)
    log.info(f"파이프라인 시작: {year}년 {months}월")
    metrics = {
        "engine": "pandas",
        "options": {
            "year": year, "months": months, "limit": limit, "use_cache": use_cache,
            "prune_columns": prune_columns, "load_method": load_method,
            "copy_chunk_rows": copy_chunk_rows if load_method == "copy" else None,
        },
        "stages": {},
    }

    with stage("extract", metrics):
        raw = extract(year, months, limit, use_cache, prune_columns)
    with stage("transform", metrics):
        cleaned = transform(raw, year, months)
    del raw  # 원본 DataFrame 메모리 해제
    with stage("load", metrics):
        load(cleaned, year, months, method=load_method, chunk_rows=copy_chunk_rows)

    return summarize(metrics, len(cleaned))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi 배치 ETL")
    parser.add_argument("--year",  type=int, default=2024)
    parser.add_argument("--month", "--months", dest="months", default="1",
                        help="처리할 월. 하나(1), 범위(1-3), 목록(1,4,7)")
    parser.add_argument("--limit", type=int, default=None, help="각 달에서 앞에서부터 N행만 처리 (빠른 실험용)")
    parser.add_argument("--no-cache", action="store_true",
                        help="다운로드 캐시를 쓰지 않고 매번 새로 받기 (캐싱 도입 전 상태)")
    parser.add_argument("--prune-columns", action="store_true",
                        help="필요한 8개 컬럼만 읽기")
    parser.add_argument("--load-method", default="multi", choices=["single", "multi", "copy"],
                        help="multi: 기존 방식(다중 행 INSERT) / copy: COPY / single: chunksize 없는 INSERT")
    parser.add_argument("--copy-chunk-rows", type=int, default=COPY_CHUNK_ROWS,
                        help="COPY를 나눠 보낼 행 수 (0 = 한 번에)")
    parser.add_argument("--metrics-out", default=None, help="측정 결과를 JSON으로 저장할 경로")
    args = parser.parse_args()

    try:
        result = run(args.year, args.months, args.load_method, args.limit,
                     use_cache=not args.no_cache, prune_columns=args.prune_columns,
                     copy_chunk_rows=args.copy_chunk_rows)
    except DataNotPublished as e:
        log.warning(f"건너뜀: {e}")
        sys.exit(0)

    if args.metrics_out:
        with open(args.metrics_out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
