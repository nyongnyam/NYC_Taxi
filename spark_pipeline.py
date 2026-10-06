"""
spark_pipeline.py — 같은 ETL을 Apache Spark(PySpark)로 처리한다.

    python spark_pipeline.py --months 1                    # raw: 정제한 모든 행을 clean_taxi_trips에 적재
    python spark_pipeline.py --months 1-6 --mode agg       # agg: 시간대·요일별 집계 결과만 적재
    python spark_pipeline.py --months 1 --limit 300000     # 각 달에서 앞의 N행만 (빠른 실험용)
    python spark_pipeline.py --master "local[8]"            # 코어 8개 사용 (기본은 최대 4개)
    python spark_pipeline.py --master spark://spark-master:7077   # 클러스터로 실행 (docker compose의 spark 프로필)

pandas 파이프라인과 다른 점:
    - 지연 실행(lazy): 읽기·정제는 계획만 세우고, 실제 계산은 적재(쓰기)를 시작할 때 한꺼번에 일어난다.
      그래서 측정 구간이 startup(세션 준비)과 process(읽기+정제+적재)로 나뉜다.
    - 파티션 단위 처리: 데이터를 파티션으로 나눠 코어마다 동시에 처리하고,
      raw 모드에서는 파티션마다 DB 연결을 따로 열어 COPY를 병렬로 보낸다.
      (파티션마다 따로 커밋하므로 중간에 실패하면 일부만 적재될 수 있다 — pandas 쪽 단일 트랜잭션과의 차이)
    - 메모리: 전체 데이터를 한 번에 올리지 않고 파티션 단위로 흘려보내므로, 데이터가 메모리보다 커도 처리할 수 있다.
"""

import argparse
import json
import logging
import os

import psycopg2

import config
from metrics import stage, summarize
from pipeline import DataNotPublished, RAW_COLUMNS, TABLE_COLUMNS, download, month_ranges, parse_months

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark_pipeline")

AGG_SQL = os.path.join(config.BASE_DIR, "sql", "agg_tables.sql")


def build_session(master: str, driver_memory: str, shuffle_partitions: int):
    import socket

    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName("nyc-taxi-etl").master(master)
    if master.startswith("spark://"):
        # 클러스터 모드: 워커(executor)들이 이 드라이버에 다시 접속할 수 있도록 IP를 알려 준다
        builder = (builder
                   .config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
                   .config("spark.driver.bindAddress", "0.0.0.0")
                   .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g")))
    spark = (
        builder
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")          # 시간을 변환 없이 그대로 다룬다
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", 50_000)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_months(spark, year: int, months: list[int], limit: int | None):
    """달마다 파일을 읽어 필요한 컬럼만 같은 타입으로 맞춘 뒤 합친다 (달마다 스키마가 조금씩 다름)."""
    from pyspark.sql import functions as F

    frames = []
    for month in months:
        path = download(year, month, use_cache=True)
        df = spark.read.parquet(path)
        cols = {c.lower(): c for c in df.columns}
        df = df.select(
            F.col(cols["vendorid"]).cast("int").alias("vendor_id"),
            F.col(cols["tpep_pickup_datetime"]).cast("timestamp").alias("tpep_pickup_datetime"),
            F.col(cols["tpep_dropoff_datetime"]).cast("timestamp").alias("tpep_dropoff_datetime"),
            *[F.col(cols[c]).cast("double").alias(c) for c in RAW_COLUMNS[3:]],
        )
        if limit:
            # 그냥 limit()을 쓰면 먼저 끝난 파티션의 행을 가져와서 매번 다른 행이 뽑힌다.
            # 파일 순서대로 번호를 매긴 뒤 앞에서 N행을 가져와 pandas의 head()와 같은 행을 쓴다.
            df = (df.withColumn("_row_id", F.monotonically_increasing_id())
                    .orderBy("_row_id").limit(limit).drop("_row_id"))
        frames.append(df)

    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other)
    return df


def transform(df, year: int, months: list[int]):
    """pipeline.clean()과 같은 규칙을 Spark 함수로 옮긴 것"""
    from pyspark.sql import functions as F

    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    df = (
        df.where((F.year(pickup) == year) & F.month(pickup).isin(months))
        .where((F.col("trip_distance") > 0) & (F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
        .where(F.col("passenger_count").between(1, 6))
        .withColumn("trip_duration_min", F.round((F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60, 2))
        .withColumn("pickup_hour", F.hour(pickup))
        .withColumn("pickup_weekday", F.date_format(pickup, "EEEE"))
        .withColumn("tip_rate", F.round(F.col("tip_amount") / F.col("fare_amount"), 4))
        .where(F.col("trip_duration_min").between(1, 180))
    )
    return df.select(*TABLE_COLUMNS)


def _connect():
    return psycopg2.connect(**config.psycopg2_kwargs())


def load_raw(df, year: int, months: list[int], partitions: int, defer_indexes: bool = False) -> int:
    """파티션마다 DB 연결을 열어 COPY를 병렬로 보낸다.

    Spark → 파이썬 워커로 넘길 때 pandas를 거치지 않고 Arrow 배치를 그대로 받아(mapInArrow)
    pyarrow로 바로 CSV를 만든다. pandas.to_csv보다 몇 배 빠르다.
    """
    from sqlalchemy import create_engine

    from pipeline import drop_indexes

    deleted = 0
    with _connect() as conn, conn.cursor() as cur:
        for start, end in month_ranges(year, months):
            cur.execute(f'DELETE FROM "{config.TABLE_NAME}" WHERE tpep_pickup_datetime >= %s '
                        "AND tpep_pickup_datetime < %s", (start, end))
            deleted += cur.rowcount
    if deleted:
        log.info(f"  기존 데이터 {deleted:,}행 삭제")

    index_defs = []
    if defer_indexes:
        with create_engine(config.DB_URL).begin() as conn:
            index_defs = drop_indexes(conn)

    conn_kwargs = config.psycopg2_kwargs()
    table = config.TABLE_NAME
    copy_sql = f'COPY "{table}" ({", ".join(TABLE_COLUMNS)}) FROM STDIN WITH (FORMAT csv)'

    def write_partition(batches):
        # 이 함수는 Spark 워커 프로세스 안에서 파티션마다 한 번 실행된다
        import io

        import psycopg2 as pg
        import pyarrow as pa
        import pyarrow.csv as pacsv

        conn = pg.connect(**conn_kwargs)
        written = 0
        try:
            with conn.cursor() as cur:
                for batch in batches:
                    if batch.num_rows == 0:
                        continue
                    # 시간 컬럼의 시간대 표시(UTC)를 떼어 PostgreSQL TIMESTAMP 형식으로 맞춘다
                    cols = [c.cast(pa.timestamp("us")) if pa.types.is_timestamp(c.type) else c
                            for c in batch.columns]
                    buf = io.BytesIO()
                    pacsv.write_csv(pa.RecordBatch.from_arrays(cols, names=batch.schema.names), buf,
                                    pacsv.WriteOptions(include_header=False))
                    buf.seek(0)
                    cur.copy_expert(copy_sql, buf)
                    written += batch.num_rows
            conn.commit()
        finally:
            conn.close()
        yield pa.RecordBatch.from_pydict({"rows": [written]})

    log.info(f"PostgreSQL 적재 중... (Spark 파티션 {partitions}개가 병렬로 COPY)")
    result = df.repartition(partitions).mapInArrow(write_partition, schema="rows long")
    loaded = result.groupBy().sum("rows").collect()[0][0] or 0

    if index_defs:
        import time
        t = time.perf_counter()
        with create_engine(config.DB_URL).begin() as conn:
            from sqlalchemy import text
            for ddl in index_defs:
                conn.execute(text(ddl))
        log.info(f"  인덱스 {len(index_defs)}개 다시 생성: {time.perf_counter() - t:.1f}초")
    log.info(f"  적재 완료: {loaded:,}행")
    return loaded


def load_agg(df, year: int, months: list[int]) -> int:
    """원본 행 대신 월·시간대·요일별 집계만 적재한다 (합계와 건수를 저장해 여러 달을 다시 합칠 수 있게)."""
    from pyspark.sql import functions as F

    month_col = F.trunc("tpep_pickup_datetime", "month").alias("month")
    measures = [
        F.count("*").alias("trip_count"),
        F.sum("fare_amount").alias("fare_sum"),
        F.sum("trip_duration_min").alias("duration_sum"),
        F.sum("tip_rate").alias("tip_rate_sum"),
        F.sum("total_amount").alias("revenue_sum"),
    ]
    df = df.cache()  # 두 가지 집계에 같은 데이터를 쓰므로 한 번만 계산
    hourly = df.groupBy(month_col, "pickup_hour").agg(*measures).toPandas()
    weekday = df.groupBy(month_col, "pickup_weekday").agg(*measures).toPandas()
    df.unpersist()

    log.info(f"PostgreSQL 적재 중... (집계 결과 {len(hourly) + len(weekday):,}행)")
    with _connect() as conn, conn.cursor() as cur:
        with open(AGG_SQL, encoding="utf-8") as f:
            cur.execute(f.read())
        for table, frame in (("agg_hourly_monthly", hourly), ("agg_weekday_monthly", weekday)):
            for start, end in month_ranges(year, months):
                cur.execute(f"DELETE FROM {table} WHERE month >= %s AND month < %s", (start, end))
            rows = frame.astype(object).where(frame.notna(), None).values.tolist()  # numpy 값 → 파이썬 값
            cols = ", ".join(frame.columns)
            placeholders = ", ".join(["%s"] * len(frame.columns))
            cur.executemany(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", rows)
    loaded = len(hourly) + len(weekday)
    log.info(f"  적재 완료: {loaded:,}행 (원본 {int(hourly['trip_count'].sum()):,}건을 요약)")
    return loaded


def default_master() -> str:
    """기본은 이 컴퓨터의 코어 중 최대 SPARK_LOCAL_CORES(기본 4)개만 쓴다.

    local[*]는 모든 코어를 쓰는데, 코어마다 파이썬 워커가 하나씩 떠서 메모리를 쓴다.
    코어가 28개인 PC에서는 워커 28개가 동시에 떠서 작은 데이터에도 메모리 한도를 넘길 수 있다.
    """
    env = os.getenv("SPARK_MASTER", "").strip()
    if env:
        return env
    cores = min(os.cpu_count() or 1, int(os.getenv("SPARK_LOCAL_CORES", "4")))
    return f"local[{cores}]"


def auto_driver_memory(cores: int) -> str:
    """코어당 약 512MB를 주되, 1GB 이상·이 컴퓨터(컨테이너) 메모리의 40% 이하로 맞춘다."""
    import psutil
    total_gb = psutil.virtual_memory().total / 1024**3
    try:  # 컨테이너에 메모리 제한이 걸려 있으면 그 값을 쓴다
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw != "max":
            total_gb = min(total_gb, int(raw) / 1024**3)
    except OSError:
        pass
    gb = max(1, min(int(cores * 0.5), int(total_gb * 0.4)))
    return f"{gb}g"


def parallelism_of(master: str) -> int:
    """master 문자열에서 동시에 처리할 작업 수를 계산 (클러스터면 4를 기본으로)"""
    if master.startswith("local["):
        n = master[len("local["):-1]
        return (os.cpu_count() or 1) if n == "*" else int(n)
    return 4


def run(year: int, months, mode: str = "raw", limit: int | None = None, master: str | None = None,
        driver_memory: str = "auto", partitions: int | None = None, defer_indexes: bool = False) -> dict:
    months = parse_months(months)
    master = master or default_master()
    partitions = partitions or parallelism_of(master)
    if driver_memory == "auto":
        driver_memory = auto_driver_memory(parallelism_of(master))
    log.info(f"Spark 파이프라인 시작: {year}년 {months}월, mode={mode}, master={master}, 메모리={driver_memory}")
    metrics = {
        "engine": "spark",
        "options": {"year": year, "months": months, "limit": limit, "mode": mode, "master": master,
                    "driver_memory": driver_memory, "partitions": partitions, "defer_indexes": defer_indexes},
        "stages": {},
    }

    with stage("startup", metrics):
        spark = build_session(master, driver_memory, partitions)
    try:
        with stage("process", metrics):  # 읽기 + 정제 + 적재 (지연 실행이라 여기서 한꺼번에 실행됨)
            df = transform(read_months(spark, year, months, limit), year, months)
            loaded = (load_agg(df, year, months) if mode == "agg"
                      else load_raw(df, year, months, partitions, defer_indexes))
    finally:
        spark.stop()

    return summarize(metrics, loaded)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi ETL — Spark 버전")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", "--months", dest="months", default="1",
                        help="처리할 월. 하나(1), 범위(1-3), 목록(1,4,7)")
    parser.add_argument("--limit", type=int, default=None, help="각 달에서 앞에서부터 N행만 처리")
    parser.add_argument("--mode", choices=["raw", "agg"], default="raw",
                        help="raw: 정제한 모든 행 적재 / agg: 시간대·요일별 집계만 적재")
    parser.add_argument("--master", default=None,
                        help="local[4] = 이 컴퓨터의 코어 4개 / local[*] = 모든 코어 / spark://host:7077 = 클러스터 "
                             "(기본: 환경 변수 SPARK_MASTER, 없으면 local[최대 4])")
    parser.add_argument("--driver-memory", default=os.getenv("SPARK_DRIVER_MEMORY", "auto"),
                        help="Spark 메모리 (auto = 코어 수와 컴퓨터 메모리에 맞춰 자동)")
    parser.add_argument("--defer-indexes", action="store_true", help="적재 전에 인덱스를 지우고 적재 후 다시 만들기")
    parser.add_argument("--partitions", type=int, default=None, help="병렬 처리·적재 단위 수 (기본: 사용하는 코어 수)")
    parser.add_argument("--metrics-out", default=None)
    # benchmark.py가 pandas 파이프라인과 같은 옵션을 넘겨도 무시되도록
    parser.add_argument("--prune-columns", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--load-method", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    try:
        result = run(args.year, args.months, args.mode, args.limit, args.master,
                     args.driver_memory, args.partitions, args.defer_indexes)
    except DataNotPublished as e:
        log.warning(f"건너뜀: {e}")
        raise SystemExit(0)

    if args.metrics_out:
        with open(args.metrics_out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
