"""
spark_backfill.py — 새 환경에서 과거 데이터를 Spark로 한 번에 채운다 (초기 적재).
매월 적재는 그대로 pipeline.py / Airflow DAG가 한다.
 
    python spark_backfill.py                          # 2024-01부터 지금까지 공개된 모든 달
    python spark_backfill.py --from 2011-01           # 시작 월 지정 (2011-01 이후 파일만 같은 형식)
    python spark_backfill.py --from 2024-01 --to 2024-12
    python spark_backfill.py --master "local[8]"      # 코어 8개 사용 (기본은 최대 SPARK_LOCAL_CORES=4개)
 
순서
    1. 다운로드  : 기간 안의 달을 스레드로 동시에 받는다. 아직 공개되지 않은 달은 건너뛴다 (pipeline.download 재사용)
    2. 읽기·정제 : Spark로 모든 파일을 한꺼번에 읽어 정제한다 (local-bottleneck-lab의 spark_pipeline.py와 같은 규칙)
    3. 삭제      : 기간 안의 기존 데이터를 지운다 (다시 실행해도 중복되지 않게)
    4. 적재      : 파티션마다 DB 연결을 열어 COPY를 병렬로 보낸다. CSV 변환은 mapInArrow + pyarrow
                   적재량이 테이블의 20% 이상이면 인덱스를 지웠다가 마지막에 한 번에 만든다
 
pandas(pipeline.py)는 여러 달을 한 번에 메모리에 올리지만, Spark는 파티션 단위로 흘려보내므로
기간이 길어져도 메모리 사용량이 크게 늘지 않는다 (실험: 6개월, 4GB 제한에서 pandas 실패 / Spark 106초).
"""
 
import argparse
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
 
from sqlalchemy import inspect, text
 
import config
from pipeline import RAW_COLUMNS, TABLE_COLUMNS, DataNotPublished, _should_defer_indexes, download, engine
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spark_backfill")
 
 
def month_list(start: str, end: str) -> list[tuple[int, int]]:
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out
 
 
# ── 1. 다운로드 ──────────────────────────────────────
def download_all(months: list[tuple[int, int]], workers: int) -> list[tuple[int, int, str]]:
    """공개된 달만 (연, 월, 경로)로 돌려준다. 다운로드는 네트워크 일이라 Spark가 아니라 스레드로 한다."""
    def fetch(ym):
        try:
            return (*ym, download(*ym))
        except DataNotPublished:
            return None
 
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        found = [r for r in pool.map(fetch, months) if r]
    log.info(f"다운로드: 공개된 {len(found)}/{len(months)}개월, {time.perf_counter() - start:.1f}초")
    return found
 
 
# ── Spark 세션 (spark_pipeline.py와 같은 설정) ─────────────
def default_master() -> str:
    """기본은 코어 중 최대 SPARK_LOCAL_CORES(기본 4)개만 쓴다.
    local[*]는 코어마다 파이썬 워커를 띄워서, 코어가 많은 PC에서는 메모리 한도를 넘길 수 있다 (실험에서 확인)."""
    env = os.getenv("SPARK_MASTER", "").strip()
    if env:
        return env
    return f"local[{min(os.cpu_count() or 1, int(os.getenv('SPARK_LOCAL_CORES', '4')))}]"
 
 
def parallelism_of(master: str) -> int:
    if master.startswith("local["):
        n = master[len("local["):-1]
        return (os.cpu_count() or 1) if n == "*" else int(n)
    return int(os.getenv("SPARK_LOCAL_CORES", "4"))
 
 
def auto_driver_memory(cores: int) -> str:
    """코어당 약 512MB, 1GB 이상·이 컴퓨터(컨테이너) 메모리의 40% 이하"""
    total_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024**3
    try:  # 컨테이너에 메모리 제한이 걸려 있으면 그 값을 쓴다
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw != "max":
            total_gb = min(total_gb, int(raw) / 1024**3)
    except OSError:
        pass
    return f"{max(1, min(int(cores * 0.5), int(total_gb * 0.4)))}g"
 
 
def build_session(master: str, driver_memory: str, partitions: int):
    import socket
 
    from pyspark.sql import SparkSession
 
    builder = SparkSession.builder.appName("nyc-taxi-backfill").master(master)
    if master.startswith("spark://"):  # 클러스터: 워커가 이 컨테이너(드라이버)로 접속할 수 있어야 한다
        builder = (builder
                   .config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
                   .config("spark.driver.bindAddress", "0.0.0.0")
                   .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g")))
    spark = (
        builder
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")          # 시간을 변환 없이 그대로 다룬다
        .config("spark.sql.shuffle.partitions", partitions)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", 50_000)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
 
 
# ── 2. 읽기·정제 ─────────────────────────────────────
def read_all(spark, files: list[tuple[int, int, str]]):
    """달마다 필요한 컬럼만 같은 타입으로 맞추고, 파일에 섞인 다른 달 기록은 뺀 뒤 합친다.
    (해마다 컬럼 타입이 조금씩 달라서 파일별로 맞춘다. 컬럼 이름이 다른 2009~2010년 파일은 건너뛴다)"""
    from pyspark.sql import functions as F
 
    frames = []
    for year, month, path in files:
        df = spark.read.parquet(path)
        cols = {c.lower(): c for c in df.columns}
        if not all(c in cols for c in RAW_COLUMNS):
            log.warning(f"  {year}-{month:02d}: 컬럼 형식이 달라 건너뜀")
            continue
        pickup = F.col(cols["tpep_pickup_datetime"]).cast("timestamp")
        frames.append(df.select(
            F.col(cols["vendorid"]).cast("int").alias("vendor_id"),
            pickup.alias("tpep_pickup_datetime"),
            F.col(cols["tpep_dropoff_datetime"]).cast("timestamp").alias("tpep_dropoff_datetime"),
            *[F.col(cols[c]).cast("double").alias(c) for c in RAW_COLUMNS[3:]],
        ).where((F.year(pickup) == year) & (F.month(pickup) == month)))
 
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other)
    return df
 
 
def transform(df):
    """pipeline.transform()과 같은 규칙을 Spark 함수로 옮긴 것 (spark_pipeline.transform과 같음)"""
    from pyspark.sql import functions as F
 
    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    df = (
        df.where((F.col("trip_distance") > 0) & (F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
        .where(F.col("passenger_count").between(1, 6))
        .withColumn("trip_duration_min", F.round((F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60, 2))
        .withColumn("pickup_hour", F.hour(pickup))
        .withColumn("pickup_weekday", F.date_format(pickup, "EEEE"))
        .withColumn("tip_rate", F.round(F.col("tip_amount") / F.col("fare_amount"), 4))
        .where(F.col("trip_duration_min").between(1, 180))
    )
    return df.select(*TABLE_COLUMNS)
 
 
# ── 3·4. 삭제·적재 ───────────────────────────────────
def load(df, start: str, end: str, partitions: int, estimated_rows: int) -> int:
    """기간 데이터를 지우고, 파티션마다 DB 연결을 열어 COPY를 병렬로 보낸다 (spark_pipeline.load_raw와 같은 방식)."""
    with engine.begin() as conn:
        if not inspect(conn).has_table(config.TABLE_NAME):
            raise RuntimeError(f"{config.TABLE_NAME} 테이블이 없습니다. sql/setup.sql을 먼저 실행하세요.")
        deleted = conn.execute(text(
            f'DELETE FROM "{config.TABLE_NAME}" WHERE tpep_pickup_datetime >= :s AND tpep_pickup_datetime < :e'),
            {"s": start, "e": end}).rowcount
        indexes = []
        if _should_defer_indexes(conn, estimated_rows):
            indexes = conn.execute(text(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = :t"), {"t": config.TABLE_NAME}).fetchall()
            for name, _ in indexes:
                conn.execute(text(f'DROP INDEX IF EXISTS "{name}"'))
    if deleted:
        log.info(f"  기존 데이터 {deleted:,}행 삭제")
 
    conn_kwargs = config.psycopg2_kwargs()
    copy_sql = f'COPY "{config.TABLE_NAME}" ({", ".join(TABLE_COLUMNS)}) FROM STDIN WITH (FORMAT csv)'
 
    def write_partition(batches):
        # Spark 워커 프로세스 안에서 파티션마다 한 번 실행된다
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
 
    log.info(f"PostgreSQL 적재 중... (Spark 파티션 {partitions}개가 병렬로 COPY, 인덱스 나중에 생성: {bool(indexes)})")
    try:
        result = df.repartition(partitions).mapInArrow(write_partition, schema="rows long")
        loaded = result.groupBy().sum("rows").collect()[0][0] or 0
    except Exception:
        # 파티션마다 따로 커밋하므로 일부만 들어갔을 수 있다 → 기간 데이터를 지워 깨끗한 상태로 되돌린다
        log.error("적재 실패: 일부만 들어간 데이터를 지웁니다")
        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{config.TABLE_NAME}" '
                              "WHERE tpep_pickup_datetime >= :s AND tpep_pickup_datetime < :e"),
                         {"s": start, "e": end})
        raise
    finally:
        if indexes:  # 성공하든 실패하든 인덱스는 반드시 되살린다
            t = time.perf_counter()
            with engine.begin() as conn:
                for _, ddl in indexes:
                    conn.execute(text(ddl))
            log.info(f"  인덱스 {len(indexes)}개 다시 생성: {time.perf_counter() - t:.1f}초")
    return loaded
 
 
def run(start: str, end: str, master: str | None = None, driver_memory: str = "auto",
        download_workers: int = 4) -> int:
    t0 = time.perf_counter()
    files = download_all(month_list(start, end), download_workers)
    if not files:
        log.warning("공개된 달이 없습니다")
        return 0
 
    import pyarrow.parquet as pq
    estimated = sum(pq.ParquetFile(p).metadata.num_rows for _, _, p in files)  # 인덱스 지연 여부 판단용
 
    master = master or default_master()
    partitions = parallelism_of(master)
    if driver_memory == "auto":
        driver_memory = auto_driver_memory(partitions)
    log.info(f"Spark 시작: master={master}, 병렬 {partitions}개, 메모리 {driver_memory}, "
             f"파일 {len(files)}개 (원본 약 {estimated:,}행)")
    spark = build_session(master, driver_memory, partitions)
    try:
        first, last = files[0], files[-1]
        range_start = f"{first[0]}-{first[1]:02d}-01"
        range_end = f"{last[0] + (last[1] == 12)}-{last[1] % 12 + 1:02d}-01"
        loaded = load(transform(read_all(spark, files)), range_start, range_end, partitions, estimated)
    finally:
        spark.stop()
    log.info(f"초기 적재 완료: {first[0]}-{first[1]:02d} ~ {last[0]}-{last[1]:02d}, "
             f"{loaded:,}행, 총 {time.perf_counter() - t0:.1f}초")
    return loaded
 
 
if __name__ == "__main__":
    today = date.today()
    parser = argparse.ArgumentParser(description="과거 데이터 초기 적재 (Spark)")
    parser.add_argument("--from", dest="start", default="2024-01", help="시작 월 YYYY-MM (기본 2024-01)")
    parser.add_argument("--to", dest="end", default=f"{today.year}-{today.month:02d}", help="끝 월 YYYY-MM (기본: 이번 달)")
    parser.add_argument("--master", default=None, help="기본: local[최대 SPARK_LOCAL_CORES]. 클러스터: spark://spark-master:7077")
    parser.add_argument("--driver-memory", default=os.getenv("SPARK_DRIVER_MEMORY", "auto"))
    parser.add_argument("--download-workers", type=int, default=4, help="동시에 받을 파일 수")
    args = parser.parse_args()
    run(args.start, args.end, args.master, args.driver_memory, args.download_workers)