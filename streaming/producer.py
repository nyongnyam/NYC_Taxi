"""
streaming/producer.py — 월별 parquet을 '실시간 운행 이벤트'처럼 Kafka 토픽으로 재생한다.

실행 예:
    python streaming/producer.py --year 2024 --month 1                 # 최대 속도로 전송
    python streaming/producer.py --limit 100000 --rate 2000            # 초당 2,000건으로 천천히
    python streaming/producer.py --linger-ms 0 --compression none      # 배칭·압축 끄고 비교

이벤트 1건 = 운행 1건(JSON). 키는 승차 지역(PULocationID)이라
같은 지역의 이벤트는 항상 같은 파티션으로 들어가 순서가 보장된다.
"""

import argparse
import json
import logging
import os
import sys
import time

import pyarrow.parquet as pq
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from pipeline import RAW_COLUMNS, download  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer")

KEY_COLUMN = "pulocationid"


def ensure_topic(bootstrap: str, topic: str, partitions: int):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    try:
        futures[topic].result()
        log.info(f"토픽 생성: {topic} (파티션 {partitions}개)")
    except KafkaException as e:
        if "TOPIC_ALREADY_EXISTS" not in str(e):
            raise


def encode_row(row: dict) -> tuple[bytes, bytes]:
    """parquet 한 행 → (key, value) 바이트. datetime은 문자열로 직렬화한다."""
    key = str(row.get(KEY_COLUMN, "")).encode()
    value = json.dumps(row, default=str).encode()
    return key, value


def iter_rows(path: str, batch_rows: int, limit: int | None):
    """parquet을 batch_rows 단위로 나눠 읽는다 (파일 전체를 메모리에 올리지 않음)."""
    pf = pq.ParquetFile(path)
    wanted = set(RAW_COLUMNS) | {KEY_COLUMN}
    columns = [name for name in pf.schema_arrow.names if name.lower() in wanted]
    sent = 0
    for batch in pf.iter_batches(batch_size=batch_rows, columns=columns):
        for row in batch.to_pylist():
            yield {k.lower(): v for k, v in row.items()}
            sent += 1
            if limit and sent >= limit:
                return


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi parquet → Kafka 재생기")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1, choices=range(1, 13))
    parser.add_argument("--limit", type=int, default=None, help="앞에서부터 N건만 전송")
    parser.add_argument("--rate", type=float, default=0, help="초당 전송 건수 (0 = 제한 없음)")
    parser.add_argument("--partitions", type=int, default=int(os.getenv("KAFKA_PARTITIONS", "3")))
    parser.add_argument("--batch-rows", type=int, default=50_000, help="parquet을 한 번에 읽을 행 수")
    # ── 처리량에 큰 영향을 주는 Producer 설정 ──
    parser.add_argument("--linger-ms", type=int, default=20, help="배치를 모으기 위해 기다리는 시간")
    parser.add_argument("--batch-size", type=int, default=256 * 1024, help="파티션당 배치 최대 바이트")
    parser.add_argument("--compression", default="lz4", choices=["none", "gzip", "snappy", "lz4", "zstd"])
    parser.add_argument("--acks", default="all", choices=["0", "1", "all"])
    args = parser.parse_args()

    ensure_topic(config.KAFKA_BOOTSTRAP, config.KAFKA_TOPIC, args.partitions)

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "linger.ms": args.linger_ms,
        "batch.size": args.batch_size,
        "compression.type": args.compression,
        "acks": args.acks,
        "enable.idempotence": args.acks == "all",  # 재전송 시 중복 방지
    })

    failed = 0

    def on_delivery(err, _msg):
        nonlocal failed
        if err is not None:
            failed += 1
            if failed <= 5:
                log.error(f"전송 실패: {err}")

    path = download(args.year, args.month)
    log.info(f"전송 시작 → {config.KAFKA_TOPIC} @ {config.KAFKA_BOOTSTRAP}")

    start = last_report = time.perf_counter()
    sent = backpressure = 0
    for row in iter_rows(path, args.batch_rows, args.limit):
        key, value = encode_row(row)
        while True:
            try:
                producer.produce(config.KAFKA_TOPIC, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                # 로컬 전송 큐가 가득 참 = 브로커가 받는 속도보다 빨리 만들고 있음 (backpressure)
                backpressure += 1
                producer.poll(0.1)
        sent += 1
        producer.poll(0)  # 전송 완료 콜백 처리

        if args.rate:
            # 목표 속도보다 앞서 있으면 잠깐 쉰다
            ahead = sent / args.rate - (time.perf_counter() - start)
            if ahead > 0:
                time.sleep(ahead)

        now = time.perf_counter()
        if now - last_report >= 5:
            log.info(f"  {sent:,}건 전송 ({sent / (now - start):,.0f}건/초, 대기 큐 {len(producer):,})")
            last_report = now

    log.info("남은 메시지 flush 중...")
    producer.flush()
    elapsed = time.perf_counter() - start
    log.info(
        f"전송 완료: {sent:,}건 / {elapsed:.1f}초 = {sent / elapsed:,.0f}건/초 "
        f"(실패 {failed:,}건, 큐 가득 참 {backpressure:,}회)"
    )


if __name__ == "__main__":
    main()
