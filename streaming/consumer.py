"""
streaming/consumer.py — Kafka 토픽의 운행 이벤트를 묶음(micro-batch)으로 꺼내
배치 파이프라인과 같은 규칙으로 정제한 뒤 PostgreSQL에 COPY로 적재한다.

실행 예:
    python streaming/consumer.py                         # 계속 실행 (Ctrl+C로 종료)
    python streaming/consumer.py --batch-size 1000       # 묶음 크기를 줄여서 비교
    python streaming/consumer.py --exit-when-idle 15     # 15초 동안 새 메시지가 없으면 종료 (실험용)

전달 보장: at-least-once
    DB 커밋이 성공한 뒤에만 Kafka 오프셋을 커밋한다. 그 사이에 죽으면 재시작 후
    같은 묶음을 다시 읽으므로 데이터 유실은 없지만 중복이 생길 수 있다.
    (docs/BOTTLENECKS.md의 '전달 보장' 실험 참고)
"""

import argparse
import json
import logging
import os
import signal
import sys
import time

import pandas as pd
import psycopg2
from confluent_kafka import Consumer, KafkaError, TopicPartition

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from pipeline import clean, copy_dataframe  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")

running = True


def _stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def decode_batch(values: list[bytes]) -> pd.DataFrame:
    return pd.DataFrame.from_records(json.loads(v) for v in values)


def write_batch(conn, df: pd.DataFrame) -> int:
    """정제 후 COPY. 같은 트랜잭션 안에서 처리하고 커밋한다."""
    cleaned = clean(df)
    if not cleaned.empty:
        copy_dataframe(conn, cleaned)
    conn.commit()
    return len(cleaned)


def total_lag(consumer: Consumer) -> int | None:
    """할당된 파티션들의 (마지막 오프셋 - 현재 읽은 위치) 합계 = 아직 처리 못 한 메시지 수"""
    assigned = consumer.assignment()
    if not assigned:
        return None
    lag = 0
    for tp in consumer.position(assigned):
        _, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5)
        lag += max(high - tp.offset, 0) if tp.offset >= 0 else high
    return lag


def main():
    parser = argparse.ArgumentParser(description="Kafka → PostgreSQL 적재기")
    parser.add_argument("--batch-size", type=int, default=10_000, help="한 번에 DB에 쓰는 메시지 수")
    parser.add_argument("--poll-timeout", type=float, default=1.0, help="묶음을 채우기 위해 기다리는 최대 초")
    parser.add_argument("--exit-when-idle", type=float, default=0,
                        help="N초 동안 메시지가 없으면 종료 (0 = 계속 실행)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # 오프셋은 DB 커밋 후 직접 커밋
    })
    consumer.subscribe([config.KAFKA_TOPIC])
    conn = psycopg2.connect(**config.psycopg2_kwargs())

    log.info(f"구독 시작: {config.KAFKA_TOPIC} (group={config.KAFKA_GROUP_ID}, batch={args.batch_size:,})")
    start = last_report = last_message = time.perf_counter()
    consumed = loaded = 0
    db_seconds = 0.0

    try:
        while running:
            msgs = consumer.consume(num_messages=args.batch_size, timeout=args.poll_timeout)
            now = time.perf_counter()

            values = []
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.error(f"Kafka 오류: {m.error()}")
                    continue
                values.append(m.value())

            if values:
                t = time.perf_counter()
                loaded += write_batch(conn, decode_batch(values))
                db_seconds += time.perf_counter() - t
                consumer.commit(asynchronous=False)
                consumed += len(values)
                last_message = now
            elif args.exit_when_idle and now - last_message >= args.exit_when_idle:
                log.info(f"{args.exit_when_idle:.0f}초 동안 새 메시지 없음 → 종료")
                break

            if now - last_report >= 5 and consumed:
                elapsed = now - start
                log.info(
                    f"  소비 {consumed:,}건 ({consumed / elapsed:,.0f}건/초) | 적재 {loaded:,}행 | "
                    f"DB 시간 비중 {db_seconds / elapsed:.0%} | lag {total_lag(consumer)}"
                )
                last_report = now
    finally:
        consumer.close()
        conn.close()
        elapsed = time.perf_counter() - start
        if consumed:
            log.info(f"종료: 소비 {consumed:,}건, 적재 {loaded:,}행, {elapsed:.1f}초")


if __name__ == "__main__":
    main()
