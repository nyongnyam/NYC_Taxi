"""
streaming/partition_bench.py — Kafka 파티션 수와 consumer 수를 바꿔 가며 처리량을 비교한다.

    python streaming/partition_bench.py                              # 기본 조합 1x1, 3x1, 3x3, 3x4, 6x6
    python streaming/partition_bench.py --combos 1x1,2x2,4x4,8x8     # 원하는 조합 (파티션x컨슈머)
    python streaming/partition_bench.py --limit 1000000              # 메시지 수 늘리기

    # Docker
    docker compose run --rm kafka-bench
    docker compose run --rm kafka-bench --combos 1x1,3x3

진행 순서 (파티션 수마다):
    1. 그 파티션 수로 실험용 토픽을 새로 만든다 (bench-p3-날짜 …)
    2. producer로 메시지를 전부 넣는다 → producer 처리량, 파티션별 메시지 분포 측정
    3. consumer N개를 동시에 띄워 토픽을 비울 때까지 처리한다 → consumer 쪽 처리량 측정
       (같은 토픽을 consumer 수만 바꿔 다시 읽을 때는 group id를 바꿔 처음부터 읽는다)
    4. 실험용 토픽 삭제

적재는 실제 테이블이 아니라 실험용 테이블(bench_stream_trips)에 하므로 기존 데이터는 건드리지 않는다.
결과는 화면에 표로 출력되고 data/benchmarks/kafka_partitions_*.md 로 저장된다.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
import config  # noqa: E402

BENCH_TABLE = "bench_stream_trips"
OUT_DIR = os.path.join(BASE_DIR, "data", "benchmarks")
PRODUCER = os.path.join(BASE_DIR, "streaming", "producer.py")
CONSUMER = os.path.join(BASE_DIR, "streaming", "consumer.py")


def parse_combos(text: str) -> list[tuple[int, int]]:
    combos = []
    for item in text.split(","):
        p, c = item.lower().split("x")
        combos.append((int(p), int(c)))
    return combos


def prepare_table():
    with psycopg2.connect(**config.psycopg2_kwargs()) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {BENCH_TABLE} (LIKE clean_taxi_trips INCLUDING DEFAULTS)")


def truncate_table():
    with psycopg2.connect(**config.psycopg2_kwargs()) as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {BENCH_TABLE}")


def count_table() -> int:
    with psycopg2.connect(**config.psycopg2_kwargs()) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {BENCH_TABLE}")
        return cur.fetchone()[0]


def create_topic(admin: AdminClient, topic: str, partitions: int):
    admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic].result()
    # 메타데이터에 반영될 때까지 잠깐 대기
    for _ in range(50):
        md = admin.list_topics(topic=topic, timeout=5).topics.get(topic)
        if md and len(md.partitions) == partitions and md.error is None:
            return
        time.sleep(0.2)


def partition_counts(topic: str, partitions: int) -> list[int]:
    c = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": "partition-bench-inspector"})
    counts = []
    for p in range(partitions):
        low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        counts.append(high - low)
    c.close()
    return counts


def child_env(**extra) -> dict:
    env = os.environ.copy()
    env.update({k: str(v) for k, v in extra.items()})
    return env


def run_producer(topic: str, partitions: int, args) -> dict:
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    cmd = [sys.executable, PRODUCER, "--year", str(args.year), "--month", str(args.month),
           "--limit", str(args.limit), "--partitions", str(partitions), "--metrics-out", path]
    proc = subprocess.run(cmd, env=child_env(KAFKA_TOPIC=topic), capture_output=True, text=True)
    try:
        if proc.returncode != 0:
            raise RuntimeError("producer 실패:\n" + "\n".join(proc.stderr.strip().splitlines()[-5:]))
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    finally:
        os.remove(path)


def run_consumers(topic: str, n: int, args) -> dict:
    group = f"{topic}-c{n}"
    procs, paths = [], []
    for i in range(n):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        paths.append(path)
        cmd = [sys.executable, CONSUMER, "--batch-size", str(args.batch_size),
               "--exit-when-idle", str(args.idle), "--metrics-out", path]
        log = open(os.path.join(tempfile.gettempdir(), f"{group}-{i}.log"), "w", encoding="utf-8")
        procs.append((subprocess.Popen(
            cmd, stdout=log, stderr=subprocess.STDOUT,
            env=child_env(KAFKA_TOPIC=topic, KAFKA_GROUP_ID=group, TABLE_NAME=BENCH_TABLE),
        ), log))

    for p, log in procs:
        p.wait()
        log.close()

    results = []
    for path in paths:
        try:
            with open(path, encoding="utf-8") as f:
                content = f.read()
            results.append(json.loads(content) if content else {"consumed": 0})
        finally:
            os.remove(path)

    active = [r for r in results if r.get("consumed")]
    total = sum(r["consumed"] for r in active)
    if active:
        span = max(r["last_ts"] for r in active) - min(r["first_ts"] for r in active)
    else:
        span = 0
    return {
        "consumed": total,
        "loaded": sum(r.get("loaded", 0) for r in active),
        "seconds": round(span, 2),
        "rate": total / span if span else 0,
        "active": len(active),
        "per_consumer": [(r.get("partitions", []), r.get("consumed", 0)) for r in results],
        "db_share": (sum(r.get("db_seconds", 0) for r in active) / sum(r.get("busy_seconds", 0) for r in active))
                    if active and sum(r.get("busy_seconds", 0) for r in active) else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Kafka 파티션 수 × consumer 수 처리량 실험")
    parser.add_argument("--combos", default="1x1,3x1,3x3,3x4,6x6",
                        help="파티션x컨슈머 조합 (쉼표로 구분). 예: 1x1,3x3,6x6")
    parser.add_argument("--limit", type=int, default=300_000, help="토픽에 넣을 메시지 수")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1, choices=range(1, 13))
    parser.add_argument("--batch-size", type=int, default=10_000, help="consumer 묶음 크기")
    parser.add_argument("--idle", type=float, default=15, help="consumer가 이 초 동안 메시지가 없으면 종료")
    parser.add_argument("--keep-topics", action="store_true", help="실험용 토픽을 지우지 않고 남김")
    args = parser.parse_args()

    combos = parse_combos(args.combos)
    by_partition: dict[int, list[int]] = defaultdict(list)
    for p, c in combos:
        by_partition[p].append(c)

    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    prepare_table()
    stamp = datetime.now().strftime("%m%d%H%M%S")
    cpus = os.cpu_count()

    print(f"\nKafka 파티션 실험: 메시지 {args.limit:,}건, 조합 {args.combos}, 사용 가능한 CPU {cpus}개\n")

    producer_rows, consumer_rows = [], []
    for partitions, consumer_counts in by_partition.items():
        topic = f"bench-p{partitions}-{stamp}"
        create_topic(admin, topic, partitions)

        print(f"[파티션 {partitions}개] producer로 {args.limit:,}건 전송 중...", flush=True)
        prod = run_producer(topic, partitions, args)
        counts = partition_counts(topic, partitions)
        avg = sum(counts) / len(counts)
        skew = max(counts) / avg if avg else 0
        producer_rows.append((partitions, prod, counts, skew))
        print(f"    → {prod['sent'] / prod['seconds']:,.0f}건/초, 파티션별 메시지 {counts} (최대/평균 {skew:.2f})")

        for n in consumer_counts:
            truncate_table()
            print(f"[파티션 {partitions}개 × consumer {n}개] 처리 중...", flush=True)
            res = run_consumers(topic, n, args)
            res["db_rows"] = count_table()
            consumer_rows.append((partitions, n, res))
            idle = n - res["active"]
            print(f"    → {res['rate']:,.0f}건/초 ({res['seconds']:.1f}초), 일한 consumer {res['active']}/{n}"
                  + (f", 논 consumer {idle}개" if idle else ""))
            for parts, consumed in res["per_consumer"]:
                print(f"       파티션 {parts if parts else '없음'}: {consumed:,}건")

        if not args.keep_topics:
            admin.delete_topics([topic])

    # ── 결과 표 ──
    base = next((r for p, n, r in consumer_rows if n == 1), consumer_rows[0][2])
    lines = [
        f"# Kafka 파티션 실험 — 메시지 {args.limit:,}건",
        "",
        f"측정 시각: {datetime.now():%Y-%m-%d %H:%M}, CPU {cpus}개, consumer 묶음 {args.batch_size:,}건",
        "",
        "## Consumer 처리량",
        "",
        "| 파티션 | consumer | 일한 consumer | 처리 시간 | 처리량 | consumer 1개 대비 | DB 시간 비중 | 적재 행 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for p, n, r in consumer_rows:
        ratio = r["rate"] / base["rate"] if base["rate"] else 0
        lines.append(
            f"| {p} | {n} | {r['active']} | {r['seconds']:.1f}초 | {r['rate']:,.0f}건/초 "
            f"| {ratio:.2f}배 | {r['db_share']:.0%} | {r['db_rows']:,} |"
        )
    lines += [
        "",
        "## Producer 처리량과 파티션 분포",
        "",
        "| 파티션 | 전송 시간 | 처리량 | 파티션별 메시지 수 | 최대/평균 (1.00 = 완전 균등) |",
        "|---|---|---|---|---|",
    ]
    for p, prod, counts, skew in producer_rows:
        lines.append(f"| {p} | {prod['seconds']:.1f}초 | {prod['sent'] / prod['seconds']:,.0f}건/초 "
                     f"| {', '.join(f'{c:,}' for c in counts)} | {skew:.2f} |")

    report = "\n".join(lines)
    print("\n" + report + "\n")
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"kafka_partitions_{datetime.now():%Y%m%d_%H%M%S}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"저장: {path}")


if __name__ == "__main__":
    main()
