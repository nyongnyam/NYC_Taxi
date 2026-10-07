"""
tune.py — 이 컴퓨터에서 가장 빠른 설정을 실제로 돌려 보며 찾는다.

    python tune.py                         # 1월 전체로 pandas 병렬 COPY 연결 수 + Spark 코어 수를 탐색
    python tune.py --months 1-3            # 더 큰 데이터로 탐색 (Spark가 유리해지는 구간)
    python tune.py --skip-spark            # pandas 쪽만
    python tune.py --workers 4,8,16 --cores 8,16,28   # 탐색할 값 직접 지정

    # Docker
    docker compose run --rm --entrypoint python bench tune.py

하는 일:
    1. 이 컴퓨터(컨테이너)의 CPU 코어 수, 메모리, PostgreSQL 설정을 확인한다.
    2. pandas 경로: Arrow CSV + 인덱스 나중에 생성을 켠 채로 DB 연결 수(COPY_WORKERS)를 바꿔 가며 잰다.
    3. Spark 경로: 동시에 쓰는 코어 수(SPARK_LOCAL_CORES)를 바꿔 가며 잰다.
    4. 가장 빠른 값을 .env에 넣을 형태로 출력하고 data/benchmarks/ 에 저장한다.
"""

import argparse
import os
import sys
from datetime import datetime

import psutil
import psycopg2

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
import config  # noqa: E402
from benchmark import OUT_DIR, run_step  # noqa: E402
from pipeline import download, parse_months  # noqa: E402

PG_KEYS = ["shared_buffers", "max_wal_size", "synchronous_commit", "wal_level",
           "maintenance_work_mem", "max_connections"]


def memory_gb() -> float:
    total = psutil.virtual_memory().total
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw != "max":
            total = min(total, int(raw))
    except OSError:
        pass
    return total / 1024**3


def pg_settings() -> dict:
    with psycopg2.connect(**config.psycopg2_kwargs()) as conn, conn.cursor() as cur:
        out = {}
        for k in PG_KEYS:
            cur.execute(f"SHOW {k}")
            out[k] = cur.fetchone()[0]
        return out


def candidates(text: str | None, defaults: list[int], cap: int) -> list[int]:
    values = [int(x) for x in text.split(",")] if text else defaults
    values = sorted({min(v, cap) for v in values if v >= 1})
    return values


def measure(script: str, opts: list[str], common: list[str], env: dict, repeat: int, timeout: int) -> dict:
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        results = [run_step(script, opts, common, timeout) for _ in range(repeat)]
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    ok = sorted((r for r in results if "error" not in r), key=lambda r: r["total_seconds"])
    return ok[len(ok) // 2] if ok else results[0]


def pick_best(rows: list[tuple[int, dict]]) -> tuple[int, dict] | None:
    """가장 빠른 값. 5% 이내로 비슷하면 자원을 덜 쓰는(작은) 값을 고른다."""
    ok = [(v, r) for v, r in rows if "error" not in r]
    if not ok:
        return None
    fastest = min(r["total_seconds"] for _, r in ok)
    close = [(v, r) for v, r in ok if r["total_seconds"] <= fastest * 1.05]
    return min(close, key=lambda x: x[0])


def table(title: str, label: str, rows: list[tuple[int, dict]], best) -> list[str]:
    lines = [f"## {title}", "", f"| {label} | 총 시간 | 적재 구간 | 최대 메모리 | 비고 |", "|---|---|---|---|---|"]
    base = next((r for _, r in rows if "error" not in r), None)
    for v, r in rows:
        if "error" in r:
            lines.append(f"| {v} | 실패 | | | {r['error'][:80]} |")
            continue
        st = r["stages"]
        load_s = st.get("load", st.get("process", {})).get("seconds", 0)
        speed = f"{base['total_seconds'] / r['total_seconds']:.2f}배" if base else ""
        mark = " ✅ 추천" if best and v == best[0] else ""
        lines.append(f"| {v} | {r['total_seconds']:.1f}초 | {load_s:.1f}초 | {r['peak_mb']:,}MB | "
                     f"첫 값 대비 {speed}{mark} |")
    return lines + [""]


def main():
    parser = argparse.ArgumentParser(description="이 컴퓨터에 맞는 최적 설정 탐색")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--months", default="1", help="탐색에 쓸 월 (예: 1, 1-3)")
    parser.add_argument("--limit", type=int, default=None, help="각 달에서 앞의 N행만 (기본: 전체)")
    parser.add_argument("--workers", default=None, help="pandas 병렬 COPY 연결 수 후보 (기본: 1,2,4,8,16)")
    parser.add_argument("--cores", default=None, help="Spark 코어 수 후보 (기본: 2,4,8,16,전체)")
    parser.add_argument("--skip-pandas", action="store_true")
    parser.add_argument("--skip-spark", action="store_true")
    parser.add_argument("--repeat", type=int, default=1, help="값마다 반복 횟수 (중앙값 사용)")
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()

    cpus = os.cpu_count() or 1
    mem = memory_gb()
    pg = pg_settings()
    months = parse_months(args.months)
    common = ["--year", str(args.year), "--months", ",".join(map(str, months))]
    if args.limit:
        common += ["--limit", str(args.limit)]
    for m in months:
        download(args.year, m, use_cache=True)

    bulk = pg["wal_level"] == "minimal" and pg["synchronous_commit"] == "off"
    print(f"\n환경: CPU {cpus}코어, 메모리 {mem:.1f}GB, PostgreSQL 대량 적재 설정 {'켜짐' if bulk else '꺼짐'}")
    print(f"데이터: {args.year}년 {months}월" + (f", 각 달 {args.limit:,}행" if args.limit else ", 전체 행") + "\n")

    report = [
        f"# 튜닝 결과 — {args.year}년 " + (f"{months[0]}월" if len(months) == 1 else f"{months[0]}~{months[-1]}월"),
        "",
        f"측정 시각: {datetime.now():%Y-%m-%d %H:%M} · CPU {cpus}코어 · 메모리 {mem:.1f}GB",
        "",
        "PostgreSQL: " + ", ".join(f"`{k}={v}`" for k, v in pg.items()),
        "",
    ]
    recommend = {}

    if not args.skip_pandas:
        pandas_rows = []
        for w in candidates(args.workers, [1, 2, 4, 8, 16], cpus):
            print(f"[pandas] DB 연결 {w}개 ...", flush=True)
            r = measure("pipeline.py",
                        ["--prune-columns", "--load-method", "copy", "--csv-engine", "arrow",
                         "--copy-workers", str(w), "--defer-indexes"],
                        common, {}, args.repeat, args.timeout)
            print("    → " + (f"{r['total_seconds']:.1f}초, {r['peak_mb']:,}MB" if "error" not in r else r["error"]))
            pandas_rows.append((w, r))
        best = pick_best(pandas_rows)
        report += table("pandas: 병렬 COPY 연결 수 (Arrow CSV + 인덱스 나중에 생성)", "연결 수", pandas_rows, best)
        if best:
            recommend.update({"CSV_ENGINE": "arrow", "COPY_WORKERS": str(best[0])})
            recommend["_pandas_seconds"] = best[1]["total_seconds"]

    if not args.skip_spark:
        spark_rows = []
        for c in candidates(args.cores, [2, 4, 8, 16, cpus], cpus):
            print(f"[Spark] 코어 {c}개 ...", flush=True)
            r = measure("spark_pipeline.py", ["--mode", "raw", "--defer-indexes"], common,
                        {"SPARK_MASTER": f"local[{c}]"}, args.repeat, args.timeout)
            print("    → " + (f"{r['total_seconds']:.1f}초, {r['peak_mb']:,}MB" if "error" not in r else r["error"]))
            spark_rows.append((c, r))
        best = pick_best(spark_rows)
        report += table("Spark: 동시에 쓰는 코어 수 (raw 모드, 인덱스 나중에 생성)", "코어 수", spark_rows, best)
        if best:
            recommend["SPARK_LOCAL_CORES"] = str(best[0])
            recommend["_spark_seconds"] = best[1]["total_seconds"]

    # ── 결론 ──
    report += ["## 추천 설정", ""]
    if "_pandas_seconds" in recommend and "_spark_seconds" in recommend:
        winner = "pandas" if recommend["_pandas_seconds"] <= recommend["_spark_seconds"] else "Spark"
        report.append(f"이 데이터 크기에서는 **{winner}** 쪽이 더 빠르다 "
                      f"(pandas {recommend['_pandas_seconds']:.1f}초 / Spark {recommend['_spark_seconds']:.1f}초). "
                      "데이터가 커질수록 Spark가 유리해지므로 `--months`를 늘려 다시 재 볼 것.")
        report.append("")
    if not bulk:
        report += ["PostgreSQL 대량 적재 설정이 꺼져 있다. 병렬 COPY에서는 이 설정의 효과가 커지므로 켜고 다시 재 보자:",
                   "", "```", "docker compose exec -T db psql -U postgres -d nyctaxi < sql/pg_bulk_load.sql",
                   "docker compose restart db", "```", ""]
    env_lines = [f"{k}={v}" for k, v in recommend.items() if not k.startswith("_")]
    if env_lines:
        report += ["`.env`에 추가할 값:", "", "```", *env_lines, "```"]

    text = "\n".join(report)
    print("\n" + text + "\n")
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"tuning_{datetime.now():%Y%m%d_%H%M%S}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print(f"저장: {path}")


if __name__ == "__main__":
    main()
