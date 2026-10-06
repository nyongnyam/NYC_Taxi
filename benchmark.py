"""
benchmark.py — 병목 해결 기법을 하나씩 켜면서 시간·메모리가 몇 % 줄었는지 측정한다.

    python benchmark.py                      # 30만 행으로 0~4단계 비교 (몇 분)
    python benchmark.py --limit 1000000      # 행 수 늘리기
    python benchmark.py --full               # 한 달 전체 (기존 방식 단계는 10분 이상 걸림)
    python benchmark.py --steps 2,3,4        # 일부 단계만
    python benchmark.py --repeat 3           # 단계마다 3번 돌려 중앙값 사용

단계마다 pipeline.py를 별도 프로세스로 실행해 메모리 측정이 서로 섞이지 않게 한다.
결과는 화면에 표로 출력되고 data/benchmarks/ 에 .md / .csv 로 저장된다.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "data", "benchmarks")

# (단계 번호, 이름, 이 단계에서 새로 적용한 기법, pipeline.py 옵션)
STEPS = [
    (0, "기존 코드 (기준)", "-", ["--no-cache", "--load-method", "multi"]),
    (1, "+ 다운로드 캐싱", "이미 받은 parquet 재사용", ["--load-method", "multi"]),
    (2, "+ 컬럼 프루닝", "19개 중 필요한 8개 컬럼만 읽기", ["--prune-columns", "--load-method", "multi"]),
    (3, "+ COPY 적재", "다중 행 INSERT → PostgreSQL COPY", ["--prune-columns", "--load-method", "copy", "--copy-chunk-rows", "0"]),
    (4, "+ 청크 COPY", "COPY를 20만 행씩 나눠 전송", ["--prune-columns", "--load-method", "copy"]),
]


def run_step(opts: list[str], common: list[str], timeout: int) -> dict:
    fd, metrics_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    cmd = [sys.executable, os.path.join(BASE_DIR, "pipeline.py"), *common, *opts, "--metrics-out", metrics_path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout).strip().splitlines()[-3:]
            reason = "메모리 부족으로 강제 종료(OOM)" if proc.returncode in (-9, 137) else " / ".join(tail)
            return {"error": reason}
        with open(metrics_path, encoding="utf-8") as f:
            content = f.read()
        if not content:
            return {"error": "결과 없음 (데이터 미공개 월일 수 있음)"}
        return json.loads(content)
    except subprocess.TimeoutExpired:
        return {"error": f"{timeout}초 안에 끝나지 않아 중단"}
    finally:
        os.remove(metrics_path)


def median_result(results: list[dict]) -> dict:
    ok = [r for r in results if "error" not in r]
    if not ok:
        return results[0]
    ok.sort(key=lambda r: r["total_seconds"])
    return ok[len(ok) // 2]


def pct_change(new: float, old: float) -> str:
    if not old:
        return "-"
    change = (new - old) / old * 100
    if abs(change) < 0.5:
        return "변화 없음"
    return f"{-change:.1f}% 감소" if change < 0 else f"{change:.1f}% 증가"


def main():
    parser = argparse.ArgumentParser(description="병목 해결 전·후 비교 벤치마크")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1, choices=range(1, 13))
    parser.add_argument("--limit", type=int, default=300_000, help="처리할 행 수 (기본 30만)")
    parser.add_argument("--full", action="store_true", help="한 달 전체 데이터로 측정")
    parser.add_argument("--steps", default="0,1,2,3,4", help="실행할 단계 번호 (쉼표로 구분)")
    parser.add_argument("--repeat", type=int, default=1, help="단계마다 반복 횟수 (중앙값 사용)")
    parser.add_argument("--timeout", type=int, default=1800, help="단계별 최대 실행 시간(초)")
    args = parser.parse_args()

    wanted = {int(s) for s in args.steps.split(",")}
    steps = [s for s in STEPS if s[0] in wanted]
    common = ["--year", str(args.year), "--month", str(args.month)]
    if not args.full:
        common += ["--limit", str(args.limit)]
    scope = "한 달 전체" if args.full else f"앞에서 {args.limit:,}행"

    # 캐시를 쓰는 단계 전에 파일을 미리 받아 둔다 (측정에 포함하지 않음)
    if any(s[0] >= 1 for s in steps):
        sys.path.insert(0, BASE_DIR)
        from pipeline import download
        download(args.year, args.month, use_cache=True)

    print(f"\n벤치마크: {args.year}-{args.month:02d}, {scope}, 단계 {[s[0] for s in steps]}, 반복 {args.repeat}회\n")

    rows = []
    for num, name, technique, opts in steps:
        print(f"[{num}] {name} 실행 중...", flush=True)
        results = [run_step(opts, common, args.timeout) for _ in range(args.repeat)]
        r = median_result(results)
        if "error" in r:
            print(f"    → 실패: {r['error']}")
        else:
            st = r["stages"]
            print(f"    → {r['total_seconds']:.1f}초, 최대 {r['peak_mb']:,}MB "
                  f"(extract {st['extract']['seconds']:.1f}s / transform {st['transform']['seconds']:.1f}s"
                  f" / load {st['load']['seconds']:.1f}s)")
        rows.append({"num": num, "name": name, "technique": technique, "result": r})

    ok_rows = [row for row in rows if "error" not in row["result"]]
    if not ok_rows:
        print("\n성공한 단계가 없습니다.")
        return
    base = ok_rows[0]["result"]

    # ── 요약 표 ──
    lines = [
        f"# 벤치마크 결과 — {args.year}-{args.month:02d}, {scope}",
        "",
        f"측정 시각: {datetime.now():%Y-%m-%d %H:%M}, 기준: [{ok_rows[0]['num']}] {ok_rows[0]['name']}, 반복 {args.repeat}회(중앙값)",
        "",
        "| 단계 | 새로 적용한 기법 | 총 시간 | 기준 대비 | 직전 단계 대비 | 최대 메모리 | 기준 대비 | 가장 오래 걸린 구간 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    csv_rows = []
    prev = None
    for row in rows:
        r = row["result"]
        if "error" in r:
            lines.append(f"| [{row['num']}] {row['name']} | {row['technique']} | 실패 | {r['error']} | | | | |")
            csv_rows.append({"step": row["num"], "name": row["name"], "error": r["error"]})
            continue
        st = r["stages"]
        slowest = max(st, key=lambda k: st[k]["seconds"])
        share = st[slowest]["seconds"] / r["total_seconds"] if r["total_seconds"] else 0
        lines.append(
            f"| [{row['num']}] {row['name']} | {row['technique']} | {r['total_seconds']:.1f}초 "
            f"| {pct_change(r['total_seconds'], base['total_seconds']) if r is not base else '기준'} "
            f"| {pct_change(r['total_seconds'], prev['total_seconds']) if prev else '-'} "
            f"| {r['peak_mb']:,}MB "
            f"| {pct_change(r['peak_mb'], base['peak_mb']) if r is not base else '기준'} "
            f"| {slowest} ({share:.0%}) |"
        )
        csv_rows.append({
            "step": row["num"], "name": row["name"], "technique": row["technique"],
            "total_seconds": r["total_seconds"], "peak_mb": r["peak_mb"],
            "extract_s": st["extract"]["seconds"], "transform_s": st["transform"]["seconds"],
            "load_s": st["load"]["seconds"], "rows_loaded": r["rows_loaded"],
        })
        prev = r

    # ── 구간별 상세 ──
    lines += ["", "## 구간별 시간 (초)", "", "| 단계 | extract | transform | load | 적재 행 수 |", "|---|---|---|---|---|"]
    for row in ok_rows:
        st = row["result"]["stages"]
        lines.append(f"| [{row['num']}] {row['name']} | {st['extract']['seconds']:.1f} | "
                     f"{st['transform']['seconds']:.1f} | {st['load']['seconds']:.1f} | {row['result']['rows_loaded']:,} |")

    report = "\n".join(lines)
    print("\n" + report + "\n")

    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    md_path = os.path.join(OUT_DIR, f"bench_{stamp}.md")
    csv_path = os.path.join(OUT_DIR, f"bench_{stamp}.csv")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        fields = sorted({k for r in csv_rows for k in r}, key=lambda k: list(csv_rows[0]).index(k) if k in csv_rows[0] else 99)
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(csv_rows)
    print(f"저장: {md_path}\n      {csv_path}")


if __name__ == "__main__":
    main()
