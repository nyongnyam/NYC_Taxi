"""
benchmark.py — 병목 해결 기법을 하나씩 켜면서 시간·메모리가 몇 % 줄었는지 측정한다.

하나씩 추가하며 보기 (결과가 data/benchmarks/history.json 에 쌓인다):
    python benchmark.py --step 0             # 기준: 기존 코드
    python benchmark.py --step 1             # + 다운로드 캐싱 → 0단계 대비 감소율 표시
    python benchmark.py --step 2             # + 컬럼 프루닝  → 0·1단계와 함께 표시
    ...
    python benchmark.py --show               # 실행 없이 지금까지의 표만 보기
    python benchmark.py --reset --step 0     # 처음부터 다시

한 번에 전부:
    python benchmark.py                      # 0~4단계 (30만 행, 몇 분)
    python benchmark.py --full --step 3      # 한 달 전체 (행 수가 다르면 결과도 따로 쌓임)
    python benchmark.py --months 1-6 --full --steps 4,5,6   # 여러 달 규모 실험 (pandas vs Spark)
    python benchmark.py --repeat 3           # 단계마다 3번 돌려 중앙값 사용

단계마다 pipeline.py를 별도 프로세스로 실행해 메모리 측정이 서로 섞이지 않게 한다.
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

# (단계 번호, 이름, 이 단계에서 새로 적용한 기법, 실행 옵션, 실행할 스크립트)
STEPS = [
    (0, "기존 코드 (기준)", "-", ["--no-cache", "--load-method", "multi"], "pipeline.py"),
    (1, "+ 다운로드 캐싱", "이미 받은 parquet 재사용", ["--load-method", "multi"], "pipeline.py"),
    (2, "+ 컬럼 프루닝", "19개 중 필요한 8개 컬럼만 읽기", ["--prune-columns", "--load-method", "multi"], "pipeline.py"),
    (3, "+ COPY 적재", "다중 행 INSERT → PostgreSQL COPY",
     ["--prune-columns", "--load-method", "copy", "--copy-chunk-rows", "0"], "pipeline.py"),
    (4, "+ 청크 COPY", "COPY를 20만 행씩 나눠 전송", ["--prune-columns", "--load-method", "copy"], "pipeline.py"),
    (5, "+ Spark 분산 처리", "pandas → Spark(코어 수만큼 파티션 병렬 처리·COPY)", ["--mode", "raw"], "spark_pipeline.py"),
    (6, "+ Spark 사전 집계", "원본 행 대신 시간대·요일별 집계만 적재", ["--mode", "agg"], "spark_pipeline.py"),
]
STAGE_ORDER = ["startup", "extract", "transform", "load", "process"]


def stage_text(stages: dict) -> str:
    return " / ".join(f"{k} {stages[k]['seconds']:.1f}s" for k in STAGE_ORDER if k in stages)


def run_step(script: str, opts: list[str], common: list[str], timeout: int) -> dict:
    fd, metrics_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    cmd = [sys.executable, os.path.join(BASE_DIR, script), *common, *opts, "--metrics-out", metrics_path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if proc.returncode != 0:
            output = (proc.stdout or "") + (proc.stderr or "")
            os.makedirs(os.path.join(OUT_DIR, "logs"), exist_ok=True)
            log_path = os.path.join(OUT_DIR, "logs", f"{os.path.splitext(script)[0]}_{datetime.now():%H%M%S}.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(output)
            if proc.returncode in (-9, 137):
                reason = "메모리 부족으로 강제 종료(OOM)"
            else:
                # 진짜 원인이 담긴 줄(…Error: / …Exception:)을 찾아 보여 준다
                errors = [ln.strip() for ln in output.splitlines()
                          if ("Error" in ln or "Exception" in ln) and not ln.strip().startswith("at ")]
                reason = (errors[-1][:200] if errors else "알 수 없는 오류")
            reason += f" (전체 로그: data/benchmarks/logs/{os.path.basename(log_path)})"
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


def load_history(path: str) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def main():
    parser = argparse.ArgumentParser(description="병목 해결 전·후 비교 벤치마크")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", "--months", dest="months", default="1",
                        help="측정할 월. 하나(1), 범위(1-6) — 여러 달이면 규모 실험")
    parser.add_argument("--limit", type=int, default=300_000, help="처리할 행 수 (기본 30만)")
    parser.add_argument("--full", action="store_true", help="한 달 전체 데이터로 측정")
    parser.add_argument("--step", "--steps", dest="steps", default="0,1,2,3,4,5,6",
                        help="실행할 단계 번호. 하나만(--step 2) 또는 여러 개(--steps 0,3)")
    parser.add_argument("--repeat", type=int, default=1, help="단계마다 반복 횟수 (중앙값 사용)")
    parser.add_argument("--timeout", type=int, default=1800, help="단계별 최대 실행 시간(초)")
    parser.add_argument("--show", action="store_true", help="실행하지 않고 지금까지 쌓인 결과 표만 보기")
    parser.add_argument("--reset", action="store_true", help="이 조건(월·행 수)으로 쌓인 결과를 지우고 시작")
    args = parser.parse_args()

    sys.path.insert(0, BASE_DIR)
    from pipeline import download, parse_months
    months = parse_months(args.months)
    month_label = f"{args.year}년 {months[0]}월" if len(months) == 1 else f"{args.year}년 {months[0]}~{months[-1]}월"
    common = ["--year", str(args.year), "--months", ",".join(map(str, months))]
    if not args.full:
        common += ["--limit", str(args.limit)]
    scope = f"{month_label}, " + ("전체 행" if args.full else f"각 달 앞에서 {args.limit:,}행")
    scope_key = f"{args.year}-{','.join(map(str, months))}|{'full' if args.full else args.limit}"
    spark_master = os.getenv("SPARK_MASTER", "")
    cluster = spark_master.startswith("spark://")
    if cluster:  # 클러스터 결과는 로컬 결과와 섞이지 않게 따로 쌓는다
        scope += f", Spark 클러스터({spark_master})"
        scope_key += "|cluster"

    os.makedirs(OUT_DIR, exist_ok=True)
    history_path = os.path.join(OUT_DIR, "history.json")
    history = load_history(history_path)
    if args.reset:
        history.pop(scope_key, None)
        print(f"초기화: {scope} 조건의 기존 결과를 지웠습니다.")
    saved = history.setdefault(scope_key, {})

    just_ran: set[int] = set()
    if not args.show:
        wanted = {int(x) for x in args.steps.split(",")}
        steps = [st for st in STEPS if st[0] in wanted]

        # 캐시를 쓰는 단계 전에 파일을 미리 받아 둔다 (측정에 포함하지 않음)
        if any(st[0] >= 1 for st in steps):
            for m in months:
                download(args.year, m, use_cache=True)

        print(f"\n벤치마크: {scope}, 단계 {[st[0] for st in steps]}, 반복 {args.repeat}회\n")
        for num, name, technique, opts, script in steps:
            print(f"[{num}] {name} 실행 중... ({script} {' '.join(opts)})", flush=True)
            r = median_result([run_step(script, opts, common, args.timeout) for _ in range(args.repeat)])
            if "error" in r:
                print(f"    → 실패: {r['error']}")
            else:
                print(f"    → {r['total_seconds']:.1f}초, 최대 {r['peak_mb']:,}MB ({stage_text(r['stages'])})")
            r["measured_at"] = f"{datetime.now():%Y-%m-%d %H:%M}"
            saved[str(num)] = r
            just_ran.add(num)

        with open(history_path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    # ── 지금까지 쌓인 결과로 표 만들기 ──
    rows = [
        {"num": num, "name": name, "technique": technique, "result": saved[str(num)]}
        for num, name, technique, _, _ in STEPS if str(num) in saved
    ]
    ok_rows = [row for row in rows if "error" not in row["result"]]
    if not ok_rows:
        print("\n이 조건에서 성공한 단계가 아직 없어 표를 만들 수 없습니다. 위의 실패 원인과 로그를 확인하세요.")
        return
    base_row = ok_rows[0]
    base = base_row["result"]

    lines = [
        f"# 벤치마크 결과 — {scope}",
        "",
        f"기준: [{base_row['num']}] {base_row['name']}  (★ = 이번에 실행한 단계)",
        "",
        "| 단계 | 새로 적용한 기법 | 총 시간 | 기준 대비 | 직전 단계 대비 | 최대 메모리 | 기준 대비 | 가장 오래 걸린 구간 | 측정 시각 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    csv_rows = []
    prev = None
    for row in rows:
        r = row["result"]
        mark = " ★" if row["num"] in just_ran else ""
        label = f"[{row['num']}] {row['name']}{mark}"
        if "error" in r:
            lines.append(f"| {label} | {row['technique']} | 실패 | {r['error']} | | | | | {r.get('measured_at', '')} |")
            csv_rows.append({"step": row["num"], "name": row["name"], "error": r["error"]})
            continue
        st = r["stages"]
        slowest = max(st, key=lambda k: st[k]["seconds"])
        share = st[slowest]["seconds"] / r["total_seconds"] if r["total_seconds"] else 0
        is_base = row is base_row
        lines.append(
            f"| {label} | {row['technique']} | {r['total_seconds']:.1f}초 "
            f"| {'기준' if is_base else pct_change(r['total_seconds'], base['total_seconds'])} "
            f"| {pct_change(r['total_seconds'], prev['total_seconds']) if prev else '-'} "
            f"| {r['peak_mb']:,}MB "
            f"| {'기준' if is_base else pct_change(r['peak_mb'], base['peak_mb'])} "
            f"| {slowest} ({share:.0%}) | {r.get('measured_at', '')} |"
        )
        csv_rows.append({
            "step": row["num"], "name": row["name"], "technique": row["technique"],
            "total_seconds": r["total_seconds"], "peak_mb": r["peak_mb"],
            **{f"{k}_s": st[k]["seconds"] for k in STAGE_ORDER if k in st},
            "rows_loaded": r["rows_loaded"],
        })
        prev = r

    used = [k for k in STAGE_ORDER if any(k in row["result"]["stages"] for row in ok_rows)]
    lines += ["", "## 구간별 시간 (초)", "",
              "| 단계 | " + " | ".join(used) + " | 적재 행 수 |",
              "|---|" + "---|" * len(used) + "---|"]
    for row in ok_rows:
        st = row["result"]["stages"]
        cells = " | ".join(f"{st[k]['seconds']:.1f}" if k in st else "-" for k in used)
        lines.append(f"| [{row['num']}] {row['name']} | {cells} | {row['result']['rows_loaded']:,} |")
    if any(row["num"] >= 5 for row in ok_rows):
        lines += ["", "Spark는 지연 실행이라 읽기·정제·적재가 `process` 구간에서 한꺼번에 실행된다. "
                  "`startup`은 Spark 세션(JVM) 준비 시간. 6단계의 적재 행 수는 원본이 아니라 집계 결과 행 수다."]

    report = "\n".join(lines)
    print("\n" + report + "\n")

    remaining = [st for st in STEPS if str(st[0]) not in saved]
    if remaining:
        nxt = remaining[0]
        print(f"다음 단계: --step {nxt[0]}  ({nxt[1]} — {nxt[2]})\n")

    slug = f"{args.year}-{'-'.join(map(str, months))}_{'full' if args.full else args.limit}" + ("_cluster" if cluster else "")
    md_path = os.path.join(OUT_DIR, f"bench_{slug}.md")
    csv_path = os.path.join(OUT_DIR, f"bench_{slug}.csv")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        fields = list(dict.fromkeys(k for r in csv_rows for k in r))
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(csv_rows)
    print(f"저장: {md_path}")


if __name__ == "__main__":
    main()
