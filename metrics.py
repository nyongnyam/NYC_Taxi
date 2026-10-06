"""
metrics.py — 단계별 소요 시간과 최대 메모리 측정 (pandas 파이프라인과 Spark 파이프라인 공용)

Spark는 실제 계산을 별도의 Java 프로세스(JVM)와 파이썬 워커 프로세스에서 하므로
현재 프로세스의 메모리만 재면 거의 0에 가깝게 나온다. 그래서 자식 프로세스까지 포함한
프로세스 트리 전체의 메모리(RSS)를 0.2초마다 샘플링해 최대값을 기록한다.
"""

import logging
import sys
import threading
import time
from contextlib import contextmanager

import psutil

log = logging.getLogger(__name__)


def _self_peak_mb() -> float:
    """운영체제가 기록해 둔 현재 프로세스의 정확한 최대 메모리"""
    if sys.platform == "win32":
        return psutil.Process().memory_info().peak_wset / (1024 * 1024)
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024  # Linux는 KB, macOS는 byte


class PeakMemory:
    """현재 프로세스 + 모든 자식 프로세스의 메모리 합계 최대값을 추적한다."""

    def __init__(self, interval: float = 0.2):
        self.interval = interval
        self._peak_tree = 0.0
        self._stop = threading.Event()
        self._proc = psutil.Process()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _sample(self) -> float:
        total = 0
        procs = [self._proc]
        try:
            procs += self._proc.children(recursive=True)
        except psutil.Error:
            pass
        for p in procs:
            try:
                total += p.memory_info().rss
            except psutil.Error:
                pass
        return total / (1024 * 1024)

    def _run(self):
        while not self._stop.is_set():
            self._peak_tree = max(self._peak_tree, self._sample())
            time.sleep(self.interval)

    def peak_mb(self) -> float:
        self._peak_tree = max(self._peak_tree, self._sample())
        return max(self._peak_tree, _self_peak_mb())

    def stop(self):
        self._stop.set()


TRACKER = PeakMemory()


@contextmanager
def stage(name: str, metrics: dict):
    """with stage("load", metrics): ... 블록의 소요 시간과 그 시점까지의 최대 메모리를 기록"""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    mem = TRACKER.peak_mb()
    metrics["stages"][name] = {"seconds": round(elapsed, 3), "peak_mb": round(mem)}
    log.info(f"  ⏱ {name}: {elapsed:.1f}초 (최대 메모리 {mem:,.0f}MB)")


def summarize(metrics: dict, rows_loaded: int) -> dict:
    total = sum(s["seconds"] for s in metrics["stages"].values())
    metrics["total_seconds"] = round(total, 3)
    metrics["peak_mb"] = round(TRACKER.peak_mb())
    metrics["rows_loaded"] = rows_loaded
    summary = " | ".join(
        f"{k} {v['seconds']:.1f}s ({v['seconds'] / total:.0%})" for k, v in metrics["stages"].items()
    ) if total else ""
    log.info(f"파이프라인 완료: 총 {total:.1f}초, 최대 메모리 {metrics['peak_mb']:,}MB — {summary}")
    return metrics
