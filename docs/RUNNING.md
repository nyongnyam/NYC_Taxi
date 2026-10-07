# 실행 가이드 (Windows · Docker Desktop 기준)

명령은 모두 **cmd 창에서 레포 폴더(`nyc_taxi_lab`)로 이동한 뒤** 입력한다.

```cmd
cd C:\Users\SSAFY\nyctaxi\nyc_taxi_lab
```

결과 표는 모두 `data\benchmarks\` 폴더에 저장된다.

---

## 0. 준비 (처음 한 번)

1. **Docker Desktop**을 켜고 왼쪽 아래가 `Engine running`이 될 때까지 기다린다.
2. 최신 코드를 받는다 (커밋 없이 파일만 가져오기).

   ```cmd
   git fetch origin local-bottleneck-lab
   git restore --source=FETCH_HEAD -- .
   ```

3. 설정 파일을 만든다 (이미 있으면 건너뛴다).

   ```cmd
   copy .env.example .env
   ```

4. 이미지를 빌드한다. Spark(Java 포함)가 들어가서 **처음엔 5~10분** 걸린다.

   ```cmd
   docker compose --profile batch --profile stream --profile spark build
   ```

5. 인프라(PostgreSQL, Kafka, Kafka UI)를 띄운다.

   ```cmd
   docker compose up -d
   docker compose ps
   ```

   `db`, `kafka`가 `(healthy)`가 되면 준비 완료. Kafka UI: http://localhost:8080

---

## 1. 배치 병목 실험 — 기법을 하나씩 추가하며 비교

```cmd
docker compose run --rm bench --step 0
docker compose run --rm bench --step 1
docker compose run --rm bench --step 2
docker compose run --rm bench --step 3
docker compose run --rm bench --step 4
docker compose run --rm bench --step 7
docker compose run --rm bench --step 8
docker compose run --rm bench --step 9
```

(5·6단계는 Spark라서 아래 2장에서 따로 다룬다.)

| 단계 | 추가되는 기법 | 볼 것 |
|---|---|---|
| 0 | 기존 코드 (기준) | 시간 대부분이 `load`인지 |
| 1 | 다운로드 캐싱 | `extract` 시간 감소 |
| 2 | 컬럼 프루닝 | **메모리** 감소 |
| 3 | COPY 적재 | `load` 시간이 극적으로 감소 |
| 4 | 청크 COPY | 30만 행에선 작고, `--full`에서 메모리 차이가 커짐 |
| 7 | 4단계 + Arrow CSV 변환 | CSV 변환 시간이 사라져 `load`가 줄어듦 |
| 8 | + 병렬 COPY | 코어가 많을수록 크게 줄어듦 |
| 9 | + 인덱스 나중에 생성 | 로그의 "인덱스 다시 생성 ○초"와 함께 확인 |

실행할 때마다 지금까지의 단계가 표로 나오고, 0단계 대비·직전 단계 대비 감소율이 표시된다.

| 옵션 | 뜻 |
|---|---|
| `--show` | 실행 없이 쌓인 표만 보기 |
| `--reset --step 0` | 기록 지우고 처음부터 |
| `--limit 1000000` | 행 수 바꾸기 (기본 30만, 모든 단계에 같은 값을 붙일 것) |
| `--full` | 한 달 전체 (0~2단계는 각각 10분 이상) |
| `--repeat 3` | 3번 돌려 중앙값 |

---

## 2. Spark — 5·6단계

```cmd
docker compose run --rm bench --step 5
docker compose run --rm bench --step 6
```

- **5단계 (Spark raw)**: pandas 대신 Spark로 읽고·정제하고, 코어 수만큼 나눠 병렬로 COPY.
  30만 행에서는 **4단계보다 느린 게 정상**이다 (JVM 시작 약 5초 + 데이터 이동 비용).
- **6단계 (Spark agg)**: 원본 대신 시간대·요일별 집계만 저장. 결과는 아래로 확인한다.

  ```cmd
  docker compose exec db psql -U postgres -d nyctaxi -c "SELECT * FROM v_hourly_stats_fast;"
  ```

### 2-0. 컨테이너 메모리 제한

기본은 **제한 없음**이다 (Docker Desktop에 할당된 메모리까지 쓸 수 있다). 작은 서버를 흉내 내는 실험을 할 때만 상한을 건다. `=` 앞뒤에 공백을 넣지 않는다.

```cmd
set APP_MEM_LIMIT=2g
docker compose run --rm bench ...
set APP_MEM_LIMIT=
```

### 2-1. 규모 실험 — Spark가 이기는 지점

여러 달을 한 번에 처리한다. 2~6월 파일은 처음 실행할 때 자동으로 받는다(한 달 약 50MB).

```cmd
docker compose run --rm bench --months 1-6 --full --steps 4,5,6
```

메모리를 제한하면 차이가 더 분명해진다. pandas(4단계)는 메모리 부족으로 죽고, Spark(5·6단계)는 끝까지 처리한다. Spark는 기본으로 코어를 최대 4개만 쓴다(코어마다 메모리를 쓰므로). 늘리려면 `-e SPARK_LOCAL_CORES=8`.

```cmd
set APP_MEM_LIMIT=2g
docker compose run --rm bench --months 1-6 --full --steps 4,5,6
set APP_MEM_LIMIT=
```

마지막 줄은 제한을 다시 푸는 것이다. 6개월치는 DB에 약 1,600만 행이 들어가므로 디스크를 3~4GB 쓴다.

### 2-2. 진짜 분산 — Spark 클러스터

마스터 1개 + 워커 3개를 띄우고, 같은 작업을 클러스터에 맡긴다.

```cmd
docker compose --profile spark up -d --scale spark-worker=3
docker compose run --rm -e SPARK_MASTER=spark://spark-master:7077 bench --step 5
docker compose run --rm -e SPARK_MASTER=spark://spark-master:7077 bench --step 6
```

- http://localhost:8090 : 워커 목록과 실행 중인 작업
- `docker stats` : 워커 컨테이너별 CPU·메모리 (Ctrl+C로 종료)
- 클러스터 결과는 로컬 결과와 **따로 쌓인다** (표 제목에 `Spark 클러스터` 표시)
- 같은 PC 안에서는 로컬보다 느린 게 정상이다. 워커가 같은 CPU를 나눠 쓰면서 통신 비용만 늘기 때문

워커 수·사양 바꾸기:

```cmd
set SPARK_WORKER_CORES=2
set SPARK_WORKER_MEMORY=2g
docker compose --profile spark up -d --scale spark-worker=2
```

끝나면 클러스터만 내린다.

```cmd
docker compose --profile spark stop spark-master spark-worker
```

---

## 3. 이 PC에서 최고 성능 내기

### 3-1. PostgreSQL 대량 적재 설정 켜기

```cmd
docker compose exec -T db psql -U postgres -d nyctaxi < sql/pg_bulk_load.sql
docker compose restart db
```

되돌릴 때는 `sql/pg_bulk_load.sql` 대신 `sql/pg_reset.sql`로 같은 두 줄을 실행한다.

### 3-2. 최적 값 찾기

```cmd
docker compose run --rm --entrypoint python bench tune.py
```

pandas 병렬 COPY 연결 수(1·2·4·8·16)와 Spark 코어 수(2·4·8·16·전체)를 하나씩 돌려 본다. 끝에 나오는 값을 `.env`에 붙여 넣는다.

```
CSV_ENGINE=arrow
COPY_WORKERS=8
SPARK_LOCAL_CORES=16
```

데이터를 키워서 재려면 `tune.py --months 1-3`. 결과는 `data\benchmarks\tuning_*.md`에 저장된다.

### 3-3. 확인

```cmd
docker compose run --rm bench --full --steps 4,7,8,9
```

---

## 4. Kafka

### 4-1. 기본 흐름

```cmd
docker compose --profile stream up -d consumer
docker compose run --rm producer --limit 300000
docker compose logs -f consumer
```

로그 보기는 `Ctrl+C`로 멈춘다 (consumer는 계속 실행됨).

### 4-2. 파티션 × consumer 실험

```cmd
docker compose run --rm kafka-bench
docker compose run --rm kafka-bench --combos 1x1,2x2,4x4,8x8
```

| 조합 | 볼 것 |
|---|---|
| 1x1 → 3x1 | 파티션만 늘리고 consumer 1개면 거의 그대로 |
| 3x1 → 3x3 | consumer가 파티션을 하나씩 맡아 빨라짐 |
| 3x3 → 3x4 | 4번째 consumer는 놂 (`일한 consumer 3/4`) |
| 3x3 → 6x6 | 비례해서 빨라지지 않는 지점 |

실험용 테이블(`bench_stream_trips`)에 적재하므로 기존 데이터는 그대로다.

---

## 5. 데이터 확인과 정리

```cmd
docker compose exec db psql -U postgres -d nyctaxi -c "SELECT COUNT(*) FROM clean_taxi_trips;"
docker compose exec db psql -U postgres -d nyctaxi -c "TRUNCATE clean_taxi_trips;"
```

| 명령 | 효과 |
|---|---|
| `docker compose --profile batch --profile stream --profile spark down` | 모든 컨테이너 종료 (데이터 유지) |
| 위 명령 + `-v` | DB·Kafka 데이터까지 삭제 |
| 위 명령 + `-v --rmi all`, 그다음 `docker builder prune -af` | 이미지와 빌드 캐시까지 삭제 → PC에 흔적 거의 없음 |

---

## 6. 문제 해결

| 증상 | 해결 |
|---|---|
| `unknown shorthand flag: 'd'` | Docker Desktop이 꺼져 있음. 켜고 새 cmd 창에서 다시 |
| `No services to build` | `build` 앞에 `--profile batch --profile stream --profile spark` |
| `port is already allocated` (5432) | PC에 PostgreSQL이 이미 있음. `.env`에 `PG_PORT=5433` 추가 |
| `Coordinator load in progress` 경고 | Kafka 시작 직후의 정상 경고. 30초쯤 지나면 사라짐 |
| 벤치마크에 `메모리 부족으로 강제 종료(OOM)` | `APP_MEM_LIMIT`을 걸어 둔 상태인지 확인 (`set APP_MEM_LIMIT=`로 해제). 제한이 없는데도 나면 `--limit`을 줄이거나 Docker Desktop → Settings → Resources에서 메모리 늘리기 |
| Spark가 시작하자마자 실패 | 코어 수 대비 메모리 부족. `-e SPARK_LOCAL_CORES=4`처럼 코어를 줄인다 |
| `too many clients` (PostgreSQL) | 병렬 연결 수가 너무 많음. `COPY_WORKERS`나 Spark 코어 수를 줄인다 |
| Spark 클러스터 작업이 시작하지 않음 | http://localhost:8090 에 워커가 보이는지 확인. 워커 메모리(`SPARK_WORKER_MEMORY`)가 `SPARK_EXECUTOR_MEMORY`(기본 1g)보다 커야 함 |
| 코드를 다시 받았는데 반영이 안 됨 | `docker compose --profile batch --profile stream --profile spark build`를 다시 실행 |
