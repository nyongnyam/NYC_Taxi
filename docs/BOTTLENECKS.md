# 병목 분석과 실험 가이드

이 문서는 파이프라인을 로컬에서 직접 돌려 보며 **어디가 느린지 측정하고, 왜 느린지 확인하고, 고쳐서 다시 측정하는** 과정을 정리한 것이다. 아래 숫자는 1차 측정값이고, 같은 실험을 본인 PC에서 다시 돌려 표를 채워 나가는 것이 이 문서의 목적이다.

> **측정 환경**: Linux, 메모리 8GB, PostgreSQL 16 (로컬), 2024년 1월 데이터 (원본 2,964,624행 → 정제 후 2,713,464행)
> 머신마다 절대값은 달라지지만 **단계 간 비율과 방식 간 차이**는 거의 그대로 재현된다.

---

## 0. 한 번에 비교하기 — benchmark.py

`pipeline.py`의 기본값은 **최적화 전 동작**(전체 컬럼 읽기 + 다중 행 INSERT)이고, 병목 해결 기법은 옵션으로 하나씩 켠다. `benchmark.py`는 기존 코드를 기준으로 기법을 하나씩 추가하며 각 단계를 별도 프로세스로 실행하고, **기준 대비 몇 % 줄었는지** 표로 정리해 준다.

**하나씩 추가하며 보기 (권장)** — 실행할 때마다 결과가 쌓이고, 지금까지의 단계가 모두 표에 나온다.

```bash
docker compose run --rm bench --step 0     # 기준 측정: 기존 코드
docker compose run --rm bench --step 1     # + 다운로드 캐싱  → 0단계 대비 몇 % 줄었는지
docker compose run --rm bench --step 2     # + 컬럼 프루닝
docker compose run --rm bench --step 3     # + COPY 적재
docker compose run --rm bench --step 4     # + 청크 COPY
docker compose run --rm bench --show       # 실행 없이 지금까지의 표만 보기
docker compose run --rm bench --reset --step 0   # 처음부터 다시
```

**한 번에 전부**

```bash
docker compose run --rm bench                       # 30만 행, 0~4단계 (몇 분)
docker compose run --rm bench --full --step 3       # 한 달 전체 (행 수가 다르면 결과도 따로 쌓인다)
docker compose run --rm bench --repeat 3            # 단계마다 3번 실행해 중앙값 사용
```

로컬 Python이라면 `docker compose run --rm bench` 대신 `python benchmark.py`를 쓰면 된다.

| 단계 | 새로 적용하는 기법 | `pipeline.py` 옵션 |
|---|---|---|
| 0 | 기존 코드 (기준) | `--no-cache --load-method multi` |
| 1 | 다운로드 캐싱 | `--load-method multi` |
| 2 | 컬럼 프루닝 | `--prune-columns --load-method multi` |
| 3 | COPY 적재 | `--prune-columns --load-method copy --copy-chunk-rows 0` |
| 4 | 청크 COPY | `--prune-columns --load-method copy` |
| 5 | Spark 분산 처리 | `spark_pipeline.py --mode raw` |
| 6 | Spark 사전 집계 | `spark_pipeline.py --mode agg` |
| 7 | 4단계 + Arrow CSV 변환 | `--csv-engine arrow` |
| 8 | + 병렬 COPY | `--copy-workers 0` (자동: 코어 수, 최대 8) |
| 9 | + 인덱스 나중에 생성 | `--defer-indexes` |

결과는 화면에 출력되고 `data/benchmarks/bench_2024-01_300000.md`(`.csv`)로 저장된다. 누적 기록은 `data/benchmarks/history.json`에 있다.

### 1차 측정 결과 (30만 행)

| 단계 | 총 시간 | 기준 대비 | 직전 단계 대비 | 최대 메모리 | 기준 대비 |
|---|---|---|---|---|---|
| [0] 기존 코드 | 77.2초 | 기준 | - | 1,675MB | 기준 |
| [1] + 다운로드 캐싱 | 69.7초 | 9.7% 감소 | 9.7% 감소 | 1,664MB | 0.7% 감소 |
| [2] + 컬럼 프루닝 | 68.9초 | 10.7% 감소 | 1.1% 감소 | 1,187MB | 29.1% 감소 |
| [3] + COPY 적재 | 4.0초 | **94.9% 감소** | 94.2% 감소 | 739MB | 55.9% 감소 |
| [4] + 청크 COPY | 4.0초 | 94.8% 감소 | 변화 거의 없음 | 697MB | **58.4% 감소** |

읽는 법:

- **캐싱**은 이 측정에서 다운로드가 로컬 서버라 5초 정도만 줄었다. 실제 인터넷(NYC TLC 서버)에서는 다운로드 시간이 훨씬 길어서 감소율이 커진다. 본인 환경에서 꼭 다시 재 보자.
- **컬럼 프루닝**은 시간보다 **메모리**를 줄이는 기법이다 (-29%).
- **COPY**가 시간 병목의 본체를 해결한다. 적재 구간이 69초에서 3.5초로 줄었다.
- **청크 COPY**는 30만 행에서는 두 번만 나눠 보내서 효과가 작다. 한 달 전체(271만 행)에서는 최대 메모리가 1,988MB에서 1,233MB로 **38% 줄어든다**(아래 2장). 데이터가 커질수록 차이가 커지는 기법이라 `--full`로 비교해 보자.

---

## 0-1. 단계별 로그 읽는 법

`pipeline.py`는 단계마다 소요 시간과 그 시점까지의 최대 메모리를 남긴다.

```
⏱ extract: 0.2초 (최대 메모리 520MB)
⏱ transform: 1.5초 (최대 메모리 1,243MB)
⏱ load: 28.5초 (최대 메모리 1,233MB)
파이프라인 완료: 총 30.2초, 최대 메모리 1,233MB — extract 0.2s (1%) | transform 1.5s (5%) | load 28.5s (94%)
```

먼저 **비율**을 본다. 위 결과는 시간의 94%가 적재에서 쓰인다는 뜻이고, 정제(transform)를 아무리 최적화해도 전체는 5%밖에 빨라지지 않는다. 병목 분석은 항상 "가장 큰 덩어리부터"다.

함수 단위까지 내려가 보고 싶으면 `cProfile`을 쓴다.

```bash
python -m cProfile -s tottime pipeline.py --limit 50000 --load-method multi | head -30
```

---

## 1. 병목 ① — 원본 다운로드 (해결됨: 캐싱)

| | 소요 시간 |
|---|---|
| 첫 실행 (다운로드 약 50MB) | 네트워크 속도에 좌우 (수 초 ~ 수십 초) |
| 재실행 (캐시 사용) | 0.2초 |

**기법**: `data/raw/`에 파일이 있으면 다운로드를 건너뛴다.

**보완한 점**: 원래 코드는 최종 파일명으로 바로 내려받았기 때문에, 다운로드가 중간에 끊기면 **깨진 파일이 "이미 있음"으로 처리되어 계속 재사용**됐다. 지금은 `.part` 임시 파일로 받은 뒤 완료되면 이름을 바꾼다(`os.replace`는 원자적이다).

**직접 확인하기**: 다운로드 도중 Ctrl+C → `data/raw/`에 `.part`만 남고, 재실행하면 처음부터 다시 받는다.

---

## 2. 병목 ② — 적재 방식 (가장 큰 병목)

### 측정 결과

| 적재 방식 | 30만 행 | 한 달 전체 (271만 행) | 최대 메모리 (전체) |
|---|---|---|---|
| `multi` — 기존 방식 (`to_sql`, chunksize 5만, 다중 행 INSERT) | 61초 | **10분 넘게 걸려 중단** | — |
| `single` — `to_sql` executemany, chunksize 없음 | 15.5초 | **메모리 5GB 초과로 강제 종료(OOM)** | 5GB+ |
| `copy` — 한 번에 COPY | 3.4초 | 31.2초 | 1,988MB |
| `copy` — 20만 행씩 나눠 COPY (현재 기본값) | — | **28.5초** | **1,233MB** |

```bash
python pipeline.py --prune-columns --limit 300000 --load-method multi
python pipeline.py --prune-columns --limit 300000 --load-method single
python pipeline.py --prune-columns --limit 300000 --load-method copy
```

### 왜 `multi`가 느린가 — 프로파일 결과

5만 행을 `multi`로 넣을 때 총 19.1초 중

| 구간 | 시간 |
|---|---|
| 실제 DB 실행 (`psycopg2 cursor.execute`) | **2.3초** |
| SQLAlchemy가 INSERT 문을 조립 (`_extend_values_for_multiparams` 등) | **13.1초** |

`INSERT ... VALUES (...), (...), ...` 한 문장에 5만 행 × 12컬럼 = **약 57만 개의 바인드 파라미터**가 들어간다. SQLAlchemy가 파라미터마다 파이썬 객체를 만들고 이름을 붙이는 작업이 DB보다 6배 오래 걸린다. 즉 병목은 DB가 아니라 **파이썬 CPU**다.

### 왜 `single`은 메모리가 터지는가

`chunksize`를 주지 않으면 pandas가 271만 행 전체를 파라미터 리스트(파이썬 dict 271만 개)로 한 번에 변환한다. DataFrame 자체는 수백 MB인데, 파이썬 객체로 풀어놓는 순간 몇 배로 불어난다. **chunksize는 속도보다 메모리를 위한 옵션**이라는 걸 보여 주는 예다.

### 왜 COPY가 빠른가

COPY는 파라미터를 하나하나 바인딩하지 않고 CSV 스트림을 그대로 서버로 흘려보낸다. 파싱도 서버의 C 코드가 한다. 30만 행 기준 3.4초 중 약 1.7초는 pandas `to_csv`(파이썬 쪽 직렬화), 나머지가 서버 처리다. 여기서 더 빠르게 하려면 직렬화를 줄여야 한다(바이너리 COPY, Arrow → Postgres 직접 적재 등).

### 청크 COPY가 메모리를 줄이는 이유

한 번에 COPY하면 271만 행짜리 CSV 문자열(수백 MB)이 DataFrame과 별도로 메모리에 올라간다. 20만 행씩 나누면 그 버퍼가 작아져 **속도는 같고 최대 메모리는 약 750MB 줄어든다**. 같은 트랜잭션 안에서 나눠 보내므로 중간에 실패해도 전부 롤백된다.

```bash
python pipeline.py --prune-columns --load-method copy --copy-chunk-rows 0       # 한 번에
python pipeline.py --prune-columns --load-method copy --copy-chunk-rows 50000   # 더 잘게
```

---

## 3. 병목 ③ — 읽기 메모리 (해결됨: 컬럼 프루닝)

원본 parquet에는 19개 컬럼이 있지만 실제로 쓰는 건 8개다. parquet은 컬럼 단위로 저장되므로 필요한 컬럼만 지정하면 나머지는 디스크에서 읽지도 않는다.

| | 컬럼 수 | 읽기 시간 | DataFrame 크기 | 최대 메모리 |
|---|---|---|---|---|
| 전체 읽기 | 19 | 0.39초 | 418MB | 993MB |
| 필요한 컬럼만 | 8 | 0.23초 | 178MB | 520MB |

`--prune-columns` 옵션을 켜면 `extract()`가 `pd.read_parquet(path, columns=...)`로 필요한 컬럼만 읽는다.

---

## 4. 병목 ④ — 대시보드 조회 (미해결: 직접 해볼 과제)

`v_hourly_stats` 같은 분석 뷰는 일반 VIEW라 Tableau가 조회할 때마다 **271만 행 전체를 다시 GROUP BY**한다. `pickup_hour` 인덱스가 있어도 전체 집계에서는 거의 쓰이지 않는다.

```sql
EXPLAIN ANALYZE SELECT * FROM v_hourly_stats;   -- Seq Scan + HashAggregate 확인
```

**해볼 것**: `CREATE MATERIALIZED VIEW mv_hourly_stats AS ...`로 바꾸고, 적재가 끝날 때 `REFRESH MATERIALIZED VIEW`를 호출한다. 몇 개월 치를 쌓은 뒤 두 방식의 조회 시간을 비교해 보자.

---

## 5. Kafka 스트리밍 경로의 병목

```
parquet ──▶ producer.py ──▶ [taxi-trips 토픽, 파티션 3개] ──▶ consumer.py ×N ──▶ PostgreSQL
            (JSON 직렬화)        (키: 승차 지역 ID)             (마이크로 배치 + COPY)
```

### 브로커를 제외한 1차 측정

| 구간 | 처리량 |
|---|---|
| Producer 쪽 parquet 읽기 + JSON 직렬화 | 약 81,000건/초 |
| Consumer 쪽 JSON 파싱 + 정제 + COPY (1만 건 묶음) | 약 58,000건/초 |
| 메시지 1건 평균 크기 | 238 bytes (한 달 ≈ 700MB, 압축 전) |

배치 COPY(약 95,000행/초)보다 느린 이유는 **행마다 JSON으로 바꿨다가 다시 푸는 비용** 때문이다. 스트리밍의 대가가 숫자로 보인다. 브로커를 포함한 실제 처리량은 `docker compose`로 띄운 뒤 아래 실험으로 측정해서 이 표를 채우자.

### 실험 A — Producer 배칭과 압축

```bash
docker compose run --rm producer --limit 500000 --linger-ms 0  --compression none
docker compose run --rm producer --limit 500000 --linger-ms 20 --compression lz4
docker compose run --rm producer --limit 500000 --linger-ms 20 --compression zstd
docker compose run --rm producer --limit 500000 --acks 1
```

**볼 것**: 마지막 줄의 `건/초`와 `큐 가득 참 N회`. `linger.ms=0`이면 메시지를 거의 모으지 않고 보내므로 요청 수가 폭증한다. "큐 가득 참"이 많다면 producer가 브로커보다 빠르다는 뜻(backpressure)이다.

### 실험 B — Consumer 묶음 크기

```bash
docker compose run --rm consumer --batch-size 100   --exit-when-idle 15
docker compose run --rm consumer --batch-size 10000 --exit-when-idle 15
```

(각 실행 전에 `KAFKA_GROUP_ID`를 바꾸거나 테이블을 비워야 같은 데이터로 비교할 수 있다.)

**볼 것**: 로그의 `DB 시간 비중`과 `lag`. 묶음이 작으면 커밋(트랜잭션 + 오프셋 커밋)이 너무 자주 일어나 lag이 줄지 않는다. 묶음이 크면 처리량은 오르지만 데이터가 DB에 보이기까지의 **지연(latency)**이 늘어난다. 처리량과 지연의 트레이드오프다.

### 실험 C — 파티션 수와 Consumer 수

**자동 실험 (권장)**

```bash
docker compose run --rm kafka-bench                              # 기본 조합: 1x1, 3x1, 3x3, 3x4, 6x6
docker compose run --rm kafka-bench --combos 1x1,2x2,4x4,8x8     # 원하는 조합 (파티션x컨슈머)
docker compose run --rm kafka-bench --limit 1000000              # 메시지 수 늘리기
```

파티션 수마다 실험용 토픽을 새로 만들어 메시지를 넣고, consumer를 지정한 수만큼 동시에 띄워 토픽을 다 비울 때까지의 처리량을 잰다. 적재는 실험용 테이블(`bench_stream_trips`)에 하므로 `clean_taxi_trips`는 건드리지 않는다. 결과는 `data/benchmarks/kafka_partitions_*.md`로 저장된다.

| 조합 | 확인할 것 |
|---|---|
| 1x1 → 3x1 | 파티션만 늘리고 consumer가 1개면 처리량이 거의 그대로다. **파티션은 병렬화의 '자리'일 뿐, 일할 consumer가 있어야 빨라진다.** |
| 3x1 → 3x3 | consumer가 파티션을 하나씩 맡아 병렬로 처리한다. 몇 배가 되는지 확인 |
| 3x3 → 3x4 | 4번째 consumer는 할당받을 파티션이 없어 논다(`일한 consumer 3/4`). **병렬성의 상한은 파티션 수다.** |
| 3x3 → 6x6 | 더 늘려도 비례해서 빨라지지 않는 지점이 온다. CPU 수(결과 표 위쪽), DB 쓰기 경합, 파티션 편차 중 무엇이 원인인지 따져 보자 |

**파티션 편차**: 메시지 키가 승차 지역 ID라서, 메시지가 몰리는 지역(공항, 맨해튼 중심부)이 있는 파티션은 다른 파티션보다 메시지가 많다. 결과 표의 `최대/평균` 값이 1보다 클수록 한 consumer에 일이 몰려 나머지가 먼저 끝나고 기다리게 된다. 처리 시간은 가장 바쁜 파티션이 결정한다.

**수동으로 보기**

```bash
docker compose --profile stream up -d --scale consumer=3
docker compose run --rm producer
```

Kafka UI(http://localhost:8080)에서 consumer group을 보면 파티션이 consumer마다 하나씩 할당된다. `--scale consumer=4`로 늘려 보면 **4번째 consumer는 할당받을 파티션이 없어 놀게 된다.** 병렬성의 상한은 파티션 수다.

또 키가 승차 지역 ID라서 특정 지역(공항, 맨해튼 중심부)에 이벤트가 몰리면 파티션 간 부하가 고르지 않다(**데이터 스큐**). Kafka UI에서 파티션별 메시지 수를 비교해 보자.

### 실험 D — 전달 보장과 중복

consumer는 **DB 커밋 → 오프셋 커밋** 순서로 처리한다(at-least-once). 그 사이에 죽으면 같은 묶음을 다시 읽는다.

```bash
docker compose run --rm producer --limit 300000 &
docker compose --profile stream up -d consumer
sleep 5 && docker compose kill consumer      # 처리 도중 강제 종료
docker compose --profile stream up -d consumer
```

```sql
-- 중복 확인
SELECT COUNT(*) - COUNT(DISTINCT (tpep_pickup_datetime, tpep_dropoff_datetime, vendor_id, fare_amount, trip_distance))
FROM clean_taxi_trips;
```

**생각해 볼 것**: 원본에는 운행 ID가 없다. 중복을 막으려면 producer에서 고유 키를 만들어 붙이고 `ON CONFLICT DO NOTHING`을 쓰거나, 오프셋을 DB에 함께 저장하는 방법이 있다. 각각 처리량에 어떤 영향을 주는지 측정해 보자.

### 배치와 스트림의 결과가 다른 이유

같은 30만 행을 넣어도 배치는 287,010행, 스트림은 287,022행이 적재된다. 원본 1월 파일에는 다른 달 기록이 섞여 있는데, 배치는 "2024년 1월"로 걸러내지만 스트림은 이벤트가 들어오는 대로 받기 때문이다. 스트리밍에서는 **늦게 도착하거나 범위를 벗어난 이벤트**를 어떻게 다룰지 별도로 정해야 한다.

---

## 6. 메모리 제한 실험 (클라우드 준비용)

클라우드의 작은 인스턴스(1~2GB)를 흉내 내려면 컨테이너 메모리를 제한해 보면 된다.

```bash
APP_MEM_LIMIT=1g docker compose run --rm pipeline --prune-columns --load-method copy
APP_MEM_LIMIT=1g docker compose run --rm pipeline --load-method single
APP_MEM_LIMIT=1g docker compose run --rm bench --full
```

어떤 방식이 살아남고 어떤 방식이 OOM으로 죽는지 확인하면, 클라우드 인스턴스 크기를 근거 있게 고를 수 있다.

## 7. Spark — 언제 빨라지고 언제 느려지나

`spark_pipeline.py`는 같은 정제 규칙을 Spark로 옮긴 것이다. 두 가지 모드가 있다.

| 모드 | 하는 일 | 벤치마크 단계 |
|---|---|---|
| `raw` | 정제한 모든 행을 `clean_taxi_trips`에 적재. 파티션마다 DB 연결을 따로 열어 COPY를 병렬로 보낸다 | 5 |
| `agg` | 원본 행 대신 월·시간대·요일별 **합계와 건수**만 `agg_hourly_monthly`, `agg_weekday_monthly`에 적재 | 6 |

`agg` 결과는 `v_hourly_stats_fast`, `v_weekday_stats_fast`, `v_monthly_trend_fast` 뷰로 바로 조회할 수 있다. 기존 뷰(`v_hourly_stats` 등)와 같은 값을 주지만, 수백만 행 대신 수백 행만 읽는다. 4장의 **대시보드 조회 병목**에 대한 하나의 답이다.

### 측정 결과 1 — 데이터가 작을 때 (2024년 1월)

| | 30만 행 | 한 달 전체 (271만 행) |
|---|---|---|
| [4] pandas + 청크 COPY | **3.2초** | **36.9초** |
| [5] Spark raw (local[2]) | 14.4초 | 39.2초 |
| [6] Spark agg | 11.7초 | 18.6초 |

- 30만 행에서 Spark가 4배 넘게 느리다. 그중 약 5초는 **Spark 세션(JVM) 시작 시간**이고, 나머지는 JVM과 파이썬 워커 사이에 데이터를 옮기는 비용이다.
- 한 달 치가 되면 둘이 거의 같아진다. **이 크기가 손익분기점**이다.
- 6단계가 빠른 이유는 Spark라서가 아니라 **DB에 쓰는 양이 271만 행에서 31행으로 줄었기 때문**이다. pandas로 집계해도 같은 효과가 난다. 병목이 적재에 있으니, 적재할 양을 줄이는 게 가장 큰 개선이다.

### 측정 결과 2 — 데이터가 클 때 (6개월, 약 1,628만 행, 메모리 2GB 제한)

| | 결과 | 최대 메모리 |
|---|---|---|
| [4] pandas + 청크 COPY | **메모리 부족으로 강제 종료** | 2GB 초과 (제한 없이 돌려도 약 6GB에서 종료됨) |
| [5] Spark raw | 151.5초 | 1,967MB |
| [6] Spark agg | 50.9초 | 1,586MB |

> 이 측정은 작업 환경에서 실제 2~6월 파일을 받을 수 없어 1월 데이터를 날짜만 옮겨 만든 데이터로 했다. 본인 PC에서는 실제 파일로 다시 재 보자.

pandas는 6개월치를 **한 번에 메모리에 올리므로** 데이터 크기만큼 메모리가 필요하다. Spark는 파티션 단위로 읽고, 정제하고, 쓰고, 버리기를 반복하므로 데이터가 메모리보다 커도 끝까지 처리한다. **Spark를 쓰는 진짜 이유는 속도보다 이것이다.**

참고로 pandas도 "한 달씩 나눠서 반복"하면 메모리 문제를 피할 수 있다. Spark를 꼭 써야 하는 건 데이터가 한 대의 컴퓨터로 감당하기 어려울 만큼 커지거나, 조인·집계처럼 전체 데이터를 섞어야 하는 작업이 있을 때다.

```bash
# 규모 실험 (2~6월 파일은 처음 실행할 때 자동으로 받는다, 한 달에 약 50MB)
docker compose run --rm bench --months 1-6 --full --steps 4,5,6
# 메모리 제한을 걸고 (Windows cmd)
set APP_MEM_LIMIT=2g
docker compose run --rm bench --months 1-6 --full --steps 4,5,6
set APP_MEM_LIMIT=
```

### 측정 결과 3 — 진짜 분산: Spark 클러스터

`docker compose --profile spark`로 마스터 1개와 워커 여러 개를 띄우면, 드라이버(bench 컨테이너)가 작업을 쪼개 워커 컨테이너들에 나눠 준다.

| 2024년 1월 (워커 2개 × 코어 1개) | local[*] | 클러스터 |
|---|---|---|
| 30만 행 | 16.7초 | 28.4초 |
| 한 달 전체 | 39.2초 | 57.7초 |

**같은 컴퓨터 안에서는 클러스터가 더 느리다.** 워커들이 같은 CPU를 나눠 쓰는데, 작업을 나눠 보내고 결과를 모으는 통신 비용만 더해지기 때문이다. 클러스터의 이득은 **워커가 서로 다른 컴퓨터에 있어서 CPU와 메모리가 실제로 늘어날 때** 생긴다. 이 실험의 목적은 구조(드라이버 → 마스터 → 워커 → executor)를 직접 보는 것이다. http://localhost:8090 에서 워커와 작업 진행을 볼 수 있다.

클러스터 모드에서 표의 최대 메모리는 **드라이버 쪽만** 잰 값이다. 실제 계산은 워커 컨테이너에서 일어나므로 `docker stats`로 워커 메모리를 따로 보자.

### Spark에서 알아 둘 차이

- **지연 실행**: 읽기와 정제는 계획만 세우고, 실제 계산은 쓰기를 시작할 때 한꺼번에 일어난다. 그래서 측정 구간이 `startup`과 `process` 두 개뿐이다.
- **원자성**: raw 모드는 파티션마다 따로 커밋한다. 중간에 실패하면 일부 파티션만 적재된 상태가 남을 수 있다. pandas 쪽은 한 트랜잭션이라 전부 아니면 전무다.
- **반올림**: Spark의 `round`는 0.5를 올리고(HALF_UP), pandas는 짝수 쪽으로 반올림한다(HALF_EVEN). 그래서 `tip_rate` 합계가 아주 조금 다를 수 있다.

## 8. 로컬 최적화 — 남은 병목을 끝까지 쫓기

4단계(청크 COPY)에서 한 달 치 적재는 약 31초였다. 여기서 시간을 더 쪼개 보면 이렇다.

| 구간 | 시간 | 원인 |
|---|---|---|
| pandas `to_csv`로 CSV 문자열 만들기 | **15.9초** | 파이썬이 한 줄씩 문자열을 만든다 (코어 1개) |
| PostgreSQL이 COPY 받아들이기 | 12.9초 | 연결 1개 = DB 프로세스 1개 = 코어 1개 |
| └ 그중 인덱스 갱신 | 약 4초 | 행이 들어올 때마다 인덱스 2개를 같이 고친다 |

**둘 다 코어 1개만 쓰는 게 문제**였다. 코어가 여러 개여도 놀고 있었던 것이다.

### 단계별 개선 (2024년 1월 전체, 이 측정 환경은 2코어)

| 단계 | 기법 | 총 시간 | 4단계 대비 |
|---|---|---|---|
| [4] | 청크 COPY (pandas CSV, 연결 1개) | 31.3초 | 기준 |
| [7] | + Arrow CSV 변환 (C++·컬럼 단위 변환, 15.9초 → 2.5초) | 20.9초 | 33.4% 감소 |
| [8] | + 병렬 COPY (연결 2개) | 13.9초 | 55.5% 감소 |
| [9] | + 인덱스 나중에 생성 | 12.0초 | **61.8% 감소** |

```bash
docker compose run --rm bench --full --steps 4,7,8,9
```

**Arrow가 빠른 이유 (측정으로 확인)**

| | 순차 변환 | 스레드 2개로 나눠 변환 |
|---|---|---|
| pandas `to_csv` | 14.9초 | 14.7초 (효과 없음) |
| pyarrow `write_csv` | 2.5초 | 1.5초 (1.69배) |

- `write_csv` 자체는 코어 1개만 쓴다 (CPU 시간 ≈ 실제 시간, 옵션에도 스레드 설정이 없다). 6배 빠른 건 C++로 컬럼 단위로 묶어 변환하기 때문이다.
- 대신 변환하는 동안 파이썬 GIL을 풀어 주므로, 스레드 여러 개가 실제로 동시에 돈다. pandas는 GIL을 쥔 채 파이썬 객체를 하나씩 다뤄서 스레드를 늘려도 빨라지지 않는다. 그래서 8단계(병렬 COPY)는 Arrow와 같이 써야 효과가 난다.

코어가 많은 PC에서는 8단계(병렬 COPY)의 효과가 훨씬 커진다. 연결 수를 몇 개로 할지는 아래 `tune.py`로 직접 재서 정한다.

### 병목은 옮겨 다닌다 — PostgreSQL 설정

PostgreSQL 기본 설정(`shared_buffers=128MB`, `max_wal_size=1GB`, 커밋마다 디스크 동기화)은 작은 서버 기준이라 대량 적재에 불리하다. `sql/pg_bulk_load.sql`이 이 설정들을 대량 적재용으로 바꾼다.

| 상황 | 기본 설정 | 대량 적재 설정 |
|---|---|---|
| 4단계 (CSV 변환이 병목) | 약 34초 | 약 34초 — **효과 없음** |
| 9단계 (CSV 변환·연결 병목 해결 후) | 14.3초 | 11.5초 — **약 20% 감소** |

처음에는 DB 설정을 바꿔도 전혀 빨라지지 않았다. 가장 느린 곳이 파이썬의 CSV 변환이었기 때문이다. 그 병목을 없애고 나서야 DB의 디스크 쓰기가 다음 병목이 되어 설정의 효과가 나타났다. **가장 느린 곳을 고치지 않으면 다른 곳을 아무리 고쳐도 소용없다**는 것을 숫자로 보여 주는 사례다.

```bash
# 켜기 (Windows cmd)
docker compose exec -T db psql -U postgres -d nyctaxi < sql/pg_bulk_load.sql
docker compose restart db
# 끄기
docker compose exec -T db psql -U postgres -d nyctaxi < sql/pg_reset.sql
docker compose restart db
```

`synchronous_commit=off`는 PC가 갑자기 꺼지면 마지막 몇 초 분량의 커밋을 잃을 수 있다. 다시 적재하면 되는 실험 데이터라 속도를 택했다.

### tune.py — 이 PC에 맞는 값 찾기

연결 수나 코어 수는 많을수록 좋은 게 아니다. DB가 받아들이는 속도, 디스크, 메모리 중 하나가 먼저 한계에 닿기 때문이다. `tune.py`는 후보 값을 하나씩 실제로 돌려 보고 가장 빠른 값을 고른다. 5% 이내로 비슷하면 자원을 덜 쓰는 값을 고른다.

```bash
docker compose run --rm --entrypoint python bench tune.py                 # 1월 전체
docker compose run --rm --entrypoint python bench tune.py --months 1-3    # 데이터를 키워서
```

| 탐색 대상 | 후보 (기본) | 설정 이름 |
|---|---|---|
| pandas 병렬 COPY 연결 수 | 1, 2, 4, 8, 16 | `COPY_WORKERS` |
| Spark 동시 코어 수 | 2, 4, 8, 16, 전체 코어 | `SPARK_LOCAL_CORES` |

결과 마지막에 나오는 값을 `.env`에 넣으면 `pipeline.py`, `spark_pipeline.py`, Airflow DAG가 그 값을 쓴다. 벤치마크 0~4단계는 비교를 위해 이 값과 상관없이 원래 조건으로 고정된다.

---

## 요약

| 병목 | 원인 | 적용한 해결 | 효과 |
|---|---|---|---|
| 반복 다운로드 | 매 실행마다 50MB 수신 | 로컬 캐싱 + 원자적 저장 | 재실행 시 0.2초 |
| 읽기 메모리 | 안 쓰는 11개 컬럼까지 로드 | parquet 컬럼 프루닝 | 최대 메모리 993 → 520MB |
| 적재 속도 | 파이썬에서 57만 파라미터 SQL 조립 | PostgreSQL COPY | 10분+ → 28.5초 |
| 적재 메모리 | CSV 버퍼 전체를 메모리에 | 청크 COPY | 1,988 → 1,233MB |
| 대시보드 조회 | 일반 뷰가 매번 전체 집계 | 집계 테이블 + `*_fast` 뷰 (또는 Materialized View) | 수백만 행 → 수백 행 조회 |
| 스트리밍 처리량 | 행 단위 JSON 직렬화 | (과제) 묶음 크기·압축·파티션 튜닝 | — |
| 데이터 규모 | 여러 달을 한 번에 메모리에 올림 | Spark 파티션 단위 처리 | 6개월: pandas OOM → Spark 151.5초 (2GB 안에서) |
| 적재량 | 원본 수백만 행을 매번 적재 | Spark 사전 집계 (합계·건수만 저장) | 6개월: 151.5초 → 50.9초 (66% 감소) |
| CSV 변환 | pandas.to_csv가 코어 1개로 한 줄씩 변환 | pyarrow CSV (C++, 컬럼 단위 변환, 실행 중 GIL 해제) | 한 달: 31.3초 → 20.9초 |
| DB 연결 1개 | PostgreSQL은 연결 하나를 코어 하나로 처리 | 병렬 COPY + 인덱스 나중에 생성 | 한 달: 20.9초 → 12.0초 |
| DB 디스크 쓰기 | 작은 서버 기준 기본 설정 | 대량 적재용 설정 (`pg_bulk_load.sql`) | 9단계 기준 약 20% 추가 감소 |
