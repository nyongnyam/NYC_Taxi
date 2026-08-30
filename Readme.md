뉴욕 택시 운행 기록을 사용한 택시 운전 기사 수익 최적화 대시보드
1 프로젝트 개요


본 프로젝트는 오픈 데이터 셋인 월 별 뉴욕 택시 운행 데이터(NYC Yellow Taxi)를 대상으로 ETL 파이프라인을 설계하고, 구축하는 것, 더 나아가 데이터의 수집, 정재, 적재를 자동화하고 적재한 데이터를 활용해 택시 운전 기사의 수익을 최적화한 대시보드를 구성하는 것이다.

1.1 프로젝트 목표

NYC TLC 공개 데이터를 자동으로 수집하고 정재해 로컬 PostgreSQL에 적재하는 파이프라인 구축
Airflow를 활용한 월별 자동화 스케줄 설정
Tableau를 통한 대시보드 시각화 
모니터링 및 데이터 품질 검사 체계 구성

1.2 사용 데이터

NYC TLC (Taxi & Limousine Commission) Yellow Taxi Trip Records
출처: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
형식: Parquet 파일 (월별 제공)
규모: 월 약 270만 행 (2024년 1월 기준)
주요 컬럼: 승하차 시간, 요금, 팁, 운행 거리, 승객 수 등







2. 시스템 아키텍처




오케스트레이션
Apache Airflow - 월 별 자동 실행 스케줄 관리
데이터 수집
Python Urllib 라이브러리를 통해 NYC TLC에서 parquet 파일 형식으로 다운로드
데이터 정제
Python Pandas library를 통해 이상값 제거, 파생 칼럼 생성
데이터 적재
Python의 SQLAlchemy 라이브러리를 활용해 정제 결과를 로컬의 PostgreSQL에 적재
시각화
Tableau Desktop을 이용해 BI 대시보드 구성
버전 관리
Git을 통한 코드 및 Tableau 워크북 관리
	
3. ETL 파이프라인 상세



3.1 Extract(추출)
NYC TLC 공식 사이트에서 월별 parquet 파일을 다운로드한다. 이미 로컬에 존재하는 파일은 건너뛰며, 사이트에 아직 공개되지 않은 파일은 HTTP HEAD 요청으로 사전 확인 후 에러 없이 건너뜀.
다운로드 경로: data/raw/yellow_tripdata_YYYY-MM.parquet
중복 다운로드 방지: 파일 존재 여부 사전 확인
미공개 파일 처리: HTTP 403 에러 시 경고 로그 후 건너뜀 (NYC TLC 데이터가 2~3개월의 공개 딜레이가 있기 때문)
3.2 Transform(정제)
Pandas 라이브러리를 활용해 원본 데이터의 이상값을 제거하고 분석에 필요한 파생 칼럼을 생성함.

이상값 제거 기준
정제 항목
조건
비고
운행 거리
df[“trip_distance”] > 0
운행 거리 0 이하 제거
기본 요금
df[“fare_amount”] > 0
음수 또는 0 요금 제거
전체 요금
df[“total_amount”] > 0
전체 금액 오류 제거
승객 수
df[“passenger_count”].between(1, 6)
택시 법적 최대 탑승 인원 기준
운행 시간
df[“trip_duration_min”].between(1, 180)
1분 미만은 오류, 3시간 초과는 비정상 운행으로 간주
날짜 필터
df[“tpep_pickup_datetime”].dt.year >= 2024
다른 달 데이터 혼입 방지



파생 칼럼 생성

파생 칼럼
계산 방식
활용 목적
trip_duration_min
(tpep_dropoff_datetime - tpep_pickup_datetime).seconds / 60
운행 시간 분석
pickup_hour
tpep_pickup_datetime.dt.hour
시간대별 패턴 분석
pickup_weekday
tpep_pickup_datetime.dt.day_name()
요일별 패턴 분석
tip_rate
tip_amount/fare_amount
팁 비율 분석


3.3 데이터 적재
정제된 DataFrame을 SQLAlchemy의 to_sql() 메서드를 통해 로컬 PostgreSQL에 적재함.

append 방식을 사용해 기존 데이터를 유지하며 새 데이터 추가
chunksize: 50000
연결 방식: postgresql://username@localhost:5432/nyctaxi

4. Airflow 자동화

매월 1일 오전 6시에 DAG(Directed Acyclic Graph)를 자동으로 실행해 데이터 수집부터 품질 검사까지의 전체 흐름을 순서대로 관리함
4.1 DAG 구성
extract
	전달 parquet 파일 다운로드 후 /tmp/에 임시 저장
transform_and_load
	정제 및 PostgreSQL에 적재, pipeline_runs에 이력 기록
quality_check
	행 수 확인, 이상 요금 감지

4.2 스케쥴
스케쥴: 0 6 1 * * (매월 1일 오전 6시)
전달 데이터 자동 수집: datetime.now() - relativedelta(months = 1)
실행 환경: airflow standalone - 웹 서버 + 스케줄러를 단일 명령으로 통합 실행

5. 데이터베이스 구조

5.2 주요 테이블
clean_taxi_trips 테이블 위에 3개의 분석 뷰를 생성함.

테이블/뷰
유형
설명
clean_taxi_trips
테이블
정제된 택시 운행 데이터(핵심 테이블)
pipeline_runs
테이블
파이프라인 실행 이력(성공/실패, 소요시간)
v_hourly_stats
뷰
시간대별 운행량, 평균 요금, 평균 운행 시간, 팁 비율, 총 배출
pickup_hour (0 ~ 23시)
v_weekday_stats
뷰
요일별 운행량, 평균 요금, 팁 비율
pickup_weekday (요일)
v_monthly_trend
뷰
월별 운행량, 평균 요금, 총 매출
월 (DATE_TRUNC)
v_driver_revenue
뷰
운행 시간 대비 수익을 시간대와 요일 조합으로 집계
v_distance_revenue
뷰
운행 거리를 5개 구간으로 나누어 구간별 시간당 수익을 비교


5.2 ERD 다이어그램


6. Tableau 시각화

