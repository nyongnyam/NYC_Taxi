-- setup.sql — 테이블 및 분석 뷰 생성
-- 실행: psql -h localhost -U postgres -d nyctaxi -f sql/setup.sql
-- (docker compose 사용 시 DB 컨테이너가 처음 뜰 때 자동 실행됨)

-- 정제 결과 테이블
CREATE TABLE IF NOT EXISTS clean_taxi_trips (
    vendor_id             INTEGER,
    tpep_pickup_datetime  TIMESTAMP NOT NULL,
    tpep_dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count       FLOAT,
    trip_distance         FLOAT,
    fare_amount           FLOAT,
    tip_amount            FLOAT,
    total_amount          FLOAT,
    trip_duration_min     FLOAT,
    pickup_hour           INTEGER,
    pickup_weekday        TEXT,
    tip_rate              FLOAT,
    loaded_at             TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pickup_dt
    ON clean_taxi_trips (tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_pickup_hour
    ON clean_taxi_trips (pickup_hour);


-- Tableau에서 바로 연결할 분석 뷰

-- 시간대별 통계
CREATE OR REPLACE VIEW v_hourly_stats AS
SELECT
    pickup_hour,
    COUNT(*)                                  AS trip_count,
    ROUND(AVG(fare_amount)::NUMERIC, 2)       AS avg_fare,
    ROUND(AVG(trip_duration_min)::NUMERIC, 1) AS avg_duration_min,
    ROUND(AVG(tip_rate)::NUMERIC, 4)          AS avg_tip_rate,
    ROUND(SUM(total_amount)::NUMERIC, 0)      AS total_revenue
FROM clean_taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- 요일별 통계
CREATE OR REPLACE VIEW v_weekday_stats AS
SELECT
    pickup_weekday,
    COUNT(*)                                  AS trip_count,
    ROUND(AVG(fare_amount)::NUMERIC, 2)       AS avg_fare,
    ROUND(AVG(tip_rate)::NUMERIC, 4)          AS avg_tip_rate
FROM clean_taxi_trips
GROUP BY pickup_weekday
ORDER BY trip_count DESC;

-- 월별 트렌드
CREATE OR REPLACE VIEW v_monthly_trend AS
SELECT
    DATE_TRUNC('month', tpep_pickup_datetime) AS month,
    COUNT(*)                                  AS trip_count,
    ROUND(AVG(fare_amount)::NUMERIC, 2)       AS avg_fare,
    ROUND(SUM(total_amount)::NUMERIC, 0)      AS total_revenue
FROM clean_taxi_trips
GROUP BY 1
ORDER BY 1;

-- 파이프라인 실행 이력
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id           SERIAL PRIMARY KEY,
    year         INTEGER,
    month        INTEGER,
    rows_loaded  INTEGER,
    status       TEXT,
    error_msg    TEXT,
    duration_sec FLOAT,
    created_at   TIMESTAMP DEFAULT NOW()
);
