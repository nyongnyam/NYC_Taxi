-- agg_tables.sql — Spark 사전 집계(spark_pipeline.py --mode agg) 결과 테이블과 조회용 뷰
-- spark_pipeline.py가 실행할 때마다 자동으로 실행하므로 따로 실행할 필요는 없다.
--
-- 평균 대신 '합계'와 '건수'를 저장한다. 평균끼리는 다시 평균 낼 수 없지만,
-- 합계와 건수는 여러 달을 더한 뒤 나누면 정확한 전체 평균이 나온다.

CREATE TABLE IF NOT EXISTS agg_hourly_monthly (
    month        DATE    NOT NULL,
    pickup_hour  INTEGER NOT NULL,
    trip_count   BIGINT  NOT NULL,
    fare_sum     DOUBLE PRECISION,
    duration_sum DOUBLE PRECISION,
    tip_rate_sum DOUBLE PRECISION,
    revenue_sum  DOUBLE PRECISION,
    PRIMARY KEY (month, pickup_hour)
);

CREATE TABLE IF NOT EXISTS agg_weekday_monthly (
    month          DATE    NOT NULL,
    pickup_weekday TEXT    NOT NULL,
    trip_count     BIGINT  NOT NULL,
    fare_sum       DOUBLE PRECISION,
    duration_sum   DOUBLE PRECISION,
    tip_rate_sum   DOUBLE PRECISION,
    revenue_sum    DOUBLE PRECISION,
    PRIMARY KEY (month, pickup_weekday)
);

-- 기존 v_hourly_stats / v_weekday_stats와 같은 결과를 집계 테이블에서 바로 계산하는 뷰
CREATE OR REPLACE VIEW v_hourly_stats_fast AS
SELECT
    pickup_hour,
    SUM(trip_count)                                                  AS trip_count,
    ROUND((SUM(fare_sum)     / SUM(trip_count))::NUMERIC, 2)         AS avg_fare,
    ROUND((SUM(duration_sum) / SUM(trip_count))::NUMERIC, 1)         AS avg_duration_min,
    ROUND((SUM(tip_rate_sum) / SUM(trip_count))::NUMERIC, 4)         AS avg_tip_rate,
    ROUND(SUM(revenue_sum)::NUMERIC, 0)                              AS total_revenue
FROM agg_hourly_monthly
GROUP BY pickup_hour
ORDER BY pickup_hour;

CREATE OR REPLACE VIEW v_weekday_stats_fast AS
SELECT
    pickup_weekday,
    SUM(trip_count)                                                  AS trip_count,
    ROUND((SUM(fare_sum)     / SUM(trip_count))::NUMERIC, 2)         AS avg_fare,
    ROUND((SUM(tip_rate_sum) / SUM(trip_count))::NUMERIC, 4)         AS avg_tip_rate
FROM agg_weekday_monthly
GROUP BY pickup_weekday
ORDER BY trip_count DESC;

CREATE OR REPLACE VIEW v_monthly_trend_fast AS
SELECT
    month,
    SUM(trip_count)                                                  AS trip_count,
    ROUND((SUM(fare_sum) / SUM(trip_count))::NUMERIC, 2)             AS avg_fare,
    ROUND(SUM(revenue_sum)::NUMERIC, 0)                              AS total_revenue
FROM agg_hourly_monthly
GROUP BY month
ORDER BY month;
