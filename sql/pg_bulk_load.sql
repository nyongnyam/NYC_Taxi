-- pg_bulk_load.sql — 대량 적재용 PostgreSQL 설정 (실험용, 로컬 전용)
--
-- 적용:  docker compose exec -T db psql -U postgres -d nyctaxi -f - < sql/pg_bulk_load.sql
--        docker compose restart db          (shared_buffers, wal_level 등은 재시작해야 반영)
-- 되돌리기: sql/pg_reset.sql 을 같은 방법으로 실행 후 재시작
--
-- 주의: synchronous_commit=off 는 PC가 갑자기 꺼지면 마지막 몇 초 분량의 커밋이 사라질 수 있다.
--       다시 적재하면 되는 실험 데이터라서 속도를 우선한 설정이다. 운영 DB에는 그대로 쓰지 않는다.

ALTER SYSTEM SET shared_buffers       = '2GB';    -- 기본 128MB: 자주 쓰는 데이터를 메모리에 더 많이 둔다
ALTER SYSTEM SET max_wal_size         = '8GB';    -- 기본 1GB: 체크포인트(디스크 정리) 빈도를 줄인다
ALTER SYSTEM SET checkpoint_timeout   = '30min';
ALTER SYSTEM SET wal_buffers          = '64MB';
ALTER SYSTEM SET synchronous_commit   = 'off';    -- 커밋할 때 디스크 기록을 기다리지 않는다
ALTER SYSTEM SET wal_level            = 'minimal';-- 복제용 로그를 최소화한다
ALTER SYSTEM SET max_wal_senders      = 0;        -- wal_level=minimal의 필수 조건
ALTER SYSTEM SET maintenance_work_mem = '1GB';    -- 인덱스 다시 만들 때 쓰는 메모리
ALTER SYSTEM SET max_parallel_maintenance_workers = 4;  -- 인덱스를 여러 코어로 만든다
ALTER SYSTEM SET max_parallel_workers_per_gather  = 4;  -- 뷰 집계 조회를 여러 코어로 한다
