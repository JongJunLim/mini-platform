import os
from datetime import datetime, timedelta, timezone
import pymysql
from pymongo import MongoClient
import psycopg

# ─────────────────────────────
# 환경변수 헬퍼
# ─────────────────────────────
def env(name, default=None):
    """
    환경변수 로딩
    - 필수 값 누락 시 즉시 실패
    """
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env: {name}")
    return v

# ── MySQL
MYSQL_HOST = env("MYSQL_HOST")
MYSQL_PORT =int(env("MYSQL_PORT","3306"))
MYSQL_DB = env("MYSQL_DB")
MYSQL_USER = env("MYSQL_USER")
MYSQL_PASSWORD = env("MYSQL_PASSWORD")

# ── Mongo
MONGO_URI = env("MONGO_URI")

# ── Postgres(DW)
PG_HOST = env("PG_HOST")
PG_PORT =int(env("PG_PORT","5432"))
PG_DB = env("PG_DB")
PG_USER = env("PG_USER")
PG_PASSWORD = env("PG_PASSWORD")

# ─────────────────────────────
# DB 커넥션 생성
# ─────────────────────────────
def mysql_conn():
    """원천 서비스 DB(MySQL) 연결"""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

def pg_conn():
    """분석용 DB(PostgreSQL) 연결"""
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

# ─────────────────────────────
# DW 초기화 (메타 + RAW 테이블)
# ─────────────────────────────
def init_dw():
    """
    - etl_watermark: 증분 ETL 관리용
    - raw_orders / raw_events: 원천 적재 테이블
    """
with pg_conn()as conn:
    with conn.cursor()as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_watermark(
              source_name TEXT PRIMARY KEY,
              last_success_ts TIMESTAMPTZ NOT NULL
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_orders(
              order_id BIGINT PRIMARY KEY,
              user_id BIGINT,
              order_ts TIMESTAMPTZ,
              status TEXT,
              total_amount INT,
              updated_at TIMESTAMPTZ
            );
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_events(
              event_id TEXT PRIMARY KEY,
              event_ts TIMESTAMPTZ,
              user_id BIGINT,
              event_name TEXT,
              device TEXT,
              page TEXT,
              product_id BIGINT,
              referrer TEXT,
              error_code TEXT
            );
            """)
            conn.commit()

# ─────────────────────────────
# 워터마크 조회/갱신
# ─────────────────────────────
def get_watermark(conn, source_name):
    """
    마지막 성공 시각 조회
    - 최초 실행 시 1970-01-01 반환
    """
    with conn.cursor()as cur:
        cur.execute(
            "SELECT last_success_ts FROM etl_watermark WHERE source_name=%s",
            (source_name,)
        )
        row = cur.fetchone()
        if row:
            return row[0]
        return datetime(1970,1,1, tzinfo=timezone.utc)

def set_watermark(conn, source_name, ts):
    """
    ETL 성공 후 워터마크 업데이트
    """
    with conn.cursor()as cur:
        cur.execute("""
        INSERT INTO etl_watermark(source_name, last_success_ts)
        VALUES (%s,%s)
        ON CONFLICT (source_name)
        DO UPDATE SET last_success_ts=EXCLUDED.last_success_ts
        """, (source_name, ts))
        conn.commit()

# ─────────────────────────────
# UPSERT 로직 (재실행 안전)
# ─────────────────────────────
def upsert_orders(pg, rows):
    """
    주문 데이터 UPSERT
    - order_id 기준
    - 상태 변경(cancelled 등) 반영 가능
    """
    if not rows:
        return 0

    with pg.cursor() as cur:
        cur.executemany("""
        INSERT INTO raw_orders(order_id,user_id,order_ts,status,total_amount,updated_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT(order_id) DO UPDATE SET
          user_id=EXCLUDED.user_id,
          order_ts=EXCLUDED.order_ts,
          status=EXCLUDED.status,
          total_amount=EXCLUDED.total_amount,
          updated_at=EXCLUDED.updated_at
        """, [
            (
                r["order_id"],
                r["user_id"],
                r["order_ts"],
                r["status"],
                r["total_amount"],
                r["updated_at"]
            )for r in rows
        ])
    pg.commit()
    return len(rows)

def upsert_events(pg, rows):
    """
    이벤트 로그 UPSERT
    - Mongo _id → event_id
    """
    if not rows:
        return 0

    with pg.cursor() as cur:
        cur.executemany("""
        INSERT INTO raw_events(event_id,event_ts,user_id,event_name,device,page,product_id,referrer,error_code)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(event_id) DO UPDATE SET
          event_ts=EXCLUDED.event_ts,
          user_id=EXCLUDED.user_id,
          event_name=EXCLUDED.event_name,
          device=EXCLUDED.device,
          page=EXCLUDED.page,
          product_id=EXCLUDED.product_id,
          referrer=EXCLUDED.referrer,
          error_code=EXCLUDED.error_code
        """, rows)
    pg.commit()
returnlen(rows)

# ─────────────────────────────
# MART 생성
# ─────────────────────────────
def build_marts(pg):
    """
    분석용 마트 테이블 생성
    - 일별 주문 KPI
    - 일별 퍼널
    """
    with pg.cursor()as cur:
    # ── 일별 주문 KPI
        cur.execute("TRUNCATE mart_daily_orders;")
        cur.execute("""
        INSERT INTO mart_daily_orders
        SELECT date_trunc('day', order_ts) AS dt,
               COUNT(*) AS orders,
               COUNT(DISTINCT user_id) AS buyers,
               SUM(total_amount) AS revenue
        FROM raw_orders
        GROUP BY 1;
        """)

    # ── 퍼널
        cur.execute("TRUNCATE mart_funnel_daily;")
        cur.execute("""
        INSERT INTO mart_funnel_daily
        SELECT date_trunc('day', event_ts) AS dt,
               COUNT(*) FILTER (WHERE event_name='page_view') AS page_view,
               COUNT(*) FILTER (WHERE event_name='product_view') AS product_view,
               COUNT(*) FILTER (WHERE event_name='add_to_cart') AS add_to_cart,
               COUNT(*) FILTER (WHERE event_name='purchase') AS purchase
        FROM raw_events
        GROUP BY 1;
        """)
    pg.commit()

# ─────────────────────────────
# 메인 ETL 플로우
# ─────────────────────────────
def main():
    init_dw()

    mongo = MongoClient(MONGO_URI)
    events = mongo["app"]["events"]

    with pg_conn()as pg:

        # 1️⃣ MySQL 주문 증분 적재
        w_orders = get_watermark(pg,"mysql_orders")

        with mysql_conn()as my:
            with my.cursor()as cur:
                cur.execute(
                    "SELECT * FROM orders WHERE updated_at > %s ORDER BY updated_at ASC",
                    (w_orders.replace(tzinfo=None),)
                )
                rows = cur.fetchall()

        upsert_orders(pg, rows)
        set_watermark(pg,"mysql_orders", datetime.now(timezone.utc))

# 2️⃣ Mongo 이벤트 증분 적재
        w_events = get_watermark(pg,"mongo_events")

        cursor = events.find(
            {"event_ts": {"$gt": w_events}}
        ).sort("event_ts",1)

        batch = []
        for doc in cursor:
            props = doc.get("props", {})
            batch.append((
                str(doc["_id"]),
                doc.get("event_ts"),
                doc.get("user_id"),
                doc.get("event_name"),
                props.get("device"),
                props.get("page"),
                props.get("product_id"),
                props.get("referrer"),
                props.get("error_code")
            ))

            if len(batch) >=5000:
                upsert_events(pg, batch)
                batch = []

        if batch:
            upsert_events(pg, batch)

        set_watermark(pg,"mongo_events", datetime.now(timezone.utc))

        # 3️⃣ MART 갱신
        build_marts(pg)

        print("✅ ETL completed successfully")

if __name__ =="__main__":
    main()
