import os, random
from datetime import datetime, timedelta, timezone
from faker import Faker
import pymysql
from pymongo import MongoClient

# 가짜 데이터 생성을 위한 Faker
fake = Faker()

# KST 타임존 (실무에서 UTC/KST 혼용 이슈 설명 가능)
KST = timezone(timedelta(hours=9))

# ─────────────────────────────
# 환경변수 로딩 (docker-compose에서 주입)
# ─────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST","mysql")
MYSQL_PORT =int(os.getenv("MYSQL_PORT","3306"))
MYSQL_DB = os.getenv("MYSQL_DB","app")
MYSQL_USER = os.getenv("MYSQL_USER","app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD","apppw")

MONGO_URI = os.getenv("MONGO_URI","mongodb://mongo:27017")

# ─────────────────────────────
# MySQL 커넥션 생성 함수
# ─────────────────────────────
def mysql_conn():
    """
    MySQL 연결 객체 반환
    - DictCursor 사용 → 컬럼명을 키로 접근
    - autocommit=True → seeder에서는 트랜잭션 관리 단순화
    """
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

# ─────────────────────────────
# MySQL 스키마 생성
# ─────────────────────────────
def create_schema():
    """
    서비스 DB 스키마 생성
    - users / products / orders / order_items
    - updated_at 컬럼 포함 → 증분 ETL을 위한 핵심 포인트
    """
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS users (
          user_id BIGINT PRIMARY KEY,
          created_at DATETIME NOT NULL,
          updated_at DATETIME NOT NULL,
          country VARCHAR(2),
          channel VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
          product_id BIGINT PRIMARY KEY,
          category VARCHAR(50),
          price INT,
          created_at DATETIME NOT NULL,
          updated_at DATETIME NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
          order_id BIGINT PRIMARY KEY,
          user_id BIGINT NOT NULL,
          order_ts DATETIME NOT NULL,
          status VARCHAR(20) NOT NULL,
          total_amount INT NOT NULL,
          updated_at DATETIME NOT NULL,
          INDEX idx_orders_updated_at(updated_at),
          INDEX idx_orders_order_ts(order_ts)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
          order_id BIGINT NOT NULL,
          product_id BIGINT NOT NULL,
          qty INT NOT NULL,
          unit_price INT NOT NULL,
          PRIMARY KEY(order_id, product_id)
        );
        """
    ]
    with mysql_conn() as conn:
        with conn.cursor() as cur:
            for q in ddl:
                cur.execute(q)

# ─────────────────────────────
# MySQL 데이터 시딩
# ─────────────────────────────
def seed_mysql(n_users=2000, n_products=300, n_orders=15000):
    now = datetime.now(KST).replace(tzinfo=None)
    countries = ["KR", "DE", "FR", "ES", "UK", "IT"]
    channels = ["web", "app", "dealer", "callcenter"]
    categories = ["SUV", "SEDAN", "EV", "ACCESSORY"]

    with mysql_conn() as conn:
        with conn.cursor() as cur:
            # ── users 생성

    statuses = ["paid", "shipped", "cancelled"]

    conn = mysql_conn()
    try:
        # ── users
        with conn.cursor() as cur:
            for uid in range(1, n_users + 1):
                created = now - timedelta(days=random.randint(1, 180))
                updated = created + timedelta(days=random.randint(0, 30))
                cur.execute(
                    "REPLACE INTO users VALUES (%s,%s,%s,%s,%s)",
                    (uid, created, updated, random.choice(countries), random.choice(channels))
                )
        # ── products
        with conn.cursor() as cur:
            for pid in range(1, n_products + 1):
                created = now - timedelta(days=random.randint(30, 365))
                updated = created + timedelta(days=random.randint(0, 60))
                price = random.randint(10, 300) * 1000
                cur.execute(
                    "REPLACE INTO products VALUES (%s,%s,%s,%s,%s)",
                    (pid, random.choice(categories),
                     price, created, updated)
                )
           # ── orders + order_items 생성
            statuses = ["paid", "shipped", "cancelled"]
            for oid in range(1, n_orders + 1):
                uid = random.randint(1, n_users)
                order_ts = now - timedelta(days=random.randint(0, 60), hours=random.randint(0, 23))

                # 기본 주문 상태
                status = random.choices(statuses, weights=[0.78, 0.17, 0.05])[0]
                updated = order_ts + timedelta(hours=random.randint(0, 72))
                # 일부는 나중에 상태 변경(실무 포인트)
                # 일부 주문은 "나중에 취소됨" → 실무에서 흔한 케이스
                if random.random() < 0.03:
                    status = "cancelled"
                    updated = now - timedelta(days=random.randint(0, 3))
                total = 0
                items = random.randint(1, 4)
                picks = random.sample(range(1, n_products + 1), items)
        # ── orders + order_items
        with conn.cursor() as cur:
            for oid in range(1, n_orders + 1):
                uid = random.randint(1, n_users)
                order_ts = now - timedelta(
                    days=random.randint(0, 60),
                    hours=random.randint(0, 23)
                )

                status = random.choices(
                    statuses, weights=[0.78, 0.17, 0.05]
                )[0]
                updated = order_ts + timedelta(hours=random.randint(0, 72))

                if random.random() < 0.03:
                    status = "cancelled"
                    updated = now - timedelta(days=random.randint(0, 3))

                total = 0
                items = random.randint(1, 4)
                picks = random.sample(range(1, n_products + 1), items)

                for pid in picks:
                    qty = random.randint(1, 3)
                    unit = random.randint(10, 300) * 1000
                    total += qty * unit
                    cur.execute(
                        "REPLACE INTO order_items VALUES (%s,%s,%s,%s)",
                        (oid, pid, qty, unit)
                    )
                cur.execute(
                    "REPLACE INTO orders VALUES (%s,%s,%s,%s,%s,%s)",
                    (oid, uid, order_ts, status, total, updated)
                )
    finally:
        conn.close()

# ─────────────────────────────
# MongoDB 이벤트 로그 생성
# ─────────────────────────────
def seed_mongo(n_events=120000):
    """
    이벤트 로그(JSON) 생성
    - page_view → product_view → add_to_cart → purchase 퍼널 구조
    - 일부 error 이벤트 포함
    """
    client = MongoClient(MONGO_URI)
    db = client["app"]
    col = db["events"]

# ETL 성능을 위한 인덱스
    col.create_index("event_ts")
    col.create_index("user_id")
    col.create_index("event_name")

    now = datetime.now(KST)

    event_names = ["page_view","product_view","add_to_cart","purchase","error"]
    devices = ["ios","android","web"]
    pages = ["/","/models","/detail","/checkout","/support"]

    docs = []

    for _ in range(n_events):
        ts = now - timedelta(days=random.randint(0,30),
                             minutes=random.randint(0,1440))
        uid = random.randint(1,2000)
        event = random.choices(
            event_names,
            weights=[0.55,0.25,0.12,0.05,0.03]
        )[0]

        doc = {
            "event_ts": ts,
            "user_id": uid,
            "event_name": event,
            "props": {
                "device": random.choice(devices),
                "page": random.choice(pages),
                "product_id": random.randint(1,300),
                "referrer": random.choice(
                    ["google","naver","direct","email","ad"]
                )
            }
        }

        # error 이벤트에만 error_code 추가
        if event =="error":
            doc["props"]["error_code"] = random.choice(
                ["PAYMENT_FAIL","TIMEOUT","UNKNOWN"]
            )

        docs.append(doc)

        # 대량 insert를 위한 배치 처리
        if len(docs) >=5000:
            col.insert_many(docs)
            docs = []

    if docs:
        col.insert_many(docs)

# ─────────────────────────────
# 실행 엔트리포인트
# ─────────────────────────────
if __name__ =="__main__":
    create_schema()
    seed_mysql()
    seed_mongo()
print("✅ Seed completed")
