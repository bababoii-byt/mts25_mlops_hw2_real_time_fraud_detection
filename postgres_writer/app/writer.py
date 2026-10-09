
import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID", "postgres-writer"
)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_password"),
}


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False

            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS transaction_scores (
                        transaction_id TEXT PRIMARY KEY,
                        score DOUBLE PRECISION NOT NULL,
                        fraud_flag INTEGER NOT NULL
                            CHECK (fraud_flag IN (0, 1)),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
            conn.commit()

            logger.info("Connected to PostgreSQL; table is ready")
            return conn

        except psycopg2.Error:
            logger.exception("PostgreSQL is not ready; retrying in 3 sec")
            time.sleep(3)


def main():
    conn = connect_db()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    logger.info("Listening to Kafka topic: %s", KAFKA_TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                result = json.loads(msg.value().decode("utf-8"))

                # Совместимость как с новым объектом, так и со старыми
                # сообщениями в формате массива из одного объекта.
                if isinstance(result, list):
                    if not result:
                        raise ValueError("Received an empty result list")
                    result = result[0]

                transaction_id = str(result["transaction_id"])
                score = float(result["score"])
                fraud_flag = int(result["fraud_flag"])

                if fraud_flag not in (0, 1):
                    raise ValueError("fraud_flag must be 0 or 1")

                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO transaction_scores
                            (transaction_id, score, fraud_flag)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (transaction_id)
                        DO UPDATE SET
                            score = EXCLUDED.score,
                            fraud_flag = EXCLUDED.fraud_flag
                    """, (transaction_id, score, fraud_flag))

                conn.commit()
                consumer.commit(message=msg)
                logger.info(
                    "Saved transaction_id=%s, score=%.6f, fraud_flag=%d",
                    transaction_id, score, fraud_flag
                )

            except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                conn.rollback()
                logger.exception("Invalid scoring message; skipping it")
                consumer.commit(message=msg)

            except psycopg2.Error:
                conn.rollback()
                logger.exception("Database write failed; will retry message")
                time.sleep(2)

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()