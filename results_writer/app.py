"""Сервис записи результатов скоринга в Postgres.

Читает топик Kafka `scores` (transaction_id, score, fraud_flag)
и складывает каждое сообщение в таблицу transaction_scores.
"""
import os
import json
import time
import logging

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('results_writer')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
SCORES_TOPIC = os.getenv('KAFKA_SCORES_TOPIC', 'scores')

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB', 'fraud_db'),
    'user': os.getenv('POSTGRES_USER', 'fraud'),
    'password': os.getenv('POSTGRES_PASSWORD', 'fraud'),
}

INSERT_SQL = """
    INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""


def connect_db(retries=30, delay=2):
    """Подключается к Postgres, при неудаче повторяет попытки."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            logger.info('Connected to Postgres at %s', DB_CONFIG['host'])
            return conn
        except psycopg2.OperationalError as e:
            logger.warning('Postgres is not ready (attempt %d/%d): %s', attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError('Could not connect to Postgres')


def main():
    conn = connect_db()
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'results-writer',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([SCORES_TOPIC])
    logger.info('Listening to topic "%s", writing to table transaction_scores', SCORES_TOPIC)

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error('Kafka error: %s', msg.error())
            continue
        try:
            data = json.loads(msg.value().decode('utf-8'))
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, (
                    str(data['transaction_id']),
                    float(data['score']),
                    int(data['fraud_flag']),
                ))
            conn.commit()
            logger.info('Saved transaction %s (fraud_flag=%s)',
                        data['transaction_id'], data['fraud_flag'])
        except psycopg2.InterfaceError:
            # Соединение с базой потеряно: переподключаемся
            logger.error('Lost connection to Postgres, reconnecting...')
            conn = connect_db()
        except Exception as e:
            conn.rollback()
            logger.error('Error saving message: %s', e)


if __name__ == '__main__':
    main()
