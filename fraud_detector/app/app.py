import os
import sys
import json
import logging

import pandas as pd
from confluent_kafka import Consumer, Producer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Модули импортируем после настройки логов, чтобы видеть их сообщения
sys.path.append(os.path.abspath('./src'))
from preprocessing import run_preproc
from scorer import make_pred

# Настройки Kafka берутся из переменных окружения (см. docker-compose.yaml)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scores")


class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        logger.info('Listening to topic "%s", writing results to "%s"',
                    TRANSACTIONS_TOPIC, SCORING_TOPIC)

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                # 1. Читаем транзакцию из Kafka
                data = json.loads(msg.value().decode('utf-8'))
                transaction_id = data['transaction_id']
                input_df = pd.DataFrame([data['data']])

                # 2. Препроцессинг (src/preprocessing.py)
                features = run_preproc(input_df)

                # 3. Скоринг моделью (src/scorer.py)
                prediction = make_pred(features, "kafka_stream")

                # 4. Отправляем результат в Kafka
                result = {
                    'transaction_id': transaction_id,
                    'score': float(prediction['score'].iloc[0]),
                    'fraud_flag': int(prediction['fraud_flag'].iloc[0]),
                }
                self.producer.produce(SCORING_TOPIC, value=json.dumps(result))
                self.producer.flush()
                logger.info('Transaction %s scored: %.4f (fraud_flag=%d)',
                            transaction_id, result['score'], result['fraud_flag'])
            except Exception as e:
                logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
