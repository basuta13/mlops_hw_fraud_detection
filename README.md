# Fraud detection в реальном времени

Домашка по MLOps. Сервис получает транзакции из Kafka по одной, считает для каждой скор модели и отправляет результат обратно в Kafka. Результаты дополнительно сохраняются в Postgres и их можно посмотреть в интерфейсе.

За основу взят код с семинара 4, данные из соревнования [teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025).

## Что внутри

- `interface` — Streamlit. Загружаешь CSV в формате `test.csv`, транзакции отправляются в топик `transactions`. На второй вкладке можно посмотреть результаты из базы.
- `fraud_detector` — сервис скоринга: читает `transactions`, делает препроцессинг (`src/preprocessing.py`), скорит моделью (`src/scorer.py`) и пишет результат в топик `scores`.
- `results_writer` — читает топик `scores` и складывает результаты в Postgres.
- `postgres` — база, таблица `transaction_scores` создаётся через `postgres/init.sql`.
- Kafka, Zookeeper и Kafka UI.

## Модель

CatBoost, обучала в `notebooks/train_model.ipynb`. Признаки: категории (продавец, категория покупки, пол, город, штат, профессия), сумма, население города, координаты и расстояние до продавца, час и день недели. Препроцессинг не использует train, поэтому каждую транзакцию можно обработать отдельно.

На валидации ROC-AUC 0.996, F1 0.854. Порог для `fraud_flag` = 0.33, подобран по F1.

## Запуск

Нужен Docker и Docker Compose.

```bash
git clone https://github.com/basuta13/mlops_hw_fraud_detection.git
cd mlops_hw_fraud_detection
docker compose up --build
```

Первый запуск занимает несколько минут.

- Интерфейс: http://localhost:8501
- Kafka UI: http://localhost:8080

## Как проверить

1. Скачать `test.csv` с Kaggle и для проверки взять кусок, например:
   ```bash
   head -n 2001 test.csv > test_2000.csv
   ```
2. В интерфейсе загрузить файл и нажать «Отправить».
3. В Kafka UI в топике `scores` появятся сообщения вида
   `{"transaction_id": "...", "score": 0.0012, "fraud_flag": 0}`.
4. В интерфейсе перейти в раздел «Результаты скоринга» и нажать «Посмотреть результаты». Появятся 10 последних транзакций с `fraud_flag = 1` и гистограмма скоров последних 100 транзакций.

Логи: `docker compose logs fraud_detector` или `docker compose logs results_writer`.

Остановить: `docker compose down`.

## Что поменяла относительно семинара

- Своя модель и препроцессинг.
- Топик с результатами называется `scores`, как в задании.
- Обновила образы Kafka и Zookeeper до 7.6.1: на 7.3.0 Zookeeper падал на новом Docker Desktop.
- Добавила Postgres, `results_writer` и вкладку с результатами.
