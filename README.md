# Real-Time Fraud Detection System

Сервис для обнаружения мошеннических транзакций в реальном времени: транзакции по одной приходят
из Kafka, сервис скорит каждую моделью CatBoost и отправляет результат обратно в Kafka.

Домашнее задание по MLOps (МТС ШАД 2025). Основа — код семинара 4, данные — соревнование
[teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025).

## Архитектура

```
Streamlit UI ──► Kafka: transactions ──► fraud_detector ──► Kafka: scores ──► results_writer ──► Postgres
 (CSV → JSON)                          (препроцессинг →                                        │
      ▲                                 модель → порог)                                        │
      └──────── раздел «Результаты скоринга»: последние фроды и гистограмма скоров ◄───────────┘
```

1. **`interface`** (Streamlit, порт 8501) — имитирует поток транзакций: читает CSV формата `test.csv`,
   присваивает каждой транзакции уникальный `transaction_id` и отправляет их по одной в топик `transactions`.
2. **`fraud_detector`** — ML-сервис. Этапы разделены на отдельные скрипты:
   - `app/app.py` — чтение сообщений из топика `transactions` и запись результата в топик `scores`;
   - `src/preprocessing.py` — препроцессинг одной транзакции;
   - `src/scorer.py` — загрузка модели и скоринг.
3. **`results_writer`** — отдельный сервис: читает топик `scores` (`transaction_id`, `score`, `fraud_flag`)
   и складывает каждое сообщение в таблицу `transaction_scores` в Postgres.
4. **`postgres`** — база данных с витриной `transaction_scores`. Таблица создаётся при старте
   скриптом [`postgres/init.sql`](postgres/init.sql).
5. **Kafka-инфраструктура** — Zookeeper, брокер Kafka, `kafka-setup` (создаёт топики при старте)
   и Kafka UI (порт 8080) для просмотра сообщений.

В интерфейсе два раздела (переключатель в боковой панели слева):
- **«Отправка транзакций»** — загрузка CSV и отправка в Kafka;
- **«Результаты скоринга»** — по кнопке **«Посмотреть результаты»** показывает
  10 последних транзакций с `fraud_flag = 1` и гистограмму скоров последних 100 транзакций
  (или всех, если в базе их меньше 100).

## Модель

- **CatBoostClassifier**, только inference на CPU. Модель обучена заранее в ноутбуке
  [`notebooks/train_model.ipynb`](notebooks/train_model.ipynb) и лежит в `fraud_detector/models/`:
  - `model.cbm` — веса модели;
  - `model_config.json` — порог для `fraud_flag` и список признаков.
- **Признаки** (`fraud_detector/src/preprocessing.py`):
  - категориальные `merch`, `cat_id`, `gender`, `one_city`, `us_state`, `jobs` — CatBoost кодирует их сам;
  - логарифм суммы `amount` и населения города `population_city`;
  - координаты клиента и продавца и расстояние между ними (формула гаверсинусов);
  - час и день недели транзакции.
- Препроцессинг не зависит от тренировочных данных, поэтому каждая транзакция обрабатывается
  отдельно, сразу как пришла из Kafka. Ноутбук обучения импортирует тот же `preprocessing.py`,
  поэтому признаки при обучении и в сервисе совпадают.
- **Качество на валидации (20% train):** ROC-AUC = 0.9958, F1 = 0.854.
- **Порог** `fraud_flag` = 0.3266, подобран по максимуму F1 на валидации.

## Быстрый старт

### Требования

- Docker 20.10+ и Docker Compose 2.0+ (проверено на Docker Desktop 4.38, macOS, Apple Silicon)
- Свободные порты 8080, 8501, 9095, 2181

### Запуск

```bash
git clone <ссылка на этот репозиторий>
cd <папка репозитория>
docker compose up --build
```

Первый запуск занимает несколько минут: Docker скачивает образы и собирает сервисы.
Сервис готов, когда в логах `fraud_detector` появилась строка:

```
Listening to topic "transactions", writing results to "scores"
```

После запуска:

- **Streamlit UI:** http://localhost:8501
- **Kafka UI:** http://localhost:8080

Остановка: `Ctrl+C`, затем `docker compose down`.

## Проверка работы

1. Скачайте `test.csv` со [страницы соревнования](https://www.kaggle.com/competitions/teta-ml-1-2025/data).
   Для быстрой проверки удобно взять первые 100 транзакций:
   ```bash
   head -n 101 test.csv > test_small.csv
   ```
2. Откройте http://localhost:8501, загрузите `test_small.csv` и нажмите **«Отправить test_small.csv»**.
   Статус файла сменится на «Отправлен».
3. Откройте http://localhost:8080 → **Topics**:
   - в топике `transactions` (вкладка **Messages**) — отправленные транзакции;
   - в топике `scores` — результаты скоринга, по одному сообщению на транзакцию:
     ```json
     {"transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a", "score": 0.0012, "fraud_flag": 0}
     ```
4. Откройте в интерфейсе раздел **«📊 Результаты скоринга»** (боковая панель слева) и нажмите
   **«Посмотреть результаты»**. Появятся таблица последних транзакций с `fraud_flag = 1` и гистограмма скоров.
   Фрод встречается редко, поэтому в 100 транзакциях его может не оказаться. Чтобы увидеть
   строки в таблице, отправьте выборку побольше: `head -n 2001 test.csv > test_2000.csv`.
5. Содержимое витрины можно посмотреть и напрямую в базе:
   ```bash
   docker compose exec postgres psql -U fraud -d fraud_db -c "SELECT * FROM transaction_scores ORDER BY created_at DESC LIMIT 10;"
   ```
6. Логи сервисов:
   ```bash
   docker compose logs fraud_detector   # Transaction <id> scored: 0.0012 (fraud_flag=0)
   docker compose logs results_writer   # Saved transaction <id> (fraud_flag=0)
   ```
   Те же логи пишутся в файл `/app/logs/service.log` внутри контейнера.

## Переобучение модели (необязательно)

Для запуска сервиса это не нужно: обученная модель уже лежит в репозитории.

1. Положите `train.csv` из соревнования в `fraud_detector/train_data/`
   (папка не хранится в git — файл больше 100 МБ).
2. Создайте окружение с теми же версиями библиотек, что и в контейнере:
   ```bash
   conda create -n mlops python=3.12 -y
   conda activate mlops
   pip install catboost==1.2.8 pandas==2.2.3 numpy==2.2.6 scikit-learn==1.6.1 jupyter
   ```
3. Запустите все ячейки `notebooks/train_model.ipynb`. Ноутбук перезапишет
   `fraud_detector/models/model.cbm` и `model_config.json`.
4. Пересоберите сервис: `docker compose up --build`.

## Структура проекта

```
.
├── fraud_detector/
│   ├── app/app.py              # Kafka consumer/producer: transactions → scores
│   ├── src/preprocessing.py    # Препроцессинг транзакций
│   ├── src/scorer.py           # Загрузка модели и скоринг
│   ├── models/
│   │   ├── model.cbm           # Обученная модель CatBoost
│   │   └── model_config.json   # Порог и список признаков
│   ├── requirements.txt
│   └── Dockerfile
├── interface/
│   ├── app.py                  # Streamlit UI: отправка транзакций и результаты
│   ├── requirements.txt
│   └── Dockerfile
├── results_writer/
│   ├── app.py                  # Kafka consumer: scores → Postgres
│   ├── requirements.txt
│   └── Dockerfile
├── postgres/
│   └── init.sql                # Создание витрины transaction_scores
├── notebooks/
│   └── train_model.ipynb       # Обучение модели
├── docker-compose.yaml
└── README.md
```

## Настройки Kafka

| Топик          | Назначение                                        | Партиции |
|----------------|---------------------------------------------------|----------|
| `transactions` | входные транзакции из UI                          | 3        |
| `scores`       | результаты скоринга: `transaction_id`, `score`, `fraud_flag` | 3        |

Репликация: 1 (для разработки).

## Витрина в Postgres

База `fraud_db`, пользователь `fraud`, пароль `fraud` (только для локальной разработки).

| Колонка          | Тип              | Описание                                   |
|------------------|------------------|--------------------------------------------|
| `id`             | SERIAL           | первичный ключ                             |
| `transaction_id` | VARCHAR(64)      | ID транзакции, уникальный                  |
| `score`          | DOUBLE PRECISION | скор модели                                |
| `fraud_flag`     | SMALLINT         | 1 — фрод, 0 — нет                          |
| `created_at`     | TIMESTAMP        | время записи, по нему выбираются последние |

Повторно пришедшее сообщение с тем же `transaction_id` не дублируется (`ON CONFLICT DO NOTHING`).
Данные хранятся внутри контейнера и очищаются после `docker compose down`.

## Изменения относительно кода семинара

- Своя модель и препроцессинг: препроцессинг не требует `train.csv` при запуске сервиса.
- Топик результатов переименован из `scoring` в `scores`, как в задании; сообщение — один JSON-объект.
- Образы Confluent обновлены с 7.3.0 до 7.6.1: Zookeeper 7.3.0 падал с
  `NullPointerException ... CgroupV2Subsystem` на актуальных версиях Docker Desktop.
- `fraud_detector` и `interface` стартуют только после того, как `kafka-setup` создал топики.
- Добавлены Postgres с витриной, сервис `results_writer` и раздел «Результаты скоринга» в интерфейсе.
