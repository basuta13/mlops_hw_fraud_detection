-- Витрина с результатами скоринга транзакций.
-- Скрипт выполняется автоматически при первом запуске контейнера postgres.

CREATE TABLE IF NOT EXISTS transaction_scores (
    id             SERIAL PRIMARY KEY,
    transaction_id VARCHAR(64)      NOT NULL UNIQUE,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     SMALLINT         NOT NULL,
    created_at     TIMESTAMP        NOT NULL DEFAULT NOW()
);

-- Индексы для запросов «последние записи» из интерфейса
CREATE INDEX IF NOT EXISTS idx_scores_created_at ON transaction_scores (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_scores_fraud_flag ON transaction_scores (fraud_flag);
