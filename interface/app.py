import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid

import altair as alt
import psycopg2

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

# Конфигурация Postgres (витрина с результатами скоринга)
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud"),
}

LAST_FRAUD_SQL = """
    SELECT transaction_id, score, fraud_flag, created_at
    FROM transaction_scores
    WHERE fraud_flag = 1
    ORDER BY created_at DESC, id DESC
    LIMIT 10
"""

LAST_SCORES_SQL = """
    SELECT score
    FROM transaction_scores
    ORDER BY created_at DESC, id DESC
    LIMIT 100
"""


def read_sql(query):
    """Выполняет SELECT в Postgres и возвращает DataFrame"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [c[0] for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        conn.close()

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

def show_upload_page():
    # Инициализация состояния
    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = {}

    # Интерфейс
    st.title("📤 Отправка данных в Kafka")

    # Блок загрузки файлов
    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"]
    )

    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        # Добавляем файл в состояние
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file)
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")

    # Список загруженных файлов
    if st.session_state.uploaded_files:
        st.subheader("🗂 Список загруженных файлов")

        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([4, 2, 2])

            with cols[0]:
                st.markdown(f"**Файл:** `{file_name}`")
                st.markdown(f"**Статус:** `{file_data['status']}`")

            with cols[2]:
                if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Отправка..."):
                            success = send_to_kafka(
                                file_data["df"],
                                KAFKA_CONFIG["topic"],
                                KAFKA_CONFIG["bootstrap_servers"]
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                                st.rerun()
                    else:
                        st.error("Файл не содержит данных")


def show_results_page():
    st.title("📊 Результаты скоринга")
    st.write("Данные берутся из таблицы `transaction_scores` в Postgres.")

    if not st.button("Посмотреть результаты"):
        return

    try:
        fraud_df = read_sql(LAST_FRAUD_SQL)
        scores_df = read_sql(LAST_SCORES_SQL)
    except Exception as e:
        st.error(f"Не удалось получить данные из Postgres: {e}")
        return

    st.subheader("🚨 10 последних транзакций с fraud_flag = 1")
    if fraud_df.empty:
        st.info("Транзакций с fraud_flag = 1 в базе пока нет.")
    else:
        st.dataframe(fraud_df, use_container_width=True, hide_index=True)

    st.subheader("📈 Распределение скоров последних транзакций")
    if scores_df.empty:
        st.info("В базе пока нет транзакций. Отправьте файл на странице «Отправка транзакций».")
        return

    st.caption(f"Транзакций в выборке: {len(scores_df)} (не больше 100 последних)")
    chart = alt.Chart(scores_df).mark_bar().encode(
        x=alt.X("score:Q", bin=alt.Bin(extent=[0, 1], step=0.05), title="Скор модели"),
        y=alt.Y("count():Q", title="Количество транзакций"),
    )
    st.altair_chart(chart, use_container_width=True)


# Навигация между разделами
page = st.sidebar.radio(
    "Раздел",
    ["📤 Отправка транзакций", "📊 Результаты скоринга"],
)

if page == "📤 Отправка транзакций":
    show_upload_page()
else:
    show_results_page()
