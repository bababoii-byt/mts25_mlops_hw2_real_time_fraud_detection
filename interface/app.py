import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2  # NEW: подключение к PostgreSQL

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

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


# NEW: просмотр результатов скоринга из PostgreSQL
st.divider()
st.title("📊 Результаты обнаружения мошенничества")

if st.button("Посмотреть результаты"):
    try:
        # NEW: подключение к базе данных
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "fraud_db"),
            user=os.getenv("POSTGRES_USER", "fraud_user"),
            password=os.getenv("POSTGRES_PASSWORD", "fraud_password"),
            connect_timeout=5,
        )

        try:
            with conn.cursor() as cur:
                # NEW: последние 10 мошеннических транзакций
                cur.execute("""
                    SELECT transaction_id, score, fraud_flag, created_at
                    FROM transaction_scores
                    WHERE fraud_flag = 1
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                fraud_rows = cur.fetchall()
                fraud_columns = [desc[0] for desc in cur.description]

                # NEW: оценки последних 100 транзакций
                cur.execute("""
                    SELECT transaction_id, score, fraud_flag, created_at
                    FROM transaction_scores
                    ORDER BY created_at DESC
                    LIMIT 100
                """)
                score_rows = cur.fetchall()
                score_columns = [desc[0] for desc in cur.description]

            # NEW: показываем найденные мошеннические транзакции
            st.subheader("🚨 Последние мошеннические транзакции")
            if fraud_rows:
                fraud_df = pd.DataFrame(
                    fraud_rows, columns=fraud_columns
                )
                st.dataframe(fraud_df, use_container_width=True)
            else:
                st.info("Мошеннических транзакций пока нет.")

            # NEW: строим гистограмму оценок
            st.subheader("📈 Распределение оценок последних транзакций")
            if score_rows:
                scores_df = pd.DataFrame(
                    score_rows, columns=score_columns
                )
                st.caption(
                    f"Количество транзакций: {len(scores_df)}"
                )
                score_bins = pd.cut(scores_df["score"], bins=20)
                histogram = score_bins.value_counts(sort=False)
                histogram.index = [
                    f"{interval.left:.2f}–{interval.right:.2f}"
                    for interval in histogram.index
                ]
                st.bar_chart(histogram)
            else:
                st.info("В базе данных пока нет результатов скоринга.")

        finally:
            conn.close()

    except Exception as e:
        st.error(f"Ошибка получения результатов: {e}")