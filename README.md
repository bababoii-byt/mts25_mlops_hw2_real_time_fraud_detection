# Real-Time Fraud Detection System

DISCLAIMER

Сервис подготовлен в демонстрационных целях для студентов курса МТС ШАД 2025 в рамках занятий по MLOps. Датасеты предоставлены в рамках соревнования https://www.kaggle.com/competitions/teta-ml-1-2025

Система для обнаружения мошеннических транзакций в реальном времени с использованием ML-модели и Kafka для потоковой обработки данных.

## 🏗️ Архитектура

Компоненты системы:
1. **`interface`** (Streamlit UI):
   
   Создан для удобной симуляции потоковых данных с транзакциями. Реальный продукт использовал бы прямой поток данных из других систем.
    - Имитирует отправку транзакций в Kafka через CSV-файлы.
    - Генерирует уникальные ID для транзакций.
    - Загружает транзакции отдельными сообщениями формата JSON в топик kafka `transactions`.
    

2. **`fraud_detector`** (ML Service):
   - Загружает предобученную модель CatBoost (`my_catboost.cbm`).
   - Выполняет препроцессинг данных:
     - Извлечение временных признаков
     - Гео-расстояния
     - Кодирование категориальных переменных
   - Производит скоринг с порогом 0.98.
   - Выгружает результат скоринга в топик kafka `scoring`

3. **`Kafka Infrastructure`**:
   - Zookeeper + Kafka брокер
   - `kafka-setup`: автоматически создает топики `transactions` и `scoring`
   - Kafka UI: веб-интерфейс для мониторинга сообщений (порт 8080)

4. **`postgres`** и **`postgres_writer`** (хранение результатов):

   * PostgreSQL хранит результаты скоринга в таблице `transaction_scores`.
   * `postgres_writer` читает сообщения из топика `scoring` и сохраняет `transaction_id`, `score`, `fraud_flag` и время записи.
   * Идентификатор транзакции используется как первичный ключ.
   * Данные сохраняются в Docker volume `postgres_data`.

5. **`Просмотр результатов в Streamlit`**:

   * Кнопка «Посмотреть результаты» загружает данные из PostgreSQL.
   * Отображаются последние 10 транзакций с `fraud_flag = 1`, если они есть.
   * Гистограмма показывает распределение оценок `score` для последних 100 транзакций или всех доступных, если их меньше.


## 🚀 Быстрый старт

### Требования
- Docker 20.10+
- Docker Compose 2.0+

### Запуск

Из корневой папки проекта выполните:

```bash
docker compose up --build
```

Docker Compose соберёт необходимые образы и запустит сервисы.

После запуска:

* **Streamlit UI**: http://localhost:8501
* **Kafka UI**: http://localhost:8080

Для просмотра логов:

```bash
docker compose logs -f fraud_detector
docker compose logs -f postgres_writer
```

Для остановки сервисов:

```bash
docker compose down
```

Данные PostgreSQL хранятся в Docker volume `postgres_data` и сохраняются после остановки контейнеров.


## 🛠️ Использование

### 1. Загрузка данных:

 - Загрузите CSV через интерфейс Streamlit. Для тестирования работы проекта используется файл формата `test.csv` из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025
 - Пример структуры данных:
    ```csv
    transaction_time,amount,lat,lon,merchant_lat,merchant_lon,gender,...
    2023-01-01 12:30:00,150.50,40.7128,-74.0060,40.7580,-73.9855,M,...
    ```
 - Для первых тестов рекомендуется загружать небольшой семпл данных (до 100 транзакций) за раз, чтобы исполнение кода не заняло много времени.

### 2. Мониторинг

* **Kafka UI**: просмотр сообщений в топиках `transactions` и `scoring`.

* **Логи обработки**:

  ```bash
  docker compose logs -f fraud_detector
  ```

* **Логи сохранения результатов**:

  ```bash
  docker compose logs -f postgres_writer
  ```


### 3. Результаты:

 - Скоринговые оценки пишутся в топик scoring в формате:
    ```json
    {
    "score": 0.995, 
    "fraud_flag": 1, 
    "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"
    }
    ```
## Структура проекта
```
.
├── docker-compose.yaml
├── README.md
├── fraud_detector/
│   ├── app/
│   │   └── app.py
│   ├── src/
│   │   ├── preprocessing.py
│   │   └── scorer.py
│   ├── models/
│   │  
 └── my_catboost.cbm
│   ├── train_data/
│   │   └── train.csv
│   ├── Dockerfile
│   └── requirements.txt
├── interface/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
└── postgres_writer/
    ├── app/
    │   └── writer.py
    ├── Dockerfile
    └── requirements.txt
```


## Настройки Kafka
```yml
Топики:
- transactions (входные данные)
- scoring (результаты скоринга)

Репликация: 1 (для разработки)
Партиции: 3
```

*Примечание:* 

Для полной функциональности убедитесь, что:
1. Модель `my_catboost.cbm` размещена в `fraud_detector/models/`
2. Тренировочные данные находятся в `fraud_detector/train_data/`
3. Порты 8080, 8501 и 9095 свободны на хосте
