# Final Project: Data Pipeline на PySpark (Локальне виконання)

## Огляд

Цей проект реалізує data pipeline для обробки та аналізу продажів побутової електроніки використовуючи **локальний PySpark**. Всі дані зберігаються локально в файловій системі та PostgreSQL.

## Архітектура

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌─────────────┐
│  Raw (CSV)  │────▶│ Bronze (PQ)   │────▶│ Silver (PQ)   │────▶│ Gold (PQ)  │
│   data/     │     │spark_local/  │     │spark_local/  │     │spark_local/│
└─────────────┘     └──────────────┘     └──────────────┘     └─────────────┘
```

## Передумови

### Необхідне програмне забезпечення:

1. **Python 3.8+**
   ```bash
   python3 --version
   ```

2. **Apache Spark 3.3+**
   ```bash
   spark-submit --version
   ```
   Завантажити з: https://spark.apache.org/downloads.html

3. **PostgreSQL 12+** (опціонально, для gold layer)
   ```bash
   psql --version
   ```

4. **Apache Airflow 2.5+**
   ```bash
   airflow version
   ```

## Встановлення

### 1. Встановлення залежностей

```bash
cd final_project
pip install -r requirements.txt
```

### 2. Налаштування PostgreSQL (опціонально)

```bash
# Створити базу даних
createdb data_platform

# Або через psql
psql -U postgres -c "CREATE DATABASE data_platform;"
psql -U postgres -c "CREATE USER airflow_user WITH PASSWORD 'airflow_pass';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE data_platform TO airflow_user;"
```

### 3. Налаштування Airflow

```bash
# Встановіть AIRFLOW_HOME
export AIRFLOW_HOME=$(pwd)

# Ініціалізація Airflow бази даних
airflow db init

# Створення адміністративного користувача
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Налаштування DAGs folder в airflow.cfg
# dags_folder = /absolute/path/to/final_project/airflow/dags
```

### 4. Запуск Airflow

```bash
# Термінал 1: Webserver
airflow webserver --port 8080

# Термінал 2: Scheduler
airflow scheduler
```

## Структура проекту

```
final_project/
├── data/                          # Raw дані
│   ├── customers/                 # Дані клієнтів (CSV)
│   ├── sales/                     # Дані продажів (CSV, партиціоновані по датах)
│   └── user_profiles/             # Профілі користувачів (JSONL)
├── spark_local/                   # PySpark ETL скрипти
│   ├── bronze/                    # Bronze layer (Parquet) - згенерується
│   ├── silver/                    # Silver layer (Parquet) - згенерується
│   ├── gold/                      # Gold layer (Parquet) - згенерується
│   ├── process_sales_etl.py        # ETL для sales
│   ├── process_customers_etl.py    # ETL для customers
│   ├── process_user_profiles_etl.py # ETL для user_profiles
│   ├── enrich_user_profiles_etl.py # Enrichment pipeline
│   └── analytical_query.py         # Аналітичний запит
├── postgresql/                    # PostgreSQL SQL скрипти (опціонально)
│   ├── create_silver_tables.sql
│   ├── enrich_user_profiles.sql
│   └── analytical_query.sql
├── airflow/
│   └── dags/                      # Airflow DAGs
│       ├── process_sales_dag.py
│       ├── process_customers_dag.py
│       ├── process_user_profiles_dag.py
│       └── enrich_user_profiles_dag.py
├── requirements.txt               # Python залежності
└── README.md                      # Цей файл
```

## Опис пайплайнів

### 1. process_sales pipeline

**Маршрут:** `raw → bronze → silver`

- **Raw → Bronze**: Читання CSV файлів зі схемой schema-on-read (всі поля STRING)
- **Bronze → Silver**: 
  - Очищення даних (data cleansing)
  - Перетворення типів даних
  - Перейменування колонок: `CustomerId → client_id`, `PurchaseDate → purchase_date`, `Product → product_name`, `Price → price`
  - Партиціонування по даті (`partition_date`)

**Schedule:** Щодня (`@daily`)

### 2. process_customers pipeline

**Маршрут:** `raw → bronze → silver`

- Дані приходять як інкрементальний дамп (кожен день містить всі попередні дані)
- Не партиціонується (даних небагато)
- **Silver схема:** `client_id, first_name, last_name, email, registration_date, state`

**Schedule:** Щодня (`@daily`)

### 3. process_user_profiles pipeline

**Маршрут:** `raw → silver`

- Формат даних: JSONL
- Дані мають ідеальну якість
- **Schedule:** Ручний запуск (`schedule=None`)

### 4. enrich_user_profiles pipeline

**Маршрут:** `silver → gold`

- Збагачення даних `customers` даними з `user_profiles`
- Заповнення порожніх полів (ім'я, прізвище, штат, вік)
- Додавання полів з `user_profiles`, яких немає в `customers` (наприклад, `phone_number`)
- Створює таблицю `user_profiles_enriched` в gold layer
- **Schedule:** Ручний запуск (`schedule=None`)

## Запуск ETL Jobs

### Вручну через Spark

```bash
# Встановіть базову директорію
export DATA_LAKE_BASE_DIR=$(pwd)

# 1. Process Sales
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    spark_local/process_sales_etl.py

# 2. Process Customers
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    spark_local/process_customers_etl.py

# 3. Process User Profiles (ручний запуск)
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    spark_local/process_user_profiles_etl.py

# 4. Enrich User Profiles (ручний запуск)
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    spark_local/enrich_user_profiles_etl.py
```

### Через Airflow

1. Відкрийте Airflow UI: http://localhost:8080
2. Увійдіть з credentials, створеними під час setup
3. Знайдіть DAGs:
   - `process_sales_local` - щоденний запуск
   - `process_customers_local` - щоденний запуск
   - `process_user_profiles_local` - ручний запуск (Trigger DAG)
   - `enrich_user_profiles_local` - ручний запуск (Trigger DAG)

**Важливо:** `process_user_profiles_local` автоматично запускає `enrich_user_profiles_local` після успішного виконання.

## Аналітичний запит

Після виконання всіх пайплайнів, виконайте аналітичний запит:

**Питання:** В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду вересня?

### Через PySpark:

```bash
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    spark_local/analytical_query.py
```

### Через PostgreSQL (якщо використовуєте):

```bash
psql -U postgres -d data_platform -f postgresql/analytical_query.sql
```

## Послідовність виконання

1. ✅ Запустіть `process_sales_local` (через Airflow або вручну)
2. ✅ Запустіть `process_customers_local` (через Airflow або вручну)
3. ✅ Запустіть `process_user_profiles_local` **вручну** (через Airflow UI → Trigger DAG)
4. ✅ `enrich_user_profiles_local` запуститься автоматично після кроку 3
5. ✅ Виконайте аналітичний запит

## Вимоги до реалізації

- ✅ Оркестрація через Apache Airflow
- ✅ Data Cleansing (очищення даних)
- ✅ Data Wrangling (трансформація даних)
- ✅ Data Enrichment (збагачення даних)
- ✅ Партиціонування даних (для sales)
- ✅ Використання MERGE операції (в enrich_user_profiles)

## Troubleshooting

### Spark не знаходить дані
```bash
# Перевірте шлях до даних
export DATA_LAKE_BASE_DIR=$(pwd)
echo $DATA_LAKE_BASE_DIR
```

### Airflow DAGs не з'являються
```bash
# Перевірте AIRFLOW_HOME
export AIRFLOW_HOME=$(pwd)
echo $AIRFLOW_HOME

# Перевірте, що DAGs в правильній директорії
ls airflow/dags/
```

### Помилки пам'яті Spark
```bash
# Збільште пам'ять для Spark
spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    spark_local/process_sales_etl.py
```

## Додаткова інформація

- Документація Spark: https://spark.apache.org/docs/latest/
- Документація Airflow: https://airflow.apache.org/docs/
- Документація PostgreSQL: https://www.postgresql.org/docs/

