# Структура Final Project

## 📁 Структура директорій

```
final_project/
├── airflow/
│   └── dags/                      # Airflow DAG файли
│       ├── process_sales_dag.py           # DAG для обробки sales
│       ├── process_customers_dag.py       # DAG для обробки customers
│       ├── process_user_profiles_dag.py   # DAG для обробки user_profiles (ручний)
│       └── enrich_user_profiles_dag.py   # DAG для збагачення даних (ручний)
│
├── spark_local/                   # PySpark ETL скрипти
│   ├── process_sales_etl.py              # ETL: sales (raw → bronze → silver)
│   ├── process_customers_etl.py          # ETL: customers (raw → bronze → silver)
│   ├── process_user_profiles_etl.py      # ETL: user_profiles (raw → silver)
│   ├── enrich_user_profiles_etl.py       # Enrichment: silver → gold
│   ├── analytical_query.py               # Аналітичний запит
│   ├── load_silver_to_postgres.py        # Завантаження в PostgreSQL (опціонально)
│   ├── bronze/                            # Bronze layer (Parquet) - згенерується
│   ├── silver/                            # Silver layer (Parquet) - згенерується
│   └── gold/                              # Gold layer (Parquet) - згенерується
│
├── data/                          # Raw дані
│   ├── customers/                 # Дані клієнтів (CSV, інкрементальні дампи)
│   │   ├── 2022-08-1/
│   │   ├── 2022-08-2/
│   │   └── ...
│   ├── sales/                     # Дані продажів (CSV, партиціоновані по датах)
│   │   ├── 2022-09-1/
│   │   ├── 2022-09-10/
│   │   └── ...
│   └── user_profiles/             # Профілі користувачів (JSONL)
│       └── user_profiles.json
│
├── postgresql/                    # PostgreSQL SQL скрипти (опціонально)
│   ├── create_silver_tables.sql
│   ├── enrich_user_profiles.sql
│   └── analytical_query.sql
│
├── .gitignore                     # Git ignore правила
├── README.md                      # Основна документація
├── QUICK_START.md                 # Швидкий старт
├── requirements.txt               # Python залежності
└── setup.sh                       # Скрипт налаштування
```

## 🔄 Пайплайни

### 1. process_sales
- **Вхід:** `data/sales/*/*.csv`
- **Вихід:** `spark_local/silver/sales/` (партиціоновано по `partition_date`)
- **Schedule:** Щодня
- **Особливості:**
  - Schema-on-read (всі поля STRING в bronze)
  - Data cleansing в silver
  - Партиціонування по даті

### 2. process_customers
- **Вхід:** `data/customers/*/*.csv`
- **Вихід:** `spark_local/silver/customers/`
- **Schedule:** Щодня
- **Особливості:**
  - Інкрементальні дампи (кожен день містить всі попередні)
  - Не партиціонується

### 3. process_user_profiles
- **Вхід:** `data/user_profiles/*.json` (JSONL)
- **Вихід:** `spark_local/silver/user_profiles/`
- **Schedule:** Ручний запуск
- **Особливості:**
  - Високоякісні дані
  - Прямий перехід raw → silver (без bronze)

### 4. enrich_user_profiles
- **Вхід:** 
  - `spark_local/silver/customers/`
  - `spark_local/silver/user_profiles/`
- **Вихід:** `spark_local/gold/user_profiles_enriched/`
- **Schedule:** Ручний запуск
- **Особливості:**
  - Збагачення customers даними з user_profiles
  - Заповнення порожніх полів
  - Використання MERGE логіки

## 📊 Data Layers

### Raw Layer (`data/`)
- CSV файли (sales, customers)
- JSONL файли (user_profiles)
- Оригінальні дані без обробки

### Bronze Layer (`spark_local/bronze/`)
- Parquet формат
- Schema-on-read (всі поля STRING)
- Оригінальні назви колонок
- Мінімальна обробка

### Silver Layer (`spark_local/silver/`)
- Parquet формат
- Очищені та трансформовані дані
- Правильні типи даних
- Стандартизовані назви колонок
- Партиціонування (для sales)

### Gold Layer (`spark_local/gold/`)
- Parquet формат
- Збагачені дані
- Готові для аналітики
- Таблиця: `user_profiles_enriched`

## 🔑 Ключові файли

### DAG файли
- `process_sales_dag.py` - щоденний запуск
- `process_customers_dag.py` - щоденний запуск
- `process_user_profiles_dag.py` - ручний запуск, автоматично тригерить enrich
- `enrich_user_profiles_dag.py` - ручний запуск

### ETL скрипти
- `process_sales_etl.py` - обробка продажів
- `process_customers_etl.py` - обробка клієнтів
- `process_user_profiles_etl.py` - обробка профілів
- `enrich_user_profiles_etl.py` - збагачення даних

### Аналітика
- `analytical_query.py` - PySpark версія аналітичного запиту
- `postgresql/analytical_query.sql` - SQL версія (якщо використовується PostgreSQL)

## ⚙️ Змінні оточення

- `DATA_LAKE_BASE_DIR` - базова директорія проекту (за замовчуванням: поточна директорія)
- `AIRFLOW_HOME` - директорія Airflow (за замовчуванням: поточна директорія)
- `SPARK_MASTER` - Spark master (за замовчуванням: `local[*]`)
- `SPARK_DRIVER_MEMORY` - Пам'ять драйвера (за замовчуванням: `2g`)
- `SPARK_EXECUTOR_MEMORY` - Пам'ять executor (за замовчуванням: `2g`)

