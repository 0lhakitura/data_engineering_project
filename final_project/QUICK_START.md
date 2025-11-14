# Швидкий старт

## 1. Встановлення

```bash
cd final_project
./setup.sh
```

## 2. Запуск Airflow

```bash
# Термінал 1
export AIRFLOW_HOME=$(pwd)
airflow webserver --port 8080

# Термінал 2
export AIRFLOW_HOME=$(pwd)
airflow scheduler
```

## 3. Запуск пайплайнів

### Через Airflow UI (рекомендовано):

1. Відкрийте http://localhost:8080
2. Логін: `admin` / Пароль: `admin`
3. Запустіть DAGs:
   - `process_sales_local` - автоматично (щодня)
   - `process_customers_local` - автоматично (щодня)
   - `process_user_profiles_local` - **вручну** (Trigger DAG)
   - `enrich_user_profiles_local` - запуститься автоматично після `process_user_profiles_local`

### Вручну через Spark:

```bash
export DATA_LAKE_BASE_DIR=$(pwd)

# 1. Sales
spark-submit --master local[*] spark_local/process_sales_etl.py

# 2. Customers
spark-submit --master local[*] spark_local/process_customers_etl.py

# 3. User Profiles (ручний запуск)
spark-submit --master local[*] spark_local/process_user_profiles_etl.py

# 4. Enrichment (ручний запуск)
spark-submit --master local[*] spark_local/enrich_user_profiles_etl.py
```

## 4. Аналітичний запит

```bash
spark-submit --master local[*] spark_local/analytical_query.py
```

## Послідовність виконання

1. ✅ `process_sales_local` 
2. ✅ `process_customers_local`
3. ✅ `process_user_profiles_local` (вручну)
4. ✅ `enrich_user_profiles_local` (автоматично)
5. ✅ Аналітичний запит

## Перевірка результатів

```bash
# Перевірка bronze
ls -lh spark_local/bronze/sales/
ls -lh spark_local/bronze/customers/

# Перевірка silver
ls -lh spark_local/silver/sales/
ls -lh spark_local/silver/customers/
ls -lh spark_local/silver/user_profiles/

# Перевірка gold
ls -lh spark_local/gold/user_profiles_enriched/
```

