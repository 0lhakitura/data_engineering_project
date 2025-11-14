#!/bin/bash
# Setup script for Final Project - Local PySpark Data Pipeline

set -e

echo " Налаштування Final Project: Data Pipeline на PySpark"
echo ""

# Перевірка Python
echo "Перевірка передумов..."
if ! command -v python3 &> /dev/null; then
    echo "Python 3 не знайдено. Будь ласка, встановіть Python 3.8+"
    exit 1
fi
echo "✓ Python: $(python3 --version)"

# Перевірка Spark
if ! command -v spark-submit &> /dev/null; then
    echo " Spark не знайдено в PATH. Переконайтеся, що Spark встановлено."
    echo "   Завантажте з: https://spark.apache.org/downloads.html"
else
    echo "✓ Spark знайдено"
fi

# Встановлення залежностей
echo ""
echo " Встановлення Python залежностей..."
pip install -r requirements.txt

echo ""
echo " Створення директорій для даних..."
mkdir -p spark_local/{bronze,silver,gold}
mkdir -p spark-warehouse

# Налаштування Airflow
echo ""
echo " Налаштування Airflow..."

# Встановлення AIRFLOW_HOME
export AIRFLOW_HOME=$(pwd)
echo "AIRFLOW_HOME=$AIRFLOW_HOME" > .env
echo "DATA_LAKE_BASE_DIR=$AIRFLOW_HOME" >> .env

# Ініціалізація бази даних (якщо ще не ініціалізована)
if [ ! -f "airflow.db" ]; then
    echo "Ініціалізація Airflow бази даних..."
    airflow db init
    
    echo ""
    echo "Створення адміністративного користувача..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin || echo "Користувач вже існує"
else
    echo "✓ Airflow база даних вже існує"
fi

# Налаштування dags_folder в airflow.cfg
if [ -f "airflow.cfg" ]; then
    # Оновлення dags_folder на абсолютний шлях
    DAGS_FOLDER=$(realpath airflow/dags)
    sed -i.bak "s|^dags_folder = .*|dags_folder = $DAGS_FOLDER|" airflow.cfg
    echo "✓ Налаштовано dags_folder: $DAGS_FOLDER"
fi

echo ""
echo " Налаштування завершено!"
echo ""
echo " Наступні кроки:"
echo "1. Запустіть Airflow webserver:"
echo "   export AIRFLOW_HOME=\$(pwd)"
echo "   airflow webserver --port 8080"
echo ""
echo "2. В іншому терміналі запустіть scheduler:"
echo "   export AIRFLOW_HOME=\$(pwd)"
echo "   airflow scheduler"
echo ""
echo "3. Відкрийте Airflow UI: http://localhost:8080"
echo "   Логін: admin / Пароль: admin"
echo ""
echo "4. Запустіть DAGs через UI або вручну через spark-submit"

