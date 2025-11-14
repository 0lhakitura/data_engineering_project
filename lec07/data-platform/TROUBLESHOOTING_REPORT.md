## Health Check Commands

### Check Container Status
```bash
docker-compose ps
```

### Check Container Logs
```bash
# All containers
docker-compose logs

# Specific service
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver
docker-compose logs postgres_analytics
docker-compose logs postgres_airflow
```

### Check dbt Installation
```bash
docker exec airflow_scheduler which dbt
docker exec airflow_scheduler /home/airflow/.local/bin/dbt --version
```

### Check DAG Status
```bash
docker exec airflow_scheduler airflow dags list
docker exec airflow_scheduler airflow tasks list custom_dbt_transformations
```

### Test Database Connections
```bash
# Analytics DB
docker exec -it analytics_db psql -U datauser -d analytics

# Airflow Metastore
docker exec -it airflow_metastore psql -U airflowuser -d airflow
```


- **Action Required:** Restart containers to apply docker-compose changes:
  ```bash
  docker-compose down
  docker-compose up -d
  ```

