# Docker Container Troubleshooting Report
Generated: $(date)

## Container Status Summary

All containers are **RUNNING** and appear healthy:

| Container | Status | Health | Uptime |
|-----------|--------|--------|--------|
| `airflow_webserver` | Up | Healthy | 4 hours |
| `airflow_scheduler` | Up | Running | 4 hours |
| `analytics_db` | Up | Healthy | 4 hours |
| `airflow_metastore` | Up | Healthy | 4 hours |

## Issues Identified

### ✅ Critical Issue #1: dbt Command Not Found in DAG Tasks (FIXED)

**Problem:**
- DAG `custom_dbt_transformations` is failing with error: `dbt: command not found`
- dbt is installed at `/home/airflow/.local/bin/dbt` but this path is not in the PATH when BashOperator tasks execute
- Tasks show `state=failed` even though executor reports `status=success`

**Error Details:**
```
/bin/bash: line 1: dbt: command not found
Command exited with return code 127
```

**Root Cause:**
- dbt is installed via pip in user space (`/home/airflow/.local/bin/`)
- BashOperator doesn't have this directory in PATH by default
- Tasks can't find the dbt executable

**Solution:** ✅ **IMPLEMENTED**
Updated the DAG to add `/home/airflow/.local/bin` to PATH in the bash command and environment variables for both `dbt_run` and `dbt_test` tasks.

### ✅ Warning #1: Incorrect Volume Mount in docker-compose.yaml (FIXED)

**Problem:**
Lines 61 and 106 in `docker-compose.yaml` have incorrect volume mounts:
```yaml
- ./dbt/packages:/opt/airflow/dbt/packages.yml
```

**Issue:**
- Mounting a directory (`./dbt/packages`) to a file path (`packages.yml`)
- This should be a directory mount, not a file mount

**Previous:** ❌
- `./dbt/packages` (directory) → `/opt/airflow/dbt/packages.yml` (file path)

**Fixed:** ✅
- `./dbt/packages` (directory) → `/opt/airflow/dbt/packages` (directory)

### 🟡 Warning #2: PostgreSQL Transaction Warnings

**Problem:**
Analytics database shows warnings:
```
WARNING: there is already a transaction in progress
```

**Impact:**
- Multiple concurrent transactions trying to execute
- May indicate connection pool issues or connection reuse problems
- Not critical but should be monitored

### ✅ Warning #3: Airflow Configuration Deprecations (FIXED)

**Warnings Found:**
1. `dag_default_view` setting has old default value `'tree'` (should be `'grid'`)
2. `base_log_folder` option moved from `[core]` to `[logging]`
3. `dag_concurrency` renamed to `max_active_tasks_per_dag`

**Impact:**
- Non-critical, but will break in Airflow 3.0
- Should update `airflow.cfg` to use new configuration options

**Solution:** ✅ **IMPLEMENTED**
All three deprecation warnings have been resolved in `airflow.cfg`:
- Changed `dag_default_view = tree` to `dag_default_view = grid`
- Moved `base_log_folder` from `[core]` to new `[logging]` section
- Replaced `dag_concurrency` with `max_active_tasks_per_dag`

## Docker Compose Configuration Review

### Volume Mounts Analysis

**Correct Mounts:**
- ✅ `./airflow/dags:/opt/airflow/dags` - DAG files
- ✅ `./airflow/plugins:/opt/airflow/plugins` - Custom plugins
- ✅ `./dbt:/opt/airflow/dbt` - dbt project directory
- ✅ `airflow_logs:/opt/airflow/logs` - Persistent logs
- ✅ `ml_models:/opt/airflow/models` - ML model storage
- ✅ `ml_results:/opt/airflow/inference_results` - ML results storage

**Issues:**
- ✅ `./dbt/packages:/opt/airflow/dbt/packages.yml` - Fixed to `./dbt/packages:/opt/airflow/dbt/packages` (directory mount)

### Network Configuration
- ✅ All services on `data_network` bridge network
- ✅ Proper service dependencies configured
- ✅ Health checks working correctly

### Environment Variables
- ✅ All required variables present
- ✅ Database connections properly configured
- ✅ Airflow secrets configured

## Recommended Fixes

### ✅ Fix 1: Update DAG to Use Full dbt Path (COMPLETED)

**File:** `airflow/dags/dbt_dag_simple.py` ✅ **FIXED**

Updated bash commands to include PATH in environment:
- Added `export PATH="/home/airflow/.local/bin:$PATH"` to bash commands
- Added `PATH` environment variable to both `dbt_run` and `dbt_test` tasks
- This ensures dbt is accessible when tasks execute

### ✅ Fix 2: Correct Volume Mounts (COMPLETED)

**File:** `docker-compose.yaml` ✅ **FIXED**

Fixed volume mounts on lines 61 and 106:
```yaml
# Changed FROM:
- ./dbt/packages:/opt/airflow/dbt/packages.yml

# TO:
- ./dbt/packages:/opt/airflow/dbt/packages
```
Applied to both `airflow-webserver` and `airflow-scheduler` services.

### ✅ Fix 3: Update Airflow Configuration (COMPLETED)

**File:** `airflow/airflow.cfg` ✅ **FIXED**

All three deprecation warnings resolved:
1. ✅ Changed `dag_default_view` from `tree` to `grid` in `[webserver]` section
2. ✅ Moved `base_log_folder` from `[core]` to new `[logging]` section
3. ✅ Replaced `dag_concurrency` with `max_active_tasks_per_dag` in `[core]` section

This ensures compatibility with Airflow 3.0 and eliminates deprecation warnings.

## Container Logs Summary

### Airflow Scheduler
- ✅ Running normally
- ⚠️ DAG execution failures due to dbt path issue
- ✅ Successfully processing task queues
- ⚠️ Some DAG runs marked as failed despite executor success (related to dbt path issue)

### Airflow Webserver
- ✅ Running normally
- ✅ Serving HTTP requests successfully
- ✅ No critical errors

### PostgreSQL Analytics
- ✅ Database healthy and accepting connections
- ⚠️ Transaction warnings (non-critical)
- ✅ All schemas and users created correctly

### PostgreSQL Airflow Metastore
- ✅ Database healthy and accepting connections
- ✅ Airflow tables initialized
- ✅ No errors detected

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

## Next Steps

1. ~~**Immediate:** Fix dbt path issue in DAG (Critical)~~ ✅ **COMPLETED**
   - Updated `dbt_dag_simple.py` to include `/home/airflow/.local/bin` in PATH
   - Added PATH environment variable to both `dbt_run` and `dbt_test` tasks

2. ~~**High Priority:** Fix volume mount in docker-compose.yaml~~ ✅ **COMPLETED**
   - Fixed volume mount from `./dbt/packages:/opt/airflow/dbt/packages.yml` to `./dbt/packages:/opt/airflow/dbt/packages`
   - Applied to both `airflow-webserver` and `airflow-scheduler` services

3. ~~**Medium Priority:** Update Airflow configuration for compatibility~~ ✅ **COMPLETED**
   - ✅ Changed `dag_default_view` from `tree` to `grid` in `[webserver]` section
   - ✅ Moved `base_log_folder` from `[core]` to `[logging]` section
   - ✅ Replaced `dag_concurrency` with `max_active_tasks_per_dag` in `[core]` section

4. **Low Priority:** Investigate PostgreSQL transaction warnings
   - Monitor for recurring transaction warnings in analytics_db
   - Check connection pooling settings if warnings persist

## Additional Notes

- All containers are currently running and stable
- Network connectivity between containers is working
- Database health checks are passing
- ✅ DAG execution issue fixed - PATH configuration updated in DAG tasks
- ✅ Volume mount configuration corrected in docker-compose.yaml
- No data loss or corruption detected
- **Action Required:** Restart containers to apply docker-compose changes:
  ```bash
  docker-compose down
  docker-compose up -d
  ```

