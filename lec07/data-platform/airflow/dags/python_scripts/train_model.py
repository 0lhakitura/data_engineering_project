import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
import os
import joblib
from datetime import datetime

def process_iris_data(**kwargs):
    """
    Process Iris dataset from PostgreSQL, train a model, and save results.
    This function is designed to be used with Airflow's PythonOperator.
    """
    pg_host = os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('ANALYTICS_DB', 'analytics')
    pg_user = os.getenv('ETL_USER', 'etl_user')
    pg_password = os.getenv('ETL_PASSWORD', 'etl_password')
    
    conn_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(conn_string)
    
    query = """
    SELECT * FROM homework.iris_processed
    """
    
    df = pd.read_sql(query, engine)
    print(f"Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")
    
    df.drop(
        [
            'species',
            'is_species__setosa',
            'is_species__versicolor',
            'is_species__virginica',
            'is_species__',
         ],
         axis=1, inplace=True, errors='ignore'  # Use errors='ignore' to handle columns that might not exist
    )
    
    X = df.drop(columns=['species_label_encoded'])
    y = df['species_label_encoded']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    print(f"Training set: {X_train.shape[0]} samples, Test set: {X_test.shape[0]} samples")
    
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    
    train_score = clf.score(X_train, y_train)
    test_score = clf.score(X_test, y_test)
    print(f"Initial model - Training accuracy: {train_score:.4f}, Test accuracy: {test_score:.4f}")
    
    importances = clf.feature_importances_
    feature_names = X_train.columns
    feature_importance_df = pd.DataFrame({
                              'Feature': feature_names,
                              'Importance': importances
                            })
    
    top_features = feature_importance_df.sort_values(
                      by='Importance',
                      ascending=False
                    ).head(5)['Feature'].tolist()
    
    print(f"Top 5 features: {', '.join(top_features)}")
    
    X_train_top5 = X_train[top_features]
    X_test_top5 = X_test[top_features]
    
    clf_top5 = RandomForestClassifier(n_estimators=100, random_state=42)
    clf_top5.fit(X_train_top5, y_train)
    
    train_score_top5 = clf_top5.score(X_train_top5, y_train)
    test_score_top5 = clf_top5.score(X_test_top5, y_test)
    print(f"Top 5 features model - Training accuracy: {train_score_top5:.4f}, Test accuracy: {test_score_top5:.4f}")
    
    models_dir = os.getenv('MODELS_DIR', '/opt/airflow/models')
    
    # Try to create directory, if it fails try alternative locations
    try:
        os.makedirs(models_dir, exist_ok=True)
        # Test if we can write to this directory
        test_file = os.path.join(models_dir, '.test_write')
        try:
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except (PermissionError, OSError):
            # Can't write here, try alternative location
            raise PermissionError(f"Cannot write to {models_dir}")
    except (PermissionError, OSError) as e:
        # Try using /tmp as fallback
        models_dir = os.path.join('/tmp', 'airflow_models')
        os.makedirs(models_dir, exist_ok=True)
        print(f"⚠️ Cannot use {os.getenv('MODELS_DIR', '/opt/airflow/models')}, using {models_dir} instead")
    
    full_model_path = os.path.join(models_dir, 'iris_full_model.joblib')
    joblib.dump(clf, full_model_path)
    print(f"Saved full model to {full_model_path}")
    
    top5_model_path = os.path.join(models_dir, 'iris_top5_model.joblib')
    joblib.dump(clf_top5, top5_model_path)
    print(f"Saved top5 model to {top5_model_path}")
    
    preprocessing_info = {
        'top_features': top_features,
        'feature_names': list(feature_names),
        'model_type': 'top5',
        'training_date': datetime.now().isoformat(),
        'train_accuracy': float(train_score_top5),
        'test_accuracy': float(test_score_top5),
        'n_estimators': 100,
        'random_state': 42,
        'species_mapping': {
            0: 'setosa',
            1: 'versicolor',
            2: 'virginica'
        }
    }
    import json
    info_path = os.path.join(models_dir, 'iris_model_info.json')
    with open(info_path, 'w') as f:
        json.dump(preprocessing_info, f, indent=2)
    print(f"✅ Saved model info for inference to {info_path}")
    print(f"   Model ready for inference with {len(top_features)} features")
    
    results_df = pd.DataFrame({
        'model_type': ['full_model', 'top5_features_model'],
        'train_accuracy': [train_score, train_score_top5],
        'test_accuracy': [test_score, test_score_top5],
        'features_count': [X_train.shape[1], 5],
        'run_timestamp': [pd.Timestamp.now(), pd.Timestamp.now()]
    })
    
    feature_importance_df['run_timestamp'] = pd.Timestamp.now()
    feature_importance_df = feature_importance_df.rename(columns={'Feature': 'feature', 'Importance': 'importance'})

    from sqlalchemy import text
    with engine.begin() as connection:
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS ml_results.iris_model_metrics (
            id SERIAL PRIMARY KEY,
            model_type VARCHAR(100),
            train_accuracy FLOAT,
            test_accuracy FLOAT,
            features_count INTEGER,
            run_timestamp TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS ml_results.iris_feature_importance (
            id SERIAL PRIMARY KEY,
            feature VARCHAR(100),
            importance FLOAT,
            run_timestamp TIMESTAMP
        );
        """))
        
    results_df.to_sql('iris_model_metrics', engine, schema='ml_results',
                      if_exists='append', index=False)
    
    feature_importance_df.to_sql('iris_feature_importance', engine, schema='ml_results',
                                if_exists='append', index=False)
    
    return {
        'top_features': top_features,
        'full_model_accuracy': float(test_score),
        'top5_model_accuracy': float(test_score_top5),
        'model_path': top5_model_path,
        'model_info_path': info_path,
        'models_dir': models_dir,
        'model_saved': True,
        'inference_ready': True
    }

if __name__ == "__main__":
    process_iris_data()