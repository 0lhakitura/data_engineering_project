import pandas as pd
from sqlalchemy import create_engine
import os
import joblib
import json

SPECIES_MAP = {
    0: 'setosa',
    1: 'versicolor',
    2: 'virginica'
}

def predict_iris(**kwargs):
    """
    Perform inference on Iris data from iris_processed table.
    Reads the trained model and makes predictions on data in iris_processed format.
    Returns predictions that can be passed via XCom for email notification.
    
    This function:
    1. Loads data from homework.iris_processed table (dbt-transformed format)
    2. Loads the trained model from disk
    3. Applies the same preprocessing (top 5 features)
    4. Makes predictions and returns results via XCom
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
    LIMIT 10
    """
    
    df = pd.read_sql(query, engine)
    print(f"✅ Loaded {df.shape[0]} rows from iris_processed table for inference")
    print(f"   Columns in iris_processed: {list(df.columns)}")
    

    models_dir = os.getenv('MODELS_DIR', '/opt/airflow/models')
    if kwargs and 'ti' in kwargs:
        train_results = kwargs['ti'].xcom_pull(task_ids='ml_train', key='return_value')
        if train_results and train_results.get('models_dir'):
            models_dir = train_results['models_dir']
            print(f"Using models_dir from training task: {models_dir}")
    
    # Fallback to /tmp if models_dir doesn't exist or is not accessible
    if not os.path.exists(models_dir) or not os.access(models_dir, os.W_OK):
        tmp_models_dir = os.path.join('/tmp', 'airflow_models')
        if os.path.exists(tmp_models_dir):
            models_dir = tmp_models_dir
            print(f"⚠️ Using fallback models_dir: {models_dir}")
    
    model_path = os.path.join(models_dir, 'iris_top5_model.joblib')
    info_path = os.path.join(models_dir, 'iris_model_info.json')
    
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"Model file not found: {model_path}\n"
            f"Please ensure ml_train task completed successfully and saved the model."
        )
    
    if not os.path.exists(info_path):
        raise FileNotFoundError(
            f"Model info file not found: {info_path}\n"
            f"Please ensure ml_train task completed successfully and saved the model info."
        )
    
    model = joblib.load(model_path)
    print(f"✅ Loaded inference model from {model_path}")
    
    with open(info_path, 'r') as f:
        model_info = json.load(f)
    
    top_features = model_info['top_features']
    species_mapping_raw = model_info.get('species_mapping', SPECIES_MAP)
    
    # Convert species_mapping keys to int (JSON stores keys as strings)
    if isinstance(species_mapping_raw, dict):
        species_mapping = {int(k): v for k, v in species_mapping_raw.items()}
    else:
        species_mapping = SPECIES_MAP
    
    print(f"✅ Using top {len(top_features)} features: {', '.join(top_features)}")
    print(f"✅ Species mapping: {species_mapping}")
    
    df_features = df.drop(columns=[
        'species',
        'species_label_encoded',
        'is_species__setosa',
        'is_species__versicolor',
        'is_species__virginica',
        'is_species__'
    ], errors='ignore')
    
    X_inference = df_features[top_features]
    
    predictions = model.predict(X_inference)
    prediction_proba = model.predict_proba(X_inference)
    
    predicted_species = [species_mapping.get(int(pred), 'unknown') for pred in predictions]
    
    actual_species = df['species'].tolist() if 'species' in df.columns else None
    
    results = []
    for i, (pred_label, pred_species, proba) in enumerate(zip(predictions, predicted_species, prediction_proba)):
        result = {
            'index': i,
            'predicted_label': int(pred_label),
            'predicted_species': pred_species,
            'confidence_setosa': float(proba[0]),
            'confidence_versicolor': float(proba[1]),
            'confidence_virginica': float(proba[2]),
            'max_confidence': float(max(proba))
        }
        if actual_species:
            result['actual_species'] = actual_species[i]
            result['is_correct'] = pred_species == actual_species[i]
        results.append(result)
    
    results_df = pd.DataFrame(results)
 
    results_dir = os.getenv('RESULTS_DIR', '/opt/airflow/inference_results')
    
    # Try to create directory, if it fails try alternative locations
    try:
        os.makedirs(results_dir, exist_ok=True)
        # Test if we can write to this directory
        test_file = os.path.join(results_dir, '.test_write')
        try:
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except (PermissionError, OSError):
            # Can't write here, try alternative location
            raise PermissionError(f"Cannot write to {results_dir}")
    except (PermissionError, OSError) as e:
        # Try using /tmp as fallback
        results_dir = os.path.join('/tmp', 'airflow_inference_results')
        os.makedirs(results_dir, exist_ok=True)
        print(f"⚠️ Cannot use {os.getenv('RESULTS_DIR', '/opt/airflow/inference_results')}, using {results_dir} instead")
    
    results_csv_path = os.path.join(results_dir, f'iris_predictions_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv')
    results_df.to_csv(results_csv_path, index=False)
    print(f"Saved inference results to {results_csv_path}")

    from sqlalchemy import text
    with engine.begin() as connection:
        connection.execute(text("""
        CREATE TABLE IF NOT EXISTS ml_results.iris_predictions (
            id SERIAL PRIMARY KEY,
            prediction_index INTEGER,
            predicted_label INTEGER,
            predicted_species VARCHAR(50),
            actual_species VARCHAR(50),
            is_correct BOOLEAN,
            confidence_setosa FLOAT,
            confidence_versicolor FLOAT,
            confidence_virginica FLOAT,
            max_confidence FLOAT,
            inference_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
    
    results_df['inference_timestamp'] = pd.Timestamp.now()
    if 'index' in results_df.columns:
        results_df = results_df.rename(columns={'index': 'prediction_index'})
    
    results_df.to_sql('iris_predictions', engine, schema='ml_results',
                     if_exists='append', index=False)
    
    total_predictions = len(results_df)
    if 'is_correct' in results_df.columns:
        correct_predictions = results_df['is_correct'].sum()
        accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0
    else:
        accuracy = None
    
    avg_confidence = results_df['max_confidence'].mean()
    

    predictions_for_xcom = results_df.drop(columns=['inference_timestamp'], errors='ignore').to_dict('records')
    for pred in predictions_for_xcom:
        for key, value in pred.items():
            if isinstance(value, pd.Timestamp):
                pred[key] = value.isoformat()
    
    summary = {
        'total_predictions': int(total_predictions),
        'accuracy': float(accuracy) if accuracy is not None else None,
        'average_confidence': float(avg_confidence),
        'predictions': predictions_for_xcom[:5],  # Only first 5 for XCom to avoid size limits
        'results_csv_path': results_csv_path
    }
    
    print(f"✅ Inference complete: {total_predictions} predictions made")
    if accuracy is not None:
        print(f"   Accuracy: {accuracy:.2%}")
    print(f"   Average confidence: {avg_confidence:.2%}")
    print(f"✅ Results saved to database and CSV")
    print(f"   Results ready for email notification via XCom")
    
    return summary

if __name__ == "__main__":
    result = predict_iris()
    print("\nInference Results:")
    print(result)

