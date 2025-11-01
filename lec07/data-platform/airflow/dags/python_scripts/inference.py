# iris_inference.py
import pandas as pd
from sqlalchemy import create_engine
import os
import joblib
import json

# Species mapping (label encoding to species name)
SPECIES_MAP = {
    0: 'setosa',
    1: 'versicolor',
    2: 'virginica'
}

def predict_iris(**kwargs):
    """
    Perform inference on Iris data from iris_processed table.
    Reads the trained model and makes predictions.
    Returns predictions that can be passed via XCom.
    """
    # Get connection parameters from environment variables or use defaults
    pg_host = os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('ANALYTICS_DB', 'analytics')
    pg_user = os.getenv('ETL_USER', 'etl_user')
    pg_password = os.getenv('ETL_PASSWORD', 'etl_password')
    
    # Create SQLAlchemy engine for DataFrame operations
    conn_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(conn_string)
    
    # Query the processed Iris data from the dbt-transformed table
    query = """
    SELECT * FROM homework.iris_processed
    LIMIT 10
    """
    
    df = pd.read_sql(query, engine)
    print(f"Loaded {df.shape[0]} rows for inference")
    
    # Load model and preprocessing info
    models_dir = os.getenv('MODELS_DIR', '/opt/airflow/models')
    model_path = os.path.join(models_dir, 'iris_top5_model.joblib')
    info_path = os.path.join(models_dir, 'iris_model_info.json')
    
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")
    
    if not os.path.exists(info_path):
        raise FileNotFoundError(f"Model info file not found: {info_path}")
    
    # Load model
    model = joblib.load(model_path)
    print(f"Loaded model from {model_path}")
    
    # Load preprocessing info
    with open(info_path, 'r') as f:
        model_info = json.load(f)
    
    top_features = model_info['top_features']
    print(f"Using top features: {', '.join(top_features)}")
    
    # Prepare features (remove target columns)
    df_features = df.drop(columns=[
        'species',
        'species_label_encoded',
        'is_species__setosa',
        'is_species__versicolor',
        'is_species__virginica',
        'is_species__'
    ], errors='ignore')
    
    # Select only top features
    X_inference = df_features[top_features]
    
    # Make predictions
    predictions = model.predict(X_inference)
    prediction_proba = model.predict_proba(X_inference)
    
    # Map predictions to species names
    predicted_species = [SPECIES_MAP.get(int(pred), 'unknown') for pred in predictions]
    
    # Get actual species for comparison
    actual_species = df['species'].tolist() if 'species' in df.columns else None
    
    # Create results DataFrame
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
    
    # Save inference results to database
    results_dir = os.getenv('RESULTS_DIR', '/opt/airflow/inference_results')
    os.makedirs(results_dir, exist_ok=True)
    
    # Save to CSV for reference
    results_csv_path = os.path.join(results_dir, f'iris_predictions_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv')
    results_df.to_csv(results_csv_path, index=False)
    print(f"Saved inference results to {results_csv_path}")
    
    # Save to database
    # Create table if it doesn't exist (schema ml_results already exists)
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
    
    # Add inference timestamp and rename index column
    results_df['inference_timestamp'] = pd.Timestamp.now()
    # Rename 'index' column to 'prediction_index' if it exists
    if 'index' in results_df.columns:
        results_df = results_df.rename(columns={'index': 'prediction_index'})
    
    # Save to database (using engine directly for pandas to_sql)
    results_df.to_sql('iris_predictions', engine, schema='ml_results', 
                     if_exists='append', index=False)
    
    # Calculate summary statistics
    total_predictions = len(results_df)
    if 'is_correct' in results_df.columns:
        correct_predictions = results_df['is_correct'].sum()
        accuracy = correct_predictions / total_predictions if total_predictions > 0 else 0
    else:
        accuracy = None
    
    avg_confidence = results_df['max_confidence'].mean()
    
    # Prepare summary for XCom
    # Convert DataFrame to dict, excluding non-serializable columns
    predictions_for_xcom = results_df.drop(columns=['inference_timestamp'], errors='ignore').to_dict('records')
    # Convert any remaining Timestamp objects to strings
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
    
    print(f"Inference complete: {total_predictions} predictions made")
    if accuracy is not None:
        print(f"Accuracy: {accuracy:.2%}")
    print(f"Average confidence: {avg_confidence:.2%}")
    
    return summary

if __name__ == "__main__":
    result = predict_iris()
    print("\nInference Results:")
    print(result)

