from __future__ import annotations

from datetime import timedelta
import pendulum
import sys
import os

# Airflow imports
# Note: Red underlines in IDE are normal - Airflow is only installed in Docker container
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Add python_scripts to path
python_scripts_path = os.path.join(os.path.dirname(__file__), 'python_scripts')
sys.path.insert(0, python_scripts_path)

# Import custom functions
# Note: These imports work correctly in Airflow container
from train_model import process_iris_data
from inference import predict_iris

# Київський час (використовуємо старе ім'я для сумісності)
try:
    KYIV = pendulum.timezone("Europe/Kyiv")
except:
    KYIV = pendulum.timezone("Europe/Kiev")  # Fallback для старіших версій

# Ми хочемо три ранки: 22, 23, 24 квітня 2025 о 01:00 (GMT+3)
START = KYIV.datetime(2025, 4, 22, 1, 0, 0)
END   = KYIV.datetime(2025, 4, 24, 1, 0, 0)

default_args = {
    "owner": "airflow",
    "retries": 0,
    # end_date на рівні task обмежить планування після 24.04
    "end_date": END + timedelta(seconds=1),
}

with DAG(
    dag_id="process_iris",
    description="DBT transform -> ML train -> Inference -> Email notify (Iris), per-day processing",
    start_date=START,
    schedule="0 1 * * *",            # щодня о 01:00
    catchup=True,                     # проганяємо історичні дати в діапазоні
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["iris", "ml", "dbt"],
) as dag:

    # 1) ML тренування моделі
    ml_train = PythonOperator(
        task_id="ml_train",
        python_callable=process_iris_data,
        provide_context=True,
    )

    # 2) Інференс (класифікація ірисів)
    ml_inference = PythonOperator(
        task_id="ml_inference",
        python_callable=predict_iris,
        provide_context=True,
    )

    # 3) Нотифікація про успіх листом з результатами інференсу
    def format_email_content(**context):
        """Функція для форматування email з результатами через XCom"""
        # Отримуємо результати з XCom
        train_results = context['ti'].xcom_pull(task_ids='ml_train', key='return_value')
        inference_results = context['ti'].xcom_pull(task_ids='ml_inference', key='return_value')
        
        # Форматуємо email
        html_content = f"""
        <h2>Iris Pipeline Execution Report</h2>
        <p><strong>Execution Date:</strong> {context['ds']}</p>
        <p><strong>Execution Time:</strong> {context['ts']}</p>
        
        <h3>Model Training Results:</h3>
        <ul>
            <li><strong>Full Model Accuracy:</strong> {train_results.get('full_model_accuracy', 0):.4f}</li>
            <li><strong>Top 5 Features Model Accuracy:</strong> {train_results.get('top5_model_accuracy', 0):.4f}</li>
            <li><strong>Top Features:</strong> {', '.join(train_results.get('top_features', []))}</li>
        </ul>
        
        <h3>Inference Results:</h3>
        <ul>
            <li><strong>Total Predictions:</strong> {inference_results.get('total_predictions', 0)}</li>
        """
        
        if inference_results.get('accuracy') is not None:
            html_content += f"<li><strong>Accuracy:</strong> {inference_results['accuracy']*100:.2f}%</li>"
        
        html_content += f"""
            <li><strong>Average Confidence:</strong> {inference_results.get('average_confidence', 0)*100:.2f}%</li>
        </ul>
        
        <h3>Sample Predictions:</h3>
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <tr>
                <th>Index</th>
                <th>Predicted Species</th>
        """
        
        # Перевіряємо чи є actual_species
        has_actual = inference_results.get('predictions') and len(inference_results['predictions']) > 0 and 'actual_species' in inference_results['predictions'][0]
        if has_actual:
            html_content += "<th>Actual Species</th><th>Correct?</th>"
        
        html_content += "<th>Max Confidence</th></tr>"
        
        # Додаємо перші 5 прогнозів
        predictions = inference_results.get('predictions', [])[:5]
        for pred in predictions:
            html_content += f"<tr><td>{pred.get('index', 'N/A')}</td><td>{pred.get('predicted_species', 'N/A')}</td>"
            if has_actual:
                correct = '✓' if pred.get('is_correct', False) else '✗'
                html_content += f"<td>{pred.get('actual_species', 'N/A')}</td><td>{correct}</td>"
            html_content += f"<td>{pred.get('max_confidence', 0)*100:.2f}%</td></tr>"
        
        html_content += f"""
        </table>
        
        <p><em>Full results saved to: {inference_results.get('results_csv_path', 'N/A')}</em></p>
        
        <p>Best regards,<br>Airflow Pipeline</p>
        """
        
        return html_content

    def send_email_notification(**context):
        """Функція для відправки email з результатами"""
        from airflow.models import Variable
        
        # Отримуємо відформатований контент з XCom
        html_content = context['ti'].xcom_pull(task_ids='format_email', key='return_value')
        
        if not html_content:
            print("WARNING: No HTML content from format_email task. Creating default content.")
            html_content = f"""
            <h2>Iris Pipeline Execution Report</h2>
            <p><strong>Execution Date:</strong> {context.get('ds', 'N/A')}</p>
            <p><strong>Execution Time:</strong> {context.get('ts', 'N/A')}</p>
            <p><em>Note: Detailed results were not available. Please check task logs.</em></p>
            """
        
        # Отримуємо email змінну або використовуємо дефолт
        try:
            to_email = Variable.get('notify_email', default_var='admin@example.com')
        except Exception as e:
            print(f"WARNING: Could not get notify_email variable: {e}. Using default.")
            to_email = 'admin@example.com'
        
        print(f"Preparing to send email to: {to_email}")
        print(f"HTML content length: {len(html_content) if html_content else 0} characters")
        
        # Відправляємо email
        try:
            send_email(
                to=to_email,
                subject=f"Iris Pipeline Results for {context.get('ds', 'Unknown Date')}",
                html_content=html_content,
                mime_subtype='html'
            )
            print(f"✓ Email successfully sent to {to_email}")
        except Exception as e:
            print(f"✗ ERROR sending email: {e}")
            raise

    notify_email = PythonOperator(
        task_id="notify_email",
        python_callable=send_email_notification,
        provide_context=True,
        trigger_rule='all_success',
    )
    
    # Допоміжна задача для форматування email (виконується перед notify_email)
    format_email = PythonOperator(
        task_id="format_email",
        python_callable=format_email_content,
        provide_context=True,
    )

    chain(ml_train, ml_inference, format_email, notify_email)



