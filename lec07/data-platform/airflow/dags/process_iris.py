from __future__ import annotations

from datetime import timedelta
import pendulum
import sys
import os


from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email


try:
    from plugins.dbt_operator import DbtOperator
except ImportError:
    import importlib.util
    plugins_path = '/opt/airflow/plugins/dbt_operator.py'
    if os.path.exists(plugins_path):
        spec = importlib.util.spec_from_file_location("dbt_operator", plugins_path)
        dbt_operator_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dbt_operator_module)
        DbtOperator = dbt_operator_module.DbtOperator
    else:
        plugins_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'plugins')
        if os.path.exists(plugins_dir):
            sys.path.insert(0, plugins_dir)
            from dbt_operator import DbtOperator
        else:
            raise ImportError("Cannot find dbt_operator. Please check plugins directory.")

python_scripts_path = os.path.join(os.path.dirname(__file__), 'python_scripts')
sys.path.insert(0, python_scripts_path)

from train_model import process_iris_data
from inference import predict_iris

try:
    KYIV = pendulum.timezone("Europe/Kyiv")
except:
    KYIV = pendulum.timezone("Europe/Kiev")  # Fallback для старіших версій

# Ми хочемо дві ранки: 22, 24 грудня 2025 о 01:00 (GMT+3)
# Для тестування використовуємо поточну дату
START = KYIV.datetime(2025, 11, 17, 1, 0, 0)  # Змінено для тестування
END   = KYIV.datetime(2025, 12, 24, 1, 0, 0)

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
    catchup=True,
    default_args=default_args,
    render_template_as_native_obj=True,
    tags=["iris", "ml", "dbt"],
) as dag:

    # 1) DBT трансформація датасету іріс для навчання ML класифікатору
    dbt_transform = DbtOperator(
        task_id="dbt_transform",
        command="run",
        profile="homework",
        target="dev",
        project_dir="/opt/airflow/dags/dbt/homework",
        profiles_dir="/opt/airflow/dags/dbt",
        env_vars={
            "POSTGRES_ANALYTICS_HOST": os.getenv("POSTGRES_ANALYTICS_HOST", "postgres_analytics"),
            "ETL_USER": os.getenv("ETL_USER", "etl_user"),
            "ETL_PASSWORD": os.getenv("ETL_PASSWORD", "etl_password"),
            "ANALYTICS_DB": os.getenv("ANALYTICS_DB", "analytics"),
        },
    )

    # 2) ML тренування моделі
    ml_train = PythonOperator(
        task_id="ml_train",
        python_callable=process_iris_data,
        provide_context=True,
    )

    # 3) Інференс (класифікація ірисів)
    ml_inference = PythonOperator(
        task_id="ml_inference",
        python_callable=predict_iris,
        provide_context=True,
    )

    # 4) Нотифікація про успіх листом з результатами інференсу
    def format_email_content(**context):
        """
        Функція для форматування email з результатами через XCom.
        Отримує результати тренування та інференсу з попередніх задач.
        """
        train_results = context['ti'].xcom_pull(task_ids='ml_train', key='return_value')
        inference_results = context['ti'].xcom_pull(task_ids='ml_inference', key='return_value')
        
        # Перевірка наявності результатів
        if not train_results:
            train_results = {'full_model_accuracy': 0, 'top5_model_accuracy': 0, 'top_features': []}
            print("WARNING: No training results found in XCom")
        
        if not inference_results:
            inference_results = {'total_predictions': 0, 'accuracy': None, 'average_confidence': 0, 'predictions': []}
            print("WARNING: No inference results found in XCom")
        
        # Форматуємо email з результатами прогнозування
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        <div style="max-width: 800px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
                🌸 Iris Pipeline Execution Report
            </h2>
            
            <div style="background-color: #ecf0f1; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <p><strong>📅 Execution Date:</strong> {context.get('ds', 'N/A')}</p>
                <p><strong>⏰ Execution Time:</strong> {context.get('ts', 'N/A')}</p>
            </div>
            
            <h3 style="color: #27ae60; margin-top: 30px;">📊 Model Training Results</h3>
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <ul style="list-style-type: none; padding: 0;">
                    <li style="margin: 10px 0;">
                        <strong>Full Model Accuracy:</strong> 
                        <span style="color: #27ae60; font-size: 1.1em;">
                            {train_results.get('full_model_accuracy', 0)*100:.2f}%
                        </span>
                    </li>
                    <li style="margin: 10px 0;">
                        <strong>Top 5 Features Model Accuracy:</strong> 
                        <span style="color: #27ae60; font-size: 1.1em;">
                            {train_results.get('top5_model_accuracy', 0)*100:.2f}%
                        </span>
                    </li>
                    <li style="margin: 10px 0;">
                        <strong>Top Features Used:</strong> 
                        <code style="background-color: #e8e8e8; padding: 3px 8px; border-radius: 3px;">
                            {', '.join(train_results.get('top_features', []))}
                        </code>
                    </li>
                    <li style="margin: 10px 0;">
                        <strong>Model Status:</strong> 
                        <span style="color: #27ae60;">
                            {'✅ Ready for Inference' if train_results.get('inference_ready') else '⚠️ Not Ready'}
                        </span>
                    </li>
                </ul>
            </div>
            
            <h3 style="color: #e74c3c; margin-top: 30px;">🔮 Inference Results (Predictions)</h3>
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <ul style="list-style-type: none; padding: 0;">
                    <li style="margin: 10px 0;">
                        <strong>Total Predictions Made:</strong> 
                        <span style="color: #e74c3c; font-size: 1.1em;">
                            {inference_results.get('total_predictions', 0)}
                        </span>
                    </li>
        """
        
        if inference_results.get('accuracy') is not None:
            accuracy_pct = inference_results['accuracy'] * 100
            accuracy_color = '#27ae60' if accuracy_pct >= 90 else '#f39c12' if accuracy_pct >= 70 else '#e74c3c'
            html_content += f"""
                    <li style="margin: 10px 0;">
                        <strong>Prediction Accuracy:</strong> 
                        <span style="color: {accuracy_color}; font-size: 1.1em; font-weight: bold;">
                            {accuracy_pct:.2f}%
                        </span>
                    </li>
            """
        
        html_content += f"""
                    <li style="margin: 10px 0;">
                        <strong>Average Confidence:</strong> 
                        <span style="color: #3498db; font-size: 1.1em;">
                            {inference_results.get('average_confidence', 0)*100:.2f}%
                        </span>
                    </li>
                </ul>
            </div>
            
            <h3 style="color: #9b59b6; margin-top: 30px;">📋 Sample Predictions</h3>
            <table border="1" style="border-collapse: collapse; width: 100%; margin-bottom: 20px; background-color: white;">
                <thead>
                    <tr style="background-color: #3498db; color: white;">
                        <th style="padding: 10px; text-align: left;">#</th>
                        <th style="padding: 10px; text-align: left;">Predicted Species</th>
        """
        
        has_actual = (inference_results.get('predictions') and
                     len(inference_results['predictions']) > 0 and 
                     'actual_species' in inference_results['predictions'][0])
        
        if has_actual:
            html_content += """
                        <th style="padding: 10px; text-align: left;">Actual Species</th>
                        <th style="padding: 10px; text-align: center;">Correct?</th>
            """
        
        html_content += """
                        <th style="padding: 10px; text-align: center;">Confidence</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        predictions = inference_results.get('predictions', [])[:5]
        if not predictions:
            html_content += """
                    <tr>
                        <td colspan="5" style="padding: 10px; text-align: center; color: #7f8c8d;">
                            No predictions available
                        </td>
                    </tr>
            """
        else:
            for i, pred in enumerate(predictions, 1):
                predicted = pred.get('predicted_species', 'N/A')
                confidence = pred.get('max_confidence', 0) * 100
                confidence_color = '#27ae60' if confidence >= 90 else '#f39c12' if confidence >= 70 else '#e74c3c'
                
                html_content += f"""
                    <tr style="border-bottom: 1px solid #ddd;">
                        <td style="padding: 8px;">{i}</td>
                        <td style="padding: 8px; font-weight: bold; color: #2c3e50;">{predicted}</td>
                """
                
                if has_actual:
                    actual = pred.get('actual_species', 'N/A')
                    is_correct = pred.get('is_correct', False)
                    correct_symbol = '✅' if is_correct else '❌'
                    correct_color = '#27ae60' if is_correct else '#e74c3c'
                    html_content += f"""
                        <td style="padding: 8px;">{actual}</td>
                        <td style="padding: 8px; text-align: center; color: {correct_color}; font-size: 1.2em;">
                            {correct_symbol}
                        </td>
                    """
                
                html_content += f"""
                        <td style="padding: 8px; text-align: center; color: {confidence_color}; font-weight: bold;">
                            {confidence:.1f}%
                        </td>
                    </tr>
                """
        
        html_content += f"""
                </tbody>
            </table>
            
            <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin-top: 20px;">
                <p style="margin: 0;">
                    <strong>💾 Full Results:</strong> 
                    <code style="background-color: #e8e8e8; padding: 3px 8px; border-radius: 3px;">
                        {inference_results.get('results_csv_path', 'N/A')}
                    </code>
                </p>
            </div>
            
            <div style="margin-top: 30px; padding-top: 20px; border-top: 2px solid #ecf0f1; text-align: center; color: #7f8c8d;">
                <p>Best regards,<br><strong>Airflow ML Pipeline</strong></p>
            </div>
        </div>
        </body>
        </html>
        """
        
        return html_content

    def send_email_notification(**context):
        """Функція для відправки email з результатами"""
        from airflow.models import Variable
        
        html_content = context['ti'].xcom_pull(task_ids='format_email', key='return_value')
        
        if not html_content:
            print("WARNING: No HTML content from format_email task. Creating default content.")
            html_content = f"""
            <h2>Iris Pipeline Execution Report</h2>
            <p><strong>Execution Date:</strong> {context.get('ds', 'N/A')}</p>
            <p><strong>Execution Time:</strong> {context.get('ts', 'N/A')}</p>
            <p><em>Note: Detailed results were not available. Please check task logs.</em></p>
            """
        
        try:
            to_email = Variable.get('notify_email', default_var='admin@example.com')
        except Exception as e:
            print(f"WARNING: Could not get notify_email variable: {e}. Using default.")
            to_email = 'admin@example.com'
        
        print(f"Preparing to send email to: {to_email}")
        print(f"HTML content length: {len(html_content) if html_content else 0} characters")
        
        try:
            send_email(
                to=[to_email] if isinstance(to_email, str) else to_email,
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
    
    format_email = PythonOperator(
        task_id="format_email",
        python_callable=format_email_content,
        provide_context=True,
    )

    # Послідовність виконання: DBT трансформація -> ML тренування -> Інференс -> Email нотифікація
    chain(dbt_transform, ml_train, ml_inference, format_email, notify_email)



