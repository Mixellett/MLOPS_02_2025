from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def teste_func():
    print("DAG de teste executando!")

with DAG(
    dag_id="dag_teste_minimo",
    start_date=datetime(2025, 10, 26),
    schedule=None,  # DAG manual
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="print_teste",
        python_callable=teste_func
    )
