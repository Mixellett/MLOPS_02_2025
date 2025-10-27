from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Função que faz o ETL
def etl_csv_para_parquet():
    # Caminho do CSV local
    csv_path = "/Users/micheledeoliviocorrea/MLOPS_02_2025/02-dados/exercicio_extra/internacoes.csv"
    # Caminho para salvar o Parquet
    parquet_path = "/Users/micheledeoliviocorrea/MLOPS_02_2025/02-dados/exercicio_extra/internacoes_julagoset_2024.parquet"
    
    # Ler CSV
    df = pd.read_csv(csv_path, sep=';', encoding='latin1')
    
    # Transformações simples (exemplo)
    # df['idade'] = df['idade'].astype(int)
    
    # Salvar em Parquet
    df.to_parquet(parquet_path, index=False)
    print(f"Parquet atualizado: {parquet_path}")

# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2025, 10, 26),  # Data inicial
}

dag = DAG(
    'etl_csv_para_parquet_diario',
    default_args=default_args,
    description='DAG diário: CSV -> Parquet',
    schedule='@daily',  # executa uma vez por dia
    catchup=False
)

# Task
etl_task = PythonOperator(
    task_id='etl_csv_para_parquet_task',
    python_callable=etl_csv_para_parquet,
    dag=dag
)

etl_task
