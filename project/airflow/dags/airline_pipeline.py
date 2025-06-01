import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# Ajout des chemins pour les imports depuis le conteneur Docker
sys.path.append('/opt/airflow/project_root')
sys.path.append('/opt/airflow/project_root/connection_to_database')

# Import des fonctions refactorisées
from main import (
    create_database,
    download_data,
    split_and_train_language,
    transform_data,
    insert_data
)
from connection_to_database.new_dataframe import run as run_model

# Arguments par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['emperatornang@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Définition du DAG
with DAG(
    dag_id='airlines_etl_ml_pipeline',
    default_args=default_args,
    description='Pipeline ETL + ML pour DST Airlines',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['airlines', 'ETL', 'ML']
) as dag:

    task_1_create_db = PythonOperator(
        task_id='create_database',
        python_callable=create_database
    )

    task_2_download = PythonOperator(
        task_id='download_data',
        python_callable=download_data
    )

    task_3_train_language = PythonOperator(
        task_id='split_and_train_language',
        python_callable=split_and_train_language
    )

    task_4_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    task_5_insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data
    )

    task_6_predict_model = PythonOperator(
        task_id='run_prediction_pipeline',
        python_callable=run_model
    )

    task_7_send_email = EmailOperator(
        task_id='send_success_email',
        to='emperatornang@gmail.com',
        subject='Succès - Pipeline airlines_etl_ml_pipeline',
        html_content="""
            <h3>Le pipeline airlines_etl_ml_pipeline a été exécuté avec succès.</h3>
            <p>Toutes les tâches se sont terminées sans erreur.</p>
        """
    )

    # Dépendances dans l'ordre logique souhaité
    task_1_create_db >> task_2_download >> task_3_train_language >> task_4_transform >> task_5_insert_data >> task_6_predict_model >> task_7_send_email
