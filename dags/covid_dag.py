from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.etl.extract import run_extraction
from src.etl.transform import run_transformation
from src.etl.load import run_load


def run_extraction_job():
    run_extraction('03-25-2025.csv')

def run_transformation_job():
    run_transformation()

def run_loading_job():
    run_load()



with DAG(dag_id = "covid_dag" , description="Dag for orchestration ETL of covid data pipeline" ,start_date=datetime(2025,11,26),schedule_interval="@once", tags=["Covid-19" ],
         catchup=False)  as dag:
    
    
    dataset_waiting = FileSensor(task_id = 'dataset_waiting' , 
                                fs_conn_id = 'file_system_connection',
                                filepath = 'data/03-25-2025.csv',
                                poke_interval=5,
                                timeout=20,
                                mode = 'poke'
                                )
    
    extraction_task = PythonOperator(task_id = 'extraction_task',
                                    python_callable=run_extraction_job)
    
    transformation_task = PythonOperator(task_id = 'transformation_task',
                                    python_callable=run_transformation_job)
    
    load_task = PythonOperator(task_id = 'loading_task',
                                    python_callable=run_loading_job)
    
    verifiy_by_count = PostgresOperator(task_id = 'verifiy_by_count',
                                        postgres_conn_id = 'postgres_connection',
                                        sql = """
                                                SELECT count(*)
                                                FROM bronze.covid
                                                where ingested_at = (SELECT MAX(ingested_at) from bronze.covid);
                                            """)

    

dataset_waiting>>extraction_task>>transformation_task>>load_task>>verifiy_by_count


   