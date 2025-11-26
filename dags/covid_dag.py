from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime






with DAG(dag_id = "covid_dag" , description="Dag for orchestration ETL of covid data pipeline" ,start_date=datetime(2025,11,26),schedule_interval="@daily", tags=["Covid-19" ],catchup=False)  as dag:
    
    max_date = PostgresOperator(
        task_id = "max_date",
        postgres_conn_id="postgres_connection",
        sql = """
                SELECT max(ingested_at) from bronze.covid;
            """
    )