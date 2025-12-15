from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from load_one_csv_to_stg import load_all_csv_to_stg

with DAG(
    dag_id="srm_load_all_stg",
    start_date=datetime(2025, 12, 14),
    schedule_interval="@once",
    catchup=False,
    tags=["srm", "stg", "dds", "dm"],
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    load_all = PythonOperator(
        task_id="load_all_csv_to_stg",
        python_callable=load_all_csv_to_stg,
        op_kwargs={
            "postgres_conn_id": "dataset_db",
            "data_dir": "/opt/airflow/data",
            "delimiter": ",",
            "encoding": "utf-8",
        },
    )

    transform_to_dds = PostgresOperator(
        task_id="transform_stg_to_dds",
        postgres_conn_id="dataset_db",
        sql="srm_transform_stg_to_dds.sql",
    )

    build_dm = PostgresOperator(
        task_id="build_dm",
        postgres_conn_id="dataset_db",
        sql="srm_build_dm.sql",   # файл, который создает 3 DM
    )

    load_all >> transform_to_dds >> build_dm
