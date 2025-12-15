from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from srm_generate import generate_and_save_srm_csv


with DAG(
    dag_id="srm_generate_csv",
    start_date=datetime(2025, 12, 14),
    schedule_interval="@once",
    catchup=False,
    tags=["srm", "generate"],
) as dag:

    generate_csv = PythonOperator(
        task_id="generate_csv",
        python_callable=generate_and_save_srm_csv,
        op_kwargs={
            "out_dir": "/opt/airflow/data",
            "n_suppliers": 500,
            "n_departments": 16,
            "n_contracts": 1500,
            "n_po_lines": 100_000,
            "n_receipts": 120_000,
            "months": 24,
            "seed": 42,
        },
    )
