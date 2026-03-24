from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from etl.validation import *
from etl.superstore_transform_data import *

DATASET = "jr2ngb/superstore-data"
OUTPUT_PATH = "~/superstores-analytics/data"

with DAG(
    dag_id="superstore_sales",
    start_date=datetime(2024,1,1),
    schedule="@monthly",
    catchup=False
) as dag:
    mkdir_dag           = BashOperator(task_id="clean_data_directory",bash_command="""rm -rf {OUTPUT_PATH}; mkdir {OUTPUT_PATH}""")
    download_dataset    = BashOperator(task_id="download_dataset",bash_command=f"""kaggle datasets download -d {DATASET} -p {OUTPUT_PATH} --unzip""")
    validate_dataset    = PythonOperator(task_id="validate_dataset",python_callable=validate_data)
    augment_dataset     = PythonOperator(task_id="augment_dataset", python_callable=augment_data)
    update_dataset      = BashOperator(task_id="update_dataset",bash_command=f"""rm {OUTPUT_PATH}/*.csv; mv {OUTPUT_PATH}/superstore/*.csv {OUTPUT_PATH}; rm -rf {OUTPUT_PATH}/superstore; mv {OUTPUT_PATH}/*.csv {OUTPUT_PATH}/superstore.csv;""")

    transform_dataset   = PythonOperator(task_id="transform_dataset",python_callable=transform_data)
    build_datamart      = PythonOperator(task_id="build_data_mart",python_callable=build_data_mart)
    upload2db           = PythonOperator(task_id="upload_to_db", python_callable=upload_to_db)

    mkdir_dag >> download_dataset >> validate_dataset >> augment_dataset >> update_dataset >> transform_dataset >> build_datamart >> upload2db
