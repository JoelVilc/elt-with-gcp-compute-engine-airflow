from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# dag default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# dag definition
with DAG(
    dag_id='check_load_csv_to_bq',
    start_date=datetime(2025, 1, 1),
    schedule=None,  # '@daily'
    description='Load a CSV file from GCS to Bigquery',
    tags=['bigquery', 'csv', 'gcs'],
    default_args=default_args,
    # on_failure_callback=handle_dag_failure,
    dagrun_timeout=timedelta(minutes=20)
) as dag:

    # task to check if the file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='bkt-src-global-data-dev',
        object='global_health_data.csv',
        timeout=300,
        poke_interval=30,
        mode='poke'
    )
    # task to load CSV file from GCS to Bigquery
    load_csv_to_big_query = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='bkt-src-global-data-dev',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table='silken-forest-440617-v9.raw_dataset.global_health_data',
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=';',
        autodetect=True,
    )

    check_file_exists >> load_csv_to_big_query
