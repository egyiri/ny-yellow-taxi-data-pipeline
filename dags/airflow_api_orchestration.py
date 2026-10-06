from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import (
    GlueDataQualityRuleSetEvaluationRunOperator,
    GlueJobOperator,
)
from docker.types import Mount


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BUCKET_NAME = "bright-first-s3-bucket-demo-563649768086-us-east-1-an"
SCRIPTS_BUCKET_NAME = "scripts-bucket-563649768086-us-east-1-an"
DATASET_ID = "4b4i-vvec"
API_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"

@dag(
    default_args=default_args,
    description="NY Yellow Taxi Data pipeline. Run AWS Glue jobs with parameters and perform data quality checks",
    schedule ="0 0 1 * *",  # Runs at midnight on the first day of every month
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 3, 1),
    catchup=True,
    max_active_runs=1,
    dag_id="ny_yellow_taxi_data_pipeline",
)

def ny_yellow_taxi_data_pipeline():
    start = EmptyOperator(task_id="start")

    # Task to run the Glue job for data extraction
    extract_data = GlueJobOperator(
        task_id="extract_data",
        job_name="ny_yellow_taxi_data_extraction",
        script_location=f"s3://{SCRIPTS_BUCKET_NAME}/glue_scripts/api_extraction_job.py",
        job_desc="Glue Job to extract data from User's API endpoint",
        # The name of the role from the Terraform output `glue_role_arn`.
        iam_role_name="city-data-project-glue-role",
        # `s3_bucket` is set to the scripts bucket.
        s3_bucket=f"{SCRIPTS_BUCKET_NAME}",
        region_name="us-east-1",
        create_job_kwargs={
            "GlueVersion": "3.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
        },
        script_args={
    "--target_path": f"s3://{BUCKET_NAME}/landing_zone/city_taxi_data",
    "--api_url": (
        API_URL
        + "?$where=tpep_pickup_datetime >= '{{ data_interval_start | ds }}T00:00:00' "
        "AND tpep_pickup_datetime < '{{ (data_interval_start + macros.dateutil.relativedelta.relativedelta(months=1)) | ds }}T00:00:00'"
         "&$order=tpep_pickup_datetime"
    ),
    "--api_start_date": "{{ data_interval_start | ds }}",
    "--api_end_date": "{{ (data_interval_start + macros.dateutil.relativedelta.relativedelta(months=1) - macros.timedelta(days=1)) | ds }}",
    "--ingest_date": "{{ (data_interval_start + macros.dateutil.relativedelta.relativedelta(months=1)) | ds }}",
    "--max_rows": "200000",
},
    )

    # Task to run the Glue job for data transformation
    transform_data = GlueJobOperator(
        task_id="transform_data",
        job_name="ny_yellow_taxi_data_transformation",
        script_location=f"s3://{SCRIPTS_BUCKET_NAME}/glue_scripts/api_data_transformation_job.py",
        region_name="us-east-1",
        iam_role_name="city-data-project-glue-role",
        s3_bucket=f"{SCRIPTS_BUCKET_NAME}",
        create_job_kwargs={
            "GlueVersion": "3.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
        },
        script_args={
            "--target_path": f"s3://{BUCKET_NAME}/landing_zone/transformation_results/transformed_city_taxi_data",

        
            "--source_path": f"s3://{BUCKET_NAME}/landing_zone/city_taxi_data/api_data_*/",
            "--zone_lookup_path": f"s3://{BUCKET_NAME}/reference/taxi_zone_lookup.csv",
            "--api_url": API_URL,
            "--api_start_date": "{{ data_interval_start | ds }}",
            "--api_end_date": "{{ (data_interval_start + macros.dateutil.relativedelta.relativedelta(months=1) - macros.timedelta(days=1)) | ds }}",
            "--ingest_date": "{{ (data_interval_start + macros.dateutil.relativedelta.relativedelta(months=1)) | ds }}",
        },
    )

    # Task to perform data quality checks
    data_quality_check = GlueDataQualityRuleSetEvaluationRunOperator(
    task_id="data_quality_check",
    datasource={
        "GlueTable": {
            "DatabaseName": "ny_taxi_db",
            "TableName": "clean_trips",
        }
    },
    role="arn:aws:iam::563649768086:role/city-data-project-glue-role",
    rule_set_names=["ny_yellow_taxi_data_quality_ruleset"],
    region_name="us-east-1",
)

    end = EmptyOperator(task_id="end")

    # Define the task dependencies
    start >> extract_data >> transform_data >> data_quality_check >> end


ny_yellow_taxi_data_pipeline()