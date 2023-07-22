import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

LAMBDA_FN = "sls-redfin-import-lambda-dev-import_raw"
TEST_EVENT = { "event_datetime": datetime.now() }

with DAG(
    dag_id='redfin_raw_data_import',
    schedule_interval=None,
    start_date=datetime(2023, 7, 20),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
) as dag:
    invoke_lambda_function = LambdaInvokeFunctionOperator(
        task_id='invoke_lambda_function',
        function_name=LAMBDA_FN,
        payload=TEST_EVENT
    )