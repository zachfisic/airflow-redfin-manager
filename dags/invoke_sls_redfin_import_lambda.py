import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from hooks.MySqsHook import MySqsHook
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

LAMBDA_FN = "sls-redfin-import-lambda-dev-import_raw"
TEST_EVENT = {
    "event_datetime": datetime.now(),
    "zip_codes": ["29601","29603","29607","29609","29611"]
}


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

@task
def get_queue():
    queue_url = MySqsHook().get_queue_url(queue_name='zip_code_queue.fifo')
    print(queue_url)

@task
def pull_messages(ti) -> None:
    messages = ti.xcom_pull(key='messages', task_ids=['read_from_queue_in_batch'])
    if not messages:
        raise ValueError('No value currently stored in XComs.')
    print("messages: ", messages)

with DAG(
    dag_id='redfin_raw_data_import',
    schedule_interval=None,
    start_date=datetime(2023, 7, 20),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
) as dag:

    sqs_queue = get_queue()

    read_from_queue_in_batch = SqsSensor(
        task_id="read_from_queue_in_batch",
        sqs_queue=sqs_queue,
        # Get maximum 5 messages each poll
        max_messages=5,
        # 1 poll before returning results
        num_batches=1,
    )

    pull_test_messages = pull_messages()

    # invoke_lambda_function = LambdaInvokeFunctionOperator(
    #     task_id='invoke_lambda_function',
    #     function_name=LAMBDA_FN,
    #     payload=json.dumps(TEST_EVENT, cls=DateTimeEncoder)
    # )
    # test_python_operator_zip_code >> invoke_lambda_function
    
    chain(
        sqs_queue,
        read_from_queue_in_batch,
        pull_test_messages
    )