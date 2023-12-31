import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from hooks.MySqsHook import MySqsHook
from hooks.MyDynamoDBHook import MyDynamoDBHook
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import date

LAMBDA_FN = "sls-redfin-import-lambda-dev-import_raw"

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

def get_queue():
    response = MySqsHook().get_queue_url(queue_name='zip_code_queue.fifo')
    return response['QueueUrl']

    
@task
def extract_zips(ti=None):
    messages = ti.xcom_pull(key='messages', task_ids='read_from_queue_in_batch')
    zip_list = []
    if not messages:
        raise ValueError('No value currently stored in XComs.')
    for message in messages:
        content = json.loads(message['Body'])
        zip_list.append(content)

    ret = {"zips": zip_list}
    return json.dumps(ret)


@task
def update_run_date(ti=None):
    data = ti.xcom_pull(key='return_value', task_ids='extract_zips')
    data = json.loads(data)
    zips = [(k,v) for z in data['zips'] for k,v in z.items() if k == 'zip']
    ddb_hook = MyDynamoDBHook(table_keys=["zip"], table_name="zip_codes")
    items = ddb_hook.get_items(zips)
    for item in items:
        item["attempted"] = "Y"
        item["last_run_date"] = date.today().isoformat()

    print(items)
    ddb_hook.write_batch_data(items)
    


@task
def delete_messages(ti=None) -> None:
    messages = ti.xcom_pull(key='messages', task_ids='read_from_queue_in_batch')
    if not messages:
        raise ValueError('No value currently stored in XComs.')

    for message in messages:
        response = MySqsHook().delete_message(
            queue_url=get_queue(),
            receipt_handle=message['ReceiptHandle']
        )
        print(response)


with DAG(
    dag_id='redfin_raw_data_import',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2023, 7, 20),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
) as dag:

    # Gets messages from SQS and store them in xcom under key "messages"
    read_from_queue_in_batch = SqsSensor(
        task_id="read_from_queue_in_batch",
        sqs_queue=get_queue(),
        max_messages=5, # Get maximum 5 messages each poll
        num_batches=1,  # poll once before returning results
        delete_message_on_reception=False,
        timeout=60*60
    )

    extract_zips_from_xcom = extract_zips()

    invoke_lambda_function = LambdaInvokeFunctionOperator(
        task_id='invoke_lambda_function',
        function_name=LAMBDA_FN,
        payload="{{ ti.xcom_pull(key='return_value', task_ids='extract_zips') }}"
    )

    update_run_date_ddb = update_run_date()

    delete_test_messages = delete_messages()
    
    chain(
        read_from_queue_in_batch,
        extract_zips_from_xcom,
        invoke_lambda_function,
        update_run_date_ddb,
        delete_test_messages
    )