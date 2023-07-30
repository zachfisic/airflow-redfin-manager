from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class MySqsHook(AwsBaseHook):

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "sqs"
        super().__init__(*args, **kwargs)

    def get_queue_url(self, queue_name):
        return self.get_conn().get_queue_url(QueueName=queue_name)