from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class MyDynamoDBHook(AwsBaseHook):

    def __init__(self, *args, table_keys=None, table_name=None, **kwargs) -> None:
        self.table_keys = table_keys
        self.table_name = table_name
        kwargs["resource_type"] = "dynamodb"
        super().__init__(*args, **kwargs)

    def get_items(self, data):
        try:
            items = []
            table = self.get_conn().Table(self.table_name)
            for k,v in data:
                response = table.get_item(
                    Key={
                        k: v
                    }
                )
            items.append(response['Item'])
            return items
        except Exception as e:
            raise AirflowException(f"Failed to retrieve items from dynamodb, error: {str(e)}")

    def write_batch_data(self, items) -> bool:
        try:
            table = self.get_conn().Table(self.table_name)
            with table.batch_writer(overwrite_by_pkeys=self.table_keys) as batch:
                for item in items:
                    batch.put_item(Item=item)
            return True
        except Exception as general_error:
            raise AirflowException(f"Failed to insert items in dynamodb, error: {str(general_error)}")