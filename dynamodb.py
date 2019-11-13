from __future__ import print_function # Python 2/3 compatibility
import boto3



class DynamoDBTest:

    def __init__(self):
        self._dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1', endpoint_url="http://localhost:8000")


    def _create_table(self):
        table = self._dynamodb.create_table(
            TableName='Order',
            KeySchema=[
                {
                    'AttributeName': 'order_id',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'ts',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'order_id',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'ts',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        return table


    def _delete_table(self):
        table = self._dynamodb.Table('Order')

        table.delete()
