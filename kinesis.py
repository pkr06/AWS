import boto3
from boto.kinesis.exceptions import ResourceInUseException
import os
import time, datetime
import threading
import pandas as pd

class KinesisProducer(threading.Thread):
    _service = "kinesis"
    _region = "ap-northeast-1"
    _sleep_interval = 60


    def __init__(self, stream_name, sleep_interval = 10, ip_addr = "8.8.8.8"):
        self._stream_name = stream_name
        self._sleep_interval = sleep_interval
        self._ip_addr = ip_addr
        self._client = boto3.client(self._service, region_name= self._region)
        self._create_stream()
        super().__init__()



    def put_record(self):
        timestamp = datetime.datetime.utcnow()
        part_key = self._ip_addr

        data = pd.read_csv ("data.csv", sep=",")
        shard_count = 1
        kinesis_records = []
        (rows, columns) = data.shape
        current_bytes = 0
        row_count = 0
        total_row_count = rows
        send_kinesis = False
        kinesis_shard_count = 1
        partition_key = 'partition_key'

        for _, row in data.iterrows():
            values = '|'.join(str(value) for value in row)
            encoded_vals = bytes(values, 'utf-8')


            kinesis_record = {
                "Data": encoded_vals,
                "PartitionKey": str(shard_count)
            }
            kinesis_records.append(kinesis_record)

            string_bytes = len(values.encode('utf-8'))
            current_bytes = current_bytes + string_bytes
            if len(kinesis_records) == 5000:
                send_kinesis = True

            if current_bytes > 5000:
                send_kinesis = True

            if row_count == total_row_count -1:
                send_kinesis = True

                if send_kinesis == True:
                    response = self._client.put_records(
                        Recrods = kinesis_records,
                        StreamName = self._stream_name
                    )

                    kinesis_records = []
                    send_kinesis = False
                    current_bytes = 0
                    shard_count += 1
                    if shard_count > kinesis_shard_count :
                        shard_count = 1

                    row_count += 1
                    print('Total record sent to Kinesis:  {0}'.format(total_row_count))

    def run_cotinuously(self):
        while True:
            self.put_record()
            time.sleep(self._sleep_interval)

    def run(self):
        try:
            if self._sleep_interval:
                self.run_cotinuously()
            else:
                self.put_record()
        except Exception as ex:
            print ('stream {} not found, exiting'.format(self._stream_name))

    def _create_stream(self):
        if self._get_status() == True:
            print ("stream {} already exists in region {}". format(self._stream_name, self._region) )
            return True

        try:
            self._client.create_stream(StreamName= self._stream_name, ShardCount=1)
            print('stream {} created in region {}'.format(self._stream_name, self._region))
        except ResourceInUseException:
            print('stream {} already exists in region {}'.format(self._stream_name, self._region))

        while self._get_status() != 'ACTIVE':
            time.sleep(1)

        print('stream {} is active'.format(self._stream_name))
        return True

    def _get_status(self):
        r = self._client.describe_stream(StreamName= self._stream_name)
        description = r.get('StreamDescription')
        status = description.get('StreamStatus')
        return status == 'ACTIVE'

def main():
    s_name = "test_stream"
    kn = KinesisProducer(s_name)
    kn.run()


main()