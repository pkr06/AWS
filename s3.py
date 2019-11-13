import logging
import boto3
from botocore.exceptions import ClientError


class S3Example:
    def __init__(self):
        pass

    def _bucket_exists(self,bucket_name):
        s3 = boto3.client('s3')
        try:
            response = s3.head_bucket(Bucket=bucket_name, region_name='ap-northeast-1')
        except ClientError as e:
            logging.debug(e)
            return False
        return True

    def _copy_object(self, src_bucket_name, src_object_name,
                    dest_bucket_name, dest_object_name=None):

        copy_source = {'Bucket': src_bucket_name, 'Key': src_object_name}
        if dest_object_name is None:
            dest_object_name = src_object_name

        # Copy the object
        s3 = boto3.client('s3')
        try:
            s3.copy_object(CopySource=copy_source, Bucket=dest_bucket_name,
                           Key=dest_object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True


    def run(self):
        test_bucket_name = 'data_for_test'

        logging.basicConfig(level=logging.DEBUG,
                            format='%(levelname)s: %(asctime)s: %(message)s')

        # Check if the bucket exists
        if self._bucket_exists(test_bucket_name):
            logging.info(f'{test_bucket_name} exists and you have permission to access it.')
        else:
            logging.info(f'{test_bucket_name} does not exist or '
                         f'you do not have permission to access it.')


if __name__ == '__main__':
    s3 = S3Example()
    s3.run()