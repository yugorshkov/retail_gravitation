import io
import os

import boto3
from dotenv import load_dotenv

load_dotenv()
access_key = os.getenv("S3_ACCESS_KEY")
secret_key = os.getenv("S3_SECRET_KEY")
endpoint = os.getenv("S3_ENDPOINT_URL")


class S3:
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str = os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key: str = os.getenv("S3_SECRET_KEY"),
        endpoint_url: str = os.getenv("S3_ENDPOINT_URL"),
    ):
        self.session = boto3.Session(aws_access_key_id, aws_secret_access_key)
        self.client = self.session.client("s3", endpoint_url=endpoint_url)
        self.bucket = bucket_name

    def print_list_objects(self):
        response = self.client.list_objects(Bucket=self.bucket)
        for obj in response.get("Contents"):
            print(obj.get("Key"))

    def get_object_from_storage(self, obj):
        response = self.client.get_object(Bucket=self.bucket, Key=obj)
        return response["Body"]

    def put_gdf(self, gdf, file_name):
        f = io.BytesIO()
        gdf.to_parquet(f)
        self.client.put_object(Body=f.getvalue(), Bucket=self.bucket, Key=file_name)
