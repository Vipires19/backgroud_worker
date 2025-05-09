import boto3
from botocore.client import Config
import streamlit as st
import os

R2_KEY = os.environ.get("R2_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
ENDPOINT_URL = os.environ.get("ENDPOINT_URL")
BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")


def get_r2_client(R2_KEY, R2_SECRET_KEY, ENDPOINT_URL):
    return boto3.client(
        's3',
        aws_access_key_id=R2_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
        config=Config(signature_version='s3v4')
    )

def upload_to_r2(client, file_path, r2_bucket, object_name):
    
    with open(file_path, "rb") as f:
        client.upload_fileobj(f, r2_bucket, object_name)
