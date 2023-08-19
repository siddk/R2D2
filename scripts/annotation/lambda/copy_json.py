import urllib.parse
from pathlib import Path

import boto3

# Create S3 Client
s3 = boto3.client("s3")


def lambda_handler(event, context):
    src_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    src_json_key = urllib.parse.unquote(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")

    # Debug
    print(f"Source Key: {src_json_key}")
    print(f"Base Name: {Path(src_json_key).name}")

    try:
        _ = s3.copy_object(
            Bucket=src_bucket,
            CopySource=f"/{src_bucket}/{src_json_key}",
            Key=f"lab-json-dry-run/{Path(src_json_key).name}",
        )
        return True

    except Exception as e:
        print(f"Error getting object {src_json_key} from bucket {src_bucket}")
        raise e
