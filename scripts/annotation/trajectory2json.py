"""
trajectory2json.py

Standalone script that iterates through S3 bucket raw trajectory uploads (`s3://r2d2-data/lab-uploads`), extracting all
JSON files to a separate prefix (`s3://r2d2-data/lab-uploads-json`). Necessary for Athena + Dashboard updates.
"""
from pathlib import Path

import boto3
from tqdm import tqdm

# AUTOLab, CLVR, GuptaLab, ILIAD, IPRL, IRIS, PennPAL, RAD, RAIL, REAL, TRI
BUCKET, SRC, DST = "r2d2-data", "lab-uploads/TRI", "lab-uploads-json"


def trajectory2json() -> None:
    print(f"[*] Lifting JSON files from s3://{BUCKET}/{SRC} --> s3://{BUCKET}/{DST}")

    # Initialize S3 Client
    session = boto3.Session(profile_name="r2d2-poweruser")
    client = session.client("s3")

    # Iterate through all objects...
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=SRC)
    progress = tqdm(desc="[*] Iterating through Bucket")
    for page in pages:
        for obj in page["Contents"]:
            if obj["Key"].endswith(".json"):
                client.copy_object(
                    Bucket=BUCKET, CopySource=f"/{BUCKET}/{obj['Key']}", Key=f"{DST}/{Path(obj['Key']).name}"
                )

            progress.update()


if __name__ == "__main__":
    trajectory2json()
