"""
json2stitched.py

Standalone script that iterates through the exhaustive set of trajectory metadata (`s3://r2d2-data/lab-uploads-json`)
and pulls out left/right MP4s to stitch together. Downloads them to disk, runs `ffmpeg` to fuse, and uploads back to
S3 bucket (`s3://r2d2-data/fused-annotation-mp4s`).
"""
import json
import os
import pickle
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

# Constants
BUCKET, SRC, DST, RAW_DATA = "r2d2-data", "lab-uploads-json", "fused-annotation-mp4s", "lab-uploads"
CACHE_FILE = Path("scripts/annotation/X-CONVERT-CACHE.pkl")
REBUILD_CACHE = False


def utcnow() -> str:
    return datetime.now(tz=timezone.utc).strftime("%m/%d/%Y, %H:%M:%S")


def s3upload(fused_mp4: Path, name: str, client: "boto3.Client") -> Tuple[bool, Optional[str]]:
    try:
        client.upload_file(str(fused_mp4), BUCKET, name)
        return True, None

    except ClientError as e:
        print(f"[*] Upload Error --> {e}")
        return False, str(e)


def json2stitched() -> None:
    print(f"[*] Bulk Converting Fused MP4s from Metadata in s3://{BUCKET}/{SRC} --> s3://{BUCKET}/{DST}")

    # Initialize S3 Client
    session = boto3.Session(profile_name="r2d2-poweruser")
    client = session.client("s3")

    # Create Temporary Directory & Index/Cache Structures =>> Note: Cache maps "keys" to access/modification time (UTC)
    cache = {"new": {}, "exported": {}, "failure-trajectories": {}, "errored": {}}
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "rb") as f:
            cache = pickle.load(f)

    # Step 1 (if `REBUILD_CACHE`) :: Iterate through all JSON files to build a local index/cache
    if REBUILD_CACHE:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET, Prefix=SRC)
        progress = tqdm("[*] Iterating through Metadata JSON")
        for page in pages:
            for obj in page["Contents"]:
                # Either parent prefix directory or errant `timestamps` file (from *old* R2D2 version)... skip!
                if (fpath := obj["Key"]).endswith("_timestamps.json") or not fpath.endswith(".json"):
                    continue

                # Add to `cache` depending on status...
                assert fpath.endswith(".json"), f"Unexpected file in `s3://{BUCKET}/{SRC}` --> {fpath}!"
                if not (fpath in cache["exported"] or fpath in cache["errored"]):
                    cache["new"][fpath] = utcnow()

                # Update Progress Bar
                progress.update()

        # Write Cache
        print("Done Rebuilding Cache... Writing Cache to Disk!")
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(cache, f)

    # Stage 2 :: Iterate through `cache["new"]` and handle exports...
    os.makedirs(tmp := Path("/tmp/r2d2-conversion"), exist_ok=True)
    try:
        # for fpath in tqdm(list(cache["new"].keys())m desc="[*] Iterating through Cached Metadata Files"):
        for fpath in tqdm(list(cache["errored"].keys()), desc="[*] Iterating through Cached Metadata Files"):
            try:
                # Download Metadata --> parse left/right MP4 paths (in Bucket)
                with TemporaryFile() as mfile:
                    client.download_fileobj(BUCKET, fpath, mfile)
                    mfile.seek(0)
                    metadata = json.loads(mfile.read().decode("utf-8"))

                # Filter out "failure" trajectories
                if not metadata["success"]:
                    cache["failure-trajectories"][fpath] = utcnow()
                    # cache["new"].pop(fpath)
                    cache["errored"].pop(fpath)
                    continue

                # Otherwise, grab {left.mp4, right.mp4} *relative paths* (from `s3://r2d2-data/lab-uploads/{LAB}`)
                s3_lab_prefix = Path(RAW_DATA) / metadata["lab"]
                left_mp4_relpath, right_mp4_relpath = metadata["left_mp4_path"], metadata["right_mp4_path"]

                # Download MP4s to `tmp` --> run FFMPEG `hstack` to fuse...
                client.download_file(BUCKET, str(s3_lab_prefix / left_mp4_relpath), tmp_left := (tmp / "left.mp4"))
                client.download_file(BUCKET, str(s3_lab_prefix / right_mp4_relpath), tmp_right := (tmp / "right.mp4"))
                subprocess.run(
                    f"ffmpeg -y -i {tmp_left!s} -i {tmp_right!s} -vsync 2 -filter_complex hstack {tmp / 'fused.mp4'!s}",
                    shell=True,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )

                # Upload to S3 "Annotation" Bucket
                fused_mp4_name = str(Path(DST) / f"{Path(fpath).stem.split('_')[1]}.mp4")
                maybe_success, err_msg = s3upload(tmp / "fused.mp4", fused_mp4_name, client)

                # Update Cache
                if maybe_success:
                    cache["exported"][fpath] = utcnow()
                    # cache["new"].pop(fpath)
                    cache["errored"].pop(fpath)

                else:
                    cache["errored"][fpath] = [utcnow(), f"Error: {err_msg}"]
                    # cache["new"].pop(fpath)

            except Exception as e:
                print(f"[*] Error --> {e}")

                # Caught some error --> log & continue!
                cache["errored"][fpath] = [utcnow(), f"Error: {e}"]
                # cache["new"].pop(fpath)
                continue

    finally:
        print("[*] Terminating... Writing Cache to Disk!")
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(cache, f)


if __name__ == "__main__":
    json2stitched()
