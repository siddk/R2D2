"""
svo2stereo.py

Standalone script that iterates through the exhaustive set of trajectory metadata (`s3://r2d2-data/lab-uploads-json`)
and pulls out left/right SVOs to create & export stereo MP4s. Downloads them to disk & uses the ZED SDK to fuse, then
uploads back to the S3 bucket.

Prior to running --> run `aws configure sso` for the R2D2 PowerUser Bucket (Sandbox 7691) and set the profile name
to `r2d2-poweruser`
"""
import json
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryFile
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

from r2d2.postprocessing.util.svo2mp4 import export_mp4

# Constants
BUCKET, SRC, RAW_DATA = "r2d2-data", "lab-uploads-json", "lab-uploads"
CACHE_FILE = Path("scripts/annotation/X-STEREO-CONVERT-CACHE.pkl")
REBUILD_CACHE = False


def utcnow() -> str:
    return datetime.now(tz=timezone.utc).strftime("%m/%d/%Y, %H:%M:%S")


def s3upload(stereo_mp4: Path, name: str, client: "boto3.Client") -> Tuple[bool, Optional[str]]:
    try:
        client.upload_file(str(stereo_mp4), BUCKET, name)
        return True, None

    except ClientError as e:
        print(f"[*] Upload Error --> {e}")
        return False, str(e)


def full_svo2stereo() -> None:
    print(f"[*] Bulk Converting SVO -> Stereo MP4 (stitched left/right) for all Demos in s3://{BUCKET}/{RAW_DATA}")

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

    # Stage 2 :: Iterate through `cache["new"]` and handle exports from SVO...
    os.makedirs(tmp := Path("/tmp/r2d2-stereo-conversion"), exist_ok=True)
    try:
        n_success, n_errored = 0, 0
        pbar_desc = "[*] Iterating though Trajectories => ({n_success} Successful / {n_errored} Errored)"
        progress = tqdm(pbar_desc.format(n_success=0, n_errored=0), total=len(list(cache["new"].keys())))
        for fpath in list(cache["new"].keys()):
            try:
                # Download Metadata --> parse left/right SVO paths (in Bucket)
                with TemporaryFile() as mfile:
                    client.download_fileobj(BUCKET, fpath, mfile)
                    mfile.seek(0)
                    metadata = json.loads(mfile.read().decode("utf-8"))

                # Filter out "failure" trajectories
                if not metadata["success"]:
                    cache["failure-trajectories"][fpath] = utcnow()
                    cache["new"].pop(fpath)

                    # Update Progress (treat as "success")
                    n_success += 1
                    progress.set_description(pbar_desc.format(n_success=n_success, n_errored=n_errored))
                    progress.update()

                    continue

                # Otherwise, grab {left.mp4, right.mp4} *relative paths* (from `s3://r2d2-data/lab-uploads/{LAB}`)
                s3_lab_prefix = Path(RAW_DATA) / metadata["lab"]
                left_mp4_relpath, right_mp4_relpath = metadata["left_mp4_path"], metadata["right_mp4_path"]
                wrist_mp4_relpath = metadata["wrist_mp4_path"]

                left_svo_relpath = left_mp4_relpath.replace("MP4", "SVO").replace(".mp4", ".svo")
                right_svo_relpath = right_mp4_relpath.replace("MP4", "SVO").replace(".mp4", ".svo")
                wrist_svo_relpath = wrist_mp4_relpath.replace("MP4", "SVO").replace(".mp4", ".svo")

                # Download SVOs to `tmp` --> use ZED SDK to pull out left/right stereo pairs & fuse!
                client.download_file(BUCKET, str(s3_lab_prefix / left_svo_relpath), tmp_left := (tmp / "left.svo"))
                client.download_file(BUCKET, str(s3_lab_prefix / right_svo_relpath), tmp_right := (tmp / "right.svo"))
                client.download_file(BUCKET, str(s3_lab_prefix / wrist_svo_relpath), tmp_wrist := (tmp / "wrist.svo"))

                # Export "both" Stereo Images to a single MP4 (Side-by-Side) layout!
                left_success = export_mp4(tmp_left, tmp, stereo_view="both")
                right_success = export_mp4(tmp_right, tmp, stereo_view="both")
                wrist_success = export_mp4(tmp_wrist, tmp, stereo_view="both")
                if not (left_success and right_success and wrist_success):
                    print(
                        "\t=>> Failure to Convert SVO -- "
                        f"(Left: {left_success}, Right: {right_success}, Wrist: {wrist_success})!"
                    )
                    cache["errored"][fpath] = [
                        utcnow(),
                        (
                            "Error: Failure to Convert SVO -- "
                            f"(Left: {left_success}, Right: {right_success}, Wrist: {wrist_success})!"
                        ),
                    ]
                    cache["new"].pop(fpath)

                    # Update Progress
                    n_errored += 1
                    progress.set_description(pbar_desc.format(n_success=n_success, n_errored=n_errored))
                    progress.update()

                    continue

                # Upload to S3
                left_s3_path = s3_lab_prefix / left_mp4_relpath.replace(".mp4", "-stereo.mp4")
                right_s3_path = s3_lab_prefix / right_mp4_relpath.replace(".mp4", "-stereo.mp4")
                wrist_s3_path = s3_lab_prefix / wrist_mp4_relpath.replace(".mp4", "-stereo.mp4")

                left_maybe_success, left_err = s3upload(tmp / "left-stereo.mp4", str(left_s3_path), client)
                right_maybe_success, right_err = s3upload(tmp / "right-stereo.mp4", str(right_s3_path), client)
                wrist_maybe_success, wrist_err = s3upload(tmp / "wrist-stereo.mp4", str(wrist_s3_path), client)

                # Update Cache
                if left_maybe_success and right_maybe_success and wrist_maybe_success:
                    cache["exported"][fpath] = utcnow()
                    cache["new"].pop(fpath)

                    # Update Progress
                    n_success += 1
                    progress.set_description(pbar_desc.format(n_success=n_success, n_errored=n_errored))
                    progress.update()

                else:
                    print(
                        "\t=>> Failure to Upload MP4 -- "
                        f"(Left: {left_maybe_success}, Right: {right_maybe_success}, Wrist: {wrist_maybe_success})!"
                    )
                    cache["errored"][fpath] = [
                        utcnow(),
                        (
                            "Error: Failure to Upload MP4 -- "
                            f"(Left: {left_maybe_success}, Right: {right_maybe_success}, Wrist: {wrist_maybe_success})!"
                        ),
                    ]
                    cache["new"].pop(fpath)

                    # Update Progress
                    n_errored += 1
                    progress.set_description(pbar_desc.format(n_success=n_success, n_errored=n_errored))
                    progress.set_description()

            except Exception as e:
                print(f"[*] Error --> {e}")

                # Caught some error --> log & continue!
                cache["errored"][fpath] = [utcnow(), f"Error: {e}"]
                cache["new"].pop(fpath)
                continue

    finally:
        print("[*] Terminating... Writing Cache to Disk!")
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(cache, f)


if __name__ == "__main__":
    full_svo2stereo()