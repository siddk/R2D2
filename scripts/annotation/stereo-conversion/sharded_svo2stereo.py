"""
sharded_svo2stereo.py

Splits the work of `full_svo2stereo.py` by parallelizing over "shards" (derived from the CACHE built in the `full`
script). Takes as input a command line argument specifying path to X-SHARD-{i}.json, and iterates through selected
demonstrations, downloading the SVO files, exporting the side-by-side stereo MP4s, then re-uploading to S3.

Prior to running --> run `aws configure sso` for the R2D2 PowerUser Bucket (Sandbox 7691) and set the profile name
to `r2d2-poweruser`

Run (from current directory) with: `python sharded_svo2stereo.py X-SHARD-{i}.json`
"""
import json
import os
from argparse import ArgumentParser
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


def utcnow() -> str:
    return datetime.now(tz=timezone.utc).strftime("%m/%d/%Y, %H:%M:%S")


def s3upload(stereo_mp4: Path, name: str, client: "boto3.Client") -> Tuple[bool, Optional[str]]:
    try:
        client.upload_file(str(stereo_mp4), BUCKET, name)
        return True, None

    except ClientError as e:
        print(f"[*] Upload Error --> {e}")
        return False, str(e)


def sharded_svo2stereo(shard: Path) -> None:
    print(f"[*] Converting SVO -> Stereo MPR (stitched left/right) for all Demos in `{shard}`")

    # Initialize S3 Client
    session = boto3.Session(profile_name="r2d2-poweruser")
    client = session.client("s3")

    # Load Cache from Shard File
    #   Structure --> {"new": {...}, "exported": {...}, "failure-trajectories": {...}, "errored": {...}}
    with open(shard, "r") as f:
        cache = json.load(f)

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
        with open(shard, "w") as f:
            json.dump(cache, f)


if __name__ == "__main__":
    parser = ArgumentParser(description="Specify a path to a `X-SHARD-{i}.json file to process!")
    parser.add_argument("shard_filepath", type=str, help="Path to `X-SHARD-{i}.json file to process.")
    args = parser.parse_args()

    shard_file = Path(args.shard_filepath)
    assert shard_file.exists(), f"Shard File `{shard_file}` does not exist!"

    sharded_svo2stereo(shard_file)
