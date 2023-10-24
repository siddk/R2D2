"""
manifests.py

Create BATCH-{DATE} Manifest file by scanning directories, to better keep track of what UUIDs/Trajectories are currently
up for annotation via Tasq AI.

To upload data to Tasq's S3 Bucket do the following:
    - Run `json2stitched.py` to build mp4s `s3://r2d2-data/lab-uploads` --> `s3://r2d2-data/fused-annotation-mp4s`

    - Copy all MP4s to local disk - to directory `BATCH-{DATE}`
        1) `mkdir annotation-data/BATCH-{DATE}`
        2) `aws --profile r2d2-poweruser s3 cp --recursive
                s3://r2d2-data/fused-annotation-mp4s/ annotation-data/BATCH-{DATE}`

        TODO (siddk) :: Seems unnecessary, but permissions are... annoying?
        TODO (siddk) :: Need some way of *only* retrieving the new trajectories... probably convert to script?

    - Update `manifest.json` with all trajectories associated with the given batch
        + Update DATE constant below
        + Run `python scripts/annotation/manifests.py`

    - Upload all data from the `BATCH-{DATE}` directory to Tasq's S3 Bucket (`s3://tasq-pilot-1/ANNOTATION-BATCHES`)
        + `aws --profile tasq s3 cp --recursive
            annotation-data/BATCH-{DATE}/
            s3://tasq-pilot-1/ANNOTATION-BATCHES/BATCH-{DATE}/

        Note =>> Above should be one line and make sure to keep trailing slashes!
"""
import json
from pathlib import Path

# CONSTANTS (stand-in for command line arguments)
BATCH_DIRECTORY, DATE = Path("annotation-data"), "10-09-23"
MANIFEST_FILE = Path("annotation-data/manifest.json")


def create_manifest() -> None:
    print(f"[*] Updating Manifest with Batch `{DATE}`")

    # Create / Load Manifest --> `manifest` :: date (mm-dd-yy) -> {"total": int, "trajectories": [<UUID>.mp4]}
    manifest = {}
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, "r") as f:
            manifest = json.load(f)

    # Simple Validation --> No Overwriting!
    assert DATE not in manifest, f"Found date `{DATE}` in `manifest.json` --> will not overwrite!"

    # Update & Write Manifest
    batch_mp4s = sorted([mp4.name for mp4 in (BATCH_DIRECTORY / f"BATCH-{DATE}").iterdir() if mp4.suffix == ".mp4"])
    manifest[DATE] = {"total": len(batch_mp4s), "trajectories": batch_mp4s}
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=2)

    # Log & Terminate
    print(f"[*] Added Batch `{DATE}` to Manifest with {len(batch_mp4s)} Total Trajectories!")


if __name__ == "__main__":
    create_manifest()
