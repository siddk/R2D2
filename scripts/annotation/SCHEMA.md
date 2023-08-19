# R2D2 Dataset Schema

This is a general note for anyone trying to use the R2D2 dataset, either by pulling directly from S3, or via one of the
mirrors (e.g., the RT-X GCP bucket).

The uploaded data has the following structure -- note that depending on where/when you retrieve the data, some of the
uploaded trajectories **may not be complete/valid**. The invariant you can trust is that all "complete/valid"
trajectories contain a `metadata_<uuid>.json`; asserting this file exists will guarantee you are only working with
"good" trajectories.

## Directory Structure

The R2D2 trajectories are structured as follows:

```bash
- <lab>/
  - success/ (Contains the "successful" trajectories as designated by the data collector)
    - <YYYY>-<MM>-<DD>/ (Corresponds to the day the trajectory was collected on)
      - <Day>_<Mon>_<DD>_<HH>:<MM>:<SS>_<YYYY>/ (Path to an individual trajectory directory)
        - metadata_<uuid>.json  (Metadata associated with the given trajectory -- see below)
        - trajectory.h5 (HDF5 file containing states, actions, camera data as time series)
        - recordings/
          - SVO/ (Directory with videos for each of the 3 cameras in the proprietary ZED format)
            - [Wrist] <serial>.svo
            - [External 1] <serial>.svo
            - [External 2] <serial>.svo
          - MP4/ (Directory with MP4s for each of the 3 cameras -- converted from the SVO files post-hoc)
            - [Wrist] <serial>.mp4
            - [External 1] <serial>.mp4
            - [External 2] <serial>.mp4

  - failure/ (Contains "failed" trajectories -- e.g., unsuccessful trajectories, corrupt/interrupted trajectories)
    - ... (follows same structure as above)
```

### JSON Metadata

Each directory associated with a trajectory (e.g., `CLVR/success/2023-05-15/Mon_May_15_00:51:15_2023/`) has a JSON file
that stores all relevant metadata (`metadata_<uuid>.json`). A `uuid` is formatted as
`<lab>+<collector_id>+<YYYY>-<MM>-<DD>-<HH>h-<MM>m-<SS>s` where `collector_id` is an 8-character alphanumeric string
serving as the unique ID for a given data collector.

The JSON file has the following structure:

```
{
  "uuid": string,
  "lab": string,
  "user": string,
  "user_id": string,
  "date": string (YYYY-MM-DD),
  "timestamp": string (YYYY-MM-DD-<HH>h-<MM>m-<SS>s),
  "hdf5_path": string (Relative to <lab>/ -- e.g., "success/2023-05-14/Mon_May_15_00:51:15_2023/trajectory.h5",
  "building": string,
  "scene_id": int,
  "success": bool,
  "robot_serial": string,
  "r2d2_version": string (follows semantic versioning -- e.g., "1.1"),
  "current_task": string,
  "trajectory_length": int,
  "wrist_cam_serial": str,
  "ext1_cam_serial": str,
  "wrist_cam_extrinsics": array<float> (6-Dof [pos; rot] relative to base; rot is expressed as Euler("xyz")),
  "ext1_cam_extrinsics": array<float>,
  "ext2_cam_extrinsics": array<float>,
  "wrist_svo_path": string (Relative to <lab>/ -- e.g., "success/2023-05-15/Mon_May_15_00:51:15_2023/recordings/SVO/16787047.svo")
  "wrist_mp4_path": string (Relative to <lab>/ -- e.g., "success/2023-05-15/Mon_May_15_00:51:15_2023/recordings/MP4/16787047.mp4")
  "ext1_svo_path": string,
  "ext1_mp4_path": string,
  "ext2_svo_path": string,
  "ext2_mp4_path": string,
  "left_mp4_path": string (One of `ext1_mp4_path` or `ext2_mp4_path`, "left" computed from extrinsics `pos`),
  "right_mp4_path": string
}
```

### HDF5 Structure

All timeseries data assumes a fixed control frequency of 15 Hz. Each `trajectory.h5` file is structured as follows:

```
- .attrs (Metadata)
    + building: string
    + scene_id: int
    + user: string ("First Last")
    + current_task: string,
    + fixed_tasks: List[string],
    + new_tasks: Optional[List[string]],
    + failure: bool,
    + success: bool,
    + robot_serial_number: string,
    + time: string ("<DAY>_<MON>_<DD>_<HH>:<MM>:<SS>_<YYYY>")
    + version_number: float

- action/ (T = `trajectory_length`)
    + cartesian_position: Array[T, 6]
    + cartesian_velocity: Array[T, 6]
    + gripper_position: Array[T,]
    + gripper_velocity: Array[T,]
    + joint_position: Array[T, 7]
    + joint_velocity: Array[T, 7]
    + target_cartesian_position: Array[T, 6]
    + target_gripper_position: Array[T,]

- observation/
    - camera_extrinsics/
        + <cam_serial_1>_left: Array[T, 6]
        + <cam_serial_1>_left_gripper_offset: Array[T, 6]
        + <cam_serial_1>_right: Array[T, 6]
        + <cam_serial_1>_right_gripper_offset: Array[T, 6]
        + <cam_serial_2>_left: Array[T, 6]
        + <cam_serial_2>_right: Array[T, 6]
        + <cam_serial_3>_left: Array[T, 6]
        + <cam_serial_3>_right: Array[T, 6]

    - camera_type/
        + <cam_serial_[1..3]>: Array[T,] -- (0 means Wrist, 1 means External)

    - controller_info/
        + controller_on: Array[T,]
        + movement_enabled: Array[T,]
        + failure: Array[T,]
        + success: Array[T,]

    - robot_state/
        + cartesian_position: Array[T, 6]
        + gripper_position: Array[T,]
        + joint_positions: Array[T, 7]
        + joint_torques_computed: Array[T, 7]
        + joint_velocities: Array[T, 7]
        + motor_torques_measured: Array[T, 7]
        + prev_command_successful: Array[T, 7]
        + prev_controller_latency_ms: Array[T, 7]
        + prev_joint_torques_computed: Array[T, 7]
        + prev_joint_torques_computed_safened: Array[T, 7]

    - timestamp/
        + skip_action: Array[T,]

        - cameras/
            + <cam_serial_[1..3]>_estimated_capture: Array[T,]
            + <cam_serial_[1..3]>_frame_received: Array[T,]
            + <cam_serial_[1..3]>_read_start: Array[T,]
            + <cam_serial_[1..3]>_read_end: Array[T,]

        - control/
            + control_start: Array[T,]
            + policy_start: Array[T,]
            + sleep_start: Array[T,]
            + step_start: Array[T,]
            + step_end: Array[T,]

        - robot_state/
            + read_start: Array[T,]
            + read_end: Array[T,]
            + robot_timestamp_nanos: Array[T,]
            + robot_timestamp_seconds: Array[T,]
```
