# Process Segmentation Stage 2 Outputs

This folder contains the reproducible Stage 2 workflow built on the finalized cleaned dataset from `sensor_mapping_outputs`.

## Files

- `generate_process_segmentation_outputs.py`: reproducible script for this stage.
- `results/analysis_dataset.csv`: unique-timestamp analysis dataset used for all downstream steps.
- `results/windowed_features.csv`: sliding window features built from the Stage 2 analysis dataset.
- `results/phase_labels.csv`: per-window phase labels with quality gates for stable-run candidates.
- `results/phase_segments.csv`: contiguous phase segments after smoothing.
- `results/stable_runs.csv`: extracted stable operating runs.
- `results/stable_run_windows.csv`: window-level rows assigned to stable runs.
- `results/profile_clusters.csv`: stable runs with cluster assignments.
- `results/cluster_summary.csv`: per-cluster summary of run profiles.
- `results/overview.json`: machine-readable run of this stage.

## Inputs Used

- Cleaned dataset rows: 30858
- Analysis rows after duplicate timestamp collapse: 30849
- Duplicate timestamp groups collapsed for Stage 2: 9
- Extra rows removed by Stage 2 timestamp collapse: 9

## Modeling Sensors Used

- Core sensors: Val_1, Val_5, Val_6, Val_7, Val_8, Val_9, Val_10, Val_11, Val_27, Val_28, Val_29, Val_30, Val_31, Val_32
- Supporting sensors: Val_2, Val_3, Val_4, Val_19, Val_20, Val_33
- Excluded sensors remain out of this stage: Val_12, Val_14, Val_15, Val_21, Val_22, Val_23, Val_34, Val_35, Val_36, Val_37, Val_38, Val_39, Val_40, Val_41, Val_42, Val_43, Val_44, Val_45, Val_46, Val_47, Val_48

## Windowing Configuration

- Window size: 15 rows
- Window step: 5 rows
- Nominal sample period: 60 seconds
- Total windows created: 6167

## Stable Run Configuration

- Speed OFF threshold: 0.5
- Speed ON threshold: 5.0
- Stable speed minimum: 20.0
- Stable speed delta limit: 8.0
- Stable temperature delta limit: 2.0
- Max core invalid fraction: 0.05
- Max core outlier fraction: 0.2
- Min stable windows: 3
- Min stable duration (minutes): 20.0
- Max bridged quality-gap windows inside a stable phase: 3
- Stable phase segments detected before run filtering: 17
- Stable runs retained after filtering: 13

## Phase Segments Preview

| phase_segment_id | phase_label | window_count | duration_minutes   | start_time               | end_time                 | mean_speed          |
| ---------------- | ----------- | ------------ | ------------------ | ------------------------ | ------------------------ | ------------------- |
| 1                | off         | 20           | 107.06666666666666 | 2026-02-16 07:05:36+0000 | 2026-02-16 08:52:40+0000 | 0.05433333387666667 |
| 2                | transition  | 1            | 14.0               | 2026-02-16 08:43:40+0000 | 2026-02-16 08:57:40+0000 | 1.0399999719333333  |
| 3                | ramp_up     | 6            | 38.016666666666666 | 2026-02-16 08:48:40+0000 | 2026-02-16 09:26:41+0000 | 54.902222886855554  |
| 4                | ramp_down   | 1            | 14.0               | 2026-02-16 09:17:41+0000 | 2026-02-16 09:31:41+0000 | 101.89333295333334  |
| 5                | transition  | 1            | 14.016666666666667 | 2026-02-16 09:22:41+0000 | 2026-02-16 09:36:42+0000 | 96.75333307999999   |
| 6                | ramp_up     | 2            | 19.016666666666666 | 2026-02-16 09:27:41+0000 | 2026-02-16 09:46:42+0000 | 108.51666540333332  |
| 7                | stable_run  | 37           | 192.11666666666667 | 2026-02-16 09:37:42+0000 | 2026-02-16 12:49:49+0000 | 113.94288291351353  |
| 8                | ramp_down   | 1            | 14.0               | 2026-02-16 12:40:49+0000 | 2026-02-16 12:54:49+0000 | 117.78666673333333  |
| 9                | transition  | 2            | 18.016666666666666 | 2026-02-16 12:45:49+0000 | 2026-02-16 13:03:50+0000 | 113.98333349999999  |
| 10               | stable_run  | 32           | 166.1              | 2026-02-16 12:55:49+0000 | 2026-02-16 15:41:55+0000 | 111.04479158333334  |

## Stable Runs Preview

| stable_run_id | window_count | duration_minutes   | start_time               | end_time                 | mean_Val_1         | mean_Val_5        | mean_Val_6         | front_temp_mean    | rear_temp_mean     |
| ------------- | ------------ | ------------------ | ------------------------ | ------------------------ | ------------------ | ----------------- | ------------------ | ------------------ | ------------------ |
| 1             | 37           | 192.11666666666667 | 2026-02-16 09:37:42+0000 | 2026-02-16 12:49:49+0000 | 114.39179486153847 | 74.8102563323077  | 370.09538433846154 | 172.96964098871797 | 174.85324785299144 |
| 2             | 32           | 166.1              | 2026-02-16 12:55:49+0000 | 2026-02-16 15:41:55+0000 | 111.12352936470589 | 72.58823543117647 | 373.2364711882353  | 172.98305879529414 | 174.83333399509803 |
| 3             | 5            | 34.016666666666666 | 2026-02-16 15:57:56+0000 | 2026-02-16 16:31:57+0000 | 118.46857117142858 | 77.39714268       | 377.7171421714286  | 172.9474285885714  | 174.83761944761903 |
| 4             | 33           | 174.1              | 2026-02-16 22:03:10+0000 | 2026-02-17 00:57:16+0000 | 110.47314309142857 | 72.21028560114287 | 353.55657207999997 | 172.9940570605714  | 174.8238095809524  |
| 5             | 175          | 873.55             | 2026-02-17 01:13:17+0000 | 2026-02-17 15:46:50+0000 | 106.51480216214688 | 69.66406776779661 | 366.6754802124293  | 172.9894011143503  | 174.84273085103578 |
| 6             | 586          | 2892.8166666666666 | 2026-02-17 15:47:50+0000 | 2026-02-19 16:00:39+0000 | 106.6761905202381  | 69.74221087394558 | 382.60431956870747 | 172.99675506857145 | 174.8671887909297  |
| 7             | 30           | 155.1              | 2026-02-19 16:01:39+0000 | 2026-02-19 18:36:45+0000 | 107.81312483125001 | 70.4512496925     | 389.766875075      | 173.03300003       | 174.87208415833334 |
| 8             | 376          | 1856.1666666666667 | 2026-02-19 18:52:45+0000 | 2026-02-21 01:48:55+0000 | 105.67523818650793 | 69.09261905992064 | 392.7130156936508  | 172.99534393280422 | 174.8457589681658  |
| 9             | 112          | 562.35             | 2026-02-21 01:53:56+0000 | 2026-02-21 11:16:17+0000 | 104.8015789308772  | 68.54438584842106 | 396.2678940210526  | 172.99021049157892 | 174.8664919631579  |
| 10            | 1456         | 7174.5             | 2026-02-23 07:29:57+0000 | 2026-02-28 07:04:27+0000 | 98.45037723170783  | 64.34980108279836 | 385.19723602277094 | 173.58551159832646 | 174.70085300046867 |

## Cluster Summary

| cluster_id | stable_run_count | total_duration_minutes | mean_speed         | mean_load         | mean_pressure      | mean_front_temp    | mean_rear_temp     | mean_temperature_spread |
| ---------- | ---------------- | ---------------------- | ------------------ | ----------------- | ------------------ | ------------------ | ------------------ | ----------------------- |
| 1          | 2                | 7400.65                | 96.18464513531043  | 62.89511795270353 | 382.8312270331246  | 173.79627746916321 | 174.68602455186476 | 7.3794148985477435      |
| 2          | 4                | 5466.433333333333      | 106.24153311721831 | 69.45761636869682 | 390.3380260896027  | 173.00382738073864 | 174.86288097014668 | 7.510864981045996       |
| 3          | 2                | 5187.283333333334      | 109.49480782723901 | 71.54837900316622 | 376.5790459997253  | 173.97036448575824 | 174.66229781521747 | 7.370734793681319       |
| 4          | 4                | 1405.8666666666666     | 110.62581736995494 | 72.31821128310591 | 365.89097695478154 | 172.98403948973345 | 174.8382805700194  | 7.475548152458489       |
| 5          | 1                | 34.016666666666666     | 118.46857117142858 | 77.39714268       | 377.7171421714286  | 172.9474285885714  | 174.83761944761903 | 7.917145171428574       |

## Collapsed Duplicate Timestamps Preview

| analysis_row_id | TrendDate                | source_row_count | source_idx_first | source_idx_last |
| --------------- | ------------------------ | ---------------- | ---------------- | --------------- |
| 5971            | 2026-02-20 09:05:17+0000 | 2                | 5971             | 5972            |
| 7634            | 2026-02-21 12:24:19+0000 | 2                | 7635             | 7636            |
| 10996           | 2026-02-23 19:26:24+0000 | 2                | 10998            | 10999           |
| 15994           | 2026-02-27 05:30:29+0000 | 2                | 15997            | 15998           |
| 17602           | 2026-02-28 07:50:29+0000 | 2                | 17606            | 17607           |
| 27531           | 2026-03-07 02:46:40+0000 | 2                | 27536            | 27537           |
| 27555           | 2026-03-07 03:10:41+0000 | 2                | 27561            | 27562           |
| 29221           | 2026-03-08 06:28:43+0000 | 2                | 29228            | 29229           |
| 30836           | 2026-03-09 09:00:43+0000 | 2                | 30844            | 30845           |
