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
- `results/regime_coverage_summary.csv`: explicit low/mid/high stable-run coverage and short-vs-long counts.
- `results/regime_cause_attribution.csv`: evidence table for settings-vs-machine-behavior interpretation by regime.
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

- Window size: 5 rows
- Window step: 1 rows
- Nominal sample period: 60 seconds
- Total windows created: 30845

## Stable Run Configuration

- Speed OFF threshold: 0.5
- Speed ON threshold: 5.0
- Stable speed minimum: 20.0
- Stable speed delta limit: 8.0
- Stable temperature delta limit: 2.0
- Stable pressure/rpm CV limit: 0.1
- Stable load/pressure CV limit: 0.12
- Stable pressure/rpm delta limit: 0.6
- Stable load/pressure delta limit: 0.06
- Stable front-rear temp gap delta limit: 1.5
- Stable pressure/temperature delta limit: 0.8
- Max core invalid fraction: 0.05
- Max core outlier fraction: 0.2
- Min stable windows: 1
- Min stable duration (minutes): 4.0
- Max bridged quality-gap windows inside a stable phase: 1
- Stable phase segments detected before run filtering: 43
- Stable runs retained after filtering: 281

## Pressure Regimes

- Low: mean pressure below 280.0 bar
- Mid: mean pressure from 280.0 to 320.0 bar
- High: mean pressure above 320.0 bar
- Context-only signals kept out of stability/clustering: Val_19
- Cluster feature columns: mean_Val_1, mean_pressure_per_rpm, mean_load_per_pressure, mean_pressure_to_temperature, mean_front_rear_temp_gap, temperature_spread_mean
- Regime counts in this rerun: | pressure_regime | stable_run_count | total_duration_minutes |
| --------------- | ---------------- | ---------------------- |
| high            | 280              | 19086.95               |
| mid             | 1                | 4.0                    |

## Regime Coverage Gaps

| pressure_regime | stable_run_count | duration_minutes_total | duration_share_of_all_stable_runs | short_stable_run_count_lt_20m | long_stable_run_count_gte_20m | coverage_status |
| --------------- | ---------------- | ---------------------- | --------------------------------- | ----------------------------- | ----------------------------- | --------------- |
| low             | 0                | 0.0                    | 0.0                               | 0                             | 0                             | missing         |
| mid             | 1                | 4.0                    | 0.00020952336054517982            | 1                             | 0                             | present         |
| high            | 280              | 19086.949999999997     | 0.9997904766394549                | 38                            | 242                           | present         |

## Driver Attribution (Current Data Evidence)

| pressure_regime | stable_run_count | evidence_strength | likely_primary_driver                | material_signal_assessment                                               | reason                                                           | setpoint_between_run_cv_Val19 | process_ratio_between_run_cv_pressure_per_rpm | process_ratio_between_run_cv_load_per_pressure | thermal_between_run_cv_temperature_spread |
| --------------- | ---------------- | ----------------- | ------------------------------------ | ------------------------------------------------------------------------ | ---------------------------------------------------------------- | ----------------------------- | --------------------------------------------- | ---------------------------------------------- | ----------------------------------------- |
| low             | 0                | none              | insufficient_data                    | cannot_assess_without_regime_runs                                        | No retained stable runs in this regime.                          |                               |                                               |                                                |                                           |
| mid             | 1                | limited           | insufficient_data                    | no direct material sensor; attribution is indirect from process features | Too few stable runs for reliable driver attribution.             | 0.0                           | 0.0                                           | 0.0                                            | 0.0                                       |
| high            | 280              | moderate          | machine_behavior_or_process_dynamics | no direct material sensor; attribution is indirect from process features | Normalized process-ratio variation dominates setpoint variation. | 0.05267184100129482           | 0.06968623507540507                           | 0.07087577675532168                            | 0.02198501691751976                       |

## Phase Segments Preview

| phase_segment_id | phase_label | window_count | duration_minutes   | start_time               | end_time                 | mean_speed          |
| ---------------- | ----------- | ------------ | ------------------ | ------------------------ | ------------------------ | ------------------- |
| 1                | off         | 109          | 110.06666666666666 | 2026-02-16 07:05:36+0000 | 2026-02-16 08:55:40+0000 | 0.05871559679266055 |
| 2                | transition  | 5            | 8.0                | 2026-02-16 08:52:40+0000 | 2026-02-16 09:00:40+0000 | 5.463999938599999   |
| 3                | ramp_up     | 11           | 13.016666666666667 | 2026-02-16 08:57:40+0000 | 2026-02-16 09:10:41+0000 | 41.06545507436363   |
| 4                | transition  | 3            | 5.016666666666667  | 2026-02-16 09:08:40+0000 | 2026-02-16 09:13:41+0000 | 74.29333447333333   |
| 5                | stable_run  | 1            | 4.0                | 2026-02-16 09:10:41+0000 | 2026-02-16 09:14:41+0000 | 79.14000094000001   |
| 6                | ramp_up     | 6            | 9.0                | 2026-02-16 09:11:41+0000 | 2026-02-16 09:20:41+0000 | 92.50000156666665   |
| 7                | stable_run  | 10           | 13.0               | 2026-02-16 09:17:41+0000 | 2026-02-16 09:30:41+0000 | 103.785999906       |
| 8                | ramp_down   | 4            | 7.016666666666667  | 2026-02-16 09:27:41+0000 | 2026-02-16 09:34:42+0000 | 88.67999847499999   |
| 9                | ramp_up     | 7            | 10.016666666666667 | 2026-02-16 09:31:41+0000 | 2026-02-16 09:41:42+0000 | 107.02857034857142  |
| 10               | stable_run  | 183          | 184.11666666666667 | 2026-02-16 09:38:42+0000 | 2026-02-16 12:42:49+0000 | 113.9657923737705   |

## Stable Runs Preview

| stable_run_id | pressure_regime | window_count | duration_minutes   | start_time               | end_time                 | mean_Val_1         | mean_Val_5         | mean_Val_6         | mean_pressure_per_rpm | mean_load_per_pressure |
| ------------- | --------------- | ------------ | ------------------ | ------------------------ | ------------------------ | ------------------ | ------------------ | ------------------ | --------------------- | ---------------------- |
| 1             | mid             | 1            | 4.0                | 2026-02-16 09:10:41+0000 | 2026-02-16 09:14:41+0000 | 79.14000094000001  | 52.000000019999995 | 297.5799988        | 3.760339973856376     | 0.17499666904821665    |
| 2             | high            | 8            | 11.0               | 2026-02-16 09:19:41+0000 | 2026-02-16 09:30:41+0000 | 103.77499965       | 68.20833300833333  | 348.6499965        | 3.362808107393064     | 0.19576694239442663    |
| 3             | high            | 29           | 32.016666666666666 | 2026-02-16 09:40:42+0000 | 2026-02-16 10:12:43+0000 | 116.69696966666667 | 76.32121207272728  | 368.43333478787883 | 3.1632890782003638    | 0.2071432572817284     |
| 4             | high            | 101          | 103.06666666666666 | 2026-02-16 10:15:43+0000 | 2026-02-16 11:58:47+0000 | 113.49999998095238 | 74.23333296857142  | 370.2904759047619  | 3.2648363534807663    | 0.20046815918763414    |
| 5             | high            | 39           | 41.03333333333333  | 2026-02-16 12:01:47+0000 | 2026-02-16 12:42:49+0000 | 113.02790706976745 | 73.87441874883721  | 371.78836846511626 | 3.2931493796354214    | 0.1987073431052499     |
| 6             | high            | 2            | 5.0                | 2026-02-16 12:41:49+0000 | 2026-02-16 12:46:49+0000 | 120.45000066666667 | 78.46666716666668  | 368.700002         | 3.062315642369756     | 0.21281869677433804    |
| 7             | high            | 124          | 125.08333333333333 | 2026-02-16 12:46:49+0000 | 2026-02-16 14:51:54+0000 | 111.447656390625   | 72.79140627578124  | 372.631250140625   | 3.3453459702591744    | 0.19535531357206642    |
| 8             | high            | 27           | 29.016666666666666 | 2026-02-16 14:52:54+0000 | 2026-02-16 15:21:55+0000 | 111.0387087419355  | 72.44516163870966  | 373.987097935484   | 3.3694797066677222    | 0.19371320152574148    |
| 9             | high            | 19           | 22.016666666666666 | 2026-02-16 15:23:55+0000 | 2026-02-16 15:45:56+0000 | 111.70869621739129 | 72.9782615347826   | 374.20434965217385 | 3.3509372323281337    | 0.19502241620782268    |
| 10            | high            | 39           | 42.016666666666666 | 2026-02-16 15:53:56+0000 | 2026-02-16 16:35:57+0000 | 118.5232552093023  | 77.52093026744187  | 376.8209292558139  | 3.1837096178174473    | 0.2057782024418005     |

## Cluster Summary

| cluster_id | stable_run_count | total_duration_minutes | mean_speed         | mean_load          | mean_pressure      | mean_front_temp    | mean_rear_temp     | mean_temperature_spread | mean_pressure_per_rpm | mean_load_per_pressure | mean_pressure_to_temperature | mean_front_rear_temp_gap |
| ---------- | ---------------- | ---------------------- | ------------------ | ------------------ | ------------------ | ------------------ | ------------------ | ----------------------- | --------------------- | ---------------------- | ---------------------------- | ------------------------ |
| 1          | 1                | 4.0                    | 79.14000094000001  | 52.000000019999995 | 297.5799988        | 174.13199952       | 174.42666676666664 | 6.739999399999999       | 3.760339973856376     | 0.17499666904821665    | 1.707512805371604            | -0.2946672466666769      |
| 2          | 69               | 4577.883333333333      | 94.7870110378709   | 61.96095084554701  | 383.43823855656893 | 173.92154998372754 | 174.67594799634838 | 7.355023719726767       | 4.048355310674796     | 0.16161648681707985    | 2.199471078680162            | -0.7543980126208203      |
| 3          | 122              | 6918.366666666667      | 105.32470603232304 | 68.86348859308521  | 389.99957933558096 | 172.99486080470098 | 174.82163524002232 | 7.461424102714032       | 3.7054090101526236    | 0.1766037038894941     | 2.2414899260075485           | -1.826774435321324       |
| 4          | 50               | 5155.2                 | 108.1789917746357  | 70.73583751720297  | 373.60678387453686 | 173.97516708668604 | 174.65174036696027 | 7.381239040511321       | 3.4561631874616454    | 0.1894062015759592     | 2.1429142150324334           | -0.6765732802742321      |
| 5          | 37               | 2426.483333333333      | 109.57747210832991 | 71.64618224577123  | 369.59729143808215 | 172.98860150091414 | 174.85892541125278 | 7.499131519910804       | 3.3786011962072418    | 0.19394792922796697    | 2.1240154459119664           | -1.8703239103386404      |
| 6          | 2                | 9.016666666666666      | 122.81499903333334 | 80.02333373333335  | 359.400001         | 173.53466729666667 | 175.16444447777778 | 8.723332733333333       | 2.9312892658191063    | 0.22293328804361645    | 2.060517389593473            | -1.6297771811111068      |

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
