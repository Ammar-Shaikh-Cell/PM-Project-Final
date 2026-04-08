# Sensor Mapping Step 1 Outputs

This folder contains the reproducible work for the sensor-identification and cleaning step on `tab_actual_export.csv`.

## Files

- `generate_sensor_mapping_outputs.py`: reproducible script used to create every result in this folder.
- `results/basic_stats.csv`: min, max, mean, std, zero ratio, negative ratio, change ratio, and other descriptive statistics for each `Val_X`.
- `results/sensor_mapping_table.csv`: final mapping table with guessed sensor type, confidence, and reasoning.
- `results/active_sensors.csv`: active sensor list.
- `results/inactive_sensors.csv`: inactive sensor list.
- `results/key_sensors.csv`: identified key sensors for screw speed, pressure, motor load/current, and temperature zones.
- `results/correlation_matrix.csv`: correlation matrix for active sensors.
- `results/top_correlations.csv`: strongest pairwise correlations for each active sensor.
- `results/correlation_clusters.csv`: correlation-based grouping using absolute correlation >= 0.95.
- `results/process_behavior_checks.csv`: stop-event validation rows used to confirm speed/pressure/temperature behavior.
- `results/cleaning_summary_by_column.csv`: per-column counts for invalids, outliers, and interpolated values.
- `results/cleaning_log.csv`: row-level log of every flagged or changed cell.
- `results/cleaned/tab_actual_export_cleaned.csv`: cleaned dataset with flag columns added.
- `results/plots/`: one plot per sensor plus grouped plots and a correlation heatmap.

## Dataset overview

- Rows after exact deduplication: 30858
- Value columns present: 41
- Duplicate timestamps: 9
- Active sensors: 31
- Inactive sensors: 10

## Key sensor identification

    | column_name | guessed_type                    | confidence | reason                                                                                                  |
| ----------- | ------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------- |
| Val_1       | screw_speed_actual_candidate    | high       | 0-138 range, immediate ON/OFF response, strongest primary production signal                             |
| Val_5       | motor_load_or_current_candidate | high       | 0-91.7 range, immediate ON/OFF response, strongest short-term coupling with Val_1 among load candidates |
| Val_6       | melt_pressure_candidate         | high       | 2-402 range, collapses to low floor on stop, tracks production/load strongly                            |
| Val_7       | temperature_zone_candidate      | high       | 24-187 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_8       | temperature_zone_candidate      | high       | 25-186 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_9       | temperature_zone_candidate      | high       | 25-183 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_10      | temperature_zone_candidate      | high       | 22.5-180 range, smooth heating/cooling, strong thermal inertia                                          |
| Val_11      | temperature_zone_candidate      | high       | 22.5-180 range, smooth heating/cooling, strong thermal inertia                                          |
| Val_27      | temperature_zone_candidate      | high       | 24-175 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_28      | temperature_zone_candidate      | high       | 24-181 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_29      | temperature_zone_candidate      | high       | 24-183 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_30      | temperature_zone_candidate      | high       | 21-185 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_31      | temperature_zone_candidate      | high       | 21-185 range, smooth heating/cooling, strong thermal inertia                                            |
| Val_32      | temperature_zone_candidate      | high       | 24.5-182 range, smooth heating/cooling, strong thermal inertia                                          |

## Cleaning notes

- Exact duplicate rows were removed from the cleaned dataset using `TrendDate + all Val_X columns`.
- Exact duplicate rows removed: 29842
- Same-timestamp conflicting rows retained: 18 across 9 timestamp groups.
- Duplicate timestamps that remain in the cleaned dataset are only retained when values differ.
- Negative values were only considered invalid for channels classified as non-negative physical or percentage-like signals.
- Sentinel values `-1` and `-100` in bounded score/deviation channels `Val_38`, `Val_46`, `Val_47`, and `Val_48` are replaced with `NaN`.
- Other in-range negative values in bounded score/deviation channels are preserved.
- Interpolation is limited to short gaps (up to 3 samples) in active physical or slow-process channels only.
- Outliers are flagged using a rolling robust threshold; they are not overwritten.

## Retained same-timestamp conflicts

These rows were kept because the timestamp is the same but the sensor values are not identical, so they are not full duplicates.

| TrendDate                | row_count | differing_column_count | differing_columns                                                                                                                                                        |
| ------------------------ | --------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2026-02-20 09:05:17+0000 | 2         | 20                     | Val_1, Val_3, Val_4, Val_5, Val_6, Val_7, Val_8, Val_10, Val_20, Val_27, Val_28, Val_31, Val_38, Val_39, Val_40, Val_42, Val_44, Val_45, Val_46, Val_47                  |
| 2026-02-21 12:24:19+0000 | 2         | 13                     | Val_4, Val_7, Val_8, Val_9, Val_10, Val_11, Val_27, Val_28, Val_29, Val_30, Val_31, Val_32, Val_33                                                                       |
| 2026-02-23 19:26:24+0000 | 2         | 20                     | Val_1, Val_4, Val_5, Val_6, Val_7, Val_8, Val_11, Val_20, Val_27, Val_32, Val_38, Val_39, Val_40, Val_42, Val_43, Val_44, Val_45, Val_46, Val_47, Val_48                 |
| 2026-02-27 05:30:29+0000 | 2         | 19                     | Val_1, Val_3, Val_4, Val_5, Val_6, Val_7, Val_8, Val_10, Val_29, Val_31, Val_38, Val_39, Val_40, Val_42, Val_43, Val_44, Val_45, Val_46, Val_47                          |
| 2026-02-28 07:50:29+0000 | 2         | 22                     | Val_1, Val_4, Val_5, Val_6, Val_7, Val_8, Val_10, Val_11, Val_27, Val_28, Val_32, Val_34, Val_38, Val_39, Val_40, Val_42, Val_43, Val_44, Val_45, Val_46, Val_47, Val_48 |
| 2026-03-07 02:46:40+0000 | 2         | 12                     | Val_1, Val_3, Val_4, Val_7, Val_8, Val_9, Val_10, Val_27, Val_28, Val_29, Val_30, Val_32                                                                                 |
| 2026-03-07 03:10:41+0000 | 2         | 11                     | Val_4, Val_7, Val_8, Val_9, Val_10, Val_11, Val_27, Val_28, Val_29, Val_31, Val_32                                                                                       |
| 2026-03-08 06:28:43+0000 | 2         | 6                      | Val_1, Val_4, Val_10, Val_11, Val_27, Val_29                                                                                                                             |
| 2026-03-09 09:00:43+0000 | 2         | 19                     | Val_1, Val_7, Val_8, Val_9, Val_10, Val_11, Val_27, Val_28, Val_29, Val_30, Val_31, Val_32, Val_38, Val_39, Val_41, Val_44, Val_45, Val_46, Val_47                       |

## Process behavior validation

| row_index | TrendDate                | Val_1_before | Val_1_at    | Val_1_after | Val_5_before | Val_5_at | Val_5_after | Val_6_before | Val_6_at   | Val_6_after | Val_7_before | Val_7_at   | Val_7_after | Val_28_before | Val_28_at  | Val_28_after | Val_38_before | Val_38_at  | Val_38_after | Val_44_before | Val_44_at  | Val_44_after |
| --------- | ------------------------ | ------------ | ----------- | ----------- | ------------ | -------- | ----------- | ------------ | ---------- | ----------- | ------------ | ---------- | ----------- | ------------- | ---------- | ------------ | ------------- | ---------- | ------------ | ------------- | ---------- | ------------ |
| 7590      | 2026-02-21 11:41:18+0000 | 8.69999981   | 0.0         | 0.0         | 5.69999981   | 0.0      | 0.0         | 77.5999985   | 6.80000019 | 2.70000005  | 155.100006   | 154.300003 | 153.5       | 162.600006    | 161.800003 | 161.399994   |               | 0.0        | 0.0          | 0.0           | 0.0        | 0.0          |
| 17818     | 2026-02-28 11:19:37+0000 | 5.9000001    | 0.100000001 | 0.100000001 | 4.19999981   | 0.0      | 0.0         | 50.7999992   | 2.4000001  | 2.20000005  | 162.899994   | 162.100006 | 161.199997  | 166.800003    | 165.899994 | 164.899994   |               |            |              | 0.0           | 0.0        | 0.0          |
| 20563     | 2026-03-02 08:18:19+0000 | 12.8000002   | 0.0         | 0.0         | 6.4000001    | 0.0      | 0.0         | 74.1999969   | 5.4000001  | 5.19999981  | 178.0        | 176.800003 | 175.800003  | 174.899994    | 174.899994 | 174.800003   | 4.0999999     | 15.1999998 | 29.5         | 0.0           | 1.29999995 | 4.69999981   |
| 27451     | 2026-03-07 01:22:37+0000 | 9.69999981   | 0.0         | 0.0         | 6.80000019   | 0.0      | 0.0         | 83.5         | 3.0        | 3.0         | 177.399994   | 176.199997 | 175.199997  | 174.600006    | 174.600006 | 174.699997   | -0.200000003  | 1.20000005 | 19.2000008   | 0.0           | 3.9000001  | 8.69999981   |
| 27498     | 2026-03-07 02:09:39+0000 | 7.30000019   | 0.0         | 0.0         | 2.9000001    | 0.0      | 0.0         | 10.8000002   | 4.4000001  | 4.4000001   | 162.300003   | 161.300003 | 160.300003  | 167.0         | 166.0      | 165.199997   |               | 0.0        | 0.0          | 0.0           | 0.0        | 0.0          |

## Basic statistics preview

| column_name | count | min        | max        | mean               | std                | pct_zero           | pct_negative | n_unique | change_ratio        | lag1_autocorr      | median_abs_step      | mean_abs_step        | longest_constant_run |
| ----------- | ----- | ---------- | ---------- | ------------------ | ------------------ | ------------------ | ------------ | -------- | ------------------- | ------------------ | -------------------- | -------------------- | -------------------- |
| Val_1       | 30858 | 0.0        | 138.300003 | 67.67583770794211  | 49.71831438347665  | 21.083673601659214 | 0.0          | 598      | 0.7819690193790914  | 0.9988170462413906 | 0.7000050000000044   | 1.3917587729064071   | 94                   |
| Val_2       | 30858 | 18.0       | 53.0       | 36.258312269103634 | 7.500600040602244  | 0.0                | 0.0          | 36       | 0.07732192624278955 | 0.999222871651541  | 0.0                  | 0.07991703665294747  | 872                  |
| Val_3       | 30858 | 24.0       | 86.0       | 59.052401322185496 | 22.135962848977712 | 0.0                | 0.0          | 63       | 0.26100200920344807 | 0.9997198851142539 | 0.0                  | 0.2649641896490261   | 272                  |
| Val_4       | 30858 | 0.0        | 15.0100002 | 6.835828611263484  | 4.9290008820654565 | 0.204160995527902  | 0.0          | 112      | 0.7453820727202022  | 0.9990779155422835 | 0.010000200000000348 | 0.021964913585346014 | 29                   |
| Val_5       | 30858 | 0.0        | 91.6999969 | 44.2334823908964   | 32.50866700695779  | 34.30552855013287  | 0.0          | 440      | 0.6163717674509042  | 0.9993256219984448 | 0.29999540000000025  | 0.5700748522636679   | 3360                 |
| Val_6       | 30858 | 2.0        | 402.0      | 249.2768974488781  | 180.3489821957853  | 0.0                | 0.0          | 826      | 0.6553567956445654  | 0.9997421273737258 | 0.40002400000003036  | 0.8690346376971191   | 1229                 |
| Val_7       | 30858 | 24.2000008 | 187.5      | 131.30053475916782 | 63.96833205770798  | 0.0                | 0.0          | 1363     | 0.6462505671138765  | 0.9999544763462481 | 0.10000600000000759  | 0.19989969926110787  | 86                   |
| Val_8       | 30858 | 24.6000004 | 185.699997 | 131.9348467312593  | 63.347999860753205 | 0.0                | 0.0          | 1417     | 0.6332231512087627  | 0.999970512539883  | 0.10000600000000759  | 0.20789126919337597  | 112                  |
| Val_9       | 30858 | 24.7999992 | 183.300003 | 131.09178159444554 | 63.64657344739357  | 0.0                | 0.0          | 1310     | 0.45443645083932854 | 0.9999809940581731 | 0.0                  | 0.10090123831869631  | 118                  |
| Val_10      | 30858 | 22.5       | 179.600006 | 128.30549616552273 | 63.840010597144285 | 0.0                | 0.0          | 1263     | 0.5859420571650787  | 0.9999775127063918 | 0.0999984999999981   | 0.1382061515669065   | 88                   |

## Cleaning summary preview

| column_name | invalid_count | nan_replacement_count | outlier_count | interpolated_count |
| ----------- | ------------- | --------------------- | ------------- | ------------------ |
| Val_1       | 0             | 0                     | 166           | 0                  |
| Val_2       | 0             | 0                     | 0             | 0                  |
| Val_3       | 0             | 0                     | 0             | 0                  |
| Val_4       | 0             | 0                     | 79            | 0                  |
| Val_5       | 0             | 0                     | 306           | 0                  |
| Val_6       | 0             | 0                     | 90            | 0                  |
| Val_7       | 0             | 0                     | 525           | 0                  |
| Val_8       | 0             | 0                     | 397           | 0                  |
| Val_9       | 0             | 0                     | 87            | 0                  |
| Val_10      | 0             | 0                     | 64            | 0                  |
| Val_11      | 0             | 0                     | 57            | 0                  |
| Val_12      | 0             | 0                     | 0             | 0                  |
| Val_14      | 0             | 0                     | 0             | 0                  |
| Val_15      | 0             | 0                     | 0             | 0                  |
| Val_19      | 0             | 0                     | 0             | 0                  |
