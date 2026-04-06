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

- Rows: 60700
- Value columns present: 41
- Duplicate timestamps: 29851
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

- Rows were not deleted.
- Duplicate timestamps were kept and flagged in the cleaned dataset.
- Negative values were only considered invalid for channels classified as non-negative physical or percentage-like signals.
- Bounded score/deviation channels that legitimately span `-100..100` were preserved.
- Interpolation is limited to short gaps (up to 3 samples) in active physical or slow-process channels only.
- Outliers are flagged using a rolling robust threshold; they are not overwritten.

## Process behavior validation

| row_index | TrendDate                | Val_1_before | Val_1_at    | Val_1_after | Val_5_before | Val_5_at | Val_5_after | Val_6_before | Val_6_at   | Val_6_after | Val_7_before | Val_7_at   | Val_7_after | Val_28_before | Val_28_at  | Val_28_after | Val_38_before | Val_38_at  | Val_38_after | Val_44_before | Val_44_at  | Val_44_after |
| --------- | ------------------------ | ------------ | ----------- | ----------- | ------------ | -------- | ----------- | ------------ | ---------- | ----------- | ------------ | ---------- | ----------- | ------------- | ---------- | ------------ | ------------- | ---------- | ------------ | ------------- | ---------- | ------------ |
| 14942     | 2026-02-21 11:41:18+0000 | 8.69999981   | 0.0         | 0.0         | 5.69999981   | 0.0      | 0.0         | 77.5999985   | 6.80000019 | 6.80000019  | 155.100006   | 154.300003 | 154.300003  | 162.600006    | 161.800003 | 161.800003   | -100.0        | 0.0        | 0.0          | 0.0           | 0.0        | 0.0          |
| 35046     | 2026-02-28 11:19:37+0000 | 5.9000001    | 0.100000001 | 0.100000001 | 4.19999981   | 0.0      | 0.0         | 50.7999992   | 2.4000001  | 2.4000001   | 162.899994   | 162.100006 | 162.100006  | 166.800003    | 165.899994 | 165.899994   | -100.0        | -100.0     | -100.0       | 0.0           | 0.0        | 0.0          |
| 40440     | 2026-03-02 08:18:19+0000 | 12.8000002   | 0.0         | 0.0         | 6.4000001    | 0.0      | 0.0         | 74.1999969   | 5.4000001  | 5.4000001   | 178.0        | 176.800003 | 176.800003  | 174.899994    | 174.899994 | 174.899994   | 4.0999999     | 15.1999998 | 15.1999998   | 0.0           | 1.29999995 | 1.29999995   |
| 54000     | 2026-03-07 01:22:37+0000 | 9.69999981   | 0.0         | 0.0         | 6.80000019   | 0.0      | 0.0         | 83.5         | 3.0        | 3.0         | 177.399994   | 176.199997 | 176.199997  | 174.600006    | 174.600006 | 174.600006   | -0.200000003  | 1.20000005 | 1.20000005   | 0.0           | 3.9000001  | 3.9000001    |
| 54094     | 2026-03-07 02:09:39+0000 | 7.30000019   | 0.0         | 0.0         | 2.9000001    | 0.0      | 0.0         | 10.8000002   | 4.4000001  | 4.4000001   | 162.300003   | 161.300003 | 161.300003  | 167.0         | 166.0      | 166.0        | -100.0        | 0.0        | 0.0          | 0.0           | 0.0        | 0.0          |

## Basic statistics preview

| column_name | count | min        | max        | mean               | std                | pct_zero            | pct_negative | n_unique | change_ratio         | lag1_autocorr      | median_abs_step | mean_abs_step        | longest_constant_run |
| ----------- | ----- | ---------- | ---------- | ------------------ | ------------------ | ------------------- | ------------ | -------- | -------------------- | ------------------ | --------------- | -------------------- | -------------------- |
| Val_1       | 60700 | 0.0        | 138.300003 | 67.70308731367908  | 49.71030295371452  | 21.059308072487646  | 0.0          | 598      | 0.39752883031301484  | 0.999398456390684  | 0.0             | 0.7075157820651575   | 186                  |
| Val_2       | 60700 | 18.0       | 53.0       | 36.26161449752883  | 7.498974994653058  | 0.0                 | 0.0          | 36       | 0.039308072487644154 | 0.9996047789228804 | 0.0             | 0.04062669895714921  | 1714                 |
| Val_3       | 60700 | 24.0       | 86.0       | 59.06304777594728  | 22.13314569279849  | 0.0                 | 0.0          | 63       | 0.13268533772652388  | 0.9998575696504696 | 0.0             | 0.13469744147350038  | 530                  |
| Val_4       | 60700 | 0.0        | 15.0100002 | 6.838937703402259  | 4.928028249751246  | 0.20428336079077428 | 0.0          | 112      | 0.3789291598023064   | 0.999531077443639  | 0.0             | 0.011166103865022849 | 56                   |
| Val_5       | 60700 | 0.0        | 91.6999969 | 44.25148268953644  | 32.50347149503604  | 34.28006589785832   | 0.0          | 440      | 0.31334431630971993  | 0.9996570730708064 | 0.0             | 0.2898037812204484   | 6606                 |
| Val_6       | 60700 | 2.0        | 402.0      | 249.37190778625967 | 180.32269191498045 | 0.0                 | 0.0          | 826      | 0.333163097199341    | 0.9998688733182356 | 0.0             | 0.44178325533237783  | 2414                 |
| Val_7       | 60700 | 24.2000008 | 187.5      | 131.32930482695224 | 63.95652205531856  | 0.0                 | 0.0          | 1363     | 0.3285337726523888   | 0.9999768489634678 | 0.0             | 0.10162119672647006  | 170                  |
| Val_8       | 60700 | 24.6000004 | 185.699997 | 131.962886338514   | 63.33592797331179  | 0.0                 | 0.0          | 1417     | 0.3219110378912685   | 0.999985003918792  | 0.0             | 0.10568379863753935  | 218                  |
| Val_9       | 60700 | 24.7999992 | 183.300003 | 131.1203788007611  | 63.63469080814429  | 0.0                 | 0.0          | 1310     | 0.23102141680395388  | 0.9999903343578965 | 0.0             | 0.05129424720011882  | 234                  |
| Val_10      | 60700 | 22.5       | 179.600006 | 128.33502967372652 | 63.82826607935901  | 0.0                 | 0.0          | 1263     | 0.29787479406919276  | 0.9999885639711915 | 0.0             | 0.07025860753719229  | 172                  |

## Cleaning summary preview

| column_name | invalid_count | outlier_count | interpolated_count |
| ----------- | ------------- | ------------- | ------------------ |
| Val_1       | 0             | 388           | 0                  |
| Val_2       | 0             | 0             | 0                  |
| Val_3       | 0             | 0             | 0                  |
| Val_4       | 0             | 108           | 0                  |
| Val_5       | 0             | 689           | 0                  |
| Val_6       | 0             | 124           | 0                  |
| Val_7       | 0             | 641           | 0                  |
| Val_8       | 0             | 968           | 0                  |
| Val_9       | 0             | 31            | 0                  |
| Val_10      | 0             | 42            | 0                  |
| Val_11      | 0             | 6             | 0                  |
| Val_12      | 0             | 0             | 0                  |
| Val_14      | 0             | 0             | 0                  |
| Val_15      | 0             | 0             | 0                  |
| Val_19      | 0             | 0             | 0                  |
