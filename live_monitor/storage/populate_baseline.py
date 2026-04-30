import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage.db_writer import BaselineRegistry, engine

# path to historical stable runs from Stage 2 of PM-Project
STABLE_RUNS_CSV = "timeSeriesDB/time-series-database/process_segmentation_outputs/results/stable_runs.csv"
# TODO: update path if different in your project directory

LOW_REGIME_CSV = "timeSeriesDB/time-series-database/process_segmentation_outputs/results/low_regime1.csv"
# updated LOW regime dataset — matches current operating conditions
# speed ~93 RPM, load ~61%, pressure ~251 bar (295 stable rows)
# TODO: update path if different in your project directory


def get_confidence(run_count):
    if run_count >= 50:
        return "HIGH"
    elif run_count >= 5:
        return "MEDIUM"
    else:
        return "LOW"
    # LOW confidence = provisional, use with caution


def build_low_regime_baseline():
    # builds LOW regime baseline from separate live CSV
    #               filters stable rows: speed >= 20, pressure >= 50
    #               calculates same features as historical pipeline
    df = pd.read_csv(LOW_REGIME_CSV)

    # filter stable production rows only
    df = df[(df["speed"] >= 20) & (df["pressure"] >= 50)].copy()
    # remove OFF/startup rows using same thresholds as historical

    # calculate temperature mean from 4 zones
    df["temperature_mean"] = df[["temp1", "temp2", "temp3", "temp4"]].mean(axis=1)
    # average of 4 temperature zone sensors

    # calculate derived features
    df["pressure_per_rpm"] = df["pressure"] / df["speed"]
    df["temp_spread"] = (
        df[["temp1", "temp2", "temp3", "temp4"]].max(axis=1)
        - df[["temp1", "temp2", "temp3", "temp4"]].min(axis=1)
    )
    df["load_per_pressure"] = df["load"] / df["pressure"]
    # same derived features as historical and live pipeline

    # feature mapping: live name -> CSV column
    LOW_FEATURE_MAP = {
        "screw_speed_mean": "speed",
        "screw_speed_std": "speed",
        "pressure_mean": "pressure",
        "pressure_std": "pressure",
        "load_mean": "load",
        "temperature_mean": "temperature_mean",
        "pressure_per_rpm": "pressure_per_rpm",
        "temp_spread": "temp_spread",
        "load_per_pressure": "load_per_pressure",
    }
    # std features use same column, .std() calculated separately

    rows = []
    run_count = len(df)
    confidence = "HIGH" if run_count >= 50 else "MEDIUM"
    # 3640 stable rows -> HIGH confidence

    for feature_name, col in LOW_FEATURE_MAP.items():
        values = df[col].dropna()
        if len(values) == 0:
            continue

        mean_val = float(values.mean())
        std_val = float(values.std()) if len(values) > 1 else 0.0
        min_val = float(values.min())
        max_val = float(values.max())
        p10_val = float(np.percentile(values, 10))
        p90_val = float(np.percentile(values, 90))

        warning_low = mean_val - 2.0 * std_val
        warning_high = mean_val + 2.0 * std_val
        critical_low = mean_val - 3.0 * std_val
        critical_high = mean_val + 3.0 * std_val
        # warning = mean ± 2std | critical = mean ± 3std

        rows.append(
            BaselineRegistry(
                regime_type="LOW",
                profile_id=None,
                feature_name=feature_name,
                mean_value=mean_val,
                std_value=std_val,
                min_value=min_val,
                max_value=max_val,
                p10_value=p10_val,
                p90_value=p90_val,
                warning_low=warning_low,
                warning_high=warning_high,
                critical_low=critical_low,
                critical_high=critical_high,
                sample_count=len(values),
                source_run_count=run_count,
                baseline_confidence=confidence,
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
        )

    print(f"LOW regime: {len(rows)} baseline rows built from {run_count} stable rows")
    return rows


def main() -> None:
    df = pd.read_csv(STABLE_RUNS_CSV)
    # each row = one stable run with aggregated features + regime label

    # maps live pipeline feature names -> historical CSV column names
    FEATURE_MAP = {
        "screw_speed_mean": "mean_Val_1",
        "screw_speed_std": "std_Val_1",
        "pressure_mean": "mean_Val_6",
        "pressure_std": "std_Val_6",
        "load_mean": "mean_Val_5",
        "temperature_mean": "temperature_mean",
        "pressure_per_rpm": "mean_pressure_per_rpm",
        "temp_spread": "temperature_spread_mean",
        "load_per_pressure": "mean_load_per_pressure",
    }

    # based on stable run count per regime from historical analysis
    # HIGH regime  = 280 runs -> HIGH confidence
    # MID regime   = 1 run   -> LOW confidence (provisional)
    # LOW regime   = 0 runs  -> not in CSV, skip

    # group stable runs by pressure_regime, then compute stats
    baseline_rows = []

    for regime, group in df.groupby("pressure_regime"):
        regime_label = regime.upper()
        # convert "high"/"mid"/"low" to "HIGH"/"MID"/"LOW"

        run_count = len(group)
        confidence = get_confidence(run_count)

        for feature_name, csv_col in FEATURE_MAP.items():
            if csv_col not in group.columns:
                print(f"Skipping {feature_name}: column {csv_col} not found")
                continue

            values = group[csv_col].dropna()
            if len(values) == 0:
                continue

            mean_val = float(values.mean())
            std_val = float(values.std()) if len(values) > 1 else 0.0
            min_val = float(values.min())
            max_val = float(values.max())
            p10_val = float(np.percentile(values, 10))
            p90_val = float(np.percentile(values, 90))

            # warning bands = mean +- 2 * std
            warning_low = mean_val - 2.0 * std_val
            warning_high = mean_val + 2.0 * std_val
            # outside warning band = needs attention

            # critical bands = mean +- 3 * std
            critical_low = mean_val - 3.0 * std_val
            critical_high = mean_val + 3.0 * std_val
            # outside critical band = process deviation, alert needed

            baseline_rows.append(
                BaselineRegistry(
                    regime_type=regime_label,
                    profile_id=None,
                    feature_name=feature_name,
                    mean_value=mean_val,
                    std_value=std_val,
                    min_value=min_val,
                    max_value=max_val,
                    p10_value=p10_val,
                    p90_value=p90_val,
                    warning_low=warning_low,
                    warning_high=warning_high,
                    critical_low=critical_low,
                    critical_high=critical_high,
                    sample_count=len(values),
                    source_run_count=run_count,
                    baseline_confidence=confidence,
                    created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    updated_at=datetime.now(timezone.utc).replace(tzinfo=None),
                )
            )

    # add LOW regime baselines from separate live CSV
    low_rows = build_low_regime_baseline()
    baseline_rows.extend(low_rows)

    # clear old baseline rows first to avoid duplicates on re-run
    with Session(engine) as session:
        existing = session.query(BaselineRegistry).count()
        if existing > 0:
            session.query(BaselineRegistry).delete()
            session.commit()
            print(f"Cleared {existing} existing baseline rows")
        # safe to re-run: always starts fresh

        session.add_all(baseline_rows)
        session.commit()
        print(f"Inserted {len(baseline_rows)} baseline rows")

    # verify what was inserted
    with Session(engine) as session:
        rows = session.query(BaselineRegistry).all()
        print("\n--- Baseline Registry Summary ---")
        for r in rows:
            print(
                f"regime={r.regime_type} | feature={r.feature_name} | "
                f"mean={r.mean_value:.3f} | std={r.std_value:.3f} | "
                f"confidence={r.baseline_confidence} | runs={r.source_run_count}"
            )


if __name__ == "__main__":
    print("Populating baseline registry...")
    # run once before starting live pipeline
    # run with: python storage/populate_baseline.py
    main()
