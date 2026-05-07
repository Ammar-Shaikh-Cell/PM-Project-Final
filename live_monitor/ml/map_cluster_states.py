import os
import sys
from pathlib import Path

import pandas as pd

# ensure live_monitor root is importable when run as script
_LIVE_MONITOR_ROOT = Path(__file__).resolve().parent.parent
if str(_LIVE_MONITOR_ROOT) not in sys.path:
    sys.path.insert(0, str(_LIVE_MONITOR_ROOT))

import config  # noqa: E402


def _pick_low_production(summary: pd.DataFrame, used: set[int]) -> int:
    # choose next strongest production-like cluster by speed + pressure ranks
    candidates = summary[~summary["cluster_id"].isin(used)].copy()
    candidates["prod_score"] = candidates["speed_rank"] + candidates["pressure_rank"]
    candidates = candidates.sort_values(["prod_score", "speed_rank", "pressure_rank"])
    return int(candidates.iloc[0]["cluster_id"])


def _pick_ready(summary: pd.DataFrame, used: set[int]) -> int:
    # choose cluster with strongest speed rise and most stable temperature slope
    candidates = summary[~summary["cluster_id"].isin(used)].copy()
    candidates["ready_score"] = candidates["speed_slope_rank"] + candidates["temp_stable_rank"]
    candidates = candidates.sort_values(["ready_score", "speed_slope_rank", "temp_stable_rank"])
    return int(candidates.iloc[0]["cluster_id"])


def _pick_heating(summary: pd.DataFrame, used: set[int]) -> int:
    # choose highest temperature rising trend cluster
    candidates = summary[~summary["cluster_id"].isin(used)].copy()
    candidates = candidates.sort_values(["temp_slope_rank"], ascending=True)
    return int(candidates.iloc[0]["cluster_id"])


def _pick_cooling(summary: pd.DataFrame, used: set[int]) -> int:
    # choose strongest temperature falling trend cluster
    candidates = summary[~summary["cluster_id"].isin(used)].copy()
    candidates = candidates.sort_values(["temp_slope_rank"], ascending=False)
    return int(candidates.iloc[0]["cluster_id"])


def _pick_off(summary: pd.DataFrame, used: set[int]) -> int:
    # choose cluster with lowest valid fraction and lowest speed
    candidates = summary[~summary["cluster_id"].isin(used)].copy()
    candidates["off_score"] = candidates["valid_rank"] + candidates["speed_low_rank"]
    candidates = candidates.sort_values(["off_score", "valid_rank", "speed_low_rank"])
    return int(candidates.iloc[0]["cluster_id"])


def main() -> None:
    # step 1: load clustered windows
    input_path = os.path.join(config.ML_OUTPUT_DIR, "ml_clustered_states.csv")
    output_path = os.path.join(config.ML_OUTPUT_DIR, "ml_labeled_states.csv")
    df = pd.read_csv(input_path)

    # step 2: compute cluster summary statistics
    # statistics describe each cluster behavior
    summary = (
        df.groupby("cluster_id", dropna=False)
        .agg(
            mean_Val_1=("mean_Val_1", "mean"),
            mean_Val_6=("mean_Val_6", "mean"),
            mean_Val_5=("mean_Val_5", "mean"),
            temperature_mean=("mean_temperature_mean", "mean"),
            slope_temperature=("slope_temperature", "mean"),
            slope_Val_1=("slope_Val_1", "mean"),
            valid_fraction=("valid_fraction", "mean"),
        )
        .reset_index()
    )
    summary["cluster_id"] = summary["cluster_id"].astype(int)

    # step 3: rank clusters by behavior signals
    # ranking lets data decide boundaries not hardcoded values
    summary["speed_rank"] = summary["mean_Val_1"].rank(ascending=False, method="dense")
    summary["pressure_rank"] = summary["mean_Val_6"].rank(ascending=False, method="dense")
    summary["load_rank"] = summary["mean_Val_5"].rank(ascending=False, method="dense")
    summary["temp_slope_rank"] = summary["slope_temperature"].rank(ascending=False, method="dense")
    summary["speed_slope_rank"] = summary["slope_Val_1"].rank(ascending=False, method="dense")
    summary["valid_rank"] = summary["valid_fraction"].rank(ascending=True, method="dense")
    summary["temp_stable_rank"] = summary["slope_temperature"].abs().rank(ascending=True, method="dense")
    summary["speed_low_rank"] = summary["mean_Val_1"].rank(ascending=True, method="dense")

    # step 4: map clusters to states using data-driven ranking logic
    # data-driven mapping, no fixed value thresholds
    cluster_to_state: dict[int, str] = {}
    used: set[int] = set()

    production_id = int((summary["speed_rank"] + summary["pressure_rank"]).idxmin())
    production_cluster = int(summary.loc[production_id, "cluster_id"])
    cluster_to_state[production_cluster] = "PRODUCTION"
    used.add(production_cluster)

    low_prod_cluster = _pick_low_production(summary, used)
    cluster_to_state[low_prod_cluster] = "LOW_PRODUCTION"
    used.add(low_prod_cluster)

    ready_cluster = _pick_ready(summary, used)
    cluster_to_state[ready_cluster] = "READY"
    used.add(ready_cluster)

    heating_cluster = _pick_heating(summary, used)
    cluster_to_state[heating_cluster] = "HEATING"
    used.add(heating_cluster)

    cooling_cluster = _pick_cooling(summary, used)
    cluster_to_state[cooling_cluster] = "COOLING"
    used.add(cooling_cluster)

    off_cluster = _pick_off(summary, used)
    cluster_to_state[off_cluster] = "OFF"
    used.add(off_cluster)

    # assign any leftover clusters by nearest behavior if cluster count differs from six
    for cluster_id in summary["cluster_id"]:
        if int(cluster_id) not in cluster_to_state:
            cluster_to_state[int(cluster_id)] = "OFF"

    # step 5: add predicted state per row from cluster mapping
    df["predicted_state"] = df["cluster_id"].astype("Int64").map(cluster_to_state)

    # step 6: print mapping table for review
    # review this to verify mapping makes sense
    mapping_rows = []
    for _, row in summary.iterrows():
        cid = int(row["cluster_id"])
        mapping_rows.append(
            {
                "cluster_id": cid,
                "predicted_state": cluster_to_state.get(cid),
                "mean_speed": row["mean_Val_1"],
                "mean_pressure": row["mean_Val_6"],
                "temp_slope": row["slope_temperature"],
            }
        )
    mapping_df = pd.DataFrame(mapping_rows).sort_values("cluster_id")
    print("cluster_id | predicted_state | mean_speed | mean_pressure | temp_slope")
    for _, row in mapping_df.iterrows():
        print(
            f"{int(row['cluster_id'])} | {row['predicted_state']} | "
            f"{row['mean_speed']:.4f} | {row['mean_pressure']:.4f} | {row['temp_slope']:.6f}"
        )

    # step 7: print predicted state distribution
    print("\nstate distribution:")
    print(df["predicted_state"].value_counts(dropna=False).to_string())

    # step 8: save labeled clustered windows
    os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"\nsaved: {output_path}")


if __name__ == "__main__":
    main()
