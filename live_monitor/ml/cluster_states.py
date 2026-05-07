import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# ensure live_monitor root is importable when run as script
_LIVE_MONITOR_ROOT = Path(__file__).resolve().parent.parent
if str(_LIVE_MONITOR_ROOT) not in sys.path:
    sys.path.insert(0, str(_LIVE_MONITOR_ROOT))

import config  # noqa: E402


def main() -> None:
    # step 1: load 30-min windows dataset
    # 30-min windows with slope features included
    input_path = os.path.join(config.ML_OUTPUT_DIR, "ml_feature_matrix_30min.csv")
    output_path = os.path.join(config.ML_OUTPUT_DIR, "ml_clustered_states.csv")
    elbow_plot_path = os.path.join(config.ML_OUTPUT_DIR, "elbow_plot.png")

    df = pd.read_csv(input_path)

    # step 2: select clustering features
    # these features capture speed/pressure/load/temp/trends
    cluster_features = [
        "mean_Val_1",
        "std_Val_1",
        "mean_Val_5",
        "std_Val_5",
        "mean_Val_6",
        "std_Val_6",
        "mean_temperature_mean",
        "slope_Val_1",
        "slope_Val_6",
        "slope_temperature",
        "valid_fraction",
    ]

    # step 3: drop rows with null values in selected features
    work_df = df.dropna(subset=cluster_features).copy()

    # step 4: scale selected features
    # scaling ensures no single feature dominates clustering
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(work_df[cluster_features])

    # step 5: elbow method from k=2 to k=10
    # elbow point = natural number of groups in data
    ks = list(range(2, 11))
    inertias = []
    for k in ks:
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        model.fit(x_scaled)
        inertias.append(model.inertia_)

    os.makedirs(config.ML_OUTPUT_DIR, exist_ok=True)
    plt.figure(figsize=(8, 5))
    plt.plot(ks, inertias, marker="o")
    plt.title("KMeans Elbow Plot")
    plt.xlabel("Number of clusters (K)")
    plt.ylabel("Inertia")
    plt.xticks(ks)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(elbow_plot_path, dpi=150)
    plt.close()

    # step 6: train final KMeans with K=6
    # 6 matches our expected states:
    # OFF/HEATING/READY/LOW_PRODUCTION/PRODUCTION/COOLING
    final_model = KMeans(n_clusters=6, random_state=42, n_init=10)
    cluster_ids = final_model.fit_predict(x_scaled)

    # step 7: assign cluster labels back to windows
    work_df["cluster_id"] = cluster_ids
    df["cluster_id"] = pd.NA
    df.loc[work_df.index, "cluster_id"] = work_df["cluster_id"].astype(int)

    # step 8: print per-cluster summary statistics
    # statistics help map clusters to real machine states
    for cluster_id, group in work_df.groupby("cluster_id"):
        print(f"Cluster {cluster_id}")
        print(f"  count: {len(group)}")
        print(f"  mean_Val_1 mean: {group['mean_Val_1'].mean():.4f}")
        print(f"  mean_Val_6 mean: {group['mean_Val_6'].mean():.4f}")
        print(f"  temperature_mean mean: {group['mean_temperature_mean'].mean():.4f}")
        print(f"  slope_temperature mean: {group['slope_temperature'].mean():.4f}")
        print(f"  valid_fraction mean: {group['valid_fraction'].mean():.4f}")

    # step 9: save clustered output
    df.to_csv(output_path, index=False)
    print(f"Saved clustered states: {output_path}")
    print(f"Saved elbow plot: {elbow_plot_path}")


if __name__ == "__main__":
    main()
