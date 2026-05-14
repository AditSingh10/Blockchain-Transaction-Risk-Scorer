"""One-shot setup script: fit the StandardScaler on training data and save it.

Run this once from the project root before starting the API server:

    ELLIPTIC_DATA_DIR=/path/to/data python prepare_model.py

Produces: models/scaler.pkl
"""

import os
import sys

import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler


def main() -> None:
    data_dir = os.environ.get("ELLIPTIC_DATA_DIR")
    if not data_dir:
        print("Error: ELLIPTIC_DATA_DIR environment variable is not set.", file=sys.stderr)
        print("Usage: ELLIPTIC_DATA_DIR=/path/to/data python prepare_model.py", file=sys.stderr)
        sys.exit(1)

    scaler_path = "models/scaler.pkl"
    if os.path.exists(scaler_path):
        print(f"Scaler already exists at '{scaler_path}'. Delete it first to re-fit.")
        return

    dataset_dir = os.path.join(data_dir, "elliptic_bitcoin_dataset")
    features_path = os.path.join(dataset_dir, "elliptic_txs_features.csv")
    classes_path = os.path.join(dataset_dir, "elliptic_txs_classes.csv")

    print(f"Loading features from {features_path} ...")
    features = pd.read_csv(features_path, header=None)
    features.rename(columns={0: "txId", 1: "time_step"}, inplace=True)

    print(f"Loading classes from {classes_path} ...")
    classes = pd.read_csv(classes_path)
    label_map = {"1": 1, "2": 0}
    classes["label"] = classes["class"].map(label_map).fillna(-1).astype(int)

    nodes = features.merge(classes[["txId", "label"]], on="txId", how="left")
    nodes["label"] = nodes["label"].fillna(-1).astype(int)

    feature_cols = [c for c in nodes.columns if c not in ["txId", "time_step", "label"]]
    print(f"Feature columns: {len(feature_cols)} (expected 165)")

    X = nodes[feature_cols].values.astype(np.float32)

    # Mirror exactly what the training notebook does: fit on labeled nodes in timesteps 1–34
    train_mask = (nodes["time_step"].values <= 34) & (nodes["label"].values != -1)
    print(f"Training nodes for scaler fit: {train_mask.sum()} (labeled, timestep <= 34)")

    scaler = StandardScaler()
    scaler.fit(X[train_mask])

    joblib.dump(scaler, scaler_path)
    print(f"Scaler saved to '{scaler_path}'")
    print(f"  mean shape: {scaler.mean_.shape}")
    print(f"  std  shape: {scaler.scale_.shape}")


if __name__ == "__main__":
    main()
