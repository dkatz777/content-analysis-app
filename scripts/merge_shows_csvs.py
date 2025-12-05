# scripts/merge_show_csvs.py

from __future__ import annotations

from pathlib import Path
from datetime import date
import argparse
import pandas as pd

# Project root is one level up from this script
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"


def load_csv(path: Path) -> pd.DataFrame:
    """Load a CSV file into a DataFrame with a clear error if missing."""
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    return pd.read_csv(path)


def align_columns(
    master_df: pd.DataFrame,
    snapshot_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ensure both DataFrames share the same set of columns.
    Missing columns are filled with NA, and columns are ordered consistently.
    """
    all_cols = sorted(set(master_df.columns) | set(snapshot_df.columns))

    master_aligned = master_df.copy()
    snapshot_aligned = snapshot_df.copy()

    for col in all_cols:
        if col not in master_aligned.columns:
            master_aligned[col] = pd.NA
        if col not in snapshot_aligned.columns:
            snapshot_aligned[col] = pd.NA

    master_aligned = master_aligned[all_cols]
    snapshot_aligned = snapshot_aligned[all_cols]

    return master_aligned, snapshot_aligned


def dedupe_on_key(df: pd.DataFrame, key: str, label: str) -> pd.DataFrame:
    """
    Drop duplicate key values, keeping the last row for each key.

    Prints a small log so you know how many duplicates were removed.
    """
    if key not in df.columns:
        raise ValueError(f"Expected key column '{key}' in {label} DataFrame")

    before = len(df)
    dup_count = df[key].duplicated(keep="last").sum()
    if dup_count > 0:
        print(f"{label}: found {dup_count} duplicate '{key}' values, dropping duplicates")
        df = df.drop_duplicates(subset=[key], keep="last").reset_index(drop=True)
    else:
        print(f"{label}: no duplicate '{key}' values")

    after = len(df)
    print(f"{label}: {before} rows before dedupe, {after} rows after dedupe")
    return df


def merge_master_with_snapshot(
    master_path: Path,
    snapshot_path: Path,
    output_path: Path | None = None,
    key: str = "video_id",
) -> None:
    """
    Merge a per show master CSV with a cleaned snapshot CSV.

    Rules:
      - Use `key` (video_id) as the unique identifier.
      - For overlapping videos (same key):
          New snapshot values overwrite master values.
      - For videos only in snapshot:
          Append them as new rows.
      - For videos only in master:
          Keep them. They are not dropped automatically.
      - Mark which videos are present in the latest snapshot and
        update `last_seen_snapshot_date` and `last_merged_on`.
    """
    if output_path is None:
        output_path = master_path

    master_df = load_csv(master_path)
    snapshot_df = load_csv(snapshot_path)

    if key not in master_df.columns:
        raise ValueError(f"Key column '{key}' is missing from master: {master_path}")
    if key not in snapshot_df.columns:
        raise ValueError(f"Key column '{key}' is missing from snapshot: {snapshot_path}")

    # Drop rows from snapshot that do not have a key
    snapshot_df = snapshot_df.dropna(subset=[key])

    # Align schemas so update / concat behaves predictably
    master_df, snapshot_df = align_columns(master_df, snapshot_df)

    # Deduplicate on key before setting index
    master_df = dedupe_on_key(master_df, key=key, label="master")
    snapshot_df = dedupe_on_key(snapshot_df, key=key, label="snapshot")

    # Use key as index for overwrite semantics
    master_df = master_df.set_index(key)
    snapshot_df = snapshot_df.set_index(key)

    # Start with master, overwrite overlapping rows with snapshot values
    updated_master = master_df.copy()
    updated_master.update(snapshot_df)

    # Add rows that are new in the snapshot
    new_only = snapshot_df.loc[~snapshot_df.index.isin(master_df.index)]

    merged = pd.concat([updated_master, new_only], axis=0)

    # Track presence in latest snapshot
    merged["present_in_latest_snapshot"] = merged.index.isin(snapshot_df.index)

    today = date.today().isoformat()

    # Track when the video was last seen in a snapshot
    if "last_seen_snapshot_date" not in merged.columns:
        merged["last_seen_snapshot_date"] = pd.NA

    seen_mask = merged["present_in_latest_snapshot"]
    merged.loc[seen_mask, "last_seen_snapshot_date"] = today

    # Track last merge time for the row set as a whole
    merged["last_merged_on"] = today

    # Reset index back to a regular column
    merged = merged.reset_index().rename(columns={"index": key})

    # Optional: put key first
    cols = merged.columns.tolist()
    if key in cols:
        cols.insert(0, cols.pop(cols.index(key)))
        merged = merged[cols]

    # Small log so you can tell the merge worked
    print(f"Merged: {len(master_df)} rows in master, "
          f"{len(snapshot_df)} rows in snapshot, "
          f"{len(merged)} rows in final merged")

    merged.to_csv(output_path, index=False)
    print(f"Merged file written to {output_path}")


def merge_for_show(
    slug: str,
    snapshot_filename: str,
) -> None:
    """
    Convenience wrapper for a single show.

    Assumes:
      master:   data/{slug}_master.csv
      snapshot: data/{snapshot_filename}  (already cleaned of non show videos)
    """
    master_path = DATA_DIR / f"{slug}_master.csv"
    snapshot_path = DATA_DIR / snapshot_filename

    if not master_path.exists():
        raise FileNotFoundError(
            f"Expected master CSV at {master_path}. "
            f"Rename your old cleaned file for this show to match that pattern."
        )

    if not snapshot_path.exists():
        raise FileNotFoundError(
            f"Snapshot CSV not found at {snapshot_path}. "
            f"Make sure you saved your cleaned snapshot with that filename."
        )

    merge_master_with_snapshot(
        master_path=master_path,
        snapshot_path=snapshot_path,
        output_path=master_path,
        key="video_id",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge a per show master CSV with a cleaned snapshot CSV."
    )
    parser.add_argument(
        "--slug",
        required=True,
        help="Show slug, for example 'the_amazing_race'. "
             "Expects data/{slug}_master.csv to exist.",
    )
    parser.add_argument(
        "--snapshot",
        required=True,
        help="Snapshot filename in the data/ folder, for example "
             "'the_amazing_race_20251201_clean.csv'.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    merge_for_show(slug=args.slug, snapshot_filename=args.snapshot)
