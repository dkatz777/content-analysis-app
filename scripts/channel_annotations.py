# scripts/channel_annotations.py

from __future__ import annotations

from pathlib import Path
from datetime import date
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
CHANNEL_ANN_PATH = DATA_DIR / "channel_annotations.csv"


def load_channel_annotations() -> pd.DataFrame:
    """
    Load global channel annotations.

    Columns:
      channel_id       YouTube channel id (primary key)
      channel_title    Last known title for convenience
      ugc              bool flag, True if the channel is UGC
      first_marked_at  ISO date string when first annotated
      last_updated_at  ISO date string when last updated
    """
    if CHANNEL_ANN_PATH.exists():
        df = pd.read_csv(CHANNEL_ANN_PATH)

        # Ensure required columns exist
        for col, default in [
            ("channel_id", ""),
            ("channel_title", ""),
            ("ugc", False),
            ("first_marked_at", ""),
            ("last_updated_at", ""),
        ]:
            if col not in df.columns:
                df[col] = default

        # Normalize channel_id to string
        df["channel_id"] = df["channel_id"].astype(str)

        # Robust normalization of ugc to bool
        def to_bool(x):
            if isinstance(x, bool):
                return x
            if pd.isna(x):
                return False
            s = str(x).strip().lower()
            if s in ("true", "1", "yes", "y"):
                return True
            if s in ("false", "0", "no", "n", ""):
                return False
            return False

        df["ugc"] = df["ugc"].apply(to_bool)

        return df

    return pd.DataFrame(
        {
            "channel_id": pd.Series(dtype="string"),
            "channel_title": pd.Series(dtype="string"),
            "ugc": pd.Series(dtype="bool"),
            "first_marked_at": pd.Series(dtype="string"),
            "last_updated_at": pd.Series(dtype="string"),
        }
    )


def upsert_channel_annotations(rows_df: pd.DataFrame) -> None:
    """
    Upsert UGC flags for the given channels.

    rows_df must contain:
      - channel_id
      - channel_title
      - ugc  (bool)
    """
    if rows_df.empty:
        return

    ann = load_channel_annotations()
    today = date.today().isoformat()

    new_rows = rows_df[["channel_id", "channel_title", "ugc"]].copy()
    new_rows["channel_id"] = new_rows["channel_id"].astype(str)

    # Ensure ugc is bool
    new_rows["ugc"] = new_rows["ugc"].apply(lambda x: bool(x))
    new_rows["last_updated_at"] = today

    if "first_marked_at" not in ann.columns:
        ann["first_marked_at"] = ""
    if "last_updated_at" not in ann.columns:
        ann["last_updated_at"] = ""

    # For new rows, first_marked_at = today if not set before
    new_rows["first_marked_at"] = today

    combined = pd.concat([ann, new_rows], ignore_index=True)

    combined = (
        combined.sort_values("last_updated_at")
        .drop_duplicates(subset=["channel_id"], keep="last")
        .reset_index(drop=True)
    )

    combined.to_csv(CHANNEL_ANN_PATH, index=False)
    print(f"Channel annotations updated at {CHANNEL_ANN_PATH}")
